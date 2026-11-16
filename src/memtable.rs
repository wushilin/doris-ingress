use std::collections::HashMap;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow::compute::interleave;
use arrow::row::Rows;

use crate::error::Result;

/// One version of a key: where its row lives and whether it is a tombstone.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Version {
    pub seq: u64,
    pub batch: u32,
    pub row: u32,
    pub deleted: bool,
}

/// Most keys are written once per memtable, so the single-version case avoids a heap allocation.
enum Versions {
    One(Version),
    Many(Vec<Version>),
}

impl Versions {
    fn push(&mut self, v: Version) {
        match self {
            Versions::One(first) => *self = Versions::Many(vec![*first, v]),
            Versions::Many(all) => all.push(v),
        }
    }

    /// Latest version with `seq <= snapshot`. Versions are stored in increasing seq order.
    fn latest(&self, snapshot: u64) -> Option<Version> {
        match self {
            Versions::One(v) => (v.seq <= snapshot).then_some(*v),
            Versions::Many(all) => all.iter().rev().find(|v| v.seq <= snapshot).copied(),
        }
    }
}

/// Append-only Arrow batches plus a primary-key index.
#[derive(Default)]
pub(crate) struct Memtable {
    batches: Vec<RecordBatch>,
    /// Per batch: sequence number of row 0, and whether the batch holds tombstones.
    batch_meta: Vec<(u64, bool)>,
    index: HashMap<Box<[u8]>, Versions>,
    bytes: usize,
    rows: usize,
    first_write: Option<Instant>,
    seq_min: u64,
    seq_max: u64,
}

const INDEX_ENTRY_OVERHEAD: usize = 64;

impl Memtable {
    /// Adds a full-schema batch. Row `i` gets `base_seq + i`. Returns the bytes accounted.
    pub fn insert(&mut self, batch: RecordBatch, keys: &Rows, base_seq: u64, deleted: bool) -> usize {
        let n = batch.num_rows();
        if n == 0 {
            return 0;
        }
        let batch_idx = self.batches.len() as u32;
        let mut added = batch.get_array_memory_size();
        for row in 0..n {
            let v = Version { seq: base_seq + row as u64, batch: batch_idx, row: row as u32, deleted };
            let key = keys.row(row);
            match self.index.get_mut(key.as_ref()) {
                Some(versions) => {
                    versions.push(v);
                    added += std::mem::size_of::<Version>();
                }
                None => {
                    let key: Box<[u8]> = key.as_ref().into();
                    added += key.len() + INDEX_ENTRY_OVERHEAD;
                    self.index.insert(key, Versions::One(v));
                }
            }
        }
        if self.rows == 0 {
            self.first_write = Some(Instant::now());
            self.seq_min = base_seq;
        }
        self.seq_max = base_seq + n as u64 - 1;
        self.rows += n;
        self.bytes += added;
        self.batches.push(batch);
        self.batch_meta.push((base_seq, deleted));
        added
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn first_write(&self) -> Option<Instant> {
        self.first_write
    }
    pub fn seq_range(&self) -> (u64, u64) {
        (self.seq_min, self.seq_max)
    }
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn lookup(&self, key: &[u8], snapshot: u64) -> Option<Version> {
        self.index.get(key)?.latest(snapshot)
    }

    /// Latest version of every key at `snapshot`.
    pub fn iter_latest(&self, snapshot: u64) -> impl Iterator<Item = (&[u8], Version)> {
        self.index.iter().filter_map(move |(k, v)| v.latest(snapshot).map(|v| (k.as_ref(), v)))
    }
}

/// A row picked by a scan: `batch` indexes the batch list handed to [`materialize`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Selected {
    pub batch: usize,
    pub row: usize,
    pub seq: u64,
    pub deleted: bool,
}

/// Rows gathered into Arrow arrays: the table's columns, then seq and the tombstone flag.
pub(crate) struct Chunk {
    pub columns: Vec<ArrayRef>,
    pub seq: Int64Array,
    pub deleted: BooleanArray,
}

/// Copies the selected rows out of `batches`, at most `chunk_rows` rows per chunk.
pub(crate) fn materialize(
    batches: &[RecordBatch],
    selected: &[Selected],
    chunk_rows: usize,
) -> Result<Vec<Chunk>> {
    let mut out = Vec::new();
    let Some(first) = batches.first() else { return Ok(out) };
    let num_cols = first.num_columns();
    for part in selected.chunks(chunk_rows.max(1)) {
        let indices: Vec<(usize, usize)> = part.iter().map(|s| (s.batch, s.row)).collect();
        let mut columns = Vec::with_capacity(num_cols);
        for c in 0..num_cols {
            let arrays: Vec<&dyn arrow::array::Array> =
                batches.iter().map(|b| b.column(c).as_ref()).collect();
            columns.push(interleave(&arrays, &indices)?);
        }
        out.push(Chunk {
            columns,
            seq: Int64Array::from_iter_values(part.iter().map(|s| s.seq as i64)),
            deleted: BooleanArray::from_iter(part.iter().map(|s| Some(s.deleted))),
        });
    }
    Ok(out)
}

/// Rows picked from one memtable batch: a selection bitmap over the batch.
pub(crate) struct BatchSelection {
    pub batch: RecordBatch,
    pub base_seq: u64,
    pub deleted: bool,
    pub mask: arrow::buffer::BooleanBuffer,
    pub count: usize,
}

/// Latest version of every key across `layers` (newest first) at `snapshot`, as per-batch
/// selection bitmaps. Avoids per-row gathering: rows are later taken with a vectorized filter, or
/// not copied at all when a whole batch is selected.
pub(crate) fn select_latest(layers: &[&Memtable], snapshot: u64, include_deleted: bool) -> Vec<BatchSelection> {
    // Cross-layer dedupe is only needed when more than one memtable holds rows.
    let multi = layers.iter().filter(|l| !l.is_empty()).count() > 1;
    let mut seen: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    let mut out = Vec::new();
    for layer in layers {
        if layer.is_empty() {
            continue;
        }
        let mut masks: Vec<arrow::array::BooleanBufferBuilder> = layer
            .batches
            .iter()
            .map(|b| {
                let mut m = arrow::array::BooleanBufferBuilder::new(b.num_rows());
                m.append_n(b.num_rows(), false);
                m
            })
            .collect();
        let mut counts = vec![0usize; layer.batches.len()];
        for (key, v) in layer.iter_latest(snapshot) {
            if multi && !seen.insert(key) {
                continue;
            }
            if !include_deleted && v.deleted {
                continue;
            }
            masks[v.batch as usize].set_bit(v.row as usize, true);
            counts[v.batch as usize] += 1;
        }
        for (i, mut m) in masks.into_iter().enumerate() {
            if counts[i] > 0 {
                let (base_seq, deleted) = layer.batch_meta[i];
                out.push(BatchSelection { batch: layer.batches[i].clone(), base_seq, deleted, mask: m.finish(), count: counts[i] });
            }
        }
    }
    out
}

/// The selected rows of one batch: table columns, their sequence numbers, the tombstone flag.
pub(crate) fn emit(sel: &BatchSelection) -> Result<Chunk> {
    let n = sel.batch.num_rows();
    let seq_all = Int64Array::from_iter_values((0..n as i64).map(|i| sel.base_seq as i64 + i));
    let (columns, seq) = if sel.count == n {
        (sel.batch.columns().to_vec(), seq_all)
    } else {
        let pred = BooleanArray::new(sel.mask.clone(), None);
        let filtered = arrow::compute::filter_record_batch(&sel.batch, &pred)?;
        let seq = arrow::compute::filter(&seq_all, &pred)?;
        (filtered.columns().to_vec(), seq.as_any().downcast_ref::<Int64Array>().expect("int64 filter").clone())
    };
    Ok(Chunk { columns, seq, deleted: BooleanArray::from(vec![sel.deleted; sel.count]) })
}
