//! The replicated state machine: memtables per table, the sequence counter, flush-unit states.
//!
//! `apply` is deterministic: it uses only the entry contents and prior state.
//!
//! Snapshots are by reference. A snapshot records metadata plus, for every buffered row, which
//! log entry (index, op, base seq) it came from. Building one writes a few KB; the log store keeps
//! the referenced entries (the retention floor) so memtables can be rebuilt after a restart.
//! Entries a follower lacks are shipped inside [`SnapshotData`] and embedded in the follower's
//! snapshot file.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine, Snapshot};

type SnapshotOf = openraft::type_config::alias::SnapshotOf<TypeConfig, SnapshotData>;
use openraft::EntryPayload;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

use super::log_store::RetentionFloor;
use super::{Entry, LogId, Request, Response, SnapshotData, SnapshotMeta, StoredMembership, TypeConfig, decode, encode};
use crate::config::MemoryConfig;
use crate::memory::Governor;
use crate::memtable::Memtable;
use crate::schema::{TableCodec, TableDef, TableDefWire};
use crate::wal::format::{FileKind, Frame, FrameKind, build_tag};
use crate::wal::{Wal, read_container_file, write_container_file};

/// Where a buffered row came from: op `op` of the Write entry at `index`, first seq `base_seq`.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntryRef {
    pub index: u64,
    pub op: u32,
    pub base_seq: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum UnitState {
    Frozen,
    Loading { label: String },
    Visible,
}

pub(crate) struct Unit {
    pub mem: Arc<Memtable>,
    pub refs: Vec<EntryRef>,
    pub seq_start: u64,
    pub seq_end: u64,
    pub state: UnitState,
}

#[derive(Default)]
pub(crate) struct Units {
    pub active: Memtable,
    pub active_refs: Vec<EntryRef>,
    /// Frozen units in seq order.
    pub frozen: VecDeque<Unit>,
    pub bytes: usize,
}

impl Units {
    fn rotate(&mut self) {
        if self.active.is_empty() {
            return;
        }
        let mem = std::mem::take(&mut self.active);
        let (seq_start, seq_end) = mem.seq_range();
        self.frozen.push_back(Unit {
            mem: Arc::new(mem),
            refs: std::mem::take(&mut self.active_refs),
            seq_start,
            seq_end,
            state: UnitState::Frozen,
        });
    }

    fn unit_mut(&mut self, seq_end: u64) -> Option<&mut Unit> {
        self.frozen.iter_mut().find(|u| u.seq_end == seq_end)
    }
}

pub(crate) struct TableSm {
    pub def: Arc<TableDef>,
    pub codec: TableCodec,
    pub loader: String,
    pub units: Mutex<Units>,
}

/// Apply progress. Only the apply task writes it; snapshot capture reads it consistently.
struct ApplyMeta {
    last_applied: Option<LogId>,
    membership: StoredMembership,
    next_seq: u64,
}

/// Shared state machine core, read by the node for queries and flush decisions.
pub struct SmCore {
    /// Attached tables. Written only by attach and restore, so the write path's table lookups
    /// never wait behind applying writes. Lock order: `meta` before `tables`.
    tables: RwLock<BTreeMap<String, Arc<TableSm>>>,
    meta: Mutex<ApplyMeta>,
    /// Highest assigned sequence number: the default read point.
    visible: AtomicU64,
    pub(crate) governor: Governor,
    wal: Arc<Wal>,
    snap_dir: PathBuf,
    floor: RetentionFloor,
    /// Entries referenced by the current snapshot that are not in the local WAL.
    embedded: Mutex<BTreeMap<u64, Bytes>>,
    current: Mutex<Option<(SnapshotMeta, PathBuf)>>,
    /// Signalled after every applied entry.
    pub(crate) applied: Notify,
    /// Time spent applying entries, and entries/ops applied (for diagnosing apply saturation).
    pub apply_nanos: AtomicU64,
    pub apply_entries: AtomicU64,
    pub apply_ops: AtomicU64,
}

/// The snapshot file body: openraft metadata plus our state.
#[derive(Serialize, Deserialize)]
struct SnapFile {
    meta: SnapshotMeta,
    state: Bytes,
}

#[derive(Serialize, Deserialize)]
struct SnapState {
    build: String,
    next_seq: u64,
    tables: Vec<TableSnap>,
}

#[derive(Serialize, Deserialize)]
struct TableSnap {
    def: TableDefWire,
    loader: String,
    active_refs: Vec<EntryRef>,
    units: Vec<UnitSnap>,
}

#[derive(Serialize, Deserialize)]
struct UnitSnap {
    seq_start: u64,
    seq_end: u64,
    state: UnitState,
    refs: Vec<EntryRef>,
}

impl SmCore {
    /// Opens the state machine, restoring the latest snapshot (if any) from disk.
    pub fn open(wal: Arc<Wal>, snap_dir: PathBuf, floor: RetentionFloor, memory: &MemoryConfig) -> io::Result<Arc<Self>> {
        fs::create_dir_all(&snap_dir)?;
        let core = Arc::new(Self {
            tables: RwLock::new(BTreeMap::new()),
            meta: Mutex::new(ApplyMeta { last_applied: None, membership: StoredMembership::default(), next_seq: 1 }),
            visible: AtomicU64::new(0),
            governor: Governor::new(memory),
            wal,
            snap_dir,
            floor,
            embedded: Mutex::default(),
            current: Mutex::default(),
            applied: Notify::new(),
            apply_nanos: AtomicU64::new(0),
            apply_entries: AtomicU64::new(0),
            apply_ops: AtomicU64::new(0),
        });
        if let Some(path) = latest_snapshot_file(&core.snap_dir)? {
            let (file, entries) = read_snapshot_file(&path)?;
            core.restore(&file.meta, &file.state, entries)?;
            *core.current.lock() = Some((file.meta, path));
            core.update_floor();
        }
        Ok(core)
    }

    pub fn snapshot_seq(&self) -> u64 {
        self.visible.load(Ordering::Acquire)
    }

    pub(crate) fn table(&self, name: &str) -> Option<Arc<TableSm>> {
        self.tables.read().get(name).cloned()
    }

    pub(crate) fn tables(&self) -> Vec<Arc<TableSm>> {
        self.tables.read().values().cloned().collect()
    }

    pub fn last_applied(&self) -> Option<LogId> {
        self.meta.lock().last_applied
    }

    fn apply_entry(&self, entry: &Entry) -> Response {
        let index = entry.log_id.index;
        let resp = match &entry.payload {
            EntryPayload::Blank => Response::Ok,
            EntryPayload::Membership(m) => {
                self.meta.lock().membership = StoredMembership::new(Some(entry.log_id), m.clone());
                Response::Ok
            }
            EntryPayload::Normal(req) => self.apply_request(index, req),
        };
        let mut m = self.meta.lock();
        m.last_applied = Some(entry.log_id);
        self.visible.store(m.next_seq - 1, Ordering::Release);
        resp
    }

    fn apply_request(&self, index: u64, req: &Request) -> Response {
        match req {
            Request::AttachTable { def, loader, min_next_seq } => {
                let mut m = self.meta.lock();
                let mut tables = self.tables.write();
                let name = format!("{}.{}", def.db, def.name);
                if tables.contains_key(&name) {
                    return Response::Rejected(format!("table {name} is already attached"));
                }
                let built = TableDef::from_wire(def).and_then(|d| Ok((TableCodec::new(&d)?, d)));
                let (codec, def) = match built {
                    Ok(v) => v,
                    Err(e) => return Response::Rejected(e.to_string()),
                };
                tables.insert(
                    name,
                    Arc::new(TableSm { def: Arc::new(def), codec, loader: loader.clone(), units: Mutex::default() }),
                );
                m.next_seq = m.next_seq.max(*min_next_seq);
                Response::Ok
            }
            Request::Write { ops, groups } => {
                let groups: Vec<u32> = if groups.is_empty() { vec![ops.len() as u32] } else { groups.clone() };
                Response::Write { groups: self.apply_write(index, ops, &groups) }
            }
            Request::Rotate { table } => self.with_units(table, |u| {
                u.rotate();
                Response::Ok
            }),
            Request::FlushStart { table, seq_end, label } => self.with_units(table, |u| match u.unit_mut(*seq_end) {
                Some(unit) => {
                    if unit.state == UnitState::Frozen {
                        unit.state = UnitState::Loading { label: label.clone() };
                    }
                    Response::Ok
                }
                None => Response::Rejected(format!("no unit ending at {seq_end}")),
            }),
            Request::FlushDone { table, seq_end } => self.with_units(table, |u| match u.unit_mut(*seq_end) {
                Some(unit) => {
                    unit.state = UnitState::Visible;
                    Response::Ok
                }
                None => Response::Rejected(format!("no unit ending at {seq_end}")),
            }),
            Request::Drop { table, seq_end } => self.with_units(table, |u| {
                match u.frozen.iter().position(|x| x.seq_end == *seq_end && x.state == UnitState::Visible) {
                    Some(pos) => {
                        let unit = u.frozen.remove(pos).expect("position is valid");
                        let freed = unit.mem.bytes();
                        u.bytes -= freed;
                        self.governor.release(freed);
                        Response::Ok
                    }
                    None => Response::Rejected(format!("no visible unit ending at {seq_end}")),
                }
            }),
        }
    }

    /// Applies a Write entry. Each group (one client write) is validated as a whole and applied
    /// or rejected independently. Accepted rows get sequence numbers in op order; consecutive
    /// accepted ops on the same table and kind are merged into one memtable batch.
    fn apply_write(&self, index: u64, ops: &[crate::raft::WriteOpWire], groups: &[u32]) -> Vec<Result<Vec<(u64, u64)>, String>> {
        struct Op {
            idx: usize,
            group: usize,
            table: Arc<TableSm>,
            batch: arrow::array::RecordBatch,
            delete: bool,
        }
        let mut results: Vec<Result<Vec<(u64, u64)>, String>> = Vec::with_capacity(groups.len());
        let mut accepted: Vec<Op> = Vec::with_capacity(ops.len());
        let mut start = 0usize;
        for (g, &n) in groups.iter().enumerate() {
            let end = (start + n as usize).min(ops.len());
            let mut decoded = Vec::with_capacity(end - start);
            let mut failure = None;
            for (idx, op) in ops.iter().enumerate().take(end).skip(start) {
                let Some(table) = self.table(&op.table) else {
                    failure = Some(format!("unknown table {}", op.table));
                    break;
                };
                match crate::ipc::decode_op(op.format, &op.ipc, &table.codec.schema) {
                    Ok(b) if b.schema().fields() == table.codec.schema.fields() => {
                        decoded.push(Op { idx, group: g, table, batch: b, delete: op.delete })
                    }
                    Ok(_) => {
                        failure = Some(format!("schema mismatch for {}", op.table));
                        break;
                    }
                    Err(e) => {
                        failure = Some(format!("bad batch for {}: {e}", op.table));
                        break;
                    }
                }
            }
            match failure {
                Some(f) => results.push(Err(f)),
                None => {
                    results.push(Ok(Vec::with_capacity(decoded.len())));
                    accepted.extend(decoded);
                }
            }
            start = end;
        }

        let mut m = self.meta.lock();
        let mut i = 0;
        while i < accepted.len() {
            // A run of consecutive accepted ops on the same table and kind.
            let mut j = i + 1;
            while j < accepted.len() && Arc::ptr_eq(&accepted[j].table, &accepted[i].table) && accepted[j].delete == accepted[i].delete {
                j += 1;
            }
            let run_base = m.next_seq;
            let table = accepted[i].table.clone();
            let delete = accepted[i].delete;
            let mut refs = Vec::with_capacity(j - i);
            let mut parts = Vec::with_capacity(j - i);
            for op in &accepted[i..j] {
                let rows = op.batch.num_rows() as u64;
                let base = m.next_seq;
                if let Ok(seqs) = &mut results[op.group] {
                    seqs.push((base, base + rows.max(1) - 1));
                }
                if rows > 0 {
                    refs.push(EntryRef { index, op: op.idx as u32, base_seq: base });
                    parts.push(op.batch.clone());
                    m.next_seq += rows;
                }
            }
            if !parts.is_empty() {
                let merged = if parts.len() == 1 {
                    parts.pop().expect("one part")
                } else {
                    arrow::compute::concat_batches(&table.codec.schema, &parts).expect("same schema")
                };
                // Keys were validated by the leader; encoding cannot fail for a conforming batch.
                if let Ok(keys) = table.codec.encode_keys(&merged) {
                    let mut u = table.units.lock();
                    let added = u.active.insert(merged, &keys, run_base, delete);
                    u.active_refs.extend(refs);
                    u.bytes += added;
                    self.governor.add(added);
                }
            }
            i = j;
        }
        results
    }

    fn with_units(&self, table: &str, f: impl FnOnce(&mut Units) -> Response) -> Response {
        match self.table(table) {
            Some(t) => f(&mut t.units.lock()),
            None => Response::Rejected(format!("unknown table {table}")),
        }
    }

    /// Captures metadata and row references at the current applied point.
    fn capture(&self) -> io::Result<(SnapshotMeta, Bytes, Vec<u64>)> {
        let m = self.meta.lock(); // excludes concurrent apply
        let attached = self.tables.read();
        let mut tables = Vec::new();
        let mut refs = Vec::new();
        for t in attached.values() {
            let u = t.units.lock();
            refs.extend(u.active_refs.iter().map(|r| r.index));
            let units = u
                .frozen
                .iter()
                .map(|x| {
                    refs.extend(x.refs.iter().map(|r| r.index));
                    UnitSnap { seq_start: x.seq_start, seq_end: x.seq_end, state: x.state.clone(), refs: x.refs.clone() }
                })
                .collect();
            tables.push(TableSnap {
                def: t.def.to_wire().map_err(io::Error::other)?,
                loader: t.loader.clone(),
                active_refs: u.active_refs.clone(),
                units,
            });
        }
        let state = encode(&SnapState { build: build_tag(), next_seq: m.next_seq, tables })?;
        let meta = SnapshotMeta { last_log_id: m.last_applied, last_membership: m.membership.clone() };
        refs.sort_unstable();
        refs.dedup();
        Ok((meta, state, refs))
    }

    /// Reads a referenced entry from the WAL or the embedded set.
    fn entry_body(&self, index: u64) -> io::Result<Bytes> {
        if let Some(b) = self.embedded.lock().get(&index) {
            return Ok(b.clone());
        }
        self.wal
            .read(index)?
            .ok_or_else(|| io::Error::other(format!("log entry {index} needed by the state machine is missing")))
    }

    /// Replaces the whole state with a snapshot. `entries` supplies referenced entries not in the
    /// local WAL.
    fn restore(&self, meta: &SnapshotMeta, state: &[u8], entries: BTreeMap<u64, Bytes>) -> io::Result<()> {
        let snap: SnapState = decode(state)?;
        *self.embedded.lock() = entries;
        let mut tables = BTreeMap::new();
        let mut bytes_total = 0;
        let mut cache: Option<(u64, Request)> = None;
        let mut load = |r: &EntryRef| -> io::Result<(crate::raft::WriteOpWire, u64)> {
            if cache.as_ref().is_none_or(|(i, _)| *i != r.index) {
                let entry: Entry = decode(&self.entry_body(r.index)?)?;
                let EntryPayload::Normal(req) = entry.payload else {
                    return Err(io::Error::other(format!("entry {} is not a write", r.index)));
                };
                cache = Some((r.index, req));
            }
            match &cache.as_ref().expect("filled above").1 {
                Request::Write { ops, .. } => ops
                    .get(r.op as usize)
                    .cloned()
                    .map(|op| (op, r.base_seq))
                    .ok_or_else(|| io::Error::other(format!("entry {} has no op {}", r.index, r.op))),
                _ => Err(io::Error::other(format!("entry {} is not a write", r.index))),
            }
        };
        for t in snap.tables {
            let def = TableDef::from_wire(&t.def).map_err(io::Error::other)?;
            let codec = TableCodec::new(&def).map_err(io::Error::other)?;
            let mut units = Units::default();
            let mut fill = |mem: &mut Memtable, refs: &[EntryRef]| -> io::Result<usize> {
                let mut added = 0;
                for r in refs {
                    let (op, base) = load(r)?;
                    let batch = crate::ipc::decode_op(op.format, &op.ipc, &codec.schema).map_err(io::Error::other)?;
                    let keys = codec.encode_keys(&batch).map_err(io::Error::other)?;
                    added += mem.insert(batch, &keys, base, op.delete);
                }
                Ok(added)
            };
            let mut active = Memtable::default();
            units.bytes += fill(&mut active, &t.active_refs)?;
            units.active = active;
            units.active_refs = t.active_refs;
            for us in t.units {
                let mut mem = Memtable::default();
                units.bytes += fill(&mut mem, &us.refs)?;
                units.frozen.push_back(Unit {
                    mem: Arc::new(mem),
                    refs: us.refs,
                    seq_start: us.seq_start,
                    seq_end: us.seq_end,
                    state: us.state,
                });
            }
            bytes_total += units.bytes;
            let name = def.full_name();
            tables.insert(name, Arc::new(TableSm { def: Arc::new(def), codec, loader: t.loader, units: Mutex::new(units) }));
        }
        let mut m = self.meta.lock();
        let mut attached = self.tables.write();
        let old: usize = attached.values().map(|t| t.units.lock().bytes).sum();
        self.governor.release(old);
        self.governor.add(bytes_total);
        *attached = tables;
        *m = ApplyMeta { last_applied: meta.last_log_id, membership: meta.last_membership.clone(), next_seq: snap.next_seq };
        self.visible.store(snap.next_seq - 1, Ordering::Release);
        drop(attached);
        drop(m);
        self.applied.notify_waiters();
        Ok(())
    }

    /// Sets the retention floor to the oldest referenced entry that lives in the WAL.
    fn update_floor(&self) {
        let embedded = self.embedded.lock();
        let mut floor = u64::MAX;
        for t in self.tables() {
            let u = t.units.lock();
            for r in u.active_refs.iter().chain(u.frozen.iter().flat_map(|x| x.refs.iter())) {
                if !embedded.contains_key(&r.index) {
                    floor = floor.min(r.index);
                }
            }
        }
        self.floor.set(floor);
    }

    fn persist_snapshot(&self, meta: &SnapshotMeta, state: &Bytes, refs: &[u64]) -> io::Result<PathBuf> {
        // Embed referenced entries that are not in the local WAL (e.g. from an installed snapshot).
        let wal_range = self.wal.range();
        let in_wal = |i: u64| wal_range.is_some_and(|(f, l)| i >= f && i <= l);
        let mut frames = vec![Frame::new(FrameKind::SnapshotMeta, 0, encode(&SnapFile { meta: meta.clone(), state: state.clone() })?)];
        let mut keep = BTreeMap::new();
        {
            let embedded = self.embedded.lock();
            for &i in refs {
                if !in_wal(i) {
                    let body = embedded
                        .get(&i)
                        .cloned()
                        .ok_or_else(|| io::Error::other(format!("snapshot needs entry {i}, which is neither in the WAL nor embedded")))?;
                    frames.push(Frame::new(FrameKind::RaftEntry, i, body.clone()));
                    keep.insert(i, body);
                }
            }
        }
        let path = self.snap_dir.join(snapshot_file_name(meta));
        write_container_file(&path, FileKind::Snapshot, &frames)?;
        let referenced: HashSet<u64> = refs.iter().copied().collect();
        self.embedded.lock().retain(|i, _| referenced.contains(i) && keep.contains_key(i));
        let previous = self.current.lock().replace((meta.clone(), path.clone()));
        if let Some((_, old)) = previous
            && old != path
        {
            let _ = fs::remove_file(old);
        }
        Ok(path)
    }

    /// Full snapshot data for a follower: state plus every referenced entry.
    fn materialize(&self, path: &Path) -> io::Result<(SnapshotMeta, SnapshotData)> {
        let (file, embedded) = read_snapshot_file(path)?;
        let snap: SnapState = decode(&file.state)?;
        let mut indexes: Vec<u64> = snap
            .tables
            .iter()
            .flat_map(|t| t.active_refs.iter().chain(t.units.iter().flat_map(|u| u.refs.iter())).map(|r| r.index))
            .collect();
        indexes.sort_unstable();
        indexes.dedup();
        let mut entries = Vec::with_capacity(indexes.len());
        for i in indexes {
            let body = match embedded.get(&i) {
                Some(b) => b.clone(),
                None => self.entry_body(i)?,
            };
            entries.push((i, body));
        }
        Ok((file.meta, SnapshotData { meta: file.state, entries }))
    }
}

/// `snapshot-<last applied index>-<nanos>.snap`: sortable by index, unique per build.
fn snapshot_file_name(meta: &SnapshotMeta) -> String {
    let index = meta.last_log_id.as_ref().map(|l| l.index).unwrap_or(0);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("snapshot-{index:020}-{nanos}.snap")
}

fn latest_snapshot_file(dir: &Path) -> io::Result<Option<PathBuf>> {
    let mut best: Option<(u64, String, PathBuf)> = None;
    for e in fs::read_dir(dir)? {
        let path = e?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()).map(str::to_string) else { continue };
        let Some(id) = name.strip_prefix("snapshot-").and_then(|n| n.strip_suffix(".snap")) else { continue };
        let index = id.split('-').next().and_then(|i| i.parse().ok()).unwrap_or(0);
        if best.as_ref().is_none_or(|(bi, bn, _)| (index, &name) > (*bi, bn)) {
            best = Some((index, name, path));
        }
    }
    Ok(best.map(|(_, _, p)| p))
}

fn read_snapshot_file(path: &Path) -> io::Result<(SnapFile, BTreeMap<u64, Bytes>)> {
    let (header, frames) = read_container_file(path)?;
    if header.kind != FileKind::Snapshot {
        return Err(io::Error::other(format!("{} is not a snapshot file", path.display())));
    }
    let mut file = None;
    let mut entries = BTreeMap::new();
    for f in frames {
        match f.kind {
            FrameKind::SnapshotMeta => file = Some(decode::<SnapFile>(&f.body)?),
            FrameKind::RaftEntry => {
                entries.insert(f.index, f.body);
            }
            FrameKind::RaftMeta => {}
        }
    }
    let file = file.ok_or_else(|| io::Error::other(format!("{} has no snapshot metadata", path.display())))?;
    Ok((file, entries))
}

/// openraft-facing handle.
#[derive(Clone)]
pub struct StateMachine {
    pub core: Arc<SmCore>,
}

pub struct SnapBuilder {
    core: Arc<SmCore>,
}

impl RaftSnapshotBuilder<TypeConfig> for SnapBuilder {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf, io::Error> {
        let (meta, state, refs) = self.core.capture()?;
        self.core.persist_snapshot(&meta, &state, &refs)?;
        self.core.update_floor();
        // openraft keeps only the metadata of a built snapshot; data is materialized on demand.
        Ok(Snapshot { meta, snapshot: SnapshotData { meta: state, entries: Vec::new() } })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = SnapBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        let m = self.core.meta.lock();
        Ok((m.last_applied, m.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>> + Unpin + Send,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let t = std::time::Instant::now();
            let resp = self.core.apply_entry(&entry);
            self.core.apply_nanos.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            self.core.apply_entries.fetch_add(1, Ordering::Relaxed);
            if let EntryPayload::Normal(Request::Write { ops, .. }) = &entry.payload {
                self.core.apply_ops.fetch_add(ops.len() as u64, Ordering::Relaxed);
            }
            if let Some(r) = responder {
                r.send(resp);
            }
            self.core.applied.notify_waiters();
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapBuilder { core: self.core.clone() }
    }

    async fn install_snapshot(&mut self, meta: &SnapshotMeta, snapshot: SnapshotData) -> Result<(), io::Error> {
        let entries: BTreeMap<u64, Bytes> = snapshot.entries.into_iter().collect();
        // Persist first (all entries embedded), then switch state.
        let mut frames = vec![Frame::new(
            FrameKind::SnapshotMeta,
            0,
            encode(&SnapFile { meta: meta.clone(), state: snapshot.meta.clone() })?,
        )];
        frames.extend(entries.iter().map(|(i, b)| Frame::new(FrameKind::RaftEntry, *i, b.clone())));
        let path = self.core.snap_dir.join(snapshot_file_name(meta));
        write_container_file(&path, FileKind::Snapshot, &frames)?;
        self.core.restore(meta, &snapshot.meta, entries)?;
        let previous = self.core.current.lock().replace((meta.clone(), path.clone()));
        if let Some((_, old)) = previous
            && old != path
        {
            let _ = fs::remove_file(old);
        }
        self.core.update_floor();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf>, io::Error> {
        let Some((_, path)) = self.core.current.lock().clone() else { return Ok(None) };
        let (meta, data) = self.core.materialize(&path)?;
        Ok(Some(Snapshot { meta, snapshot: data }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::raft::WriteOpWire;
    use crate::schema::SeqTarget;
    use crate::wal::WalOptions;

    #[test]
    fn writers_batched_into_one_entry_are_rejected_independently() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = Wal::open(WalOptions::new(dir.path().join("wal"))).unwrap();
        let core = SmCore::open(Arc::new(wal), dir.path().join("snap"), RetentionFloor::default(), &MemoryConfig::default()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false), Field::new("v", DataType::Utf8, true)]));
        let def = TableDef {
            db: "db".into(),
            name: "t".into(),
            schema: schema.clone(),
            key_columns: vec!["k".into()],
            seq: SeqTarget::Hidden,
            config: Default::default(),
        };
        let attach = Request::AttachTable { def: Box::new(def.to_wire().unwrap()), loader: "x".into(), min_next_seq: 1 };
        assert_eq!(core.apply_request(1, &attach), Response::Ok);

        let batch = |k: i64| {
            let b = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![k])), Arc::new(StringArray::from(vec!["v"]))]).unwrap();
            crate::ipc::encode_batch(&b).unwrap()
        };
        let op = |table: &str, k: i64| WriteOpWire { table: table.into(), delete: false, ipc: batch(k), format: crate::ipc::FORMAT_STREAM };
        // Writer A: one good op. Writer B: a good op and one for an unknown table. Writer C: good.
        let req = Request::Write { ops: vec![op("db.t", 1), op("db.t", 2), op("db.nope", 3), op("db.t", 4)], groups: vec![1, 2, 1] };
        let Response::Write { groups } = core.apply_request(2, &req) else { panic!("expected a write response") };
        assert_eq!(groups[0], Ok(vec![(1, 1)]));
        assert!(groups[1].as_ref().unwrap_err().contains("unknown table db.nope"));
        assert_eq!(groups[2], Ok(vec![(2, 2)]), "C gets the next sequence number; B consumed none");
        let t = core.table("db.t").unwrap();
        assert_eq!(t.units.lock().active.rows(), 2, "B's valid op was not applied either");
    }
}
