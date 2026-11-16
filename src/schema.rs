use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, make_array, new_null_array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};

use crate::config::TableConfig;
use crate::error::{Error, Result};

/// Column name of the sequence value in scan results.
pub const SEQ_COLUMN: &str = "__seq";
/// Column name of the tombstone flag in scan results.
pub const DELETED_COLUMN: &str = "__deleted";
/// Source column carrying the delete flag in a load sent to Doris.
pub const LOAD_DELETE_COLUMN: &str = "__ingress_del";
/// Source column carrying the sequence value when the Doris table uses a hidden sequence column.
pub const LOAD_HIDDEN_SEQ_COLUMN: &str = "__ingress_seq";

/// How the Doris table stores its sequence value.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SeqTarget {
    /// `function_column.sequence_col = '<name>'`: a real BIGINT column. It is owned by
    /// doris-ingress and is not part of the schema clients write.
    Column(String),
    /// `function_column.sequence_type = 'bigint'`: the hidden sequence column.
    Hidden,
}

/// A Doris table attached to doris-ingress.
#[derive(Clone, Debug)]
pub struct TableDef {
    pub db: String,
    pub name: String,
    /// Columns clients write, in Doris order, without the sequence column.
    pub schema: SchemaRef,
    /// Unique-key column names.
    pub key_columns: Vec<String>,
    pub seq: SeqTarget,
    pub config: TableConfig,
}

/// Serializable form of [`TableDef`], carried in Raft entries and snapshots.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableDefWire {
    pub db: String,
    pub name: String,
    /// Arrow IPC-encoded schema.
    pub schema: bytes::Bytes,
    pub key_columns: Vec<String>,
    pub seq: SeqTarget,
    pub config: TableConfig,
}

impl TableDef {
    pub fn to_wire(&self) -> Result<TableDefWire> {
        Ok(TableDefWire {
            db: self.db.clone(),
            name: self.name.clone(),
            schema: crate::ipc::encode_schema(&self.schema)?,
            key_columns: self.key_columns.clone(),
            seq: self.seq.clone(),
            config: self.config.clone(),
        })
    }

    pub fn from_wire(w: &TableDefWire) -> Result<Self> {
        Ok(Self {
            db: w.db.clone(),
            name: w.name.clone(),
            schema: crate::ipc::decode_schema(&w.schema)?,
            key_columns: w.key_columns.clone(),
            seq: w.seq.clone(),
            config: w.config.clone(),
        })
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}", self.db, self.name)
    }

    /// Name of the sequence column in the data sent to Doris.
    pub fn load_seq_column(&self) -> &str {
        match &self.seq {
            SeqTarget::Column(name) => name,
            SeqTarget::Hidden => LOAD_HIDDEN_SEQ_COLUMN,
        }
    }
}

/// Schema-derived helpers shared by the write, read and flush paths.
pub(crate) struct TableCodec {
    pub schema: SchemaRef,
    pub key_idx: Vec<usize>,
    pub scan_schema: SchemaRef,
    pub load_schema: SchemaRef,
    converter: RowConverter,
}

impl TableCodec {
    pub fn new(def: &TableDef) -> Result<Self> {
        if def.key_columns.is_empty() {
            return Err(Error::Schema("table has no key columns".into()));
        }
        let reserved = [SEQ_COLUMN, DELETED_COLUMN, LOAD_DELETE_COLUMN, def.load_seq_column()];
        for f in def.schema.fields() {
            if reserved.contains(&f.name().as_str()) {
                return Err(Error::Schema(format!(
                    "column {} is reserved (the sequence column must not be in the client schema)",
                    f.name()
                )));
            }
        }
        let mut key_idx = Vec::new();
        let mut sort_fields = Vec::new();
        for k in &def.key_columns {
            let idx = def
                .schema
                .index_of(k)
                .map_err(|_| Error::Schema(format!("key column {k} is not in the schema")))?;
            key_idx.push(idx);
            sort_fields.push(SortField::new(def.schema.field(idx).data_type().clone()));
        }
        let converter = RowConverter::new(sort_fields)?;

        let mut scan = def.schema.fields().iter().cloned().collect::<Vec<_>>();
        scan.push(Arc::new(Field::new(SEQ_COLUMN, DataType::Int64, false)));
        scan.push(Arc::new(Field::new(DELETED_COLUMN, DataType::Boolean, false)));

        let mut load = def.schema.fields().iter().cloned().collect::<Vec<_>>();
        load.push(Arc::new(Field::new(def.load_seq_column(), DataType::Int64, false)));
        load.push(Arc::new(Field::new(LOAD_DELETE_COLUMN, DataType::Int8, false)));

        Ok(Self {
            schema: def.schema.clone(),
            key_idx,
            scan_schema: Arc::new(Schema::new(scan)),
            load_schema: Arc::new(Schema::new(load)),
            converter,
        })
    }

    /// Reorders an upsert batch to the table schema and checks types and key nullability.
    pub fn conform_upsert(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.num_columns() != self.schema.fields().len() {
            return Err(Error::Schema(format!(
                "expected {} columns, got {} (full-row writes only)",
                self.schema.fields().len(),
                batch.num_columns()
            )));
        }
        let mut cols = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let col = column_by_name(batch, field)?;
            if !field.is_nullable() && col.null_count() > 0 {
                return Err(Error::Schema(format!("column {} must not be null", field.name())));
            }
            cols.push(col);
        }
        self.check_keys_not_null(&cols)?;
        Ok(RecordBatch::try_new(self.schema.clone(), cols)?)
    }

    /// Builds a full-schema tombstone batch from a batch holding the key columns.
    /// Non-key columns are null where allowed and a zero value otherwise.
    pub fn conform_delete(&self, keys: &RecordBatch) -> Result<RecordBatch> {
        let n = keys.num_rows();
        let mut cols = Vec::with_capacity(self.schema.fields().len());
        for (i, field) in self.schema.fields().iter().enumerate() {
            if self.key_idx.contains(&i) {
                cols.push(column_by_name(keys, field)?);
            } else if field.is_nullable() {
                cols.push(new_null_array(field.data_type(), n));
            } else {
                cols.push(zero_array(field, n)?);
            }
        }
        self.check_keys_not_null(&cols)?;
        Ok(RecordBatch::try_new(self.schema.clone(), cols)?)
    }

    /// Encodes the key columns of a full-schema batch.
    pub fn encode_keys(&self, batch: &RecordBatch) -> Result<Rows> {
        let cols: Vec<ArrayRef> = self.key_idx.iter().map(|&i| batch.column(i).clone()).collect();
        Ok(self.converter.convert_columns(&cols)?)
    }

    /// Encodes a batch that holds only key columns (looked up by name).
    pub fn encode_key_batch(&self, keys: &RecordBatch) -> Result<Rows> {
        let mut cols = Vec::with_capacity(self.key_idx.len());
        for &i in &self.key_idx {
            cols.push(column_by_name(keys, self.schema.field(i))?);
        }
        self.check_key_cols_not_null(&cols)?;
        Ok(self.converter.convert_columns(&cols)?)
    }

    fn check_keys_not_null(&self, cols: &[ArrayRef]) -> Result<()> {
        let keys: Vec<ArrayRef> = self.key_idx.iter().map(|&i| cols[i].clone()).collect();
        self.check_key_cols_not_null(&keys)
    }

    fn check_key_cols_not_null(&self, keys: &[ArrayRef]) -> Result<()> {
        for (col, &i) in keys.iter().zip(&self.key_idx) {
            if col.null_count() > 0 {
                return Err(Error::Schema(format!(
                    "key column {} must not be null",
                    self.schema.field(i).name()
                )));
            }
        }
        Ok(())
    }
}

fn column_by_name(batch: &RecordBatch, field: &Field) -> Result<ArrayRef> {
    let col = batch
        .column_by_name(field.name())
        .ok_or_else(|| Error::Schema(format!("missing column {}", field.name())))?;
    if col.data_type() != field.data_type() {
        // Accept compatible encodings (e.g. LargeUtf8 for Utf8, Int32 for Int64); a cast that
        // would lose or invent values fails instead of silently converting.
        let opts = arrow::compute::CastOptions { safe: false, ..Default::default() };
        return arrow::compute::cast_with_options(col, field.data_type(), &opts).map_err(|e| {
            Error::Schema(format!("column {} has type {}, expected {}: {e}", field.name(), col.data_type(), field.data_type()))
        });
    }
    Ok(col.clone())
}

/// A non-null array of zero values (0, empty string, epoch…) for a NOT NULL column of a tombstone.
fn zero_array(field: &Field, n: usize) -> Result<ArrayRef> {
    let data = new_null_array(field.data_type(), n).to_data();
    let data = data.into_builder().nulls(None).build().map_err(|e| {
        Error::Schema(format!(
            "cannot build a tombstone filler for NOT NULL column {} of type {}: {e}",
            field.name(),
            field.data_type()
        ))
    })?;
    Ok(make_array(data))
}
