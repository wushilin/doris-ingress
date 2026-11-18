//! Arrow IPC encoding of batches and schemas carried inside Raft entries.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;

use crate::error::{Error, Result};

pub fn encode_batch(batch: &RecordBatch) -> Result<Bytes> {
    let mut w = StreamWriter::try_new(Vec::new(), &batch.schema())?;
    w.write(batch)?;
    w.finish()?;
    Ok(Bytes::from(w.into_inner()?))
}

pub fn decode_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut r = StreamReader::try_new(Cursor::new(bytes), None)?;
    let schema = r.schema();
    let mut batches = Vec::new();
    for b in &mut r {
        batches.push(b?);
    }
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(schema)),
        1 => Ok(batches.pop().unwrap()),
        _ => Ok(arrow::compute::concat_batches(&schema, &batches)?),
    }
}

pub fn encode_schema(schema: &Schema) -> Result<Bytes> {
    let mut w = StreamWriter::try_new(Vec::new(), schema)?;
    w.finish()?;
    Ok(Bytes::from(w.into_inner()?))
}

pub fn decode_schema(bytes: &[u8]) -> Result<SchemaRef> {
    let r = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| Error::Schema(format!("bad schema bytes: {e}")))?;
    Ok(Arc::clone(&r.schema()))
}

/// Encoding of a write operation's rows inside a Raft entry ([`crate::raft::WriteOpWire::format`]).
pub const FORMAT_STREAM: u8 = 0;
/// One Arrow record-batch message without the schema (the table schema is known to every node):
/// `u32 LE metadata length | flatbuffer metadata | body`. Much smaller than a stream for few rows.
pub const FORMAT_BATCH: u8 = 1;

/// Encodes a batch as a schema-less record-batch message ([`FORMAT_BATCH`]).
pub fn encode_batch_message(batch: &RecordBatch) -> Result<Bytes> {
    use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
    let (dicts, enc) = IpcDataGenerator::default().encode(
        batch,
        &mut DictionaryTracker::new(false),
        &IpcWriteOptions::default(),
        &mut IpcWriteContext::default(),
    )?;
    if !dicts.is_empty() {
        return Err(Error::Schema("dictionary-encoded columns are not supported".into()));
    }
    let mut out = Vec::with_capacity(4 + enc.ipc_message.len() + enc.arrow_data.len());
    out.extend_from_slice(&(enc.ipc_message.len() as u32).to_le_bytes());
    out.extend_from_slice(&enc.ipc_message);
    out.extend_from_slice(&enc.arrow_data);
    Ok(Bytes::from(out))
}

/// Decodes a [`FORMAT_BATCH`] message against the table schema.
pub fn decode_batch_message(bytes: &[u8], schema: &SchemaRef) -> Result<RecordBatch> {
    let bad = |m: &str| Error::Schema(format!("bad batch message: {m}"));
    if bytes.len() < 4 {
        return Err(bad("short"));
    }
    let len = u32::from_le_bytes(bytes[..4].try_into().expect("4 bytes")) as usize;
    let meta = bytes.get(4..4 + len).ok_or_else(|| bad("metadata length"))?;
    let body = &bytes[4 + len..];
    let msg = arrow::ipc::root_as_message(meta).map_err(|e| bad(&e.to_string()))?;
    let rb = msg.header_as_record_batch().ok_or_else(|| bad("not a record batch"))?;
    let buf = arrow::buffer::Buffer::from(body.to_vec());
    Ok(arrow::ipc::reader::read_record_batch(&buf, rb, schema.clone(), &Default::default(), None, &msg.version())?)
}

/// Decodes a write operation's rows in either format.
pub fn decode_op(format: u8, bytes: &[u8], schema: &SchemaRef) -> Result<RecordBatch> {
    match format {
        FORMAT_STREAM => decode_batch(bytes),
        FORMAT_BATCH => decode_batch_message(bytes, schema),
        other => Err(Error::Schema(format!("unknown write format {other}"))),
    }
}

/// Bytes of memory a batch's rows occupy. A batch decoded from a write shares one buffer among
/// all its columns, and Arrow's `get_array_memory_size` counts that whole buffer once per
/// column, which overstates such a batch several times over. This counts each column's own
/// part of the buffer.
pub fn batch_memory_size(batch: &RecordBatch) -> usize {
    use arrow::array::Array;
    batch.columns().iter().map(|c| c.to_data().get_slice_memory_size().unwrap_or_else(|_| c.get_array_memory_size())).sum()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    #[test]
    fn a_decoded_batch_is_counted_at_its_real_size() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let n = 10_000i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..n)),
                Arc::new(StringArray::from_iter_values((0..n).map(|i| format!("sg-{}", 100_000 + i)))),
                Arc::new(Float64Array::from_iter_values((0..n).map(|i| i as f64))),
                Arc::new(TimestampMicrosecondArray::from_iter_values(0..n)),
                Arc::new(StringArray::from_iter_values((0..n).map(|i| format!("ca-{}", 100_000 + i)))),
            ],
        )
        .unwrap();
        let wire = super::encode_batch_message(&batch).unwrap();
        let decoded = super::decode_batch_message(&wire, &schema).unwrap();
        // About 50 bytes a row. Arrow's own count for the decoded batch is about seven times that.
        let counted = super::batch_memory_size(&decoded);
        assert!(counted.abs_diff(wire.len()) < wire.len() / 20, "counted {counted}, on the wire {}", wire.len());
        assert!(decoded.get_array_memory_size() > 5 * counted);
    }
}
