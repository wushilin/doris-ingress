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
