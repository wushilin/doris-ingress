//! Stream Load through the `doris_rust_stream_load` SDK: CSV rows, compressed, uploaded by
//! several workers.
//!
//! How this differs from [`super::StreamLoader`]:
//! * A flush unit is cut into several loads (at most `batch_bytes` each, `workers` at a time), so a
//!   unit is no longer one Doris transaction. Readers of the merged view do not notice: the rows
//!   stay in the buffer until every load of the unit is visible.
//! * The SDK picks its own label for every upload and retries under a fresh one. The unit's own
//!   label is therefore not known to Doris; this loader remembers which units it has finished.
//!   After a crash or a leader change the unit is sent again in full, which is harmless: the
//!   sequence column makes a repeated row a no-op.
//! * The unit is done only when every load is VISIBLE. The SDK also accepts COMMITTED, so loads
//!   that did not answer `Success` are polled here until they are visible.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::DataType;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use async_trait::async_trait;
use doris_rust_stream_load::{AsyncClient, AuthenticationType, Compression, Config, LogLevel, Mode, ValidationMode};
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{LoadState, LoadUnit, Loader, StreamLoadConfig, StreamLoader};
use crate::error::{Error, Result};
use crate::schema::{LOAD_DELETE_COLUMN, SeqTarget, TableDef};

#[derive(Clone, Debug)]
pub struct SdkLoadConfig {
    /// FE URL, user and password. `be_url` is not used.
    pub doris: StreamLoadConfig,
    /// Concurrent uploads per table.
    pub workers: usize,
    /// How long an open batch waits for more rows before it is offered to a worker.
    pub linger: Duration,
    /// Largest request body before compression. The SDK allows at most 90 MiB.
    pub batch_bytes: usize,
    /// Compress request bodies with gzip.
    pub gzip: bool,
    /// Dry run: nothing is sent to Doris. Every upload is answered as a success after
    /// `dry_run_delay` (the SDK's fake send). The flusher then trims the rows as usual, so they
    /// end up nowhere: for testing the buffer and its backpressure without a Doris cluster.
    pub dry_run: bool,
    pub dry_run_delay: Duration,
}

impl SdkLoadConfig {
    pub fn new(doris: StreamLoadConfig) -> Self {
        Self {
            doris,
            workers: 4,
            linger: Duration::from_millis(5),
            batch_bytes: 80 << 20,
            gzip: true,
            dry_run: false,
            dry_run_delay: Duration::from_millis(500),
        }
    }
}

/// Rows handed to the SDK in one call. A call's rows always travel in the same upload.
const CHUNK_BYTES: usize = 4 << 20;
/// How many finished unit labels are remembered.
const REMEMBERED_UNITS: usize = 4096;

pub struct SdkLoader {
    cfg: SdkLoadConfig,
    /// One SDK client per table and column list.
    clients: tokio::sync::Mutex<HashMap<String, Arc<AsyncClient>>>,
    /// Labels of units whose every load is visible, oldest first.
    done: Mutex<(HashSet<String>, VecDeque<String>)>,
    /// For label-state lookups.
    poller: StreamLoader,
}

impl SdkLoader {
    pub fn new(cfg: SdkLoadConfig) -> Result<Self> {
        if cfg.dry_run {
            tracing::warn!(delay = ?cfg.dry_run_delay, "stream_sdk loader is in DRY RUN: nothing is sent to Doris, and flushed rows are discarded");
        }
        let poller = StreamLoader::new(cfg.doris.clone())?;
        Ok(Self { cfg, clients: Default::default(), done: Default::default(), poller })
    }

    async fn client(&self, unit: &LoadUnit) -> Result<Arc<AsyncClient>> {
        let t = &unit.table;
        let key = format!("{}|{}", t.full_name(), unit.columns.join(","));
        let mut clients = self.clients.lock().await;
        if let Some(c) = clients.get(&key) {
            return Ok(c.clone());
        }
        let mut headers = http_02::HeaderMap::new();
        let mut put = |k: &'static str, v: &str| -> Result<()> {
            let v = http_02::HeaderValue::from_str(v).map_err(|e| Error::Http(format!("header {k}: {e}")))?;
            headers.insert(k, v);
            Ok(())
        };
        put("merge_type", "MERGE")?;
        put("delete", &format!("{LOAD_DELETE_COLUMN}=1"))?;
        if t.seq == SeqTarget::Hidden {
            put("function_column.sequence_col", t.load_seq_column())?;
        }
        put("escape", "\\")?; // inside quoted text, `\` escapes `"` and itself
        let config = Config::builder()
            .endpoint(self.cfg.doris.fe_url.clone())
            .database(t.db.clone())
            .table(t.name.clone())
            .with_columns(unit.columns.iter().cloned())
            .mode(Mode::Csv)
            .compression(if self.cfg.gzip { Compression::Gz } else { Compression::None })
            .validation(ValidationMode::None) // rows are generated here
            .batch_bytes(self.cfg.batch_bytes)
            .linger(self.cfg.linger)
            .doris_upload_workers(self.cfg.workers)
            // One HTTP request (an upload or a label check) may take a minute; a stalled one is
            // then retried by the SDK. The flusher's own load timeout bounds the whole unit.
            .doris_upload_request_timeout(self.cfg.doris.timeout.min(Duration::from_secs(60)))
            .doris_upload_timeout(self.cfg.doris.timeout)
            .authentication_type(AuthenticationType::Basic)
            .authentication_token(format!("{}:{}", self.cfg.doris.user, self.cfg.doris.password))
            .headers(headers)
            .fake_send(self.cfg.dry_run)
            .fake_send_delay(self.cfg.dry_run_delay)
            .label_prefix("ingress_sdk")
            .log_level(LogLevel::Info)
            // At `Info` the SDK reports retries and label checks, which are worth seeing.
            .logger(|level, message| match level {
                LogLevel::Error => tracing::warn!(target: "doris_ingress::loader::sdk", "{message}"),
                LogLevel::Info => tracing::info!(target: "doris_ingress::loader::sdk", "{message}"),
                _ => tracing::debug!(target: "doris_ingress::loader::sdk", "{message}"),
            })
            .build()
            .map_err(|e| Error::Load(format!("stream load sdk config: {e}")))?;
        let client = Arc::new(AsyncClient::new(config).map_err(|e| Error::Load(format!("stream load sdk: {e}")))?);
        clients.insert(key, client.clone());
        Ok(client)
    }

    /// Waits until Doris reports `label` visible.
    async fn wait_visible(&self, table: &TableDef, label: &str) -> Result<()> {
        let deadline = Instant::now() + self.cfg.doris.timeout;
        loop {
            match self.poller.state(table, label).await? {
                LoadState::Visible => return Ok(()),
                LoadState::Failed(msg) => return Err(Error::Load(format!("label {label}: {msg}"))),
                LoadState::Running | LoadState::Unknown => {}
            }
            if Instant::now() >= deadline {
                return Err(Error::Load(format!("label {label} is not visible after {:?}", self.cfg.doris.timeout)));
            }
            tokio::time::sleep(table.config.poll_interval).await;
        }
    }
}

#[async_trait]
impl Loader for SdkLoader {
    fn kind(&self) -> &'static str {
        "stream_sdk"
    }

    async fn submit(&self, unit: &LoadUnit) -> Result<LoadState> {
        let client = self.client(unit).await?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(unit.parquet.clone())?.build()?;
        let mut handles = Vec::new();
        let (mut chunk, mut bytes) = (Vec::new(), 0);
        for batch in reader {
            for row in csv_rows(&batch?)? {
                bytes += row.len() + 1;
                chunk.push(row);
                if bytes >= CHUNK_BYTES {
                    let rows = std::mem::take(&mut chunk);
                    handles.push(client.send_batch(rows).await.map_err(|e| Error::Load(format!("stream load sdk: {e}")))?);
                    bytes = 0;
                }
            }
        }
        if !chunk.is_empty() {
            handles.push(client.send_batch(chunk).await.map_err(|e| Error::Load(format!("stream load sdk: {e}")))?);
        }

        // Every load must end cleanly. Wait for all of them, even after one has failed, so that
        // nothing of this attempt is still in flight when the unit is retried.
        let mut failure = None;
        let mut to_check = HashSet::new(); // labels committed but not yet reported visible
        for handle in handles {
            let result = handle.wait().await;
            let response = result.response.as_ref();
            if let Some(err) = &result.err {
                let detail = response.and_then(|r| r.message.clone()).unwrap_or_default();
                let url = response.and_then(|r| r.error_url.clone()).filter(|u| !u.is_empty()).map(|u| format!(" (see {u})")).unwrap_or_default();
                failure.get_or_insert(format!("{err}: {detail}{url} after {} attempts", result.attempts));
            } else if response.and_then(|r| r.status.as_deref()) != Some("Success") {
                match response.and_then(|r| r.label.clone()) {
                    Some(label) => {
                        to_check.insert(label);
                    }
                    None => {
                        failure.get_or_insert("a load succeeded without reporting its label".to_string());
                    }
                }
            }
        }
        if let Some(msg) = failure {
            return Ok(LoadState::Failed(msg));
        }
        for label in to_check {
            self.wait_visible(&unit.table, &label).await?;
        }

        let mut done = self.done.lock();
        if done.0.insert(unit.label.clone()) {
            done.1.push_back(unit.label.clone());
            if done.1.len() > REMEMBERED_UNITS
                && let Some(old) = done.1.pop_front()
            {
                done.0.remove(&old);
            }
        }
        Ok(LoadState::Visible)
    }

    /// Doris does not know unit labels (see the module comment): a unit is visible once this
    /// process has finished it, and unknown otherwise, which makes the flusher submit it.
    async fn state(&self, _table: &TableDef, label: &str) -> Result<LoadState> {
        Ok(if self.done.lock().0.contains(label) { LoadState::Visible } else { LoadState::Unknown })
    }
}

/// One CSV line per row, for a load with separator `,`, quote `"` and escape `\`.
/// Null is `\N`. Text is always quoted, so an empty string, a literal `\N`, separators and line
/// breaks inside text survive.
fn csv_rows(batch: &RecordBatch) -> Result<Vec<String>> {
    enum Column<'a> {
        Text(&'a arrow::array::StringArray),
        LargeText(&'a arrow::array::LargeStringArray),
        Other(ArrayFormatter<'a>),
    }
    let options = FormatOptions::default().with_null("\\N").with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.6f"));
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        columns.push(match field.data_type() {
            DataType::Utf8 => Column::Text(array.as_string::<i32>()),
            DataType::LargeUtf8 => Column::LargeText(array.as_string::<i64>()),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) | DataType::BinaryView | DataType::Utf8View => {
                return Err(Error::Schema(format!("column {}: type {} cannot be loaded as CSV", field.name(), field.data_type())));
            }
            _ => Column::Other(ArrayFormatter::try_new(array.as_ref(), &options)?),
        });
    }
    let mut rows = Vec::with_capacity(batch.num_rows());
    for r in 0..batch.num_rows() {
        let mut line = String::new();
        for (i, column) in columns.iter().enumerate() {
            if i > 0 {
                line.push(',');
            }
            match column {
                Column::Text(a) if a.is_valid(r) => quote(&mut line, a.value(r)),
                Column::LargeText(a) if a.is_valid(r) => quote(&mut line, a.value(r)),
                Column::Text(_) | Column::LargeText(_) => line.push_str("\\N"),
                Column::Other(f) => write!(line, "{}", f.value(r)).map_err(|e| Error::Load(format!("csv: {e}")))?,
            }
        }
        rows.push(line);
    }
    Ok(rows)
}

fn quote(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        if ch == '"' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use arrow::array::{BooleanArray, Float64Array, Int8Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn rows_are_quoted_and_nulls_are_marked() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("b", DataType::Boolean, true),
            Field::new("del", DataType::Int8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, -2, 3])),
                Arc::new(StringArray::from(vec![Some("a,\"b\"\\c\nd"), None, Some("")])),
                Arc::new(Float64Array::from(vec![Some(12.5), None, Some(-0.25)])),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1_790_000_000_123_456), None, Some(0)])),
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                Arc::new(Int8Array::from(vec![0, 1, 0])),
            ],
        )
        .unwrap();
        assert_eq!(
            csv_rows(&batch).unwrap(),
            vec![
                "1,\"a,\\\"b\\\"\\\\c\nd\",12.5,2026-09-21 14:13:20.123456,true,0",
                "-2,\\N,\\N,\\N,\\N,1",
                "3,\"\",-0.25,1970-01-01 00:00:00.000000,false,0",
            ]
        );
    }
}
