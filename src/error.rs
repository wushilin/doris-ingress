use std::time::Duration;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown table {0}")]
    UnknownTable(String),
    #[error("table {0} is already attached")]
    TableExists(String),
    #[error("schema error: {0}")]
    Schema(String),
    /// The table cannot accept writes right now. Not retried by doris-ingress; the
    /// client decides whether to retry later.
    #[error("table {table} rejects writes: {reason}")]
    TableStuck { table: String, reason: String },
    /// Memory stayed at the hard watermark for the whole admission wait. Retryable.
    #[error("the buffer is full: waited {waited:?} for space while Doris catches up; retry")]
    Backpressure { waited: Duration },
    #[error("load failed: {0}")]
    Load(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("sql error: {0}")]
    Sql(String),
    #[error("object storage error: {0}")]
    Storage(String),
    /// This node is not the Raft leader; retry against `addr` (the leader's Raft address).
    #[error("not the leader (leader: {leader:?} at {addr:?})")]
    NotLeader { leader: Option<u64>, addr: Option<String> },
    #[error("raft error: {0}")]
    Raft(String),
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
