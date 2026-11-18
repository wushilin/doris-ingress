use std::path::PathBuf;
use std::time::Duration;

/// Node-wide settings.
#[derive(Clone, Debug)]
pub struct NodeConfig {
    /// Raft node id. Never reuse the id of a removed node for a node with an empty log.
    pub id: u64,
    /// Address peers use to reach this node's Raft endpoint, e.g. `10.0.0.5:7701`.
    pub raft_advertise: String,
    /// Address the Raft endpoint listens on, e.g. `0.0.0.0:7701`.
    pub raft_listen: String,
    /// WAL, Raft metadata and snapshots live here.
    pub data_dir: PathBuf,
    /// A WAL segment is closed, and a new one started, once it reaches this size (default 100 MiB).
    pub wal_segment_bytes: u64,
    pub raft: RaftTuning,
    pub batch: BatchConfig,
    pub memory: MemoryConfig,
    /// Loads running at once across all tables (each table still loads one unit at a time).
    pub max_concurrent_loads: usize,
}

impl NodeConfig {
    pub fn new(id: u64, raft_addr: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        let addr = raft_addr.into();
        Self {
            id,
            raft_advertise: addr.clone(),
            raft_listen: addr,
            data_dir: data_dir.into(),
            wal_segment_bytes: 100 << 20,
            raft: RaftTuning::default(),
            batch: BatchConfig::default(),
            memory: MemoryConfig::default(),
            max_concurrent_loads: 4,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RaftTuning {
    pub heartbeat_interval: Duration,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    /// Build a (metadata-only) snapshot after this many applied entries.
    pub snapshot_every_entries: u64,
    /// The leader keeps the log a follower still needs for as long as that follower keeps
    /// answering. A follower silent for this long loses its hold and gets a snapshot when it
    /// returns. Longer than a node takes to restart, so that a rolling restart needs no snapshot.
    pub catch_up_idle: Duration,
    /// Most WAL the leader keeps on disk only because followers are behind (not counting what
    /// buffered rows still need). Above it, the follower furthest behind loses its hold.
    pub catch_up_hold_bytes: u64,
    /// Most bytes per second one snapshot send to a follower may take (0: no limit).
    pub snapshot_send_bytes_per_sec: u64,
}

impl Default for RaftTuning {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(500),
            election_timeout_max: Duration::from_millis(1000),
            snapshot_every_entries: 2000,
            catch_up_idle: Duration::from_secs(30),
            catch_up_hold_bytes: 2 << 30,
            snapshot_send_bytes_per_sec: 200 << 20,
        }
    }
}

/// Group commit of client writes into Raft entries.
#[derive(Clone, Debug)]
pub struct BatchConfig {
    /// Close a batch once it holds this many bytes of encoded data.
    pub max_bytes: usize,
    /// While other entries are in flight, wait this long for more writers before proposing.
    /// Default 0: batching comes from the pipeline (writers that arrive while all
    /// `max_in_flight` slots are busy join the next entry), which measured better than lingering.
    pub linger: Duration,
    /// Raft proposals in flight at once.
    pub max_in_flight: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self { max_bytes: 4 << 20, linger: Duration::ZERO, max_in_flight: 8 }
    }
}

#[derive(Clone, Debug)]
pub struct MemoryConfig {
    /// Target memory for buffered data. Writers are stopped at this level.
    pub target_bytes: usize,
    /// Fraction of the target at which writers start being paced.
    pub soft_watermark: f64,
    /// Longest pacing delay applied to one write just below the target.
    pub max_pacing_delay: Duration,
    /// How long a write waits for space, at the node's target or at a table's cap, before it
    /// fails with the retryable `Backpressure` error.
    pub max_admission_wait: Duration,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            target_bytes: 8 << 30,
            soft_watermark: 0.7,
            max_pacing_delay: Duration::from_secs(10),
            max_admission_wait: Duration::from_secs(60),
        }
    }
}

/// Per-table settings.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableConfig {
    /// Start a load once this much is buffered (only checked when no load is in flight).
    pub flush_bytes: usize,
    /// Start a load once the oldest unflushed row is this old.
    pub flush_interval: Duration,
    /// Freeze the active memtable at this size even while a load is in flight.
    pub max_load_bytes: usize,
    /// How long a memtable is kept after its load is visible in Doris.
    pub grace: Duration,
    /// How much the table should buffer at most. It is a soft cap: writes are delayed more and
    /// more from 70% of it, and are held only at 120% of it.
    pub memory_cap_bytes: usize,
    /// Writes are rejected after this many consecutive flush failures.
    pub max_flush_failures: u32,
    /// Wait between failed flush attempts.
    pub retry_backoff: Duration,
    /// Wait between label-state polls while a load is running.
    pub poll_interval: Duration,
    /// Rows per Arrow batch when a flush unit or scan result is materialised.
    pub chunk_rows: usize,
    /// A load that has not become visible after this long is given up and submitted again, so a
    /// stalled connection cannot leave flushing dormant. Sending a unit twice is harmless.
    #[serde(default = "default_load_timeout")]
    pub load_timeout: Duration,
}

fn default_load_timeout() -> Duration {
    Duration::from_secs(120)
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            flush_bytes: 256 << 20,
            flush_interval: Duration::from_secs(2),
            max_load_bytes: 1 << 30,
            grace: Duration::from_secs(10),
            memory_cap_bytes: 4 << 30,
            max_flush_failures: 5,
            retry_backoff: Duration::from_secs(1),
            poll_interval: Duration::from_millis(200),
            chunk_rows: 1 << 20,
            load_timeout: default_load_timeout(),
        }
    }
}
