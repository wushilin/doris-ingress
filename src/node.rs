//! A doris-ingress node: Raft + WAL + state machine, write batching, leader-driven flushing.
//!
//! Write path (leader): validate and Arrow-IPC-encode each op → admission (stuck tables, memory
//! governor) → batcher. The batcher group-commits waiting writers into one `Request::Write` Raft
//! entry (closed by size or linger), keeps several entries in flight, and wakes every writer of an
//! entry once it is committed and applied, with its assigned sequence range.
//!
//! Flush path (leader only, per table, one load at a time, global concurrency cap):
//! `Rotate` → `FlushStart{label}` → load → label state VISIBLE → `FlushDone` → grace → `Drop`.
//! Every step is a Raft entry, so a new leader resumes from the replicated state: a unit in
//! `Loading{label}` is resolved by asking Doris for that label first.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int8Array, RecordBatch};
use bytes::Bytes;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::error::{ClientWriteError, RaftError};
use parking_lot::Mutex;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::config::NodeConfig;
use crate::error::{Error, Result};
use crate::loader::{LoadState, LoadUnit, Loader, make_label};
use crate::memtable::{Memtable, Selected, emit, materialize, select_latest};
use crate::raft::log_store::{LogStore, RetentionFloor};
use crate::raft::network::{NetworkFactory, router};
use crate::raft::state_machine::{SmCore, StateMachine, TableSm, UnitState};
use crate::raft::{BasicNode, NodeId, Raft, Request, Response, WriteOpWire};
use crate::schema::TableDef;
use crate::wal::{Recovery, Wal, WalOptions};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteKind {
    /// Full-row upsert. The batch must contain every table column.
    Upsert,
    /// Delete by key. The batch must contain the key columns; other columns are ignored.
    Delete,
}

#[derive(Clone, Debug)]
pub struct WriteOp {
    pub table: String,
    pub kind: WriteKind,
    pub batch: RecordBatch,
}

/// Acknowledgement of a committed write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteAck {
    /// Read token: the write is visible to every read at a snapshot `>= seq`, on any replica that
    /// has applied it (see [`Node::wait_for_seq`]).
    pub seq: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ScanOptions {
    /// Include tombstones (rows with `__deleted = true`). The merged view in Doris needs them.
    pub include_deleted: bool,
    /// Read point; `None` reads at the latest applied write.
    pub snapshot: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct TableStats {
    pub buffered_bytes: usize,
    pub active_rows: usize,
    /// Frozen units not yet visible in Doris.
    pub unflushed_units: usize,
    /// Units visible in Doris and kept for the grace period.
    pub grace_units: usize,
    pub consecutive_failures: u32,
    pub last_error: Option<String>,
    /// Why writes are rejected, if they are (meaningful on the leader).
    pub rejecting: Option<String>,
}

#[derive(Default, Clone)]
struct FlushHealth {
    failures: u32,
    last_error: Option<String>,
}

struct Pending {
    ops: Vec<WriteOpWire>,
    bytes: usize,
    queued_at: Instant,
    tx: oneshot::Sender<Result<Vec<(u64, u64)>>>,
}

pub struct Node {
    inner: Arc<Inner>,
}

struct Inner {
    id: NodeId,
    cfg: NodeConfig,
    raft: Raft,
    sm: Arc<SmCore>,
    loaders: HashMap<String, Arc<dyn Loader>>,
    batch_tx: mpsc::Sender<Pending>,
    health: Mutex<HashMap<String, FlushHealth>>,
    /// Leader-local: when this node first saw each unit visible (grace timer start).
    visible_seen: Mutex<HashMap<(String, u64), Instant>>,
    load_slots: Arc<Semaphore>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    recovery: Recovery,
    /// Client for forwarding writes and read-index requests to the leader.
    http: reqwest::Client,
    /// Leadership as last published by Raft metrics, cached so hot paths do not contend with the
    /// Raft core on the metrics channel.
    is_leader: std::sync::atomic::AtomicBool,
    /// Write-path timing counters (diagnostics).
    pub(crate) stats: WriteStats,
    leader_info: Mutex<(Option<NodeId>, Option<String>)>,
    /// Writes a follower forwards to the leader; batched into one internal request.
    forward_tx: mpsc::Sender<(Vec<WriteOpWire>, oneshot::Sender<Result<WriteAck>>)>,
    /// Follower reads waiting for a read index; one leader request serves all queued waiters.
    read_index_tx: mpsc::Sender<oneshot::Sender<std::result::Result<u64, String>>>,
}

const TICK: Duration = Duration::from_millis(20);
const MAX_UNKNOWN_POLLS: u32 = 10;

impl Node {
    /// Opens the WAL and state machine, starts Raft and its HTTP endpoint, and the background
    /// batcher and flusher. Call [`Node::initialize`] once on a brand-new cluster.
    pub async fn start(cfg: NodeConfig, loaders: HashMap<String, Arc<dyn Loader>>) -> Result<Self> {
        let (wal, recovery) = Wal::open(WalOptions { dir: cfg.data_dir.join("wal"), segment_bytes: cfg.wal_segment_bytes })?;
        if recovery.truncated_tail_bytes > 0 {
            tracing::warn!(bytes = recovery.truncated_tail_bytes, "WAL torn tail truncated on open");
        }
        let wal = Arc::new(wal);
        let floor = RetentionFloor::default();
        let log = LogStore::open(wal.clone(), floor.clone())?;
        let sm = SmCore::open(wal.clone(), cfg.data_dir.join("snapshots"), floor, &cfg.memory)?;

        let raft_cfg = openraft::Config {
            cluster_name: "doris-ingress".into(),
            heartbeat_interval: cfg.raft.heartbeat_interval.as_millis() as u64,
            election_timeout_min: cfg.raft.election_timeout_min.as_millis() as u64,
            election_timeout_max: cfg.raft.election_timeout_max.as_millis() as u64,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(cfg.raft.snapshot_every_entries),
            max_in_snapshot_log_to_keep: 0,
            // Entries can be several MB (group-committed Arrow batches): bound each AppendEntries.
            max_payload_entries: 16,
            ..Default::default()
        }
        .validate()
        .map_err(|e| Error::Raft(e.to_string()))?;
        let raft = Raft::new(cfg.id, Arc::new(raft_cfg), NetworkFactory::default(), log, StateMachine { core: sm.clone() })
            .await
            .map_err(|e| Error::Raft(e.to_string()))?;

        let listener = tokio::net::TcpListener::bind(&cfg.raft_listen).await?;
        let raft_routes = router(cfg.id, raft.clone());

        let (batch_tx, batch_rx) = mpsc::channel(4096);
        let (read_index_tx, read_index_rx) = mpsc::channel(65_536);
        let (forward_tx, forward_rx) = mpsc::channel(65_536);
        let inner = Arc::new(Inner {
            id: cfg.id,
            load_slots: Arc::new(Semaphore::new(cfg.max_concurrent_loads.max(1))),
            cfg,
            raft,
            sm,
            loaders,
            batch_tx,
            health: Mutex::default(),
            visible_seen: Mutex::default(),
            tasks: Mutex::default(),
            recovery,
            read_index_tx,
            forward_tx,
            is_leader: std::sync::atomic::AtomicBool::new(false),
            stats: WriteStats::default(),
            leader_info: Mutex::new((None, None)),
            http: crate::raft::network::internal_client(16),
        });
        let app = raft_routes.merge(internal_router(inner.clone()));
        let server = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(error = %e, "raft http server stopped");
            }
        });
        let batcher = tokio::spawn(run_batcher(inner.clone(), batch_rx));
        let read_indexer = tokio::spawn(run_read_index_batcher(inner.clone(), read_index_rx));
        let forwarder = tokio::spawn(run_forward_batcher(inner.clone(), forward_rx));
        let watcher = tokio::spawn(run_leader_watcher(Arc::downgrade(&inner)));
        let flusher = tokio::spawn(run_flush_supervisor(Arc::downgrade(&inner)));
        inner.tasks.lock().extend([server, batcher, flusher, read_indexer, forwarder, watcher]);
        Ok(Self { inner })
    }

    pub fn id(&self) -> NodeId {
        self.inner.id
    }

    /// What WAL recovery found when this node started.
    pub fn wal_recovery(&self) -> &Recovery {
        &self.inner.recovery
    }

    /// Initializes a new cluster with the given voters `(id, raft address)`. Call once, on one node.
    pub async fn initialize(&self, members: Vec<(NodeId, String)>) -> Result<()> {
        let m: std::collections::BTreeMap<NodeId, BasicNode> =
            members.into_iter().map(|(id, addr)| (id, BasicNode { addr })).collect();
        self.inner.raft.initialize(m).await.map_err(|e| Error::Raft(e.to_string()))
    }

    /// Adds a node as a learner and waits until it has caught up.
    pub async fn add_learner(&self, id: NodeId, addr: String) -> Result<()> {
        self.inner.raft.add_learner(id, BasicNode { addr }, true).await.map(|_| ()).map_err(map_write_err)
    }

    /// Sets the voters.
    pub async fn change_membership(&self, voters: Vec<NodeId>) -> Result<()> {
        let set: std::collections::BTreeSet<NodeId> = voters.into_iter().collect();
        self.inner.raft.change_membership(set, false).await.map(|_| ()).map_err(map_write_err)
    }

    pub fn current_leader(&self) -> Option<NodeId> {
        self.inner.raft.metrics().borrow_watched().current_leader
    }

    pub fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    /// Cumulative leader write-path timings: (submits, submit ns, queue ns, proposals, propose ns).
    pub fn write_stats(&self) -> (u64, u64, u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let s = &self.inner.stats;
        (s.submits.load(Relaxed), s.submit_ns.load(Relaxed), s.queue_ns.load(Relaxed), s.proposals.load(Relaxed), s.propose_ns.load(Relaxed))
    }

    /// Cumulative apply statistics: (nanoseconds spent applying, entries, write ops).
    pub fn apply_stats(&self) -> (u64, u64, u64) {
        use std::sync::atomic::Ordering::Relaxed;
        let sm = &self.inner.sm;
        (sm.apply_nanos.load(Relaxed), sm.apply_entries.load(Relaxed), sm.apply_ops.load(Relaxed))
    }

    /// Index of the last entry in this node's Raft log.
    pub fn raft_last_log_index(&self) -> u64 {
        self.inner.raft.metrics().borrow_watched().last_log_index.unwrap_or(0)
    }

    /// Waits until some node is leader, returning its id.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<NodeId> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(l) = self.current_leader() {
                return Ok(l);
            }
            if Instant::now() > deadline {
                return Err(Error::Raft("no leader elected".into()));
            }
            tokio::time::sleep(TICK).await;
        }
    }

    /// Attaches a Doris table (leader only). `min_next_seq` must be above the largest sequence
    /// value in the Doris table, tombstones included ([`crate::doris::max_seq`] + 1).
    pub async fn attach(&self, def: TableDef, loader: &str, min_next_seq: u64) -> Result<()> {
        if !self.inner.loaders.contains_key(loader) {
            return Err(Error::Load(format!("no loader named {loader} on this node")));
        }
        crate::schema::TableCodec::new(&def)?;
        let req = Request::AttachTable { def: Box::new(def.to_wire()?), loader: loader.into(), min_next_seq };
        match self.inner.propose(req).await? {
            Response::Rejected(r) if r.contains("already attached") => Err(Error::TableExists(def.full_name())),
            Response::Rejected(r) => Err(Error::Schema(r)),
            _ => Ok(()),
        }
    }

    pub async fn upsert(&self, table: &str, batch: RecordBatch) -> Result<WriteAck> {
        self.write(vec![WriteOp { table: table.into(), kind: WriteKind::Upsert, batch }]).await
    }

    pub async fn delete(&self, table: &str, keys: RecordBatch) -> Result<WriteAck> {
        self.write(vec![WriteOp { table: table.into(), kind: WriteKind::Delete, batch: keys }]).await
    }

    /// Commits all operations atomically (one Raft entry): a read sees all of them or none.
    /// Returns once the write is durable on a majority and applied on this node.
    pub async fn write(&self, ops: Vec<WriteOp>) -> Result<WriteAck> {
        let inner = &self.inner;
        let mut wire = Vec::with_capacity(ops.len());
        for op in ops {
            let table = inner.table(&op.table)?;
            let batch = match op.kind {
                WriteKind::Upsert => table.codec.conform_upsert(&op.batch)?,
                WriteKind::Delete => table.codec.conform_delete(&op.batch)?,
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let ipc = crate::ipc::encode_batch_message(&batch)?;
            wire.push(WriteOpWire { table: op.table, delete: op.kind == WriteKind::Delete, ipc, format: crate::ipc::FORMAT_BATCH });
        }
        if wire.is_empty() {
            return Ok(WriteAck { seq: inner.sm.snapshot_seq() });
        }
        if inner.is_leader() {
            return inner.submit(wire).await;
        }
        inner.forward_write(wire).await
    }

    /// The latest applied read point on this node.
    pub fn snapshot(&self) -> u64 {
        self.inner.sm.snapshot_seq()
    }

    /// Waits until this node has applied the write that returned `seq` (read-your-writes on a
    /// follower).
    pub async fn wait_for_seq(&self, seq: u64, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.inner.sm.applied.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.snapshot() >= seq {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(Error::Raft(format!("seq {seq} not applied within {timeout:?}")));
            }
        }
    }

    /// Linearizable read barrier: confirms leadership with a quorum and waits until everything
    /// committed before the call is applied here. Reads after it see all acknowledged writes.
    pub async fn read_barrier(&self) -> Result<()> {
        let inner = &self.inner;
        if inner.is_leader() {
            return inner.leader_read_index().await.map(|_| ());
        }
        // Follower: get the leader's read index (requests are coalesced), then wait until this
        // node has applied it.
        let (tx, rx) = oneshot::channel();
        inner.read_index_tx.send(tx).await.map_err(|_| Error::Raft("node is shutting down".into()))?;
        let index = rx.await.map_err(|_| Error::Raft("read index dropped".into()))?.map_err(Error::Raft)?;
        let deadline = tokio::time::Instant::now() + FORWARD_TIMEOUT;
        loop {
            let notified = inner.sm.applied.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if inner.sm.last_applied().is_some_and(|l| l.index >= index) {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(Error::Raft(format!("log index {index} not applied within {FORWARD_TIMEOUT:?}")));
            }
        }
    }

    /// Looks up keys. One row per buffered key, tombstones included, in the scan schema (table
    /// columns, `__seq`, `__deleted`). Keys not in the buffer are absent.
    pub fn get(&self, table: &str, keys: &RecordBatch, snapshot: Option<u64>) -> Result<RecordBatch> {
        let t = self.inner.table(table)?;
        let snap = snapshot.unwrap_or_else(|| self.snapshot());
        let encoded = t.codec.encode_key_batch(keys)?;
        let (batches, mut selected) = {
            let u = t.units.lock();
            let layers = layers(&u);
            // Gather only from the batches that hold a hit, not every batch of the table.
            let mut batches: Vec<RecordBatch> = Vec::new();
            let mut slot: HashMap<(usize, u32), usize> = HashMap::new();
            let mut selected = Vec::new();
            let mut seen = HashSet::new();
            for key in encoded.iter() {
                if !seen.insert(key.as_ref().to_vec()) {
                    continue;
                }
                for (li, layer) in layers.iter().enumerate() {
                    if let Some(v) = layer.lookup(key.as_ref(), snap) {
                        let b = *slot.entry((li, v.batch)).or_insert_with(|| {
                            batches.push(layer.batches()[v.batch as usize].clone());
                            batches.len() - 1
                        });
                        selected.push(Selected { batch: b, row: v.row as usize, seq: v.seq, deleted: v.deleted });
                        break;
                    }
                }
            }
            (batches, selected)
        };
        selected.sort_by_key(|s| s.seq);
        let mut out = to_scan_batches(&t, &batches, &selected, usize::MAX)?;
        Ok(out.pop().unwrap_or_else(|| RecordBatch::new_empty(t.codec.scan_schema.clone())))
    }

    /// Latest version of every buffered key at the read point.
    ///
    /// Limitation: key selection runs under the table lock, briefly blocking apply for that table.
    pub fn scan(&self, table: &str, opts: ScanOptions) -> Result<Vec<RecordBatch>> {
        let t = self.inner.table(table)?;
        let snap = opts.snapshot.unwrap_or_else(|| self.snapshot());
        let selections = {
            let u = t.units.lock();
            select_latest(&layers(&u), snap, opts.include_deleted)
        };
        selections
            .iter()
            .map(|sel| {
                let c = emit(sel)?;
                let mut cols = c.columns;
                cols.push(Arc::new(c.seq) as ArrayRef);
                cols.push(Arc::new(c.deleted) as ArrayRef);
                Ok(RecordBatch::try_new(t.codec.scan_schema.clone(), cols)?)
            })
            .collect()
    }

    /// Schema of scan results for a table.
    pub fn scan_schema(&self, table: &str) -> Result<arrow::datatypes::SchemaRef> {
        Ok(self.inner.table(table)?.codec.scan_schema.clone())
    }

    /// Attached tables (`db.table`).
    pub fn tables(&self) -> Vec<Arc<TableDef>> {
        self.inner.sm.tables().iter().map(|t| t.def.clone()).collect()
    }

    /// Freezes everything buffered for the table and waits until it is visible in Doris
    /// (leader only).
    pub async fn flush(&self, table: &str, timeout: Duration) -> Result<()> {
        let t = self.inner.table(table)?;
        self.inner.propose(Request::Rotate { table: table.into() }).await?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.inner.sm.applied.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if t.units.lock().frozen.iter().all(|u| u.state == UnitState::Visible) {
                return Ok(());
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                let h = self.inner.health.lock().get(table).cloned().unwrap_or_default();
                return Err(Error::Load(format!(
                    "flush of {table} not visible after {timeout:?}: {}",
                    h.last_error.as_deref().unwrap_or("still loading")
                )));
            }
        }
    }

    pub fn stats(&self, table: &str) -> Result<TableStats> {
        let t = self.inner.table(table)?;
        let h = self.inner.health.lock().get(table).cloned().unwrap_or_default();
        let rejecting = self.inner.rejecting(&t);
        let u = t.units.lock();
        Ok(TableStats {
            buffered_bytes: u.bytes,
            active_rows: u.active.rows(),
            unflushed_units: u.frozen.iter().filter(|x| x.state != UnitState::Visible).count(),
            grace_units: u.frozen.iter().filter(|x| x.state == UnitState::Visible).count(),
            consecutive_failures: h.failures,
            last_error: h.last_error,
            rejecting,
        })
    }

    /// Bytes buffered across all tables on this node.
    pub fn memory_used(&self) -> usize {
        self.inner.sm.governor.used()
    }

    /// Stops Raft, the HTTP endpoint and background tasks.
    pub async fn shutdown(&self) {
        let _ = self.inner.raft.shutdown().await;
        for t in self.inner.tasks.lock().drain(..) {
            t.abort();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        for t in self.inner.tasks.lock().drain(..) {
            t.abort();
        }
    }
}

fn map_write_err(e: RaftError<crate::raft::TypeConfig, ClientWriteError<crate::raft::TypeConfig>>) -> Error {
    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(f)) => Error::NotLeader {
            leader: f.leader_id,
            addr: f.leader_node.map(|n| n.addr),
        },
        other => Error::Raft(other.to_string()),
    }
}

/// Cumulative write-path timings on this node (all in nanoseconds).
#[derive(Default)]
pub(crate) struct WriteStats {
    /// Leader `submit` calls and their total duration (admission to acknowledgement).
    submit_ns: std::sync::atomic::AtomicU64,
    submits: std::sync::atomic::AtomicU64,
    /// Time writers spent queued before their Raft entry was proposed.
    queue_ns: std::sync::atomic::AtomicU64,
    /// Raft proposals (entries) and their commit+apply duration.
    propose_ns: std::sync::atomic::AtomicU64,
    proposals: std::sync::atomic::AtomicU64,
}

/// How long a follower keeps retrying to reach a leader for a forwarded request.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);

/// Errors carried back over the internal forwarding endpoints.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum WireError {
    NotLeader,
    Backpressure { waited_ms: u64 },
    TableStuck { table: String, reason: String },
    UnknownTable(String),
    Schema(String),
    Other(String),
}

impl From<Error> for WireError {
    fn from(e: Error) -> Self {
        match e {
            Error::NotLeader { .. } => WireError::NotLeader,
            Error::Backpressure { waited } => WireError::Backpressure { waited_ms: waited.as_millis() as u64 },
            Error::TableStuck { table, reason } => WireError::TableStuck { table, reason },
            Error::UnknownTable(t) => WireError::UnknownTable(t),
            Error::Schema(s) => WireError::Schema(s),
            other => WireError::Other(other.to_string()),
        }
    }
}

impl WireError {
    fn into_error(self, leader: Option<NodeId>, addr: Option<String>) -> Error {
        match self {
            WireError::NotLeader => Error::NotLeader { leader, addr },
            WireError::Backpressure { waited_ms } => Error::Backpressure { waited: Duration::from_millis(waited_ms) },
            WireError::TableStuck { table, reason } => Error::TableStuck { table, reason },
            WireError::UnknownTable(t) => Error::UnknownTable(t),
            WireError::Schema(s) => Error::Schema(s),
            WireError::Other(s) => Error::Raft(s),
        }
    }
}

/// Internal endpoints on the Raft port: followers forward writes and read-index requests here.
fn internal_router(inner: Arc<Inner>) -> axum::Router {
    use axum::body::Bytes as Body;
    use axum::extract::{Path, State};
    use axum::routing::post;

    async fn write(State(inner): State<Arc<Inner>>, Path(target): Path<NodeId>, body: Body) -> Vec<u8> {
        let result: std::result::Result<u64, WireError> = if target != inner.id {
            Err(WireError::Other(format!("this is node {}, not {target}", inner.id)))
        } else {
            match crate::raft::decode::<Vec<WriteOpWire>>(&body) {
                Ok(ops) if inner.is_leader() => inner.submit(ops).await.map(|a| a.seq).map_err(WireError::from),
                Ok(_) => Err(WireError::NotLeader),
                Err(e) => Err(WireError::Other(e.to_string())),
            }
        };
        crate::raft::encode(&result).map(|b| b.to_vec()).unwrap_or_default()
    }

    async fn write_batch(State(inner): State<Arc<Inner>>, Path(target): Path<NodeId>, body: Body) -> Vec<u8> {
        type Reply = std::result::Result<Vec<std::result::Result<u64, WireError>>, WireError>;
        let result: Reply = if target != inner.id {
            Err(WireError::Other(format!("this is node {}, not {target}", inner.id)))
        } else if !inner.is_leader() {
            Err(WireError::NotLeader)
        } else {
            match crate::raft::decode::<Vec<Vec<WriteOpWire>>>(&body) {
                // Each forwarded client write is submitted separately: same group commit and the
                // same per-writer success or rejection as a write made on the leader directly.
                Ok(groups) => Ok(futures_util::future::join_all(groups.into_iter().map(|g| inner.submit(g)))
                    .await
                    .into_iter()
                    .map(|r| r.map(|a| a.seq).map_err(WireError::from))
                    .collect()),
                Err(e) => Err(WireError::Other(e.to_string())),
            }
        };
        crate::raft::encode(&result).map(|b| b.to_vec()).unwrap_or_default()
    }

    async fn read_index(State(inner): State<Arc<Inner>>, Path(target): Path<NodeId>) -> Vec<u8> {
        let result: std::result::Result<u64, WireError> = if target != inner.id {
            Err(WireError::Other(format!("this is node {}, not {target}", inner.id)))
        } else if !inner.is_leader() {
            Err(WireError::NotLeader)
        } else {
            inner.leader_read_index().await.map_err(WireError::from)
        };
        crate::raft::encode(&result).map(|b| b.to_vec()).unwrap_or_default()
    }

    axum::Router::new()
        .route("/internal/{target}/write", post(write))
        .route("/internal/{target}/read_index", post(read_index))
        .route("/internal/{target}/write_batch", post(write_batch))
        .layer(axum::extract::DefaultBodyLimit::max(crate::raft::network::MAX_RPC_BODY))
        .with_state(inner)
}

impl Inner {
    /// Leader write path: stuck-table check, memory admission, then group commit.
    async fn submit(&self, wire: Vec<WriteOpWire>) -> Result<WriteAck> {
        let bytes = wire.iter().map(|o| o.ipc.len()).sum();
        let touched: HashSet<&str> = wire.iter().map(|o| o.table.as_str()).collect();
        for name in touched {
            if let Some(reason) = self.rejecting(&*self.table(name)?) {
                return Err(Error::TableStuck { table: name.to_string(), reason });
            }
        }
        self.sm.governor.admit().await?;
        let t0 = Instant::now();
        let r = self.submit_inner(wire, bytes).await;
        self.stats.submit_ns.fetch_add(t0.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
        self.stats.submits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        r
    }

    async fn submit_inner(&self, wire: Vec<WriteOpWire>, bytes: usize) -> Result<WriteAck> {
        let (tx, rx) = oneshot::channel();
        self.batch_tx.send(Pending { ops: wire, bytes, queued_at: Instant::now(), tx }).await.map_err(|_| Error::Raft("node is shutting down".into()))?;
        let seqs = rx.await.map_err(|_| Error::Raft("write dropped".into()))??;
        Ok(WriteAck { seq: seqs.iter().map(|s| s.1).max().unwrap_or(0) })
    }

    async fn leader_read_index(&self) -> Result<u64> {
        self.raft
            .ensure_linearizable(openraft::raft::ReadPolicy::ReadIndex)
            .await
            .map(|r| r.index())
            .map_err(|e| Error::Raft(e.to_string()))
    }

    async fn forward_write(&self, wire: Vec<WriteOpWire>) -> Result<WriteAck> {
        let (tx, rx) = oneshot::channel();
        self.forward_tx.send((wire, tx)).await.map_err(|_| Error::Raft("node is shutting down".into()))?;
        rx.await.map_err(|_| Error::Raft("forwarded write dropped".into()))?
    }

    /// Sends several client writes to the leader in one request; each gets its own result.
    async fn forward_batch(&self, groups: Vec<Vec<WriteOpWire>>) -> Vec<Result<WriteAck>> {
        let n = groups.len();
        let body = match crate::raft::encode(&groups) {
            Ok(b) => b,
            Err(e) => return (0..n).map(|_| Err(Error::Raft(e.to_string()))).collect(),
        };
        let deadline = tokio::time::Instant::now() + FORWARD_TIMEOUT;
        loop {
            if self.is_leader() {
                // Became leader: commit locally.
                return futures_util::future::join_all(groups.into_iter().map(|g| self.submit(g))).await;
            }
            let (leader, addr) = self.leader();
            let attempt: Result<Vec<std::result::Result<u64, WireError>>> = async {
                let (Some(leader), Some(addr)) = (leader, addr.clone()) else {
                    return Err(Error::NotLeader { leader: None, addr: None });
                };
                let resp = self
                    .http
                    .post(format!("http://{addr}/internal/{leader}/write_batch"))
                    .timeout(FORWARD_TIMEOUT)
                    .body(body.clone())
                    .send()
                    .await
                    .map_err(|e| Error::NotLeader { leader: Some(leader), addr: Some(format!("{addr} unreachable: {e}")) })?;
                let bytes = resp.bytes().await.map_err(|e| Error::Http(e.to_string()))?;
                let outer: std::result::Result<Vec<std::result::Result<u64, WireError>>, WireError> =
                    crate::raft::decode(&bytes).map_err(|e| Error::Http(format!("bad reply from leader: {e}")))?;
                outer.map_err(|e| e.into_error(Some(leader), Some(addr.clone())))
            }
            .await;
            match attempt {
                Ok(results) if results.len() == n => {
                    return results
                        .into_iter()
                        .map(|r| r.map(|seq| WriteAck { seq }).map_err(|e| e.into_error(leader, addr.clone())))
                        .collect();
                }
                Ok(results) => {
                    let msg = format!("leader answered {} results for {n} writes", results.len());
                    return (0..n).map(|_| Err(Error::Raft(msg.clone()))).collect();
                }
                Err(Error::NotLeader { .. }) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    let msg = e.to_string();
                    return (0..n)
                        .map(|_| Err(match &e {
                            Error::NotLeader { leader, addr } => Error::NotLeader { leader: *leader, addr: addr.clone() },
                            _ => Error::Raft(msg.clone()),
                        }))
                        .collect();
                }
            }
        }
    }

    /// Calls an internal endpoint on the current leader, following leader changes until
    /// [`FORWARD_TIMEOUT`].
    async fn call_leader<Req: serde::Serialize>(&self, path: &str, req: &Req) -> Result<u64> {
        let body = crate::raft::encode(req)?;
        let deadline = tokio::time::Instant::now() + FORWARD_TIMEOUT;
        loop {
            let (leader, addr) = self.leader();
            let attempt = async {
                let (Some(leader), Some(addr)) = (leader, addr.clone()) else {
                    return Err(Error::NotLeader { leader: None, addr: None });
                };
                if leader == self.id {
                    // Became leader meanwhile; the caller's own path handles that on retry.
                    return Err(Error::NotLeader { leader: Some(leader), addr: Some(addr) });
                }
                let resp = self
                    .http
                    .post(format!("http://{addr}/internal/{leader}/{path}"))
                    .timeout(FORWARD_TIMEOUT)
                    .body(body.clone())
                    .send()
                    .await
                    .map_err(|e| Error::NotLeader { leader: Some(leader), addr: Some(format!("{addr} unreachable: {e}")) })?;
                let bytes = resp.bytes().await.map_err(|e| Error::Http(e.to_string()))?;
                let result: std::result::Result<u64, WireError> =
                    crate::raft::decode(&bytes).map_err(|e| Error::Http(format!("bad reply from leader: {e}")))?;
                result.map_err(|e| e.into_error(Some(leader), Some(addr.clone())))
            };
            match attempt.await {
                Err(Error::NotLeader { .. }) if tokio::time::Instant::now() < deadline => {
                    if self.is_leader() && path == "read_index" {
                        return self.leader_read_index().await;
                    }
                    if self.is_leader() && path == "write" {
                        let ops: Vec<WriteOpWire> = crate::raft::decode(&body)?;
                        return self.submit(ops).await.map(|a| a.seq);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                other => return other,
            }
        }
    }

    /// Current leader id and Raft address, as this node knows them.
    fn leader(&self) -> (Option<NodeId>, Option<String>) {
        self.leader_info.lock().clone()
    }

    fn is_leader(&self) -> bool {
        self.is_leader.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Refreshes the cached leadership from the current Raft metrics.
    fn refresh_leader(&self) {
        let (leader, addr, me) = {
            let rx = self.raft.metrics();
            let m = rx.borrow_watched();
            let addr = m.current_leader.and_then(|l| m.membership_config.membership().get_node(&l).map(|n| n.addr.clone()));
            (m.current_leader, addr, m.state == openraft::ServerState::Leader && m.current_leader == Some(self.id))
        };
        *self.leader_info.lock() = (leader, addr);
        self.is_leader.store(me, std::sync::atomic::Ordering::Release);
    }


    fn table(&self, name: &str) -> Result<Arc<TableSm>> {
        self.sm.table(name).ok_or_else(|| Error::UnknownTable(name.into()))
    }

    async fn propose(&self, req: Request) -> Result<Response> {
        self.raft.client_write(req).await.map(|r| r.data).map_err(map_write_err)
    }

    fn rejecting(&self, t: &TableSm) -> Option<String> {
        let cfg = &t.def.config;
        if let Some(h) = self.health.lock().get(&t.def.full_name())
            && h.failures >= cfg.max_flush_failures
        {
            return Some(format!("{} consecutive flush failures, last: {}", h.failures, h.last_error.as_deref().unwrap_or("?")));
        }
        let bytes = t.units.lock().bytes;
        if bytes >= cfg.memory_cap_bytes {
            return Some(format!("buffered {bytes} bytes, table cap is {}", cfg.memory_cap_bytes));
        }
        None
    }

    fn record_failure(&self, table: &str, msg: String) {
        let mut h = self.health.lock();
        let e = h.entry(table.to_string()).or_default();
        e.failures += 1;
        e.last_error = Some(msg);
    }

    fn record_success(&self, table: &str) {
        self.health.lock().remove(table);
    }
}

/// Memtables newest first: active, then frozen from newest to oldest.
fn layers(u: &crate::raft::state_machine::Units) -> Vec<&Memtable> {
    let mut v: Vec<&Memtable> = vec![&u.active];
    v.extend(u.frozen.iter().rev().map(|f| f.mem.as_ref()));
    v
}

fn to_scan_batches(t: &TableSm, batches: &[RecordBatch], selected: &[Selected], chunk_rows: usize) -> Result<Vec<RecordBatch>> {
    materialize(batches, selected, chunk_rows)?
        .into_iter()
        .map(|c| {
            let mut cols = c.columns;
            cols.push(Arc::new(c.seq) as ArrayRef);
            cols.push(Arc::new(c.deleted) as ArrayRef);
            Ok(RecordBatch::try_new(t.codec.scan_schema.clone(), cols)?)
        })
        .collect()
}

/// Keeps the cached leadership in step with Raft metrics: one reader of the metrics channel
/// instead of one per request.
async fn run_leader_watcher(inner: std::sync::Weak<Inner>) {
    let Some(strong) = inner.upgrade() else { return };
    let mut rx = strong.raft.metrics();
    drop(strong);
    loop {
        let Some(node) = inner.upgrade() else { return };
        node.refresh_leader();
        drop(node);
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Batches writes a follower forwards to the leader: one internal request carries every write
/// queued while a pipeline slot was taken (up to 8 requests in flight).
async fn run_forward_batcher(inner: Arc<Inner>, mut rx: mpsc::Receiver<(Vec<WriteOpWire>, oneshot::Sender<Result<WriteAck>>)>) {
    let slots = Arc::new(Semaphore::new(inner.cfg.batch.max_in_flight.max(1)));
    let max_bytes = inner.cfg.batch.max_bytes;
    while let Some(first) = rx.recv().await {
        let Ok(permit) = slots.clone().acquire_owned().await else { return };
        let mut bytes: usize = first.0.iter().map(|o| o.ipc.len()).sum();
        let mut items = vec![first];
        while bytes < max_bytes {
            match rx.try_recv() {
                Ok(item) => {
                    bytes += item.0.iter().map(|o| o.ipc.len()).sum::<usize>();
                    items.push(item);
                }
                Err(_) => break,
            }
        }
        let inner = inner.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let (groups, waiters): (Vec<_>, Vec<_>) = items.into_iter().unzip();
            let results = inner.forward_batch(groups).await;
            for (tx, r) in waiters.into_iter().zip(results) {
                let _ = tx.send(r);
            }
        });
    }
}

/// Coalesces follower read-index requests: one request to the leader answers every read that was
/// queued before it was sent (each of those reads started before the leader confirmed the index,
/// so the shared index is valid for all of them).
async fn run_read_index_batcher(inner: Arc<Inner>, mut rx: mpsc::Receiver<oneshot::Sender<std::result::Result<u64, String>>>) {
    while let Some(first) = rx.recv().await {
        let mut waiters = vec![first];
        while let Ok(w) = rx.try_recv() {
            waiters.push(w);
        }
        let result = if inner.is_leader() {
            inner.leader_read_index().await
        } else {
            inner.call_leader("read_index", &()).await
        }
        .map_err(|e| e.to_string());
        for w in waiters {
            let _ = w.send(result.clone());
        }
    }
}

/// Group commit: collects writers into one Raft entry per batch, several entries in flight.
async fn run_batcher(inner: Arc<Inner>, mut rx: mpsc::Receiver<Pending>) {
    let cfg = inner.cfg.batch.clone();
    let in_flight = Arc::new(Semaphore::new(cfg.max_in_flight.max(1)));
    let max_in_flight = cfg.max_in_flight.max(1);
    while let Some(first) = rx.recv().await {
        // Wait for a pipeline slot first: writes that arrive meanwhile join this batch.
        let Ok(permit) = in_flight.clone().acquire_owned().await else { return };
        let mut batch = vec![first];
        let mut bytes = batch[0].bytes;
        while bytes < cfg.max_bytes {
            match rx.try_recv() {
                Ok(p) => {
                    bytes += p.bytes;
                    batch.push(p);
                }
                Err(_) => break,
            }
        }
        // Linger for more writers only while other entries are in flight; an idle pipeline
        // sends at once, so a lone writer does not pay the linger.
        let busy = in_flight.available_permits() + 1 < max_in_flight;
        if busy && !cfg.linger.is_zero() {
            let deadline = tokio::time::Instant::now() + cfg.linger;
            while bytes < cfg.max_bytes {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(p)) => {
                        bytes += p.bytes;
                        batch.push(p);
                    }
                    Ok(None) | Err(_) => break,
                }
            }
        }
        let inner = inner.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let counts: Vec<usize> = batch.iter().map(|p| p.ops.len()).collect();
            let mut ops = Vec::with_capacity(counts.iter().sum());
            let mut waiters = Vec::with_capacity(batch.len());
            let now = Instant::now();
            let queued_ns: u64 = batch.iter().map(|p| (now - p.queued_at).as_nanos() as u64).sum();
            for p in batch {
                ops.extend(p.ops);
                waiters.push(p.tx);
            }
            let groups: Vec<u32> = counts.iter().map(|&n| n as u32).collect();
            use std::sync::atomic::Ordering::Relaxed;
            inner.stats.queue_ns.fetch_add(queued_ns, Relaxed);
            let tp = Instant::now();
            let proposed = inner.propose(Request::Write { ops, groups }).await;
            inner.stats.propose_ns.fetch_add(tp.elapsed().as_nanos() as u64, Relaxed);
            inner.stats.proposals.fetch_add(1, Relaxed);
            match proposed {
                Ok(Response::Write { groups }) if groups.len() == waiters.len() => {
                    for (tx, result) in waiters.into_iter().zip(groups) {
                        let _ = tx.send(result.map_err(Error::Schema));
                    }
                }
                Ok(Response::Rejected(reason)) => {
                    for tx in waiters {
                        let _ = tx.send(Err(Error::Schema(reason.clone())));
                    }
                }
                Ok(other) => {
                    for tx in waiters {
                        let _ = tx.send(Err(Error::Raft(format!("unexpected response {other:?}"))));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    for tx in waiters {
                        let _ = tx.send(Err(match &e {
                            Error::NotLeader { leader, addr } => Error::NotLeader { leader: *leader, addr: addr.clone() },
                            _ => Error::Raft(msg.clone()),
                        }));
                    }
                }
            }
        });
    }
}

/// Starts one flush worker per table as tables appear.
async fn run_flush_supervisor(inner: std::sync::Weak<Inner>) {
    let mut started: HashMap<String, JoinHandle<()>> = HashMap::new();
    loop {
        tokio::time::sleep(TICK).await;
        let Some(strong) = inner.upgrade() else { break };
        for t in strong.sm.tables() {
            let name = t.def.full_name();
            if !started.contains_key(&name) {
                started.insert(name.clone(), tokio::spawn(run_table_flusher(inner.clone(), name)));
            }
        }
    }
    for (_, h) in started {
        h.abort();
    }
}

async fn run_table_flusher(inner: std::sync::Weak<Inner>, table: String) {
    loop {
        let Some(node) = inner.upgrade() else { return };
        if !node.is_leader() {
            drop(node);
            tokio::time::sleep(TICK).await;
            continue;
        }
        match flush_step(&node, &table).await {
            Ok(true) => {}
            Ok(false) => {
                let sm = node.sm.clone();
                drop(node);
                let _ = tokio::time::timeout(TICK, sm.applied.notified()).await;
            }
            Err(e) => {
                let backoff = node.table(&table).map(|t| t.def.config.retry_backoff).unwrap_or(TICK);
                if !matches!(e, Error::NotLeader { .. }) {
                    tracing::warn!(%table, error = %e, "flush step failed");
                    node.record_failure(&table, e.to_string());
                }
                drop(node);
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

/// One step of the flush state machine for a table. Returns whether it made progress.
async fn flush_step(inner: &Arc<Inner>, table: &str) -> Result<bool> {
    let t = inner.table(table)?;
    let cfg = t.def.config.clone();

    enum Next {
        Drop(u64),
        Rotate,
        Start { seq_end: u64, label: String },
        Load { mem: Arc<Memtable>, seq_end: u64, label: String },
        Idle,
    }
    let next = {
        let u = t.units.lock();
        let mut seen = inner.visible_seen.lock();
        let mut next = Next::Idle;
        for unit in u.frozen.iter().filter(|x| x.state == UnitState::Visible) {
            let at = *seen.entry((table.to_string(), unit.seq_end)).or_insert_with(Instant::now);
            if at.elapsed() >= cfg.grace {
                next = Next::Drop(unit.seq_end);
                break;
            }
        }
        if matches!(next, Next::Idle) {
            let unflushed = u.frozen.iter().find(|x| x.state != UnitState::Visible);
            let active_bytes = u.active.bytes();
            let age = u.active.first_write().map(|t| t.elapsed()).unwrap_or_default();
            let idle_trigger = active_bytes >= cfg.flush_bytes || age >= cfg.flush_interval;
            if active_bytes > 0 && (active_bytes >= cfg.max_load_bytes || (unflushed.is_none() && idle_trigger)) {
                next = Next::Rotate;
            } else if let Some(unit) = unflushed {
                next = match &unit.state {
                    UnitState::Frozen => Next::Start {
                        seq_end: unit.seq_end,
                        label: make_label(&t.def, unit.seq_start, unit.seq_end),
                    },
                    UnitState::Loading { label } => Next::Load { mem: unit.mem.clone(), seq_end: unit.seq_end, label: label.clone() },
                    UnitState::Visible => unreachable!("filtered above"),
                };
            }
        }
        next
    };

    match next {
        Next::Idle => Ok(false),
        Next::Drop(seq_end) => {
            inner.propose(Request::Drop { table: table.into(), seq_end }).await?;
            inner.visible_seen.lock().remove(&(table.to_string(), seq_end));
            Ok(true)
        }
        Next::Rotate => {
            inner.propose(Request::Rotate { table: table.into() }).await?;
            Ok(true)
        }
        Next::Start { seq_end, label } => {
            // The intent is replicated before anything is sent to Doris.
            inner.propose(Request::FlushStart { table: table.into(), seq_end, label }).await?;
            Ok(true)
        }
        Next::Load { mem, seq_end, label } => {
            let _slot = inner.load_slots.acquire().await.map_err(|_| Error::Load("shutting down".into()))?;
            load_until_visible(inner, &t, &mem, &label).await?;
            inner.propose(Request::FlushDone { table: table.into(), seq_end }).await?;
            inner.record_success(table);
            tracing::debug!(%table, %label, rows = mem.rows(), "flush visible");
            Ok(true)
        }
    }
}

/// Makes the unit visible in Doris under `label`. Success is only ever taken from the label's
/// state as reported by Doris.
async fn load_until_visible(inner: &Inner, t: &TableSm, mem: &Memtable, label: &str) -> Result<()> {
    let loader = inner.loaders.get(&t.loader).ok_or_else(|| Error::Load(format!("no loader named {} on this node", t.loader)))?;
    let poll = t.def.config.poll_interval;
    // A previous leader (or attempt) may already have submitted this label.
    match loader.state(&t.def, label).await? {
        LoadState::Visible => return finish(loader.as_ref(), t, label).await,
        LoadState::Running => {}
        LoadState::Unknown | LoadState::Failed(_) => {
            let unit = build_unit(t, mem, label)?;
            if let LoadState::Failed(msg) = loader.submit(&unit).await? {
                return Err(Error::Load(msg));
            }
        }
    }
    let mut unknown = 0;
    loop {
        match loader.state(&t.def, label).await? {
            LoadState::Visible => return finish(loader.as_ref(), t, label).await,
            LoadState::Running => {}
            LoadState::Failed(msg) => return Err(Error::Load(msg)),
            LoadState::Unknown => {
                unknown += 1;
                if unknown >= MAX_UNKNOWN_POLLS {
                    return Err(Error::Load(format!("label {label} unknown to Doris after submit")));
                }
            }
        }
        tokio::time::sleep(poll).await;
    }
}

async fn finish(loader: &dyn Loader, t: &TableSm, label: &str) -> Result<()> {
    if let Err(e) = loader.cleanup(&t.def, label).await {
        tracing::warn!(%label, error = %e, "load cleanup failed");
    }
    Ok(())
}

/// Encodes the latest version of every key in the unit as one Parquet file.
fn build_unit(t: &TableSm, mem: &Memtable, label: &str) -> Result<LoadUnit> {
    let codec = &t.codec;
    let props = WriterProperties::builder().set_compression(Compression::SNAPPY).build();
    let mut writer = ArrowWriter::try_new(Vec::new(), codec.load_schema.clone(), Some(props))?;
    let mut rows = 0;
    for sel in select_latest(&[mem], u64::MAX, true) {
        let chunk = emit(&sel)?;
        rows += sel.count;
        let mut cols = chunk.columns;
        cols.push(Arc::new(chunk.seq) as ArrayRef);
        cols.push(Arc::new(Int8Array::from(vec![i8::from(sel.deleted); sel.count])) as ArrayRef);
        writer.write(&RecordBatch::try_new(codec.load_schema.clone(), cols)?)?;
    }
    let parquet = Bytes::from(writer.into_inner()?);
    let (seq_start, seq_end) = mem.seq_range();
    Ok(LoadUnit {
        table: t.def.clone(),
        label: label.to_string(),
        columns: codec.load_schema.fields().iter().map(|f| f.name().clone()).collect(),
        parquet,
        rows,
        seq_start,
        seq_end,
    })
}
