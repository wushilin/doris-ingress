//! Raft replication: entry types, WAL-backed log store, state machine and HTTP transport.

pub mod log_store;
pub mod network;
pub mod state_machine;

use std::fmt;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::schema::TableDefWire;

pub type NodeId = u64;
pub use openraft::BasicNode;

/// One table operation inside a [`Request::Write`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteOpWire {
    pub table: String,
    pub delete: bool,
    /// The rows: one full-schema batch (tombstones carry filler values), encoded per `format`.
    pub ipc: Bytes,
    /// [`crate::ipc::FORMAT_STREAM`] (0, full Arrow IPC stream) or
    /// [`crate::ipc::FORMAT_BATCH`] (1, record-batch message without schema).
    #[serde(default)]
    pub format: u8,
}

/// Commands replicated through Raft. Leader-decided values (sequence floors, labels, rotation
/// and drop points) are carried in the entry so `apply` stays deterministic.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Request {
    AttachTable { def: Box<TableDefWire>, loader: String, min_next_seq: u64 },
    /// A group-committed batch of client writes. `groups` gives the op count of each client write
    /// in order; each group applies atomically and independently (empty = one group of all ops).
    Write {
        ops: Vec<WriteOpWire>,
        #[serde(default)]
        groups: Vec<u32>,
    },
    /// Freeze the table's active memtable into a flush unit.
    Rotate { table: String },
    /// The leader is about to load the unit ending at `seq_end` under `label`.
    FlushStart { table: String, seq_end: u64, label: String },
    /// Doris reported `label` visible.
    FlushDone { table: String, seq_end: u64 },
    /// The unit's grace period passed; free it.
    Drop { table: String, seq_end: u64 },
    /// Replace the table's settings (flush sizes and timings, grace, memory cap, ...).
    SetTableConfig { table: String, config: Box<crate::config::TableConfig> },
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::AttachTable { def, .. } => write!(f, "AttachTable({}.{})", def.db, def.name),
            Request::Write { ops, groups } => write!(f, "Write({} ops, {} writes)", ops.len(), groups.len().max(1)),
            Request::Rotate { table } => write!(f, "Rotate({table})"),
            Request::FlushStart { table, seq_end, label } => write!(f, "FlushStart({table}, {seq_end}, {label})"),
            Request::FlushDone { table, seq_end } => write!(f, "FlushDone({table}, {seq_end})"),
            Request::Drop { table, seq_end } => write!(f, "Drop({table}, {seq_end})"),
            Request::SetTableConfig { table, .. } => write!(f, "SetTableConfig({table})"),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Response {
    Ok,
    /// Per client write (group): the sequence range `(first, last)` of each of its ops, or why
    /// the group was rejected (nothing of it applied).
    Write { groups: Vec<Result<Vec<(u64, u64)>, String>> },
    /// The command was not applied; nothing changed.
    Rejected(String),
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = BasicNode,
);

pub type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::type_config::alias::VoteOf<TypeConfig>;
pub type Entry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type StoredMembership = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;
pub type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
pub type Raft = openraft::Raft<TypeConfig, state_machine::StateMachine>;

/// Snapshot payload for a lagging follower: state metadata plus the raw log entries that still
/// hold unflushed or grace-period rows.
#[derive(Clone, Debug, Default)]
pub struct SnapshotData {
    pub meta: Bytes,
    pub entries: SnapshotEntries,
}

/// Where a snapshot's entries are. They can be many gigabytes, so they are never all in memory
/// for a transfer: the leader reads them from its WAL as it sends, and the follower writes them
/// to its snapshot file as they arrive.
#[derive(Clone)]
pub enum SnapshotEntries {
    /// In memory (a snapshot that has just been built carries none).
    Memory(Vec<(u64, Bytes)>),
    /// On the sending node: read one at a time from the snapshot file's embedded entries or the
    /// WAL, which is kept from being trimmed for as long as this is alive.
    Lazy(std::sync::Arc<state_machine::LazyEntries>),
    /// On the receiving node: already written to this snapshot file.
    File(std::path::PathBuf),
}

impl Default for SnapshotEntries {
    fn default() -> Self {
        Self::Memory(Vec::new())
    }
}

impl fmt::Debug for SnapshotEntries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Memory(v) => write!(f, "Memory({} entries)", v.len()),
            Self::Lazy(l) => write!(f, "Lazy({} entries)", l.len()),
            Self::File(p) => write!(f, "File({})", p.display()),
        }
    }
}

impl SnapshotEntries {
    /// The entries in index order, read one at a time.
    pub fn read(self) -> Box<dyn Iterator<Item = std::io::Result<(u64, Bytes)>> + Send> {
        match self {
            Self::Memory(v) => Box::new(v.into_iter().map(Ok)),
            Self::Lazy(l) => Box::new((0..l.len()).map(move |k| l.get(k))),
            Self::File(path) => match state_machine::read_snapshot_entries(&path) {
                Ok(entries) => Box::new(entries.into_iter().map(Ok)),
                Err(e) => Box::new(std::iter::once(Err(e))),
            },
        }
    }
}

pub(crate) fn encode<T: Serialize>(v: &T) -> std::io::Result<Bytes> {
    rmp_serde::to_vec_named(v).map(Bytes::from).map_err(std::io::Error::other)
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(b: &[u8]) -> std::io::Result<T> {
    rmp_serde::from_slice(b).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
