//! doris-ingress: a replicated write buffer in front of Doris Unique Key Merge-on-Write tables.
//! See `PLAN.md` for the design.

pub mod config;
pub mod doris;
pub mod error;
pub mod flight;
pub mod ipc;
pub mod loader;
mod memory;
mod memtable;
mod node;
pub mod raft;
pub mod schema;
mod tls;
pub mod wal;

pub use config::{BatchConfig, MemoryConfig, NodeConfig, RaftTuning, TableConfig};
pub use error::{Error, Result};
pub use node::{Node, ScanOptions, TableStats, WriteAck, WriteKind, WriteOp};
pub use schema::{SeqTarget, TableDef};
