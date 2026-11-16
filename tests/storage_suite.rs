//! openraft's storage conformance suite against the WAL-backed log store and state machine.

use std::sync::Arc;

use doris_ingress::MemoryConfig;
use doris_ingress::raft::TypeConfig;
use doris_ingress::raft::log_store::{LogStore, RetentionFloor};
use doris_ingress::raft::state_machine::{SmCore, StateMachine};
use doris_ingress::wal::{Wal, WalOptions};
use openraft::StorageError;
use openraft::testing::log::{StoreBuilder, Suite};

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, StateMachine, tempfile::TempDir> for Builder {
    async fn build(&self) -> Result<(tempfile::TempDir, LogStore, StateMachine), StorageError<TypeConfig>> {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = Wal::open(WalOptions { dir: dir.path().join("wal"), segment_bytes: 4096 }).unwrap();
        let wal = Arc::new(wal);
        let floor = RetentionFloor::default();
        let log = LogStore::open(wal.clone(), floor.clone()).unwrap();
        let core = SmCore::open(wal, dir.path().join("snapshots"), floor, &MemoryConfig::default()).unwrap();
        Ok((dir, log, StateMachine { core }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openraft_storage_suite() {
    Suite::test_all(Builder).await.unwrap();
}
