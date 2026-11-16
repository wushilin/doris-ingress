//! Three-node clusters over real HTTP transport.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use doris_ingress::{Node, NodeConfig, ScanOptions, TableConfig};

struct Cluster {
    dirs: Vec<tempfile::TempDir>,
    addrs: Vec<String>,
    nodes: Vec<Option<Node>>,
    doris: Arc<MockLoader>,
    tweak: fn(&mut NodeConfig),
}

impl Cluster {
    async fn start(tweak: fn(&mut NodeConfig)) -> Self {
        let doris = MockLoader::new();
        let mut c = Self { dirs: Vec::new(), addrs: Vec::new(), nodes: Vec::new(), doris, tweak };
        for _ in 0..3 {
            c.dirs.push(tempfile::tempdir().unwrap());
            c.addrs.push(free_addr());
        }
        for i in 0..3 {
            let n = c.boot(i).await;
            c.nodes.push(Some(n));
        }
        let members: Vec<(u64, String)> = (0..3).map(|i| (i as u64 + 1, c.addrs[i].clone())).collect();
        c.node(0).initialize(members).await.unwrap();
        c.leader().await;
        c
    }

    async fn boot(&self, i: usize) -> Node {
        let mut cfg = node_config(i as u64 + 1, &self.addrs[i], self.dirs[i].path());
        (self.tweak)(&mut cfg);
        start_node(cfg, self.doris.clone()).await
    }

    fn node(&self, i: usize) -> &Node {
        self.nodes[i].as_ref().expect("node is running")
    }

    /// Index of the current leader among running nodes.
    async fn leader(&self) -> usize {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            for (i, n) in self.nodes.iter().enumerate() {
                // Leadership counts once a quorum confirms it (a read barrier succeeds).
                if let Some(n) = n
                    && n.is_leader()
                    && n.read_barrier().await.is_ok()
                {
                    return i;
                }
            }
            assert!(std::time::Instant::now() < deadline, "no leader");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(&mut self, i: usize) {
        if let Some(n) = self.nodes[i].take() {
            n.shutdown().await;
        }
    }

    async fn restart(&mut self, i: usize) {
        let n = self.boot(i).await;
        self.nodes[i] = Some(n);
    }

    async fn shutdown(mut self) {
        for i in 0..3 {
            self.stop(i).await;
        }
    }
}

fn no_tweak(_: &mut NodeConfig) {}

fn live_keys(n: &Node) -> Vec<i64> {
    scan_rows(&n.scan(TABLE, ScanOptions::default()).unwrap()).iter().map(|r| r.k).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_replicate_and_survive_leader_failover() {
    let mut c = Cluster::start(no_tweak).await;
    let l = c.leader().await;
    c.node(l).attach(table_def(fast_config()), "mock", 1).await.unwrap();
    let ack = c.node(l).upsert(TABLE, rows(&[(1, "a"), (2, "b")])).await.unwrap();
    c.node(l).delete(TABLE, keys(&[2])).await.unwrap();

    // Followers serve the write once they have applied it (read-your-writes via the token).
    for i in (0..3).filter(|i| *i != l) {
        c.node(i).wait_for_seq(ack.seq + 1, Duration::from_secs(5)).await.unwrap();
        assert_eq!(live_keys(c.node(i)), vec![1]);
        // A follower accepts writes by forwarding them to the leader.
        let fwd = c.node(i).upsert(TABLE, rows(&[(10 + i as i64, "fwd")])).await.unwrap();
        c.node(l).wait_for_seq(fwd.seq, Duration::from_secs(5)).await.unwrap();
        c.node(i).delete(TABLE, keys(&[10 + i as i64])).await.unwrap();
        // ...and serves linearizable reads after a read-index round with the leader.
        c.node(i).read_barrier().await.unwrap();
        assert_eq!(live_keys(c.node(i)), vec![1]);
    }

    // Kill the leader: nothing acknowledged is lost, and flushing continues on the new leader.
    c.stop(l).await;
    let nl = c.leader().await;
    assert_ne!(nl, l);
    c.node(nl).read_barrier().await.unwrap();
    assert_eq!(live_keys(c.node(nl)), vec![1]);
    c.node(nl).upsert(TABLE, rows(&[(3, "c")])).await.unwrap();
    c.node(nl).flush(TABLE, Duration::from_secs(5)).await.unwrap();
    let loaded: Vec<(i64, bool)> = c.doris.loads.lock().iter().flat_map(|(_, r)| r.iter().map(|r| (r.k, r.deleted))).collect();
    // Keys 2, 11, 12 were deleted (11 and 12 through followers): loaded as tombstones.
    assert_eq!(loaded, vec![(1, false), (2, true), (3, false), (11, true), (12, true)]);

    // The old leader rejoins as a follower and catches up.
    c.restart(l).await;
    let seq = c.node(nl).snapshot();
    c.node(l).wait_for_seq(seq, Duration::from_secs(10)).await.unwrap();
    assert_eq!(live_keys(c.node(l)), vec![1, 3]);
    c.shutdown().await;
}

fn small_snapshots(cfg: &mut NodeConfig) {
    cfg.raft.snapshot_every_entries = 10;
    cfg.wal_segment_bytes = 4096;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catches_up_from_a_snapshot() {
    let mut c = Cluster::start(small_snapshots).await;
    let l = c.leader().await;
    let lagging = (l + 1) % 3;
    let table = TableConfig { grace: Duration::from_millis(50), ..fast_config() };
    c.node(l).attach(table_def(table), "mock", 1).await.unwrap();
    c.stop(lagging).await;

    // Rows that get flushed and dropped while the follower is away...
    for k in 0..40 {
        c.node(l).upsert(TABLE, rows(&[(k, "old")])).await.unwrap();
    }
    c.node(l).flush(TABLE, Duration::from_secs(5)).await.unwrap();
    wait_for("drop", Duration::from_secs(5), || c.node(l).stats(TABLE).unwrap().buffered_bytes == 0).await;
    // ...and rows still buffered.
    for k in 100..160 {
        c.node(l).upsert(TABLE, rows(&[(k, "new")])).await.unwrap();
    }

    c.restart(lagging).await;
    let seq = c.node(l).snapshot();
    c.node(lagging).wait_for_seq(seq, Duration::from_secs(10)).await.unwrap();
    let keys = live_keys(c.node(lagging));
    assert_eq!(keys, (100..160).collect::<Vec<_>>());
    assert_eq!(keys, live_keys(c.node(l)));

    // The caught-up follower survives its own restart (its snapshot embeds the entries it got).
    c.stop(lagging).await;
    c.restart(lagging).await;
    c.node(lagging).wait_for_seq(seq, Duration::from_secs(10)).await.unwrap();
    assert_eq!(live_keys(c.node(lagging)), (100..160).collect::<Vec<_>>());
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_megabyte_writes_replicate() {
    let c = Cluster::start(no_tweak).await;
    let l = c.leader().await;
    c.node(l).attach(table_def(fast_config()), "mock", 1).await.unwrap();
    // ~6 MB per write: one Raft entry larger than common HTTP body limits.
    let big = "x".repeat(600);
    let data: Vec<(i64, &str)> = (0..10_000).map(|k| (k, big.as_str())).collect();
    let follower = (l + 1) % 3;
    let ack = tokio::time::timeout(Duration::from_secs(20), c.node(follower).upsert(TABLE, rows(&data)))
        .await
        .expect("large write must commit")
        .unwrap();
    for i in 0..3 {
        c.node(i).wait_for_seq(ack.seq, Duration::from_secs(10)).await.unwrap();
        assert_eq!(live_keys(c.node(i)).len(), 10_000);
    }
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_read_barrier_is_fast() {
    let c = Cluster::start(no_tweak).await;
    let l = c.leader().await;
    c.node(l).attach(table_def(fast_config()), "mock", 1).await.unwrap();
    c.node(l).upsert(TABLE, rows(&[(1, "a")])).await.unwrap();
    let f = (l + 1) % 3;
    let mut worst = Duration::ZERO;
    let start = std::time::Instant::now();
    for _ in 0..200 {
        let t = std::time::Instant::now();
        c.node(f).read_barrier().await.unwrap();
        worst = worst.max(t.elapsed());
    }
    let avg = start.elapsed() / 200;
    eprintln!("follower read barrier: avg {avg:?}, worst {worst:?}");
    assert!(avg < Duration::from_millis(5), "avg {avg:?}");

    // Many concurrent barriers on one follower.
    let node = Arc::new(c.nodes[f].as_ref().unwrap() as *const Node as usize);
    let _ = node;
    let start = std::time::Instant::now();
    let worst = Arc::new(parking_lot::Mutex::new(Duration::ZERO));
    let fut = (0..64).map(|_| {
        let worst = worst.clone();
        let n = c.node(f);
        async move {
            for _ in 0..50 {
                let t = std::time::Instant::now();
                n.read_barrier().await.unwrap();
                let e = t.elapsed();
                let mut w = worst.lock();
                *w = (*w).max(e);
            }
        }
    });
    futures_util::future::join_all(fut).await;
    let worst = *worst.lock();
    eprintln!("64 concurrent: 3200 barriers in {:?}, worst {worst:?}", start.elapsed());
    assert!(worst < Duration::from_secs(1), "worst {worst:?}");
    c.shutdown().await;
}
