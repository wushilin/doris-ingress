//! Three-node clusters over real HTTP transport.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use doris_ingress::{Node, NodeConfig, ScanOptions, TableConfig};

struct Cluster {
    dirs: Vec<tempfile::TempDir>,
    addrs: Vec<String>,
    nodes: Vec<Option<Arc<Node>>>,
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
            c.nodes.push(Some(Arc::new(n)));
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
        self.nodes[i] = Some(Arc::new(n));
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
        // Catch up with everything committed so far, including the previous follower's writes.
        c.node(i).read_barrier().await.unwrap();
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
    // It ends with the new leader's buffer: keys 1 and 3, or nothing once the flushed unit has
    // been dropped after its grace period (the drop is replicated like any other entry).
    wait_for("old leader matches the new one", Duration::from_secs(5), || live_keys(c.node(l)) == live_keys(c.node(nl))).await;
    let keys = live_keys(c.node(l));
    assert!(keys == vec![1, 3] || keys.is_empty(), "{keys:?}");
    c.shutdown().await;
}

/// Everything a node holds in memory for the table: every buffered row version it would serve
/// (tombstones included), the row counts per stage, and the watermarks.
fn memory_state(n: &Node) -> (Vec<Row>, [usize; 3], [u64; 2], u64) {
    let rows = scan_rows(&n.scan(TABLE, ScanOptions { include_deleted: true, snapshot: None }).unwrap());
    let s = n.stats(TABLE).unwrap();
    (rows, [s.active_rows, s.unflushed_rows, s.grace_rows], [s.visible_seq, s.trimmed_seq], n.snapshot())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_member_joins_with_exactly_the_leaders_memory_state() {
    let mut c = Cluster::start(no_tweak).await;
    let l = c.leader().await;
    let table = TableConfig { grace: Duration::from_millis(50), ..fast_config() };
    c.node(l).attach(table_def(table), "mock", 1).await.unwrap();

    // History the new member never sees as rows: flushed to Doris and trimmed.
    for k in 0..40 {
        c.node(l).upsert(TABLE, rows(&[(k, "trimmed")])).await.unwrap();
    }
    c.node(l).flush(TABLE, Duration::from_secs(5)).await.unwrap();
    wait_for("trim", Duration::from_secs(5), || c.node(l).stats(TABLE).unwrap().trimmed_seq == 40).await;
    // What is still buffered: new rows, an overwritten key, a delete, and a delete of a trimmed key.
    for k in 100..130 {
        c.node(l).upsert(TABLE, rows(&[(k, "buffered")])).await.unwrap();
    }
    c.node(l).upsert(TABLE, rows(&[(100, "overwritten")])).await.unwrap();
    c.node(l).delete(TABLE, keys(&[101, 5])).await.unwrap();
    let expected = memory_state(c.node(l));
    assert_eq!(expected.0.len(), 31);
    assert_eq!(expected.2, [40, 40]);

    // A fourth node with an empty disk joins as a learner, then as a voter.
    c.dirs.push(tempfile::tempdir().unwrap());
    c.addrs.push(free_addr());
    let new = c.boot(3).await;
    c.nodes.push(Some(Arc::new(new)));
    c.node(l).add_learner(4, c.addrs[3].clone()).await.unwrap();
    c.node(l).change_membership(vec![1, 2, 3, 4]).await.unwrap();
    c.node(3).wait_for_seq(expected.3, Duration::from_secs(10)).await.unwrap();
    assert_eq!(memory_state(c.node(3)), expected);
    // The original followers hold the same state too.
    for i in (0..3).filter(|i| *i != l) {
        c.node(i).wait_for_seq(expected.3, Duration::from_secs(10)).await.unwrap();
        assert_eq!(memory_state(c.node(i)), expected);
    }

    // The new member rebuilds the same state from its own disk after a restart...
    c.stop(3).await;
    c.restart(3).await;
    c.node(3).wait_for_seq(expected.3, Duration::from_secs(10)).await.unwrap();
    assert_eq!(memory_state(c.node(3)), expected);
    // ...and keeps following: a later write and trim reach it.
    c.node(l).upsert(TABLE, rows(&[(200, "later")])).await.unwrap();
    c.node(l).flush(TABLE, Duration::from_secs(5)).await.unwrap();
    wait_for("second trim on the new member", Duration::from_secs(10), || {
        let s = c.node(3).stats(TABLE).unwrap();
        s.trimmed_seq == c.node(l).snapshot() && s.active_rows + s.unflushed_rows + s.grace_rows == 0
    })
    .await;
    assert_eq!(memory_state(c.node(3)), memory_state(c.node(l)));
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
    // The trim watermark is replicated state: the follower that missed the drop (and got it from
    // a snapshot, then restarted) has the leader's value.
    let leader_stats = c.node(l).stats(TABLE).unwrap();
    assert_eq!(leader_stats.trimmed_seq, 40);
    assert_eq!(leader_stats.visible_seq, 40);
    let follower_stats = c.node(lagging).stats(TABLE).unwrap();
    assert_eq!((follower_stats.visible_seq, follower_stats.trimmed_seq), (40, 40));
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_megabyte_writes_replicate() {
    let c = Cluster::start(no_tweak).await;
    let l = c.leader().await;
    c.node(l).attach(table_def(fast_config()), "mock", 1).await.unwrap();
    let follower = (l + 1) % 3;
    wait_for("attach on the follower", Duration::from_secs(5), || !c.node(follower).tables().is_empty()).await;
    // ~6 MB per write: one Raft entry larger than common HTTP body limits.
    let big = "x".repeat(600);
    let data: Vec<(i64, &str)> = (0..10_000).map(|k| (k, big.as_str())).collect();
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

fn slow_to_forget(cfg: &mut NodeConfig) {
    small_snapshots(cfg);
    cfg.raft.catch_up_idle = Duration::from_secs(8);
}

/// The leader holds the log for a follower that is only briefly away: the follower catches up
/// from the log, without a snapshot. A follower that stays silent loses its hold, the log is
/// purged, and it gets a snapshot when it returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_holds_the_log_for_a_follower_that_keeps_answering() {
    let mut c = Cluster::start(slow_to_forget).await;
    let l = c.leader().await;
    let f = (l + 1) % 3;
    let table = TableConfig { grace: Duration::from_millis(50), ..fast_config() };
    c.node(l).attach(table_def(table), "mock", 1).await.unwrap();
    c.node(f).wait_for_seq(c.node(l).snapshot(), Duration::from_secs(5)).await.unwrap();
    // Flush and trim a batch of writes; with every follower level, the leader's log is purged.
    let mut k = 0;
    let mut write_flush_trim = async |c: &Cluster, rows_n: i64| {
        for _ in 0..rows_n {
            c.node(l).upsert(TABLE, rows(&[(k, "v")])).await.unwrap();
            k += 1;
        }
        c.node(l).flush(TABLE, Duration::from_secs(5)).await.unwrap();
        let end = c.node(l).snapshot();
        wait_for("trim", Duration::from_secs(5), || c.node(l).stats(TABLE).unwrap().trimmed_seq == end).await;
    };
    write_flush_trim(&c, 30).await;
    wait_for("purge with all followers level", Duration::from_secs(5), || c.node(l).raft_purged_index() > 20).await;

    // 1. The follower is away for a moment while more is written, flushed and trimmed.
    let held_from = c.node(f).raft_last_log_index();
    c.stop(f).await;
    write_flush_trim(&c, 30).await;
    tokio::time::sleep(Duration::from_millis(1500)).await; // time for a snapshot and a purge round
    assert!(c.node(l).raft_purged_index() <= held_from, "purged {} past the follower at {held_from}", c.node(l).raft_purged_index());
    c.restart(f).await;
    c.node(f).wait_for_seq(c.node(l).snapshot(), Duration::from_secs(10)).await.unwrap();
    assert_eq!(c.node(f).snapshots_installed(), 0, "caught up from the log");
    // (The trim is a later log entry than the writes, so it can arrive a moment after them.)
    wait_for("the follower has the leader's trim watermark", Duration::from_secs(5), || {
        c.node(f).stats(TABLE).unwrap().trimmed_seq == c.node(l).stats(TABLE).unwrap().trimmed_seq
    })
    .await;

    // 2. The follower stays away: after the idle time its hold is dropped and the log is purged.
    let held_from = c.node(f).raft_last_log_index();
    c.stop(f).await;
    write_flush_trim(&c, 30).await;
    wait_for("purge once the follower is given up", Duration::from_secs(25), || c.node(l).raft_purged_index() > held_from).await;
    c.restart(f).await;
    c.node(f).wait_for_seq(c.node(l).snapshot(), Duration::from_secs(10)).await.unwrap();
    assert!(c.node(f).snapshots_installed() >= 1, "caught up from a snapshot");
    // (The trim is a later log entry than the writes, so it can arrive a moment after them.)
    wait_for("the follower has the leader's trim watermark", Duration::from_secs(5), || {
        c.node(f).stats(TABLE).unwrap().trimmed_seq == c.node(l).stats(TABLE).unwrap().trimmed_seq
    })
    .await;
    c.shutdown().await;
}

/// A table that flushes and trims all the time, so that the leader always has something to purge.
fn busy_table() -> TableConfig {
    TableConfig { flush_interval: Duration::from_millis(50), grace: Duration::from_millis(20), ..fast_config() }
}

/// Writes single rows to `node` as fast as it can until `stop` is set.
fn spawn_writer(node: Arc<Node>, stop: Arc<std::sync::atomic::AtomicBool>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut k = 0;
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            if node.upsert(TABLE, rows(&[(k, "load")])).await.is_ok() {
                k += 1;
            }
        }
    })
}

/// The fault tests watch one leader throughout, so it must stay leader: elections only after
/// seconds without a heartbeat, not after a busy moment on a loaded machine.
fn steady_leader(cfg: &mut NodeConfig) {
    cfg.raft.election_timeout_min = Duration::from_secs(3);
    cfg.raft.election_timeout_max = Duration::from_secs(4);
}

fn quick_to_forget(cfg: &mut NodeConfig) {
    small_snapshots(cfg);
    steady_leader(cfg);
    cfg.raft.catch_up_idle = Duration::from_secs(2);
}

/// Two followers in trouble in turn, under constant writes. One goes silent: after the idle time
/// it loses its hold, the leader purges past it, and it returns by snapshot. The other then
/// becomes slow but keeps answering: the log it still needs is kept for as long as it needs it,
/// while the first one, level again, moves on freely. The slow one catches up from the log, and
/// in the end the leader's log is purged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cursors_are_independent_and_a_silent_follower_loses_its_hold() {
    use doris_ingress::raft::network::PeerFault;
    let mut c = Cluster::start(quick_to_forget).await;
    let l = c.leader().await;
    let (slow, gone) = ((l + 1) % 3, (l + 2) % 3);
    c.node(l).attach(table_def(busy_table()), "mock", 1).await.unwrap();
    wait_for("table on every node", Duration::from_secs(5), || (0..3).all(|i| !c.node(i).tables().is_empty())).await;
    let leader = c.nodes[l].clone().unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = spawn_writer(leader.clone(), stop.clone());

    // 1. A follower goes silent. Its hold lasts the idle time; then the log is purged well past it.
    c.stop(gone).await;
    let gone_at = leader.raft_last_log_index(); // it cannot have more than this
    wait_for("purged past the silent follower", Duration::from_secs(30), || leader.raft_purged_index() > gone_at + 50).await;
    assert!(leader.is_leader(), "the leader under test lost leadership");

    // 2. It returns: its entries are gone, so it gets a snapshot, then follows the log.
    c.restart(gone).await;
    wait_for("returned follower close to the leader", Duration::from_secs(15), || {
        c.node(gone).snapshots_installed() >= 1 && leader.raft_last_log_index() < c.node(gone).raft_last_log_index() + 20
    })
    .await;

    // 3. The other follower turns slow (the first one is healthy again, so writes go on at full
    //    speed). Nothing the slow one still needs is purged while it lags.
    leader.inject_peer_fault(slow as u64 + 1, Some(PeerFault { append_delay: Duration::from_millis(400), ..Default::default() }));
    let mut lagged = 0;
    let until = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < until {
        let purged = leader.raft_purged_index();
        let has = c.node(slow).raft_last_log_index();
        assert!(purged <= has, "purged up to {purged}, but the slow follower only has {has}");
        lagged = lagged.max(leader.raft_last_log_index().saturating_sub(has));
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(lagged > 20, "the slow follower was never behind (at most {lagged} entries)");

    // 4. It speeds up and catches up from the log, still under load.
    leader.inject_peer_fault(slow as u64 + 1, None);
    wait_for("slow follower close to the leader", Duration::from_secs(15), || leader.raft_last_log_index() < c.node(slow).raft_last_log_index() + 20).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await.unwrap();
    wait_for("both followers level", Duration::from_secs(15), || {
        c.node(slow).raft_last_log_index() == leader.raft_last_log_index() && c.node(gone).raft_last_log_index() == leader.raft_last_log_index()
    })
    .await;
    assert_eq!(c.node(slow).snapshots_installed(), 0, "the slow follower caught up from the log");
    wait_for("all three hold the same state", Duration::from_secs(10), || {
        memory_state(c.node(slow)) == memory_state(&leader) && memory_state(c.node(gone)) == memory_state(&leader)
    })
    .await;
    // With everyone level and everything flushed, the leader's log is purged up to its snapshot.
    wait_for("the log is purged in the end", Duration::from_secs(10), || leader.raft_purged_index() + 30 >= leader.raft_last_log_index()).await;
    drop(leader);
    c.shutdown().await;
}

fn small_hold(cfg: &mut NodeConfig) {
    small_snapshots(cfg);
    steady_leader(cfg);
    cfg.raft.catch_up_idle = Duration::from_secs(120); // not what releases the follower here
    cfg.raft.catch_up_hold_bytes = 32 << 10;
}

/// The node that leads now (the one given if none claims to at this instant).
fn leading<'a>(c: &'a Cluster, fallback: &'a Node) -> &'a Node {
    c.nodes.iter().flatten().map(|n| n.as_ref()).find(|n| n.is_leader()).unwrap_or(fallback)
}

/// A follower that answers but cannot take anything (every append and every snapshot send times
/// out) is released once the WAL kept for it passes the size limit. When the faults clear it
/// rejoins by snapshot and follows the log, all under constant writes. The faults are set on
/// every node, so they hold whoever leads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_follower_is_released_by_the_size_limit_and_rejoins_under_load() {
    use doris_ingress::raft::network::PeerFault;
    if std::env::var("TEST_LOG").is_ok() {
        let _ = tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::new(std::env::var("TEST_LOG").unwrap())).with_ansi(false).try_init();
    }
    let c = Cluster::start(small_hold).await;
    let l = c.leader().await;
    let stalled = (l + 1) % 3;
    let id = stalled as u64 + 1;
    c.node(l).attach(table_def(busy_table()), "mock", 1).await.unwrap();
    wait_for("table on every node", Duration::from_secs(5), || (0..3).all(|i| !c.node(i).tables().is_empty())).await;
    let first = c.nodes[l].clone().unwrap();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = spawn_writer(first.clone(), stop.clone());
    let inject = |fault: Option<PeerFault>| (0..3).for_each(|i| c.node(i).inject_peer_fault(id, fault));

    // Every append to the follower times out, and so does every snapshot send: it stays alive
    // but gets nothing.
    inject(Some(PeerFault { append_delay: Duration::from_millis(100), append_fails: true, snapshot_fails: true }));
    wait_for("released by the size limit", Duration::from_secs(30), || {
        leading(&c, &first).raft_purged_index() > c.node(stalled).raft_last_log_index()
    })
    .await;

    // It keeps trying and failing for a while longer, as the leader moves on.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(c.node(stalled).snapshots_installed(), 0);
    assert!(leading(&c, &first).raft_last_log_index() > c.node(stalled).raft_last_log_index() + 10, "still behind");

    // Everything works again: it catches up while the writes continue.
    inject(None);
    wait_for("rejoined under load", Duration::from_secs(30), || {
        c.node(stalled).snapshots_installed() >= 1 && leading(&c, &first).raft_last_log_index() < c.node(stalled).raft_last_log_index() + 30
    })
    .await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await.unwrap();
    let same = || (1..3).all(|i| memory_state(c.node(i)) == memory_state(c.node(0)));
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while !same() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if !same() {
        for i in 0..3 {
            let (rows, stages, marks, seq) = memory_state(c.node(i));
            eprintln!(
                "node {} ({}): leader={} log={} purged={} snapshots_installed={} rows={} stages={stages:?} watermarks={marks:?} seq={seq}",
                i + 1,
                if i == stalled { "the stalled one" } else { "healthy" },
                c.node(i).is_leader(),
                c.node(i).raft_last_log_index(),
                c.node(i).raft_purged_index(),
                c.node(i).snapshots_installed(),
                rows.len(),
            );
        }
        panic!("the three nodes did not reach the same state");
    }
    drop(first);
    c.shutdown().await;
}

/// A follower rejoins by a snapshot much larger than fits comfortably in one request: the
/// entries behind 96 MB of buffered rows. It is streamed from the leader's WAL into the
/// follower's snapshot file, at the configured rate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_snapshot_is_streamed_to_a_returning_follower() {
    fn tweak(cfg: &mut NodeConfig) {
        quick_to_forget(cfg);
        cfg.raft.snapshot_send_bytes_per_sec = 64 << 20;
    }
    let mut c = Cluster::start(tweak).await;
    let l = c.leader().await;
    let away = (l + 1) % 3;
    let table = TableConfig { grace: Duration::from_millis(50), ..fast_config() };
    c.node(l).attach(table_def(table), "mock", 1).await.unwrap();
    wait_for("table on every node", Duration::from_secs(5), || (0..3).all(|i| !c.node(i).tables().is_empty())).await;
    c.stop(away).await;
    let away_at = c.node(l).raft_last_log_index();

    // Something flushed and trimmed, so that the log moves past the absent follower...
    for k in 0..20 {
        c.node(l).upsert(TABLE, rows(&[(k, "trimmed")])).await.unwrap();
    }
    c.node(l).flush(TABLE, Duration::from_secs(5)).await.unwrap();
    // ...and 96 MB that stays buffered: one megabyte a row.
    let big = "x".repeat(1 << 20);
    for k in 100..196 {
        c.node(l).upsert(TABLE, rows(&[(k, big.as_str())])).await.unwrap();
    }
    wait_for("purged past the absent follower", Duration::from_secs(20), || c.node(l).raft_purged_index() > away_at + 20).await;

    let started = std::time::Instant::now();
    c.restart(away).await;
    wait_for("the follower has installed the snapshot", Duration::from_secs(60), || c.node(away).snapshots_installed() >= 1).await;
    // 96 MB at 64 MB a second cannot arrive in less than a second.
    assert!(started.elapsed() >= Duration::from_secs(1), "arrived in {:?}", started.elapsed());
    wait_for("same state as the leader", Duration::from_secs(20), || memory_state(c.node(away)) == memory_state(c.node(l))).await;
    assert_eq!(live_keys(c.node(away)), (100..196).collect::<Vec<_>>());
    // The follower restarts from its own copy of the snapshot.
    c.stop(away).await;
    c.restart(away).await;
    wait_for("same state after its own restart", Duration::from_secs(30), || memory_state(c.node(away)) == memory_state(c.node(l))).await;
    c.shutdown().await;
}
