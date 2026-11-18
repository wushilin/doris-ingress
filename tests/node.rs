//! Single-node cluster behaviour: write/read semantics, flushing, backpressure, crash recovery.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use doris_ingress::loader::LoadState;
use doris_ingress::{Error, ScanOptions, TableConfig, WriteKind, WriteOp};

fn row(k: i64, v: &str, seq: i64, deleted: bool) -> Row {
    Row { k, v: Some(v.into()), seq, deleted }
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_see_latest_version_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let e = single_node(dir.path(), fast_config(), MockLoader::new()).await;

    let first = e.upsert(TABLE, rows(&[(1, "a"), (2, "b"), (3, "c")])).await.unwrap();
    assert_eq!(first.seq, 3);
    e.upsert(TABLE, rows(&[(1, "a2")])).await.unwrap();
    e.delete(TABLE, keys(&[2])).await.unwrap();

    let got = scan_rows(&[e.get(TABLE, &keys(&[1, 2, 4]), None).unwrap()]);
    assert_eq!(got.len(), 2, "key 4 was never written");
    assert_eq!(got[0], row(1, "a2", 4, false));
    assert!(got[1].deleted && got[1].k == 2 && got[1].seq == 5);

    let live = scan_rows(&e.scan(TABLE, ScanOptions::default()).unwrap());
    assert_eq!(live.iter().map(|r| r.k).collect::<Vec<_>>(), vec![1, 3]);
    let all = scan_rows(&e.scan(TABLE, ScanOptions { include_deleted: true, snapshot: None }).unwrap());
    assert_eq!(all.len(), 3);
    let old = scan_rows(&e.scan(TABLE, ScanOptions { include_deleted: true, snapshot: Some(first.seq) }).unwrap());
    assert_eq!(old, vec![row(1, "a", 1, false), row(2, "b", 2, false), row(3, "c", 3, false)]);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn later_duplicate_in_one_batch_wins_and_bad_writes_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let e = single_node(dir.path(), fast_config(), MockLoader::new()).await;
    e.upsert(TABLE, rows(&[(1, "first"), (1, "second")])).await.unwrap();
    assert_eq!(scan_rows(&[e.get(TABLE, &keys(&[1]), None).unwrap()]), vec![row(1, "second", 2, false)]);
    assert!(matches!(e.upsert(TABLE, keys(&[1])).await, Err(Error::Schema(_))));
    assert!(matches!(e.upsert("db.nope", rows(&[(1, "a")])).await, Err(Error::UnknownTable(_))));
    assert!(matches!(e.attach(table_def(fast_config()), "mock", 1).await, Err(Error::TableExists(_))));
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sequence_starts_above_doris_max() {
    let dir = tempfile::tempdir().unwrap();
    let e = single_node(dir.path(), fast_config(), MockLoader::new()).await;
    e.attach(table_def_named("t2", fast_config()), "mock", 1000).await.unwrap();
    assert_eq!(e.upsert("db.t2", rows(&[(1, "a")])).await.unwrap().seq, 1000);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_writers_are_group_committed() {
    let dir = tempfile::tempdir().unwrap();
    let e = Arc::new(single_node(dir.path(), fast_config(), MockLoader::new()).await);
    let entries_before = e.raft_last_log_index();
    let mut tasks = Vec::new();
    for k in 0..500i64 {
        let e = e.clone();
        tasks.push(tokio::spawn(async move { e.upsert(TABLE, rows(&[(k, "v")])).await.unwrap().seq }));
    }
    let mut seqs = Vec::new();
    for t in tasks {
        seqs.push(t.await.unwrap());
    }
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs.len(), 500, "every writer gets its own sequence number");
    let entries = e.raft_last_log_index() - entries_before;
    assert!(entries < 250, "500 writes used {entries} raft entries; expected batching");
    assert_eq!(scan_rows(&e.scan(TABLE, ScanOptions::default()).unwrap()).len(), 500);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn flush_loads_latest_versions_then_drops_after_grace() {
    let dir = tempfile::tempdir().unwrap();
    let loader = MockLoader::new();
    let e = single_node(dir.path(), fast_config(), loader.clone()).await;
    e.upsert(TABLE, rows(&[(1, "a"), (2, "b")])).await.unwrap();
    e.upsert(TABLE, rows(&[(1, "a2")])).await.unwrap();
    e.delete(TABLE, keys(&[2])).await.unwrap();

    e.flush(TABLE, Duration::from_secs(5)).await.unwrap();

    let loads = loader.loads.lock().clone();
    assert_eq!(loads.len(), 1);
    let (label, rows_loaded) = &loads[0];
    assert_eq!(label, "ingress_db_t_1_4");
    assert_eq!(rows_loaded, &vec![row(1, "a2", 3, false), Row { k: 2, v: None, seq: 4, deleted: true }]);

    assert_eq!(e.get(TABLE, &keys(&[1]), None).unwrap().num_rows(), 1, "readable during grace");
    wait_for("grace drop", Duration::from_secs(3), || e.stats(TABLE).unwrap().buffered_bytes == 0).await;
    assert_eq!(e.memory_used(), 0);
    assert_eq!(e.get(TABLE, &keys(&[1]), None).unwrap().num_rows(), 0);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_during_a_load_go_into_one_larger_next_load() {
    let dir = tempfile::tempdir().unwrap();
    let loader = MockLoader::new();
    loader.hold.store(true, Ordering::SeqCst);
    let cfg = TableConfig { flush_interval: Duration::from_millis(30), ..fast_config() };
    let e = single_node(dir.path(), cfg, loader.clone()).await;

    e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();
    wait_for("first load", Duration::from_secs(3), || loader.load_count() == 1).await;
    for k in 2..=5 {
        e.upsert(TABLE, rows(&[(k, "x")])).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert_eq!(loader.load_count(), 1, "only one load in flight per table");
    loader.release();
    wait_for("second load", Duration::from_secs(3), || loader.load_count() == 2).await;
    loader.release();
    let second = loader.loads.lock()[1].1.clone();
    assert_eq!(second.iter().map(|r| r.k).collect::<Vec<_>>(), vec![2, 3, 4, 5]);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn failing_flush_retries_same_label_and_rejects_writes_until_it_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let loader = MockLoader::new();
    loader.fail_next.store(3, Ordering::SeqCst);
    let cfg = TableConfig { max_flush_failures: 2, ..fast_config() };
    let e = single_node(dir.path(), cfg, loader.clone()).await;
    e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();

    assert!(e.flush(TABLE, Duration::from_millis(30)).await.is_err());
    wait_for("table rejecting", Duration::from_secs(3), || e.stats(TABLE).unwrap().rejecting.is_some()).await;
    match e.upsert(TABLE, rows(&[(2, "b")])).await {
        Err(Error::TableStuck { reason, .. }) => assert!(reason.contains("injected failure"), "{reason}"),
        other => panic!("expected TableStuck, got {other:?}"),
    }
    wait_for("recovery", Duration::from_secs(5), || e.stats(TABLE).unwrap().rejecting.is_none()).await;
    let submits = loader.submits.lock().clone();
    assert_eq!(submits.len(), 4);
    assert!(submits.iter().all(|l| l == "ingress_db_t_1_1"), "{submits:?}");
    e.upsert(TABLE, rows(&[(2, "b")])).await.unwrap();
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn memory_target_delays_then_fails_writers_until_doris_catches_up() {
    let dir = tempfile::tempdir().unwrap();
    let loader = MockLoader::new();
    loader.hold.store(true, Ordering::SeqCst);
    let addr = free_addr();
    let mut cfg = node_config(1, &addr, dir.path());
    cfg.memory.target_bytes = 1;
    cfg.memory.max_admission_wait = Duration::from_millis(100);
    let e = start_node(cfg, loader.clone()).await;
    e.initialize(vec![(1, addr)]).await.unwrap();
    wait_leader(&e, 1).await;
    e.attach(table_def(fast_config()), "mock", 1).await.unwrap();

    e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();
    let started = std::time::Instant::now();
    assert!(matches!(e.upsert(TABLE, rows(&[(2, "b")])).await, Err(Error::Backpressure { .. })));
    assert!(started.elapsed() >= Duration::from_millis(100));

    let _ = e.flush(TABLE, Duration::from_millis(10)).await;
    loader.release();
    let retry = async {
        loop {
            match e.upsert(TABLE, rows(&[(2, "b")])).await {
                Ok(ack) => return ack,
                Err(Error::Backpressure { .. }) => continue,
                Err(other) => panic!("unexpected {other:?}"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), retry).await.expect("writer admitted after release");
    assert_eq!(e.get(TABLE, &keys(&[2]), None).unwrap().num_rows(), 1);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_table_makes_writers_wait_instead_of_failing() {
    let dir = tempfile::tempdir().unwrap();
    let loader = MockLoader::new();
    loader.hold.store(true, Ordering::SeqCst);
    let addr = free_addr();
    let e = Arc::new(start_node(node_config(1, &addr, dir.path()), loader.clone()).await);
    e.initialize(vec![(1, addr)]).await.unwrap();
    wait_leader(&e, 1).await;
    // One row fills the table. The node's admission wait is the default (a minute).
    e.attach(table_def(TableConfig { memory_cap_bytes: 1, ..fast_config() }), "mock", 1).await.unwrap();
    e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();
    // The backpressure monitor notices within its 100 ms interval.
    wait_for("writes are held", Duration::from_secs(2), || e.stats(TABLE).unwrap().writes_held).await;
    assert!(e.stats(TABLE).unwrap().rejecting.is_some_and(|r| r.contains("writes are held")));

    // The next writer is held, not refused...
    let started = std::time::Instant::now();
    let writer = tokio::spawn({
        let e = e.clone();
        async move { e.upsert(TABLE, rows(&[(2, "b")])).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!writer.is_finished());
    // ...until Doris takes the buffered row and it is trimmed.
    let _ = e.flush(TABLE, Duration::from_millis(10)).await;
    loader.release();
    let ack = tokio::time::timeout(Duration::from_secs(5), writer).await.expect("writer admitted once there is space").unwrap().unwrap();
    assert!(started.elapsed() >= Duration::from_millis(300));
    assert_eq!(ack.seq, 2);
    assert_eq!(e.get(TABLE, &keys(&[2]), None).unwrap().num_rows(), 1);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_are_delayed_more_as_a_table_fills_up_and_reads_are_not() {
    let dir = tempfile::tempdir().unwrap();
    let loader = MockLoader::new();
    loader.hold.store(true, Ordering::SeqCst); // nothing drains: the table only fills up
    let addr = free_addr();
    let e = start_node(node_config(1, &addr, dir.path()), loader).await;
    e.initialize(vec![(1, addr)]).await.unwrap();
    wait_leader(&e, 1).await;
    let big = "x".repeat(1000);
    let fill = |k: i64| rows(&[(k, big.as_str())]);
    let cap = 1 << 20;
    e.attach(table_def(TableConfig { memory_cap_bytes: cap, ..fast_config() }), "mock", 1).await.unwrap();

    // Below 70% of the cap: no delay.
    e.upsert(TABLE, fill(0)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(e.stats(TABLE).unwrap().write_delay, Duration::ZERO);

    // Fill to between 80% and 85%: the published delay is 40 ms, and a write takes that long.
    let mut k = 1;
    while (e.stats(TABLE).unwrap().buffered_bytes as f64) < 0.81 * cap as f64 {
        e.upsert(TABLE, fill(k)).await.unwrap();
        k += 1;
    }
    wait_for("the delay is published", Duration::from_secs(2), || e.stats(TABLE).unwrap().write_delay >= Duration::from_millis(40)).await;
    let s = e.stats(TABLE).unwrap();
    assert!((0.80..0.85).contains(&(s.buffered_bytes as f64 / cap as f64)), "{s:?}");
    assert_eq!(s.write_delay, Duration::from_millis(40));
    assert!(!s.writes_held && s.rejecting.is_none());
    let started = std::time::Instant::now();
    e.upsert(TABLE, rows(&[(k, "small")])).await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(40), "{:?}", started.elapsed());

    // Reads take no delay.
    let started = std::time::Instant::now();
    for _ in 0..50 {
        assert_eq!(e.get(TABLE, &keys(&[0]), None).unwrap().num_rows(), 1);
    }
    assert!(started.elapsed() < Duration::from_millis(40), "{:?}", started.elapsed());
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_table_write_is_all_or_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let mut cfg = node_config(1, &addr, dir.path());
    cfg.memory.max_admission_wait = Duration::from_millis(50);
    let e = start_node(cfg, MockLoader::new()).await;
    e.initialize(vec![(1, addr)]).await.unwrap();
    wait_leader(&e, 1).await;
    e.attach(table_def(fast_config()), "mock", 1).await.unwrap();
    // A table with no room: a write to it waits, then fails.
    let full = TableConfig { memory_cap_bytes: 0, ..fast_config() };
    e.attach(table_def_named("t2", full), "mock", 1).await.unwrap();
    wait_for("writes to the full table are held", Duration::from_secs(2), || e.stats("db.t2").unwrap().writes_held).await;
    let res = e
        .write(vec![
            WriteOp { table: TABLE.into(), kind: WriteKind::Upsert, batch: rows(&[(1, "a")]) },
            WriteOp { table: "db.t2".into(), kind: WriteKind::Upsert, batch: rows(&[(1, "a")]) },
        ])
        .await;
    assert!(matches!(res, Err(Error::Backpressure { .. })), "{res:?}");
    assert_eq!(e.get(TABLE, &keys(&[1]), None).unwrap().num_rows(), 0);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    {
        let e = single_node_at(dir.path(), &addr, MockLoader::new()).await;
        e.upsert(TABLE, rows(&[(1, "a"), (2, "b")])).await.unwrap();
        e.delete(TABLE, keys(&[1])).await.unwrap();
        e.shutdown().await;
    }
    let loader = MockLoader::new();
    let e = start_node(node_config(1, &addr, dir.path()), loader.clone()).await;
    wait_leader(&e, 1).await;
    wait_for("replay", Duration::from_secs(5), || e.get(TABLE, &keys(&[1, 2]), None).map(|b| b.num_rows() == 2).unwrap_or(false)).await;
    let got = scan_rows(&[e.get(TABLE, &keys(&[1, 2]), None).unwrap()]);
    assert!(got[0].deleted && got[1] == row(2, "b", 2, false), "{got:?}");
    // Sequence numbers continue after the replayed ones.
    assert_eq!(e.upsert(TABLE, rows(&[(3, "c")])).await.unwrap().seq, 4);
    e.flush(TABLE, Duration::from_secs(5)).await.unwrap();
    assert_eq!(loader.loads.lock()[0].1.len(), 3);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_drop_trims_the_wal_without_waiting_for_the_next_periodic_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let mut cfg = node_config(1, &addr, dir.path());
    cfg.raft.snapshot_every_entries = 1_000_000; // never reached here
    cfg.wal_segment_bytes = 4096;
    let e = start_node(cfg.clone(), MockLoader::new()).await;
    e.initialize(vec![(1, addr.clone())]).await.unwrap();
    wait_leader(&e, 1).await;
    e.attach(table_def(TableConfig { grace: Duration::from_millis(50), ..fast_config() }), "mock", 1).await.unwrap();
    for k in 0..100 {
        e.upsert(TABLE, rows(&[(k, "flushed")])).await.unwrap();
    }
    assert_eq!(wal_first_index(dir.path()), 0);
    e.flush(TABLE, Duration::from_secs(5)).await.unwrap();
    wait_for("drop", Duration::from_secs(5), || e.stats(TABLE).unwrap().trimmed_seq == 100).await;
    // Rows written after the drop keep the newest WAL files alive; the flushed ones go.
    for k in 100..110 {
        e.upsert(TABLE, rows(&[(k, "buffered")])).await.unwrap();
    }
    wait_for("wal trimmed", Duration::from_secs(5), || wal_first_index(dir.path()) > 50).await;
    let before = scan_rows(&e.scan(TABLE, ScanOptions::default()).unwrap());
    assert_eq!(before.len(), 10);
    e.shutdown().await;

    // Restart on the trimmed WAL: the buffered rows, the watermarks and the sequence are back,
    // and the node keeps taking writes and trimming.
    let e = start_node(cfg, MockLoader::new()).await;
    wait_leader(&e, 1).await;
    wait_for("rebuilt", Duration::from_secs(5), || e.scan(TABLE, ScanOptions::default()).map(|b| scan_rows(&b).len() == 10).unwrap_or(false)).await;
    assert_eq!(scan_rows(&e.scan(TABLE, ScanOptions::default()).unwrap()), before);
    let s = e.stats(TABLE).unwrap();
    assert_eq!((s.visible_seq, s.trimmed_seq), (100, 100));
    assert_eq!(e.upsert(TABLE, rows(&[(500, "after restart")])).await.unwrap().seq, 111);
    e.flush(TABLE, Duration::from_secs(5)).await.unwrap();
    wait_for("second trim", Duration::from_secs(5), || e.stats(TABLE).unwrap().trimmed_seq == 111).await;
    // Nothing is buffered now, so every WAL file that is full can go: at most the newest remains.
    wait_for("wal trimmed again", Duration::from_secs(5), || {
        let files = std::fs::read_dir(dir.path().join("wal")).unwrap().filter(|f| f.as_ref().unwrap().path().extension().is_some_and(|x| x == "wal"));
        files.count() <= 1
    })
    .await;
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_load_that_hangs_is_abandoned_and_retried() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let doris = MockLoader::new();
    let e = start_node(node_config(1, &addr, dir.path()), doris.clone()).await;
    e.initialize(vec![(1, addr.clone())]).await.unwrap();
    wait_leader(&e, 1).await;
    let table = TableConfig { load_timeout: Duration::from_millis(300), grace: Duration::from_millis(50), ..fast_config() };
    e.attach(table_def(table), "mock", 1).await.unwrap();

    // Doris accepts the load but never reports it visible: the load step would wait forever.
    doris.hold.store(true, Ordering::SeqCst);
    e.upsert(TABLE, rows(&[(1, "a"), (2, "b")])).await.unwrap();
    let _ = e.flush(TABLE, Duration::from_millis(100)).await;
    wait_for("the load is reported as running", Duration::from_secs(5), || e.stats(TABLE).unwrap().load_running.is_some()).await;
    wait_for("the stalled load is given up", Duration::from_secs(5), || {
        e.stats(TABLE).unwrap().last_error.is_some_and(|m| m.contains("will be submitted again"))
    })
    .await;
    assert_eq!(e.stats(TABLE).unwrap().trimmed_seq, 0);

    // Doris recovers: the next attempt finds the same label visible, and the unit is trimmed.
    doris.release();
    wait_for("trim", Duration::from_secs(5), || e.stats(TABLE).unwrap().trimmed_seq == 2).await;
    let labels: Vec<String> = doris.loads.lock().iter().map(|(label, _)| label.clone()).collect();
    assert!(labels.iter().all(|l| *l == labels[0]), "{labels:?}");
    assert!(e.stats(TABLE).unwrap().load_running.is_none());
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn table_settings_can_be_changed_and_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let slow = TableConfig { grace: Duration::from_secs(3600), ..fast_config() };
    let fast = TableConfig { grace: Duration::from_millis(50), memory_cap_bytes: 123 << 20, ..fast_config() };
    {
        let e = start_node(node_config(1, &addr, dir.path()), MockLoader::new()).await;
        e.initialize(vec![(1, addr.clone())]).await.unwrap();
        wait_leader(&e, 1).await;
        e.attach(table_def(slow.clone()), "mock", 1).await.unwrap();
        assert_eq!(e.table_config(TABLE).unwrap(), slow);

        // With an hour of grace the flushed unit stays in memory.
        e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();
        e.flush(TABLE, Duration::from_secs(5)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(e.stats(TABLE).unwrap().grace_rows, 1);

        // A shorter grace takes effect at once, for the unit already waiting.
        e.set_table_config(TABLE, fast.clone()).await.unwrap();
        wait_for("trim under the new grace", Duration::from_secs(5), || e.stats(TABLE).unwrap().trimmed_seq == 1).await;
        let s = e.stats(TABLE).unwrap();
        assert_eq!((s.grace, s.memory_cap_bytes), (Duration::from_millis(50), 123 << 20));
        assert!(e.set_table_config("db.nope", fast.clone()).await.is_err());
        e.shutdown().await;
    }
    let e = start_node(node_config(1, &addr, dir.path()), MockLoader::new()).await;
    wait_leader(&e, 1).await;
    wait_for("table is back", Duration::from_secs(5), || e.table_config(TABLE).is_ok()).await;
    assert_eq!(e.table_config(TABLE).unwrap(), fast);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn watermarks_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    {
        let e = single_node_at(dir.path(), &addr, MockLoader::new()).await;
        e.upsert(TABLE, rows(&[(1, "a"), (2, "b"), (3, "c")])).await.unwrap();
        e.flush(TABLE, Duration::from_secs(5)).await.unwrap();
        wait_for("drop", Duration::from_secs(5), || e.stats(TABLE).unwrap().trimmed_seq == 3).await;
        assert_eq!(e.stats(TABLE).unwrap().visible_seq, 3);
        e.upsert(TABLE, rows(&[(4, "d")])).await.unwrap(); // still buffered at shutdown
        e.shutdown().await;
    }
    let e = start_node(node_config(1, &addr, dir.path()), MockLoader::new()).await;
    wait_leader(&e, 1).await;
    wait_for("replay", Duration::from_secs(5), || e.get(TABLE, &keys(&[4]), None).map(|b| b.num_rows() == 1).unwrap_or(false)).await;
    let s = e.stats(TABLE).unwrap();
    assert!(s.visible_seq >= 3 && s.trimmed_seq >= 3, "{s:?}");
    // The trimmed rows are not in the buffer any more; the later one is.
    assert_eq!(e.get(TABLE, &keys(&[1, 2, 3]), None).unwrap().num_rows(), 0);
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn restart_mid_load_checks_the_label_before_resubmitting() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let doris = MockLoader::new(); // shared "Doris" across both runs
    doris.hold.store(true, Ordering::SeqCst);
    let label = {
        let e = single_node_at(dir.path(), &addr, doris.clone()).await;
        e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();
        let _ = e.flush(TABLE, Duration::from_millis(200)).await;
        assert_eq!(doris.load_count(), 1);
        e.shutdown().await;
        doris.loads.lock()[0].0.clone()
    };
    // Doris finishes the load while doris-ingress is down.
    doris.labels.lock().insert(label.clone(), LoadState::Visible);

    let e = start_node(node_config(1, &addr, dir.path()), doris.clone()).await;
    wait_leader(&e, 1).await;
    wait_for("flush done", Duration::from_secs(5), || {
        e.stats(TABLE).map(|s| s.grace_units == 1 && s.unflushed_units == 0).unwrap_or(false)
    })
    .await;
    assert_eq!(doris.submits.lock().len(), 1, "label was visible; no resubmit");
    e.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshots_trim_the_wal_and_restart_rebuilds_from_references() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let loader = MockLoader::new();
    let mut cfg = node_config(1, &addr, dir.path());
    cfg.raft.snapshot_every_entries = 20;
    cfg.wal_segment_bytes = 4096;
    {
        let e = start_node(cfg.clone(), loader.clone()).await;
        e.initialize(vec![(1, addr.clone())]).await.unwrap();
        wait_leader(&e, 1).await;
        e.attach(table_def(TableConfig { grace: Duration::from_millis(50), ..fast_config() }), "mock", 1).await.unwrap();
        for k in 0..100 {
            e.upsert(TABLE, rows(&[(k, "flushed")])).await.unwrap();
        }
        e.flush(TABLE, Duration::from_secs(5)).await.unwrap();
        wait_for("grace drop", Duration::from_secs(5), || e.stats(TABLE).unwrap().buffered_bytes == 0).await;
        for k in 100..150 {
            e.upsert(TABLE, rows(&[(k, "buffered")])).await.unwrap();
        }
        // Force more snapshots after the drop so the floor moves past the flushed entries.
        for k in 150..200 {
            e.upsert(TABLE, rows(&[(k, "buffered")])).await.unwrap();
        }
        wait_for("wal trimmed", Duration::from_secs(5), || wal_first_index(dir.path()) > 50).await;
        e.shutdown().await;
    }
    let e = start_node(cfg, loader.clone()).await;
    wait_leader(&e, 1).await;
    wait_for("rebuilt", Duration::from_secs(5), || {
        e.scan(TABLE, ScanOptions::default()).map(|b| scan_rows(&b).len() == 100).unwrap_or(false)
    })
    .await;
    let rows = scan_rows(&e.scan(TABLE, ScanOptions::default()).unwrap());
    assert!(rows.iter().all(|r| r.k >= 100 && r.v.as_deref() == Some("buffered")));
    // The watermarks come back from the snapshot: the entries that set them are behind the
    // snapshot and are not applied again (and the WAL holding them has been trimmed).
    let s = e.stats(TABLE).unwrap();
    assert_eq!((s.visible_seq, s.trimmed_seq), (100, 100));
    e.shutdown().await;
}

async fn single_node_at(dir: &std::path::Path, addr: &str, loader: Arc<MockLoader>) -> doris_ingress::Node {
    let e = start_node(node_config(1, addr, dir), loader).await;
    e.initialize(vec![(1, addr.to_string())]).await.unwrap();
    wait_leader(&e, 1).await;
    e.attach(table_def(fast_config()), "mock", 1).await.unwrap();
    e
}

/// Smallest segment start index in the node's WAL directory.
fn wal_first_index(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().to_str()?.strip_suffix(".wal")?.parse().ok())
        .min()
        .unwrap_or(0)
}
