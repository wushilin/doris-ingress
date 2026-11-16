//! A doris-ingress node with a no-op loader and flushing effectively disabled, serving Flight SQL
//! for benchmarks. Table `bench.t` (id BIGINT key, region, amount, ts, note); user admin / bench.
//!
//! Single node:  `flight_bench_server 18816`
//! Cluster node: `flight_bench_server id=1 raft=IP:PORT flight=IP:PORT dir=PATH
//!                peers=1=IP:PORT,2=IP:PORT,3=IP:PORT [init=1]`
//! `init=1` on one node initializes the cluster; the leader attaches `bench.t`.
//! `LINEARIZABLE=0` disables the per-query read barrier.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use doris_ingress::flight::{FlightConfig, serve};
use doris_ingress::loader::{LoadState, LoadUnit, Loader};
use doris_ingress::{Node, NodeConfig, SeqTarget, TableConfig, TableDef};

struct NullLoader;

#[async_trait]
impl Loader for NullLoader {
    fn kind(&self) -> &'static str {
        "null"
    }
    async fn submit(&self, _: &LoadUnit) -> doris_ingress::Result<LoadState> {
        Ok(LoadState::Visible)
    }
    async fn state(&self, _: &TableDef, _: &str) -> doris_ingress::Result<LoadState> {
        Ok(LoadState::Unknown)
    }
}

fn table_def() -> TableDef {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("note", DataType::Utf8, true),
    ]));
    TableDef {
        db: "bench".into(),
        name: "t".into(),
        schema,
        key_columns: vec!["id".into()],
        seq: SeqTarget::Hidden,
        config: TableConfig {
            flush_interval: Duration::from_secs(86_400),
            flush_bytes: usize::MAX,
            max_load_bytes: usize::MAX,
            memory_cap_bytes: usize::MAX,
            ..TableConfig::default()
        },
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let kv: HashMap<String, String> =
        args.iter().filter_map(|a| a.split_once('=')).map(|(k, v)| (k.to_string(), v.to_string())).collect();

    let tmp = tempfile::tempdir().unwrap();
    let (id, raft, flight, dir, peers, init) = if kv.is_empty() {
        let port: u16 = args.first().and_then(|p| p.parse().ok()).unwrap_or(18816);
        let raft = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();
        (1u64, raft.clone(), format!("127.0.0.1:{port}"), tmp.path().to_path_buf(), vec![(1u64, raft)], true)
    } else {
        let peers = kv["peers"]
            .split(',')
            .map(|p| {
                let (i, a) = p.split_once('=').expect("peers=id=addr,...");
                (i.parse::<u64>().unwrap(), a.to_string())
            })
            .collect();
        (
            kv["id"].parse().unwrap(),
            kv["raft"].clone(),
            kv["flight"].clone(),
            kv.get("dir").map(Into::into).unwrap_or_else(|| tmp.path().to_path_buf()),
            peers,
            kv.get("init").is_some_and(|v| v == "1"),
        )
    };

    let mut loaders: HashMap<String, Arc<dyn Loader>> = HashMap::new();
    loaders.insert("null".into(), Arc::new(NullLoader));
    let mut cfg = NodeConfig::new(id, raft.clone(), &dir);
    cfg.memory.target_bytes = 48 << 30;
    cfg.raft.snapshot_every_entries = 100_000;
    if let Some(n) = std::env::var("BATCH_IN_FLIGHT").ok().and_then(|v| v.parse().ok()) {
        cfg.batch.max_in_flight = n;
    }
    if let Some(us) = std::env::var("BATCH_LINGER_US").ok().and_then(|v| v.parse().ok()) {
        cfg.batch.linger = Duration::from_micros(us);
    }
    let node = Arc::new(Node::start(cfg, loaders).await.unwrap());
    if init && let Err(e) = node.initialize(peers).await {
        eprintln!("initialize: {e}");
    }
    let users = HashMap::from([("admin".to_string(), "bench".to_string())]);
    let linearizable_reads = std::env::var("LINEARIZABLE").map(|v| v != "0").unwrap_or(true);
    let (addr, _h) = serve(node.clone(), FlightConfig { listen: flight, users, linearizable_reads }).await.unwrap();
    println!("ready node={id} flight={addr} raft={raft}");

    // Whoever is leader attaches the table once.
    let attacher = node.clone();
    tokio::spawn(async move {
        loop {
            if attacher.is_leader() && attacher.tables().is_empty() {
                match attacher.attach(table_def(), "null", 1).await {
                    Ok(()) => println!("attached bench.t"),
                    Err(e) => eprintln!("attach: {e}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    if std::env::var("APPLY_STATS").is_ok_and(|v| v == "1") {
        let n = node.clone();
        tokio::spawn(async move {
            let mut last = n.apply_stats();
            let mut last_w: Option<(u64, u64, u64, u64, u64)> = None;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = n.apply_stats();
                let w = n.write_stats();
                if let Some(lw) = last_w {
                    let (ds, dsn, dq, dp, dpn): (u64, u64, u64, u64, u64) = (w.0 - lw.0, w.1 - lw.1, w.2 - lw.2, w.3 - lw.3, w.4 - lw.4);
                    if ds > 0 && dp > 0 {
                        println!(
                            "write path: {ds} submits/s avg {:.2} ms (queued {:.2} ms) | {dp} entries/s, commit+apply {:.2} ms/entry",
                            dsn as f64 / ds as f64 / 1e6,
                            dq as f64 / ds as f64 / 1e6,
                            dpn as f64 / dp as f64 / 1e6
                        );
                    }
                }
                last_w = Some(w);
                let (dn, de, dops) = (now.0 - last.0, now.1 - last.1, now.2 - last.2);
                if de > 0 {
                    println!(
                        "apply: busy {:.0}%  {de} entries/s  {dops} ops/s  {:.1} ops/entry  {:.1} us/op",
                        dn as f64 / 1e7,
                        dops as f64 / de as f64,
                        if dops > 0 { dn as f64 / 1e3 / dops as f64 } else { 0.0 }
                    );
                }
                last = now;
            }
        });
    }
    tokio::signal::ctrl_c().await.unwrap();
    node.shutdown().await;
}
