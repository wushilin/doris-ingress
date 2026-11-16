//! Write throughput of a one-node Raft cluster (WAL with fsync) and a loader that accepts
//! everything instantly.
//! `cargo run --release --example write_bench -- <writers> <batch_rows> <total_rows>`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
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

#[tokio::main]
async fn main() {
    let args: Vec<u64> = std::env::args().skip(1).map(|a| a.parse().unwrap()).collect();
    let writers = args.first().copied().unwrap_or(16);
    let batch_rows = args.get(1).copied().unwrap_or(10_000);
    let total = args.get(2).copied().unwrap_or(10_000_000);

    let dir = tempfile::tempdir().unwrap();
    let addr = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().to_string();
    let mut loaders: HashMap<String, Arc<dyn Loader>> = HashMap::new();
    loaders.insert("null".into(), Arc::new(NullLoader));
    let node = Arc::new(Node::start(NodeConfig::new(1, addr.clone(), dir.path()), loaders).await.unwrap());
    node.initialize(vec![(1, addr)]).await.unwrap();
    while !node.is_leader() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false), Field::new("v", DataType::Utf8, true)]));
    let def = TableDef {
        db: "db".into(),
        name: "t".into(),
        schema: schema.clone(),
        key_columns: vec!["k".into()],
        seq: SeqTarget::Hidden,
        config: TableConfig::default(),
    };
    node.attach(def, "null", 1).await.unwrap();

    let per_writer = total / writers;
    let start = Instant::now();
    let mut tasks = Vec::new();
    for w in 0..writers {
        let (node, schema) = (node.clone(), schema.clone());
        tasks.push(tokio::spawn(async move {
            let mut k = (w * per_writer) as i64;
            let end = k + per_writer as i64;
            while k < end {
                let n = batch_rows.min((end - k) as u64) as i64;
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from_iter_values(k..k + n)),
                        Arc::new(StringArray::from_iter_values((k..k + n).map(|i| format!("value-{i:012}")))),
                    ],
                )
                .unwrap();
                node.upsert("db.t", batch).await.unwrap();
                k += n;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let secs = start.elapsed().as_secs_f64();
    let rows = per_writer * writers;
    println!(
        "{rows} rows in {secs:.2}s = {:.2} M rows/s (writers={writers}, batch={batch_rows}); raft entries {}; buffered {} MB",
        rows as f64 / secs / 1e6,
        node.raft_last_log_index(),
        node.memory_used() >> 20
    );
    node.shutdown().await;
}
