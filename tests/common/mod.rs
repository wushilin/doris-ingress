#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use arrow::array::{Array, AsArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int8Type, Int64Type, Schema, SchemaRef};
use async_trait::async_trait;
use doris_ingress::loader::{LoadState, LoadUnit, Loader};
use doris_ingress::{Node, NodeConfig, SeqTarget, TableConfig, TableDef};
use parking_lot::Mutex;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

pub const TABLE: &str = "db.t";

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ]))
}

pub fn fast_config() -> TableConfig {
    TableConfig {
        flush_interval: Duration::from_secs(3600),
        grace: Duration::from_millis(200),
        retry_backoff: Duration::from_millis(20),
        poll_interval: Duration::from_millis(10),
        ..TableConfig::default()
    }
}

pub fn table_def(config: TableConfig) -> TableDef {
    table_def_named("t", config)
}

pub fn table_def_named(name: &str, config: TableConfig) -> TableDef {
    TableDef {
        db: "db".into(),
        name: name.into(),
        schema: schema(),
        key_columns: vec!["k".into()],
        seq: SeqTarget::Column("seq".into()),
        config,
    }
}

pub fn rows(data: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(data.iter().map(|(k, _)| *k))),
            Arc::new(StringArray::from_iter_values(data.iter().map(|(_, v)| *v))),
        ],
    )
    .unwrap()
}

pub fn keys(ks: &[i64]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ks.to_vec()))]).unwrap()
}

/// A row as seen in scan or load output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub k: i64,
    pub v: Option<String>,
    pub seq: i64,
    pub deleted: bool,
}

/// Decodes scan-schema batches (k, v, __seq, __deleted), sorted by key.
pub fn scan_rows(batches: &[RecordBatch]) -> Vec<Row> {
    decode(batches, "__seq", |b, i| b.column_by_name("__deleted").unwrap().as_boolean().value(i))
}

/// Decodes load batches (k, v, seq, __ingress_del), sorted by key.
pub fn load_rows(batches: &[RecordBatch]) -> Vec<Row> {
    decode(batches, "seq", |b, i| b.column_by_name("__ingress_del").unwrap().as_primitive::<Int8Type>().value(i) == 1)
}

fn decode(batches: &[RecordBatch], seq_col: &str, deleted: impl Fn(&RecordBatch, usize) -> bool) -> Vec<Row> {
    let mut out = Vec::new();
    for b in batches {
        let k = b.column_by_name("k").unwrap().as_primitive::<Int64Type>();
        let v = b.column_by_name("v").unwrap().as_string::<i32>();
        let seq = b.column_by_name(seq_col).unwrap().as_primitive::<Int64Type>();
        for i in 0..b.num_rows() {
            out.push(Row {
                k: k.value(i),
                v: v.is_valid(i).then(|| v.value(i).to_string()),
                seq: seq.value(i),
                deleted: deleted(b, i),
            });
        }
    }
    out.sort_by_key(|r| r.k);
    out
}

pub fn decode_parquet(bytes: &bytes::Bytes) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

/// In-memory stand-in for Doris.
#[derive(Default)]
pub struct MockLoader {
    /// Accepted loads: label and decoded rows.
    pub loads: Mutex<Vec<(String, Vec<Row>)>>,
    /// Every submit call's label, including failed ones.
    pub submits: Mutex<Vec<String>>,
    pub labels: Mutex<HashMap<String, LoadState>>,
    /// Number of upcoming submits to fail.
    pub fail_next: AtomicU32,
    /// Keep accepted loads in `Running` until `release` is called.
    pub hold: AtomicBool,
}

impl MockLoader {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn release(&self) {
        self.hold.store(false, Ordering::SeqCst);
        for s in self.labels.lock().values_mut() {
            if *s == LoadState::Running {
                *s = LoadState::Visible;
            }
        }
    }

    pub fn load_count(&self) -> usize {
        self.loads.lock().len()
    }
}

#[async_trait]
impl Loader for MockLoader {
    fn kind(&self) -> &'static str {
        "mock"
    }

    async fn submit(&self, unit: &LoadUnit) -> doris_ingress::Result<LoadState> {
        self.submits.lock().push(unit.label.clone());
        if self.fail_next.load(Ordering::SeqCst) > 0 {
            self.fail_next.fetch_sub(1, Ordering::SeqCst);
            return Ok(LoadState::Failed("injected failure".into()));
        }
        if let Some(existing) = self.labels.lock().get(&unit.label) {
            return Ok(existing.clone());
        }
        let rows = load_rows(&decode_parquet(&unit.parquet));
        assert_eq!(rows.len(), unit.rows);
        self.loads.lock().push((unit.label.clone(), rows));
        let state = if self.hold.load(Ordering::SeqCst) { LoadState::Running } else { LoadState::Visible };
        self.labels.lock().insert(unit.label.clone(), state.clone());
        Ok(state)
    }

    async fn state(&self, _table: &TableDef, label: &str) -> doris_ingress::Result<LoadState> {
        Ok(self.labels.lock().get(label).cloned().unwrap_or(LoadState::Unknown))
    }
}

/// Polls `cond` every 10 ms until it holds, panicking after `timeout`.
pub async fn wait_for(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A free localhost port (bound then released).
pub fn free_addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().to_string()
}

pub fn node_config(id: u64, addr: &str, dir: &std::path::Path) -> NodeConfig {
    let mut cfg = NodeConfig::new(id, addr, dir);
    cfg.raft.heartbeat_interval = Duration::from_millis(50);
    cfg.raft.election_timeout_min = Duration::from_millis(300);
    cfg.raft.election_timeout_max = Duration::from_millis(600);
    cfg
}

pub async fn start_node(cfg: NodeConfig, loader: Arc<MockLoader>) -> Node {
    let mut loaders: HashMap<String, Arc<dyn Loader>> = HashMap::new();
    loaders.insert("mock".into(), loader);
    Node::start(cfg, loaders).await.unwrap()
}

/// A one-node cluster with table `db.t` attached (sequence starting at 1).
pub async fn single_node(dir: &std::path::Path, table: TableConfig, loader: Arc<MockLoader>) -> Node {
    let addr = free_addr();
    let node = start_node(node_config(1, &addr, dir), loader).await;
    node.initialize(vec![(1, addr)]).await.unwrap();
    wait_leader(&node, 1).await;
    node.attach(table_def(table), "mock", 1).await.unwrap();
    node
}

pub async fn wait_leader(node: &Node, id: u64) {
    wait_for("leader", Duration::from_secs(10), || node.current_leader() == Some(id) && node.is_leader() == (node.id() == id)).await;
    if node.id() == id {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while node.read_barrier().await.is_err() {
            assert!(std::time::Instant::now() < deadline, "leadership never confirmed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// A one-node cluster using `loader` for the given table.
pub async fn node_with(dir: &std::path::Path, loader: Arc<dyn Loader>, def: TableDef, min_next_seq: u64) -> Node {
    let addr = free_addr();
    let mut loaders: HashMap<String, Arc<dyn Loader>> = HashMap::new();
    loaders.insert("l".into(), loader);
    let node = Node::start(node_config(1, &addr, dir), loaders).await.unwrap();
    node.initialize(vec![(1, addr)]).await.unwrap();
    wait_leader(&node, 1).await;
    node.attach(def, "l", min_next_seq).await.unwrap();
    node
}
