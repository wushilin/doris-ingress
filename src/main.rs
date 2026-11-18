//! doris-ingress server: `doris-ingress <config.toml>`. See `config.example.toml`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use doris_ingress::doris::{MysqlConfig, describe_table, max_seq};
use doris_ingress::flight::{FlightConfig, serve};
use doris_ingress::loader::{BrokerLoadConfig, BrokerLoader, Loader, S3Config, SdkLoadConfig, SdkLoader, StreamLoadConfig, StreamLoader};
use doris_ingress::{Node, NodeConfig, TableConfig};
use serde::Deserialize;

#[derive(Deserialize)]
struct Config {
    node_id: u64,
    data_dir: PathBuf,
    raft_listen: String,
    /// Address other nodes use for this node's Raft endpoint (default: `raft_listen`).
    raft_advertise: Option<String>,
    flight_listen: String,
    /// Confirm leadership with a quorum before each query (default true).
    #[serde(default = "default_true")]
    linearizable_reads: bool,
    /// `[[id, "raft address"], ...]`. On first start, the node with the lowest id initializes the
    /// cluster with these voters. Ignored once the cluster exists.
    #[serde(default)]
    initial_members: Vec<(u64, String)>,
    memory_target_mb: Option<usize>,
    /// How long a write waits for buffer space before it fails with a retryable error
    /// (default 60).
    max_admission_wait_s: Option<u64>,
    /// A WAL file is closed and a new one started at this size (default 100).
    wal_segment_mb: Option<u64>,
    /// A follower silent for this long loses its hold on the leader's log (default 30).
    catch_up_idle_s: Option<u64>,
    /// Most WAL the leader keeps for followers that are behind (default 2048).
    catch_up_hold_mb: Option<u64>,
    /// Most megabytes per second one snapshot send to a follower may take (default 200; 0: no
    /// limit).
    snapshot_send_mb_per_s: Option<u64>,
    max_concurrent_loads: Option<usize>,
    /// Flight SQL users: name → password (`env:VAR` reads the password from the environment).
    users: HashMap<String, String>,
    doris: DorisConfig,
    #[serde(default)]
    tables: Vec<TableToml>,
}

#[derive(Deserialize)]
struct DorisConfig {
    /// FE HTTP(S) URL for Stream Load, e.g. `https://fe:8080`.
    http_url: String,
    /// Optional BE URL to load to directly (avoids the FE redirect re-upload).
    be_http_url: Option<String>,
    mysql_host: String,
    #[serde(default = "default_mysql_port")]
    mysql_port: u16,
    user: String,
    /// Plain text or `env:VAR`.
    password: String,
    #[serde(default)]
    tls: bool,
    s3: Option<S3Toml>,
    /// Settings of the `stream_sdk` loader.
    #[serde(default)]
    sdk: SdkToml,
}

/// `[doris.sdk]`: Stream Load through the `doris_rust_stream_load` SDK (CSV rows, several workers).
#[derive(Deserialize, Default)]
struct SdkToml {
    /// Concurrent uploads per table (default 4).
    workers: Option<usize>,
    /// How long an open batch waits for more rows (default 5).
    linger_ms: Option<u64>,
    /// Largest request body before compression, at most 90 (default 80).
    batch_mb: Option<usize>,
    /// Compress request bodies with gzip (default true).
    gzip: Option<bool>,
    /// Send nothing to Doris; answer every upload as a success after `dry_run_delay_ms`
    /// (default false). Flushed rows are discarded.
    dry_run: Option<bool>,
    /// Simulated time for one upload in a dry run (default 500).
    dry_run_delay_ms: Option<u64>,
}

fn default_true() -> bool {
    true
}

fn default_mysql_port() -> u16 {
    9030
}

#[derive(Deserialize)]
struct S3Toml {
    endpoint: String,
    region: String,
    bucket: String,
    #[serde(default)]
    prefix: String,
    access_key: String,
    secret_key: String,
    #[serde(default)]
    path_style: bool,
}

#[derive(Deserialize)]
struct TableToml {
    /// `db.table`
    name: String,
    /// `stream` (default), `stream_sdk` or `broker`.
    #[serde(default = "default_loader")]
    loader: String,
    flush_interval_ms: Option<u64>,
    flush_mb: Option<usize>,
    max_load_mb: Option<usize>,
    grace_ms: Option<u64>,
    memory_cap_mb: Option<usize>,
    /// A load not visible after this many seconds is abandoned and submitted again (default 120).
    load_timeout_s: Option<u64>,
}

fn default_loader() -> String {
    "stream".into()
}

fn secret(v: &str) -> anyhow::Result<String> {
    match v.strip_prefix("env:") {
        Some(var) => std::env::var(var).map_err(|_| anyhow::anyhow!("environment variable {var} is not set")),
        None => Ok(v.to_string()),
    }
}

impl TableToml {
    fn config(&self) -> TableConfig {
        let mut c = TableConfig::default();
        if let Some(v) = self.flush_interval_ms {
            c.flush_interval = Duration::from_millis(v);
        }
        if let Some(v) = self.flush_mb {
            c.flush_bytes = v << 20;
        }
        if let Some(v) = self.max_load_mb {
            c.max_load_bytes = v << 20;
        }
        if let Some(v) = self.grace_ms {
            c.grace = Duration::from_millis(v);
        }
        if let Some(v) = self.load_timeout_s {
            c.load_timeout = Duration::from_secs(v);
        }
        if let Some(v) = self.memory_cap_mb {
            c.memory_cap_bytes = v << 20;
        }
        c
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let path = std::env::args().nth(1).ok_or_else(|| anyhow::anyhow!("usage: doris-ingress <config.toml>"))?;
    let cfg: Config = toml::from_str(&std::fs::read_to_string(&path)?)?;

    let mysql = MysqlConfig {
        host: cfg.doris.mysql_host.clone(),
        port: cfg.doris.mysql_port,
        user: cfg.doris.user.clone(),
        password: secret(&cfg.doris.password)?,
        tls: cfg.doris.tls,
    };
    let mut loaders: HashMap<String, Arc<dyn Loader>> = HashMap::new();
    let stream = StreamLoadConfig {
        fe_url: cfg.doris.http_url.clone(),
        be_url: cfg.doris.be_http_url.clone(),
        user: mysql.user.clone(),
        password: mysql.password.clone(),
        timeout: Duration::from_secs(600),
    };
    loaders.insert("stream".into(), Arc::new(StreamLoader::new(stream.clone())?));
    let mut sdk = SdkLoadConfig::new(stream);
    if let Some(v) = cfg.doris.sdk.workers {
        sdk.workers = v;
    }
    if let Some(v) = cfg.doris.sdk.linger_ms {
        sdk.linger = Duration::from_millis(v);
    }
    if let Some(v) = cfg.doris.sdk.batch_mb {
        sdk.batch_bytes = v << 20;
    }
    if let Some(v) = cfg.doris.sdk.gzip {
        sdk.gzip = v;
    }
    if let Some(v) = cfg.doris.sdk.dry_run {
        sdk.dry_run = v;
    }
    if let Some(v) = cfg.doris.sdk.dry_run_delay_ms {
        sdk.dry_run_delay = Duration::from_millis(v);
    }
    loaders.insert("stream_sdk".into(), Arc::new(SdkLoader::new(sdk)?));
    if let Some(s3) = &cfg.doris.s3 {
        loaders.insert(
            "broker".into(),
            Arc::new(BrokerLoader::new(BrokerLoadConfig {
                mysql: mysql.clone(),
                s3: S3Config {
                    endpoint: s3.endpoint.clone(),
                    region: s3.region.clone(),
                    bucket: s3.bucket.clone(),
                    prefix: s3.prefix.clone(),
                    access_key: secret(&s3.access_key)?,
                    secret_key: secret(&s3.secret_key)?,
                    path_style: s3.path_style,
                },
                job_timeout_secs: 3600,
            })?),
        );
    }

    let mut node_cfg = NodeConfig::new(cfg.node_id, cfg.raft_listen.clone(), cfg.data_dir.clone());
    node_cfg.raft_advertise = cfg.raft_advertise.clone().unwrap_or_else(|| cfg.raft_listen.clone());
    if let Some(mb) = cfg.memory_target_mb {
        node_cfg.memory.target_bytes = mb << 20;
    }
    if let Some(s) = cfg.max_admission_wait_s {
        node_cfg.memory.max_admission_wait = Duration::from_secs(s);
    }
    if let Some(s) = cfg.catch_up_idle_s {
        node_cfg.raft.catch_up_idle = Duration::from_secs(s);
    }
    if let Some(mb) = cfg.snapshot_send_mb_per_s {
        node_cfg.raft.snapshot_send_bytes_per_sec = mb << 20;
    }
    if let Some(mb) = cfg.catch_up_hold_mb {
        node_cfg.raft.catch_up_hold_bytes = mb << 20;
    }
    if let Some(mb) = cfg.wal_segment_mb {
        node_cfg.wal_segment_bytes = mb << 20;
    }
    if let Some(n) = cfg.max_concurrent_loads {
        node_cfg.max_concurrent_loads = n;
    }
    let node = Arc::new(Node::start(node_cfg, loaders).await?);
    let r = node.wal_recovery();
    tracing::info!(segments = r.segments, entries = r.entries, torn_tail_bytes = r.truncated_tail_bytes, "WAL opened");

    if let Some(min_id) = cfg.initial_members.iter().map(|m| m.0).min()
        && min_id == cfg.node_id
    {
        match node.initialize(cfg.initial_members.clone()).await {
            Ok(()) => tracing::info!("cluster initialized"),
            Err(e) => tracing::info!(reason = %e, "cluster not initialized (already initialized?)"),
        }
    }

    let users = cfg.users.iter().map(|(u, p)| Ok((u.clone(), secret(p)?))).collect::<anyhow::Result<HashMap<_, _>>>()?;
    let (flight_addr, flight) = serve(node.clone(), FlightConfig { listen: cfg.flight_listen.clone(), users, linearizable_reads: cfg.linearizable_reads }).await?;
    tracing::info!(%flight_addr, raft = %cfg.raft_listen, "doris-ingress node {} started", cfg.node_id);

    // The leader attaches configured tables, reading schema and sequence floor from Doris.
    let attach = tokio::spawn({
        let node = node.clone();
        let pool = mysql.pool();
        let tables = cfg.tables;
        async move {
            loop {
                if node.is_leader() {
                    let attached: Vec<String> = node.tables().iter().map(|t| t.full_name()).collect();
                    // Settings changed in the config file are applied to tables already attached.
                    for t in tables.iter().filter(|t| attached.contains(&t.name)) {
                        if node.table_config(&t.name).is_ok_and(|current| current != t.config()) {
                            match node.set_table_config(&t.name, t.config()).await {
                                Ok(()) => tracing::info!(table = %t.name, settings = ?t.config(), "table settings updated"),
                                Err(e) => tracing::warn!(table = %t.name, error = %e, "updating table settings failed; will retry"),
                            }
                        }
                    }
                    for t in tables.iter().filter(|t| !attached.contains(&t.name)) {
                        let Some((db, name)) = t.name.split_once('.') else {
                            tracing::error!(table = %t.name, "table name must be db.table");
                            continue;
                        };
                        let result = async {
                            let def = describe_table(&pool, db, name, t.config()).await?;
                            let floor = max_seq(&pool, &def).await? + 1;
                            node.attach(def, &t.loader, floor).await
                        }
                        .await;
                        match result {
                            Ok(()) => tracing::info!(table = %t.name, loader = %t.loader, "table attached"),
                            Err(e) => tracing::warn!(table = %t.name, error = %e, "attach failed; will retry"),
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    attach.abort();
    flight.abort();
    node.shutdown().await;
    Ok(())
}
