//! Integration tests against a real Doris / VeloDB cluster.
//! Run when `VELODB_HOST`, `VELODB_USER`, `VELODB_PASSWORD` are set (Stream Load over HTTPS on
//! `VELODB_STREAM_LOAD_PORT`, SQL over `VELODB_MYSQL_PORT`, TLS if `VELODB_TLS=true`).
//! Each test creates and drops its own tables in database `doris_ingress_test`.

mod common;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Int8Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use common::*;
use doris_ingress::doris::{MysqlConfig, max_seq};
use doris_ingress::loader::{LoadState, LoadUnit, Loader, StreamLoadConfig, StreamLoader};
use doris_ingress::{SeqTarget, TableDef};
use mysql_async::Pool;
use mysql_async::prelude::Queryable;
use parquet::arrow::ArrowWriter;

const DB: &str = "doris_ingress_test";

struct Velo {
    pool: Pool,
    stream: StreamLoadConfig,
}

fn velo() -> Option<Velo> {
    let host = std::env::var("VELODB_HOST").ok()?;
    let user = std::env::var("VELODB_USER").ok()?;
    let password = std::env::var("VELODB_PASSWORD").ok()?;
    let http_port = std::env::var("VELODB_STREAM_LOAD_PORT").unwrap_or_else(|_| "8080".into());
    let mysql_port = std::env::var("VELODB_MYSQL_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(9030);
    let tls = std::env::var("VELODB_TLS").map(|v| v == "true").unwrap_or(true);
    let mysql = MysqlConfig { host: host.clone(), port: mysql_port, user: user.clone(), password: password.clone(), tls };
    Some(Velo {
        pool: mysql.pool(),
        stream: StreamLoadConfig {
            fe_url: format!("https://{host}:{http_port}"),
            be_url: None,
            user,
            password,
            timeout: Duration::from_secs(60),
        },
    })
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos % 1_000_000_000)
}

async fn create_table(v: &Velo, name: &str, seq: &SeqTarget) -> TableDef {
    let mut conn = v.pool.get_conn().await.unwrap();
    conn.query_drop(format!("CREATE DATABASE IF NOT EXISTS {DB}")).await.unwrap();
    let (seq_col, seq_prop) = match seq {
        SeqTarget::Column(c) => (format!(", `{c}` BIGINT NULL"), format!("\"function_column.sequence_col\" = \"{c}\"")),
        SeqTarget::Hidden => (String::new(), "\"function_column.sequence_type\" = \"bigint\"".to_string()),
    };
    conn.query_drop(format!(
        "CREATE TABLE {DB}.`{name}` (`k` BIGINT NOT NULL, `v` VARCHAR(64) NULL{seq_col}) \
         UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1 \
         PROPERTIES (\"enable_unique_key_merge_on_write\" = \"true\", {seq_prop})"
    ))
    .await
    .unwrap();
    TableDef { db: DB.into(), name: name.into(), seq: seq.clone(), ..table_def(fast_config()) }
}

async fn drop_table(v: &Velo, name: &str) {
    let mut conn = v.pool.get_conn().await.unwrap();
    let _ = conn.query_drop(format!("DROP TABLE IF EXISTS {DB}.`{name}` FORCE")).await;
}

async fn doris_rows(v: &Velo, name: &str) -> Vec<(i64, Option<String>)> {
    let mut conn = v.pool.get_conn().await.unwrap();
    conn.query(format!("SELECT k, v FROM {DB}.`{name}` ORDER BY k")).await.unwrap()
}

/// A hand-built load unit with exact seq/delete values, to replay old data at Doris.
fn raw_unit(def: &TableDef, label: &str, data: &[(i64, Option<&str>, i64, bool)]) -> LoadUnit {
    let seq_col = def.load_seq_column().to_string();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new(&seq_col, DataType::Int64, false),
        Field::new("__ingress_del", DataType::Int8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(data.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter(data.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter_values(data.iter().map(|r| r.2))),
            Arc::new(Int8Array::from_iter_values(data.iter().map(|r| r.3 as i8))),
        ],
    )
    .unwrap();
    let mut w = ArrowWriter::try_new(Vec::new(), schema, None).unwrap();
    w.write(&batch).unwrap();
    LoadUnit {
        table: Arc::new(def.clone()),
        label: label.into(),
        columns: vec!["k".into(), "v".into(), seq_col, "__ingress_del".into()],
        parquet: w.into_inner().unwrap().into(),
        rows: data.len(),
        seq_start: 0,
        seq_end: 0,
    }
}

async fn stream_load_end_to_end(seq: SeqTarget) {
    let Some(v) = velo() else {
        eprintln!("VELODB_* not set; skipping");
        return;
    };
    let name = unique(if seq == SeqTarget::Hidden { "hidden" } else { "mapped" });
    let def = create_table(&v, &name, &seq).await;
    let full = def.full_name();
    let v = Arc::new(v);
    let (v2, name2) = (v.clone(), name.clone());
    let result = async move {
        let (v, name) = (v2, name2);
        let loader = Arc::new(StreamLoader::new(v.stream.clone()).unwrap());
        let dir = tempfile::tempdir().unwrap();
        let start = max_seq(&v.pool, &def).await.unwrap() + 1;
        let e = node_with(dir.path(), loader.clone(), def.clone(), start).await;

        // Upserts, an update and a delete in one flush unit.
        e.upsert(&full, rows(&[(1, "a"), (2, "b"), (3, "c")])).await.unwrap();
        e.upsert(&full, rows(&[(1, "a2")])).await.unwrap();
        e.delete(&full, keys(&[2])).await.unwrap();
        e.flush(&full, Duration::from_secs(60)).await.unwrap();
        assert_eq!(doris_rows(&v, &name).await, vec![(1, Some("a2".into())), (3, Some("c".into()))]);
        // Includes the tombstone of key 2 (the 5th seq), which is hidden from normal queries.
        assert_eq!(max_seq(&v.pool, &def).await.unwrap(), start + 4, "sequence values written to Doris");

        // A second flush unit: delete wins over an older version already in Doris.
        e.delete(&full, keys(&[3])).await.unwrap();
        e.upsert(&full, rows(&[(4, "d")])).await.unwrap();
        e.flush(&full, Duration::from_secs(60)).await.unwrap();
        assert_eq!(doris_rows(&v, &name).await, vec![(1, Some("a2".into())), (4, Some("d".into()))]);

        // O3: replaying an older insert under a new label must not resurrect a deleted key,
        // nor overwrite a newer version.
        let replay = raw_unit(&def, &unique("replay"), &[(2, Some("stale"), 2, false), (1, Some("stale"), 1, false)]);
        assert_eq!(loader.submit(&replay).await.unwrap(), LoadState::Visible);
        assert_eq!(doris_rows(&v, &name).await, vec![(1, Some("a2".into())), (4, Some("d".into()))]);

        // Resubmitting a finished label is reported as visible, not as a failure.
        assert_eq!(loader.submit(&replay).await.unwrap(), LoadState::Visible);
        assert_eq!(loader.state(&def, &replay.label).await.unwrap(), LoadState::Visible);
    };
    let outcome = tokio::spawn(result).await;
    drop_table(&v, &name).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic.into_panic());
    }
}

#[tokio::test]
async fn velodb_stream_load_mapped_sequence_column() {
    stream_load_end_to_end(SeqTarget::Column("seq".into())).await;
}

#[tokio::test]
async fn velodb_stream_load_hidden_sequence_column() {
    stream_load_end_to_end(SeqTarget::Hidden).await;
}
