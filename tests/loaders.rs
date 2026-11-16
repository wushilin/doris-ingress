mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use common::*;
use doris_ingress::loader::{BrokerLoadConfig, BrokerLoader, LoadUnit, S3Config, StreamLoadConfig, StreamLoader};
use doris_ingress::SeqTarget;
use parking_lot::Mutex;

const BASIC_AUTH: &str = "Basic cm9vdDpzZWNyZXQ="; // root:secret

/// Minimal FE + BE: the FE redirects loads to the BE; the BE answers with a configurable status.
struct MockDoris {
    base: String,
    load_status: String,
    load_message: String,
    be_requests: Mutex<Vec<(HeaderMap, usize)>>,
    /// Labels of loads the BE accepted.
    accepted: Mutex<Vec<String>>,
    /// `get_load_state` calls for accepted labels.
    state_polls: AtomicU32,
    /// `get_load_state` answers COMMITTED this many times before VISIBLE.
    committed_polls: u32,
}

async fn fe_load(State(d): State<Arc<MockDoris>>) -> Response {
    (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, format!("{}/be/api/db/t/_stream_load", d.base))]).into_response()
}

async fn be_load(State(d): State<Arc<MockDoris>>, headers: HeaderMap, body: Bytes) -> Response {
    if headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) != Some(BASIC_AUTH) {
        return (StatusCode::UNAUTHORIZED, "no auth").into_response();
    }
    let label = headers.get("label").unwrap().to_str().unwrap().to_string();
    d.be_requests.lock().push((headers, body.len()));
    if d.load_status != "Fail" {
        d.accepted.lock().push(label.clone());
    }
    let body = serde_json::json!({
        "TxnId": 7, "Label": label, "Status": d.load_status, "Message": d.load_message,
        "NumberLoadedRows": 2,
    });
    axum::Json(body).into_response()
}

async fn load_state(State(d): State<Arc<MockDoris>>, Query(q): Query<HashMap<String, String>>) -> Response {
    let label = q.get("label").expect("label parameter");
    let state = if !d.accepted.lock().contains(label) {
        "UNKNOWN"
    } else if d.state_polls.fetch_add(1, Ordering::SeqCst) < d.committed_polls {
        "COMMITTED"
    } else {
        "VISIBLE"
    };
    axum::Json(serde_json::json!({"msg": "success", "code": 0, "data": state, "count": 0})).into_response()
}

async fn start_doris(load_status: &str, load_message: &str, committed_polls: u32) -> Arc<MockDoris> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let d = Arc::new(MockDoris {
        base,
        load_status: load_status.into(),
        load_message: load_message.into(),
        be_requests: Mutex::default(),
        accepted: Mutex::default(),
        state_polls: AtomicU32::new(0),
        committed_polls,
    });
    let app = Router::new()
        .route("/api/{db}/{table}/_stream_load", put(fe_load))
        .route("/be/api/{db}/{table}/_stream_load", put(be_load))
        .route("/api/{db}/get_load_state", get(load_state))
        .with_state(d.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    d
}

fn stream_loader(d: &MockDoris) -> Arc<StreamLoader> {
    Arc::new(
        StreamLoader::new(StreamLoadConfig {
            fe_url: d.base.clone(),
            be_url: None,
            user: "root".into(),
            password: "secret".into(),
            timeout: Duration::from_secs(5),
        })
        .unwrap(),
    )
}

fn header<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
    h.get(name).map(|v| v.to_str().unwrap())
}

#[tokio::test]
async fn stream_load_follows_redirect_with_credentials_and_waits_for_visible_label() {
    let doris = start_doris("Publish Timeout", "", 2).await;
    let dir = tempfile::tempdir().unwrap();
    let e = node_with(dir.path(), stream_loader(&doris), table_def(fast_config()), 1).await;
    e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();
    e.delete(TABLE, keys(&[2])).await.unwrap();

    e.flush(TABLE, Duration::from_secs(5)).await.unwrap();

    let reqs = doris.be_requests.lock();
    assert_eq!(reqs.len(), 1);
    let (h, body_len) = &reqs[0];
    assert!(*body_len > 0);
    assert_eq!(header(h, "label"), Some("ingress_db_t_1_2"));
    assert_eq!(header(h, "format"), Some("parquet"));
    assert_eq!(header(h, "columns"), Some("k,v,seq,__ingress_del"));
    assert_eq!(header(h, "merge_type"), Some("MERGE"));
    assert_eq!(header(h, "delete"), Some("__ingress_del=1"));
    assert_eq!(header(h, "function_column.sequence_col"), None, "mapped sequence column needs no header");
    assert!(doris.state_polls.load(Ordering::SeqCst) >= 3, "visible only after the label says so");
}

#[tokio::test]
async fn stream_load_with_hidden_sequence_column_names_it() {
    let doris = start_doris("Success", "", 0).await;
    let dir = tempfile::tempdir().unwrap();
    let def = doris_ingress::TableDef { seq: SeqTarget::Hidden, ..table_def(fast_config()) };
    let e = node_with(dir.path(), stream_loader(&doris), def, 1).await;
    e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();
    e.flush(TABLE, Duration::from_secs(5)).await.unwrap();

    let reqs = doris.be_requests.lock();
    let h = &reqs[0].0;
    assert_eq!(header(h, "columns"), Some("k,v,__ingress_seq,__ingress_del"));
    assert_eq!(header(h, "function_column.sequence_col"), Some("__ingress_seq"));
}

#[tokio::test]
async fn stream_load_failure_is_retried_and_reported() {
    let doris = start_doris("Fail", "too many filtered rows", 0).await;
    let dir = tempfile::tempdir().unwrap();
    let e = node_with(dir.path(), stream_loader(&doris), table_def(fast_config()), 1).await;
    e.upsert(TABLE, rows(&[(1, "a")])).await.unwrap();

    let err = e.flush(TABLE, Duration::from_millis(300)).await.unwrap_err();
    assert!(err.to_string().contains("too many filtered rows"), "{err}");
    let stats = e.stats(TABLE).unwrap();
    assert!(stats.consecutive_failures >= 2, "{stats:?}");
    assert_eq!(doris.state_polls.load(Ordering::SeqCst), 0, "a failed load is never polled as done");
    assert_eq!(stats.grace_units, 0);
    assert_eq!(stats.unflushed_units, 1);
}

#[tokio::test]
async fn broker_load_statement() {
    let loader = BrokerLoader::new(BrokerLoadConfig {
        mysql: doris_ingress::doris::MysqlConfig {
            host: "127.0.0.1".into(),
            port: 9030,
            user: "root".into(),
            password: "secret".into(),
            tls: false,
        },
        s3: S3Config {
            endpoint: "http://minio:9000".into(),
            region: "us-east-1".into(),
            bucket: "staging".into(),
            prefix: "ingress".into(),
            access_key: "AK".into(),
            secret_key: "S'K".into(),
            path_style: true,
        },
        job_timeout_secs: 3600,
    })
    .unwrap();
    let def = Arc::new(doris_ingress::TableDef { seq: SeqTarget::Hidden, ..table_def(fast_config()) });
    let unit = LoadUnit {
        table: def,
        label: "ingress_db_t_1_2".into(),
        columns: vec!["k".into(), "v".into(), "__ingress_seq".into(), "__ingress_del".into()],
        parquet: Default::default(),
        rows: 2,
        seq_start: 1,
        seq_end: 2,
    };
    let sql = loader.load_sql(&unit, "s3://staging/ingress/db/t/ingress_db_t_1_2.parquet");
    for part in [
        "LOAD LABEL `db`.`ingress_db_t_1_2` (",
        "MERGE DATA INFILE('s3://staging/ingress/db/t/ingress_db_t_1_2.parquet') INTO TABLE `t`",
        "FORMAT AS \"parquet\" (`k`, `v`, `__ingress_seq`, `__ingress_del`)",
        "DELETE ON `__ingress_del` = 1 ORDER BY `__ingress_seq`)",
        "\"AWS_SECRET_KEY\" = 'S\\'K'",
        "\"use_path_style\" = 'true'",
        "PROPERTIES (\"timeout\" = \"3600\")",
    ] {
        assert!(sql.contains(part), "missing {part:?} in\n{sql}");
    }
}
