//! Flight SQL endpoint, exercised with arrow-flight's Flight SQL client.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetPrimaryKeys, CommandGetTables, CommandStatementIngest};
use arrow_flight::FlightInfo;
use common::*;
use doris_ingress::flight::{FlightConfig, serve};
use futures_util::TryStreamExt;
use tonic::transport::Channel;

async fn setup() -> (tempfile::TempDir, Arc<doris_ingress::Node>, String) {
    let dir = tempfile::tempdir().unwrap();
    let node = Arc::new(single_node(dir.path(), fast_config(), MockLoader::new()).await);
    let users = HashMap::from([("admin".to_string(), "secret".to_string())]);
    let (addr, _h) = serve(node.clone(), FlightConfig { listen: "127.0.0.1:0".into(), users, linearizable_reads: true }).await.unwrap();
    (dir, node, format!("http://{addr}"))
}

async fn client(url: &str) -> FlightSqlServiceClient<Channel> {
    let channel = Channel::from_shared(url.to_string()).unwrap().connect().await.unwrap();
    FlightSqlServiceClient::new(channel)
}

async fn fetch(c: &mut FlightSqlServiceClient<Channel>, info: FlightInfo) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    for ep in info.endpoint {
        let stream = c.do_get(ep.ticket.unwrap()).await.unwrap();
        out.extend(stream.try_collect::<Vec<_>>().await.unwrap());
    }
    out
}

async fn query(c: &mut FlightSqlServiceClient<Channel>, sql: &str) -> Vec<RecordBatch> {
    let info = c.execute(sql.to_string(), None).await.unwrap();
    fetch(c, info).await
}

fn ingest_cmd() -> CommandStatementIngest {
    CommandStatementIngest { table: "t".into(), schema: Some("db".into()), ..Default::default() }
}

async fn ingest(c: &mut FlightSqlServiceClient<Channel>, batches: Vec<RecordBatch>) -> i64 {
    let stream = futures_util::stream::iter(batches.into_iter().map(Ok));
    c.execute_ingest(ingest_cmd(), stream).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn authentication_is_required() {
    let (_dir, node, url) = setup().await;
    let mut anon = client(&url).await;
    assert!(anon.execute("SELECT 1".into(), None).await.is_err());
    let mut bad = client(&url).await;
    assert!(bad.handshake("admin", "wrong").await.is_err());
    let mut good = client(&url).await;
    good.handshake("admin", "secret").await.unwrap();
    assert_eq!(query(&mut good, "SELECT 1 AS one").await[0].num_rows(), 1);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_upserts_and_deletes_then_sql_reads_them() {
    let (_dir, node, url) = setup().await;
    let mut c = client(&url).await;
    c.handshake("admin", "secret").await.unwrap();

    assert_eq!(ingest(&mut c, vec![rows(&[(1, "a"), (2, "b"), (3, "c")])]).await, 3);

    // A batch with the reserved __delete column: update key 1, delete key 2. Int32 keys are cast.
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("v", DataType::Utf8, true),
        Field::new("__delete", DataType::Boolean, true),
    ]));
    let mixed = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a2"), None])),
            Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
        ],
    )
    .unwrap();
    assert_eq!(ingest(&mut c, vec![mixed]).await, 2);

    // Point lookups (served from the key index) include tombstones.
    let got = query(&mut c, "SELECT k, v, __deleted FROM db.t WHERE k IN (1, 2, 4) ORDER BY k").await;
    let b = arrow::compute::concat_batches(&got[0].schema(), &got).unwrap();
    assert_eq!(b.num_rows(), 2);
    assert_eq!(b.column(0).as_primitive::<Int64Type>().values(), &[1, 2]);
    assert_eq!(b.column(1).as_string::<i32>().value(0), "a2");
    assert!(b.column(1).is_null(1));
    assert_eq!(b.column(2).as_boolean().iter().collect::<Vec<_>>(), vec![Some(false), Some(true)]);

    // Arbitrary SQL over the live rows.
    let got = query(&mut c, "SELECT count(*) AS n, max(__seq) AS s FROM db.t WHERE NOT __deleted").await;
    assert_eq!(got[0].column(0).as_primitive::<Int64Type>().value(0), 2);
    assert_eq!(got[0].column(1).as_primitive::<Int64Type>().value(0), 4);

    // Equality on the key from the other side, and a non-key filter.
    let got = query(&mut c, "SELECT v FROM db.t WHERE 3 = k").await;
    assert_eq!(got[0].column(0).as_string::<i32>().value(0), "c");
    let got = query(&mut c, "SELECT k FROM db.t WHERE v = 'c'").await;
    assert_eq!(got[0].column(0).as_primitive::<Int64Type>().value(0), 3);

    // Writes are visible to the node API too.
    assert_eq!(node.get(TABLE, &keys(&[1]), None).unwrap().num_rows(), 1);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_errors_are_reported() {
    let (_dir, node, url) = setup().await;
    let mut c = client(&url).await;
    c.handshake("admin", "secret").await.unwrap();
    let unknown = CommandStatementIngest { table: "nope".into(), schema: Some("db".into()), ..Default::default() };
    let stream = futures_util::stream::iter(vec![Ok(rows(&[(1, "a")]))]);
    assert!(c.execute_ingest(unknown, stream).await.is_err());
    // Missing the value column: full-row writes only.
    let only_keys = futures_util::stream::iter(vec![Ok(keys(&[1]))]);
    assert!(c.execute_ingest(ingest_cmd(), only_keys).await.is_err());
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn metadata_calls_describe_attached_tables() {
    let (_dir, node, url) = setup().await;
    let mut c = client(&url).await;
    c.handshake("admin", "secret").await.unwrap();

    let info = c.get_catalogs().await.unwrap();
    let cats = fetch(&mut c, info).await;
    assert_eq!(cats[0].column(0).as_string::<i32>().value(0), "ingress");

    let info = c.get_tables(CommandGetTables { include_schema: true, ..Default::default() }).await.unwrap();
    let tables = fetch(&mut c, info).await;
    let t = &tables[0];
    assert_eq!(t.num_rows(), 1);
    assert_eq!(t.column_by_name("db_schema_name").unwrap().as_string::<i32>().value(0), "db");
    assert_eq!(t.column_by_name("table_name").unwrap().as_string::<i32>().value(0), "t");
    let schema_bytes = t.column_by_name("table_schema").unwrap().as_binary::<i32>().value(0).to_vec();
    let schema = arrow::ipc::convert::try_schema_from_ipc_buffer(&schema_bytes).unwrap();
    let names: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["k", "v", "__seq", "__deleted"]);

    let info = c
        .get_primary_keys(CommandGetPrimaryKeys { db_schema: Some("db".into()), table: "t".into(), ..Default::default() })
        .await
        .unwrap();
    let pk = fetch(&mut c, info).await;
    assert_eq!(pk[0].column_by_name("column_name").unwrap().as_string::<i32>().value(0), "k");
    let _ = Int64Array::from(vec![0i64]);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prepared_statements_bind_parameters_and_fast_path_matches_datafusion() {
    let (_dir, node, url) = setup().await;
    let mut c = client(&url).await;
    c.handshake("admin", "secret").await.unwrap();
    ingest(&mut c, vec![rows(&[(1, "a"), (2, "b"), (3, "c")])]).await;
    let del = futures_util::stream::iter(vec![Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
            Field::new("__delete", DataType::Boolean, false),
        ])),
        vec![Arc::new(Int64Array::from(vec![2])), Arc::new(StringArray::from(vec![None::<&str>])), Arc::new(BooleanArray::from(vec![true]))],
    )
    .unwrap())]);
    c.execute_ingest(ingest_cmd(), del).await.unwrap();

    // Prepared statement with a parameter, executed several times with different values.
    let mut p = c.prepare("SELECT k, v, __deleted FROM db.t WHERE k = $1".into(), None).await.unwrap();
    let param_schema = Arc::new(Schema::new(vec![Field::new("$1", DataType::Int64, false)]));
    for (k, expect_v, expect_deleted) in [(1, Some("a"), false), (2, None, true), (3, Some("c"), false)] {
        p.set_parameters(RecordBatch::try_new(param_schema.clone(), vec![Arc::new(Int64Array::from(vec![k]))]).unwrap()).unwrap();
        let info = p.execute().await.unwrap();
        let got = fetch(&mut c, info).await;
        let b = arrow::compute::concat_batches(&got[0].schema(), &got).unwrap();
        assert_eq!(b.num_rows(), 1, "k={k}");
        assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), k);
        assert_eq!(b.column(1).as_string::<i32>().is_valid(0).then(|| b.column(1).as_string::<i32>().value(0)), expect_v);
        assert_eq!(b.column(2).as_boolean().value(0), expect_deleted);
    }
    p.close().await.unwrap();

    // Fast-path shapes give exactly what DataFusion gives for an equivalent non-fast-path query.
    for (fast, slow) in [
        ("SELECT * FROM db.t WHERE k = 1", "SELECT * FROM db.t WHERE k = 1 AND 1 = 1"),
        ("SELECT v, k FROM db.t WHERE k IN (3, 2, 9)", "SELECT v, k FROM db.t WHERE k IN (3, 2, 9) AND 1 = 1"),
        ("SELECT k AS key FROM db.t WHERE 2 = k", "SELECT k AS key FROM db.t WHERE 2 = k AND 1 = 1"),
    ] {
        let a = query(&mut c, fast).await;
        let b = query(&mut c, slow).await;
        let sort = |bs: Vec<RecordBatch>| {
            let b = arrow::compute::concat_batches(&bs[0].schema(), &bs).unwrap();
            let idx = arrow::compute::sort_to_indices(b.column(b.num_columns() - 1), None, None).unwrap();
            arrow::compute::take_record_batch(&b, &idx).unwrap()
        };
        assert_eq!(sort(a), sort(b), "{fast}");
    }
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delete_by_primary_key_in_sql() {
    let (_dir, node, url) = setup().await;
    let mut c = client(&url).await;
    c.handshake("admin", "secret").await.unwrap();
    ingest(&mut c, vec![rows(&[(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"), (7, "g")])]).await;
    let live = async |c: &mut FlightSqlServiceClient<Channel>| {
        let got = query(c, "SELECT k FROM db.t WHERE NOT __deleted ORDER BY k").await;
        got.iter().flat_map(|b| b.column(0).as_primitive::<Int64Type>().values().to_vec()).collect::<Vec<i64>>()
    };

    // As an update statement: `=` and `IN`. The count is the number of keys named.
    assert_eq!(c.execute_update("DELETE FROM db.t WHERE k = 1".into(), None).await.unwrap(), 1);
    assert_eq!(c.execute_update("DELETE FROM db.t WHERE k IN (2, 3)".into(), None).await.unwrap(), 2);
    assert_eq!(live(&mut c).await, vec![4, 5, 6, 7]);

    // As a query (what a SQL console sends): the result is one `count` row.
    let got = query(&mut c, "DELETE FROM db.t WHERE 4 = k").await;
    assert_eq!(got[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0), 1);

    // As a prepared statement, executed with different parameters.
    let mut p = c.prepare("DELETE FROM db.t WHERE k = $1".into(), None).await.unwrap();
    let param_schema = Arc::new(Schema::new(vec![Field::new("$1", DataType::Int64, false)]));
    for k in [5, 6] {
        p.set_parameters(RecordBatch::try_new(param_schema.clone(), vec![Arc::new(Int64Array::from(vec![k]))]).unwrap()).unwrap();
        assert_eq!(p.execute_update().await.unwrap(), 1);
    }
    p.close().await.unwrap();
    assert_eq!(live(&mut c).await, vec![7]);

    // A key that is not in the buffer still gets a tombstone (its row may be in Doris).
    assert_eq!(c.execute_update("DELETE FROM db.t WHERE k = 99".into(), None).await.unwrap(), 1);
    let got = query(&mut c, "SELECT __deleted FROM db.t WHERE k = 99").await;
    assert!(got[0].column(0).as_boolean().value(0));

    // Anything but the primary key alone is refused, and deletes nothing.
    for sql in [
        "DELETE FROM db.t",
        "DELETE FROM db.t WHERE v = 'g'",
        "DELETE FROM db.t WHERE k = 7 AND v = 'g'",
        "DELETE FROM db.t WHERE k = 7 OR k = 8",
        "DELETE FROM db.t WHERE k > 1",
        "DELETE FROM db.t WHERE k NOT IN (1)",
        "DELETE FROM db.t WHERE k = NULL",
        "DELETE FROM db.t WHERE k = 'x'",
        "UPDATE db.t SET v = 'z' WHERE k = 7",
    ] {
        assert!(c.execute_update(sql.into(), None).await.is_err(), "{sql} must be refused as an update");
        let as_query = match c.execute(sql.into(), None).await {
            Ok(info) => match c.do_get(info.endpoint[0].ticket.clone().unwrap()).await {
                Ok(stream) => stream.try_collect::<Vec<_>>().await.map(|_| ()).map_err(|e| e.to_string()),
                Err(e) => Err(e.to_string()),
            },
            Err(e) => Err(e.to_string()),
        };
        assert!(as_query.is_err(), "{sql} must be refused as a query");
    }
    assert_eq!(live(&mut c).await, vec![7]);

    // Composite key: every key column must be named, with `=`.
    let mut def = table_def_named("c", fast_config());
    def.key_columns = vec!["k".into(), "v".into()];
    node.attach(def, "mock", 1).await.unwrap();
    let cmd = CommandStatementIngest { table: "c".into(), schema: Some("db".into()), ..Default::default() };
    c.execute_ingest(cmd, futures_util::stream::iter(vec![Ok(rows(&[(1, "a"), (1, "b"), (2, "a")]))])).await.unwrap();
    assert!(c.execute_update("DELETE FROM db.c WHERE k = 1".into(), None).await.is_err());
    assert!(c.execute_update("DELETE FROM db.c WHERE k IN (1, 2)".into(), None).await.is_err());
    assert_eq!(c.execute_update("DELETE FROM db.c WHERE v = 'b' AND k = 1".into(), None).await.unwrap(), 1);
    let got = query(&mut c, "SELECT k, v FROM db.c WHERE NOT __deleted ORDER BY k").await;
    let b = arrow::compute::concat_batches(&got[0].schema(), &got).unwrap();
    assert_eq!(b.column(0).as_primitive::<Int64Type>().values().to_vec(), vec![1, 2]);
    assert_eq!(b.column(1).as_string::<i32>().iter().collect::<Vec<_>>(), vec![Some("a"), Some("a")]);
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_in_sql_upserts_full_rows() {
    let (_dir, node, url) = setup().await;
    let mut c = client(&url).await;
    c.handshake("admin", "secret").await.unwrap();
    let live = async |c: &mut FlightSqlServiceClient<Channel>| {
        let got = query(c, "SELECT k, v FROM db.t WHERE NOT __deleted ORDER BY k").await;
        let b = arrow::compute::concat_batches(&got[0].schema(), &got).unwrap();
        let (k, v) = (b.column(0).as_primitive::<Int64Type>(), b.column(1).as_string::<i32>());
        (0..b.num_rows()).map(|i| (k.value(i), v.is_valid(i).then(|| v.value(i).to_string()))).collect::<Vec<_>>()
    };
    let s = |v: &str| Some(v.to_string());

    // Without a column list (the hidden columns need no values), and with one in any order.
    assert_eq!(c.execute_update("INSERT INTO db.t VALUES (1, 'a'), (2, 'b')".into(), None).await.unwrap(), 2);
    assert_eq!(c.execute_update("INSERT INTO db.t (v, k) VALUES ('c', 3)".into(), None).await.unwrap(), 1);
    assert_eq!(live(&mut c).await, vec![(1, s("a")), (2, s("b")), (3, s("c"))]);

    // An existing key is replaced, and a column that is left out becomes null.
    assert_eq!(c.execute_update("INSERT INTO ingress.db.t VALUES (1, 'a2')".into(), None).await.unwrap(), 1);
    assert_eq!(c.execute_update("INSERT INTO db.t (k) VALUES (2)".into(), None).await.unwrap(), 1);
    assert_eq!(live(&mut c).await, vec![(1, s("a2")), (2, None), (3, s("c"))]);

    // A deleted key can be inserted again.
    assert_eq!(c.execute_update("DELETE FROM db.t WHERE k = 3".into(), None).await.unwrap(), 1);
    assert_eq!(c.execute_update("INSERT INTO db.t VALUES (3, 'c2')".into(), None).await.unwrap(), 1);

    // As a query: the result is one `count` row. And from a SELECT over the buffer.
    let got = query(&mut c, "INSERT INTO db.t VALUES (4, 'd')").await;
    assert_eq!(got[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0), 1);
    assert_eq!(c.execute_update("INSERT INTO db.t SELECT k + 10, v FROM db.t WHERE k <= 2".into(), None).await.unwrap(), 2);
    assert_eq!(
        live(&mut c).await,
        vec![(1, s("a2")), (2, None), (3, s("c2")), (4, s("d")), (11, s("a2")), (12, None)]
    );

    // As a prepared statement with parameters.
    let mut p = c.prepare("INSERT INTO db.t (k, v) VALUES ($1, $2)".into(), None).await.unwrap();
    let param_schema = Arc::new(Schema::new(vec![Field::new("$1", DataType::Int64, false), Field::new("$2", DataType::Utf8, true)]));
    for (k, v) in [(5, "e"), (1, "a3")] {
        let params = RecordBatch::try_new(param_schema.clone(), vec![Arc::new(Int64Array::from(vec![k])), Arc::new(StringArray::from(vec![v]))]);
        p.set_parameters(params.unwrap()).unwrap();
        assert_eq!(p.execute_update().await.unwrap(), 1);
    }
    p.close().await.unwrap();
    let got = live(&mut c).await;
    assert_eq!(got[0], (1, s("a3")));
    assert!(got.contains(&(5, s("e"))));

    // Refused: the hidden columns, a null or missing key, wrong shapes.
    for sql in [
        "INSERT INTO db.t (k, v, __seq) VALUES (9, 'z', 1)",
        "INSERT INTO db.t (k, v, __deleted) VALUES (9, 'z', true)",
        "INSERT INTO db.t (k, v, __delete) VALUES (9, 'z', true)",
        "INSERT INTO db.t (k, v, \"__SEQ\") VALUES (9, 'z', 1)",
        "INSERT INTO db.t VALUES (9, 'z', 1, false)",
        "INSERT INTO db.t (v) VALUES ('z')",
        "INSERT INTO db.t VALUES (NULL, 'z')",
        "INSERT INTO db.t VALUES (9)",
        "INSERT INTO db.t (k, nope) VALUES (9, 'z')",
        "INSERT INTO db.nope VALUES (9, 'z')",
        "INSERT OVERWRITE db.t VALUES (9, 'z')",
    ] {
        assert!(c.execute_update(sql.into(), None).await.is_err(), "{sql} must be refused");
    }
    assert!(!live(&mut c).await.iter().any(|(k, _)| *k == 9));
    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn only_queries_are_accepted_and_multi_scan_queries_work() {
    let (_dir, node, url) = setup().await;
    let mut c = client(&url).await;
    c.handshake("admin", "secret").await.unwrap();
    ingest(&mut c, vec![rows(&[(1, "a"), (2, "b")])]).await;
    for sql in ["CREATE TABLE x AS SELECT 1", "CREATE VIEW v AS SELECT * FROM db.t", "SET datafusion.execution.batch_size = 1", "DROP TABLE db.t"] {
        assert!(c.execute(sql.into(), None).await.is_err(), "{sql} must be refused");
    }
    // Status tables describe the answering node; an INSERT into one is refused.
    let got = query(&mut c, "SELECT node_id, is_leader, leader_id, tables FROM system.node").await;
    assert_eq!(got[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0), 1);
    assert!(got[0].column(1).as_boolean().value(0));
    assert_eq!(got[0].column(2).as_primitive::<arrow::datatypes::UInt64Type>().value(0), 1);
    assert_eq!(got[0].column(3).as_primitive::<arrow::datatypes::UInt64Type>().value(0), 1);
    let got = query(&mut c, "SELECT name, key_columns, active_rows FROM system.tables WHERE db = 'db'").await;
    assert_eq!(got[0].column(0).as_string::<i32>().value(0), "t");
    assert_eq!(got[0].column(1).as_string::<i32>().value(0), "k");
    assert_eq!(got[0].column(2).as_primitive::<arrow::datatypes::UInt64Type>().value(0), 2);
    assert!(c.execute_update("INSERT INTO system.node (node_id) VALUES (9)".into(), None).await.is_err());
    // Two scans of the same table in one query (read at one snapshot).
    let got = query(&mut c, "SELECT a.k, b.v FROM db.t a JOIN db.t b ON a.k = b.k UNION ALL SELECT k, v FROM db.t").await;
    assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
    node.shutdown().await;
}
