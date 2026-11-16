//! Runs SQL statements (one per argument) against the VELODB_* cluster and prints result rows.
use doris_ingress::doris::MysqlConfig;
use mysql_async::prelude::Queryable;
use mysql_async::{Row, Value};

#[tokio::main]
async fn main() {
    let env = |k: &str| std::env::var(k).unwrap();
    let pool = MysqlConfig {
        host: env("VELODB_HOST"),
        port: env("VELODB_MYSQL_PORT").parse().unwrap(),
        user: env("VELODB_USER"),
        password: env("VELODB_PASSWORD"),
        tls: true,
    }
    .pool();
    let mut c = pool.get_conn().await.unwrap();
    for sql in std::env::args().skip(1) {
        let rows: Vec<Row> = c.query(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        for r in rows {
            let cells: Vec<String> = (0..r.len())
                .map(|i| match r.as_ref(i) {
                    Some(Value::NULL) | None => "NULL".into(),
                    Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
                    Some(v) => format!("{v:?}"),
                })
                .collect();
            println!("{}", cells.join(" | "));
        }
    }
}
