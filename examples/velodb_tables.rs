//! Lists tables in the `doris_ingress_test` database (VELODB_* env), to check tests clean up.
use doris_ingress::doris::MysqlConfig;
use mysql_async::prelude::Queryable;

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
    let t: Vec<String> = c.query("SHOW TABLES FROM doris_ingress_test").await.unwrap();
    println!("tables in doris_ingress_test: {t:?}");
}
