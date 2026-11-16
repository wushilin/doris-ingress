use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{Pool, Row};
use object_store::{ObjectStore, ObjectStoreExt};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;

use super::{LoadState, LoadUnit, Loader, quote_ident, quote_str};
use crate::doris::MysqlConfig;
use crate::error::{Error, Result};
use crate::schema::{LOAD_DELETE_COLUMN, SeqTarget, TableDef};

/// S3-compatible storage that both doris-ingress and Doris can reach.
#[derive(Clone, Debug)]
pub struct S3Config {
    /// e.g. `https://s3.ap-southeast-1.amazonaws.com` or a MinIO URL.
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    /// Key prefix for staging files; files are deleted once the load is done.
    pub prefix: String,
    pub access_key: String,
    pub secret_key: String,
    /// Path-style addressing (MinIO and most self-hosted stores).
    pub path_style: bool,
}

#[derive(Clone, Debug)]
pub struct BrokerLoadConfig {
    /// FE MySQL-protocol connection, used to submit and track jobs.
    pub mysql: MysqlConfig,
    pub s3: S3Config,
    /// Doris-side job timeout.
    pub job_timeout_secs: u64,
}

/// Loads units with Broker Load: upload the Parquet file to S3, then `LOAD LABEL … WITH S3`.
pub struct BrokerLoader {
    cfg: BrokerLoadConfig,
    store: Arc<dyn ObjectStore>,
    pool: Pool,
}

impl BrokerLoader {
    pub fn new(cfg: BrokerLoadConfig) -> Result<Self> {
        let s3 = &cfg.s3;
        let store = AmazonS3Builder::new()
            .with_endpoint(&s3.endpoint)
            .with_region(&s3.region)
            .with_bucket_name(&s3.bucket)
            .with_access_key_id(&s3.access_key)
            .with_secret_access_key(&s3.secret_key)
            .with_virtual_hosted_style_request(!s3.path_style)
            .with_allow_http(s3.endpoint.starts_with("http://"))
            .build()
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { store: Arc::new(store), pool: cfg.mysql.pool(), cfg })
    }

    fn object_path(&self, table: &TableDef, label: &str) -> Path {
        let prefix = self.cfg.s3.prefix.trim_matches('/');
        let rel = format!("{}/{}/{label}.parquet", table.db, table.name);
        if prefix.is_empty() { Path::from(rel) } else { Path::from(format!("{prefix}/{rel}")) }
    }

    /// The `LOAD LABEL` statement for a unit whose file is at `s3_uri`.
    pub fn load_sql(&self, unit: &LoadUnit, s3_uri: &str) -> String {
        let t = &unit.table;
        let cols: Vec<String> = unit.columns.iter().map(|c| quote_ident(c)).collect();
        let order_by = match t.seq {
            SeqTarget::Hidden => format!(" ORDER BY {}", quote_ident(t.load_seq_column())),
            SeqTarget::Column(_) => String::new(),
        };
        let s3 = &self.cfg.s3;
        format!(
            "LOAD LABEL {db}.{label} (\
             MERGE DATA INFILE({uri}) INTO TABLE {table} FORMAT AS \"parquet\" ({cols}) \
             DELETE ON {del} = 1{order_by}) \
             WITH S3 (\"AWS_ENDPOINT\" = {ep}, \"AWS_REGION\" = {region}, \
             \"AWS_ACCESS_KEY\" = {ak}, \"AWS_SECRET_KEY\" = {sk}, \"use_path_style\" = {ps}) \
             PROPERTIES (\"timeout\" = \"{timeout}\")",
            db = quote_ident(&t.db),
            label = quote_ident(&unit.label),
            uri = quote_str(s3_uri),
            table = quote_ident(&t.name),
            cols = cols.join(", "),
            del = quote_ident(LOAD_DELETE_COLUMN),
            ep = quote_str(&s3.endpoint),
            region = quote_str(&s3.region),
            ak = quote_str(&s3.access_key),
            sk = quote_str(&s3.secret_key),
            ps = quote_str(if s3.path_style { "true" } else { "false" }),
            timeout = self.cfg.job_timeout_secs,
        )
    }
}

#[async_trait]
impl Loader for BrokerLoader {
    fn kind(&self) -> &'static str {
        "broker"
    }

    async fn submit(&self, unit: &LoadUnit) -> Result<LoadState> {
        // A retry of a label that is still running must not overwrite its file.
        match self.state(&unit.table, &unit.label).await? {
            LoadState::Running => return Ok(LoadState::Running),
            LoadState::Visible => return Ok(LoadState::Visible),
            LoadState::Unknown | LoadState::Failed(_) => {}
        }
        let path = self.object_path(&unit.table, &unit.label);
        self.store
            .put(&path, unit.parquet.clone().into())
            .await
            .map_err(|e| Error::Storage(format!("upload {path}: {e}")))?;
        let uri = format!("s3://{}/{}", self.cfg.s3.bucket, path);
        let sql = self.load_sql(unit, &uri);
        let mut conn = self.pool.get_conn().await.map_err(|e| Error::Sql(e.to_string()))?;
        match conn.query_drop(sql).await {
            Ok(()) => Ok(LoadState::Running),
            Err(e) if e.to_string().contains("already been used") => Ok(LoadState::Running),
            Err(e) => Ok(LoadState::Failed(format!("LOAD LABEL rejected: {e}"))),
        }
    }

    async fn state(&self, table: &TableDef, label: &str) -> Result<LoadState> {
        let mut conn = self.pool.get_conn().await.map_err(|e| Error::Sql(e.to_string()))?;
        let sql = format!(
            "SHOW LOAD FROM {} WHERE LABEL = {} ORDER BY CreateTime DESC LIMIT 1",
            quote_ident(&table.db),
            quote_str(label)
        );
        let rows: Vec<Row> = conn.query(sql).await.map_err(|e| Error::Sql(e.to_string()))?;
        let Some(row) = rows.into_iter().next() else { return Ok(LoadState::Unknown) };
        let state: String = row.get("State").unwrap_or_default();
        Ok(match state.as_str() {
            "FINISHED" => LoadState::Visible,
            "CANCELLED" => {
                let msg: Option<String> = row.get("ErrorMsg");
                LoadState::Failed(msg.unwrap_or_else(|| "cancelled".into()))
            }
            _ => LoadState::Running,
        })
    }

    async fn cleanup(&self, table: &TableDef, label: &str) -> Result<()> {
        let path = self.object_path(table, label);
        match self.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(Error::Storage(format!("delete {path}: {e}"))),
        }
    }
}
