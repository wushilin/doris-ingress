//! Arrow Flight SQL endpoint, for ADBC and Flight SQL JDBC drivers and the Doris ADBC catalog.
//!
//! * **Auth:** handshake with HTTP basic credentials returns a bearer token; every other call must
//!   carry the token (or basic credentials).
//! * **Ingest** (`DoPut` + `CommandStatementIngest`, e.g. ADBC `adbc_ingest`): rows are upserted
//!   by primary key. An optional boolean column `__delete` marks rows to delete by key. Each Arrow
//!   batch is committed through Raft atomically before the next is read.
//! * **Query** (`CommandStatementQuery`): SQL via DataFusion over catalog `ingress`, schema = Doris
//!   database, table = Doris table. Results include `__seq` and `__deleted` (tombstones are
//!   returned, as the merged view needs them). `=` / `IN` on a single-column primary key is served
//!   from the key index. On the leader, queries run after a linearizable read barrier.
//! * **Insert** (`INSERT INTO db.t [(columns)] VALUES ...` or `... SELECT ...`): an upsert of full
//!   rows by primary key; a column that is left out is written as null. `__seq`, `__deleted` and
//!   `__delete` cannot be set.
//! * **Delete** (`DELETE FROM db.t WHERE <primary key> = v` or `IN (...)`): writes a tombstone for
//!   each key named. The `WHERE` clause must name the primary key and nothing else, because the
//!   buffer cannot check other columns of a row that has already moved to Doris.
//! * Both work as an update statement, a prepared statement or a query (which returns the count).
//!   There is no other DML.
//! * **Node status** (`SELECT * FROM system.node`, `system.tables`): the state of the node that
//!   answers, read without the barrier so that a node cut off from the leader still reports.
//! * **Metadata:** catalogs, schemas, tables (with schemas), primary keys, SqlInfo.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int32Array, RecordBatch, StringArray, UInt64Array};
use arrow::compute::{filter_record_batch, not};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    CommandPreparedStatementQuery, DoPutPreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery, CommandStatementUpdate, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket};
use base64::Engine as _;
use bytes::Bytes;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use datafusion::sql::parser::Statement as ParsedStatement;
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, Statement as SqlStatement, TableObject};
use futures_util::{Stream, TryStreamExt};
use parking_lot::Mutex;
use prost::Message;
use tokio::task::JoinHandle;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

use crate::error::Error;
use crate::node::{Node, ScanOptions, WriteKind, WriteOp};
use crate::schema::{DELETED_COLUMN, SEQ_COLUMN};

/// Catalog name exposed to SQL and metadata calls.
pub const CATALOG: &str = "ingress";
/// Optional boolean ingest column: `true` deletes the row's key instead of upserting it.
pub const DELETE_COLUMN: &str = "__delete";
/// SQL schema of the tables that describe the answering node: `system.node`, `system.tables`.
pub const SYSTEM_SCHEMA: &str = "system";

#[derive(Clone, Debug)]
pub struct FlightConfig {
    pub listen: String,
    /// username → password.
    pub users: HashMap<String, String>,
    /// Before every query, obtain a read index from the leader (a quorum round on the leader; one
    /// extra hop on a follower) and wait until this node has applied it, so any node's reads see
    /// every write acknowledged before the query started. When false, queries read the node's
    /// applied state directly: cheaper, but a follower or deposed leader may serve slightly stale
    /// data.
    pub linearizable_reads: bool,
}

/// Starts the Flight SQL server; returns its bound address.
pub async fn serve(node: Arc<Node>, cfg: FlightConfig) -> crate::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind(&cfg.listen).await?;
    let addr = listener.local_addr()?;
    let svc = FlightServiceServer::new(FlightSqlServer::new(node, cfg.users).with_linearizable_reads(cfg.linearizable_reads))
        .max_decoding_message_size(512 << 20)
        .max_encoding_message_size(512 << 20);
    // Small request/response messages: disable Nagle, or every call waits ~40 ms for delayed ACKs.
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).map_ok(|s| {
        let _ = s.set_nodelay(true);
        s
    });
    let handle = tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder().add_service(svc).serve_with_incoming(incoming).await {
            tracing::error!(error = %e, "flight sql server stopped");
        }
    });
    Ok((addr, handle))
}

pub struct FlightSqlServer {
    node: Arc<Node>,
    users: HashMap<String, String>,
    tokens: Mutex<HashSet<String>>,
    /// One session for all queries; its catalog reads the node's tables at plan time.
    ctx: SessionContext,
    /// SQL text → (table-set fingerprint, logical plan). Data is read when the plan executes, so a
    /// cached plan never returns stale rows; it is replanned if the attached tables change.
    plans: Mutex<HashMap<String, (u64, LogicalPlan)>>,
    /// Open prepared statements by handle: the plan (reused on every execution) and bound
    /// parameter values.
    prepared: Mutex<HashMap<Bytes, Prepared>>,
    linearizable_reads: bool,
}

struct Prepared {
    plan: LogicalPlan,
    /// Result schema, computed once at prepare time.
    schema: Schema,
    params: Option<Vec<ScalarValue>>,
}

const MAX_PREPARED: usize = 65_536;

const PLAN_CACHE_ENTRIES: usize = 4096;

impl FlightSqlServer {
    pub fn new(node: Arc<Node>, users: HashMap<String, String>) -> Self {
        let cfg = SessionConfig::new().with_default_catalog_and_schema(CATALOG, "default");
        let ctx = SessionContext::new_with_config(cfg);
        ctx.register_catalog(CATALOG, Arc::new(IngressCatalog { node: node.clone() }));
        Self { node, users, tokens: Mutex::default(), ctx, plans: Mutex::default(), prepared: Mutex::default(), linearizable_reads: true }
    }

    pub fn with_linearizable_reads(mut self, on: bool) -> Self {
        self.linearizable_reads = on;
        self
    }

    async fn read_barrier(&self) -> Result<(), Status> {
        if self.linearizable_reads {
            self.node.read_barrier().await.map_err(to_status)?;
        }
        Ok(())
    }

    fn basic_ok(&self, b64: &str) -> bool {
        // Accept padded and unpadded base64 (the Go Flight client used by ADBC omits padding).
        use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};
        const LENIENT: GeneralPurpose = GeneralPurpose::new(
            &base64::alphabet::STANDARD,
            GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
        );
        let Ok(raw) = LENIENT.decode(b64.trim()) else { return false };
        let Ok(s) = String::from_utf8(raw) else { return false };
        let Some((user, pass)) = s.split_once(':') else { return false };
        self.users.get(user).is_some_and(|p| constant_time_eq(p.as_bytes(), pass.as_bytes()))
    }

    fn check<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let header = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("missing authorization"))?;
        if let Some(token) = header.strip_prefix("Bearer ")
            && self.tokens.lock().contains(token.trim())
        {
            return Ok(());
        }
        if let Some(b) = header.strip_prefix("Basic ")
            && self.basic_ok(b)
        {
            return Ok(());
        }
        Err(Status::unauthenticated("invalid credentials"))
    }

    fn tables_fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for t in self.node.tables() {
            t.full_name().hash(&mut h);
            t.schema.fields().len().hash(&mut h);
        }
        h.finish()
    }

    /// The logical plan for `sql`, from the cache when the same text was planned before.
    /// Only queries, `INSERT` and `DELETE` by primary key are accepted: DDL, other DML and session
    /// statements are refused.
    async fn plan_logical(&self, sql: &str) -> Result<LogicalPlan, Status> {
        let version = self.tables_fingerprint();
        let cached = self.plans.lock().get(sql).filter(|(v, _)| *v == version).map(|(_, p)| p.clone());
        if let Some(plan) = cached {
            return Ok(plan);
        }
        let opts = datafusion::execution::context::SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(true)
            .with_allow_statements(false);
        // Planning a DML statement does not run it.
        let invalid = |e: DataFusionError| Status::invalid_argument(e.to_string());
        let state = self.ctx.state();
        let mut statement = state.sql_to_statement(sql, &state.config().options().sql_parser.dialect).map_err(invalid)?;
        self.complete_insert(&mut statement)?;
        let plan = state.statement_to_plan(statement).await.map_err(invalid)?;
        opts.verify_plan(&plan).map_err(invalid)?;
        write_plan(&plan)?; // refuses DML other than INSERT and DELETE
        let mut plans = self.plans.lock();
        if plans.len() >= PLAN_CACHE_ENTRIES {
            plans.clear();
        }
        plans.insert(sql.to_string(), (version, plan.clone()));
        Ok(plan)
    }

    /// Prepares a parsed `INSERT` for planning. The hidden columns cannot be set, and an `INSERT`
    /// without a column list gets the table's own columns, so that it does not have to give
    /// values for the hidden ones.
    fn complete_insert(&self, statement: &mut ParsedStatement) -> Result<(), Status> {
        let ParsedStatement::Statement(inner) = statement else { return Ok(()) };
        let SqlStatement::Insert(insert) = inner.as_mut() else { return Ok(()) };
        let name = |i: &Ident| if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() };
        for column in &insert.columns {
            let column = column.0.last().and_then(|p| p.as_ident()).map(name).unwrap_or_default();
            if [SEQ_COLUMN, DELETED_COLUMN, DELETE_COLUMN].contains(&column.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "INSERT cannot set {column}: the sequence is assigned by the server, and rows are deleted with DELETE"
                )));
            }
        }
        if !insert.columns.is_empty() {
            return Ok(());
        }
        let TableObject::TableName(table) = &insert.table else { return Ok(()) };
        let Some(parts) = table.0.iter().map(|p| p.as_ident().map(name)).collect::<Option<Vec<_>>>() else { return Ok(()) };
        // `db.table` or `ingress.db.table`. An unknown table is reported by the planner.
        let full = parts[parts.len().saturating_sub(2)..].join(".");
        if let Some(def) = self.node.tables().into_iter().find(|t| t.full_name() == full) {
            insert.columns =
                def.schema.fields().iter().map(|f| ObjectName::from(vec![Ident::with_quote('"', f.name().as_str())])).collect();
        }
        Ok(())
    }

    /// Runs a planned `INSERT` or `DELETE` and returns the number of rows written.
    /// * `INSERT` upserts full rows by primary key.
    /// * `DELETE` writes a tombstone for every key its `WHERE` clause names. A key that is not in
    ///   the buffer gets a tombstone too, because its row may already be in Doris.
    async fn run_write(&self, plan: &LogicalPlan, params: Option<&[ScalarValue]>) -> Result<i64, Status> {
        let write = write_plan(plan)?
            .ok_or_else(|| Status::invalid_argument("not an update statement; use INSERT, or DELETE by primary key"))?;
        let invalid = |e: DataFusionError| Status::invalid_argument(e.to_string());
        match write {
            WritePlan::Delete { table, predicate } => {
                let table = table.as_ref().downcast_ref::<BufferTable>().expect("checked by write_plan");
                let keys = table.delete_keys(predicate, params)?;
                let n = keys.num_rows() as i64;
                self.node.delete(&table.table, keys).await.map_err(to_status)?;
                Ok(n)
            }
            WritePlan::Insert { table, input } => {
                let table = table.as_ref().downcast_ref::<BufferTable>().expect("checked by write_plan");
                // `INSERT ... SELECT` reads the buffer, so it needs the same barrier as a query.
                if input.exists(|p| Ok(matches!(p, LogicalPlan::TableScan(_)))).map_err(invalid)? {
                    self.read_barrier().await?;
                }
                let mut df = self.frame(input.clone());
                if let Some(values) = params {
                    df = df.with_param_values(values.to_vec()).map_err(invalid)?;
                }
                let batches = df.collect().await.map_err(invalid)?;
                let Some(rows) = table.insert_rows(&batches)? else { return Ok(0) };
                let n = rows.num_rows() as i64;
                self.node.upsert(&table.table, rows).await.map_err(to_status)?;
                Ok(n)
            }
        }
    }

    /// Runs an `INSERT` or `DELETE` that arrived as a query, and answers with its one-row `count` result.
    async fn write_as_query(
        &self,
        plan: &LogicalPlan,
        params: Option<&[ScalarValue]>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let n = self.run_write(plan, params).await?;
        let schema = Arc::new(plan.schema().as_arrow().clone());
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(UInt64Array::from(vec![n as u64]))]);
        Ok(Response::new(one_batch_stream(schema, batch)))
    }

    /// A DataFrame for `plan` whose every table scan reads at one snapshot, taken now. A query
    /// that reads several tables, or one table twice, sees a single consistent point in time.
    fn frame(&self, plan: LogicalPlan) -> DataFrame {
        // Clone the session and set the extension in place (rebuilding the session would replace
        // our catalog with a fresh default one).
        let mut state = self.ctx.state();
        state.config_mut().set_extension(Arc::new(QuerySnapshot(self.node.snapshot())));
        DataFrame::new(state, plan)
    }


    /// Answers `SELECT <columns> FROM <table> WHERE <pk> = v | <pk> IN (...)` straight from the key
    /// index, skipping DataFusion's physical planning and execution (most of a point lookup's cost).
    /// Returns `None` for any other plan shape, which then runs through DataFusion unchanged.
    fn fast_lookup(&self, plan: &LogicalPlan, params: Option<&[ScalarValue]>) -> Option<RecordBatch> {
        let LogicalPlan::Projection(proj) = plan else { return None };
        let LogicalPlan::Filter(filter) = proj.input.as_ref() else { return None };
        let LogicalPlan::TableScan(scan) = filter.input.as_ref() else { return None };
        if scan.fetch.is_some() || !scan.filters.is_empty() {
            return None;
        }
        let provider = datafusion::catalog::default_table_source::source_as_provider(&scan.source).ok()?;
        let table = provider.as_ref().downcast_ref::<BufferTable>()?;
        let values = table.key_values_with(&filter.predicate, params)?;
        if values.iter().any(|v| v.is_null()) {
            return None;
        }
        let keys = table.key_batch(values).ok()?;
        let rows = self.node.get(&table.table, &keys, None).ok()?;
        let cols = proj
            .expr
            .iter()
            .map(|e| match e {
                Expr::Column(c) => rows.column_by_name(&c.name).cloned(),
                Expr::Alias(a) => match a.expr.as_ref() {
                    Expr::Column(c) => rows.column_by_name(&c.name).cloned(),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        RecordBatch::try_new(Arc::new(plan.schema().as_arrow().clone()), cols).ok()
    }

    /// The DataFrame for an open prepared statement, with its bound parameters applied.
    async fn prepared_frame(&self, handle: &Bytes) -> Result<DataFrame, Status> {
        let (plan, params) = {
            let p = self.prepared.lock();
            let st = p.get(handle).ok_or_else(|| Status::not_found("unknown prepared statement handle"))?;
            (st.plan.clone(), st.params.clone())
        };
        let df = self.frame(plan);
        match params {
            Some(values) => df.with_param_values(values).map_err(|e| Status::invalid_argument(e.to_string())),
            None => Ok(df),
        }
    }

    async fn ingest_batch(&self, table: &str, batch: RecordBatch) -> Result<i64, Status> {
        let rows = batch.num_rows() as i64;
        if rows == 0 {
            return Ok(0);
        }
        let mut ops = Vec::new();
        match batch.schema().index_of(DELETE_COLUMN) {
            Ok(idx) => {
                let flag = batch.column(idx);
                let flag = flag
                    .as_boolean_opt()
                    .ok_or_else(|| Status::invalid_argument(format!("{DELETE_COLUMN} must be boolean")))?;
                // Null counts as "not a delete".
                let deletes = BooleanArray::from_iter(flag.iter().map(|v| Some(v == Some(true))));
                let mut data = batch.clone();
                data.remove_column(idx);
                let upserts = filter_record_batch(&data, &not(&deletes).map_err(arrow_status)?).map_err(arrow_status)?;
                let removed = filter_record_batch(&data, &deletes).map_err(arrow_status)?;
                if upserts.num_rows() > 0 {
                    ops.push(WriteOp { table: table.into(), kind: WriteKind::Upsert, batch: upserts });
                }
                if removed.num_rows() > 0 {
                    ops.push(WriteOp { table: table.into(), kind: WriteKind::Delete, batch: removed });
                }
            }
            Err(_) => ops.push(WriteOp { table: table.into(), kind: WriteKind::Upsert, batch }),
        }
        self.node.write(ops).await.map_err(to_status)?;
        Ok(rows)
    }
}

/// A write statement the server runs itself.
enum WritePlan<'a> {
    /// `INSERT`: `input` produces the rows, in the table's scan schema.
    Insert { table: Arc<dyn TableProvider>, input: &'a LogicalPlan },
    /// `DELETE`: `predicate` is the `WHERE` clause.
    Delete { table: Arc<dyn TableProvider>, predicate: &'a Expr },
}

/// The write a plan asks for, or `None` for a query. DML other than `INSERT` and `DELETE` is
/// refused.
fn write_plan(plan: &LogicalPlan) -> Result<Option<WritePlan<'_>>, Status> {
    use datafusion::catalog::default_table_source::source_as_provider;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::logical_expr::WriteOp as Dml;
    let dml = match plan {
        LogicalPlan::Dml(dml) => dml,
        LogicalPlan::Copy(_) => return Err(Status::invalid_argument("DML not supported: Copy")),
        _ => return Ok(None),
    };
    let buffered = |table: Arc<dyn TableProvider>| match table.as_ref().downcast_ref::<BufferTable>() {
        Some(_) => Ok(table),
        None => Err(Status::invalid_argument(format!("{} is not a buffered table", dml.table_name))),
    };
    match dml.op {
        Dml::Insert(InsertOp::Append | InsertOp::Replace) => {
            let table = source_as_provider(&dml.target).map_err(|e| Status::invalid_argument(e.to_string()))?;
            Ok(Some(WritePlan::Insert { table: buffered(table)?, input: dml.input.as_ref() }))
        }
        Dml::Delete => {
            let no_where =
                || Status::invalid_argument(format!("DELETE on {} needs a WHERE clause on its primary key", dml.table_name));
            let LogicalPlan::Filter(filter) = dml.input.as_ref() else { return Err(no_where()) };
            let LogicalPlan::TableScan(scan) = filter.input.as_ref() else { return Err(no_where()) };
            let table = source_as_provider(&scan.source).map_err(|e| Status::invalid_argument(e.to_string()))?;
            Ok(Some(WritePlan::Delete { table: buffered(table)?, predicate: &filter.predicate }))
        }
        _ => Err(Status::invalid_argument(format!(
            "DML not supported: {}. Use INSERT (an upsert of full rows) or DELETE by primary key",
            dml.op.name()
        ))),
    }
}

/// True when every table a plan reads is a `system` table. Such a query describes this node, so
/// it needs no read barrier.
fn reads_only_system(plan: &LogicalPlan) -> bool {
    let (mut system, mut other) = (false, false);
    let _ = plan.apply_with_subqueries(|p| {
        if let LogicalPlan::TableScan(scan) = p {
            let is_system = datafusion::catalog::default_table_source::source_as_provider(&scan.source)
                .is_ok_and(|t| t.as_ref().downcast_ref::<SystemTable>().is_some());
            if is_system { system = true } else { other = true }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    system && !other
}

/// The value of a literal, or of a `$n` placeholder when parameters are bound.
fn scalar_value(e: &Expr, params: Option<&[ScalarValue]>) -> Option<ScalarValue> {
    match e {
        Expr::Literal(v, _) => Some(v.clone()),
        Expr::Placeholder(p) => {
            let i: usize = p.id.strip_prefix('$')?.parse().ok()?;
            params?.get(i.checked_sub(1)?).cloned()
        }
        _ => None,
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn new_token() -> String {
    let mut buf = [0u8; 24];
    getrandom::fill(&mut buf).expect("os random source");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

fn to_status(e: Error) -> Status {
    match &e {
        Error::NotLeader { .. } => Status::unavailable(e.to_string()),
        Error::Backpressure { .. } | Error::TableStuck { .. } => Status::resource_exhausted(e.to_string()),
        Error::UnknownTable(_) => Status::not_found(e.to_string()),
        Error::Schema(_) | Error::TableExists(_) => Status::invalid_argument(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

fn arrow_status(e: arrow::error::ArrowError) -> Status {
    Status::invalid_argument(e.to_string())
}

fn df_err(e: Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// `db.table` from an ingest command (`db_schema` + `table`, or a dotted table name).
fn ingest_target(cmd: &CommandStatementIngest) -> Result<String, Status> {
    match (&cmd.schema, cmd.table.contains('.')) {
        (Some(db), false) if !db.is_empty() => Ok(format!("{db}.{}", cmd.table)),
        (_, true) => Ok(cmd.table.clone()),
        _ => Err(Status::invalid_argument("target table needs a schema (the Doris database), e.g. db.table")),
    }
}

fn flight_info(cmd_any: Vec<u8>, schema: &Schema, descriptor: FlightDescriptor) -> Result<Response<FlightInfo>, Status> {
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(cmd_any));
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|e| Status::internal(e.to_string()))?
        .with_endpoint(endpoint)
        .with_descriptor(descriptor);
    Ok(Response::new(info))
}

type DoGetStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>;

fn one_batch_stream<E: Into<FlightError> + Send + 'static>(schema: SchemaRef, batch: Result<RecordBatch, E>) -> DoGetStream {
    let stream = FlightDataEncoderBuilder::new()
        .with_schema(schema)
        .build(futures_util::stream::once(async move { batch.map_err(Into::into) }))
        .map_err(Status::from);
    Box::pin(stream)
}

static SQL_INFO: LazyLock<SqlInfoData> = LazyLock::new(|| {
    let mut b = SqlInfoDataBuilder::new();
    b.append(SqlInfo::FlightSqlServerName, "doris-ingress");
    b.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    b.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    b.append(SqlInfo::FlightSqlServerReadOnly, false);
    b.build().expect("static sql info")
});

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServer {
    type FlightService = FlightSqlServer;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>, Status> {
        let header = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("missing authorization"))?;
        let b64 = header.strip_prefix("Basic ").ok_or_else(|| Status::unauthenticated("basic authorization required"))?;
        if !self.basic_ok(b64) {
            return Err(Status::unauthenticated("invalid credentials"));
        }
        let token = new_token();
        self.tokens.lock().insert(token.clone());
        let out = futures_util::stream::iter(vec![Ok(HandshakeResponse { protocol_version: 0, payload: token.clone().into() })]);
        let mut resp: Response<Pin<Box<dyn Stream<Item = _> + Send>>> = Response::new(Box::pin(out));
        let value = MetadataValue::try_from(format!("Bearer {token}")).map_err(|e| Status::internal(e.to_string()))?;
        resp.metadata_mut().insert("authorization", value);
        Ok(resp)
    }

    async fn do_put_statement_ingest(
        &self,
        cmd: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.check(&request)?;
        let table = ingest_target(&cmd)?;
        let data = request.into_inner().map_err(|s| FlightError::Tonic(Box::new(s)));
        let mut batches = FlightRecordBatchStream::new_from_flight_data(data);
        let mut total = 0;
        while let Some(batch) = batches.try_next().await.map_err(|e| Status::invalid_argument(e.to_string()))? {
            total += self.ingest_batch(&table, batch).await?;
        }
        Ok(total)
    }

    async fn do_put_statement_update(
        &self,
        cmd: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.check(&request)?;
        let plan = self.plan_logical(&cmd.query).await?;
        self.run_write(&plan, None).await
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check(&request)?;
        let t0 = std::time::Instant::now();
        let plan = self.plan_logical(&query.query).await?;
        let schema = plan.schema().as_arrow().clone();
        tracing::debug!(plan_us = t0.elapsed().as_micros() as u64, "get_flight_info_statement");
        let ticket = TicketStatementQuery { statement_handle: query.query.into_bytes().into() };
        flight_info(ticket.as_any().encode_to_vec(), &schema, request.into_inner())
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check(&request)?;
        let sql = String::from_utf8(ticket.statement_handle.to_vec()).map_err(|_| Status::invalid_argument("bad statement handle"))?;
        let t0 = std::time::Instant::now();
        let plan = self.plan_logical(&sql).await?;
        if write_plan(&plan)?.is_some() {
            return self.write_as_query(&plan, None).await;
        }
        if !reads_only_system(&plan) {
            self.read_barrier().await?;
        }
        let t1 = std::time::Instant::now();
        if let Some(batch) = self.fast_lookup(&plan, None) {
            return Ok(Response::new(one_batch_stream::<FlightError>(batch.schema(), Ok(batch))));
        }
        let df = self.frame(plan);
        let schema = Arc::new(df.schema().as_arrow().clone());
        let t2 = std::time::Instant::now();
        let stream = df.execute_stream().await.map_err(|e| Status::internal(e.to_string()))?;
        tracing::debug!(
            barrier_us = (t1 - t0).as_micros() as u64,
            plan_us = (t2 - t1).as_micros() as u64,
            physical_us = t2.elapsed().as_micros() as u64,
            "do_get_statement"
        );
        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(flight)))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check(&request)?;
        let schema = query.into_builder().schema();
        flight_info(query.as_any().encode_to_vec(), &schema, request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check(&request)?;
        let mut b = query.into_builder();
        b.append(CATALOG);
        let schema = b.schema();
        Ok(Response::new(one_batch_stream(schema, b.build())))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check(&request)?;
        let schema = query.clone().into_builder().schema();
        flight_info(query.as_any().encode_to_vec(), &schema, request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check(&request)?;
        let dbs: BTreeSet<String> = self.node.tables().iter().map(|t| t.db.clone()).collect();
        let mut b = query.into_builder();
        for db in dbs {
            b.append(CATALOG, db);
        }
        let schema = b.schema();
        Ok(Response::new(one_batch_stream(schema, b.build())))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check(&request)?;
        let schema = query.clone().into_builder().schema();
        flight_info(query.as_any().encode_to_vec(), &schema, request.into_inner())
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check(&request)?;
        let mut b = query.into_builder();
        for t in self.node.tables() {
            let schema = self.node.scan_schema(&t.full_name()).map_err(to_status)?;
            b.append(CATALOG, &t.db, &t.name, "TABLE", &schema).map_err(|e| Status::internal(e.to_string()))?;
        }
        let schema = b.schema();
        Ok(Response::new(one_batch_stream(schema, b.build())))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check(&request)?;
        let schema = query.into_builder().schema();
        flight_info(query.as_any().encode_to_vec(), &schema, request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check(&request)?;
        let mut b = query.into_builder();
        b.append("TABLE");
        let schema = b.schema();
        Ok(Response::new(one_batch_stream(schema, b.build())))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check(&request)?;
        let schema = query.clone().into_builder(&SQL_INFO).schema();
        flight_info(query.as_any().encode_to_vec(), &schema, request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check(&request)?;
        let b = query.into_builder(&SQL_INFO);
        let schema = b.schema();
        Ok(Response::new(one_batch_stream(schema, b.build())))
    }

    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check(&request)?;
        flight_info(query.as_any().encode_to_vec(), &primary_keys_schema(), request.into_inner())
    }

    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check(&request)?;
        let (mut cat, mut db, mut tbl, mut col, mut seq) = (vec![], vec![], vec![], vec![], vec![]);
        for t in self.node.tables() {
            let matches = query.db_schema.as_deref().is_none_or(|s| s == t.db) && query.table == t.name;
            if !matches {
                continue;
            }
            for (i, k) in t.key_columns.iter().enumerate() {
                cat.push(CATALOG.to_string());
                db.push(t.db.clone());
                tbl.push(t.name.clone());
                col.push(k.clone());
                seq.push(i as i32 + 1);
            }
        }
        let n = col.len();
        let schema = Arc::new(primary_keys_schema());
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(cat)) as ArrayRef,
                Arc::new(StringArray::from(db)),
                Arc::new(StringArray::from(tbl)),
                Arc::new(StringArray::from(col)),
                Arc::new(Int32Array::from(seq)),
                arrow::array::new_null_array(&DataType::Utf8, n),
            ],
        );
        Ok(Response::new(one_batch_stream(schema, batch)))
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.check(&request)?;
        let plan = self.plan_logical(&query.query).await?;
        let result_schema = plan.schema().as_arrow().clone();
        // An empty dataset schema tells drivers that the statement is an update, not a query.
        let dataset_schema = if write_plan(&plan)?.is_some() { Bytes::new() } else { schema_ipc(&result_schema)? };
        // Placeholders ($1, $2, ... or ?) become the parameter schema, in positional order.
        let mut params: Vec<(String, Option<DataType>)> =
            plan.get_parameter_types().map_err(|e| Status::invalid_argument(e.to_string()))?.into_iter().collect();
        params.sort_by_key(|(name, _)| name.trim_start_matches('$').parse::<usize>().unwrap_or(usize::MAX));
        let fields: Vec<Field> =
            params.into_iter().map(|(name, ty)| Field::new(name, ty.unwrap_or(DataType::Utf8), true)).collect();
        let parameter_schema = if fields.is_empty() { Bytes::new() } else { schema_ipc(&Schema::new(fields))? };
        let handle = Bytes::from(new_token());
        {
            let mut p = self.prepared.lock();
            if p.len() >= MAX_PREPARED {
                return Err(Status::resource_exhausted("too many open prepared statements"));
            }
            p.insert(handle.clone(), Prepared { plan, schema: result_schema, params: None });
        }
        Ok(ActionCreatePreparedStatementResult { prepared_statement_handle: handle, dataset_schema, parameter_schema })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        self.check(&request)?;
        self.prepared.lock().remove(&query.prepared_statement_handle);
        Ok(())
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        self.check(&request)?;
        let data = request.into_inner().map_err(|s| FlightError::Tonic(Box::new(s)));
        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(data)
            .try_collect()
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        // One parameter set: the first row. (Executing a statement for many parameter rows is not
        // supported for queries.)
        let values = match batches.iter().find(|b| b.num_rows() > 0) {
            Some(b) => b
                .columns()
                .iter()
                .map(|c| ScalarValue::try_from_array(c, 0))
                .collect::<datafusion::error::Result<Vec<_>>>()
                .map_err(|e| Status::invalid_argument(e.to_string()))?,
            None => Vec::new(),
        };
        let handle = query.prepared_statement_handle;
        let mut p = self.prepared.lock();
        let st = p.get_mut(&handle).ok_or_else(|| Status::not_found("unknown prepared statement handle"))?;
        st.params = Some(values);
        Ok(DoPutPreparedStatementResult { prepared_statement_handle: Some(handle) })
    }

    async fn do_put_prepared_statement_update(
        &self,
        cmd: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.check(&request)?;
        // Some clients send only the command, in a message that carries no Arrow data.
        let data = request
            .into_inner()
            .map_err(|s| FlightError::Tonic(Box::new(s)))
            .try_filter(|d| std::future::ready(!d.data_header.is_empty()));
        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(data)
            .try_collect()
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let (plan, bound) = {
            let p = self.prepared.lock();
            let st = p.get(&cmd.prepared_statement_handle).ok_or_else(|| Status::not_found("unknown prepared statement handle"))?;
            (st.plan.clone(), st.params.clone())
        };
        // Every parameter row is one execution. Without any, use the parameters bound earlier.
        if batches.iter().all(|b| b.num_rows() == 0) {
            return self.run_write(&plan, bound.as_deref()).await;
        }
        let mut total = 0;
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let values = batch
                    .columns()
                    .iter()
                    .map(|c| ScalarValue::try_from_array(c, row))
                    .collect::<datafusion::error::Result<Vec<_>>>()
                    .map_err(|e| Status::invalid_argument(e.to_string()))?;
                total += self.run_write(&plan, Some(&values)).await?;
            }
        }
        Ok(total)
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check(&request)?;
        let schema = {
            let p = self.prepared.lock();
            p.get(&cmd.prepared_statement_handle).ok_or_else(|| Status::not_found("unknown prepared statement handle"))?.schema.clone()
        };
        flight_info(cmd.as_any().encode_to_vec(), &schema, request.into_inner())
    }

    async fn do_get_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check(&request)?;
        let delete = {
            let p = self.prepared.lock();
            let st = p.get(&cmd.prepared_statement_handle).ok_or_else(|| Status::not_found("unknown prepared statement handle"))?;
            write_plan(&st.plan)?.is_some().then(|| (st.plan.clone(), st.params.clone()))
        };
        if let Some((plan, params)) = delete {
            return self.write_as_query(&plan, params.as_deref()).await;
        }
        self.read_barrier().await?;
        let fast = {
            let p = self.prepared.lock();
            let st = p.get(&cmd.prepared_statement_handle).ok_or_else(|| Status::not_found("unknown prepared statement handle"))?;
            self.fast_lookup(&st.plan, st.params.as_deref())
        };
        if let Some(batch) = fast {
            return Ok(Response::new(one_batch_stream::<FlightError>(batch.schema(), Ok(batch))));
        }
        let df = self.prepared_frame(&cmd.prepared_statement_handle).await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let stream = df.execute_stream().await.map_err(|e| Status::internal(e.to_string()))?;
        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(flight)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// A schema as a Flight IPC message, as Flight SQL expects in prepared-statement results.
fn schema_ipc(schema: &Schema) -> Result<Bytes, Status> {
    let opts = arrow::ipc::writer::IpcWriteOptions::default();
    let msg: arrow_flight::IpcMessage =
        arrow_flight::SchemaAsIpc::new(schema, &opts).try_into().map_err(|e: arrow::error::ArrowError| Status::internal(e.to_string()))?;
    Ok(msg.0)
}

fn primary_keys_schema() -> Schema {
    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("key_sequence", DataType::Int32, false),
        Field::new("key_name", DataType::Utf8, true),
    ])
}

// ---------------------------------------------------------------------------------------------
// DataFusion catalog over the buffer.

struct IngressCatalog {
    node: Arc<Node>,
}

impl fmt::Debug for IngressCatalog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("IngressCatalog")
    }
}

impl CatalogProvider for IngressCatalog {
    fn schema_names(&self) -> Vec<String> {
        let mut dbs: BTreeSet<String> = self.node.tables().iter().map(|t| t.db.clone()).collect();
        dbs.insert(SYSTEM_SCHEMA.to_string());
        dbs.into_iter().collect()
    }

    /// A Doris database named `system` takes the name; the status tables are then hidden.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if self.node.tables().iter().any(|t| t.db == name) {
            return Some(Arc::new(IngressSchema { node: self.node.clone(), db: name.to_string() }));
        }
        (name == SYSTEM_SCHEMA).then(|| Arc::new(SystemSchema { node: self.node.clone() }) as Arc<dyn SchemaProvider>)
    }
}

/// Schema `system`: tables that describe the node answering the query.
struct SystemSchema {
    node: Arc<Node>,
}

impl fmt::Debug for SystemSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SystemSchema")
    }
}

#[async_trait::async_trait]
impl SchemaProvider for SystemSchema {
    fn table_names(&self) -> Vec<String> {
        vec!["node".into(), "tables".into()]
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let kind = match name {
            "node" => SystemKind::Node,
            "tables" => SystemKind::Tables,
            _ => return Ok(None),
        };
        Ok(Some(Arc::new(SystemTable { node: self.node.clone(), kind })))
    }

    fn table_exist(&self, name: &str) -> bool {
        name == "node" || name == "tables"
    }
}

#[derive(Clone, Copy, Debug)]
enum SystemKind {
    /// One row: this node's Raft role and progress, and its memory use.
    Node,
    /// One row per attached table: what this node buffers for it, and its flush health.
    /// `buffered_rows` = `active_rows` (still taking writes) + `unflushed_rows` (frozen, not yet
    /// visible in Doris) + `grace_rows` (visible in Doris, kept for the grace period). All count
    /// row versions, so a key written twice counts twice. `visible_seq` and `trimmed_seq` are the
    /// replicated watermarks: the highest sequence visible in Doris, and trimmed from the buffer.
    Tables,
}

/// A status table. Its rows are produced when a query scans it, never at plan time (plans are
/// cached).
struct SystemTable {
    node: Arc<Node>,
    kind: SystemKind,
}

impl fmt::Debug for SystemTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SystemTable({:?})", self.kind)
    }
}

impl SystemTable {
    fn rows(&self) -> datafusion::error::Result<RecordBatch> {
        let u64s = |v: Vec<u64>| Arc::new(UInt64Array::from(v)) as ArrayRef;
        let strs = |v: Vec<Option<String>>| Arc::new(StringArray::from(v)) as ArrayRef;
        let cols = match self.kind {
            SystemKind::Node => vec![
                u64s(vec![self.node.id()]),
                Arc::new(BooleanArray::from(vec![self.node.is_leader()])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![self.node.current_leader()])),
                u64s(vec![self.node.raft_last_log_index()]),
                u64s(vec![self.node.raft_purged_index()]),
                u64s(vec![self.node.snapshots_installed()]),
                u64s(vec![self.node.snapshot()]),
                u64s(vec![self.node.memory_used() as u64]),
                u64s(vec![self.node.process_memory()]),
                u64s(vec![self.node.wal_bytes()]),
                u64s(vec![self.node.tables().len() as u64]),
            ],
            SystemKind::Tables => {
                let tables = self.node.tables();
                let stats: Vec<_> = tables.iter().map(|t| self.node.stats(&t.full_name()).ok()).collect();
                let stat = |f: fn(&crate::node::TableStats) -> u64| u64s(stats.iter().map(|s| s.as_ref().map_or(0, f)).collect());
                vec![
                    strs(tables.iter().map(|t| Some(t.db.clone())).collect()),
                    strs(tables.iter().map(|t| Some(t.name.clone())).collect()),
                    strs(tables.iter().map(|t| Some(t.key_columns.join(", "))).collect()),
                    stat(|s| (s.active_rows + s.unflushed_rows + s.grace_rows) as u64),
                    stat(|s| s.active_rows as u64),
                    stat(|s| s.unflushed_rows as u64),
                    stat(|s| s.grace_rows as u64),
                    stat(|s| s.buffered_bytes as u64),
                    stat(|s| s.memory_cap_bytes as u64),
                    stat(|s| s.grace.as_millis() as u64),
                    stat(|s| s.write_delay.as_millis() as u64),
                    Arc::new(BooleanArray::from(stats.iter().map(|s| s.as_ref().is_some_and(|s| s.writes_held)).collect::<Vec<_>>())) as ArrayRef,
                    stat(|s| s.load_running.map_or(0, |d| d.as_millis() as u64)),
                    stat(|s| s.visible_seq),
                    stat(|s| s.trimmed_seq),
                    stat(|s| s.unflushed_units as u64),
                    stat(|s| s.grace_units as u64),
                    stat(|s| s.consecutive_failures as u64),
                    strs(stats.iter().map(|s| s.as_ref().and_then(|s| s.last_error.clone())).collect()),
                    strs(stats.iter().map(|s| s.as_ref().and_then(|s| s.rejecting.clone())).collect()),
                ]
            }
        };
        Ok(RecordBatch::try_new(self.schema(), cols)?)
    }
}

#[async_trait::async_trait]
impl TableProvider for SystemTable {
    fn schema(&self) -> SchemaRef {
        let u64 = |name: &str| Field::new(name, DataType::UInt64, false);
        let text = |name: &str, nullable: bool| Field::new(name, DataType::Utf8, nullable);
        Arc::new(Schema::new(match self.kind {
            SystemKind::Node => vec![
                u64("node_id"),
                Field::new("is_leader", DataType::Boolean, false),
                Field::new("leader_id", DataType::UInt64, true),
                u64("raft_last_log_index"),
                u64("raft_purged_index"),
                u64("snapshots_installed"),
                u64("applied_seq"),
                u64("memory_used_bytes"),
                u64("process_memory_bytes"),
                u64("wal_bytes"),
                u64("tables"),
            ],
            SystemKind::Tables => vec![
                text("db", false),
                text("name", false),
                text("key_columns", false),
                u64("buffered_rows"),
                u64("active_rows"),
                u64("unflushed_rows"),
                u64("grace_rows"),
                u64("buffered_bytes"),
                u64("memory_cap_bytes"),
                u64("grace_ms"),
                u64("write_delay_ms"),
                Field::new("writes_held", DataType::Boolean, false),
                u64("load_running_ms"),
                u64("visible_seq"),
                u64("trimmed_seq"),
                u64("unflushed_units"),
                u64("grace_units"),
                u64("consecutive_failures"),
                text("last_error", true),
                text("rejecting", true),
            ],
        }))
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(&[vec![self.rows()?]], self.schema(), projection.cloned())?)
    }
}

struct IngressSchema {
    node: Arc<Node>,
    db: String,
}

impl fmt::Debug for IngressSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IngressSchema({})", self.db)
    }
}

#[async_trait::async_trait]
impl SchemaProvider for IngressSchema {
    fn table_names(&self) -> Vec<String> {
        self.node.tables().iter().filter(|t| t.db == self.db).map(|t| t.name.clone()).collect()
    }

    async fn table(&self, name: &str) -> datafusion::error::Result<Option<Arc<dyn TableProvider>>> {
        let full = format!("{}.{name}", self.db);
        let Some(def) = self.node.tables().into_iter().find(|t| t.full_name() == full) else { return Ok(None) };
        let schema = self.node.scan_schema(&full).map_err(df_err)?;
        let key = (def.key_columns.len() == 1)
            .then(|| def.schema.field_with_name(&def.key_columns[0]).ok().map(|f| (f.name().clone(), f.data_type().clone())))
            .flatten();
        Ok(Some(Arc::new(BufferTable { node: self.node.clone(), table: full, schema, key })))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names().iter().any(|n| n == name)
    }
}

/// The read point shared by all table scans of one query.
#[derive(Debug)]
struct QuerySnapshot(u64);

/// One attached table, read at the query's snapshot.
struct BufferTable {
    node: Arc<Node>,
    table: String,
    schema: SchemaRef,
    /// Name and type of the primary key, when it is a single column.
    key: Option<(String, DataType)>,
}

impl fmt::Debug for BufferTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BufferTable({})", self.table)
    }
}

impl BufferTable {
    /// Key values selected by `k = v`, `v = k` or `k IN (...)`.
    fn key_values(&self, e: &Expr) -> Option<Vec<ScalarValue>> {
        self.key_values_with(e, None)
    }

    /// Like [`Self::key_values`], also resolving `$n` placeholders from bound parameters.
    fn key_values_with(&self, e: &Expr, params: Option<&[ScalarValue]>) -> Option<Vec<ScalarValue>> {
        let (key, _) = self.key.as_ref()?;
        let is_key = |e: &Expr| matches!(e, Expr::Column(c) if &c.name == key);
        let value = |e: &Expr| scalar_value(e, params);
        match e {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => {
                if is_key(left) {
                    Some(vec![value(right)?])
                } else if is_key(right) {
                    Some(vec![value(left)?])
                } else {
                    None
                }
            }
            Expr::InList(list) if !list.negated && is_key(&list.expr) => list.list.iter().map(value).collect(),
            _ => None,
        }
    }

    /// The rows an `INSERT` produced, cut down to the table's own columns (the plan also carries
    /// the hidden ones, as nulls). `None` when there are no rows.
    fn insert_rows(&self, batches: &[RecordBatch]) -> Result<Option<RecordBatch>, Status> {
        let Some(first) = batches.first() else { return Ok(None) };
        let all = arrow::compute::concat_batches(&first.schema(), batches).map_err(arrow_status)?;
        if all.num_rows() == 0 {
            return Ok(None);
        }
        let def = self
            .node
            .tables()
            .into_iter()
            .find(|t| t.full_name() == self.table)
            .ok_or_else(|| Status::not_found(format!("unknown table {}", self.table)))?;
        let (mut fields, mut cols) = (Vec::new(), Vec::new());
        for f in def.schema.fields() {
            let col = all.column_by_name(f.name()).ok_or_else(|| Status::internal(format!("INSERT plan has no column {}", f.name())))?;
            fields.push(Field::new(f.name(), col.data_type().clone(), true));
            cols.push(col.clone());
        }
        Ok(Some(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).map_err(arrow_status)?))
    }

    /// The keys a `DELETE` predicate names, as a batch of the key columns. The predicate must be
    /// `k = v` or `k IN (...)` for a single-column key, or `k1 = v AND k2 = v ...` naming every
    /// column of a composite key. Nothing else is accepted: the buffer cannot evaluate a condition
    /// on a row that has already moved to Doris.
    fn delete_keys(&self, predicate: &Expr, params: Option<&[ScalarValue]>) -> Result<RecordBatch, Status> {
        let def = self
            .node
            .tables()
            .into_iter()
            .find(|t| t.full_name() == self.table)
            .ok_or_else(|| Status::not_found(format!("unknown table {}", self.table)))?;
        let keys = &def.key_columns;
        let usage = || {
            let shape = match keys.as_slice() {
                [k] => format!("{k} = value, or {k} IN (...)"),
                _ => keys.iter().map(|k| format!("{k} = value")).collect::<Vec<_>>().join(" AND "),
            };
            Status::invalid_argument(format!(
                "DELETE on {} must select rows by primary key and nothing else: WHERE {shape}",
                self.table
            ))
        };
        let mut values: HashMap<&str, Vec<ScalarValue>> = HashMap::new();
        if self.key.is_some() {
            values.insert(keys[0].as_str(), self.key_values_with(predicate, params).ok_or_else(usage)?);
        } else {
            for cond in split_conjunction(predicate) {
                let Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) = cond else { return Err(usage()) };
                let (col, value) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), v) | (v, Expr::Column(c)) => (c, v),
                    _ => return Err(usage()),
                };
                let name = keys.iter().find(|k| **k == col.name).ok_or_else(usage)?;
                let value = scalar_value(value, params).ok_or_else(usage)?;
                if values.insert(name.as_str(), vec![value]).is_some() {
                    return Err(usage());
                }
            }
            if values.len() != keys.len() {
                return Err(usage());
            }
        }
        let (mut fields, mut cols) = (Vec::new(), Vec::new());
        for k in keys {
            let field = def.schema.field_with_name(k).map_err(arrow_status)?;
            let arr = ScalarValue::iter_to_array(values.remove(k.as_str()).expect("every key column has values"))
                .and_then(|a| Ok(arrow::compute::cast(&a, field.data_type())?))
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            if arr.null_count() > 0 {
                return Err(Status::invalid_argument(format!("DELETE: a value for {k} is NULL or is not a valid {}", field.data_type())));
            }
            fields.push(Field::new(k, field.data_type().clone(), false));
            cols.push(arr);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).map_err(arrow_status)
    }

    fn key_batch(&self, values: Vec<ScalarValue>) -> datafusion::error::Result<RecordBatch> {
        let (name, dt) = self.key.as_ref().expect("key_values checked");
        let arr = ScalarValue::iter_to_array(values)?;
        let arr = arrow::compute::cast(&arr, dt)?;
        let schema = Arc::new(Schema::new(vec![Field::new(name, dt.clone(), arr.null_count() > 0)]));
        Ok(RecordBatch::try_new(schema, vec![arr])?)
    }
}

#[async_trait::async_trait]
impl TableProvider for BufferTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if self.key_values(f).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let snapshot = match state.config().get_extension::<QuerySnapshot>() {
            Some(s) => s.0,
            None => self.node.snapshot(),
        };
        let batches = match filters.iter().find_map(|f| self.key_values(f)) {
            Some(values) => {
                let keys = self.key_batch(values)?;
                vec![self.node.get(&self.table, &keys, Some(snapshot)).map_err(df_err)?]
            }
            None => self
                .node
                .scan(&self.table, ScanOptions { include_deleted: true, snapshot: Some(snapshot) })
                .map_err(df_err)?,
        };
        Ok(MemorySourceConfig::try_new_exec(&[batches], self.schema.clone(), projection.cloned())?)
    }
}
