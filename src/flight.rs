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
//! * **Metadata:** catalogs, schemas, tables (with schemas), primary keys, SqlInfo.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int32Array, RecordBatch, StringArray};
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
    CommandGetTables, CommandStatementIngest, CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, Ticket};
use base64::Engine as _;
use bytes::Bytes;
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::common::ScalarValue;
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use futures_util::{Stream, TryStreamExt};
use parking_lot::Mutex;
use prost::Message;
use tokio::task::JoinHandle;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

use crate::error::Error;
use crate::node::{Node, ScanOptions, WriteKind, WriteOp};

/// Catalog name exposed to SQL and metadata calls.
pub const CATALOG: &str = "ingress";
/// Optional boolean ingest column: `true` deletes the row's key instead of upserting it.
pub const DELETE_COLUMN: &str = "__delete";

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
    /// Only queries are accepted: DDL, DML and session statements are refused.
    async fn plan_logical(&self, sql: &str) -> Result<LogicalPlan, Status> {
        let version = self.tables_fingerprint();
        let cached = self.plans.lock().get(sql).filter(|(v, _)| *v == version).map(|(_, p)| p.clone());
        if let Some(plan) = cached {
            return Ok(plan);
        }
        let opts = datafusion::execution::context::SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = self.ctx.sql_with_options(sql, opts).await.map_err(|e| Status::invalid_argument(e.to_string()))?;
        let plan = df.logical_plan().clone();
        let mut plans = self.plans.lock();
        if plans.len() >= PLAN_CACHE_ENTRIES {
            plans.clear();
        }
        plans.insert(sql.to_string(), (version, plan.clone()));
        Ok(plan)
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
        self.read_barrier().await?;
        let t1 = std::time::Instant::now();
        let plan = self.plan_logical(&sql).await?;
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
        let dataset_schema = schema_ipc(&result_schema)?;
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
        let dbs: BTreeSet<String> = self.node.tables().iter().map(|t| t.db.clone()).collect();
        dbs.into_iter().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.node
            .tables()
            .iter()
            .any(|t| t.db == name)
            .then(|| Arc::new(IngressSchema { node: self.node.clone(), db: name.to_string() }) as Arc<dyn SchemaProvider>)
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
        let value = |e: &Expr| -> Option<ScalarValue> {
            match e {
                Expr::Literal(v, _) => Some(v.clone()),
                Expr::Placeholder(p) => {
                    let i: usize = p.id.strip_prefix('$')?.parse().ok()?;
                    params?.get(i.checked_sub(1)?).cloned()
                }
                _ => None,
            }
        };
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
