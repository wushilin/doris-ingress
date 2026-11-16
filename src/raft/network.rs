//! Raft transport over HTTP with MessagePack bodies.
//!
//! Every request names the target node id in its path; a node answers 409 if the id is not its
//! own, so a reused address never delivers a message to the wrong member.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes as AxumBytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response as HttpResponse};
use axum::routing::post;
use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, v2::RaftNetworkV2};
use openraft::raft::{AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse};
use serde::{Deserialize, Serialize};

use super::{BasicNode, LogId, NodeId, Raft, SnapshotData, SnapshotMeta, TypeConfig, Vote, decode, encode};

/// Serializable form of `AppendResult` (which has no serde impl).
#[derive(Serialize, Deserialize)]
enum AppendReply {
    Ok(Option<LogId>),
    Conflict(LogId),
    HigherVote(Vote),
}

#[derive(Serialize, Deserialize)]
struct SnapshotRequest {
    vote: Vote,
    meta: SnapshotMeta,
    data: SnapshotData,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct NetErr(String);

/// HTTP client for node-to-node traffic (Raft RPCs, forwarded writes, read-index requests):
/// HTTP/1.1 with keep-alive, one pooled connection per concurrent request. HTTP/2 multiplexing
/// over a single connection was measured 3x slower for small writes, with much worse tails.
pub fn internal_client(idle_per_host: usize) -> reqwest::Client {
    reqwest::Client::builder().pool_max_idle_per_host(idle_per_host).tcp_nodelay(true).build().expect("http client")
}

#[derive(Clone)]
pub struct NetworkFactory {
    http: reqwest::Client,
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self { http: internal_client(4) }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Peer;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        Peer { http: self.http.clone(), target, base: format!("http://{}", node.addr) }
    }
}

pub struct Peer {
    http: reqwest::Client,
    target: NodeId,
    base: String,
}

impl Peer {
    async fn call<Req: Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
        timeout: Duration,
    ) -> Result<Resp, NetCallError> {
        let body = encode(req).map_err(|e| NetCallError::Network(e.to_string()))?;
        let url = format!("{}/raft/{}/{path}", self.base, self.target);
        let resp = self
            .http
            .post(&url)
            .timeout(timeout)
            .body(body)
            .send()
            .await
            .map_err(|e| NetCallError::Unreachable(format!("{url}: {e}")))?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| NetCallError::Unreachable(e.to_string()))?;
        if status == StatusCode::CONFLICT {
            return Err(NetCallError::Unreachable(format!("{url}: {}", String::from_utf8_lossy(&bytes))));
        }
        if !status.is_success() {
            return Err(NetCallError::Network(format!("{url}: HTTP {status}: {}", String::from_utf8_lossy(&bytes))));
        }
        decode(&bytes).map_err(|e| NetCallError::Network(e.to_string()))
    }
}

enum NetCallError {
    Unreachable(String),
    Network(String),
}

impl NetCallError {
    fn rpc(self) -> RPCError<TypeConfig> {
        match self {
            NetCallError::Unreachable(m) => RPCError::Unreachable(Unreachable::new(&NetErr(m))),
            NetCallError::Network(m) => RPCError::Network(NetworkError::new(&NetErr(m))),
        }
    }
    fn streaming(self) -> StreamingError<TypeConfig> {
        match self {
            NetCallError::Unreachable(m) => StreamingError::Unreachable(Unreachable::new(&NetErr(m))),
            NetCallError::Network(m) => StreamingError::Network(NetworkError::new(&NetErr(m))),
        }
    }
}

impl RaftNetworkV2<TypeConfig> for Peer {
    type SnapshotData = SnapshotData;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let reply: AppendReply = self.call("append", &rpc, option.hard_ttl()).await.map_err(NetCallError::rpc)?;
        Ok(match reply {
            AppendReply::Ok(l) => Ok(l),
            AppendReply::Conflict(log_id) => Err(openraft::raft::AppendError::Conflict { log_id, hint: None }),
            AppendReply::HigherVote(v) => Err(openraft::raft::AppendError::HigherVote(v)),
        })
    }

    async fn vote(&mut self, rpc: VoteRequest<TypeConfig>, option: RPCOption) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.call("vote", &rpc, option.hard_ttl()).await.map_err(NetCallError::rpc)
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: openraft::type_config::alias::SnapshotOf<TypeConfig, SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let req = SnapshotRequest { vote, meta: snapshot.meta, data: snapshot.snapshot };
        let timeout = option.hard_ttl().max(Duration::from_secs(60));
        tokio::select! {
            r = self.call("snapshot", &req, timeout) => r.map_err(NetCallError::streaming),
            closed = cancel => Err(StreamingError::Closed(closed)),
        }
    }
}

/// Largest Raft or internal RPC body accepted.
pub const MAX_RPC_BODY: usize = 1 << 30;

#[derive(Clone)]
struct ServerState {
    id: NodeId,
    raft: Raft,
}

/// HTTP routes for incoming Raft RPCs.
pub fn router(id: NodeId, raft: Raft) -> Router {
    Router::new()
        .route("/raft/{target}/append", post(append))
        .route("/raft/{target}/vote", post(vote))
        .route("/raft/{target}/snapshot", post(snapshot))
        // Group-committed entries carry whole Arrow batches: allow large bodies (axum's 2 MB
        // default would make replication of big writes fail forever).
        .layer(axum::extract::DefaultBodyLimit::max(MAX_RPC_BODY))
        .with_state(Arc::new(ServerState { id, raft }))
}

/// The rejection to send when a request was addressed to another node id.
fn wrong_target(s: &ServerState, target: NodeId) -> Option<HttpResponse> {
    (s.id != target).then(|| (StatusCode::CONFLICT, format!("this is node {}, not {target}", s.id)).into_response())
}

fn reply<T: Serialize>(v: &T) -> HttpResponse {
    match encode(v) {
        Ok(b) => (StatusCode::OK, b.to_vec()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

fn bad(e: impl std::fmt::Display) -> HttpResponse {
    (StatusCode::BAD_REQUEST, e.to_string()).into_response()
}

fn fail(e: impl std::fmt::Display) -> HttpResponse {
    (StatusCode::SERVICE_UNAVAILABLE, e.to_string()).into_response()
}

async fn append(State(s): State<Arc<ServerState>>, Path(target): Path<NodeId>, body: AxumBytes) -> HttpResponse {
    if let Some(r) = wrong_target(&s, target) {
        return r;
    }
    let req: AppendEntriesRequest<TypeConfig> = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad(e),
    };
    match s.raft.append_entries(req).await {
        Ok(Ok(l)) => reply(&AppendReply::Ok(l)),
        Ok(Err(openraft::raft::AppendError::Conflict { log_id, .. })) => reply(&AppendReply::Conflict(log_id)),
        Ok(Err(openraft::raft::AppendError::HigherVote(v))) => reply(&AppendReply::HigherVote(v)),
        Err(e) => fail(e),
    }
}

async fn vote(State(s): State<Arc<ServerState>>, Path(target): Path<NodeId>, body: AxumBytes) -> HttpResponse {
    if let Some(r) = wrong_target(&s, target) {
        return r;
    }
    let req: VoteRequest<TypeConfig> = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad(e),
    };
    match s.raft.vote(req).await {
        Ok(v) => reply(&v),
        Err(e) => fail(e),
    }
}

async fn snapshot(State(s): State<Arc<ServerState>>, Path(target): Path<NodeId>, body: AxumBytes) -> HttpResponse {
    if let Some(r) = wrong_target(&s, target) {
        return r;
    }
    let req: SnapshotRequest = match decode(&body) {
        Ok(r) => r,
        Err(e) => return bad(e),
    };
    let snap = openraft::storage::Snapshot { meta: req.meta, snapshot: req.data };
    match s.raft.install_full_snapshot(req.vote, snap).await {
        Ok(r) => reply(&r),
        Err(e) => fail(e),
    }
}
