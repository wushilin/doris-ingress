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

use super::{BasicNode, LogId, NodeId, Raft, SnapshotData, SnapshotEntries, SnapshotMeta, TypeConfig, Vote, decode, encode};

/// Serializable form of `AppendResult` (which has no serde impl).
#[derive(Serialize, Deserialize)]
enum AppendReply {
    Ok(Option<LogId>),
    Conflict(LogId),
    HigherVote(Vote),
}

/// The start of a snapshot transfer. The request body is a stream:
/// `[u32 length][this, encoded]`, then for every entry `[u64 index][u32 length][entry]`, then
/// `[u64::MAX]`. All integers are little endian. A body that ends before the end mark is an
/// incomplete transfer and is thrown away.
#[derive(Serialize, Deserialize)]
struct SnapshotHead {
    vote: Vote,
    meta: SnapshotMeta,
    /// The state machine's metadata (tables, sequence counter, units, watermarks, references).
    state: bytes::Bytes,
}

const SNAPSHOT_END: u64 = u64::MAX;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct NetErr(String);

/// HTTP client for node-to-node traffic (Raft RPCs, forwarded writes, read-index requests):
/// HTTP/1.1 with keep-alive, one pooled connection per concurrent request. HTTP/2 multiplexing
/// over a single connection was measured 3x slower for small writes, with much worse tails.
pub fn internal_client(idle_per_host: usize) -> reqwest::Client {
    reqwest::Client::builder().pool_max_idle_per_host(idle_per_host).tcp_nodelay(true).build().expect("http client")
}

/// What this node, as leader, has sent its followers by way of snapshots.
#[derive(Default)]
pub struct SnapshotSends {
    /// Sends in flight per follower (a send returns when the follower has installed it).
    pub in_flight: std::collections::HashMap<NodeId, usize>,
    /// Log index of the last snapshot a send was started for, per follower. It is recorded
    /// before the first byte leaves, under this lock, so the purger can hold the log from
    /// exactly that index.
    pub last_index: std::collections::HashMap<NodeId, u64>,
}

pub type SnapshotsInFlight = std::sync::Arc<parking_lot::Mutex<SnapshotSends>>;

/// A fault injected into this node's replication to one follower, for tests.
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerFault {
    /// Delay before every append that carries entries: a slow follower. Heartbeats are not
    /// delayed, so the follower stays alive.
    pub append_delay: Duration,
    /// Every append that carries entries fails (after `append_delay`), as if it had timed out:
    /// a follower that answers but cannot take the log.
    pub append_fails: bool,
    /// Every snapshot send fails at once, as if it had timed out.
    pub snapshot_fails: bool,
}

pub type PeerFaults = std::sync::Arc<parking_lot::Mutex<std::collections::HashMap<NodeId, PeerFault>>>;

#[derive(Clone)]
pub struct NetworkFactory {
    http: reqwest::Client,
    pub sending: SnapshotsInFlight,
    pub faults: PeerFaults,
    /// Most bytes per second one snapshot send may take (0: no limit).
    pub snapshot_rate: u64,
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self { http: internal_client(4), sending: SnapshotsInFlight::default(), faults: PeerFaults::default(), snapshot_rate: 0 }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Peer;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        Peer { http: self.http.clone(), target, base: format!("http://{}", node.addr), sending: self.sending.clone(), faults: self.faults.clone(), snapshot_rate: self.snapshot_rate }
    }
}

pub struct Peer {
    http: reqwest::Client,
    sending: SnapshotsInFlight,
    faults: PeerFaults,
    snapshot_rate: u64,
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
        self.call_body(path, body.into(), timeout).await
    }

    async fn call_body<Resp: serde::de::DeserializeOwned>(&self, path: &str, body: reqwest::Body, timeout: Duration) -> Result<Resp, NetCallError> {
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
        let fault = self.faults.lock().get(&self.target).copied().unwrap_or_default();
        if !rpc.entries.is_empty() {
            if !fault.append_delay.is_zero() {
                tokio::time::sleep(fault.append_delay).await;
            }
            if fault.append_fails {
                return Err(NetCallError::Unreachable("injected fault: append times out".into()).rpc());
            }
        }
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
        if self.faults.lock().get(&self.target).is_some_and(|f| f.snapshot_fails) {
            return Err(NetCallError::Unreachable("injected fault: snapshot send fails".into()).streaming());
        }
        let index = snapshot.meta.last_log_id.as_ref().map_or(0, |id| id.index);
        let head = SnapshotHead { vote, meta: snapshot.meta, state: snapshot.snapshot.meta };
        let head = encode(&head).map_err(|e| NetCallError::Network(e.to_string()).streaming())?;
        let body = reqwest::Body::wrap_stream(snapshot_stream(head, snapshot.snapshot.entries, self.snapshot_rate));
        // The size is not known up front and can be many gigabytes: no deadline for the whole
        // transfer beyond a generous one. openraft cancels the send when it no longer wants it.
        let timeout = option.hard_ttl().max(Duration::from_secs(3600));
        // Marked for the whole send, however it ends, so the leader keeps this follower's hold
        // on the log while it transfers and installs.
        struct InFlight(SnapshotsInFlight, NodeId);
        impl Drop for InFlight {
            fn drop(&mut self) {
                let mut sends = self.0.lock();
                if let Some(n) = sends.in_flight.get_mut(&self.1) {
                    *n -= 1;
                    if *n == 0 {
                        sends.in_flight.remove(&self.1);
                    }
                }
            }
        }
        {
            let mut sends = self.sending.lock();
            *sends.in_flight.entry(self.target).or_insert(0) += 1;
            sends.last_index.insert(self.target, index);
        }
        let _in_flight = InFlight(self.sending.clone(), self.target);
        tokio::select! {
            r = self.call_body("snapshot", body, timeout) => r.map_err(NetCallError::streaming),
            closed = cancel => Err(StreamingError::Closed(closed)),
        }
    }
}

/// The body of a snapshot transfer (see [`SnapshotHead`]). Entries are read one at a time as the
/// stream is consumed, so memory use does not depend on the snapshot's size, and the stream is
/// paced to at most `rate` bytes per second (0: no limit).
fn snapshot_stream(head: bytes::Bytes, entries: SnapshotEntries, rate: u64) -> impl futures_util::Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static {
    let mut first = bytes::BytesMut::with_capacity(4 + head.len());
    first.extend_from_slice(&(head.len() as u32).to_le_bytes());
    first.extend_from_slice(&head);
    let frames = entries.read().flat_map(|entry| match entry {
        Ok((index, body)) => {
            let mut prefix = bytes::BytesMut::with_capacity(12);
            prefix.extend_from_slice(&index.to_le_bytes());
            prefix.extend_from_slice(&(body.len() as u32).to_le_bytes());
            vec![Ok(prefix.freeze()), Ok(body)]
        }
        Err(e) => vec![Err(e)],
    });
    let chunks = std::iter::once(Ok(first.freeze())).chain(frames).chain(std::iter::once(Ok(bytes::Bytes::copy_from_slice(&SNAPSHOT_END.to_le_bytes()))));
    let started = tokio::time::Instant::now();
    futures_util::stream::unfold((chunks, 0u64), move |(mut chunks, sent)| async move {
        // Stay at or below the rate: the next chunk is due once everything before it "fits".
        if rate > 0 {
            tokio::time::sleep_until(started + Duration::from_secs_f64(sent as f64 / rate as f64)).await;
        }
        let chunk = chunks.next()?;
        let sent = sent + chunk.as_ref().map_or(0, |b| b.len() as u64);
        Some((chunk, (chunks, sent)))
    })
}

/// Reads `n` more bytes of a streamed body into `buf` (which may already hold some), or reports
/// that the body ended first.
async fn fill<S>(buf: &mut bytes::BytesMut, body: &mut S, n: usize) -> std::io::Result<()>
where
    S: futures_util::Stream<Item = Result<bytes::Bytes, axum::Error>> + Unpin,
{
    use futures_util::StreamExt;
    while buf.len() < n {
        match body.next().await {
            Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
            Some(Err(e)) => return Err(std::io::Error::other(e)),
            None => return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "snapshot transfer ended early")),
        }
    }
    Ok(())
}

/// Receives a snapshot transfer into a file under `dir`, entry by entry, and returns its head
/// and the file. Nothing but the entry being received is held in memory.
async fn receive_snapshot(dir: &std::path::Path, body: axum::body::Body) -> std::io::Result<(SnapshotHead, std::path::PathBuf)> {
    use bytes::Buf;
    use crate::wal::format::{FileKind, Frame, FrameKind};
    let mut body = body.into_data_stream();
    let mut buf = bytes::BytesMut::new();
    fill(&mut buf, &mut body, 4).await?;
    let len = buf.get_u32_le() as usize;
    fill(&mut buf, &mut body, len).await?;
    let head: SnapshotHead = decode(&buf.split_to(len))?;

    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let path = dir.join(format!("incoming-{nanos}.tmp"));
    let result = async {
        let mut file = crate::wal::ContainerWriter::create(&path, FileKind::Snapshot)?;
        file.append(&super::state_machine::snapshot_head_frame(&head.meta, &head.state)?)?;
        loop {
            fill(&mut buf, &mut body, 8).await?;
            let index = buf.get_u64_le();
            if index == SNAPSHOT_END {
                break;
            }
            fill(&mut buf, &mut body, 4).await?;
            let len = buf.get_u32_le() as usize;
            fill(&mut buf, &mut body, len).await?;
            file.append(&Frame::new(FrameKind::RaftEntry, index, buf.split_to(len).freeze()))?;
        }
        file.finish()
    }
    .await;
    match result {
        Ok(()) => Ok((head, path)),
        Err(e) => {
            let _ = std::fs::remove_file(&path);
            Err(e)
        }
    }
}

/// Largest Raft or internal RPC body accepted (a snapshot transfer is streamed and has no limit).
pub const MAX_RPC_BODY: usize = 1 << 30;

#[derive(Clone)]
struct ServerState {
    id: NodeId,
    raft: Raft,
    /// Where snapshot files live: a snapshot being received is written here.
    snap_dir: std::path::PathBuf,
}

/// HTTP routes for incoming Raft RPCs.
pub fn router(id: NodeId, raft: Raft, snap_dir: std::path::PathBuf) -> Router {
    Router::new()
        .route("/raft/{target}/append", post(append))
        .route("/raft/{target}/vote", post(vote))
        .route("/raft/{target}/snapshot", post(snapshot))
        // Group-committed entries carry whole Arrow batches: allow large bodies (axum's 2 MB
        // default would make replication of big writes fail forever).
        .layer(axum::extract::DefaultBodyLimit::max(MAX_RPC_BODY))
        .with_state(Arc::new(ServerState { id, raft, snap_dir }))
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

async fn snapshot(State(s): State<Arc<ServerState>>, Path(target): Path<NodeId>, body: axum::body::Body) -> HttpResponse {
    if let Some(r) = wrong_target(&s, target) {
        return r;
    }
    let (head, file) = match receive_snapshot(&s.snap_dir, body).await {
        Ok(r) => r,
        Err(e) => return bad(e),
    };
    let data = SnapshotData { meta: head.state, entries: SnapshotEntries::File(file.clone()) };
    let snap = openraft::storage::Snapshot { meta: head.meta, snapshot: data };
    let result = s.raft.install_full_snapshot(head.vote, snap).await;
    // An installed snapshot has been given its own name; anything left is one that was refused.
    let _ = std::fs::remove_file(&file);
    match result {
        Ok(r) => reply(&r),
        Err(e) => fail(e),
    }
}
