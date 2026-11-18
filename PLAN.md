# doris-ingress — Design Plan

> Status: revision 2 (after design review) · Date: 2026-11-13
> Section 15 is the decision log; Section 16 lists what is still open.

## 1. Summary

`doris-ingress` is a Rust, replicated, in-memory **write buffer** in front of Apache Doris
**Unique Key Merge-on-Write (MOW)** tables.

- Clients write to doris-ingress (Stream Load–compatible HTTP, Arrow Flight SQL / ADBC).
- Writes are replicated via Raft, acknowledged on commit, held in memory.
- doris-ingress loads buffered data into Doris in large batches (Stream Load or Broker Load).
- Doris queries doris-ingress through its **ADBC catalog** (upcoming Doris feature) for the latest
  version of buffered keys, including tombstones, and merges it with its own table.

### Goals
1. Take very frequent writes off Doris: many small writes become few large loads.
2. Low-latency acknowledgement at millions of rows/sec.
3. **Read-after-write from the Doris side for acknowledged writes**, via the merged view.
4. No acknowledged write is lost (subject to Raft majority durability).
5. Drop-in target for the Flink Doris Connector.

### Consistency statement
- Guaranteed: a write acknowledged before a merged query starts is visible to that query, provided
  the buffer scan opens within the table's **grace period** of Doris planning the query (§7.4).
- **Not** a goal: snapshot consistency between doris-ingress and Doris. For a key written *while* a
  merged query runs, that query may return the old row, the new row, both, or (rarely) neither.
  The next query is correct. A deduplicating view (§10) removes the both/neither cases.

### Non-goals (v1)
- Not a general database; Doris is the system of record.
- No partial-column updates; full-row upserts and PK deletes only.
- No SQL DML, no MySQL protocol, no JDBC ingest, no clean cache (all cut from v1).
- No cross-Raft-group transactions.
- Duplicate / Aggregate key models and MOR unique tables are not supported.

## 2. Architecture overview

```
Flink / HTTP ──► Stream Load API ─┐
ADBC ──────────► Flight SQL ──────┼─► admission (memory governor, table state)
                                  │        │
                                  │        ▼
                                  │   leader: validate → Raft propose → WAL (raft-engine)
                                  │        │ commit
                                  │        ▼
                                  │   apply(): assign seq → memtable per table
                                  │        │                 (Arrow batches + PK index)
        Flight SQL reads ◄────────┘        │ rotate
                                           ▼
                                  flusher (leader, self-clocking, 1 load in flight per table)
                                           │  Loader: Stream Load | Broker Load (Parquet)
                                           ▼
                                  VISIBLE → FlushDone → grace → Drop → log trim
Doris query ──► merged view: Doris table + doris-ingress delta via ADBC catalog
```

## 3. Data model & write semantics

- **Eligible tables:** Doris Unique Key, `enable_unique_key_merge_on_write = true`, sequence column
  configured (mapped column or hidden `sequence_type` BIGINT), `light_schema_change = true`.
- **The sequence column is owned by doris-ingress.** Tables that use a business sequence (e.g.
  event time) are not eligible, and **all writers must go through doris-ingress**; a direct write to
  Doris breaks the merge ordering.
- Writes are full-row upserts or deletes by primary key. The write path never reads from Doris.
- Rejected in v1: partial-column loads, `UPDATE`, non-PK deletes, conditional writes,
  Doris-generated values (`AUTO_INCREMENT`, `DEFAULT CURRENT_TIMESTAMP`).
- Delete affected-rows is reported as "keys tombstoned".

### Sequence number (`seq`)
- A **counter in the Raft state machine** (`next_seq`). When a commit is applied, row *i* of the
  batch gets `next_seq + i` and the counter advances by the row count. Deterministic, no ties,
  row order within a batch preserved (a later duplicate key in the same batch wins).
- The same counter is the MVCC read point: a read at snapshot *S* ignores versions with seq > *S*.
- **Bootstrap is mandatory:** on attach, cluster rebuild, or moving a table between groups, bump
  the counter to at least `max(seq_col) + 1` read from Doris. This replaces the epoch scheme.
- Tombstones carry a seq too. Per-key ordering assumes one writer per key (e.g. Flink `keyBy(pk)`).

## 4. Replication & durability (Raft)

- **Library:** `openraft` 0.10 for consensus; **log storage is our own WAL** (§5). The log store and
  state machine pass openraft's storage conformance suite (`tests/storage_suite.rs`).
- **Transport:** HTTP + MessagePack; every RPC names the target node id, a mismatch is refused.
- **Coordinated writers (group commit, two levels):**
  1. *Batcher:* concurrent writers are collected into one `Write` Raft entry (closed at 4 MB or
     2 ms), up to 8 entries in flight. Each writer waits until its entry is committed by a majority
     and applied, then gets its own sequence range.
  2. *WAL IO thread:* all appends queued behind one another are written and made durable with a
     single `fdatasync`; their callbacks fire only after it.
- **Group layout:** v1 = single Raft group for all tables; a `table → group` map exists from day one.
- Ack on commit (majority fsync). Group commit by size or time (e.g. 1 MB / 2 ms), pipelined.
- **Apply is deterministic:** no wall clock, no Doris reads, no hash-iteration order. Leader-decided
  values (rotation points, purges, drops) are carried in entries.
- CSV/JSON Stream Load bodies are **parsed to Arrow on the leader before proposing**; the entry
  payload is always Arrow IPC, so parse errors never surface at apply.

### Raft entry types (sketch)
```
Write        { [(table_id, arrow_ipc, is_delete)] }
Stage        { txn, table_id, arrow_ipc }        // Stream Load 2PC
Commit       { txn }                             // seq assigned here; rows become visible
Abort        { txn }
Rotate       { table_id }                        // freeze active memtable
FlushStart   { table_id, seq_start, seq_end, label }
FlushDone    { table_id, seq_end }
Drop         { table_id, seq_end }               // after grace; leader-timed
PurgePartition { table_id, partition }           // partition dropped in Doris
BumpSeq      { min_next_seq }
SchemaChange { table_id, version, columns }
TableAttach / TableDetach / Drain / Resume
```

## 5. WAL & trimming

### 5.1 Container format (`src/wal/format.rs`)
Every file (log segment, `raft-meta`, snapshot) = **file header** + **frames**, little endian.
- **File header:** magic `DIWAL\0\0\x01`, `format_version`, `min_reader_version` (a reader refuses
  files that need a newer reader), `header_len` (new fixed fields can be appended; old readers
  skip them), file kind, flags, first index, creation time, the **build tag of the writer**
  (`doris-ingress <version> (git <sha>)`), TLV extensions, CRC32C.
- **Frame:** magic `0xD1F0`, `frame_version`, `kind` (raft entry / raft meta / snapshot meta),
  `codec` (1 = MessagePack with named fields, so struct fields can be added compatibly), `flags`
  (reserved for compression), TLV extension block, index, body length, body, **CRC32C over the
  whole frame**. Unknown TLV tags are skipped; unknown kinds/codecs/flags are errors.
- Tested: every single-bit flip in a frame is detected; every truncation is reported as torn.

### 5.2 Segments and recovery (`src/wal/mod.rs`)
- Segments named by first index, rolled at 100 MiB (`wal_segment_mb`); one IO thread executes appends, truncation,
  purge and metadata writes in submission order (a vote write never overtakes a log write).
- Reads verify the CRC of every frame read.
- On open, a corrupt or partial suffix of the **last** segment is a torn write and is truncated;
  damage in any other segment fails startup.

### 5.3 Snapshots by reference and trimming
- A snapshot is metadata only: tables, sequence counter, flush-unit states, and for every buffered
  row the `(log index, op, base seq)` it came from. Building one writes a few KB.
- The log store purges logically on openraft's request but deletes segments only below the
  **retention floor** = oldest entry still referenced. Flushed-and-dropped data releases its
  entries, so the WAL is trimmed as flushing progresses (flush-driven trim, D12).
- Restart: load the snapshot, rebuild memtables from the referenced entries, replay the rest.
- A lagging follower receives the metadata plus the referenced entries; it embeds them in its own
  snapshot file, so it can restart before its own WAL holds them.
- A stuck table pins memory before it pins disk; the per-table cap (§8) bounds both.

## 6. In-memory storage

```
per table:
  active memtable          ← apply() writes
  frozen memtables [..]    ← each has fixed [seq_start, seq_end]; the flush unit
memtable = append-only Arrow RecordBatches + PK index: encoded key → versions (seq, batch, row)
```

- **Columnar, uncompressed in memory:** writes keep the Arrow batch as received (no per-row
  allocation); the index holds row locations. Measured ~155 B/row for a 2-column table with
  small rows, dominated by the per-key index entry — worth optimising (open item O8).
- WAL entries and replication carry Arrow IPC, uncompressed for now; the frame `flags` field is
  reserved for LZ4/ZSTD (O9). Loads to Doris are Snappy-compressed Parquet.
- A tombstone is a row with only key columns set, flagged deleted.
- Read order: active → frozen (newest first); first hit at or below the snapshot wins.
- Mirror Doris **schema** and **partition metadata** only; not tablets or buckets.

## 7. Flush pipeline

### 7.1 Self-clocking flusher
- **One queue per table, tables load in parallel.** Each table loads one unit at a time (units of
  a table are loaded in order); different tables run concurrently, capped node-wide by
  `max_concurrent_loads` (default 4, below Doris's per-DB running-transaction limit). Loads are
  never combined across tables: a Stream Load / Broker Load job targets one table, and merged
  readers do not need cross-table atomicity because rows stay buffered until their own load is
  visible.
- **One load in flight per table.** While it runs, writes keep accumulating.
- When no load is in flight, start one if the active memtable meets a trigger:

| Setting | Default | Purpose |
|---|---|---|
| `flush.bytes` | 256 MB | Start a load once this much is buffered |
| `flush.interval` | 2 s | Start a load once the oldest unflushed row is this old |
| `flush.max_load` | 1 GB | Rotate at this size even while a load is in flight (caps one load) |

- Under pressure the previous load is still running when the triggers fire, so the next batch is
  simply larger (up to `max_load`). **Memory pressure never causes earlier or smaller flushes**;
  Doris needs time to absorb, and larger batches are the efficient response.
- Tune `max_load` in the load test: a larger load takes longer to become VISIBLE and costs more to
  redo after failover.

### 7.2 Loaders
Both loaders take the same flush unit, encoded once as **Parquet**: user columns, the sequence
column, and a delete-flag column. The loader is chosen per table.

| | Stream Load (HTTP) | Broker Load |
|---|---|---|
| Path | `PUT /api/{db}/{table}/_stream_load`, data in the request body | Upload Parquet to object storage, then `LOAD LABEL … WITH S3` via the MySQL port |
| Completion | Synchronous response; poll `get_load_state` on `Publish Timeout` / `Label Already Exists` | Asynchronous; poll `SHOW LOAD WHERE LABEL = …` until `FINISHED` / `CANCELLED` |
| Deletes / seq | `merge_type: MERGE`, `delete: <flag>=1`, `function_column.sequence_col` | `MERGE … DELETE ON <flag>=1 ORDER BY <seq>` |
| Needs | FE/BE HTTP reachability | Object storage Doris can read, FE MySQL port |
| Fits | Low latency, moderate batch size | Very large batches; data does not pass through FE/BE HTTP |

### 7.2a Flush protocol (label → intent → load → label check → done)
All steps are Raft entries, proposed by the leader only:
`Rotate` (freeze) → `FlushStart{label}` → load → `FlushDone` → grace → `Drop`.
1. **Label** is deterministic: `ingress_{db}_{table}_{seq_start}_{seq_end}` (a `_g{generation}`
   suffix is added once partition purge exists, §9). Every retry reuses it.
2. **Intent** = the `FlushStart{label}` entry, committed on a majority *before* anything is sent.
3. **Submit** via the table's loader. A `Failed` answer counts as a failure (retry, same label).
4. **Green only from the label:** whatever submit returned, the unit is done only when a label-state
   lookup says VISIBLE (Stream Load `get_load_state`) / FINISHED (Broker Load `SHOW LOAD`).
   A label still unknown after 10 polls is a failure.
5. **Done** record appended; staging files cleaned up; grace timer starts.
6. **After a crash or failover**, the new leader finds the unit in `Loading{label}` (replicated)
   and asks Doris for the label first: visible → `FlushDone` without resubmitting; running → keep
   polling; unknown/failed → submit again under the same label. The rows are still in the
   replicated memtables, so nothing is lost.
- Broker Load staging objects are deleted after the load finishes.
- Stream Load can target a BE directly (`be_url`): the FE redirects only after the body is sent, and
  the HTTP client cannot wait for `100-continue`, so going through the FE uploads twice.

### 7.3 Idempotency
- Duplicate label → Doris reports the existing job; poll it to a final state.
- Different label, same rows → no-op via the sequence column.
- **Must test:** insert@10, delete@20, **compaction**, replay insert@10 → row stays deleted.

### 7.4 Grace period and drop
- After VISIBLE, a memtable is retained for the table's **grace period** (configurable, default
  **10 s**), then dropped via a `Drop` Raft entry proposed by the leader.
- Why: Doris pins tablet versions at plan time and opens the buffer scan later. The grace must
  cover planning → buffer scan open (queue time, runtime-filter wait), not total query duration:
  once open, a scan holds its memtables.
- **After failover or restart the grace timer restarts**; a memtable is never dropped immediately.
- Cost: `ingest_rate × grace` of already-flushed data per replica (≈2 GB at 1M rows/s, 200 B/row).

## 8. Memory governor

A **target memory** per node, enforced at admission on the leader (apply never blocks).

| Level | Behaviour |
|---|---|
| Below soft watermark (default 70 %) | Admit immediately |
| Soft → hard | Pace writers: delay grows linearly with usage |
| At hard (= target) | Stop reading from writers until memory frees; after a bounded wait, retryable error |

- Delaying = not reading the HTTP body / Flight stream, so TCP and gRPC push back on the client.
- **Per-table cap:** a table over its cap, or whose flush has failed N times in a row, is marked
  stuck and **rejects writes upfront** with an error naming the table and cause. A batch touching
  a stuck table is rejected whole. Other tables are unaffected.
- `Commit` / `Abort` of staged transactions always bypass the governor (they free memory).
- "Doris unreachable" is reported as a distinct metric and error from plain overload.
- Leave headroom above the target for followers and for readers holding dropped memtables.

## 9. Partition policy

- **At admission:** partition exists → accept. Auto-partition table → accept without checking.
  Otherwise refresh the partition list from Doris once; still missing → reject the batch with
  Doris's "no partition for this tuple" error.
- **At flush, partition dropped after ack:** propose `PurgePartition`, which removes that
  partition's rows from the memtables on all replicas; count them in a metric and a log line;
  re-flush the rest under a new label generation. Same end state as loading then dropping.
- `max_filter_ratio` is **not** used to let Doris filter silently.
- `TRUNCATE` / partition overwrite in Doris while rows are buffered requires `DRAIN` first.

## 10. Read path & Doris integration

- **Interface:** Arrow Flight SQL, consumed by the Doris ADBC catalog and by direct clients.
- Scan returns the latest version per key at the read point, with `__seq` and `__deleted`.
- Pushdown: PK `=` / `IN` / range, partition predicates, projection (incl. key-only).
- Consistency inside doris-ingress: linearizable via batched ReadIndex; session tokens as an
  optimisation.
- Query engine: DataFusion with a custom `TableProvider` (needed only for filter/projection).

Two merged views are offered per table, on the Doris side:

```sql
-- Fast (default): pushes predicates into both sides. A key written during the query may appear
-- twice or not at all for that one query.
SELECT d.* FROM internal.db.t d LEFT ANTI JOIN ingress.db.t b ON d.k = b.k
UNION ALL
SELECT <cols> FROM ingress.db.t b WHERE b.__deleted = false;

-- Deduplicating: one buffer scan, buffer row wins. Always consistent, costs a join on every row
-- and loses non-key predicate pushdown into the Doris side.
SELECT IF(b.k IS NOT NULL, b.c1, d.c1) AS c1, ...
FROM internal.db.t d FULL OUTER JOIN ingress.db.t b ON d.k = b.k
WHERE b.k IS NULL OR b.__deleted = false;
```

## 11. Ingest interfaces

### 11.1 Stream Load–compatible HTTP API (Flink connector target)
- `PUT /api/{db}/{table}/_stream_load`, `PUT /api/{db}/_stream_load_2pc`,
  `GET /api/{db}/{table}/_schema`, `GET /api/backends`, label-state lookups.
- Doris-shaped JSON responses; label table with retention and Doris dedupe semantics.
- Reject clearly: `partial_columns`, complex `columns` expressions, unsupported options.
- **2PC terminates at doris-ingress; Doris never sees it.** Staged data is durable but invisible
  until `Commit`. A **per-transaction size cap** fails oversized transactions, and abandoned
  `PRECOMMITTED` transactions **expire** after a configurable time.
- Capture a real Flink↔Doris session as a compatibility fixture.

### 11.2 Arrow Flight SQL
- Bulk ingest (`CommandStatementIngest` via `DoPut`) = upsert by PK; `__delete` reserved column.
- Per-batch `PutResult` after Raft commit; gRPC flow control is the backpressure.

## 12. Schema management

- `ATTACH TABLE db.t` imports schema from Doris and validates eligibility (§3).
- Change detection: unknown column on write, before each flush, periodic poll.
- Change → `SchemaChange` entry → rotate so each flush unit has one schema.
- Additive changes are automatic; breaking changes need `DRAIN` → ALTER → refresh → `RESUME`.

## 13. Security

- Flight SQL username/password (basic auth → bearer token), **TLS required**.
- Stream Load HTTP endpoint checks the same users via HTTP basic auth.
- Raft inter-node port: network-restricted or mTLS.
- Doris credentials (HTTP, MySQL port, object storage) from config/env, never logged.

## 14. Failure handling

| State at crash / failover | Handling |
|---|---|
| `FlushStart` without `FlushDone` | Re-run the same range with the same label |
| `FlushDone`, grace pending | **Restart the grace timer**, then `Drop` |
| Staged 2PC txn | Stays staged until commit / abort / expiry |
| Uncommitted log tail | May be truncated by the new leader; never acked |
| Load returns `Publish Timeout` or `Label Already Exists` (`RUNNING`) | Poll to a final state |
| Flush fails N times | Table marked stuck; writes rejected; flusher keeps retrying |

- Majority disk loss → unflushed data lost; place replicas across failure domains.
- Test plan: kill -9 injection (mid-stage, mid-flush, between FlushStart and FlushDone) and compare
  buffer ∪ Doris against a reference model.

## 15. Decision log

| # | Decision | Source |
|---|---|---|
| D1 | Read interface is Flight SQL via Doris's upcoming ADBC catalog; no MySQL protocol | User |
| D2 | No snapshot consistency across systems; anomalies on concurrent writes are acceptable | User |
| D3 | Grace period is configurable per table, default 10 s | User |
| D4 | Target memory with watermarks; delay writers, never flush smaller under pressure | User |
| D5 | Stuck tables reject writes upfront | User |
| D6 | Seq is a Raft-assigned counter, not `(epoch << 40) \| raft_index` | User |
| D7 | Loading into Doris supports both Stream Load and Broker Load | User |
| D8 | Security: ADBC/Flight SQL with password | User |
| D9 | 2PC is internal to doris-ingress (read as: terminates here; cap + expiry added) | User, interpretation to confirm |
| D10 | Partition policy as in §9 | Proposed in review, **to confirm** |
| D11 | Memtable = Arrow batches + PK index (was: row skiplist) | Proposed in review, **to confirm** |
| D12 | Flush-driven log trim, on-demand snapshots (was: snapshot-driven) | Built: each node snapshots within 1 s of a `Drop` |
| D13 | Cut from v1: clean cache, SQL DML, JDBC ingest | Proposed in review, **to confirm** |
| D14 | Flush protocol: deterministic label, replicated intent, success only from label state, crash check (§7.2a) | User |
| D15 | Raft and WAL are first class: own WAL with CRC32C, versioned container format with build tag, group commit, writers wait on commit | User |
| D16 | Flush queues are per table (ordered), tables load in parallel under a node-wide cap; no cross-table loads | Answer to user question |

## 16. Open items

| # | Item | How to close |
|---|---|---|
| O1 | Does Doris group commit already meet the target rate? | Benchmark before the Raft phase |
| O2 | ADBC catalog: predicate and runtime-filter pushdown, when the scan opens relative to planning, option passthrough | Test when it ships |
| O3 | Tombstone replay after compaction (§7.3) | Integration test |
| O4 | Stream Load details: FE→BE redirect, `get_load_state` response shape, Parquet + `MERGE` with a placeholder delete column (`__ingress_del`, not a table column), `function_column.sequence_col` only for hidden-sequence tables | Test against the Doris version in use |
| O5 | Broker Load `MERGE … DELETE ON … ORDER BY` with Parquet from S3; FE job concurrency limits | Same |
| O6 | `max_load` ceiling and tablet version counts under sustained load | Load test |
| O7 | Flight SQL bulk ingest support in chosen crate/driver versions | Read crate docs |
| O8 | Memtable memory overhead (~155 B/row for small rows, index-dominated) | Profile; compact index |
| O9 | Arrow IPC compression (LZ4/ZSTD) for WAL and replication | Benchmark CPU vs bytes |
| O11 | Flight endpoint has no TLS yet (password travels in clear); followers answer writes with `UNAVAILABLE` + leader Raft address, not its Flight address | Add `ServerTlsConfig`; advertise Flight address in membership |
| O12 | Doris ADBC catalog query shapes and pushdown against our Flight SQL server | Test when the catalog ships |
| O10 | VeloDB reports an unknown label as an error ("has not found"); mapped to Unknown — confirm across Doris versions | Test |

## 17. Implementation phases

0. **Status 2026-11-13:** phases 1–4 done (core, Flight SQL/ADBC, Raft + WAL), plus a server
   binary (`src/main.rs`, `config.example.toml`) that attaches tables from Doris DDL. Verified end
   to end with the Python ADBC Flight SQL driver → doris-ingress → VeloDB Stream Load (HTTPS).
   Next: Stream Load–compatible HTTP ingest (Flink), 2PC, TLS on the Flight endpoint.
1. **Core engine (done, 2026-11-13):** seq counter, memtable, rotation,
   grace, self-clocking flusher, flush intent log, Stream Load and Broker Load loaders, memory
   governor, in-process read/write API. Tested against a mock loader and a mock FE/BE HTTP server;
   ~4.3M rows/s single node (`examples/write_bench.rs`). Not yet run against real Doris.
   Known limitation: scans select keys under the table lock, briefly blocking writes to it.
2. **Interfaces:** Flight SQL (done): basic-auth handshake → bearer token; `DoPut` bulk ingest
   (upsert, `__delete` column for deletes, safe type casts); SQL via DataFusion over catalog
   `ingress` with PK `=`/`IN` pushdown; tombstones and `__seq` in results; catalogs / schemas /
   tables / primary keys / SqlInfo; prepared statements with `$n` parameters; key-index fast path
   for PK `=`/`IN` queries; plan cache; one snapshot per query; DDL/DML refused.
   Clients verified: Python ADBC Flight SQL driver 1.13, Apache Arrow Flight SQL JDBC driver
   19.0.0 (`jdbc:arrow-flight-sql://host:port/?useEncryption=false&user=..&password=..`; on JDK 17+
   it needs `--add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow
   --enable-native-access=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true`).
   Point lookups: ~28k QPS / p50 0.75 ms (10M rows, 64 clients, one node, client on same host).
   **Any node serves clients:** followers forward writes to the leader (internal endpoint on the
   Raft port) and serve linearizable reads via a coalesced read-index request to the leader.
   Drivers do not fail over by themselves: give clients all node addresses (or a load balancer /
   DNS name over them) and reconnect to another node on error.
   3 nodes on one 16-core host (br0 address, kernel-local routing), 10M rows: writes 1.2M rows/s
   (1k-row batches, p50 13 ms) to 1.7M rows/s (10k-row batches); reads ~19–21k QPS per node,
   26.6k QPS across all three; kill -9 of the leader under load: ~2 s dip, errors only on clients
   connected to the killed node, all nodes identical afterwards.
   Row-by-row inserts (one row per `DoPut`), 3 nodes, clients over all nodes: 1 client 610 rows/s
   (p50 1.6 ms); 64 clients 16k; 256 clients 33k; 1,024 clients 49k rows/s. Concurrent writers
   share Raft entries (group commit) but succeed or fail individually; followers batch forwarded
   writes into one internal request. Batch linger defaults to 0 (pipeline-driven batching).
   After write-path tuning (schema-less batch encoding, `WriteOpWire.format = 1`; merging
   consecutive same-table ops at apply; cached leadership; tables map off the apply lock):
   1,024 single-row writers 56k rows/s (p50 17 ms, p99 31 ms); 10k-row batches 2.1M rows/s.
   Node-to-node traffic stays HTTP/1.1 keep-alive: HTTP/2 multiplexing measured 3x slower.
   All async (Tokio); high-concurrency latency is queueing at the throughput ceiling (Little's
   law), dominated by per-entry commit time (replication + fsync, 3 nodes sharing one disk here).
   Stream Load–compatible HTTP API: not started.
3. **Doris integration tests:** O3–O6 against a real cluster; merged views via the ADBC catalog.
4. **Raft (done, 2026-11-13):** openraft + own WAL, group commit, deterministic apply, snapshots
   by reference, flush-driven trim, ReadIndex barrier, follower read tokens. 3-node failover and
   snapshot catch-up tested over real HTTP. ~3.1M rows/s on one node with fsync
   (`examples/write_bench.rs`).
5. **2PC, labels, Flink fixture tests.**
6. **Schema and partition management:** ATTACH, change detection, DRAIN/RESUME, PurgePartition.
7. **Hardening:** metrics, crash injection, linearizability checks, security.
8. **Scale-out (if needed):** multiple groups.
