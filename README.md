# doris-ingress

> **Status: proof of concept.** The core works end to end and is tested, but it has not been
> hardened for production (see [Verdict](#verdict) and [Known gaps](#known-gaps)).

A Raft-replicated, in-memory write buffer in front of Apache Doris **Unique Key, merge-on-write**
tables. It is meant to solve three problems:

1. Doris struggles with very frequent small writes.
2. Data written to Doris takes too long to become visible.
3. Reads issued right after a write need to see that write ("read after write").

Clients write rows over **Arrow Flight SQL**, through **ADBC** or **JDBC**. Rows are acknowledged
once a majority of nodes has written them to disk, and you can query them with SQL right away.
In the background the buffer is flushed to Doris in large batches, by **Stream Load** (HTTPS) or
**Broker Load** (S3). Each load runs under an idempotent label, and the buffer drops its copy
only after Doris reports the load visible plus a grace period (default 10 s).

The intended read path from Doris is a merged view over the Doris table and this buffer, via the
Doris ADBC catalog, with the buffer taking priority. That catalog has not been tested yet, because
Doris's ADBC catalog support is not yet available.

```
 writers (ADBC / JDBC / Flight SQL)          readers (ADBC / JDBC / Flight SQL, SQL)
            │                                          │
            ▼                                          ▼
 ┌──────────────────── doris-ingress (3 nodes, Raft) ──────────────────────┐
 │  group commit → Raft log (own WAL, CRC32C, fsync) → apply → memtables   │
 │  SQL: DataFusion over in-memory Arrow, one consistent snapshot per query│
 │  flusher: label → intent (Raft) → load → poll label → visible → done    │
 └──────────────────────────────────┬──────────────────────────────────────┘
                                    │ Stream Load (HTTPS) / Broker Load (S3)
                                    ▼
                 Apache Doris (Unique Key, MOW, sequence column)
```

The full design, decision log and open items are in [PLAN.md](PLAN.md).

## Technology

| Area | What is used |
|---|---|
| Language / runtime | Rust (edition 2024), fully async on **Tokio** |
| Consensus | **openraft** 0.10, with this project's own log storage and state machine; passes openraft's storage conformance suite |
| Write-ahead log | Own segmented WAL: **CRC32C** on every frame and header, a versioned container format with a build tag (git SHA) in every file, group-commit `fsync` on a dedicated IO thread |
| In-memory data | **Apache Arrow** columnar record batches (append-only) plus a primary-key hash index (Arrow `RowConverter`); versions tracked by sequence number |
| Wire encoding | Arrow IPC (schema-less record-batch messages) inside Raft entries; MessagePack (`rmp-serde`) for metadata |
| SQL parsing / execution | **Apache DataFusion** 55 (uses `sqlparser-rs`): custom catalog, schema and table providers; primary-key `=`/`IN` filter pushdown; a logical-plan cache keyed by SQL text; DDL and DML refused, except `INSERT` (upsert) and `DELETE` by primary key |
| Client protocol | **Arrow Flight SQL** (`arrow-flight`, **tonic** gRPC): basic-auth handshake → bearer token, `DoPut` bulk ingest (with a `__delete` column for deletes), SQL queries, prepared statements with `$n` parameters, metadata calls (catalogs/schemas/tables/primary keys/SqlInfo), a fast path that answers primary-key lookups directly from the index |
| Client drivers verified | **ADBC** Flight SQL driver (Python `adbc-driver-flightsql` 1.13), **Apache Arrow Flight SQL JDBC driver** 19.0.0, Rust `arrow-flight` client |
| Node-to-node traffic | HTTP/1.1 keep-alive (**axum** server, **reqwest** client), MessagePack bodies; followers batch forwarded writes and leader check-ins for reads |
| Doris loading | Stream Load over HTTPS (Parquet body, `merge_type=MERGE`, label-based idempotence); Stream Load through the [`doris_rust_stream_load`](https://github.com/wushilin/doris_rust_stream_load) SDK (CSV, gzip, several workers; vendored in `vendor/`); Broker Load (Parquet on S3 via `object_store`, then `LOAD LABEL` over MySQL protocol) |
| Doris metadata | `mysql_async` over TLS (**rustls** with aws-lc-rs): table schema read from `SHOW CREATE TABLE`, sequence floor via `max(__DORIS_SEQUENCE_COL__)` |
| Files to Doris | **Parquet** (Snappy) |

## What was built

- **Consistent writes:**
  - Every write gets a sequence number from a single Raft-replicated counter. That number becomes
    the Doris sequence column, so the newest write wins in both the buffer and Doris.
  - Group commit: concurrent writers share Raft entries, yet each writer is accepted or rejected
    on its own.
  - Up to 8 proposals are pipelined in flight.
  - Any node accepts writes: followers forward them to the leader in batches.
- **Durability:** a write is acknowledged only after it is committed, which means a majority of
  nodes have fsynced it to the WAL. Crash recovery truncates a torn tail write; corruption
  anywhere else is a hard error.
- **Reads:**
  - Linearizable by default, on any node. Each query confirms with a majority before answering
    (Raft ReadIndex), and followers send these check-ins to the leader in batches.
  - Each query sees one consistent snapshot, across the active memtable and those being flushed.
- **Flushing to Doris:**
  1. The label is generated.
  2. The flush intent is recorded through Raft.
  3. The load is submitted.
  4. The label is polled until Doris reports the load visible.
  5. The flush is marked done through Raft.
  6. After the grace period, the flushed memtable is dropped.

  After a crash or leader change, the new leader checks the label in Doris before resubmitting.
  The same label is reused, so loads are idempotent. Under memory pressure the flusher sends
  larger batches, never smaller ones.
- **Backpressure** (writes only; reads are never delayed):
  - From 70% of a table's memory cap every write to it is delayed: 10 ms at 70%, doubling for
    every further 5% (20 ms at 75%, 40 ms at 80%, ... 640 ms at 100%), up to 10 s. The cap is
    soft: writes are held only at 120% of it, until loads to Doris free space.
  - The node's memory target slows writes the same way up to 320 ms, and holds them at 100%.
  - The caps apply to *buffer memory*: the buffered rows plus their key index. The process as a
    whole uses more (see `process_memory_bytes` in `system.node`): allocator overhead, queries in
    flight, and on the leader the files being built for a load, which can be several times the
    size of the unit being loaded. Leave headroom for that when setting caps.
  - A monitor works the delay out every 100 ms; a write only reads the published number.
  - A write held longer than `max_admission_wait_s` (default 60) gets a retryable error. Only a
    table whose loads keep failing rejects new writes upfront.
- **Snapshots by reference** let the WAL be trimmed only once data is visible in Doris. A lagging
  follower catches up from a snapshot.
- **Server binary** configured with TOML. Tables are attached by reading their definitions from
  Doris.

## How to run

### Build and test

```bash
cargo build --release
cargo test --release            # 68 tests; the 2 VeloDB/Doris tests skip unless VELODB_* is set
```

Real-Doris tests (they create and drop their own tables in database `doris_ingress_test`):

```bash
export VELODB_HOST=... VELODB_USER=... VELODB_PASSWORD=... \
       VELODB_STREAM_LOAD_PORT=... VELODB_MYSQL_PORT=... VELODB_TLS=true
cargo test --release --test velodb
```

### Run a cluster

1. Each Doris table must be **Unique Key**, **merge-on-write**, with a **sequence column**
   (hidden `function_column.sequence_type` or a mapped column).
2. Copy [config.example.toml](config.example.toml) once per node. Set `node_id`, `raft_listen`,
   `raft_advertise`, `flight_listen` and `data_dir`, and give every node the same
   `initial_members`, `[doris]` and `[[tables]]`. Secrets can be given as `env:VAR`.
3. Start every node:

   ```bash
   export DORIS_PASSWORD=... DORIS_INGRESS_ADMIN_PASSWORD=...
   ./target/release/doris-ingress node1.toml
   ```

   On first start, the node with the lowest id initializes the cluster. The leader then attaches
   the configured tables, reading their schema and current sequence from Doris.

A single node works too: list only itself in `initial_members`.

### Choosing a loader

Set `loader` per table:

| `loader` | How a flush unit reaches Doris | One Doris transaction per unit | After a crash or leader change |
|---|---|---|---|
| `stream` (default) | One Stream Load, Parquet body, under the unit's own label | Yes | Doris is asked about the label first; the unit is resubmitted only if needed |
| `stream_sdk` | Several Stream Loads through the `doris_rust_stream_load` SDK: CSV rows, gzip, 4 workers, 5 ms linger, 80 MiB batches (all in `[doris.sdk]`) | No: the unit is split into several loads | The unit is sent again in full; repeated rows are no-ops through the sequence column |
| `broker` | Parquet on S3, then Broker Load | Yes | As for `stream` |

The `stream_sdk` loader has a dry run (`dry_run = true` under `[doris.sdk]`): nothing is sent to
Doris, and every upload is acknowledged after `dry_run_delay_ms` (default 500). The whole flush
path still runs, so it is a way to test the buffer and its backpressure without a Doris cluster.
The flushed rows are discarded.

With every loader a unit counts as done only when Doris reports its loads VISIBLE, and its rows
stay in the buffer until then, plus the grace period. Only the leader loads. A load that is not
visible after `load_timeout_s` (default 120) is abandoned and tried again, so a stalled connection
cannot leave flushing dormant; `system.tables.load_running_ms` shows how long the current load
has been running. Each drop is logged on every node
(`trim: dropped a unit that is visible in Doris`, with the label and sequence range).

**Changing a table's settings.** Edit the table's entry in the config file of the nodes and
restart them (one at a time is fine). The leader compares the file with the settings in force and
replicates the change, so every node switches at the same point in the log and the change
survives restarts. `system.tables` shows the cap and grace period in force.

**A follower that is behind.** The leader keeps the log a follower still needs for as long as
that follower keeps answering (a catch-up cursor per follower), so a follower that was briefly
away, or one that is installing a snapshot, continues from the log afterwards instead of needing
another snapshot. A follower silent for `catch_up_idle_s` (default 30) loses its hold, as does
the one furthest behind once the WAL kept for followers exceeds `catch_up_hold_mb` (default
2048); such a follower is sent a snapshot when it returns. A follower that needs a snapshot holds
the log from exactly the snapshot it is sent, so it continues from the log once it is installed. With all followers level, the leader
purges everything its snapshot covers, as before.

**Snapshot transfer.** A snapshot for a follower is streamed: the leader reads the entries
behind the buffered rows from its WAL one at a time as it sends them, and the follower writes
them to its snapshot file as they arrive, so the size of a snapshot is not limited by memory or
by a request size. A send is paced to `snapshot_send_mb_per_s` (default 200). The WAL files a
snapshot refers to are kept until it has been sent, even if newer snapshots are taken meanwhile.

**Trim and watermarks.** Trimming is a Raft command (`Drop`): every node drops the same unit at
the same point in the log. Each table has two replicated watermarks, `visible_seq` (highest
sequence visible in Doris) and `trimmed_seq` (highest sequence trimmed from the buffer). They
change only when a Raft entry is applied, are written into every snapshot, and so survive
restarts and WAL trimming. WAL files are deleted per node, a whole file at a time, once a
snapshot covers them and no buffered row still refers to them. Every node takes a snapshot
(metadata only, a few KB) within a second of a trim, so the freed files go promptly; a snapshot
is also taken every 2,000 log entries. A WAL file is closed at 100 MiB (`wal_segment_mb`). Both watermarks are columns of
`system.tables`.

### Connect clients

Python (ADBC):

```python
import adbc_driver_flightsql.dbapi as flightsql, pyarrow as pa
from adbc_driver_manager import DatabaseOptions
conn = flightsql.connect("grpc://node1:8815", db_kwargs={
    DatabaseOptions.USERNAME.value: "admin", DatabaseOptions.PASSWORD.value: "..."})
cur = conn.cursor()
cur.adbc_ingest("orders", pa.table({...}), mode="append", db_schema_name="db")   # upsert
cur = conn.cursor()   # use a fresh cursor for queries after an ingest
cur.execute("SELECT * FROM db.orders WHERE id = 42"); print(cur.fetch_arrow_table())
```

- **Inserts in SQL:** `INSERT INTO db.orders VALUES (...)`, with or without a column list, or
  `INSERT INTO ... SELECT ...`. It is an upsert of full rows: a row with an existing primary key
  is replaced, and a column that is left out is written as null. `__seq`, `__deleted` and
  `__delete` cannot be set. For volume, prefer bulk ingest.
- **Deletes:** add a boolean `__delete` column set to true for the rows to delete, or use SQL:
  `DELETE FROM db.orders WHERE id = 42` (also `id IN (...)`; for a composite key, `a = 1 AND b = 2`
  naming every key column). The `WHERE` clause must name the primary key and nothing else. The
  count returned is the number of keys named: a key that is not in the buffer still gets a
  tombstone, because its row may already be in Doris.
- **No `UPDATE`:** insert the full row again instead.
- **Node status in SQL:** `SELECT * FROM system.node` (role, leader, Raft log position, applied
  sequence, memory) and `SELECT * FROM system.tables` (per table: rows in memory, split into
  taking writes / waiting for Doris / in grace, plus flush failures). Each node answers for
  itself, without the read barrier.
- **Table names:** tables live in catalog `ingress`, so both `db.orders` and
  `ingress.db.orders` work.

JDBC (Apache Arrow Flight SQL JDBC driver):

```
jdbc:arrow-flight-sql://node1:8815/?useEncryption=false&user=admin&password=...
```

On JDK 17+ the driver needs these JVM flags:
`--add-opens=java.base/java.nio=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow
--enable-native-access=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true`.

### Web SQL console

`webui/` is a small Go web UI: tables and their columns on the left, a SQL editor, and the
result grid. It connects over Flight SQL and moves to the next node when one is unreachable.

```bash
cd webui && go build -o doris-ingress-ui .
./doris-ingress-ui -nodes node1:8815,node2:8815,node3:8815 -user admin -password ...   # http://127.0.0.1:8080
```

"Bulk insert…" fills a table with generated rows (consecutive ids in the key column, random values
elsewhere) through Flight SQL bulk ingest, and shows progress, speed and time left. It is limited
to 50,000 rows per second by default (0 removes the limit). It needs a table
with one integer key column.

"Bulk query…" runs parallel primary-key lookups on random ids for a set time and shows queries per
second and p50/p99 latency.

The header shows how many nodes are up and which one leads; clicking it opens a cluster overview
(per node: role, rows in memory, Raft log position, applied sequence; per table: rows in memory,
split into taking writes / waiting for Doris / in grace, and flush state), read from each node's
`system.node` and `system.tables` and pushed to the browser over a WebSocket twice a second. Console queries go to
the nodes in turn, and both bulk tools spread their work over all nodes and report per node.

The password defaults to `$DORIS_INGRESS_ADMIN_PASSWORD`. Results are capped at `-max-rows`
(default 5000). The UI has no login of its own, so keep `-listen` on localhost.

**Failover:** drivers do not fail over by themselves. Give clients every node's address (or put
a load balancer or DNS name in front of them) and reconnect to another node on error.
`examples/flight_load.rs` shows how.

### Benchmarks

```bash
# 3-node cluster on one host (no-op loader, flushing disabled), table bench.t
cargo run --release --example flight_bench_server -- id=1 raft=IP:7801 flight=IP:8801 dir=/tmp/n1 \
    peers=1=IP:7801,2=IP:7802,3=IP:7803 init=1          # repeat for id=2, id=3
cargo run --release --example flight_load -- urls=http://IP:8801,http://IP:8802,http://IP:8803 \
    mode=load rows=10000000 batch=10000 conc=16        # bulk load
cargo run --release --example flight_load -- urls=... mode=write batch=1 conc=1024 secs=10
cargo run --release --example flight_load -- urls=... mode=read conc=96 secs=10
cargo run --release --example write_bench           # in-process write path, no network
```

- **Benchmark server variables:** `APPLY_STATS=1` prints per-stage write timings,
  `LINEARIZABLE=0` turns off the per-query majority check, and `BATCH_IN_FLIGHT` /
  `BATCH_LINGER_US` tune group commit.
- **Load generator options:** add `timeline=1` for a per-second throughput timeline.

## Tests

There are 68 automated tests, all passing:

| Suite | Count | Covers |
|---|---|---|
| unit (`src/`) | 25 | WAL format: every single-bit flip is detected, torn frames, forward-compatible headers; memtable versioning and dedup; DDL parsing; Doris type mapping; per-writer rejection inside one batched Raft entry |
| `tests/node.rs` | 18 | restart recovery from the WAL, snapshots trimming the WAL, flush protocol (label, intent, poll, grace, drop), memory backpressure, stuck tables rejecting writes, deletes, consistent snapshots |
| `tests/cluster.rs` | 9 | leader failover with follower write forwarding, lagging-follower snapshot catch-up, multi-megabyte writes replicating, fast follower read barrier, all over real HTTP |
| `tests/flight.rs` | 8 | auth, ingest, SQL `INSERT` and `DELETE` by primary key, error mapping, metadata, prepared statements vs the primary-key fast path (same answers), DDL refused, multi-table scans |
| `tests/loaders.rs` | 5 | Stream Load and Broker Load against a mock Doris FE/BE that tracks labels: redirects, unknown-label recovery, failures |
| `tests/storage_suite.rs` | 1 | openraft's full log storage and state machine conformance suite |
| `tests/velodb.rs` | 2 | real VeloDB (managed Doris): HTTPS Stream Load with MERGE/delete, label states, sequence floor with tombstones |

Manual end-to-end checks:
- Python ADBC → doris-ingress → VeloDB via HTTPS Stream Load, rows confirmed in Doris.
- JDBC queries through the Arrow Flight SQL JDBC driver.
- 3-node cluster: `kill -9` of the leader under load, with the old leader rejoining and all nodes
  ending with identical data.
- WAL recovery after killing a node.

## Benchmarks

Hardware: one 16-core Linux host with a btrfs NVMe drive. All 3 nodes ran on it on different
ports, connected over a bridge IP rather than loopback, with the load generator on the same host.
Every write was replicated and fsynced before it was acknowledged. Because the three nodes and the
client share one disk and one CPU, these numbers are **pessimistic** for a real deployment.

### Writes

| Workload (3 nodes) | Rows/s | Latency |
|---|---|---|
| 10,000 rows per call | **2.1–2.3 M** | — |
| 1,000 rows per call | 1.2 M | p50 13 ms |
| 1 row per call, 1 writer | 613 | p50 1.6 ms |
| 1 row per call, 64 writers | 16.7 k | p99 6.2 ms |
| 1 row per call, 256 writers | 35.8 k | p99 14.9 ms |
| 1 row per call, 1,024 writers | 56.4 k | p50 17 ms, p99 31 ms |

The 1,000-rows-per-call figure was measured before the write-path tuning; the other rows were
measured after it.

In-process, without network or replication: ~4.3 M rows/s, and ~3.1 M rows/s with fsync.

Batching on the client side is what matters: 1,000–10,000 rows per call gives more than 1M rows/s.
Row-by-row writers are limited by commit latency per entry (replication plus fsync). Group commit
lets them scale with concurrency until the cluster hits its throughput ceiling; past that, extra
writers only add queueing delay (Little's law).

### Queries: primary-key point lookups, 10M rows, linearizable reads on

| Setup | QPS | p50 / p99 |
|---|---|---|
| 1 node, 64 clients, prepared statement + fast path | **~28 k** | 0.75 ms p50 |
| 3 nodes: 32 clients on the leader | 20.7 k | 1.5 / 2.5 ms |
| 3 nodes: 32 clients on a follower | 18.8 k | 1.7 / 2.6 ms |
| 3 nodes: 96 clients over all nodes | **26.6 k** | 3.5 / 6.7 ms |

Failover under read load: `kill -9` of the leader caused a ~2 s dip, with errors only on clients
connected to the killed node.

Each lookup costs about 0.3 ms of server CPU, almost all of it gRPC and Flight SQL's two round
trips per query. The lookup itself takes microseconds. Scans and analytical queries have not been
benchmarked.

## Verdict

**As a proof of concept, it serves its purpose.**
- **Frequent writes:** it absorbs them at over 1M rows/s when clients batch, and at ~56k rows/s
  for 1,024 concurrent row-by-row writers. That is far beyond what Doris handles as individual
  loads.
- **Fast visibility:** data can be queried by SQL as soon as the write is acknowledged (~1.6 ms
  for a single writer). It reaches Doris in large, idempotent batches.
- **Read after write:** works on any node through linearizable reads. Durability comes from a
  majority fsync to a checksummed WAL.
- **Clients:** standard ADBC and JDBC drivers work unmodified.

**Not yet proven:** the merged Doris-side view, which is waiting on the Doris ADBC catalog, and
behaviour under adversarial failures.

## Known gaps

- **Testing:**
  - No crash or fault injection at each step of the flush protocol.
  - No automated linearizability checker (Jepsen-style).
  - No power-loss simulation of the WAL.
  - Benchmarks ran on one host only.
- **Not built yet:**
  - TLS on the Flight SQL endpoint.
  - A Stream Load–compatible HTTP ingest endpoint for Flink.
  - Two-phase commit.
  - Metrics.
  - Schema change detection.
- **Not tested against real Doris yet:** Broker Load (mock only), and the merged view through the
  Doris ADBC catalog.
- **Rough edges:**
  - Followers report the leader's Raft address, not its Flight address.
  - A scan briefly holds the table lock while selecting keys.
- **Losing data:** losing a majority of nodes before a flush loses unflushed data, as in any Raft
  system.
- **Write path:** every write to a buffered table must go through doris-ingress. A direct write
  to Doris can break the sequence ordering.

## License

Not yet licensed. All rights reserved until a license is added.
