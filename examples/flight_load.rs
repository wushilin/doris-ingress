//! Flight SQL load generator with client-side failover.
//!
//! `flight_load urls=http://A,http://B,http://C mode=read|write|load conc=64 secs=10 rows=N batch=B`
//! * `load`:  insert ids 0..rows once (sequential, `batch` rows per ingest call), then exit.
//! * `write`: ingest batches of `batch` random ids for `secs` seconds.
//! * `read`:  prepared point lookups `WHERE id = $1` on random ids for `secs` seconds.
//!
//! Clients are spread round-robin over the URLs. On any error a client reconnects to the next URL
//! and retries; errors and a per-second throughput timeline are reported (`timeline=1`).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_flight::sql::CommandStatementIngest;
use arrow_flight::sql::client::{FlightSqlServiceClient, PreparedStatement};
use futures_util::TryStreamExt;
use parking_lot::Mutex;
use tonic::transport::Channel;

type Client = FlightSqlServiceClient<Channel>;

async fn connect(url: &str) -> Result<Client, String> {
    let ch = Channel::from_shared(url.to_string())
        .map_err(|e| e.to_string())?
        .tcp_nodelay(true)
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(10))
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    let mut c = FlightSqlServiceClient::new(ch);
    c.handshake("admin", "bench").await.map_err(|e| e.to_string())?;
    Ok(c)
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("note", DataType::Utf8, true),
    ]))
}

fn rows(ids: &[i64]) -> RecordBatch {
    const R: [&str; 4] = ["sg", "us", "eu", "jp"];
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from_iter_values(ids.iter().map(|i| R[(*i % 4) as usize]))),
            Arc::new(Float64Array::from_iter_values(ids.iter().map(|i| (*i % 10000) as f64 / 100.0))),
            Arc::new(TimestampMicrosecondArray::from_iter_values(ids.iter().map(|i| 1_790_000_000_000_000 + i))),
            Arc::new(StringArray::from_iter_values(ids.iter().map(|i| format!("note-{i}")))),
        ],
    )
    .unwrap()
}

struct Rng(u64);
impl Rng {
    fn next(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

struct Worker {
    urls: Vec<String>,
    at: usize,
    client: Option<Client>,
    prepared: Option<PreparedStatement<Channel>>,
}

impl Worker {
    /// A connected client, reconnecting round-robin across URLs until one answers.
    async fn client(&mut self, stop: &AtomicBool) -> Option<&mut Client> {
        while self.client.is_none() {
            if stop.load(Ordering::Relaxed) {
                return None;
            }
            match connect(&self.urls[self.at % self.urls.len()]).await {
                Ok(c) => self.client = Some(c),
                Err(_) => {
                    self.at += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        self.client.as_mut()
    }

    fn fail(&mut self) {
        self.client = None;
        self.prepared = None;
        self.at += 1;
    }
}

#[tokio::main]
async fn main() {
    let kv: HashMap<String, String> =
        std::env::args().skip(1).filter_map(|a| a.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))).collect();
    let get = |k: &str, d: &str| kv.get(k).cloned().unwrap_or_else(|| d.to_string());
    let urls: Vec<String> = get("urls", "http://127.0.0.1:18816").split(',').map(String::from).collect();
    let mode = get("mode", "read");
    let conc: usize = get("conc", "64").parse().unwrap();
    let secs: u64 = get("secs", "10").parse().unwrap();
    let keyspace: u64 = get("rows", "1000000").parse().unwrap();
    let batch: usize = get("batch", "1000").parse().unwrap();
    let timeline = get("timeline", "0") == "1";

    let ops = Arc::new(AtomicU64::new(0)); // queries or ingest calls
    let rows_done = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let lat = Arc::new(Mutex::new(Vec::<u32>::new()));
    let next_load = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut tasks = Vec::new();
    for w in 0..conc {
        let (urls, ops, rows_done, errors, stop, lat, mode, next_load) =
            (urls.clone(), ops.clone(), rows_done.clone(), errors.clone(), stop.clone(), lat.clone(), mode.clone(), next_load.clone());
        tasks.push(tokio::spawn(async move {
            let mut wk = Worker { at: w, urls, client: None, prepared: None };
            let mut rng = Rng(0x9E3779B97F4A7C15 ^ (w as u64 + 1).wrapping_mul(0xBF58476D1CE4E5B9));
            let param_schema = Arc::new(Schema::new(vec![Field::new("$1", DataType::Int64, false)]));
            let mut local = Vec::new();
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let ids: Vec<i64> = match mode.as_str() {
                    "load" => {
                        let from = next_load.fetch_add(batch as u64, Ordering::Relaxed);
                        if from >= keyspace {
                            break;
                        }
                        (from as i64..(from + batch as u64).min(keyspace) as i64).collect()
                    }
                    "write" => (0..batch).map(|_| rng.next(keyspace) as i64).collect(),
                    _ => vec![rng.next(keyspace) as i64],
                };
                let t = Instant::now();
                let result: Result<usize, String> = async {
                    if wk.client(&stop).await.is_none() {
                        return Err("stopped".to_string());
                    }
                    if mode == "read" {
                        if wk.prepared.is_none() {
                            let c = wk.client.as_mut().unwrap();
                            wk.prepared = Some(c.prepare("SELECT * FROM bench.t WHERE id = $1".into(), None).await.map_err(|e| e.to_string())?);
                        }
                        let p = wk.prepared.as_mut().unwrap();
                        p.set_parameters(RecordBatch::try_new(param_schema.clone(), vec![Arc::new(Int64Array::from(ids.clone()))]).unwrap())
                            .map_err(|e| e.to_string())?;
                        let info = p.execute().await.map_err(|e| e.to_string())?;
                        let c = wk.client.as_mut().unwrap();
                        let mut n = 0;
                        for ep in info.endpoint {
                            let b: Vec<RecordBatch> = c.do_get(ep.ticket.unwrap()).await.map_err(|e| e.to_string())?.try_collect().await.map_err(|e| e.to_string())?;
                            n += b.iter().map(|b| b.num_rows()).sum::<usize>();
                        }
                        if n != 1 {
                            return Err(format!("id {} returned {n} rows", ids[0]));
                        }
                        Ok(1)
                    } else {
                        let cmd = CommandStatementIngest { table: "t".into(), schema: Some("bench".into()), ..Default::default() };
                        let stream = futures_util::stream::iter(vec![Ok(rows(&ids))]);
                        let c = wk.client.as_mut().unwrap();
                        let n = c.execute_ingest(cmd, stream).await.map_err(|e| e.to_string())?;
                        Ok(n as usize)
                    }
                }
                .await;
                match result {
                    Ok(n) => {
                        local.push(t.elapsed().as_micros() as u32);
                        ops.fetch_add(1, Ordering::Relaxed);
                        rows_done.fetch_add(n as u64, Ordering::Relaxed);
                    }
                    Err(e) if e == "stopped" => break,
                    Err(e) => {
                        if errors.fetch_add(1, Ordering::Relaxed) < 5 {
                            eprintln!("[{:>5.1}s] client {w}: {e} -> reconnecting", start.elapsed().as_secs_f64());
                        }
                        wk.fail();
                    }
                }
            }
            lat.lock().extend(local);
        }));
    }

    if mode == "load" {
        for t in tasks {
            let _ = t.await;
        }
        let el = start.elapsed().as_secs_f64();
        let r = rows_done.load(Ordering::Relaxed);
        println!("load: {r} rows in {el:.2}s = {:.2} M rows/s, errors {}", r as f64 / el / 1e6, errors.load(Ordering::Relaxed));
        return;
    }

    tokio::time::sleep(Duration::from_secs(1)).await; // warm-up
    let (o0, r0, e0) = (ops.load(Ordering::Relaxed), rows_done.load(Ordering::Relaxed), errors.load(Ordering::Relaxed));
    let t0 = Instant::now();
    let mut last = o0;
    for s in 1..=secs {
        tokio::time::sleep_until((t0 + Duration::from_secs(s)).into()).await;
        let now = ops.load(Ordering::Relaxed);
        if timeline {
            println!("  t={s:>3}s  {:>8} ops/s  errors so far {}", now - last, errors.load(Ordering::Relaxed) - e0);
        }
        last = now;
    }
    let el = t0.elapsed().as_secs_f64();
    let (o, r, e) = (ops.load(Ordering::Relaxed) - o0, rows_done.load(Ordering::Relaxed) - r0, errors.load(Ordering::Relaxed) - e0);
    stop.store(true, Ordering::Relaxed);
    for t in tasks {
        let _ = t.await;
    }
    let mut l = lat.lock().clone();
    l.sort_unstable();
    let p = |q: f64| l.get(((l.len() as f64 * q) as usize).min(l.len().saturating_sub(1))).copied().unwrap_or(0) as f64 / 1000.0;
    if mode == "read" {
        println!("read: {:.0} queries/s  p50={:.2}ms p99={:.2}ms  errors={e}  (conc={conc}, nodes={})", o as f64 / el, p(0.5), p(0.99), urls.len());
    } else {
        println!(
            "write: {:.0} rows/s ({:.0} ingest calls/s of {batch} rows)  p50={:.2}ms p99={:.2}ms  errors={e}  (conc={conc}, nodes={})",
            r as f64 / el,
            o as f64 / el,
            p(0.5),
            p(0.99),
            urls.len()
        );
    }
}
