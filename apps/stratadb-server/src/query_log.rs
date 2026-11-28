//! `system.query_log` (spec §17; STRATA-SYS-01-007, -11-006, -13-009).
//!
//! Every query a tenant runs becomes one event in a separate storage engine
//! under `<data_dir>/system/query_log`, stored under that tenant. The table
//! has the event schema, so the normal executor, pruning, retention and
//! tenant scoping apply: `SELECT ... FROM system.query_log` reads only the
//! caller's own queries.
//!
//! Recording never slows or fails a query:
//! - entries go into a bounded queue; when it is full, the entry is dropped
//!   and counted;
//! - one writer thread (`sx-maint-02`) writes batches every `flush_interval`
//!   (spec `obs.query_log_flush_ms`, default 7.5 s), so buffered entries can
//!   be lost in a crash, never acknowledged data;
//! - write errors (for example a full disk) are counted and logged.
//!
//! The SQL text is stored normalized (literals replaced, see
//! `stratadb_query::fingerprint`), so values a query names are not copied
//! into the log.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use stratadb_common::threads::Role;
use stratadb_common::{Error, IoResultExt, Result, TenantId};
use stratadb_query::{Fingerprint, QueryStats};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

/// Entries written per engine batch at most.
const MAX_BATCH: usize = 4096;

/// `[observability]` query log settings.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryLogConfig {
    pub enabled: bool,
    /// How long entries are buffered before they are written.
    pub flush_interval: Duration,
    /// Entries older than this are deleted by retention.
    pub retention: Duration,
    /// Entries buffered at most; more are dropped (and counted).
    pub queue: usize,
}

impl Default for QueryLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            flush_interval: Duration::from_millis(7500),
            retention: Duration::from_secs(7 * 86_400),
            queue: 10_000,
        }
    }
}

/// Counters for `/metrics`.
#[derive(Debug, Default)]
pub struct QueryLogCounters {
    pub recorded: AtomicU64,
    pub written: AtomicU64,
    pub dropped: AtomicU64,
    pub write_errors: AtomicU64,
}

/// One finished (or rejected) query.
#[derive(Debug, Clone)]
pub struct Entry<'a> {
    pub tenant: &'a TenantId,
    /// Unix nanoseconds at which the request arrived.
    pub started_unix_ns: i64,
    pub query_id: &'a str,
    pub fingerprint: &'a Fingerprint,
    /// `"ok"` or the error kind (`invalid_input`, `timeout`, ...).
    pub status: &'a str,
    /// Wait for a blocking worker (STRATA-SYS-11-006).
    pub queue: Duration,
    /// End-to-end time in the server.
    pub elapsed: Duration,
    /// Response serialization time.
    pub serialize: Duration,
    /// Statistics of an executed query (`None` when it failed before or
    /// during execution).
    pub stats: Option<&'a QueryStats>,
    pub result_rows: u64,
    /// Plan-time estimate: (rows to scan, single-core CPU ms), when made.
    pub estimate: Option<(u64, f64)>,
}

fn us(d: Duration) -> Value {
    Value::Int(i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

fn int(v: u64) -> Value {
    Value::Int(i64::try_from(v).unwrap_or(i64::MAX))
}

impl Entry<'_> {
    /// The stored event: `service = "strata"`, `severity` INFO (ok) or ERROR,
    /// `body` = normalized text, and the measurements as attributes.
    pub fn to_event(&self) -> Event {
        let now = stratadb_common::time::now_unix_nanos();
        let mut e = Event::new(self.tenant.clone(), self.started_unix_ns, now);
        e.service = Some("strata".into());
        e.severity = if self.status == "ok" {
            Severity::Info
        } else {
            Severity::Error
        };
        e.event_id = Some(self.query_id.to_owned());
        e.body = Some(Value::String(self.fingerprint.normalized.clone()));
        let mut a = BTreeMap::new();
        a.insert("query_id".into(), Value::String(self.query_id.to_owned()));
        a.insert("fingerprint".into(), Value::String(self.fingerprint.hex()));
        a.insert("status".into(), Value::String(self.status.to_owned()));
        a.insert("queue_us".into(), us(self.queue));
        a.insert("elapsed_us".into(), us(self.elapsed));
        a.insert("serialize_us".into(), us(self.serialize));
        a.insert("result_rows".into(), int(self.result_rows));
        if let Some(s) = self.stats {
            a.insert("table".into(), Value::String(s.table.to_owned()));
            a.insert("parse_us".into(), us(s.parse));
            a.insert("plan_us".into(), us(s.plan));
            a.insert("exec_us".into(), us(s.elapsed));
            a.insert("cpu_us".into(), us(s.cpu));
            a.insert("rows_scanned".into(), int(s.rows_scanned));
            a.insert("rows_matched".into(), int(s.rows_matched));
            a.insert("bytes_read".into(), int(s.bytes_read));
            a.insert("segments_total".into(), int(s.segments_total as u64));
            a.insert("segments_scanned".into(), int(s.segments_scanned as u64));
            a.insert("memory_rows_scanned".into(), int(s.memory_rows_scanned));
            a.insert("memory_peak".into(), int(s.peak_memory_bytes as u64));
            a.insert("memory_reserved_peak".into(), int(s.memory_reserved_peak));
            a.insert("scan_workers".into(), int(s.scan_workers as u64));
            if let Some((rows, _)) = self.estimate {
                // Estimated vs actual rows (STRATA-SYS-12-004).
                a.insert(
                    "q_error".into(),
                    Value::Float(crate::telemetry::q_error(rows, s.rows_scanned)),
                );
            }
        }
        if let Some((rows, ms)) = self.estimate {
            a.insert("est_rows".into(), int(rows));
            a.insert("est_cpu_ms".into(), Value::Float(ms));
        }
        e.attributes = a;
        e
    }
}

/// The query log: a bounded queue, a writer thread and its engine.
pub struct QueryLog {
    engine: Arc<Engine>,
    tx: Mutex<Option<SyncSender<Event>>>,
    writer: Mutex<Option<JoinHandle<()>>>,
    counters: Arc<QueryLogCounters>,
    queue: usize,
}

impl std::fmt::Debug for QueryLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryLog")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

/// Directory of the query log engine under a data directory.
pub fn dir(data_dir: &Path) -> PathBuf {
    data_dir.join("system").join("query_log")
}

impl QueryLog {
    /// Open the query log engine next to the main one. It shares the main
    /// engine's I/O engine, encryption key and disk reserve.
    pub fn open(data_dir: &Path, main: &EngineOptions, cfg: &QueryLogConfig) -> Result<Self> {
        let d = dir(data_dir);
        std::fs::create_dir_all(&d).ctx(|| format!("creating {}", d.display()))?;
        let mut o = EngineOptions::new(&d);
        o.io = main.io.clone();
        o.encryption = main.encryption.clone();
        o.disk_reserve_bytes = main.disk_reserve_bytes;
        o.disk_reserve_ratio = main.disk_reserve_ratio;
        // A batch is written every flush interval; one fsync per batch.
        o.wal.durability = Durability::Sync;
        o.memtable_max_bytes = 16 * 1024 * 1024;
        o.default_retention = Some(cfg.retention);
        o.bloom_attribute_keys = vec!["query_id".into(), "fingerprint".into()];
        o.maint_thread_index = 1;
        // Accounted in the same regions as the main engine.
        o.memory = main.memory.as_ref().map(|t| t.child("query_log", 0));
        o.cache_memory = main.cache_memory.as_ref().map(|t| t.child("query_log", 0));
        o.background_memory = main
            .background_memory
            .as_ref()
            .map(|t| t.child("query_log", 0));
        let engine = Arc::new(Engine::open(o)?);
        let (tx, rx) = std::sync::mpsc::sync_channel(cfg.queue);
        let counters = Arc::new(QueryLogCounters::default());
        let writer = {
            let (engine, counters, every) = (
                Arc::clone(&engine),
                Arc::clone(&counters),
                cfg.flush_interval,
            );
            // Memory held is at most the queue plus one batch of the same size.
            let batch = cfg.queue.clamp(1, MAX_BATCH);
            std::thread::Builder::new()
                .name(Role::Maint.thread_name(2))
                .spawn(move || writer_loop(&engine, &rx, every, batch, &counters))
                .ctx(|| "spawning the query log writer".to_owned())?
        };
        Ok(Self {
            engine,
            tx: Mutex::new(Some(tx)),
            writer: Mutex::new(Some(writer)),
            counters,
            queue: cfg.queue,
        })
    }

    /// The engine holding the log (queried as `system.query_log`).
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn counters(&self) -> &QueryLogCounters {
        &self.counters
    }

    /// Queue capacity.
    pub fn capacity(&self) -> usize {
        self.queue
    }

    /// Queue an entry; never blocks. Returns false if it was dropped.
    pub fn record(&self, entry: &Entry<'_>) -> bool {
        self.counters.recorded.fetch_add(1, Ordering::Relaxed);
        let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner());
        let sent = match tx.as_ref() {
            Some(tx) => match tx.try_send(entry.to_event()) {
                Ok(()) => true,
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => false,
            },
            None => false,
        };
        if !sent {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
        sent
    }

    /// Write every queued entry and stop the writer. Later entries are
    /// dropped. Idempotent.
    pub fn shutdown(&self) {
        drop(self.tx.lock().unwrap_or_else(|e| e.into_inner()).take());
        if let Some(h) = self.writer.lock().unwrap_or_else(|e| e.into_inner()).take() {
            if h.join().is_err() {
                tracing::error!(component = "query_log", "query log writer panicked");
            }
        }
    }

    /// Shut down and close the engine (flushes its memtable).
    pub fn close(self) -> Result<()> {
        self.shutdown();
        // The writer held the only other reference and has exited.
        let engine = Arc::clone(&self.engine);
        drop(self);
        match Arc::try_unwrap(engine) {
            Ok(e) => e.close(),
            Err(_) => Err(Error::internal("query log engine still in use at close")),
        }
    }
}

impl Drop for QueryLog {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn writer_loop(
    engine: &Engine,
    rx: &Receiver<Event>,
    every: Duration,
    max_batch: usize,
    counters: &QueryLogCounters,
) {
    let me = stratadb_common::threads::register(Role::Maint);
    loop {
        let deadline = Instant::now() + every;
        let mut batch = Vec::new();
        let mut closed = false;
        while batch.len() < max_batch {
            match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(e) => batch.push(e),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }
        write(engine, batch, counters);
        me.update_cpu();
        if closed {
            return;
        }
    }
}

fn write(engine: &Engine, batch: Vec<Event>, counters: &QueryLogCounters) {
    let mut by_tenant: BTreeMap<TenantId, Vec<Event>> = BTreeMap::new();
    for e in batch {
        by_tenant.entry(e.tenant_id.clone()).or_default().push(e);
    }
    for (tenant, events) in by_tenant {
        let n = events.len() as u64;
        match engine.write_batch(&tenant, events) {
            Ok(_) => {
                counters.written.fetch_add(n, Ordering::Relaxed);
            }
            Err(e) => {
                counters.write_errors.fetch_add(n, Ordering::Relaxed);
                tracing::warn!(component = "query_log", tenant = %tenant, entries = n, error = %e,
                    "query log entries could not be written");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn entry<'a>(t: &'a TenantId, fp: &'a Fingerprint, id: &'a str) -> Entry<'a> {
        Entry {
            tenant: t,
            started_unix_ns: 1_795_170_600_000_000_000,
            query_id: id,
            fingerprint: fp,
            status: "ok",
            queue: Duration::from_micros(3),
            elapsed: Duration::from_micros(900),
            serialize: Duration::from_micros(5),
            stats: None,
            result_rows: 2,
            estimate: Some((10, 0.5)),
        }
    }

    #[test]
    fn entries_are_written_per_tenant_and_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = QueryLogConfig {
            flush_interval: Duration::from_millis(20),
            ..QueryLogConfig::default()
        };
        let main = EngineOptions::new(dir.path());
        let (a, b) = (TenantId::new("a").unwrap(), TenantId::new("b").unwrap());
        let fp = stratadb_query::fingerprint("SELECT 1 FROM logs");
        let log = QueryLog::open(dir.path(), &main, &cfg).unwrap();
        assert!(log.record(&entry(&a, &fp, "q1")));
        assert!(log.record(&entry(&a, &fp, "q2")));
        assert!(log.record(&entry(&b, &fp, "q3")));
        log.close().unwrap();

        let log = QueryLog::open(dir.path(), &main, &cfg).unwrap();
        let rows = |t: &TenantId| log.engine().snapshot(t).scan_all().unwrap();
        let ra = rows(&a);
        assert_eq!(ra.len(), 2);
        assert_eq!(rows(&b).len(), 1);
        let ev = &ra[0].2;
        assert_eq!(ev.attributes["fingerprint"], Value::String(fp.hex()));
        assert_eq!(ev.attributes["queue_us"], Value::Int(3));
        assert_eq!(ev.body, Some(Value::String("select ? from logs".into())));
        log.close().unwrap();
    }

    #[test]
    fn a_full_queue_drops_and_counts_instead_of_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = QueryLogConfig {
            flush_interval: Duration::from_secs(3600),
            queue: 4,
            ..QueryLogConfig::default()
        };
        let log = QueryLog::open(dir.path(), &EngineOptions::new(dir.path()), &cfg).unwrap();
        let t = TenantId::new("a").unwrap();
        let fp = stratadb_query::fingerprint("SELECT 1 FROM logs");
        let started = Instant::now();
        let sent = (0..100)
            .filter(|_| log.record(&entry(&t, &fp, "q")))
            .count();
        assert!(started.elapsed() < Duration::from_secs(1));
        // The writer holds at most one batch of `queue` entries besides the
        // queue itself, and each write waits for an fsync, so most entries of
        // this burst are dropped.
        assert!(sent < 100, "{sent}");
        let c = log.counters();
        assert_eq!(c.recorded.load(Ordering::Relaxed), 100);
        assert_eq!(c.dropped.load(Ordering::Relaxed), 100 - sent as u64);
        // Shutdown writes what was accepted.
        log.shutdown();
        assert_eq!(c.written.load(Ordering::Relaxed), sent as u64);
        assert!(!log.record(&entry(&t, &fp, "late")));
        log.close().unwrap();
    }
}

#[cfg(test)]
mod cost {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// Per-query cost of recording (run: `cargo test --release -p
    /// stratadb-server --lib query_log::cost -- --ignored --nocapture`).
    #[test]
    #[ignore = "measurement, not a check"]
    fn per_query_recording_cost() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = QueryLogConfig {
            queue: 1_000_000,
            flush_interval: Duration::from_secs(3600),
            ..QueryLogConfig::default()
        };
        let log = QueryLog::open(dir.path(), &EngineOptions::new(dir.path()), &cfg).unwrap();
        let t = TenantId::new("a").unwrap();
        let sql =
            "SELECT service, count(*) AS n FROM logs WHERE event_time >= 1795170600000000000 \
                   AND severity IN ('ERROR', 'FATAL') AND attributes['region'] = 'eu-west-1' \
                   GROUP BY service ORDER BY n DESC LIMIT 10";
        let stats = QueryStats::default();
        let n = 50_000u32;
        let t0 = Instant::now();
        for _ in 0..n {
            std::hint::black_box(stratadb_query::fingerprint(std::hint::black_box(sql)));
        }
        let fp_ns = t0.elapsed().as_nanos() / u128::from(n);
        let fp = stratadb_query::fingerprint(sql);
        let t0 = Instant::now();
        for i in 0..n {
            let id = format!("q{i}");
            log.record(&Entry {
                tenant: &t,
                started_unix_ns: 1,
                query_id: &id,
                fingerprint: &fp,
                status: "ok",
                queue: Duration::from_micros(1),
                elapsed: Duration::from_micros(100),
                serialize: Duration::ZERO,
                stats: Some(&stats),
                result_rows: 1,
                estimate: None,
            });
        }
        let rec_ns = t0.elapsed().as_nanos() / u128::from(n);
        let t0 = Instant::now();
        for _ in 0..n {
            std::hint::black_box(stratadb_common::cpu::thread_cpu_time());
        }
        let clk_ns = t0.elapsed().as_nanos() / u128::from(n);
        println!("fingerprint {fp_ns} ns, record (entry + queue) {rec_ns} ns, thread_cpu_time {clk_ns} ns");
        log.close().unwrap();
    }
}
