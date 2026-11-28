//! TOML benchmark scenarios (spec VIII.19, STRATA-SYS-19-001; VIII.25).
//!
//! A scenario names a deterministic dataset, the engine settings (durability
//! is mandatory: Part V.8), warmup and repetition counts, catalogued queries
//! with plan-property assertions (STRATA-SYS-25-002), and load blocks (open-
//! or closed-loop). Running one writes `manifest.json` and `results.json`
//! (raw samples plus summaries) into the output directory. The format is
//! documented in docs/benchmarks/harness.md.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as J};
use stratadb_common::cpu::CpuTimer;
use stratadb_common::time::format_rfc3339;
use stratadb_common::{Error, Result, TenantId};
use stratadb_query::{execute, QueryOptions, QueryStats};
use stratadb_schema::Event;
use stratadb_storage::{Durability, Engine, EngineOptions};
use stratadb_testkit::{Generator, Profile};

use crate::openloop::{run_closed, run_open, LoadReport};
use crate::stats::{summarize, Summary};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub dataset: Dataset,
    pub engine: EngineCfg,
    #[serde(default)]
    pub query_options: QueryCfg,
    pub run: RunCfg,
    #[serde(default)]
    pub query: Vec<QuerySpec>,
    #[serde(default)]
    pub load: Vec<LoadSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    /// `stratadb-testkit` profile (spec §27.3).
    pub profile: String,
    pub events: usize,
    pub tenants: usize,
    pub seed: u64,
    pub batch_size: usize,
    /// Deterministic segment layout: disable background flushes and flush
    /// after every `flush_every` events (plan assertions that depend on the
    /// number of segments are then reproducible). Default: background
    /// flushing as in production.
    #[serde(default)]
    pub flush_every: Option<usize>,
    /// Flush after loading (default true). `false` leaves the last
    /// `flush_every` remainder (or, without `flush_every`, everything) in
    /// the memtable, to measure queries over unflushed data.
    #[serde(default = "yes")]
    pub final_flush: bool,
    /// Stop the explicit `flush_every` flushes after this many (default:
    /// no limit); later rows stay in the memtable.
    #[serde(default)]
    pub max_flushes: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EngineCfg {
    /// `async` | `sync` | `group_commit` — required: results are never
    /// reported without their durability mode (Part V.8).
    pub durability: String,
    #[serde(default = "default_delay")]
    pub group_commit_delay_ms: u64,
    #[serde(default = "yes")]
    pub bloom: bool,
    #[serde(default = "yes")]
    pub ngram: bool,
    #[serde(default = "yes")]
    pub compression: bool,
    #[serde(default = "yes")]
    pub shredding: bool,
    /// Memtable flush threshold (bytes); default: the engine's.
    #[serde(default)]
    pub memtable_max_bytes: Option<usize>,
    /// Memtable flush thresholds by rows and age (s); default: the engine's.
    #[serde(default)]
    pub memtable_max_rows: Option<usize>,
    #[serde(default)]
    pub memtable_max_age_s: Option<u64>,
    /// Adaptive group-commit delay (default true; false = fixed delay).
    #[serde(default = "yes")]
    pub adaptive_group_delay: bool,
    /// Account memory in a server-like tracker tree (ingest, cache,
    /// background regions and a query pool, all unlimited) — the
    /// production code path. `false`: no regions; each query is accounted
    /// on its own tracker only.
    #[serde(default = "yes")]
    pub memory_accounting: bool,
    /// WAL format written: 3 (columnar payloads, default) or 2 (row
    /// payloads; for A/B).
    #[serde(default = "wal_v3")]
    pub wal_format_version: u16,
    /// Decoded column cache bytes (0 = off, the engine default).
    #[serde(default)]
    pub column_cache_bytes: usize,
    /// Per-segment partial aggregate cache bytes (0 = off; ADR-0045).
    #[serde(default)]
    pub partial_cache_bytes: usize,
}

fn wal_v3() -> u16 {
    stratadb_storage::WAL_FORMAT_VERSION
}

fn default_delay() -> u64 {
    2
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryCfg {
    /// Scan threads (0 or absent: engine default).
    #[serde(default)]
    pub parallelism: usize,
    #[serde(default = "yes")]
    pub vectorized: bool,
    #[serde(default = "yes")]
    pub map_projection: bool,
    /// CPU ms of estimated scan work per worker (0 = always `parallelism`
    /// workers); absent = engine default.
    #[serde(default)]
    pub target_ms_per_worker: Option<f64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunCfg {
    pub warmup: usize,
    /// ≥ 5 (end-to-end minimum of STRATA-SYS-19-001).
    pub repetitions: usize,
    /// Only `warm` is supported: a valid cold run needs a dropped page cache
    /// and an idle device (STRATA-SYS-01-009), which the harness cannot
    /// arrange portably.
    #[serde(default = "warm")]
    pub cache: String,
}

fn warm() -> String {
    "warm".to_owned()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QuerySpec {
    pub id: String,
    /// StrataQL; placeholders `{trace_id}`, `{request_id}`, `{t_start}`,
    /// `{t_end}`, `{t_recent}`, `{t_mid_lo}`, `{t_mid_hi}` are filled from
    /// the dataset.
    pub sql: String,
    #[serde(default = "tenant0")]
    pub tenant: String,
    /// Plan-property assertions, e.g. `segments_pruned_bloom = ">= 1"`.
    #[serde(default)]
    pub expect: BTreeMap<String, String>,
}

fn tenant0() -> String {
    "tenant-0".to_owned()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoadSpec {
    pub id: String,
    /// `ingest` | `query`
    pub kind: String,
    /// `open` | `closed`
    pub mode: String,
    /// Offered rate (open loop).
    pub rate_per_s: Option<f64>,
    pub duration_s: f64,
    pub workers: usize,
    /// Events per ingest request.
    pub batch_size: Option<usize>,
    /// `id` of a `[[query]]` (kind = query).
    pub query: Option<String>,
    /// Work class of the load's queries (`interactive`, `normal`, `batch`;
    /// default `normal`), as the scan pool schedules them (spec VIII.11).
    pub class: Option<String>,
    /// A closed-loop query load run at the same time as interference (it
    /// starts 300 ms earlier and ends with this load).
    pub alongside: Option<AlongsideSpec>,
}

/// Interference for a load (see [`LoadSpec::alongside`]).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlongsideSpec {
    /// `id` of a `[[query]]`.
    pub query: String,
    pub workers: usize,
    pub class: Option<String>,
}

impl Scenario {
    pub fn parse(text: &str) -> Result<Self> {
        let s: Scenario =
            toml::from_str(text).map_err(|e| Error::invalid_input(format!("scenario: {e}")))?;
        s.validate()?;
        Ok(s)
    }

    fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(Error::invalid_input(format!("scenario {}: {m}", self.name)));
        if Profile::parse(&self.dataset.profile).is_none() {
            return bad(format!(
                "unknown dataset profile {:?}",
                self.dataset.profile
            ));
        }
        if self.dataset.flush_every == Some(0) {
            return bad("dataset flush_every must be > 0".into());
        }
        if self.dataset.events == 0 || self.dataset.batch_size == 0 || self.dataset.tenants == 0 {
            return bad("dataset events, batch_size and tenants must be > 0".into());
        }
        if !["async", "sync", "group_commit"].contains(&self.engine.durability.as_str()) {
            return bad(format!(
                "durability {:?} (async | sync | group_commit)",
                self.engine.durability
            ));
        }
        if self.run.repetitions < 5 {
            return bad("repetitions must be >= 5 (STRATA-SYS-19-001)".into());
        }
        if self.run.cache != "warm" {
            return bad("only cache = \"warm\" is supported (cold runs need a dropped page cache: STRATA-SYS-01-009)".into());
        }
        let mut ids = std::collections::BTreeSet::new();
        for q in &self.query {
            if !ids.insert(q.id.as_str()) {
                return bad(format!("duplicate query id {}", q.id));
            }
            for (k, v) in &q.expect {
                parse_expect(k, v)
                    .map_err(|e| Error::invalid_input(format!("query {}: {e}", q.id)))?;
            }
        }
        for l in &self.load {
            if !ids.insert(l.id.as_str()) {
                return bad(format!("duplicate id {}", l.id));
            }
            match (l.kind.as_str(), l.mode.as_str()) {
                ("ingest" | "query", "open" | "closed") => {}
                _ => {
                    return bad(format!(
                        "load {}: kind ingest|query, mode open|closed",
                        l.id
                    ))
                }
            }
            if l.mode == "open" && l.rate_per_s.is_none_or(|r| r <= 0.0) {
                return bad(format!("load {}: open loop needs rate_per_s > 0", l.id));
            }
            if !(l.duration_s > 0.0 && l.duration_s <= 3600.0) || l.workers == 0 || l.workers > 256
            {
                return bad(format!(
                    "load {}: duration_s in (0, 3600], workers in 1..=256",
                    l.id
                ));
            }
            if l.kind == "query"
                && !l
                    .query
                    .as_ref()
                    .is_some_and(|q| self.query.iter().any(|x| &x.id == q))
            {
                return bad(format!("load {}: query must name a [[query]] id", l.id));
            }
            let class_ok = |c: &Option<String>| {
                c.as_deref().is_none_or(|c| {
                    stratadb_common::workload::WorkClass::parse(c)
                        .is_some_and(|c| stratadb_common::workload::WorkClass::QUERY.contains(&c))
                })
            };
            if !class_ok(&l.class) {
                return bad(format!(
                    "load {}: class must be interactive, normal or batch",
                    l.id
                ));
            }
            if let Some(a) = &l.alongside {
                if l.kind != "query"
                    || !self.query.iter().any(|x| x.id == a.query)
                    || a.workers == 0
                    || a.workers > 256
                    || !class_ok(&a.class)
                {
                    return bad(format!(
                        "load {}: alongside needs a query load, a [[query]] id, workers in 1..=256 and a query class",
                        l.id
                    ));
                }
            }
            if l.mode == "open" {
                let n = l.rate_per_s.unwrap_or(0.0) * l.duration_s;
                if n > 1_000_000.0 {
                    return bad(format!("load {}: more than 1,000,000 requests", l.id));
                }
            }
        }
        Ok(())
    }
}

/// One plan-property assertion.
#[derive(Debug, Clone, PartialEq)]
pub enum Expect {
    Cmp(&'static str, f64),
    Contains(String),
}

pub fn parse_expect(key: &str, v: &str) -> Result<Expect> {
    let v = v.trim();
    if key == "indexes_used" {
        return match v.strip_prefix("contains ") {
            Some(x) => Ok(Expect::Contains(x.trim().to_owned())),
            None => Err(Error::invalid_input(
                "indexes_used expects \"contains <index>\"",
            )),
        };
    }
    if !STAT_KEYS.contains(&key) {
        return Err(Error::invalid_input(format!(
            "unknown expectation {key:?} (one of {})",
            STAT_KEYS.join(", ")
        )));
    }
    for op in [">=", "<=", "==", "!=", ">", "<"] {
        if let Some(n) = v.strip_prefix(op) {
            let n: f64 = n
                .trim()
                .parse()
                .map_err(|_| Error::invalid_input(format!("{key}: bad number in {v:?}")))?;
            return Ok(Expect::Cmp(op, n));
        }
    }
    Err(Error::invalid_input(format!(
        "{key}: expected `<op> <number>`, got {v:?}"
    )))
}

const STAT_KEYS: &[&str] = &[
    "result_rows",
    "rows_scanned",
    "rows_matched",
    "bytes_read",
    "segments_total",
    "segments_scanned",
    "segments_pruned",
    "segments_pruned_time",
    "segments_pruned_keys",
    "segments_pruned_severity",
    "segments_pruned_bloom",
    "segments_pruned_ngram",
    "shredded_reads",
    "memory_rows_scanned",
    "memory_chunks_pruned",
    "early_stop",
    "scan_workers",
    "sources_from_metadata",
    "dense_group_batches",
    "column_cache_hits",
    "sort_runs_merged",
    "topk_rows_skipped",
    "partials_from_cache",
];

fn stat(s: &QueryStats, result_rows: usize, key: &str) -> f64 {
    let pruned = s.segments_pruned_time
        + s.segments_pruned_keys
        + s.segments_pruned_severity
        + s.segments_pruned_bloom
        + s.segments_pruned_ngram
        + s.segments_pruned_cursor;
    (match key {
        "result_rows" => result_rows as u64,
        "rows_scanned" => s.rows_scanned,
        "rows_matched" => s.rows_matched,
        "bytes_read" => s.bytes_read,
        "segments_total" => s.segments_total as u64,
        "segments_scanned" => s.segments_scanned as u64,
        "segments_pruned" => pruned as u64,
        "segments_pruned_time" => s.segments_pruned_time as u64,
        "segments_pruned_keys" => s.segments_pruned_keys as u64,
        "segments_pruned_severity" => s.segments_pruned_severity as u64,
        "segments_pruned_bloom" => s.segments_pruned_bloom as u64,
        "segments_pruned_ngram" => s.segments_pruned_ngram as u64,
        "shredded_reads" => s.shredded_reads,
        "memory_rows_scanned" => s.memory_rows_scanned,
        "memory_chunks_pruned" => s.memory_chunks_pruned,
        "early_stop" => u64::from(s.early_stop),
        "scan_workers" => s.scan_workers as u64,
        "sources_from_metadata" => s.sources_from_metadata,
        "dense_group_batches" => s.dense_group_batches,
        "column_cache_hits" => s.column_cache_hits,
        "sort_runs_merged" => s.sort_runs_merged,
        "topk_rows_skipped" => s.topk_rows_skipped,
        "partials_from_cache" => s.partials_from_cache,
        _ => 0,
    }) as f64
}

/// Evaluate assertions; returns failure messages.
pub fn check_expectations(
    spec: &BTreeMap<String, String>,
    s: &QueryStats,
    result_rows: usize,
) -> Vec<String> {
    let mut fails = Vec::new();
    for (k, v) in spec {
        let ok = match parse_expect(k, v) {
            Ok(Expect::Contains(x)) => s.indexes_used.iter().any(|i| i == &x),
            Ok(Expect::Cmp(op, n)) => {
                let a = stat(s, result_rows, k);
                match op {
                    ">=" => a >= n,
                    "<=" => a <= n,
                    "==" => a == n,
                    "!=" => a != n,
                    ">" => a > n,
                    _ => a < n,
                }
            }
            Err(e) => {
                fails.push(e.to_string());
                continue;
            }
        };
        if !ok {
            let actual = if k == "indexes_used" {
                format!("{:?}", s.indexes_used)
            } else {
                stat(s, result_rows, k).to_string()
            };
            fails.push(format!("{k} {v} (actual {actual})"));
        }
    }
    fails
}

/// Spec VIII.1.2 per-stage budgets (p50, p99) in µs for a 1,000-row batch on
/// HW-REF-1. Stages not measured in-process (socket, auth, decode) are
/// reported by the server's `strata_insert_stage_seconds` instead.
const INSERT_BUDGETS_US: &[(&str, f64, f64)] = &[
    ("wal_encode", 20.0, 60.0),
    ("wal_append", 3.0, 15.0),
    ("group_commit_wait", 250.0, 1500.0),
    ("memtable_insert", 30.0, 100.0),
];

fn durability(e: &EngineCfg) -> Durability {
    match e.durability.as_str() {
        "async" => Durability::Async,
        "sync" => Durability::Sync,
        _ => Durability::GroupCommit {
            max_delay: Duration::from_millis(e.group_commit_delay_ms),
        },
    }
}

struct Placeholders(BTreeMap<&'static str, String>);

impl Placeholders {
    fn new(gen: &Generator, events: usize) -> Self {
        let end = gen.ingest_time(events as u64);
        let start = gen.start_ns;
        let span = (end - start).max(1);
        let lit = |t: i64| format!("TIMESTAMP '{}'", format_rfc3339(t));
        let mut m = BTreeMap::new();
        m.insert(
            "trace_id",
            gen.sample_trace_ids
                .first()
                .cloned()
                .unwrap_or_else(|| "0".repeat(32)),
        );
        m.insert(
            "request_id",
            gen.sample_request_ids
                .first()
                .cloned()
                .unwrap_or_else(|| "req_none".into()),
        );
        m.insert("t_start", lit(start));
        m.insert("t_end", lit(end));
        m.insert("t_recent", lit(end - span / 20));
        m.insert("t_mid_lo", lit(start + span * 45 / 100));
        m.insert("t_mid_hi", lit(start + span * 55 / 100));
        Self(m)
    }

    fn fill(&self, sql: &str) -> Result<String> {
        let mut out = sql.to_owned();
        for (k, v) in &self.0 {
            out = out.replace(&format!("{{{k}}}"), v);
        }
        // Any `{identifier}` left is a typo'd placeholder (braces around
        // anything else, e.g. JSON inside a string literal, are data).
        for (i, _) in out.match_indices('{') {
            let rest = &out[i + 1..];
            if let Some(end) = rest.find('}') {
                let name = &rest[..end];
                if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    return Err(Error::invalid_input(format!(
                        "unknown placeholder {{{name}}}"
                    )));
                }
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Serialize)]
struct QueryResult {
    id: String,
    sql: String,
    tenant: String,
    repetitions: usize,
    /// Raw samples (ms), in run order.
    wall_ms: Vec<f64>,
    process_cpu_ms: Vec<f64>,
    wall: Summary,
    process_cpu: Summary,
    /// Median process CPU ns per row scanned (CPU time, not cycles:
    /// STRATA-SYS-01-014 needs hardware counters).
    cpu_ns_per_row_scanned: Option<f64>,
    result_rows: usize,
    rows_scanned: u64,
    segments_total: usize,
    segments_scanned: usize,
    /// Memtable rows scanned and chunk windows pruned by zone maps.
    memory_rows_scanned: u64,
    memory_chunks_pruned: u64,
    indexes_used: Vec<String>,
    plan_us_median: f64,
    /// Workers that scanned segments in the last repetition.
    scan_workers: usize,
    est_cpu_ms: f64,
    expectations: BTreeMap<String, String>,
    expectation_failures: Vec<String>,
}

/// Run `scenario` (raw TOML `text`), writing results into `out_dir`.
/// Returns the results document; `Err` only for setup failures — failed
/// expectations and flags are reported in the document's `verdict`.
pub fn run(text: &str, out_dir: &Path, hw_profile: &str, data_dir: Option<PathBuf>) -> Result<J> {
    let sc = Scenario::parse(text)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|e| Error::invalid_input(format!("creating {}: {e}", out_dir.display())))?;
    let tmp;
    let dir = match data_dir {
        Some(d) => d,
        None => {
            tmp = unique_temp("strata-scenario");
            let _ = std::fs::remove_dir_all(&tmp);
            tmp.clone()
        }
    };
    std::fs::create_dir_all(&dir)
        .map_err(|e| Error::invalid_input(format!("creating {}: {e}", dir.display())))?;
    let settings =
        json!({"engine": &sc.engine, "query_options": &sc.query_options, "run": &sc.run});
    let manifest = crate::manifest::collect(
        &dir,
        hw_profile,
        json!({"generator": "stratadb-testkit", "generator_version": env!("CARGO_PKG_VERSION"), "spec": &sc.dataset}),
        text.as_bytes(),
        settings,
    );
    write_json(&out_dir.join("manifest.json"), &manifest)?;

    let profile = Profile::parse(&sc.dataset.profile)
        .ok_or_else(|| Error::invalid_input("unknown profile"))?;
    let mut opts = EngineOptions::new(&dir);
    opts.wal.durability = durability(&sc.engine);
    opts.wal.adaptive_group_delay = sc.engine.adaptive_group_delay;
    opts.wal.format_version = sc.engine.wal_format_version;
    opts.column_cache_bytes = sc.engine.column_cache_bytes;
    opts.bloom_enabled = sc.engine.bloom;
    opts.ngram_enabled = sc.engine.ngram;
    opts.segment_compression = sc.engine.compression;
    opts.segment_shredding = sc.engine.shredding;
    if let Some(b) = sc.engine.memtable_max_bytes {
        opts.memtable_max_bytes = b;
    }
    if let Some(r) = sc.engine.memtable_max_rows {
        opts.memtable_max_rows = r;
    }
    if let Some(a) = sc.engine.memtable_max_age_s {
        opts.memtable_max_age = Duration::from_secs(a);
    }
    // Server-like memory tree, unlimited: measures the mechanism, never
    // changes results.
    let memory = sc.engine.memory_accounting.then(|| {
        use stratadb_common::memory::MemoryTracker;
        let root = MemoryTracker::root("process", 0);
        opts.memory = Some(root.child("ingest", 0));
        opts.cache_memory = Some(root.child("cache", 0));
        opts.background_memory = Some(root.child("background", 0));
        let pool = root.child("query", 0);
        (root, pool.private_child("tenant:bench", 0))
    });
    let deterministic = sc.dataset.flush_every.is_some();
    let background = opts.background;
    if deterministic {
        opts.background = false;
    }
    let mut engine = Arc::new(Engine::open(opts.clone())?);
    let mut gen = Generator::new(profile, sc.dataset.seed, sc.dataset.tenants);

    // --- Dataset load (closed loop, throughput only).
    let ingest = load_dataset(&engine, &mut gen, &sc.dataset)?;
    let st = engine.stats();
    if deterministic {
        // Layout fixed by explicit flushes; queries and loads then run with
        // background flushing as in production.
        drop(engine);
        opts.background = background;
        engine = Arc::new(Engine::open(opts)?);
    }
    let stage_report: Vec<J> = st
        .insert_stages
        .iter()
        .map(|(name, h)| {
            let q = |p: f64| h.quantile_upper_bound(p).map(|s| s * 1e6);
            let budget = INSERT_BUDGETS_US.iter().find(|b| b.0 == *name);
            json!({
                "stage": name, "count": h.count, "mean_us": if h.count > 0 { h.sum / h.count as f64 * 1e6 } else { 0.0 },
                "p50_upper_bound_us": q(0.5), "p99_upper_bound_us": q(0.99),
                "budget_p50_us": budget.map(|b| b.1), "budget_p99_us": budget.map(|b| b.2),
            })
        })
        .collect();

    // --- Queries: warmup, then repetitions with CPU accounting.
    let holders = Placeholders::new(&gen, sc.dataset.events);
    let qopts = QueryOptions {
        max_result_rows: 1_000_000,
        enable_vectorized: sc.query_options.vectorized,
        enable_map_projection: sc.query_options.map_projection,
        parallelism: if sc.query_options.parallelism == 0 {
            QueryOptions::default().parallelism
        } else {
            sc.query_options.parallelism
        },
        target_ms_per_worker: sc
            .query_options
            .target_ms_per_worker
            .unwrap_or(QueryOptions::default().target_ms_per_worker),
        memory: memory
            .as_ref()
            .map(|(_, tenant)| std::sync::Arc::clone(tenant)),
        partial_cache: (sc.engine.partial_cache_bytes > 0).then(|| {
            std::sync::Arc::new(stratadb_query::PartialCache::new(
                sc.engine.partial_cache_bytes,
                None,
            ))
        }),
        ..QueryOptions::default()
    };
    let mut queries = Vec::new();
    let mut sqls: BTreeMap<String, (TenantId, String)> = BTreeMap::new();
    for q in &sc.query {
        let sql = holders.fill(&q.sql)?;
        let tenant = TenantId::new(&q.tenant)?;
        sqls.insert(q.id.clone(), (tenant.clone(), sql.clone()));
        for _ in 0..sc.run.warmup {
            execute(&engine.snapshot(&tenant), &sql, &qopts)?;
        }
        let (mut wall, mut cpu, mut plan) = (Vec::new(), Vec::new(), Vec::new());
        let mut last = None;
        for _ in 0..sc.run.repetitions {
            let t = CpuTimer::start();
            let out = execute(&engine.snapshot(&tenant), &sql, &qopts)?;
            let s = t.stop();
            wall.push(s.wall.as_secs_f64() * 1e3);
            cpu.push(s.process_cpu.as_secs_f64() * 1e3);
            plan.push(out.stats.plan.as_secs_f64() * 1e6);
            last = Some(out);
        }
        let out = last.ok_or_else(|| Error::internal("no repetitions"))?;
        let s = &out.stats;
        let cpu_sum = summarize(&cpu);
        queries.push(QueryResult {
            id: q.id.clone(),
            sql,
            tenant: q.tenant.clone(),
            repetitions: sc.run.repetitions,
            wall: summarize(&wall),
            process_cpu: cpu_sum.clone(),
            wall_ms: wall,
            process_cpu_ms: cpu,
            cpu_ns_per_row_scanned: (s.rows_scanned > 0)
                .then(|| cpu_sum.median * 1e6 / s.rows_scanned as f64),
            result_rows: out.rows.len(),
            rows_scanned: s.rows_scanned,
            segments_total: s.segments_total,
            segments_scanned: s.segments_scanned,
            memory_rows_scanned: s.memory_rows_scanned,
            memory_chunks_pruned: s.memory_chunks_pruned,
            indexes_used: s.indexes_used.clone(),
            plan_us_median: summarize(&plan).median,
            scan_workers: s.scan_workers,
            est_cpu_ms: s.est_cpu_ms,
            expectations: q.expect.clone(),
            expectation_failures: check_expectations(&q.expect, s, out.rows.len()),
        });
    }

    // --- Load blocks.
    let mut loads = Vec::new();
    for l in &sc.load {
        let r = run_load(&engine, &sc, l, &sqls, &qopts)?;
        loads.push((l.id.clone(), l.kind.clone(), r));
    }

    drop(engine);
    // Keep caller-provided directories; remove our own temporary one.
    if dir
        .file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with("strata-scenario-"))
    {
        let _ = std::fs::remove_dir_all(&dir);
    }

    let mut failures: Vec<String> = queries
        .iter()
        .flat_map(|q| {
            q.expectation_failures
                .iter()
                .map(move |f| format!("{}: {f}", q.id))
        })
        .collect();
    let mut flags: Vec<String> = Vec::new();
    for (id, _, r) in &loads {
        for f in &r.flags {
            flags.push(format!("{id}: {f}"));
        }
        if r.errors > 0 {
            failures.push(format!(
                "{id}: {} errors (first: {})",
                r.errors,
                r.first_error.clone().unwrap_or_default()
            ));
        }
    }
    let client_bound = loads
        .iter()
        .any(|(_, _, r)| r.flags.contains(&"CLIENT_BOUND"));
    let results = json!({
        "results_version": 1,
        "scenario": sc.name,
        "description": sc.description,
        "hw_profile": hw_profile,
        "runner_fingerprint": manifest["runner_fingerprint"],
        "started_at": manifest["started_at"],
        "durability": sc.engine.durability,
        "cache": sc.run.cache,
        "label": "measurement only; not a capacity claim. Compare only runs with the same runner_fingerprint, durability and dataset.",
        "ingest": ingest,
        "insert_stages": {
            "note": "upper bounds of the histogram bucket holding the quantile; budgets are spec VIII.1.2 targets for HW-REF-1 (1,000-row batches) and gate only on that profile (STRATA-SYS-01-002)",
            "gate_eligible": hw_profile == "HW-REF-1",
            "batch_size": sc.dataset.batch_size,
            "stages": stage_report,
        },
        "queries": queries,
        "loads": loads.iter().map(|(id, kind, r)| json!({"id": id, "kind": kind, "report": r})).collect::<Vec<_>>(),
        "verdict": {
            "ok": failures.is_empty() && !client_bound,
            "excluded_client_bound": client_bound,
            "failures": failures,
            "flags": flags,
        },
    });
    write_json(&out_dir.join("results.json"), &results)?;
    Ok(results)
}

/// A fresh temporary directory path, unique per process and call.
pub fn unique_temp(prefix: &str) -> PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("{prefix}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn write_json(p: &Path, v: &J) -> Result<()> {
    let text = serde_json::to_string_pretty(v).map_err(|e| Error::internal(e.to_string()))?;
    std::fs::write(p, text)
        .map_err(|e| Error::invalid_input(format!("writing {}: {e}", p.display())))
}

fn load_dataset(engine: &Engine, gen: &mut Generator, d: &Dataset) -> Result<J> {
    let mut ack = Vec::new();
    let t = CpuTimer::start();
    let mut written = 0usize;
    let mut next_flush = d.flush_every.unwrap_or(usize::MAX);
    let mut flushes = 0usize;
    while written < d.events {
        let n = d.batch_size.min(d.events - written);
        let mut by_tenant: BTreeMap<TenantId, Vec<Event>> = BTreeMap::new();
        for e in gen.batch(n) {
            by_tenant.entry(e.tenant_id.clone()).or_default().push(e);
        }
        for (tenant, evs) in by_tenant {
            let t0 = Instant::now();
            engine.write_batch(&tenant, evs)?;
            ack.push(t0.elapsed().as_secs_f64() * 1e3);
        }
        written += n;
        if written >= next_flush && d.max_flushes.is_none_or(|m| flushes < m) {
            flushes += 1;
            engine.flush()?;
            next_flush = next_flush.saturating_add(d.flush_every.unwrap_or(usize::MAX));
        }
    }
    let s = t.stop();
    let flush_t = Instant::now();
    if d.final_flush {
        engine.flush()?;
    }
    let flush_ms = flush_t.elapsed().as_secs_f64() * 1e3;
    let st = engine.stats();
    Ok(json!({
        "label": "closed-loop bulk load (throughput only). Burst, not sustained: it ends before compaction cycles complete, so it is not publishable as sustained ingest (STRATA-SYS-01-004/005).",
        "events": d.events,
        "wall_s": s.wall.as_secs_f64(),
        "events_per_s": d.events as f64 / s.wall.as_secs_f64().max(1e-9),
        "process_cpu_s": s.process_cpu.as_secs_f64(),
        "cpu_ns_per_event": s.process_cpu.as_secs_f64() * 1e9 / d.events.max(1) as f64,
        "ack_ms": summarize(&ack),
        "final_flush_ms": flush_ms,
        "segments": st.segments,
        "segment_bytes": st.segment_bytes,
        "wal_bytes_appended": st.wal.bytes_appended,
        "wal_bytes_per_event": st.wal.bytes_appended as f64 / (d.events as f64).max(1.0),
    }))
}

fn run_load(
    engine: &Arc<Engine>,
    sc: &Scenario,
    l: &LoadSpec,
    sqls: &BTreeMap<String, (TenantId, String)>,
    qopts: &QueryOptions,
) -> Result<LoadReport> {
    let duration = Duration::from_secs_f64(l.duration_s);
    let n = l
        .rate_per_s
        .map_or(0, |r| (r * l.duration_s).ceil() as usize);
    match l.kind.as_str() {
        "ingest" => {
            // Payloads are generated before the timeline (client cost off the
            // critical path) for a dedicated tenant so query tenants are
            // unaffected. Closed loop reuses a bounded pool of batches.
            let tenant = TenantId::new("bench-load")?;
            let per = l.batch_size.unwrap_or(sc.dataset.batch_size).max(1);
            let count = if l.mode == "open" { n } else { 256 };
            let mut g = Generator::new(
                Profile::parse(&sc.dataset.profile).unwrap_or(Profile::ALL[0]),
                sc.dataset.seed ^ 0xA5A5,
                1,
            );
            let batches: Vec<Vec<Event>> = (0..count)
                .map(|_| {
                    g.batch(per)
                        .into_iter()
                        .map(|mut e| {
                            e.tenant_id = tenant.clone();
                            e
                        })
                        .collect()
                })
                .collect();
            let batches = std::sync::Mutex::new(batches.into_iter().map(Some).collect::<Vec<_>>());
            let work = |i: usize| -> Result<()> {
                // Take a pre-generated batch under the lock, then release
                // it before writing: holding it across `write_batch` (which
                // waits for an fsync) would serialize every writer.
                let pooled = {
                    let mut b = batches.lock().unwrap_or_else(|e| e.into_inner());
                    let len = b.len();
                    b[i % len].take()
                };
                let evs = pooled.unwrap_or_else(|| regen(&tenant, per, i));
                engine.write_batch(&tenant, evs).map(|_| ())
            };
            Ok(if l.mode == "open" {
                run_open(l.rate_per_s.unwrap_or(1.0), n, l.workers, work)
            } else {
                run_closed(duration, l.workers, work)
            })
        }
        _ => {
            let lookup = |id: &str| {
                sqls.get(id)
                    .cloned()
                    .ok_or_else(|| Error::invalid_input(format!("unknown query {id}")))
            };
            let with_class = |c: &Option<String>| {
                let class = c
                    .as_deref()
                    .and_then(stratadb_common::workload::WorkClass::parse)
                    .unwrap_or(stratadb_common::workload::WorkClass::Normal);
                QueryOptions {
                    class: Some(Arc::new(stratadb_common::workload::ClassCell::new(class))),
                    ..qopts.clone()
                }
            };
            let (tenant, sql) = lookup(l.query.as_deref().unwrap_or_default())?;
            let opts = with_class(&l.class);
            let work = |_: usize| execute(&engine.snapshot(&tenant), &sql, &opts).map(|_| ());
            let main = || {
                if l.mode == "open" {
                    run_open(l.rate_per_s.unwrap_or(1.0), n, l.workers, work)
                } else {
                    run_closed(duration, l.workers, work)
                }
            };
            let Some(a) = &l.alongside else {
                return Ok(main());
            };
            let (bt, bsql) = lookup(&a.query)?;
            let bopts = with_class(&a.class);
            let lead = Duration::from_millis(300);
            let span = if l.mode == "open" {
                Duration::from_secs_f64(n as f64 / l.rate_per_s.unwrap_or(1.0))
            } else {
                duration
            };
            let bwork = |_: usize| execute(&engine.snapshot(&bt), &bsql, &bopts).map(|_| ());
            let (mut r, b) = std::thread::scope(|s| {
                let h = s.spawn(|| run_closed(lead + span, a.workers, bwork));
                std::thread::sleep(lead);
                let r = main();
                (r, h.join())
            });
            r.alongside = b.ok().map(Box::new);
            Ok(r)
        }
    }
}

/// Fallback payload when a closed-loop run exhausts the pre-generated pool:
/// minimal valid events (cheap to build, deterministic).
fn regen(tenant: &TenantId, n: usize, i: usize) -> Vec<Event> {
    (0..n)
        .map(|j| {
            let mut e = Event::new(
                tenant.clone(),
                1_795_000_000_000_000_000 + (i * n + j) as i64,
                1,
            );
            e.body = Some(stratadb_schema::Value::String(format!("load {i} {j}")));
            e
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINI: &str = r#"
name = "mini"
[dataset]
profile = "app-logs"
events = 3000
tenants = 2
seed = 7
batch_size = 500
[engine]
durability = "async"
[run]
warmup = 1
repetitions = 5
[[query]]
id = "count"
sql = "SELECT count(*) FROM logs WHERE event_time >= {t_start}"
[query.expect]
result_rows = "== 1"
rows_matched = ">= 1"
sources_from_metadata = ">= 1"
[[query]]
id = "trace"
sql = "SELECT body FROM logs WHERE trace_id = '{trace_id}'"
[query.expect]
segments_pruned_bloom = ">= 0"
[[load]]
id = "ingest_open"
kind = "ingest"
mode = "open"
rate_per_s = 200
duration_s = 0.25
workers = 2
batch_size = 20
[[load]]
id = "query_closed"
kind = "query"
mode = "closed"
duration_s = 0.1
workers = 2
query = "count"
"#;

    #[test]
    fn runs_end_to_end_and_records_everything() {
        let out = tempfile::tempdir().unwrap();
        let r = run(MINI, out.path(), "HW-REF-7", None).unwrap();
        assert!(r["verdict"]["ok"].as_bool().unwrap(), "{:#}", r["verdict"]);
        assert_eq!(r["durability"], "async");
        let q = &r["queries"][0];
        assert_eq!(q["id"], "count");
        assert_eq!(q["wall_ms"].as_array().unwrap().len(), 5);
        assert!(q["wall"]["median_ci95"].is_array());
        // Answered from metadata: no rows scanned, so no per-row CPU.
        assert_eq!(q["rows_scanned"], 0);
        assert!(q["cpu_ns_per_row_scanned"].is_null());
        let stages: Vec<&str> = r["insert_stages"]["stages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["stage"].as_str().unwrap())
            .collect();
        assert_eq!(
            stages,
            [
                "admission",
                "wal_encode",
                "wal_append",
                "group_commit_wait",
                "memtable_insert"
            ]
        );
        assert_eq!(r["insert_stages"]["gate_eligible"], false);
        let loads = r["loads"].as_array().unwrap();
        assert_eq!(loads[0]["report"]["mode"], "open");
        assert_eq!(loads[0]["report"]["requests"], 50);
        assert_eq!(loads[1]["report"]["mode"], "closed");
        assert!(out.path().join("manifest.json").exists());
        let saved: J = serde_json::from_str(
            &std::fs::read_to_string(out.path().join("results.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(saved["scenario"], "mini");
    }

    #[test]
    fn failed_plan_assertions_fail_the_verdict() {
        let sc = MINI.replace("result_rows = \"== 1\"", "result_rows = \"== 2\"");
        let out = tempfile::tempdir().unwrap();
        let r = run(&sc, out.path(), "HW-REF-7", None).unwrap();
        assert_eq!(r["verdict"]["ok"], false);
        let f = r["verdict"]["failures"][0].as_str().unwrap();
        assert!(f.contains("count: result_rows == 2 (actual 1)"), "{f}");
    }

    #[test]
    fn invalid_scenarios_are_rejected_before_running() {
        for (from, to, why) in [
            ("repetitions = 5", "repetitions = 3", ">= 5"),
            (
                "durability = \"async\"",
                "durability = \"fast\"",
                "durability",
            ),
            ("[run]", "[run]\ncache = \"cold\"", "warm"),
            (
                "result_rows = \"== 1\"",
                "result_rowz = \"== 1\"",
                "unknown expectation",
            ),
            (
                "rows_matched = \">= 1\"",
                "rows_matched = \"about 3\"",
                "expected",
            ),
            ("query = \"count\"", "query = \"nope\"", "must name"),
            ("rate_per_s = 200", "rate_per_s = 0", "rate_per_s"),
            ("seed = 7", "seed = 7\nbogus = 1", "unknown field"),
        ] {
            let e = Scenario::parse(&MINI.replace(from, to))
                .unwrap_err()
                .to_string();
            assert!(e.contains(why), "{from} -> {to}: {e}");
        }
        assert!(Scenario::parse(MINI).is_ok());
    }

    #[test]
    fn placeholders_fill_and_unknown_ones_are_errors() {
        let gen = Generator::new(Profile::ALL[0], 1, 1);
        let p = Placeholders::new(&gen, 100);
        assert!(p.fill("x >= {t_start}").unwrap().contains("TIMESTAMP '"));
        assert!(p
            .fill("x = {nope}")
            .unwrap_err()
            .to_string()
            .contains("{nope}"));
        // Braces inside string literals are data.
        assert!(p.fill("body = '{\"a\":1}'").is_ok());
    }

    #[test]
    fn expectations_compare_stats() {
        let s = QueryStats {
            segments_pruned_bloom: 2,
            rows_scanned: 10,
            indexes_used: vec!["bloom(trace_id)".into()],
            ..QueryStats::default()
        };
        let e = |k: &str, v: &str| BTreeMap::from([(k.to_owned(), v.to_owned())]);
        assert!(check_expectations(&e("segments_pruned_bloom", ">= 1"), &s, 0).is_empty());
        assert!(check_expectations(&e("segments_pruned", "> 1"), &s, 0).is_empty());
        assert!(check_expectations(&e("rows_scanned", "< 10"), &s, 0).len() == 1);
        assert!(
            check_expectations(&e("indexes_used", "contains bloom(trace_id)"), &s, 0).is_empty()
        );
        assert!(check_expectations(&e("indexes_used", "contains ngram(body)"), &s, 0).len() == 1);
        assert!(check_expectations(&e("result_rows", "!= 0"), &s, 3).is_empty());
    }
}
