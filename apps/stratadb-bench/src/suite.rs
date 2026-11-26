//! The fixed query suite (spec §27.4): `strata-bench [flags]` without a
//! subcommand. Kept for continuity with earlier reports; new measurements
//! should use TOML scenarios (`strata-bench scenario`, docs/benchmarks/harness.md).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::{json, Value as J};
use stratadb_common::time::format_rfc3339;
use stratadb_common::{Error, Result, TenantId};
use stratadb_query::{execute, QueryOptions};
use stratadb_schema::codec::encode_batch;
use stratadb_schema::Event;
use stratadb_storage::{Durability, Engine, EngineOptions};
use stratadb_testkit::{Generator, Profile};

#[derive(clap::Args, Debug)]
pub struct Args {
    #[arg(long, default_value = "app-logs")]
    pub profile: String,
    #[arg(long, default_value_t = 100_000)]
    pub events: usize,
    #[arg(long, default_value_t = 1000)]
    pub batch_size: usize,
    #[arg(long, default_value_t = 4)]
    pub tenants: usize,
    #[arg(long, default_value_t = 1)]
    pub seed: u64,
    /// async | sync | group_commit
    #[arg(long, default_value = "group_commit")]
    pub durability: String,
    #[arg(long, default_value_t = 5)]
    pub reps: usize,
    /// Data directory (default: a fresh temporary directory, removed afterwards).
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Write JSON here instead of stdout.
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Disable Bloom filters (A/B comparisons).
    #[arg(long)]
    pub no_bloom: bool,
    /// Disable segment chunk compression (A/B comparisons).
    #[arg(long)]
    pub no_compression: bool,
    /// Disable attribute shredding (A/B comparisons).
    #[arg(long)]
    pub no_shredding: bool,
    /// Disable vectorized filter evaluation (A/B comparisons).
    #[arg(long)]
    pub no_vectorize: bool,
    /// Disable trigram (ngram) filters for LIKE (A/B comparisons).
    #[arg(long)]
    pub no_ngram: bool,
    /// Scan segments sequentially (A/B comparisons).
    #[arg(long)]
    pub no_parallel: bool,
    /// Decode whole attribute maps instead of projecting referenced keys (A/B).
    #[arg(long)]
    pub no_map_projection: bool,
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn summary(mut ms: Vec<f64>) -> J {
    ms.sort_by(f64::total_cmp);
    json!({"n": ms.len(), "min_ms": ms.first(), "p50_ms": pct(&ms, 0.5), "p95_ms": pct(&ms, 0.95),
           "p99_ms": pct(&ms, 0.99), "max_ms": ms.last(),
           "mean_ms": if ms.is_empty() { 0.0 } else { ms.iter().sum::<f64>() / ms.len() as f64 }})
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

pub fn run(a: Args) -> Result<J> {
    let profile = Profile::parse(&a.profile)
        .ok_or_else(|| Error::invalid_input(format!("unknown profile {}", a.profile)))?;
    let durability = match a.durability.as_str() {
        "async" => Durability::Async,
        "sync" => Durability::Sync,
        "group_commit" => Durability::GroupCommit {
            max_delay: Duration::from_millis(2),
        },
        d => return Err(Error::invalid_input(format!("unknown durability {d}"))),
    };
    let tmp;
    let dir = match &a.data_dir {
        Some(d) => d.clone(),
        None => {
            tmp = std::env::temp_dir().join(format!("strata-bench-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&tmp);
            tmp.clone()
        }
    };
    let mut opts = EngineOptions::new(&dir);
    opts.wal.durability = durability;
    opts.bloom_enabled = !a.no_bloom;
    opts.ngram_enabled = !a.no_ngram;
    opts.segment_compression = !a.no_compression;
    opts.segment_shredding = !a.no_shredding;
    let engine = Engine::open(opts.clone())?;
    let mut gen = Generator::new(profile, a.seed, a.tenants);

    // --- Ingestion
    let mut ack_ms = Vec::new();
    let mut raw_bytes = 0u64;
    let started = Instant::now();
    let mut written = 0usize;
    let mut write_time = Duration::ZERO;
    while written < a.events {
        let n = a.batch_size.min(a.events - written);
        let batch = gen.batch(n);
        let mut by_tenant: BTreeMap<TenantId, Vec<Event>> = BTreeMap::new();
        for e in batch {
            by_tenant.entry(e.tenant_id.clone()).or_default().push(e);
        }
        for (t, evs) in by_tenant {
            raw_bytes += encode_batch(&evs).len() as u64;
            let t0 = Instant::now();
            engine.write_batch(&t, evs)?;
            let d = t0.elapsed();
            write_time += d;
            ack_ms.push(ms(d));
        }
        written += n;
    }
    let ingest_wall = started.elapsed();
    let t0 = Instant::now();
    let flush = engine.flush()?;
    let flush_ms = ms(t0.elapsed());
    let st = engine.stats();

    // --- Query suite (spec §27.4) against the largest tenant.
    let tenant = TenantId::new("tenant-0")?;
    let snap = engine.snapshot(&tenant);
    let end = gen.ingest_time(a.events as u64);
    let start = gen.start_ns;
    let span = (end - start).max(1);
    let lit = |t: i64| format!("TIMESTAMP '{}'", format_rfc3339(t));
    let trace = gen
        .sample_trace_ids
        .first()
        .cloned()
        .unwrap_or_else(|| "0".repeat(32));
    let req = gen
        .sample_request_ids
        .first()
        .cloned()
        .unwrap_or_else(|| "req_none".into());
    let suite: Vec<(&str, String)> = vec![
        ("01_trace_id_lookup", format!("SELECT event_time, body FROM logs WHERE trace_id = '{trace}'")),
        ("02_request_id_lookup", format!("SELECT event_time, body FROM logs WHERE attributes['request_id'] = '{req}'")),
        ("03_recent_errors", format!("SELECT event_time, service, body FROM logs WHERE event_time >= {} AND severity = 'ERROR' ORDER BY event_time DESC LIMIT 100", lit(end - span / 20))),
        ("04_service_severity", "SELECT count(*) FROM logs WHERE service = 'auth' AND severity = 'WARN'".into()),
        ("05_time_range_scan", format!("SELECT count(*) FROM logs WHERE event_time >= {} AND event_time < {}", lit(start + span * 45 / 100), lit(start + span * 55 / 100))),
        ("06_message_token_search", "SELECT count(*) FROM logs WHERE body LIKE '%TIMEOUT%'".into()),
        ("07_attribute_equality", "SELECT count(*) FROM logs WHERE attributes['region'] = 'eu-west-1'".into()),
        ("08_high_cardinality_agg", "SELECT attributes['user_id'] AS u, count(*) AS n FROM logs GROUP BY attributes['user_id'] ORDER BY n DESC LIMIT 10".into()),
        ("09_service_time_bucket", "SELECT service, time_bucket(INTERVAL '1 minute', event_time) AS b, count(*) FROM logs GROUP BY service, time_bucket(INTERVAL '1 minute', event_time)".into()),
        ("10_top_error_codes", "SELECT attributes['error.code'] AS code, count(*) AS n FROM logs WHERE severity IN ('ERROR', 'FATAL') GROUP BY attributes['error.code'] ORDER BY n DESC LIMIT 10".into()),
        ("12_historical_wide_range", "SELECT severity, count(*) FROM logs GROUP BY severity".into()),
        ("13_absent_substring_search", "SELECT count(*) FROM logs WHERE body LIKE '%deadlock detected%'".into()),
    ];
    let qopts = QueryOptions {
        max_result_rows: 1_000_000,
        enable_vectorized: !a.no_vectorize,
        enable_map_projection: !a.no_map_projection,
        parallelism: if a.no_parallel {
            1
        } else {
            QueryOptions::default().parallelism
        },
        ..QueryOptions::default()
    };
    let mut queries = Vec::new();
    for (name, sql) in &suite {
        let mut lat = Vec::new();
        let mut last = None;
        for _ in 0..a.reps.max(1) {
            let t0 = Instant::now();
            let out = execute(&snap, sql, &qopts)?;
            lat.push(ms(t0.elapsed()));
            last = Some(out);
        }
        let s = last.map(|o| o.stats).unwrap_or_default();
        let rows = execute(&snap, sql, &qopts)?.rows.len();
        queries.push(json!({
            "name": name, "sql": sql, "latency": summary(lat), "result_rows": rows,
            "rows_scanned": s.rows_scanned, "rows_matched": s.rows_matched, "segment_bytes_read": s.bytes_read,
            "segments_total": s.segments_total, "segments_scanned": s.segments_scanned,
            "indexes_used": s.indexes_used,
            "segments_pruned": s.segments_pruned_time + s.segments_pruned_keys + s.segments_pruned_severity + s.segments_pruned_bloom + s.segments_pruned_ngram,
        }));
    }
    drop(snap);

    // --- Restart (recovery) time: everything flushed, so this measures open cost.
    drop(engine);
    let t0 = Instant::now();
    let engine = Engine::open(opts)?;
    let reopen_ms = ms(t0.elapsed());
    drop(engine);
    if a.data_dir.is_none() {
        let _ = std::fs::remove_dir_all(&dir);
    }

    Ok(json!({
        "label": "developer-hardware measurement; not a production capacity claim",
        "stratadb_version": env!("CARGO_PKG_VERSION"),
        "build": if cfg!(debug_assertions) { "debug" } else { "release" },
        "environment": {"os": std::env::consts::OS, "arch": std::env::consts::ARCH,
                        "cpus": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0)},
        "config": {"profile": profile.name(), "events": a.events, "batch_size": a.batch_size,
                   "tenants": a.tenants, "seed": a.seed, "durability": a.durability, "reps": a.reps,
                   "bloom": !a.no_bloom, "compression": !a.no_compression, "shredding": !a.no_shredding, "vectorized": !a.no_vectorize, "ngram": !a.no_ngram, "parallel": !a.no_parallel, "map_projection": !a.no_map_projection,
                   "cache_state": "warm (same process, OS page cache not dropped)"},
        "ingest": {
            "wall_s": ingest_wall.as_secs_f64(),
            "events_per_s_wall": a.events as f64 / ingest_wall.as_secs_f64().max(1e-9),
            "encoded_bytes": raw_bytes,
            "encoded_bytes_per_s_wall": raw_bytes as f64 / ingest_wall.as_secs_f64().max(1e-9),
            "write_batch_time_s": write_time.as_secs_f64(),
            "ack_latency": summary(ack_ms),
        },
        "flush": {"final_flush_ms": flush_ms, "rows": flush.rows_flushed},
        "storage": {"segments": st.segments, "segment_bytes": st.segment_bytes,
                    "encoded_to_segment_ratio": raw_bytes as f64 / st.segment_bytes.max(1) as f64,
                    "flushes": st.flushes, "write_stalls": st.write_stalls},
        "queries": queries,
        "reopen_ms": reopen_ms,
    }))
}
