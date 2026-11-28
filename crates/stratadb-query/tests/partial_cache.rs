//! Per-segment partial aggregate cache (spec STRATA-ARCH-09-002/003,
//! ADR-0045):
//! - results are identical with the cache on and off, hit or miss;
//! - a repeated query scans only what is new;
//! - a moving time window keeps hitting the segments it covers;
//! - entries are keyed by segment identity, so compaction and new data
//!   invalidate exactly what changed;
//! - tenants never share entries;
//! - the memory region is charged exactly what the cache holds.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::sync::Arc;

use stratadb_common::memory::MemoryTracker;
use stratadb_common::TenantId;
use stratadb_query::{execute, PartialCache, QueryOptions, QueryOutput};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions, Snapshot};

const BASE: i64 = 1_795_170_600_000_000_000;
const MIN: i64 = 60_000_000_000;

fn t(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn events(tenant: &TenantId, minute: i64, n: i64) -> Vec<Event> {
    (0..n)
        .map(|j| {
            let x = (j * 7919 + minute * 104_729) % 1009;
            let mut ev = Event::new(tenant.clone(), BASE + minute * MIN + j * 1_000_000, 1);
            ev.service = Some(format!("svc-{}", x % 5));
            ev.severity = [Severity::Info, Severity::Error, Severity::Warn][(x % 3) as usize];
            ev.body = Some(Value::String(format!("request {x} took {} ms", x % 97)));
            ev.attributes.insert("ms".into(), Value::Int(x % 97));
            ev.attributes
                .insert("ratio".into(), Value::Float(x as f64 / 7.0));
            if x % 4 != 0 {
                ev.attributes
                    .insert("user".into(), Value::String(format!("u{}", x % 23)));
            }
            ev
        })
        .collect()
}

/// One segment per minute for `minutes`, plus a memtable batch after them.
fn engine(dir: &std::path::Path, tenant: &TenantId, minutes: i64) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    o.compaction.min_inputs = 2;
    o.old_segments_lifetime = std::time::Duration::ZERO;
    let e = Engine::open(o).unwrap();
    for m in 0..minutes {
        e.write_batch(tenant, events(tenant, m, 300)).unwrap();
        e.flush().unwrap();
    }
    e.write_batch(tenant, events(tenant, minutes, 300)).unwrap();
    e
}

fn opts(cache: Option<&Arc<PartialCache>>, parallelism: usize) -> QueryOptions {
    QueryOptions {
        partial_cache: cache.cloned(),
        parallelism,
        // Metadata answers would bypass both paths for plain counts.
        enable_metadata_aggregates: false,
        // Always per-segment partials, also without the cache (the cost
        // model would scan small queries sequentially, whose float sums
        // differ in the last bits from merged partials).
        target_ms_per_worker: 0.0,
        ..QueryOptions::default()
    }
}

fn run(s: &Snapshot, sql: &str, o: &QueryOptions) -> QueryOutput {
    execute(s, sql, o).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn ts(minute: i64) -> String {
    stratadb_common::time::format_rfc3339(BASE + minute * MIN)
}

fn queries() -> Vec<String> {
    vec![
        "SELECT count(*) FROM logs".into(),
        "SELECT service, count(*), sum(attributes['ms']), avg(attributes['ratio']), min(body), max(attributes['ms']) FROM logs GROUP BY service".into(),
        "SELECT attributes['user'] AS u, count(*) AS c FROM logs GROUP BY attributes['user'] ORDER BY c DESC, u LIMIT 7".into(),
        "SELECT severity, sum(attributes['ratio']) FROM logs WHERE body LIKE '%took 1%' GROUP BY severity".into(),
        format!("SELECT service, count(*) FROM logs WHERE event_time >= TIMESTAMP '{}' GROUP BY service", ts(3)),
        format!("SELECT count(*), avg(attributes['ms']) FROM logs WHERE event_time >= TIMESTAMP '{}' AND event_time < TIMESTAMP '{}' AND severity = 'ERROR'", ts(2), ts(6)),
        "SELECT count(*) FROM logs WHERE attributes['user'] = 'u3'".into(),
    ]
}

#[test]
fn results_are_identical_with_the_cache_hit_or_miss() {
    let a = t("a");
    let d = tempfile::tempdir().unwrap();
    let e = engine(d.path(), &a, 8);
    let cache = Arc::new(PartialCache::new(64 << 20, None));
    let snap = e.snapshot(&a);
    for sql in queries() {
        // Reference: no cache, per-segment partials (parallel path).
        let want = run(&snap, &sql, &opts(None, 4));
        for round in 0..3 {
            for parallelism in [1, 4] {
                let got = run(&snap, &sql, &opts(Some(&cache), parallelism));
                assert_eq!(
                    got.rows, want.rows,
                    "{sql} round {round} parallelism {parallelism}"
                );
                if round > 0 {
                    // Every segment not pruned comes from the cache; only
                    // the memtable is read.
                    let s = &got.stats;
                    let pruned = s.segments_pruned_time
                        + s.segments_pruned_keys
                        + s.segments_pruned_severity
                        + s.segments_pruned_bloom
                        + s.segments_pruned_ngram;
                    assert_eq!(s.partials_from_cache as usize, 8 - pruned, "{sql}: {s:?}");
                    assert_eq!(s.segments_scanned, 0, "{sql}");
                    assert_eq!(s.rows_scanned, s.memory_rows_scanned, "{sql}");
                    assert_eq!(s.rows_from_cache as usize, (8 - pruned) * 300, "{sql}");
                }
            }
        }
    }
    let s = cache.stats();
    assert!(s.hits > 0 && s.entries > 0, "{s:?}");
}

#[test]
fn a_moving_window_and_new_data_rescan_only_what_changed() {
    let a = t("a");
    let d = tempfile::tempdir().unwrap();
    let e = engine(d.path(), &a, 10);
    let cache = Arc::new(PartialCache::new(64 << 20, None));
    let q = |from_ms: i64| {
        format!(
            "SELECT service, count(*), sum(attributes['ms']) FROM logs WHERE event_time >= TIMESTAMP '{}' GROUP BY service",
            stratadb_common::time::format_rfc3339(BASE + from_ms * 1_000_000)
        )
    };
    // Window from minute 2 + 100 ms: minute 2 is on the edge, 3..=9 covered.
    let first = run(
        &e.snapshot(&a),
        &q(2 * 60_000 + 100),
        &opts(Some(&cache), 4),
    );
    assert_eq!(first.stats.partials_from_cache, 0);
    // The window moves by 50 ms: covered segments hit, the edge rescans.
    let sql = q(2 * 60_000 + 150);
    let moved = run(&e.snapshot(&a), &sql, &opts(Some(&cache), 4));
    assert_eq!(moved.stats.partials_from_cache, 7, "{:?}", moved.stats);
    assert_eq!(moved.stats.segments_scanned, 1);
    assert_eq!(moved.rows, run(&e.snapshot(&a), &sql, &opts(None, 4)).rows);
    // New data: one more flushed segment; only it and the memtable are read.
    e.flush().unwrap();
    e.write_batch(&a, events(&a, 11, 300)).unwrap();
    let grown = run(&e.snapshot(&a), &sql, &opts(Some(&cache), 4));
    // Same SQL as `moved`: the edge segment's partial (cached under these
    // exact bounds) hits too; only the new segment is scanned.
    assert_eq!(grown.stats.partials_from_cache, 8);
    assert_eq!(grown.stats.segments_scanned, 1, "only the new segment");
    assert_eq!(grown.rows, run(&e.snapshot(&a), &sql, &opts(None, 4)).rows);
    // A different non-time predicate is another query: no hits.
    let other = sql.replace("GROUP BY", "AND severity = 'WARN' GROUP BY");
    let o = run(&e.snapshot(&a), &other, &opts(Some(&cache), 4));
    assert_eq!(o.stats.partials_from_cache, 0);
    assert_eq!(o.rows, run(&e.snapshot(&a), &other, &opts(None, 4)).rows);
}

#[test]
fn compaction_yields_new_identities_and_tenants_never_share() {
    let (a, b) = (t("a"), t("b"));
    let d = tempfile::tempdir().unwrap();
    let e = engine(d.path(), &a, 4);
    for m in 0..4 {
        e.write_batch(&b, events(&b, m, 50)).unwrap();
        e.flush().unwrap();
    }
    let cache = Arc::new(PartialCache::new(64 << 20, None));
    let sql = "SELECT service, count(*), avg(attributes['ratio']) FROM logs GROUP BY service";
    run(&e.snapshot(&a), sql, &opts(Some(&cache), 4));
    // Same query text, other tenant: its own segments, no shared entries.
    let rb = run(&e.snapshot(&b), sql, &opts(Some(&cache), 4));
    assert_eq!(rb.stats.partials_from_cache, 0);
    assert_eq!(rb.rows, run(&e.snapshot(&b), sql, &opts(None, 4)).rows);
    let total: i64 = rb
        .rows
        .iter()
        .map(|r| match &r[1] {
            Some(Value::UInt(n)) => *n as i64,
            Some(Value::Int(n)) => *n,
            other => panic!("{other:?}"),
        })
        .sum();
    assert_eq!(total, 200, "tenant b sees only its 4 x 50 rows");
    // Compaction rewrites a's segments: the merged one is a miss, results
    // stay correct, and the old entries are never hit again.
    e.flush().unwrap();
    e.compact().unwrap();
    let after = run(&e.snapshot(&a), sql, &opts(Some(&cache), 4));
    assert_eq!(after.stats.partials_from_cache, 0, "{:?}", after.stats);
    assert_eq!(after.rows, run(&e.snapshot(&a), sql, &opts(None, 4)).rows);
    let again = run(&e.snapshot(&a), sql, &opts(Some(&cache), 4));
    assert_eq!(
        again.stats.partials_from_cache,
        again.stats.segments_total as u64
    );
    assert_eq!(again.rows, after.rows);
}

#[test]
fn the_cache_is_bounded_and_charges_its_memory_region() {
    let a = t("a");
    let d = tempfile::tempdir().unwrap();
    let e = engine(d.path(), &a, 6);
    let region = MemoryTracker::root("cache", 0);
    let cache = Arc::new(PartialCache::new(1 << 20, Some(Arc::clone(&region))));
    let sql = "SELECT attributes['user'], count(*) FROM logs GROUP BY attributes['user']";
    run(&e.snapshot(&a), sql, &opts(Some(&cache), 4));
    let s = cache.stats();
    assert_eq!(s.entries, 6);
    assert_eq!(region.used(), s.bytes as u64);
    // A full region: nothing more is cached, queries still work.
    let tiny = MemoryTracker::root("cache", 64);
    let small = Arc::new(PartialCache::new(1 << 20, Some(Arc::clone(&tiny))));
    let r = run(&e.snapshot(&a), sql, &opts(Some(&small), 4));
    assert_eq!(small.stats().entries, 0);
    assert_eq!(r.rows, run(&e.snapshot(&a), sql, &opts(None, 4)).rows);
    // Entries over 1/16 of the capacity are not cached.
    let little = Arc::new(PartialCache::new(1024, None));
    run(&e.snapshot(&a), sql, &opts(Some(&little), 4));
    assert_eq!(little.stats().entries, 0);
    drop(cache);
    assert_eq!(region.used(), 0, "released when the cache drops");
}

/// In-process A/B of repeated dashboard queries, cache off vs warm cache
/// (the window moves by one second per run, as a refreshing dashboard
/// does). Run with `cargo test --release -p stratadb-query --test
/// partial_cache dashboard_ab -- --ignored --nocapture`.
#[test]
#[ignore = "timing measurement"]
fn dashboard_ab() {
    let a = t("a");
    let d = tempfile::tempdir().unwrap();
    let mut o = EngineOptions::new(d.path());
    o.background = false;
    o.wal.durability = Durability::Async;
    let e = Engine::open(o).unwrap();
    // 24 segments of 20k rows (one per "hour"), plus a memtable batch.
    let hour = 60 * MIN;
    for h in 0..25i64 {
        let evs: Vec<Event> = events(&a, 0, 20_000)
            .into_iter()
            .enumerate()
            .map(|(j, mut ev)| {
                ev.event_time = BASE + h * hour + j as i64 * 150_000_000;
                ev
            })
            .collect();
        e.write_batch(&a, evs).unwrap();
        if h < 24 {
            e.flush().unwrap();
        }
    }
    let cache = Arc::new(PartialCache::new(256 << 20, None));
    let queries = [
        ("by_service", "SELECT service, count(*), avg(attributes['ms']), max(attributes['ratio']) FROM logs WHERE event_time >= TIMESTAMP '{from}' GROUP BY service"),
        ("by_user", "SELECT attributes['user'] AS u, count(*) AS c FROM logs WHERE event_time >= TIMESTAMP '{from}' GROUP BY attributes['user'] ORDER BY c DESC LIMIT 10"),
        ("errors", "SELECT count(*), sum(attributes['ms']) FROM logs WHERE event_time >= TIMESTAMP '{from}' AND severity = 'ERROR' AND body LIKE '%took 1%'"),
    ];
    for (name, tpl) in queries {
        let mut t = [Vec::new(), Vec::new()];
        let mut cpu = [Vec::new(), Vec::new()];
        let mut hits = 0;
        let mut mem_rows = 0;
        for round in 0..12i64 {
            // "Last 23 hours", one second later each run.
            let from = stratadb_common::time::format_rfc3339(BASE + hour + round * 1_000_000_000);
            let sql = tpl.replace("{from}", &from);
            for k in 0..2 {
                let cached = (round + k) % 2 == 0;
                let o = QueryOptions {
                    partial_cache: cached.then(|| Arc::clone(&cache)),
                    ..QueryOptions::default()
                };
                let started = std::time::Instant::now();
                let out = execute(&e.snapshot(&a), &sql, &o).unwrap();
                t[usize::from(cached)].push(started.elapsed().as_secs_f64() * 1e3);
                cpu[usize::from(cached)].push(out.stats.cpu.as_secs_f64() * 1e3);
                if cached {
                    hits = out.stats.partials_from_cache;
                    mem_rows = out.stats.memory_rows_scanned;
                }
            }
        }
        // Rounds 0-1 warm the cache; report the median of the rest.
        for v in t.iter_mut().chain(cpu.iter_mut()) {
            v.drain(..2);
            v.sort_by(f64::total_cmp);
        }
        let med = |v: &Vec<f64>| v[v.len() / 2];
        let (off, on) = (med(&t[0]), med(&t[1]));
        let (coff, con) = (med(&cpu[0]), med(&cpu[1]));
        println!(
            "{name:10} wall: off {off:7.2} ms, warm {on:6.2} ms ({:.1}x) | cpu: off {coff:7.2} ms, warm {con:6.2} ms ({:.1}x) | {hits}/24 segments cached, {mem_rows} memtable rows scanned",
            off / on,
            coff / con
        );
    }
}
