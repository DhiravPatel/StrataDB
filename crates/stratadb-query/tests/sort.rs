//! Normalized-key sorting, per-segment sorted runs and the loser-tree merge
//! (spec STRATA-SYS-14-001/002/006, ADR-0043): results are identical to the
//! comparison sort over concatenated rows, ties and pagination included.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use proptest::prelude::*;
use stratadb_common::TenantId;
use stratadb_query::{execute, execute_page, PageRequest, QueryOptions, QueryOutput};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("s").unwrap()
}

const BASE: i64 = 1_795_170_600_000_000_000;

/// 6 flushed segments plus a memtable batch. Keys have small domains (many
/// ties), long shared string prefixes (truncated normalized keys), mixed
/// numeric types in one attribute, and missing values.
fn engine() -> &'static (tempfile::TempDir, Engine) {
    static F: std::sync::OnceLock<(tempfile::TempDir, Engine)> = std::sync::OnceLock::new();
    F.get_or_init(|| {
        let d = tempfile::tempdir().unwrap();
        let mut o = EngineOptions::new(d.path());
        o.background = false;
        o.wal.durability = Durability::Async;
        let e = Engine::open(o).unwrap();
        for s in 0..7i64 {
            let evs: Vec<Event> = (0..1500i64)
                .map(|j| {
                    let x = (j * 7919 + s * 104_729) % 1009;
                    let mut ev = Event::new(tenant(), BASE + (x / 5) * 1_000_000, 1);
                    ev.event_id = Some(format!("s{s}-{j}"));
                    ev.service = Some(format!("service-{}", x % 13));
                    ev.severity =
                        [Severity::Info, Severity::Error, Severity::Warn][(x % 3) as usize];
                    ev.body = Some(Value::String(format!(
                        "a shared long prefix {} {}",
                        x % 17,
                        j % 5
                    )));
                    let n = match x % 4 {
                        0 => Some(Value::Int(x % 50 - 25)),
                        1 => Some(Value::Float((x % 50) as f64 / 2.0 - 12.5)),
                        2 => Some(Value::UInt((x % 50) as u64)),
                        _ => None,
                    };
                    if let Some(n) = n {
                        ev.attributes.insert("n".into(), n);
                    }
                    ev.attributes
                        .insert("user".into(), Value::String(format!("u{}", x % 97)));
                    ev
                })
                .collect();
            e.write_batch(&tenant(), evs).unwrap();
            if s < 6 {
                e.flush().unwrap();
            }
        }
        (d, e)
    })
}

/// `fast`: normalized sort and the top-k bound on; `false`: both off (the
/// reference). The variants in between are covered by `variant`.
fn opts(fast: bool, parallelism: usize) -> QueryOptions {
    variant(fast, fast, parallelism)
}

fn variant(normalized: bool, bound: bool, parallelism: usize) -> QueryOptions {
    QueryOptions {
        enable_normalized_sort: normalized,
        enable_topk_bound: bound,
        // The ordered scan has its own equivalence tests; keep the plain
        // scan so the sort itself is compared.
        enable_ordered_scan: false,
        parallelism,
        ..QueryOptions::default()
    }
}

fn run(sql: &str, normalized: bool, parallelism: usize) -> QueryOutput {
    execute(
        &engine().1.snapshot(&tenant()),
        sql,
        &opts(normalized, parallelism),
    )
    .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

const QUERIES: &[&str] = &[
    "SELECT event_id FROM logs ORDER BY service, event_time DESC LIMIT 100",
    "SELECT event_id, body FROM logs ORDER BY body DESC, event_id LIMIT 500",
    "SELECT event_id FROM logs ORDER BY attributes['n'], event_id LIMIT 300",
    "SELECT event_id FROM logs ORDER BY attributes['n'] DESC LIMIT 2000",
    "SELECT event_id, severity FROM logs ORDER BY severity, attributes['user'] LIMIT 50",
    "SELECT event_id FROM logs WHERE service = 'service-3' ORDER BY event_time",
    "SELECT event_id FROM logs ORDER BY event_time LIMIT 9000",
    "SELECT event_id FROM logs ORDER BY body LIMIT 7",
    "SELECT attributes['user'] AS u, count(*) AS c FROM logs GROUP BY attributes['user'] ORDER BY c DESC, u LIMIT 10",
    "SELECT service, count(*) AS c FROM logs GROUP BY service ORDER BY c, service",
];

#[test]
fn results_match_the_comparison_sort() {
    let snap = engine().1.snapshot(&tenant());
    for sql in QUERIES {
        let want = run(sql, false, 1);
        for parallelism in [1, 4] {
            for (normalized, bound) in [(true, true), (true, false), (false, true)] {
                let got = execute(&snap, sql, &variant(normalized, bound, parallelism)).unwrap();
                assert_eq!(
                    got.rows, want.rows,
                    "{sql} (parallelism {parallelism}, normalized {normalized}, bound {bound})"
                );
            }
        }
    }
}

#[test]
fn the_topk_bound_skips_rows_before_building_them() {
    let sql = "SELECT event_id, body FROM logs ORDER BY body DESC, event_id LIMIT 20";
    let fast = run(sql, true, 4);
    let slow = run(sql, false, 4);
    assert_eq!(fast.rows, slow.rows);
    assert_eq!(slow.stats.topk_rows_skipped, 0);
    // 10,500 matched rows; all but the first rows of each segment lose to
    // the bound.
    assert!(fast.stats.topk_rows_skipped > 8_000, "{:?}", fast.stats);
    // No bound without a LIMIT, or for aggregates.
    let all = run("SELECT event_id FROM logs ORDER BY body", true, 4);
    assert_eq!(all.stats.topk_rows_skipped, 0);
}

#[test]
fn segment_runs_are_merged_by_the_loser_tree() {
    let sql = "SELECT event_id FROM logs ORDER BY service, event_id LIMIT 100";
    let snap = engine().1.snapshot(&tenant());
    // Without the bound every segment yields a run (with it, a segment can
    // lose every row to the shared bound and yield none).
    let got = execute(&snap, sql, &variant(true, false, 4)).unwrap();
    assert_eq!(got.stats.sort_runs_merged, 6, "{:?}", got.stats);
    assert_eq!(run(sql, false, 4).stats.sort_runs_merged, 0);
    assert_eq!(got.rows, run(sql, false, 4).rows);
}

/// Pages walked with the normalized sort equal the comparison sort's pages
/// (row identities break ties, so the tie comparator is exercised).
#[test]
fn pagination_matches() {
    let sql = "SELECT event_id, service FROM logs ORDER BY service DESC";
    let walk = |normalized: bool| {
        let mut all = Vec::new();
        let mut cursor = None;
        loop {
            let page = execute_page(
                &engine().1.snapshot(&tenant()),
                sql,
                &opts(normalized, 4),
                &PageRequest {
                    page_size: 997,
                    cursor: cursor.take(),
                },
            )
            .unwrap();
            all.extend(page.output.rows);
            match page.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        all
    };
    let (a, b) = (walk(true), walk(false));
    assert_eq!(a.len(), 7 * 1500);
    assert_eq!(a, b);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    #[test]
    fn any_order_and_limit_matches(
        cols in proptest::collection::vec(0usize..6, 1..4),
        desc in proptest::collection::vec(any::<bool>(), 3),
        limit in proptest::option::of(1usize..6000),
    ) {
        let names = ["service", "body", "attributes['n']", "attributes['user']", "event_time", "severity"];
        let order: Vec<String> = cols
            .iter()
            .zip(&desc)
            .map(|(c, d)| format!("{}{}", names[*c], if *d { " DESC" } else { "" }))
            .collect();
        let sql = format!(
            "SELECT event_id FROM logs ORDER BY {}{}",
            order.join(", "),
            limit.map_or(String::new(), |l| format!(" LIMIT {l}"))
        );
        let want = run(&sql, false, 1);
        prop_assert_eq!(&run(&sql, true, 4).rows, &want.rows, "{}", sql);
    }
}

/// In-process A/B of the normalized sort (on vs off, interleaved, median of
/// rounds). Run with `cargo test --release -p stratadb-query --test sort
/// sort_ab -- --ignored --nocapture`.
#[test]
#[ignore = "timing measurement"]
fn sort_ab() {
    let d = tempfile::tempdir().unwrap();
    let mut o = EngineOptions::new(d.path());
    o.background = false;
    o.wal.durability = Durability::Async;
    let e = Engine::open(o).unwrap();
    let mut rng = 0x2545_f491_4f6c_dd1du64;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    for s in 0..8i64 {
        let evs: Vec<Event> = (0..60_000i64)
            .map(|j| {
                let r = next();
                let mut ev = Event::new(tenant(), BASE + ((r % 3_600_000) as i64) * 1_000_000, 1);
                ev.event_id = Some(format!("s{s}-{j}"));
                ev.service = Some(format!("service-{:02}", r % 40));
                ev.severity = [Severity::Info, Severity::Error, Severity::Warn][(r % 3) as usize];
                ev.body = Some(Value::String(format!(
                    "GET /api/v1/items/{} status {}",
                    r % 100_000,
                    200 + r % 5
                )));
                ev.attributes
                    .insert("user".into(), Value::String(format!("user-{}", r % 50_000)));
                ev.attributes
                    .insert("latency_ms".into(), Value::Int((r % 10_000) as i64));
                ev
            })
            .collect();
        e.write_batch(&tenant(), evs).unwrap();
        if s < 7 {
            e.flush().unwrap();
        }
    }
    let queries = [
        ("topk_2keys", "SELECT event_id FROM logs ORDER BY service, event_time DESC LIMIT 100"),
        ("topk_string", "SELECT event_id FROM logs ORDER BY body LIMIT 1000"),
        ("topk_int_10k", "SELECT event_id FROM logs ORDER BY attributes['latency_ms'] DESC LIMIT 10000"),
        ("full_sort_time", "SELECT event_id FROM logs WHERE service = 'service-07' ORDER BY event_time"),
        ("full_sort_int", "SELECT event_id FROM logs WHERE severity = 'ERROR' AND attributes['latency_ms'] < 600 ORDER BY attributes['latency_ms'], event_time"),
        ("group_topk", "SELECT attributes['user'] AS u, count(*) AS c FROM logs GROUP BY attributes['user'] ORDER BY c DESC, u LIMIT 20"),
    ];
    let snap = e.snapshot(&tenant());
    for (name, sql) in queries {
        // Arms: both off, normalized sort only, normalized sort + bound.
        let arms = [(false, false), (true, false), (true, true)];
        let mut t = [Vec::new(), Vec::new(), Vec::new()];
        let mut skipped = 0;
        for round in 0..9 {
            for k in 0..3 {
                let a = (round + k) % 3;
                let o = QueryOptions {
                    enable_ordered_scan: false,
                    ..variant(arms[a].0, arms[a].1, QueryOptions::default().parallelism)
                };
                let started = std::time::Instant::now();
                let out = execute(&snap, sql, &o).unwrap();
                t[a].push(started.elapsed().as_secs_f64() * 1e3);
                if a == 2 {
                    skipped = out.stats.topk_rows_skipped;
                }
                std::hint::black_box(out.rows.len());
            }
        }
        for v in &mut t {
            v.sort_by(f64::total_cmp);
        }
        let (off, norm, both) = (t[0][4], t[1][4], t[2][4]);
        println!(
            "{name:15} off {off:7.2} ms | sort {norm:7.2} ms ({:.2}x) | sort+bound {both:7.2} ms ({:.2}x), {skipped} rows skipped",
            off / norm,
            off / both
        );
    }
}
