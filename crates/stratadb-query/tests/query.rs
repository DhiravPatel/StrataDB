//! Query engine integration tests (Phase 2 exit criteria).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use proptest::prelude::*;
use stratadb_common::time::NANOS_PER_SEC;
use stratadb_common::{ErrorKind, TenantId};
use stratadb_query::{execute, Datum, QueryOptions, QueryOutput};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

const MIN: i64 = 60 * NANOS_PER_SEC;
const BASE: i64 = 1_795_170_600 * NANOS_PER_SEC; // 2026-11-20T10:30:00Z

fn tenant(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn engine(dir: &std::path::Path) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    Engine::open(o).unwrap()
}

fn mk(t: &TenantId, i: i64) -> Event {
    let mut e = Event::new(t.clone(), BASE + i * MIN, BASE + i * MIN);
    e.event_id = Some(format!("e{i}"));
    e.service = Some(["payment", "auth", "search"][(i % 3) as usize].to_owned());
    e.severity = match i % 10 {
        0 => Severity::Error,
        1 => Severity::Warn,
        _ => Severity::Info,
    };
    e.body = Some(Value::String(if i % 10 == 0 {
        format!("Payment authorization timed out #{i}")
    } else {
        format!("ok {i}")
    }));
    e.trace_id = (i % 2 == 0).then(|| format!("{i:032x}"));
    e.attributes.insert(
        "http.status_code".into(),
        Value::Int(if i % 10 == 0 { 504 } else { 200 }),
    );
    if i % 4 == 0 {
        e.attributes
            .insert("region".into(), Value::from("ap-south-1"));
    }
    if i % 5 == 0 {
        e.attributes.insert("maybe".into(), Value::Null);
    }
    if i >= 80 {
        e.attributes.insert("late_key".into(), Value::Int(i));
    }
    if i % 3 != 2 {
        e.attributes
            .insert("request_id".into(), Value::String(format!("req_{i}")));
    }
    e
}

/// 100 events for tenant a over 100 minutes: four flushed segments of 20 and
/// 20 in the memtable; plus 10 events for tenant b.
fn fixture(dir: &std::path::Path) -> Engine {
    let e = engine(dir);
    let (a, b) = (tenant("a"), tenant("b"));
    for chunk in 0..5 {
        let evs: Vec<Event> = (chunk * 20..chunk * 20 + 20).map(|i| mk(&a, i)).collect();
        e.write_batch(&a, evs).unwrap();
        if chunk < 4 {
            e.flush().unwrap();
        }
    }
    e.write_batch(&b, (0..10).map(|i| mk(&b, i)).collect())
        .unwrap();
    e
}

fn q(e: &Engine, t: &str, sql: &str) -> QueryOutput {
    execute(&e.snapshot(&tenant(t)), sql, &QueryOptions::default())
        .unwrap_or_else(|err| panic!("{sql}: {err}"))
}

fn ids(out: &QueryOutput) -> Vec<String> {
    out.rows
        .iter()
        .map(|r| match &r[0] {
            Some(Value::String(s)) => s.clone(),
            other => panic!("not a string id: {other:?}"),
        })
        .collect()
}

fn int(d: &Datum) -> i64 {
    match d {
        Some(Value::Int(i)) => *i,
        other => panic!("not an int: {other:?}"),
    }
}

fn ts(min: i64) -> String {
    format!(
        "TIMESTAMP '{}'",
        stratadb_common::time::format_rfc3339(BASE + min * MIN)
    )
}

#[test]
fn recent_error_search_like_spec_example() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let sql = format!(
        "SELECT event_id, event_time, service, severity, body, trace_id FROM logs \
         WHERE event_time >= {} AND event_time < {} AND severity = 'ERROR' \
         ORDER BY event_time DESC LIMIT 3",
        ts(25),
        ts(95)
    );
    let out = q(&e, "a", &sql);
    assert_eq!(ids(&out), vec!["e90", "e80", "e70"]);
    assert_eq!(out.columns[0], "event_id");
    // 4 segments: [0,19] pruned by time; [20,39],[40,59],[60,79] scanned.
    assert_eq!(out.stats.segments_total, 4);
    assert_eq!(out.stats.segments_pruned_time, 1);
    assert_eq!(out.stats.segments_scanned, 3);
}

#[test]
fn aggregates_group_by_and_time_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let out = q(&e, "a", "SELECT service, count(*) AS n FROM logs WHERE severity = 'ERROR' GROUP BY service ORDER BY service");
    let got: Vec<(String, i64)> = out
        .rows
        .iter()
        .map(|r| {
            (
                r[0].as_ref().unwrap().as_str().unwrap().to_owned(),
                int(&r[1]),
            )
        })
        .collect();
    // errors are i % 10 == 0 → i in {0,10,...,90}; service = i % 3
    assert_eq!(
        got,
        vec![
            ("auth".into(), 3),
            ("payment".into(), 4),
            ("search".into(), 3)
        ]
    );

    let out = q(&e, "a", "SELECT count(*), min(attributes['http.status_code']), max(attributes['http.status_code']), \
                          sum(attributes['http.status_code']), avg(attributes['http.status_code']) FROM logs");
    let r = &out.rows[0];
    assert_eq!(int(&r[0]), 100);
    assert_eq!(int(&r[1]), 200);
    assert_eq!(int(&r[2]), 504);
    assert_eq!(int(&r[3]), 90 * 200 + 10 * 504);
    assert_eq!(
        r[4],
        Some(Value::Float((90.0 * 200.0 + 10.0 * 504.0) / 100.0))
    );

    let out = q(
        &e,
        "a",
        "SELECT time_bucket(INTERVAL '30 minutes', event_time) AS b, count(*) AS n FROM logs \
                          GROUP BY time_bucket(INTERVAL '30 minutes', event_time) ORDER BY b",
    );
    let counts: Vec<i64> = out.rows.iter().map(|r| int(&r[1])).collect();
    assert_eq!(counts.iter().sum::<i64>(), 100);
    assert_eq!(counts.len(), 4);

    // Aggregates over an empty input.
    let out = q(
        &e,
        "a",
        "SELECT count(*), sum(attributes['http.status_code']) FROM logs WHERE service = 'none'",
    );
    assert_eq!(int(&out.rows[0][0]), 0);
    assert_eq!(out.rows[0][1], Some(Value::Null));
}

#[test]
fn null_missing_and_empty_are_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let c = |sql: &str| int(&q(&e, "a", sql).rows[0][0]);
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE attributes['maybe'] IS NULL"),
        20
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE attributes['maybe'] IS MISSING"),
        80
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE attributes['maybe'] IS NOT MISSING"),
        20
    );
    assert_eq!(c("SELECT count(*) FROM logs WHERE trace_id IS MISSING"), 50);
    // Comparisons with missing are unknown: neither = nor != matches.
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE attributes['region'] = 'ap-south-1'"),
        25
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE attributes['region'] != 'ap-south-1'"),
        0
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE NOT attributes['region'] = 'ap-south-1'"),
        0
    );
    // count(expr) skips missing and null.
    assert_eq!(c("SELECT count(attributes['maybe']) FROM logs"), 0);
    assert_eq!(c("SELECT count(trace_id) FROM logs"), 50);
}

#[test]
fn text_matching_and_in_lists() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let c = |sql: &str| int(&q(&e, "a", sql).rows[0][0]);
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE body LIKE '%timed out%'"),
        10
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE body ILIKE 'PAYMENT%'"),
        10
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE body NOT LIKE '%timed out%'"),
        90
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE service IN ('auth', 'search')"),
        66
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE severity IN ('ERROR', 'WARN')"),
        20
    );
    assert_eq!(
        c("SELECT count(*) FROM logs WHERE severity_number >= 13"),
        20
    );
    let out = q(
        &e,
        "a",
        &format!("SELECT event_id FROM logs WHERE trace_id = '{:032x}'", 42),
    );
    assert_eq!(ids(&out), vec!["e42"]);
}

#[test]
fn select_star_order_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let out = q(
        &e,
        "a",
        "SELECT * FROM logs ORDER BY event_time DESC LIMIT 2",
    );
    assert_eq!(out.columns.len(), 11);
    assert_eq!(out.rows.len(), 2);
    let out = q(&e, "a", "SELECT event_id FROM logs ORDER BY attributes['http.status_code'] DESC, event_time LIMIT 2");
    assert_eq!(ids(&out), vec!["e0", "e10"]);
    // LIMIT without ORDER BY stops early.
    let out = q(&e, "a", "SELECT event_id FROM logs LIMIT 5");
    assert_eq!(out.rows.len(), 5);
    assert!(out.stats.early_stop);
    assert!(out.stats.rows_scanned < 100);
    assert!(q(&e, "a", "SELECT event_id FROM logs LIMIT 0")
        .rows
        .is_empty());
}

#[test]
fn pruning_by_attribute_keys_and_severity() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let out = q(
        &e,
        "a",
        "SELECT count(*) FROM logs WHERE attributes['late_key'] > 0",
    );
    assert_eq!(int(&out.rows[0][0]), 20); // only memtable rows i >= 80
    assert_eq!(out.stats.segments_pruned_keys, 4);
    assert_eq!(out.stats.bytes_read, 0);
    let out = q(
        &e,
        "a",
        "SELECT count(*) FROM logs WHERE severity = 'FATAL'",
    );
    assert_eq!(int(&out.rows[0][0]), 0);
    assert_eq!(out.stats.segments_pruned_severity, 4);
}

#[test]
fn projection_reads_only_needed_columns() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let narrow = q(&e, "a", "SELECT count(*) FROM logs WHERE service = 'auth'");
    let wide = q(&e, "a", "SELECT * FROM logs WHERE service = 'auth'");
    assert!(
        narrow.stats.bytes_read * 5 < wide.stats.bytes_read,
        "{} vs {}",
        narrow.stats.bytes_read,
        wide.stats.bytes_read
    );
}

#[test]
fn explain_and_explain_analyze() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let out = q(
        &e,
        "a",
        &format!(
            "EXPLAIN SELECT body FROM logs WHERE event_time >= {} AND severity = 'ERROR'",
            ts(60)
        ),
    );
    let text = out.explain.unwrap();
    assert!(text.contains("pruned 3 by time"), "{text}");
    assert!(
        text.contains("columns read: [event_time, severity, body]"),
        "{text}"
    );
    assert!(text.contains("severity in: [\"ERROR\"]"), "{text}");
    assert!(out.rows.is_empty());
    assert_eq!(out.stats.rows_scanned, 0, "EXPLAIN must not execute");
    let out = q(&e, "a", "EXPLAIN ANALYZE SELECT count(*) FROM logs");
    let text = out.explain.unwrap();
    assert!(text.contains("Actual: rows scanned 100"), "{text}");
}

#[test]
fn tenant_isolation_in_queries() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    assert_eq!(int(&q(&e, "b", "SELECT count(*) FROM logs").rows[0][0]), 10);
    assert_eq!(
        int(&q(&e, "a", "SELECT count(*) FROM logs").rows[0][0]),
        100
    );
    // A predicate naming another tenant cannot widen scope; it just matches nothing.
    assert_eq!(
        int(&q(&e, "a", "SELECT count(*) FROM logs WHERE tenant_id = 'b'").rows[0][0]),
        0
    );
    assert_eq!(
        int(&q(
            &e,
            "a",
            "SELECT count(*) FROM logs WHERE tenant_id = 'a' OR tenant_id = 'b'"
        )
        .rows[0][0]),
        100
    );
    assert_eq!(
        int(&q(&e, "nobody", "SELECT count(*) FROM logs").rows[0][0]),
        0
    );
}

#[test]
fn budgets_and_cancellation_are_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    let snap = e.snapshot(&tenant("a"));
    let run = |o: QueryOptions, sql: &str| execute(&snap, sql, &o).unwrap_err().kind();
    let o = QueryOptions {
        timeout: Duration::ZERO,
        ..Default::default()
    };
    assert_eq!(run(o, "SELECT * FROM logs"), ErrorKind::Timeout);
    let o = QueryOptions {
        cancel: Some(Arc::new(AtomicBool::new(true))),
        ..Default::default()
    };
    assert_eq!(run(o, "SELECT * FROM logs"), ErrorKind::Cancelled);
    let o = QueryOptions {
        max_rows_scanned: 10,
        ..Default::default()
    };
    assert_eq!(
        run(o, "SELECT count(*) FROM logs"),
        ErrorKind::ResourceExhausted
    );
    let o = QueryOptions {
        max_result_rows: 10,
        ..Default::default()
    };
    assert_eq!(
        run(o.clone(), "SELECT event_id FROM logs"),
        ErrorKind::ResourceExhausted
    );
    assert_eq!(
        execute(&snap, "SELECT event_id FROM logs LIMIT 10", &o)
            .unwrap()
            .rows
            .len(),
        10
    );
    let o = QueryOptions {
        max_memory_bytes: 1000,
        ..Default::default()
    };
    assert_eq!(run(o, "SELECT * FROM logs"), ErrorKind::ResourceExhausted);
    // Type errors in aggregates are explicit.
    assert_eq!(
        run(QueryOptions::default(), "SELECT sum(service) FROM logs"),
        ErrorKind::InvalidInput
    );
}

#[test]
fn top_k_matches_full_sort() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let t = tenant("a");
    // Enough rows to trigger top-k compaction (> 1024).
    for c in 0..3 {
        e.write_batch(
            &t,
            (c * 1000..c * 1000 + 1000)
                .map(|i| mk(&t, (i * 7919) % 3000))
                .collect(),
        )
        .unwrap();
        e.flush().unwrap();
    }
    let all = q(
        &e,
        "a",
        "SELECT event_id FROM logs ORDER BY event_time DESC LIMIT 3000",
    );
    let top = q(
        &e,
        "a",
        "SELECT event_id FROM logs ORDER BY event_time DESC LIMIT 7",
    );
    assert_eq!(ids(&top), ids(&all)[..7].to_vec());
}

// ---------------------------------------------------------------------------
// Property test: optimized == unoptimized == independent reference evaluator.

#[derive(Debug, Clone)]
enum P {
    TimeGe(i64),
    TimeLt(i64),
    Sev(&'static str),
    Svc(&'static str),
    AttrEq(i64),
    RegionMissing,
    TraceEq(i64),
    ReqIn(Vec<i64>),
    BodyLike(&'static str, bool),
    And(Box<P>, Box<P>),
    Or(Box<P>, Box<P>),
    Not(Box<P>),
}

fn arb_p() -> impl Strategy<Value = P> {
    let leaf = prop_oneof![
        (0i64..100).prop_map(P::TimeGe),
        (0i64..100).prop_map(P::TimeLt),
        prop::sample::select(vec!["ERROR", "WARN", "INFO", "FATAL"]).prop_map(P::Sev),
        prop::sample::select(vec!["payment", "auth", "search", "x"]).prop_map(P::Svc),
        prop::sample::select(vec![200i64, 504, 7]).prop_map(P::AttrEq),
        Just(P::RegionMissing),
        (0i64..110).prop_map(P::TraceEq),
        proptest::collection::vec(0i64..110, 1..4).prop_map(P::ReqIn),
        (
            prop::sample::select(vec![
                "%timed out%",
                "%TIMED OUT%",
                "%out #40",
                "%out #4_",
                "ok 1%",
                "%k 3%",
                "%deadlock%",
                "Payment%",
                "%#9%",
                "%_%",
                "%",
            ]),
            any::<bool>()
        )
            .prop_map(|(p, ci)| P::BodyLike(p, ci)),
    ];
    leaf.prop_recursive(3, 12, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| P::And(Box::new(a), Box::new(b))),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| P::Or(Box::new(a), Box::new(b))),
            inner.prop_map(|a| P::Not(Box::new(a))),
        ]
    })
}

fn sql(p: &P) -> String {
    match p {
        P::TimeGe(m) => format!("event_time >= {}", ts(*m)),
        P::TimeLt(m) => format!("event_time < {}", ts(*m)),
        P::Sev(s) => format!("severity = '{s}'"),
        P::Svc(s) => format!("service = '{s}'"),
        P::AttrEq(v) => format!("attributes['http.status_code'] = {v}"),
        P::RegionMissing => "attributes['region'] IS MISSING".into(),
        P::TraceEq(i) => format!("trace_id = '{i:032x}'"),
        P::ReqIn(v) => format!(
            "attributes['request_id'] IN ({})",
            v.iter()
                .map(|i| format!("'req_{i}'"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        P::BodyLike(pat, ci) => format!("body {} '{pat}'", if *ci { "ILIKE" } else { "LIKE" }),
        P::And(a, b) => format!("({} AND {})", sql(a), sql(b)),
        P::Or(a, b) => format!("({} OR {})", sql(a), sql(b)),
        P::Not(a) => format!("NOT ({})", sql(a)),
    }
}

/// Reference LIKE by dynamic programming (`%`, `_`; no escapes in the
/// generated patterns); ILIKE folds each character to lowercase.
fn like_ref(text: &str, pat: &str, ci: bool) -> bool {
    let f = |c: char| {
        if ci {
            c.to_lowercase().next().unwrap_or(c)
        } else {
            c
        }
    };
    let t: Vec<char> = text.chars().map(f).collect();
    let p: Vec<char> = pat.chars().map(f).collect();
    let mut dp = vec![vec![false; t.len() + 1]; p.len() + 1];
    dp[0][0] = true;
    for i in 1..=p.len() {
        for j in 0..=t.len() {
            dp[i][j] = match p[i - 1] {
                '%' => dp[i - 1][j] || (j > 0 && dp[i][j - 1]),
                '_' => j > 0 && dp[i - 1][j - 1],
                c => j > 0 && t[j - 1] == c && dp[i - 1][j - 1],
            };
        }
    }
    dp[p.len()][t.len()]
}

/// Independent three-valued reference evaluation directly over events.
fn reference(p: &P, e: &Event) -> Option<bool> {
    match p {
        P::TimeGe(m) => Some(e.event_time >= BASE + m * MIN),
        P::TimeLt(m) => Some(e.event_time < BASE + m * MIN),
        P::Sev(s) => Some(e.severity.as_str() == *s),
        P::Svc(s) => e.service.as_ref().map(|x| x == s),
        P::AttrEq(v) => match e.attributes.get("http.status_code") {
            Some(Value::Int(x)) => Some(x == v),
            _ => None,
        },
        P::RegionMissing => Some(!e.attributes.contains_key("region")),
        P::TraceEq(i) => e.trace_id.as_ref().map(|t| *t == format!("{i:032x}")),
        P::ReqIn(v) => match e.attributes.get("request_id") {
            Some(Value::String(s)) => Some(v.iter().any(|i| *s == format!("req_{i}"))),
            _ => None,
        },
        P::BodyLike(pat, ci) => match &e.body {
            Some(Value::String(s)) => Some(like_ref(s, pat, *ci)),
            _ => None,
        },
        P::And(a, b) => match (reference(a, e), reference(b, e)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        P::Or(a, b) => match (reference(a, e), reference(b, e)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        P::Not(a) => reference(a, e).map(|x| !x),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn optimized_equals_unoptimized_equals_reference(p in arb_p()) {
        static FIXTURE: std::sync::OnceLock<(tempfile::TempDir, Engine)> = std::sync::OnceLock::new();
        let (_, e) = FIXTURE.get_or_init(|| {
            let d = tempfile::tempdir().unwrap();
            let e = fixture(d.path());
            (d, e)
        });
        let snap = e.snapshot(&tenant("a"));
        let text = format!("SELECT event_id FROM logs WHERE {}", sql(&p));
        let on = execute(&snap, &text, &QueryOptions::default()).unwrap();
        let off = execute(&snap, &text, &QueryOptions { enable_pruning: false, ..Default::default() }).unwrap();
        let scalar = execute(&snap, &text, &QueryOptions { enable_vectorized: false, ..Default::default() }).unwrap();
        let set = |o: &QueryOutput| ids(o).into_iter().collect::<BTreeSet<_>>();
        let want: BTreeSet<String> = (0..100)
            .map(|i| mk(&tenant("a"), i))
            .filter(|ev| reference(&p, ev) == Some(true))
            .map(|ev| ev.event_id.unwrap())
            .collect();
        prop_assert_eq!(set(&on), set(&off), "pruning changed results for {}", text);
        prop_assert_eq!(ids(&on), ids(&scalar), "vectorization changed results for {}", text);
        prop_assert_eq!(on.stats.rows_matched, scalar.stats.rows_matched);
        prop_assert_eq!(set(&on), want, "engine disagrees with reference for {}", text);
        prop_assert!(on.stats.rows_scanned <= off.stats.rows_scanned);
    }
}

#[test]
fn bloom_filters_prune_exact_id_lookups_without_false_negatives() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    // trace ids exist for even i; segments hold i in [0,20), [20,40), ...
    let out = q(
        &e,
        "a",
        &format!("SELECT event_id FROM logs WHERE trace_id = '{:032x}'", 42),
    );
    assert_eq!(ids(&out), vec!["e42"]);
    assert_eq!(out.stats.segments_pruned_bloom, 3, "{:?}", out.stats);
    assert_eq!(out.stats.indexes_used, vec!["bloom(trace_id)"]);
    // Every existing request id is found (no false negatives).
    for i in (0..100).filter(|i| i % 3 != 2) {
        let out = q(
            &e,
            "a",
            &format!("SELECT count(*) FROM logs WHERE attributes['request_id'] = 'req_{i}'"),
        );
        assert_eq!(int(&out.rows[0][0]), 1, "req_{i} lost");
    }
    // An absent id prunes (nearly) every segment.
    let out = q(
        &e,
        "a",
        "SELECT count(*) FROM logs WHERE attributes['request_id'] IN ('nope', 'never')",
    );
    assert_eq!(int(&out.rows[0][0]), 0);
    assert!(out.stats.segments_pruned_bloom >= 3);
    // Numeric literals never consult string filters (int/float equality semantics).
    let out = q(
        &e,
        "a",
        "SELECT count(*) FROM logs WHERE attributes['http.status_code'] = 504",
    );
    assert_eq!(int(&out.rows[0][0]), 10);
    assert_eq!(out.stats.segments_pruned_bloom, 0);
    let text = q(
        &e,
        "a",
        &format!(
            "EXPLAIN ANALYZE SELECT body FROM logs WHERE trace_id = '{:032x}'",
            42
        ),
    )
    .explain
    .unwrap();
    assert!(text.contains("indexes used: bloom(trace_id)"), "{text}");
}

#[test]
fn shredded_attribute_columns_give_identical_results_with_less_io() {
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let shredded = fixture(d1.path());
    let plain = {
        let mut o = EngineOptions::new(d2.path());
        o.background = false;
        o.wal.durability = Durability::Async;
        o.segment_shredding = false;
        let e = Engine::open(o).unwrap();
        let (a, b) = (tenant("a"), tenant("b"));
        for chunk in 0..5 {
            e.write_batch(
                &a,
                (chunk * 20..chunk * 20 + 20).map(|i| mk(&a, i)).collect(),
            )
            .unwrap();
            if chunk < 4 {
                e.flush().unwrap();
            }
        }
        e.write_batch(&b, (0..10).map(|i| mk(&b, i)).collect())
            .unwrap();
        e
    };
    let queries = [
        "SELECT count(*) AS n FROM logs WHERE attributes['http.status_code'] = 504",
        "SELECT attributes['region'] AS r, count(*) AS n FROM logs GROUP BY attributes['region'] ORDER BY r",
        "SELECT event_id FROM logs WHERE attributes['maybe'] IS NULL ORDER BY event_id",
        "SELECT event_id FROM logs WHERE attributes['maybe'] IS MISSING AND attributes['region'] IS NOT MISSING ORDER BY event_id",
        "SELECT event_id, attributes['request_id'] AS r FROM logs ORDER BY attributes['http.status_code'] DESC, event_id LIMIT 7",
        // Mixed: late_key is not shredded in the flushed segments → full-map fallback.
        "SELECT count(*) FROM logs WHERE attributes['late_key'] > 0 OR attributes['region'] = 'ap-south-1'",
        "SELECT event_id, attributes FROM logs WHERE event_id = 'e42'",
    ];
    let mut io = (0u64, 0u64);
    for sql in queries {
        let a = q(&shredded, "a", sql);
        let b = q(&plain, "a", sql);
        assert_eq!(a.rows, b.rows, "{sql}");
        io.0 += a.stats.bytes_read;
        io.1 += b.stats.bytes_read;
    }
    let one = q(&shredded, "a", queries[0]);
    assert!(
        one.stats.shredded_reads > 0,
        "fast path not used: {:?}",
        one.stats
    );
    let plain_one = q(&plain, "a", queries[0]);
    assert!(
        one.stats.bytes_read * 3 < plain_one.stats.bytes_read,
        "{} vs {}",
        one.stats.bytes_read,
        plain_one.stats.bytes_read
    );
    assert!(io.0 < io.1, "{io:?}");
}

#[test]
fn ngram_filters_prune_like_scans_without_changing_results() {
    let dir = tempfile::tempdir().unwrap();
    let e = fixture(dir.path());
    // "out #40" occurs only in the segment holding events 40..60.
    let sql = "SELECT event_id FROM logs WHERE body LIKE '%out #40%'";
    let out = q(&e, "a", sql);
    assert_eq!(ids(&out), vec!["e40"]);
    assert_eq!(out.stats.segments_pruned_ngram, 3, "{:?}", out.stats);
    assert!(out.stats.indexes_used.contains(&"ngram(body)".to_owned()));
    // Case-insensitive patterns use the same (folded) index.
    let out = q(
        &e,
        "a",
        "SELECT event_id FROM logs WHERE body ILIKE '%OUT #40%'",
    );
    assert_eq!(ids(&out), vec!["e40"]);
    assert_eq!(out.stats.segments_pruned_ngram, 3);
    // A substring present in every segment prunes nothing; results unchanged.
    let out = q(
        &e,
        "a",
        "SELECT count(*) FROM logs WHERE body LIKE '%timed out%'",
    );
    assert_eq!(int(&out.rows[0][0]), 10);
    assert_eq!(out.stats.segments_pruned_ngram, 0);
    // Absent substring: every segment pruned, memtable still scanned.
    let out = q(
        &e,
        "a",
        "SELECT count(*) FROM logs WHERE body LIKE '%deadlock%'",
    );
    assert_eq!(int(&out.rows[0][0]), 0);
    assert_eq!(out.stats.segments_pruned_ngram, 4);
    // Short literals (< 3 chars) and NOT LIKE never prune.
    let out = q(&e, "a", "SELECT count(*) FROM logs WHERE body LIKE '%#4%'");
    assert_eq!(out.stats.segments_pruned_ngram, 0);
    let out = q(
        &e,
        "a",
        "SELECT count(*) FROM logs WHERE body NOT LIKE '%deadlock%'",
    );
    assert_eq!(int(&out.rows[0][0]), 100);
    assert_eq!(out.stats.segments_pruned_ngram, 0);
    let ex = q(
        &e,
        "a",
        "EXPLAIN SELECT * FROM logs WHERE body LIKE '%deadlock%'",
    );
    let text = ex.explain.unwrap();
    assert!(text.contains("ngram(body) (6 trigram(s))"), "{text}");
}
