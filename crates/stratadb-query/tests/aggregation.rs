//! Aggregation on dictionary codes and the group hash table (spec
//! STRATA-SYS-09-002, 09-008, 09-011): dense code aggregation gives exactly
//! the rows, and the row order, of hashing every row; results match an
//! independent reference; EXPLAIN ANALYZE reports the table.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeMap;

use proptest::prelude::*;
use stratadb_common::TenantId;
use stratadb_query::{execute, QueryOptions, QueryOutput};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("g").unwrap()
}

const SERVICES: [&str; 5] = ["api", "auth", "db", "web", "queue"];
const SEVS: [Severity; 5] = [
    Severity::Error,
    Severity::Warn,
    Severity::Info,
    Severity::Debug,
    Severity::Fatal,
];

fn event(i: i64) -> Event {
    let mut e = Event::new(tenant(), 1_795_170_600_000_000_000 + i * 1_000_000, 1);
    // Skewed, deterministic mixes; some rows have no service (missing key).
    let h = (i * 2_654_435_761) as u64 >> 7;
    e.service = (!h.is_multiple_of(11)).then(|| SERVICES[(h % 5) as usize].to_owned());
    e.severity = SEVS[((h >> 5) % 5) as usize];
    e
}

/// 6 flushed segments (dictionary-encoded strings) + memtable rows (plain).
fn fixture() -> &'static (tempfile::TempDir, Engine, Vec<Event>) {
    static F: std::sync::OnceLock<(tempfile::TempDir, Engine, Vec<Event>)> =
        std::sync::OnceLock::new();
    F.get_or_init(|| {
        let d = tempfile::tempdir().unwrap();
        let mut o = EngineOptions::new(d.path());
        o.background = false;
        o.wal.durability = Durability::Async;
        let e = Engine::open(o).unwrap();
        let mut all = Vec::new();
        for s in 0..7i64 {
            let evs: Vec<Event> = (s * 3000..s * 3000 + 3000).map(event).collect();
            all.extend(evs.iter().cloned());
            e.write_batch(&tenant(), evs).unwrap();
            if s < 6 {
                e.flush().unwrap();
            }
        }
        (d, e, all)
    })
}

fn run(sql: &str, dense: bool) -> QueryOutput {
    let (_, e, _) = fixture();
    let o = QueryOptions {
        enable_dense_groups: dense,
        ..QueryOptions::default()
    };
    execute(&e.snapshot(&tenant()), sql, &o).unwrap_or_else(|err| panic!("{sql}: {err}"))
}

#[test]
fn group_by_severity_and_service_match_a_reference() {
    let (_, _, all) = fixture();
    let mut want: BTreeMap<(Option<String>, &'static str), (i64, i64)> = BTreeMap::new();
    for e in all {
        let k = (e.service.clone(), e.severity.as_str());
        let w = want.entry(k).or_insert((0, i64::MAX));
        w.0 += 1;
        w.1 = w.1.min(e.event_time);
    }
    let out = run(
        "SELECT service, severity, count(*), min(event_time) FROM logs GROUP BY service, severity",
        true,
    );
    let mut got = BTreeMap::new();
    for r in &out.rows {
        let svc = match &r[0] {
            Some(Value::String(s)) => Some(s.clone()),
            None => None,
            x => panic!("{x:?}"),
        };
        let sev = match &r[1] {
            Some(Value::String(s)) => SEVS.iter().find(|v| v.as_str() == s).unwrap().as_str(),
            x => panic!("{x:?}"),
        };
        let (n, t) = match (&r[2], &r[3]) {
            (Some(Value::Int(n)), Some(Value::Timestamp(t))) => (*n, *t),
            x => panic!("{x:?}"),
        };
        got.insert((svc, sev), (n, t));
    }
    assert_eq!(got, want);
    assert!(out.stats.dense_group_batches >= 6, "{:?}", out.stats);
}

#[test]
fn explain_analyze_reports_the_hash_table() {
    let out = run(
        "EXPLAIN ANALYZE SELECT severity, count(*) FROM logs GROUP BY severity",
        true,
    );
    let text = out.explain.unwrap();
    assert!(
        text.contains("Hash table: dense codes over serialized-key SwissTable, 5 group(s)"),
        "{text}"
    );
    let out = run(
        "EXPLAIN ANALYZE SELECT event_time, count(*) FROM logs GROUP BY event_time",
        true,
    );
    let text = out.explain.unwrap();
    assert!(
        text.contains("Hash table: serialized-key SwissTable, 21000 group(s)"),
        "{text}"
    );
    let h = out.stats.group_table.unwrap();
    assert!(
        h.resizes >= 5 && h.capacity >= 21_000 && h.bytes > 21_000 * 8,
        "{h:?}"
    );
    assert!(run("SELECT service FROM logs LIMIT 1", true)
        .stats
        .group_table
        .is_none());
}

#[test]
fn group_growth_is_reserved_against_the_query_memory_limit() {
    // 21,000 groups cannot fit in 256 KiB: the reservation before growing
    // the table refuses the query instead of allocating past the limit.
    let (_, e, _) = fixture();
    let o = QueryOptions {
        max_memory_bytes: 256 << 10,
        ..QueryOptions::default()
    };
    let err = execute(
        &e.snapshot(&tenant()),
        "SELECT event_time, count(*) FROM logs GROUP BY event_time",
        &o,
    )
    .unwrap_err();
    assert!(err.to_string().contains("MEMORY_LIMIT_EXCEEDED"), "{err}");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    /// Dense code aggregation never changes rows or their order.
    #[test]
    fn dense_equals_hashed(
        keys in proptest::sample::subsequence(vec!["service", "severity", "severity_text"], 1..=3).prop_shuffle(),
        aggs in proptest::sample::subsequence(
            vec!["count(*)", "count(service)", "min(event_time)", "max(event_time)", "sum(severity_number)", "avg(severity_number)"], 1..=3),
        filter in proptest::option::of(prop::sample::select(vec![
            "severity IN ('ERROR', 'WARN')",
            "service = 'auth'",
            "service IS NULL",
            "event_time > TIMESTAMP '2026-11-20T10:30:05Z'",
        ])),
        order in any::<bool>(),
    ) {
        let mut sql = format!("SELECT {}, {} FROM logs", keys.join(", "), aggs.join(", "));
        if let Some(f) = filter {
            sql.push_str(" WHERE ");
            sql.push_str(f);
        }
        sql.push_str(&format!(" GROUP BY {}", keys.join(", ")));
        if order {
            // Column 2 is a key or the first aggregate; ties keep first-seen
            // order, which must also match.
            sql.push_str(" ORDER BY 2 DESC");
        }
        let a = run(&sql, true);
        let b = run(&sql, false);
        prop_assert_eq!(&a.rows, &b.rows, "{}", sql);
        prop_assert_eq!(b.stats.dense_group_batches, 0);
    }
}
