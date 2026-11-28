//! Global aggregates answered from source metadata (spec STRATA-SYS-09-006,
//! 09-007): `count(*)` / `count(event_time)` / `min` / `max(event_time)`
//! skip reading a segment or memtable window when the filter provably covers
//! it, and the results are identical with the optimization off.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use proptest::prelude::*;
use stratadb_common::time::NANOS_PER_SEC;
use stratadb_common::TenantId;
use stratadb_query::{execute, QueryOptions, QueryOutput};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

const MIN: i64 = 60 * NANOS_PER_SEC;
const BASE: i64 = 1_795_170_600 * NANOS_PER_SEC;

fn tenant() -> TenantId {
    TenantId::new("a").unwrap()
}

/// 100 events, one per minute: four flushed segments of 20 (minutes 0-19,
/// 20-39, 40-59, 60-79) and 20 in the memtable (80-99). Segment 2 is all
/// INFO; the others mix ERROR, WARN, INFO and DEBUG.
fn fixture(dir: &std::path::Path) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    let e = Engine::open(o).unwrap();
    for chunk in 0..5i64 {
        let evs: Vec<Event> = (chunk * 20..chunk * 20 + 20)
            .map(|i| {
                let mut ev = Event::new(tenant(), BASE + i * MIN, 1);
                ev.severity = if chunk == 2 {
                    Severity::Info
                } else {
                    [
                        Severity::Error,
                        Severity::Warn,
                        Severity::Info,
                        Severity::Debug,
                    ][(i % 4) as usize]
                };
                ev
            })
            .collect();
        e.write_batch(&tenant(), evs).unwrap();
        if chunk < 4 {
            e.flush().unwrap();
        }
    }
    e
}

fn ts(min: i64) -> String {
    format!(
        "TIMESTAMP '{}'",
        stratadb_common::time::format_rfc3339(BASE + min * MIN)
    )
}

fn run(e: &Engine, sql: &str, meta: bool) -> QueryOutput {
    let o = QueryOptions {
        enable_metadata_aggregates: meta,
        ..QueryOptions::default()
    };
    execute(&e.snapshot(&tenant()), sql, &o).unwrap_or_else(|err| panic!("{sql}: {err}"))
}

fn fixture_engine() -> &'static Engine {
    static F: std::sync::OnceLock<(tempfile::TempDir, Engine)> = std::sync::OnceLock::new();
    &F.get_or_init(|| {
        let d = tempfile::tempdir().unwrap();
        let e = fixture(d.path());
        (d, e)
    })
    .1
}

#[test]
fn count_and_time_range_without_filter_read_no_columns() {
    let e = fixture_engine();
    let out = run(
        e,
        "SELECT count(*), min(event_time), max(event_time), count(event_time) FROM logs",
        true,
    );
    assert_eq!(
        out.rows,
        vec![vec![
            Some(Value::Int(100)),
            Some(Value::Timestamp(BASE)),
            Some(Value::Timestamp(BASE + 99 * MIN)),
            Some(Value::Int(100)),
        ]]
    );
    let s = &out.stats;
    // 4 segments + the memtable window(s): nothing read.
    assert!(s.sources_from_metadata >= 5, "{s:?}");
    assert_eq!(
        (s.rows_scanned, s.bytes_read, s.segments_scanned),
        (0, 0, 0),
        "{s:?}"
    );
    assert_eq!(s.rows_matched, 100);
    let off = run(
        e,
        "SELECT count(*), min(event_time), max(event_time), count(event_time) FROM logs",
        false,
    );
    assert_eq!(off.rows, out.rows);
    assert_eq!(off.stats.sources_from_metadata, 0);
    assert_eq!(off.stats.rows_scanned, 100);
}

#[test]
fn a_time_filter_answers_covered_sources_and_reads_the_rest() {
    let e = fixture_engine();
    // Minutes >= 30: segment 0 pruned, segment 1 (20-39) partly covered and
    // read, segments 2-3 and the memtable covered.
    let sql = format!("SELECT count(*) FROM logs WHERE event_time >= {}", ts(30));
    let out = run(e, &sql, true);
    assert_eq!(out.rows[0][0], Some(Value::Int(70)));
    assert_eq!(out.stats.segments_scanned, 1);
    assert_eq!(out.stats.rows_scanned, 20);
    assert!(out.stats.sources_from_metadata >= 3, "{:?}", out.stats);
    assert_eq!(run(e, &sql, false).rows, out.rows);
}

#[test]
fn severity_filters_use_exact_per_segment_counts() {
    let e = fixture_engine();
    let sql = "SELECT count(*) FROM logs WHERE severity IN ('ERROR', 'WARN')";
    let out = run(e, sql, true);
    assert_eq!(out.rows[0][0], Some(Value::Int(40))); // 10 + 10 + 0 + 10 + 10
                                                      // Segments know their severity counts; the memtable window is read.
    assert_eq!(out.stats.segments_scanned, 0);
    assert_eq!(out.stats.memory_rows_scanned, 20);
    assert_eq!(run(e, sql, false).rows, out.rows);
    // min/max need every row to match: only the all-INFO segment (and
    // segments without a match) can be answered.
    let sql = "SELECT min(event_time), max(event_time) FROM logs WHERE severity = 'INFO'";
    let out = run(e, sql, true);
    assert_eq!(out.rows, run(e, sql, false).rows);
    assert!(out.stats.segments_scanned == 3, "{:?}", out.stats);
    // A non-canonical spelling is not answered from metadata (evaluation
    // compares the name exactly), and the result is still right.
    let sql = "SELECT count(*) FROM logs WHERE severity = 'error'";
    let out = run(e, sql, true);
    assert_eq!(out.stats.sources_from_metadata, 0);
    assert_eq!(out.rows, run(e, sql, false).rows);
}

#[test]
fn other_shapes_are_not_answered_and_explain_reports_it() {
    let e = fixture_engine();
    for sql in [
        "SELECT count(*) FROM logs WHERE service = 'x'",
        "SELECT severity, count(*) FROM logs GROUP BY severity",
        "SELECT sum(severity_number) FROM logs",
        "SELECT count(body) FROM logs",
        "SELECT count(*) FROM logs WHERE event_time != TIMESTAMP '2026-01-01T00:00:00Z'",
    ] {
        let out = run(e, sql, true);
        assert_eq!(out.stats.sources_from_metadata, 0, "{sql}");
        assert_eq!(out.rows, run(e, sql, false).rows, "{sql}");
    }
    let out = run(e, "EXPLAIN SELECT count(*) FROM logs", true);
    let text = out.explain.unwrap();
    assert!(
        text.contains("answered from metadata (no columns read): 4 source(s)"),
        "{text}"
    );
    assert!(text.contains("0 to scan"), "{text}");
}

fn agg_list() -> impl Strategy<Value = Vec<&'static str>> {
    proptest::sample::subsequence(
        vec![
            "count(*)",
            "count(event_time)",
            "min(event_time)",
            "max(event_time)",
        ],
        1..=4,
    )
    .prop_shuffle()
}

fn time_pred() -> impl Strategy<Value = Option<String>> {
    proptest::option::of(
        (
            prop::sample::select(vec![">=", ">", "<=", "<", "="]),
            -5i64..110,
        )
            .prop_map(|(op, m)| format!("event_time {op} {}", ts(m))),
    )
}

fn sev_pred() -> impl Strategy<Value = Option<String>> {
    let names = vec!["ERROR", "WARN", "INFO", "DEBUG", "FATAL", "info"];
    proptest::option::of(prop_oneof![
        prop::sample::select(names.clone()).prop_map(|n| format!("severity = '{n}'")),
        proptest::sample::subsequence(names, 1..=3).prop_map(|v| format!(
            "severity IN ({})",
            v.iter()
                .map(|n| format!("'{n}'"))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    ])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    /// Metadata answers never change a result.
    #[test]
    fn metadata_answers_equal_reading_every_row(
        aggs in agg_list(),
        t1 in time_pred(),
        t2 in time_pred(),
        sev in sev_pred(),
    ) {
        let e = fixture_engine();
        let preds: Vec<String> = [t1, t2, sev].into_iter().flatten().collect();
        let mut sql = format!("SELECT {} FROM logs", aggs.join(", "));
        if !preds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&preds.join(" AND "));
        }
        let on = run(e, &sql, true);
        let off = run(e, &sql, false);
        prop_assert_eq!(&on.rows, &off.rows, "{}", sql);
        prop_assert_eq!(on.stats.rows_matched, off.stats.rows_matched, "{}", sql);
        prop_assert!(on.stats.rows_scanned <= off.stats.rows_scanned);
    }
}
