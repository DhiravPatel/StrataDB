//! `ORDER BY event_time ... LIMIT k` reads sources best-first and stops
//! early (spec STRATA-SYS-14-003/004), with results identical to the
//! default scan, ties included.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use proptest::prelude::*;
use stratadb_common::TenantId;
use stratadb_query::{execute, QueryOptions, QueryOutput};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("o").unwrap()
}

const BASE: i64 = 1_795_170_600_000_000_000;

/// `disjoint`: each flushed segment covers its own time span (ingest in
/// time order); otherwise every segment spans the whole range. Times are
/// coarse (many ties). The last batch stays in the memtable.
fn engine(disjoint: bool) -> (tempfile::TempDir, Engine) {
    let d = tempfile::tempdir().unwrap();
    let mut o = EngineOptions::new(d.path());
    o.background = false;
    o.wal.durability = Durability::Async;
    let e = Engine::open(o).unwrap();
    for s in 0..8i64 {
        let evs: Vec<Event> = (0..300i64)
            .map(|j| {
                let t = if disjoint {
                    s * 300 + j / 3 * 3 // ties in threes
                } else {
                    (j * 37 + s * 11) % 2400 / 4 * 4
                };
                let mut ev = Event::new(tenant(), BASE + t * 1_000_000, 1);
                ev.event_id = Some(format!("s{s}-{j}"));
                ev.severity = [Severity::Info, Severity::Error, Severity::Warn][(j % 3) as usize];
                ev.body = Some(Value::String(format!("row {j} of {s}")));
                ev
            })
            .collect();
        e.write_batch(&tenant(), evs).unwrap();
        if s < 7 {
            e.flush().unwrap();
        }
    }
    (d, e)
}

fn fixtures() -> &'static [(tempfile::TempDir, Engine); 2] {
    static F: std::sync::OnceLock<[(tempfile::TempDir, Engine); 2]> = std::sync::OnceLock::new();
    F.get_or_init(|| [engine(true), engine(false)])
}

fn run(e: &Engine, sql: &str, ordered: bool, parallelism: usize) -> QueryOutput {
    let o = QueryOptions {
        enable_ordered_scan: ordered,
        parallelism,
        ..QueryOptions::default()
    };
    execute(&e.snapshot(&tenant()), sql, &o).unwrap_or_else(|err| panic!("{sql}: {err}"))
}

#[test]
fn latest_rows_stop_after_the_newest_sources() {
    let (_, e) = &fixtures()[0];
    let sql = "SELECT event_time, event_id FROM logs ORDER BY event_time DESC LIMIT 10";
    let fast = run(e, sql, true, 4);
    let full = run(e, sql, false, 4);
    assert_eq!(fast.rows, full.rows);
    // The newest rows are in the memtable: every flushed segment is skipped.
    assert!(
        fast.stats.sources_skipped_by_order >= 7,
        "all 7 segments (and older memtable windows): {:?}",
        fast.stats
    );
    assert_eq!(fast.stats.segments_scanned, 0);
    assert!(fast.stats.early_stop);
    assert!(
        fast.stats.rows_scanned * 5 < full.stats.rows_scanned,
        "{:?}",
        fast.stats
    );
    // Oldest first: the first segment answers.
    let sql = "SELECT event_time, event_id FROM logs ORDER BY event_time LIMIT 25";
    let fast = run(e, sql, true, 4);
    assert_eq!(fast.rows, run(e, sql, false, 4).rows);
    assert_eq!(fast.stats.segments_scanned, 1);
    assert!(fast.stats.sources_skipped_by_order >= 6);
    // Overlapping ranges cannot stop early but stay correct.
    let (_, e) = &fixtures()[1];
    let sql = "SELECT event_time, event_id FROM logs ORDER BY event_time DESC LIMIT 10";
    assert_eq!(run(e, sql, true, 4).rows, run(e, sql, false, 4).rows);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    /// Identical rows (and tie order) with the ordered scan on and off.
    #[test]
    fn ordered_scan_equals_default(
        fixture in 0usize..2,
        k in 1u64..120,
        desc in any::<bool>(),
        second in prop::sample::select(vec!["", ", event_id", ", event_id DESC", ", severity"]),
        filter in prop::sample::select(vec!["", "WHERE severity = 'ERROR'", "WHERE body LIKE '%of 3%'", "WHERE severity IN ('WARN', 'INFO')"]),
        par in prop::sample::select(vec![1usize, 4]),
    ) {
        let (_, e) = &fixtures()[fixture];
        let sql = format!(
            "SELECT event_time, event_id, severity FROM logs {filter} ORDER BY event_time {}{second} LIMIT {k}",
            if desc { "DESC" } else { "ASC" }
        );
        let fast = run(e, &sql, true, par);
        let full = run(e, &sql, false, par);
        prop_assert_eq!(&fast.rows, &full.rows, "{}", sql);
        prop_assert!(fast.stats.rows_scanned <= full.stats.rows_scanned);
    }
}
