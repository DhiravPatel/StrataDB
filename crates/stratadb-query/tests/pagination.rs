//! Stable keyset pagination (spec §2.3, ADR-0023).
//!
//! The property under test: walking every page of a query returns each row
//! visible to the first page exactly once, in the query's order, regardless of
//! page size, flushes, compaction, or concurrent writes between pages.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;

use proptest::prelude::*;
use stratadb_common::time::NANOS_PER_SEC;
use stratadb_common::{ErrorKind, TenantId};
use stratadb_query::{execute, execute_page, PageRequest, QueryOptions, QueryStats};
use stratadb_schema::{Event, Value};
use stratadb_storage::{CompactionOptions, Durability, Engine, EngineOptions};

const BASE: i64 = 1_795_170_600 * NANOS_PER_SEC; // 2026-11-20T10:30:00Z

fn tenant(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn engine(dir: &std::path::Path) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    o.compaction = CompactionOptions {
        min_inputs: 2,
        ..CompactionOptions::default()
    };
    Engine::open(o).unwrap()
}

/// Event `i` at second `t` (many events share a timestamp, so ties must be
/// broken by row identity).
fn ev(t: &TenantId, i: i64, sec: i64) -> Event {
    let mut e = Event::new(t.clone(), BASE + sec * NANOS_PER_SEC, BASE);
    e.event_id = Some(format!("e{i:05}"));
    e.service = Some(["api", "db", "web"][(i % 3) as usize].to_owned());
    e.attributes.insert("n".into(), Value::Int(i % 7));
    e
}

/// Write `n` events (ids `start..start+n`) in batches of `batch`, flushing
/// every `flush_every` batches (0 = never).
fn load(e: &Engine, t: &TenantId, start: i64, n: i64, batch: i64, flush_every: i64, secs: i64) {
    let mut b = 0;
    let mut i = start;
    while i < start + n {
        let hi = (i + batch).min(start + n);
        e.write_batch(t, (i..hi).map(|j| ev(t, j, j % secs)).collect())
            .unwrap();
        b += 1;
        if flush_every > 0 && b % flush_every == 0 {
            e.flush().unwrap();
        }
        i = hi;
    }
}

fn opts() -> QueryOptions {
    QueryOptions::default()
}

fn ids(rows: &[Vec<Option<Value>>]) -> Vec<String> {
    rows.iter()
        .map(|r| match &r[0] {
            Some(Value::String(s)) => s.clone(),
            other => panic!("expected event_id, got {other:?}"),
        })
        .collect()
}

/// Walk all pages. `between` runs after each page (to mutate the database).
fn walk(
    e: &Engine,
    t: &TenantId,
    sql: &str,
    page_size: usize,
    mut between: impl FnMut(usize),
) -> (Vec<String>, Vec<usize>, Vec<QueryStats>) {
    let mut out = Vec::new();
    let mut sizes = Vec::new();
    let mut stats = Vec::new();
    let mut cursor = None;
    for n in 0.. {
        assert!(n < 10_000, "pagination did not terminate");
        let p = execute_page(
            &e.snapshot(t),
            sql,
            &opts(),
            &PageRequest {
                page_size,
                cursor: cursor.clone(),
            },
        )
        .unwrap();
        sizes.push(p.output.rows.len());
        out.extend(ids(&p.output.rows));
        stats.push(p.output.stats);
        match p.next_cursor {
            Some(c) => {
                assert_eq!(
                    sizes.last(),
                    Some(&page_size),
                    "only the last page may be short"
                );
                cursor = Some(c);
                between(n);
            }
            None => break,
        }
    }
    (out, sizes, stats)
}

fn assert_unique(ids: &[String]) {
    let set: BTreeSet<&String> = ids.iter().collect();
    assert_eq!(set.len(), ids.len(), "a row was returned twice");
}

#[test]
fn ordered_pages_match_the_unpaginated_result() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let t = tenant("a");
    // 500 rows over 20 distinct seconds: heavy ties on event_time.
    load(&e, &t, 0, 400, 37, 3, 20);
    load(&e, &t, 400, 100, 37, 0, 20); // these stay in the memtable
    for (sql, desc) in [
        ("SELECT event_id, event_time FROM logs ORDER BY event_time DESC", true),
        ("SELECT event_id FROM logs ORDER BY event_time", false),
        (
            "SELECT event_id FROM logs WHERE service != 'db' ORDER BY service, attributes['n'] DESC",
            false,
        ),
    ] {
        let full = execute(&e.snapshot(&t), sql, &opts()).unwrap();
        let full_ids = ids(&full.rows);
        for page_size in [1, 7, 50, 499, 500, 1000] {
            let (got, sizes, _) = walk(&e, &t, sql, page_size, |_| {});
            assert_unique(&got);
            assert_eq!(got.len(), full_ids.len(), "{sql} / {page_size}");
            assert_eq!(
                got.iter().collect::<BTreeSet<_>>(),
                full_ids.iter().collect::<BTreeSet<_>>()
            );
            assert_eq!(sizes.len(), full_ids.len().div_ceil(page_size).max(1));
            if desc {
                // Concatenated pages are in ORDER BY order.
                let p = execute_page(
                    &e.snapshot(&t),
                    sql,
                    &opts(),
                    &PageRequest {
                        page_size: 1000,
                        cursor: None,
                    },
                )
                .unwrap();
                assert_eq!(ids(&p.output.rows), got);
                let times: Vec<_> = p.output.rows.iter().map(|r| r[1].clone()).collect();
                assert!(times
                    .windows(2)
                    .all(|w| stratadb_query::eval::sort_cmp(&w[0], &w[1]).is_ge()));
            }
        }
    }
}

#[test]
fn without_order_by_pages_follow_ingest_order() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let t = tenant("a");
    load(&e, &t, 0, 300, 25, 2, 9);
    let (got, _, stats) = walk(&e, &t, "SELECT event_id FROM logs", 40, |_| {});
    let expect: Vec<String> = (0..300).map(|i| format!("e{i:05}")).collect();
    assert_eq!(got, expect);
    // Later pages skip whole segments that lie before the cursor.
    assert!(stats.last().unwrap().segments_pruned_cursor > 0);
}

#[test]
fn rows_written_after_the_first_page_never_appear() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let t = tenant("a");
    load(&e, &t, 0, 200, 20, 3, 50);
    let mut next = 10_000;
    let (got, _, _) = walk(
        &e,
        &t,
        "SELECT event_id FROM logs ORDER BY event_time DESC",
        30,
        |n| {
            // New rows at both ends of the order, plus flushes/compaction.
            e.write_batch(&t, vec![ev(&t, next, 49), ev(&t, next + 1, 0)])
                .unwrap();
            next += 2;
            if n % 2 == 0 {
                e.flush().unwrap();
            }
            if n == 3 {
                e.compact().unwrap();
            }
        },
    );
    assert_unique(&got);
    let expect: BTreeSet<String> = (0..200).map(|i| format!("e{i:05}")).collect();
    assert_eq!(got.into_iter().collect::<BTreeSet<_>>(), expect);
}

#[test]
fn compaction_between_pages_keeps_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let t = tenant("a");
    load(&e, &t, 0, 360, 30, 1, 12);
    let sql = "SELECT event_id FROM logs ORDER BY service DESC, event_time";
    let (got, _, _) = walk(&e, &t, sql, 25, |n| {
        if n == 1 {
            let r = e.compact().unwrap();
            assert!(r.inputs >= 2, "compaction must actually rewrite: {r:?}");
        }
    });
    assert_unique(&got);
    assert_eq!(got.len(), 360);
}

#[test]
fn limit_bounds_the_total_across_pages() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let t = tenant("a");
    load(&e, &t, 0, 100, 10, 2, 100);
    let sql = "SELECT event_id FROM logs ORDER BY event_time LIMIT 23";
    let (got, sizes, _) = walk(&e, &t, sql, 10, |_| {});
    assert_eq!(sizes, vec![10, 10, 3]);
    let full = execute(&e.snapshot(&t), sql, &opts()).unwrap();
    assert_eq!(got, ids(&full.rows));
    // Exactly-divisible LIMIT: no trailing empty page.
    let (_, sizes, _) = walk(&e, &t, "SELECT event_id FROM logs LIMIT 20", 10, |_| {});
    assert_eq!(sizes, vec![10, 10]);
    let (got, sizes, _) = walk(&e, &t, "SELECT event_id FROM logs LIMIT 0", 10, |_| {});
    assert!(got.is_empty());
    assert_eq!(sizes, vec![0]);
}

#[test]
fn time_ordered_pages_prune_segments_behind_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let t = tenant("a");
    // Ten segments with disjoint time ranges.
    for s in 0..10 {
        e.write_batch(
            &t,
            (0..20).map(|i| ev(&t, s * 20 + i, s * 100 + i)).collect(),
        )
        .unwrap();
        e.flush().unwrap();
    }
    let (got, _, stats) = walk(
        &e,
        &t,
        "SELECT event_id FROM logs ORDER BY event_time DESC",
        20,
        |_| {},
    );
    assert_eq!(got.len(), 200);
    // The last full page's cursor sits in the oldest segment: 9 newer pruned.
    let pruned: Vec<usize> = stats.iter().map(|s| s.segments_pruned_cursor).collect();
    assert_eq!(pruned[0], 0);
    assert!(pruned.windows(2).all(|w| w[0] <= w[1]), "{pruned:?}");
    assert!(*pruned.last().unwrap() >= 8, "{pruned:?}");
}

#[test]
fn invalid_requests_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let (a, b) = (tenant("a"), tenant("b"));
    load(&e, &a, 0, 30, 10, 0, 30);
    load(&e, &b, 0, 30, 10, 0, 30);
    let req = |page_size, cursor: Option<String>| PageRequest { page_size, cursor };
    let page =
        |t: &TenantId, sql: &str, r: &PageRequest| execute_page(&e.snapshot(t), sql, &opts(), r);
    let sql = "SELECT event_id FROM logs ORDER BY event_time";
    let kind =
        |r: stratadb_common::Result<_>| r.map(|_: stratadb_query::Page| ()).unwrap_err().kind();
    assert_eq!(kind(page(&a, sql, &req(0, None))), ErrorKind::InvalidInput);
    assert_eq!(
        kind(page(&a, sql, &req(opts().max_result_rows + 1, None))),
        ErrorKind::ResourceExhausted
    );
    for bad in [
        "SELECT service, count(*) FROM logs GROUP BY service",
        "SELECT count(*) FROM logs",
        "EXPLAIN SELECT * FROM logs",
        "EXPLAIN ANALYZE SELECT * FROM logs",
    ] {
        assert_eq!(
            kind(page(&a, bad, &req(5, None))),
            ErrorKind::InvalidInput,
            "{bad}"
        );
    }
    let c = page(&a, sql, &req(5, None)).unwrap().next_cursor.unwrap();
    // Valid cursor continues.
    assert_eq!(
        page(&a, sql, &req(5, Some(c.clone())))
            .unwrap()
            .output
            .rows
            .len(),
        5
    );
    // Whitespace around the query is not significant.
    assert!(page(&a, &format!("  {sql}\n"), &req(5, Some(c.clone()))).is_ok());
    // Another query, another tenant, a different ORDER BY shape, tampering.
    for (t, q, cur) in [
        (
            &a,
            "SELECT event_id FROM logs ORDER BY event_time DESC",
            c.clone(),
        ),
        (&b, sql, c.clone()),
        (&a, "SELECT event_id FROM logs", c.clone()),
        (&a, sql, format!("{c}A")),
        (&a, sql, c[..c.len() - 2].to_owned()),
        (&a, sql, "not a cursor!".to_owned()),
        (&a, sql, String::new()),
    ] {
        assert_eq!(
            kind(page(t, q, &req(5, Some(cur)))),
            ErrorKind::InvalidInput,
            "{q}"
        );
    }
}

#[test]
fn empty_result_has_no_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let t = tenant("a");
    let p = execute_page(
        &e.snapshot(&t),
        "SELECT * FROM logs ORDER BY event_time",
        &opts(),
        &PageRequest {
            page_size: 10,
            cursor: None,
        },
    )
    .unwrap();
    assert!(p.output.rows.is_empty() && p.next_cursor.is_none());
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

    /// Any page size, data layout and supported order: exactly once, in order.
    #[test]
    fn pagination_is_exactly_once(
        n in 1i64..160,
        batch in 1i64..40,
        flush_every in 0i64..4,
        secs in 1i64..12,
        page_size in 1usize..40,
        order in 0usize..5,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let e = engine(dir.path());
        let t = tenant("p");
        load(&e, &t, 0, n, batch, flush_every, secs);
        let sql = [
            "SELECT event_id FROM logs",
            "SELECT event_id FROM logs ORDER BY event_time",
            "SELECT event_id FROM logs ORDER BY event_time DESC",
            "SELECT event_id, service FROM logs ORDER BY 2 DESC, attributes['n']",
            "SELECT event_id FROM logs WHERE attributes['n'] >= 2 ORDER BY attributes['missing'], event_time DESC",
        ][order];
        let full = execute(&e.snapshot(&t), sql, &opts()).unwrap();
        let (got, _, _) = walk(&e, &t, sql, page_size, |_| {});
        assert_unique(&got);
        let want: BTreeSet<String> = ids(&full.rows).into_iter().collect();
        prop_assert_eq!(got.into_iter().collect::<BTreeSet<_>>(), want);
    }
}
