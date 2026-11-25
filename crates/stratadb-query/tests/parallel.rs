//! Parallel segment scans (spec §13.5) must return what a sequential scan
//! returns: same rows in the same order (ties included), same group order,
//! exact integer aggregates, float aggregates equal up to rounding, same pages,
//! and query-wide budgets.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use stratadb_common::{ErrorKind, TenantId};
use stratadb_query::{execute, execute_page, PageRequest, QueryOptions, QueryOutput};
use stratadb_schema::{Event, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

const BASE: i64 = 1_795_170_600_000_000_000;

fn tenant() -> TenantId {
    TenantId::new("p").unwrap()
}

/// 12 segments + a memtable; heavy ties on event_time and service.
fn engine(dir: &std::path::Path) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    let e = Engine::open(o).unwrap();
    for s in 0..13i64 {
        let evs: Vec<Event> = (0..150)
            .map(|j| {
                let i = s * 150 + j;
                let mut e = Event::new(tenant(), BASE + (i % 40) * 1_000_000_000, BASE);
                e.event_id = Some(format!("e{i:05}"));
                e.service = Some(["api", "db", "web", "auth"][(i % 4) as usize].into());
                e.attributes.insert("n".into(), Value::Int(i % 17));
                e.attributes
                    .insert("f".into(), Value::Float((i % 11) as f64 * 0.1 + 1.0 / 3.0));
                if i % 5 == 0 {
                    e.attributes.insert("mixed".into(), Value::from("text"));
                } else if i % 7 == 0 {
                    e.attributes.insert("mixed".into(), Value::Int(i));
                }
                e
            })
            .collect();
        e.write_batch(&tenant(), evs).unwrap();
        if s < 12 {
            e.flush().unwrap();
        }
    }
    e
}

fn opts(par: usize) -> QueryOptions {
    QueryOptions {
        parallelism: par,
        ..QueryOptions::default()
    }
}

fn run(e: &Engine, sql: &str, par: usize) -> QueryOutput {
    execute(&e.snapshot(&tenant()), sql, &opts(par)).unwrap_or_else(|err| panic!("{sql}: {err}"))
}

fn approx_eq(a: &[Vec<Option<Value>>], b: &[Vec<Option<Value>>]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b).all(|(x, y)| {
            x.len() == y.len()
                && x.iter().zip(y).all(|(u, v)| match (u, v) {
                    (Some(Value::Float(p)), Some(Value::Float(q))) => {
                        (p - q).abs() <= 1e-9 * p.abs().max(q.abs()).max(1.0)
                    }
                    _ => u == v,
                })
        })
}

#[test]
fn parallel_equals_sequential() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let exact = [
        // Row queries with ties: order must match exactly (stable merge).
        "SELECT event_id FROM logs ORDER BY event_time",
        "SELECT event_id FROM logs ORDER BY event_time DESC LIMIT 37",
        "SELECT event_id, service FROM logs ORDER BY service, attributes['n'] DESC LIMIT 100",
        "SELECT event_id FROM logs WHERE attributes['n'] > 10 ORDER BY attributes['n']",
        // Group order without ORDER BY = first-seen order.
        "SELECT service, count(*), min(attributes['n']), max(attributes['n']), sum(attributes['n']) FROM logs GROUP BY service",
        "SELECT attributes['n'] AS n, count(*) FROM logs GROUP BY attributes['n']",
        "SELECT time_bucket(INTERVAL '10 s', event_time) AS b, service, count(*) FROM logs GROUP BY time_bucket(INTERVAL '10 s', event_time), service ORDER BY b, service",
        "SELECT count(*), min(event_time), max(event_id) FROM logs",
        "SELECT count(*) FROM logs WHERE service = 'nope'",
    ];
    for sql in exact {
        let (s, p) = (run(&e, sql, 1), run(&e, sql, 8));
        assert_eq!(p.rows, s.rows, "{sql}");
        assert_eq!(p.columns, s.columns);
        assert_eq!(p.stats.rows_scanned, s.stats.rows_scanned, "{sql}");
        assert_eq!(p.stats.rows_matched, s.stats.rows_matched, "{sql}");
        assert_eq!(p.stats.segments_scanned, s.stats.segments_scanned, "{sql}");
        assert_eq!(p.stats.bytes_read, s.stats.bytes_read, "{sql}");
    }
    // Float aggregates: equal up to rounding (different addition order).
    for sql in [
        "SELECT service, avg(attributes['f']), sum(attributes['f']) FROM logs GROUP BY service ORDER BY service",
        "SELECT avg(attributes['f']) FROM logs",
    ] {
        let (s, p) = (run(&e, sql, 1), run(&e, sql, 8));
        assert!(approx_eq(&p.rows, &s.rows), "{sql}: {:?} vs {:?}", p.rows, s.rows);
        // Deterministic run to run.
        assert_eq!(run(&e, sql, 8).rows, p.rows, "{sql}");
    }
    // Pagination pages are identical.
    let sql = "SELECT event_id FROM logs ORDER BY event_time DESC";
    let (mut cs, mut cp) = (None, None);
    loop {
        let snap = e.snapshot(&tenant());
        let a = execute_page(
            &snap,
            sql,
            &opts(1),
            &PageRequest {
                page_size: 333,
                cursor: cs.clone(),
            },
        )
        .unwrap();
        let b = execute_page(
            &snap,
            sql,
            &opts(8),
            &PageRequest {
                page_size: 333,
                cursor: cp.clone(),
            },
        )
        .unwrap();
        assert_eq!(a.output.rows, b.output.rows);
        assert_eq!(a.next_cursor.is_some(), b.next_cursor.is_some());
        match (a.next_cursor, b.next_cursor) {
            (Some(x), Some(y)) => {
                cs = Some(x);
                cp = Some(y);
            }
            _ => break,
        }
    }
}

#[test]
fn budgets_and_errors_are_query_wide() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let snap = e.snapshot(&tenant());
    // 12 × 150 segment rows: a 1 000-row budget must fail in parallel too,
    // although no single segment exceeds it.
    let o = QueryOptions {
        max_rows_scanned: 1000,
        ..opts(8)
    };
    let err = execute(
        &snap,
        "SELECT service, count(*) FROM logs GROUP BY service",
        &o,
    )
    .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResourceExhausted, "{err}");
    // Memory budget likewise.
    let o = QueryOptions {
        max_memory_bytes: 20_000,
        ..opts(8)
    };
    let err = execute(
        &snap,
        "SELECT event_id, attributes FROM logs ORDER BY event_id",
        &o,
    )
    .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResourceExhausted, "{err}");
    // A data-dependent error (min over strings and ints) surfaces, not a
    // worker-cancellation symptom.
    let err = execute(&snap, "SELECT min(attributes['mixed']) FROM logs", &opts(8)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput, "{err}");
    let err1 = execute(&snap, "SELECT min(attributes['mixed']) FROM logs", &opts(1)).unwrap_err();
    assert_eq!(err1.kind(), ErrorKind::InvalidInput);
    // User cancellation is reported as such.
    let cancel = Arc::new(AtomicBool::new(true));
    let o = QueryOptions {
        cancel: Some(cancel),
        ..opts(8)
    };
    let err = execute(
        &snap,
        "SELECT service, count(*) FROM logs GROUP BY service",
        &o,
    )
    .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Cancelled);
}
