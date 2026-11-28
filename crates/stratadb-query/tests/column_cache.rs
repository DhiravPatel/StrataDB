//! Query results are identical with the decoded column cache on and off
//! (ADR-0042), and repeated queries are served from it.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use stratadb_common::TenantId;
use stratadb_query::{execute, QueryOptions};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("cc").unwrap()
}

fn engine(dir: &std::path::Path, cache: usize) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    o.column_cache_bytes = cache;
    let e = Engine::open(o).unwrap();
    for s in 0..6i64 {
        let evs: Vec<Event> = (0..400i64)
            .map(|i| {
                let mut ev = Event::new(
                    tenant(),
                    1_795_170_600_000_000_000 + (s * 400 + i) * 1_000_000,
                    1,
                );
                ev.service = Some(["api", "db", "web"][(i % 3) as usize].to_owned());
                ev.severity = [Severity::Info, Severity::Error, Severity::Warn][(i % 3) as usize];
                ev.body = Some(Value::String(format!(
                    "request {i} in segment {s}{}",
                    if i % 40 == 0 { " TIMEOUT" } else { "" }
                )));
                ev.attributes
                    .insert("user".into(), Value::String(format!("u{}", i % 17)));
                ev
            })
            .collect();
        e.write_batch(&tenant(), evs).unwrap();
        e.flush().unwrap();
    }
    e
}

#[test]
fn results_are_identical_with_the_cache_and_repeats_hit_it() {
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (on, off) = (engine(d1.path(), 32 << 20), engine(d2.path(), 0));
    let queries = [
        "SELECT count(body) FROM logs WHERE body LIKE '%TIMEOUT%'",
        "SELECT service, count(*) FROM logs GROUP BY service ORDER BY service",
        "SELECT event_time, body FROM logs WHERE severity = 'ERROR' ORDER BY event_time DESC LIMIT 20",
        "SELECT attributes['user'] AS u, count(*) AS n FROM logs GROUP BY attributes['user'] ORDER BY n DESC, u LIMIT 5",
        "SELECT body, service FROM logs WHERE service = 'db' LIMIT 50",
        "SELECT min(event_time), max(event_time), count(*) FROM logs WHERE body LIKE '%segment 3%'",
    ];
    let opts = QueryOptions::default();
    for _round in 0..2 {
        for q in queries {
            let a = execute(&on.snapshot(&tenant()), q, &opts).unwrap();
            let b = execute(&off.snapshot(&tenant()), q, &opts).unwrap();
            assert_eq!(a.rows, b.rows, "{q}");
            assert_eq!(b.stats.column_cache_hits, 0);
        }
    }
    // The second round read cached columns.
    let again = execute(&on.snapshot(&tenant()), queries[0], &opts).unwrap();
    assert!(again.stats.column_cache_hits >= 6, "{:?}", again.stats);
    assert_eq!(again.stats.bytes_read, 0, "every column it needs is cached");
    let s = on.stats().column_cache.unwrap();
    assert!(s.hits > 0 && s.protected_entries > 0, "{s:?}");
}
