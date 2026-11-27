//! Query memory accounting against the tracker hierarchy (spec VIII.5.2,
//! STRATA-SYS-05-005/006/007): column batches are reserved before they are
//! read, pool limits above the query apply, failures name the tracker path,
//! reservations never leak, and a query can wait for memory.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use stratadb_common::memory::{MemoryTracker, Reservation};
use stratadb_common::{ErrorKind, TenantId};
use stratadb_query::{execute, QueryOptions};
use stratadb_schema::{Event, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("m").unwrap()
}

fn engine(dir: &Path) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    let e = Engine::open(o).unwrap();
    for s in 0..6i64 {
        let evs: Vec<Event> = (0..2000i64)
            .map(|j| {
                let mut ev = Event::new(tenant(), 1_795_170_600_000_000_000 + s * 2000 + j, 1);
                ev.service = Some(format!("svc{}", j % 9));
                ev.body = Some(Value::String(format!("message number {j} of segment {s}")));
                ev
            })
            .collect();
        e.write_batch(&tenant(), evs).unwrap();
        e.flush().unwrap();
    }
    // Some rows stay in the memtable.
    let evs: Vec<Event> = (0..500i64)
        .map(|j| Event::new(tenant(), 1_795_170_700_000_000_000 + j, 1))
        .collect();
    e.write_batch(&tenant(), evs).unwrap();
    e
}

fn opts(pool: &Arc<MemoryTracker>, par: usize) -> QueryOptions {
    QueryOptions {
        memory: Some(Arc::clone(pool)),
        parallelism: par,
        target_ms_per_worker: 0.0,
        ..QueryOptions::default()
    }
}

#[test]
fn reservations_cover_batches_and_are_always_released() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let snap = e.snapshot(&tenant());
    let root = MemoryTracker::root("process", 0);
    let pool = root.child("query", 0);
    for par in [1, 4] {
        for sql in [
            "SELECT count(*) FROM logs WHERE event_time > TIMESTAMP '1970-01-01T00:00:00Z'",
            "SELECT service, count(*) FROM logs GROUP BY service ORDER BY service",
            "SELECT body FROM logs ORDER BY event_time DESC LIMIT 50",
        ] {
            let out = execute(&snap, sql, &opts(&pool, par)).unwrap();
            // A filtered count(*) holds almost no operator state, but the
            // event_time batches it filters were reserved before being read.
            assert!(
                out.stats.memory_reserved_peak >= 2000 * 8,
                "{sql}: {:?}",
                out.stats
            );
            assert_eq!(pool.used(), 0, "{sql} (par {par}) leaked a reservation");
            assert_eq!(root.used(), 0);
        }
    }
    assert!(pool.peak() > 0);
    // Failed queries release too.
    let small = QueryOptions {
        max_memory_bytes: 20_000,
        ..opts(&pool, 4)
    };
    let err = execute(&snap, "SELECT body FROM logs ORDER BY body", &small).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResourceExhausted, "{err}");
    assert!(err.to_string().contains("MEMORY_LIMIT_EXCEEDED"), "{err}");
    assert_eq!(pool.used(), 0);
}

#[test]
fn batch_memory_is_reserved_before_the_read() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let snap = e.snapshot(&tenant());
    let pool = MemoryTracker::root("query", 0);
    // A count(*) over body needs one body batch (~2000 strings) in memory:
    // with a limit below that, the read is refused before it happens.
    let o = QueryOptions {
        max_memory_bytes: 16 << 10,
        ..opts(&pool, 1)
    };
    let err = execute(&snap, "SELECT count(*) FROM logs WHERE body = 'x'", &o).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("MEMORY_LIMIT_EXCEEDED: query/exec would exceed"),
        "{msg}"
    );
    assert!(msg.contains("query limit 16384 bytes"), "{msg}");
    assert_eq!(err.kind(), ErrorKind::ResourceExhausted);
    // The same query fits with room for a batch.
    let o = QueryOptions {
        max_memory_bytes: 4 << 20,
        ..opts(&pool, 1)
    };
    let out = execute(&snap, "SELECT count(*) FROM logs WHERE body = 'x'", &o).unwrap();
    assert_eq!(out.rows[0][0], Some(Value::Int(0)));
}

#[test]
fn pool_limits_above_the_query_apply_and_name_the_pool() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let snap = e.snapshot(&tenant());
    let root = MemoryTracker::root("process", 0);
    let pool = root.child("query", 512 << 10);
    let t = pool.private_child("tenant:m", 0);
    // Another tenant holds most of the pool.
    let other = pool.private_child("tenant:other", 0);
    let held = Reservation::new(&other, 448 << 10, Duration::ZERO).unwrap();
    let err = execute(&snap, "SELECT body FROM logs LIMIT 5", &opts(&t, 1)).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("process/query would exceed its limit of 512.0 KiB"),
        "{msg}"
    );
    assert!(
        msg.contains("<other>=448.0 KiB") && !msg.contains("tenant:other"),
        "{msg}"
    );
    assert_eq!(t.used(), 0);
    // Waiting: the query succeeds once the other reservation is released.
    let waiting = QueryOptions {
        memory_wait: Duration::from_secs(10),
        ..opts(&t, 1)
    };
    let releaser = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(held);
    });
    let out = execute(&snap, "SELECT body FROM logs LIMIT 5", &waiting).unwrap();
    assert_eq!(out.rows.len(), 5);
    releaser.join().unwrap();
    assert_eq!(root.used(), 0);
}
