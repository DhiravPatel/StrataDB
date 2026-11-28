//! Decoded column cache (ADR-0042; spec STRATA-SYS-16-001/002/004): a
//! second read of a segment column is served decoded, with identical data,
//! no bytes read, and the cache memory region charged exactly what the
//! cache holds.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;

use stratadb_common::memory::MemoryTracker;
use stratadb_common::TenantId;
use stratadb_schema::column::ColumnId;
use stratadb_schema::{Event, Value};
use stratadb_storage::scan::ColumnRequest;
use stratadb_storage::{Durability, Engine, EngineOptions};

fn engine(
    dir: &std::path::Path,
    cache_bytes: usize,
    region: &std::sync::Arc<MemoryTracker>,
) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    o.column_cache_bytes = cache_bytes;
    o.cache_memory = Some(std::sync::Arc::clone(region));
    let e = Engine::open(o).unwrap();
    let t = TenantId::new("c").unwrap();
    for s in 0..4i64 {
        let evs: Vec<Event> = (0..500i64)
            .map(|i| {
                let mut ev = Event::new(t.clone(), s * 1000 + i, 1);
                ev.service = Some(format!("svc{}", i % 7));
                ev.body = Some(Value::String(format!("body {s}/{i}")));
                ev
            })
            .collect();
        e.write_batch(&t, evs).unwrap();
        e.flush().unwrap();
    }
    e
}

/// Per source: (bytes read, cached columns, columns rendered for comparison).
type Reads = Vec<(u64, u64, Vec<(ColumnId, String)>)>;

fn read_all(e: &Engine) -> Reads {
    let t = TenantId::new("c").unwrap();
    let none = BTreeSet::new();
    let cols = [
        ColumnId::EventTime,
        ColumnId::Service,
        ColumnId::Body,
        ColumnId::Severity,
    ];
    let req = ColumnRequest {
        columns: &cols,
        attr_keys: &none,
        whole_attributes: false,
        resource_keys: &none,
        whole_resource: false,
        raw_attr_keys: &none,
        raw_resource_keys: &none,
        projection: false,
    };
    e.snapshot(&t)
        .sources()
        .iter()
        .map(|s| {
            let b = s.read(&t, &req).unwrap();
            let mut cols: Vec<(ColumnId, String)> = b
                .columns
                .iter()
                .map(|(k, v)| (*k, format!("{v:?}")))
                .collect();
            cols.sort_by_key(|(k, _)| *k);
            (b.bytes_read, b.cached_columns, cols)
        })
        .collect()
}

#[test]
fn second_reads_are_served_decoded_and_identical() {
    let dir = tempfile::tempdir().unwrap();
    let region = MemoryTracker::root("cache", 0);
    let e = engine(dir.path(), 64 << 20, &region);
    let first = read_all(&e);
    assert!(first
        .iter()
        .all(|(bytes, cached, _)| *bytes > 0 && *cached == 0));
    let second = read_all(&e);
    for ((_, _, a), (bytes, cached, b)) in first.iter().zip(&second) {
        assert_eq!(a, b, "cached columns equal decoded ones");
        assert_eq!((*bytes, *cached), (0, 4), "no read, every column cached");
    }
    let s = e.stats().column_cache.unwrap();
    assert_eq!((s.hits, s.misses), (16, 16));
    assert!(s.bytes > 0);
    assert_eq!(
        region.used(),
        s.bytes as u64,
        "region charged exactly what is held"
    );
    drop(e);
    assert_eq!(region.used(), 0, "released when the engine closes");
}

#[test]
fn the_budget_and_the_memory_region_bound_the_cache() {
    // A budget far below one segment's columns: nothing big is cached.
    let dir = tempfile::tempdir().unwrap();
    let region = MemoryTracker::root("cache", 0);
    let e = engine(dir.path(), 64 * 1024, &region);
    read_all(&e);
    let s = e.stats().column_cache.unwrap();
    assert!(s.bytes <= s.capacity_bytes, "{s:?}");
    assert_eq!(region.used(), s.bytes as u64);
    // A full region: columns are not cached at all, reads still work.
    let dir = tempfile::tempdir().unwrap();
    let tiny = MemoryTracker::root("cache", 1024);
    let e = engine(dir.path(), 64 << 20, &tiny);
    let a = read_all(&e);
    let b = read_all(&e);
    assert_eq!(
        a.iter().map(|x| &x.2).collect::<Vec<_>>(),
        b.iter().map(|x| &x.2).collect::<Vec<_>>()
    );
    assert!(tiny.used() <= 1024);
    // Off by default.
    let dir = tempfile::tempdir().unwrap();
    let mut o = EngineOptions::new(dir.path());
    o.background = false;
    assert!(Engine::open(o).unwrap().stats().column_cache.is_none());
}
