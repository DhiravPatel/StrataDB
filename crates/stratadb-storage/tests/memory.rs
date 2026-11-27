//! Engine memory regions (spec VIII.5.2, STRATA-ARCH-01-006):
//! - ingest: memtable bytes are reserved before the WAL append and released
//!   by flush; a full region refuses a write before it is durable;
//! - cache: decoded filters are tracked;
//! - background: compaction reserves its input rows and is postponed when the
//!   region is full;
//! - regions are independent, and an engine never leaks reservations.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use stratadb_common::memory::MemoryTracker;
use stratadb_common::{ErrorKind, TenantId};
use stratadb_schema::{Event, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("t").unwrap()
}

fn events(n: i64, from: i64) -> Vec<Event> {
    (0..n)
        .map(|j| {
            let mut e = Event::new(tenant(), 1_795_170_600_000_000_000 + from + j, 1);
            e.trace_id = Some(format!("{:032x}", from + j));
            e.body = Some(Value::String(format!("event {j} with some body text")));
            e
        })
        .collect()
}

struct Regions {
    root: Arc<MemoryTracker>,
    ingest: Arc<MemoryTracker>,
    cache: Arc<MemoryTracker>,
    background: Arc<MemoryTracker>,
    query: Arc<MemoryTracker>,
}

fn regions(ingest_limit: u64, background_limit: u64) -> Regions {
    let root = MemoryTracker::root("process", 0);
    Regions {
        ingest: root.child("ingest", ingest_limit),
        cache: root.child("cache", 0),
        background: root.child("background", background_limit),
        query: root.child("query", 1 << 20),
        root,
    }
}

fn open(dir: &Path, r: &Regions) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Sync;
    o.write_stall_timeout = Duration::from_millis(100);
    o.memory = Some(Arc::clone(&r.ingest));
    o.cache_memory = Some(Arc::clone(&r.cache));
    o.background_memory = Some(Arc::clone(&r.background));
    o.compaction.small_segment_bytes = u64::MAX;
    Engine::open(o).unwrap()
}

#[test]
fn ingest_memory_is_reserved_before_the_wal_and_released_by_flush() {
    let dir = tempfile::tempdir().unwrap();
    let r = regions(0, 0);
    let e = open(dir.path(), &r);
    e.write_batch(&tenant(), events(100, 0)).unwrap();
    e.write_batch(&tenant(), events(50, 100)).unwrap();
    let held = r.ingest.used();
    assert!(held > 0);
    assert_eq!(
        held,
        e.stats().active_bytes as u64,
        "reservation equals memtable bytes"
    );
    e.flush().unwrap();
    assert_eq!(r.ingest.used(), 0, "flush released the memtable's memory");
    assert!(r.ingest.peak() >= held);

    // A full region refuses the write before the WAL append.
    r.ingest.set_limit(held / 2);
    let seq_before = e
        .write_batch(&tenant(), events(10, 1000))
        .unwrap()
        .commit_seq;
    let used = r.ingest.used();
    let err = e.write_batch(&tenant(), events(150, 2000)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResourceExhausted, "{err}");
    assert!(err.kind().is_retryable());
    assert!(err.to_string().contains("ingest memory is full"), "{err}");
    assert!(err.to_string().contains("process/ingest"), "{err}");
    assert_eq!(r.ingest.used(), used, "a refused write holds nothing");
    let s = e.stats();
    assert!(s.write_stalls >= 1 && s.write_rejections >= 1, "{s:?}");
    // Nothing of the refused batch was committed.
    let seq_after = e
        .write_batch(&tenant(), events(1, 3000))
        .unwrap()
        .commit_seq;
    assert_eq!(seq_after, seq_before + 1);
    assert_eq!(
        e.snapshot(&tenant()).scan_all().unwrap().len(),
        150 + 10 + 1
    );
    // After a flush the same write fits.
    e.flush().unwrap();
    e.write_batch(&tenant(), events(40, 4000)).unwrap();
    drop(e);
    assert_eq!(
        r.ingest.used(),
        0,
        "dropping the engine releases its reservations"
    );
}

#[test]
fn replayed_rows_are_tracked_and_regions_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    let r = regions(0, 0);
    {
        let e = open(dir.path(), &r);
        e.write_batch(&tenant(), events(200, 0)).unwrap();
        // Dropped without a flush: rows stay in the WAL.
    }
    assert_eq!(r.ingest.used(), 0);
    let e = open(dir.path(), &r);
    let replayed = e.stats().active_bytes as u64;
    assert!(replayed > 0);
    assert_eq!(r.ingest.used(), replayed, "replayed rows are tracked");
    // A full query pool does not affect ingest (ARCH-01-006).
    r.query.try_reserve(1 << 20).unwrap();
    assert!(r.query.try_reserve(1).is_err());
    e.write_batch(&tenant(), events(10, 500)).unwrap();
    r.query.release(1 << 20);
    e.flush().unwrap();
    assert_eq!(r.ingest.used(), 0);
}

#[test]
fn decoded_filters_are_tracked_on_the_cache_region() {
    let dir = tempfile::tempdir().unwrap();
    let r = regions(0, 0);
    let e = open(dir.path(), &r);
    e.write_batch(&tenant(), events(300, 0)).unwrap();
    e.flush().unwrap();
    assert_eq!(r.cache.used(), 0, "filters are decoded on first use");
    let snap = e.snapshot(&tenant());
    assert!(snap.segments()[0].bloom().is_some());
    let charged = r.cache.used();
    assert!(charged > 0);
    drop(snap);
    drop(e);
    assert_eq!(r.cache.used(), 0, "released with the segment");
    assert_eq!(r.root.used(), 0);
}

#[test]
fn compaction_is_postponed_while_background_memory_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let r = regions(0, 1024);
    let e = open(dir.path(), &r);
    for i in 0..4 {
        e.write_batch(&tenant(), events(100, i * 100)).unwrap();
        e.flush().unwrap();
    }
    let rep = e.compact().unwrap();
    assert_eq!((rep.groups, rep.deferred_for_memory), (0, 1), "{rep:?}");
    assert_eq!(e.snapshot(&tenant()).segments().len(), 4);
    assert_eq!(r.background.used(), 0);
    r.background.set_limit(0);
    let rep = e.compact().unwrap();
    assert_eq!((rep.groups, rep.deferred_for_memory), (1, 0), "{rep:?}");
    assert_eq!(e.snapshot(&tenant()).segments().len(), 1);
    assert!(r.background.peak() > 1024);
    assert_eq!(r.background.used(), 0);
}
