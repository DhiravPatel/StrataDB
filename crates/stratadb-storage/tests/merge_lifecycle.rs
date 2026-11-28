//! Merge task lifecycle (spec IX.22.4, STRATA-ARCH-22-004): compaction
//! rewrites go PLANNED → RESERVED → RUNNING → WRITTEN → COMMITTED, are
//! SKIPPED when background memory is full, and FAIL while running.
//!
//! One test: the state counters are process-wide.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::sync::atomic::Ordering;

use stratadb_common::memory::MemoryTracker;
use stratadb_common::state::{MergeState as M, State};
use stratadb_common::{failpoint, TenantId};
use stratadb_schema::Event;
use stratadb_storage::{Durability, Engine, EngineOptions};

fn left(s: M) -> u64 {
    M::stats().seconds[s.index()].snapshot().count
}

fn tenant() -> TenantId {
    TenantId::new("m").unwrap()
}

#[test]
fn compaction_walks_the_merge_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let bg = MemoryTracker::root("background", 1024);
    let mut o = EngineOptions::new(dir.path());
    o.background = false;
    o.wal.durability = Durability::Async;
    o.compaction.min_inputs = 2;
    o.background_memory = Some(bg.clone());
    let e = Engine::open(o).unwrap();
    let load = |base: i64| {
        for f in 0..3i64 {
            let evs: Vec<Event> = (0..100i64)
                .map(|i| Event::new(tenant(), base + f * 100 + i, 1))
                .collect();
            e.write_batch(&tenant(), evs).unwrap();
            e.flush().unwrap();
        }
    };
    load(0);

    // Background memory full: the planned merge is skipped.
    let rep = e.compact().unwrap();
    assert_eq!(rep.deferred_for_memory, 1, "{rep:?}");
    assert_eq!((left(M::Planned), left(M::Reserved)), (1, 0));

    // Room: the merge runs to COMMITTED.
    bg.set_limit(0);
    let rep = e.compact().unwrap();
    assert_eq!(rep.groups, 1, "{rep:?}");
    assert_eq!(
        [M::Planned, M::Reserved, M::Running, M::Written].map(left),
        [2, 1, 1, 1]
    );

    // A failure while writing the output fails the running task; the inputs
    // stay committed.
    load(10_000);
    let before = e.snapshot(&tenant()).segments().len();
    assert!(failpoint::set("compaction::after_output_write", "error"));
    let err = e.compact();
    failpoint::clear_all();
    assert!(err.is_err());
    assert_eq!(e.snapshot(&tenant()).segments().len(), before);
    assert_eq!([M::Running, M::Written].map(left), [2, 1]);
    for s in M::ALL {
        assert_eq!(
            M::stats().entities[s.index()].load(Ordering::Relaxed),
            0,
            "merge tasks left in {s:?}"
        );
    }
    assert_eq!(M::stats().illegal.load(Ordering::Relaxed), 0);
}
