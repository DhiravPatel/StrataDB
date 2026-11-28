//! Snapshot catalog (spec VIII.17, STRATA-SYS-17-001..005): snapshots are
//! taken from a published read view without the state lock, stay isolated
//! from later flushes and compactions, are registered with their holders,
//! report the obsolete bytes they pin, and obsolete files outlive their last
//! reference by `old_segments_lifetime`.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use stratadb_common::TenantId;
use stratadb_schema::Event;
use stratadb_storage::{Durability, Engine, EngineOptions};

fn t(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn opts(dir: &Path, lifetime: Duration) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    o.compaction.min_inputs = 2;
    o.old_segments_lifetime = lifetime;
    o
}

fn write(e: &Engine, tenant: &TenantId, from: i64, n: i64) {
    let evs: Vec<Event> = (from..from + n)
        .map(|i| Event::new(tenant.clone(), i, 1))
        .collect();
    e.write_batch(tenant, evs).unwrap();
}

fn rows(s: &stratadb_storage::Snapshot) -> usize {
    s.scan_all().unwrap().len()
}

fn segment_files(dir: &Path, tenant: &str) -> usize {
    std::fs::read_dir(dir.join("segments").join(tenant)).map_or(0, |d| d.count())
}

#[test]
fn snapshots_are_isolated_from_later_flushes_and_compactions() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(opts(dir.path(), Duration::ZERO)).unwrap();
    let a = t("a");
    for k in 0..3 {
        write(&e, &a, k * 10, 10);
        e.flush().unwrap();
    }
    write(&e, &a, 100, 5); // memtable
    let before = e.snapshot(&a);
    assert_eq!((before.segments().len(), rows(&before)), (3, 35));
    // A flush moves the memtable rows into a segment; compaction merges.
    e.flush().unwrap();
    e.compact().unwrap();
    write(&e, &a, 200, 7);
    let after = e.snapshot(&a);
    assert_eq!(after.segments().len(), 1);
    assert_eq!(rows(&after), 42);
    // The old snapshot still reads exactly what it saw, from the old files.
    assert_eq!((before.segments().len(), rows(&before)), (3, 35));
}

#[test]
fn the_registry_lists_open_snapshots_with_holders_and_forgets_dropped_ones() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(opts(dir.path(), Duration::ZERO)).unwrap();
    let (a, b) = (t("a"), t("b"));
    write(&e, &a, 0, 3);
    e.flush().unwrap();
    let s1 = e.snapshot(&a);
    s1.set_holder("q-111");
    s1.set_holder("ignored: first label wins");
    let s2 = e.snapshot(&b);
    let r = e.snapshots();
    assert_eq!(r.open.len(), 2);
    let i1 = r.open.iter().find(|x| x.id == s1.id()).unwrap();
    assert_eq!(
        (i1.tenant.as_str(), i1.holder.as_deref(), i1.segments),
        ("a", Some("q-111"), 1)
    );
    let i2 = r.open.iter().find(|x| x.id == s2.id()).unwrap();
    assert_eq!(
        (i2.tenant.as_str(), i2.holder.as_deref(), i2.segments),
        ("b", None, 0)
    );
    // Clones are the same snapshot; it is gone when the last one drops.
    let s1b = s1.clone();
    drop(s1);
    assert_eq!(e.snapshots().open.len(), 2);
    drop((s1b, s2));
    assert!(e.snapshots().open.is_empty());
}

#[test]
fn held_snapshots_pin_obsolete_bytes_until_released() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(opts(dir.path(), Duration::ZERO)).unwrap();
    let a = t("a");
    for k in 0..4 {
        write(&e, &a, k * 100, 100);
        e.flush().unwrap();
    }
    let held = e.snapshot(&a);
    held.set_holder("long-report");
    let rep = e.compact().unwrap();
    assert_eq!(
        (rep.groups, rep.obsolete_deleted),
        (1, 0),
        "pinned: not deleted"
    );
    let r = e.snapshots();
    assert_eq!(r.obsolete_segments, 4);
    assert!(r.obsolete_bytes > 0);
    let info = r.open.iter().find(|x| x.id == held.id()).unwrap();
    assert_eq!(info.obsolete_segments_pinned, 4);
    assert_eq!(info.obsolete_bytes_pinned, r.obsolete_bytes);
    assert!(
        r.obsolete_ratio > 0.0 && r.obsolete_ratio < 0.10 && !r.warn,
        "{r:?}"
    );
    assert_eq!(segment_files(dir.path(), "a"), 5);
    drop(held);
    let rep = e.compact().unwrap();
    assert_eq!(rep.obsolete_deleted, 4);
    assert_eq!(segment_files(dir.path(), "a"), 1);
    let r = e.snapshots();
    assert_eq!((r.obsolete_segments, r.obsolete_bytes), (0, 0));
}

#[test]
fn obsolete_files_outlive_their_last_reference_by_the_lifetime() {
    let dir = tempfile::tempdir().unwrap();
    // Long enough that the "still kept" check runs inside it even on a
    // loaded machine.
    let lifetime = Duration::from_secs(2);
    let e = Engine::open(opts(dir.path(), lifetime)).unwrap();
    let a = t("a");
    for k in 0..3 {
        write(&e, &a, k * 10, 10);
        e.flush().unwrap();
    }
    let started = Instant::now();
    e.compact().unwrap();
    // The inputs became obsolete during that call, so no later than now.
    let obsolete_by = Instant::now();
    // Unreferenced, but younger than the lifetime: kept.
    assert_eq!(e.compact().unwrap().obsolete_deleted, 0);
    assert!(
        started.elapsed() < lifetime,
        "test premise: still inside the lifetime"
    );
    assert_eq!(segment_files(dir.path(), "a"), 4);
    std::thread::sleep(lifetime.saturating_sub(obsolete_by.elapsed()) + Duration::from_millis(50));
    assert_eq!(e.compact().unwrap().obsolete_deleted, 3);
    assert_eq!(segment_files(dir.path(), "a"), 1);
    // Data unaffected.
    assert_eq!(rows(&e.snapshot(&a)), 30);
}

#[test]
fn snapshots_and_usage_are_tenant_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(opts(dir.path(), Duration::ZERO)).unwrap();
    let (a, b) = (t("a"), t("b"));
    write(&e, &a, 0, 10);
    write(&e, &b, 0, 4);
    e.flush().unwrap();
    write(&e, &b, 10, 2);
    let (sa, sb) = (e.snapshot(&a), e.snapshot(&b));
    assert_eq!((rows(&sa), rows(&sb)), (10, 6));
    assert!(sa.segments().iter().all(|s| s.entry.tenant == a));
    assert!(sb.segments().iter().all(|s| s.entry.tenant == b));
    assert_eq!(e.tenant_usage(&a).segment_rows, 10);
    assert_eq!(e.tenant_usage(&b).segment_rows, 4);
    assert_eq!(rows(&e.snapshot(&t("nobody"))), 0);
}

#[test]
fn concurrent_snapshots_never_see_a_row_twice_or_lose_one() {
    let dir = tempfile::tempdir().unwrap();
    let e = Arc::new(Engine::open(opts(dir.path(), Duration::ZERO)).unwrap());
    let a = t("a");
    let stop = Arc::new(AtomicBool::new(false));
    // Writer: batches of 50 rows with distinct times, flushing and
    // compacting as it goes.
    let writer = {
        let (e, a, stop) = (Arc::clone(&e), a.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut next = 0i64;
            for k in 0..120 {
                write(&e, &a, next, 50);
                next += 50;
                if k % 3 == 0 {
                    e.flush().unwrap();
                }
                if k % 10 == 0 {
                    e.compact().unwrap();
                }
            }
            stop.store(true, Ordering::Relaxed);
            next
        })
    };
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let (e, a, stop) = (Arc::clone(&e), a.clone(), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut last = 0usize;
                let mut checks = 0;
                while !stop.load(Ordering::Relaxed) {
                    let s = e.snapshot(&a);
                    let all = s.scan_all().unwrap();
                    let mut times: Vec<i64> = all.iter().map(|(_, _, ev)| ev.event_time).collect();
                    times.sort_unstable();
                    let n = times.len();
                    times.dedup();
                    assert_eq!(times.len(), n, "a row seen twice in one snapshot");
                    // Whole batches, a prefix of what was written, never shrinking.
                    assert_eq!(n % 50, 0);
                    assert_eq!(times.last().map_or(0, |t| *t as usize + 1), n);
                    assert!(n >= last);
                    last = n;
                    checks += 1;
                }
                checks
            })
        })
        .collect();
    let written = writer.join().unwrap();
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
    assert_eq!(rows(&e.snapshot(&a)) as i64, written);
    assert!(e.snapshots().open.is_empty());
}

/// Snapshot creation cost does not grow with the number of segments
/// (spec STRATA-SYS-17-003: <= 20 us at 100k parts). Run with
/// `cargo test --release -p stratadb-storage --test snapshots
/// snapshot_cost -- --ignored --nocapture`.
#[test]
#[ignore = "timing measurement"]
fn snapshot_cost_is_independent_of_segment_count() {
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open(opts(dir.path(), Duration::ZERO)).unwrap();
    let a = t("a");
    let mut made = 0;
    for target in [10usize, 300, 1500] {
        while made < target {
            // Spread segments over 50 tenants plus the measured one.
            let tenant = if made % 2 == 0 {
                a.clone()
            } else {
                t(&format!("o{}", made % 50))
            };
            write(&e, &tenant, made as i64, 1);
            e.flush().unwrap();
            made += 1;
        }
        let reps = 20_000u32;
        let started = Instant::now();
        for _ in 0..reps {
            std::hint::black_box(e.snapshot(&a));
        }
        let per = started.elapsed() / reps;
        println!(
            "{made:5} segments ({} for the tenant): {per:?} per snapshot",
            e.snapshot(&a).segments().len()
        );
    }
}
