//! End-to-end storage engine behaviour (Phase 1 exit criteria).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use stratadb_common::{ErrorKind, TenantId};
use stratadb_schema::{Event, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn opts(dir: &Path) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Sync;
    o.wal.segment_max_bytes = 8 * 1024;
    o
}

fn ev(t: &TenantId, id: &str, time: i64) -> Event {
    let mut e = Event::new(t.clone(), time, time);
    e.event_id = Some(id.to_owned());
    e.service = Some("svc".into());
    e.body = Some(Value::from(format!("body of {id}")));
    e.attributes.insert("k".into(), Value::Int(time));
    e
}

fn ids(engine: &Engine, t: &TenantId) -> Vec<String> {
    engine
        .snapshot(t)
        .scan_all()
        .unwrap()
        .into_iter()
        .map(|(_, _, e)| e.event_id.unwrap())
        .collect()
}

fn write_n(engine: &Engine, t: &TenantId, from: usize, n: usize) {
    for i in from..from + n {
        engine
            .write_batch(t, vec![ev(t, &format!("e{i}"), i as i64)])
            .unwrap();
    }
}

#[test]
fn write_restart_recover_from_wal_only() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        assert!(e.open_report().created);
        write_n(&e, &t, 0, 50);
        // Drop without close: simulates an unclean stop (no flush).
    }
    let e = Engine::open(opts(dir.path())).unwrap();
    assert_eq!(e.open_report().rows_recovered_from_wal, 50);
    let got = ids(&e, &t);
    assert_eq!(got.len(), 50);
    assert_eq!(got[0], "e0");
    assert_eq!(got[49], "e49");
}

#[test]
fn flush_then_restart_reads_segments_and_skips_flushed_wal() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        write_n(&e, &t, 0, 30);
        let r = e.flush().unwrap();
        assert_eq!(r.rows_flushed, 30);
        assert_eq!(r.segments_written, 1);
        write_n(&e, &t, 30, 20);
        assert_eq!(
            ids(&e, &t).len(),
            50,
            "segment + memtable visible, no duplicates"
        );
    }
    let e = Engine::open(opts(dir.path())).unwrap();
    assert_eq!(e.open_report().segments, 1);
    assert_eq!(e.open_report().rows_recovered_from_wal, 20);
    let got = ids(&e, &t);
    assert_eq!(got.len(), 50);
    assert_eq!(got.iter().collect::<BTreeSet<_>>().len(), 50);
    assert_eq!(e.verify().unwrap(), 1);
    e.close().unwrap();
    // close() flushed: everything is in segments now, nothing to replay.
    let e = Engine::open(opts(dir.path())).unwrap();
    assert_eq!(e.open_report().rows_recovered_from_wal, 0);
    assert_eq!(ids(&e, &t).len(), 50);
}

#[test]
fn wal_is_reclaimed_after_flush() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    let e = Engine::open(opts(dir.path())).unwrap();
    write_n(&e, &t, 0, 600);
    let before = e.stats().wal.segments;
    assert!(before > 2, "expected WAL rotation, got {before}");
    let r = e.flush().unwrap();
    assert!(r.wal_segments_reclaimed > 0);
    assert_eq!(e.stats().wal.segments, 1);
}

#[test]
fn tenant_isolation_in_storage() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (tenant("tenant-a"), tenant("tenant-b"));
    let e = Engine::open(opts(dir.path())).unwrap();
    write_n(&e, &a, 0, 10);
    write_n(&e, &b, 100, 5);
    e.flush().unwrap();
    write_n(&e, &a, 10, 3);
    let sa = e.snapshot(&a);
    assert!(sa.segments().iter().all(|s| s.entry.tenant == a));
    assert!(sa.memory_rows().iter().all(|r| r.event.tenant_id == a));
    assert_eq!(ids(&e, &a).len(), 13);
    assert_eq!(ids(&e, &b).len(), 5);
    assert!(ids(&e, &tenant("nobody")).is_empty());
    // Segments live in per-tenant directories.
    assert!(dir.path().join("segments/tenant-a").is_dir());
    assert!(dir.path().join("segments/tenant-b").is_dir());
    // A batch claiming another tenant is rejected before touching the WAL.
    let err = e.write_batch(&a, vec![ev(&b, "x", 1)]).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert_eq!(ids(&e, &b).len(), 5);
}

#[test]
fn batches_are_atomic_and_ordered_by_event_time() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    let e = Engine::open(opts(dir.path())).unwrap();
    let batch: Vec<Event> = (0..5).rev().map(|i| ev(&t, &format!("b{i}"), i)).collect();
    let r = e.write_batch(&t, batch).unwrap();
    assert_eq!(r.accepted, 5);
    assert_eq!(r.commit_seq, 1);
    assert_eq!(ids(&e, &t), vec!["b0", "b1", "b2", "b3", "b4"]);
    e.flush().unwrap();
    assert_eq!(ids(&e, &t), vec!["b0", "b1", "b2", "b3", "b4"]);
    assert!(e.write_batch(&t, vec![]).is_err());
}

#[test]
fn second_process_cannot_open_same_directory() {
    let dir = tempfile::tempdir().unwrap();
    let _e = Engine::open(opts(dir.path())).unwrap();
    let err = Engine::open(opts(dir.path())).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unavailable, "{err}");
}

#[test]
fn missing_manifest_with_data_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        write_n(&e, &t, 0, 3);
    }
    std::fs::remove_file(dir.path().join("MANIFEST")).unwrap();
    let err = Engine::open(opts(dir.path())).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Corruption, "{err}");
}

#[test]
fn corrupt_segment_detected_at_open_and_verify() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        write_n(&e, &t, 0, 20);
        e.close().unwrap();
    }
    let seg = std::fs::read_dir(dir.path().join("segments/a"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut b = std::fs::read(&seg).unwrap();
    b[40] ^= 0xff; // inside a column chunk: footer still valid
    std::fs::write(&seg, &b).unwrap();
    // Footer-only open succeeds; full verification and reads detect it.
    let e = Engine::open(opts(dir.path())).unwrap();
    assert_eq!(e.verify().unwrap_err().kind(), ErrorKind::Corruption);
    assert_eq!(
        e.snapshot(&t).scan_all().unwrap_err().kind(),
        ErrorKind::Corruption
    );
    drop(e);
    let mut o = opts(dir.path());
    o.verify_segments_on_open = true;
    assert_eq!(Engine::open(o).unwrap_err().kind(), ErrorKind::Corruption);
}

#[test]
fn missing_committed_segment_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        write_n(&e, &t, 0, 5);
        e.close().unwrap();
    }
    for f in std::fs::read_dir(dir.path().join("segments/a")).unwrap() {
        std::fs::remove_file(f.unwrap().path()).unwrap();
    }
    assert_eq!(
        Engine::open(opts(dir.path())).unwrap_err().kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn orphan_and_unreferenced_segments_are_cleaned_up_safely() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        write_n(&e, &t, 0, 5);
        e.close().unwrap();
    }
    let tdir = dir.path().join("segments/a");
    let real = std::fs::read_dir(&tdir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    // An orphan from a crashed flush (id >= next_segment_id) and an
    // unreferenced old id (must be quarantined, not deleted).
    std::fs::copy(&real, tdir.join(stratadb_segment::segment_file_name(99))).unwrap();
    std::fs::write(tdir.join("seg-00000000000000000099.sseg.tmp"), b"partial").unwrap();
    let m = stratadb_storage::Manifest::load(dir.path())
        .unwrap()
        .unwrap();
    let mut m2 = m.clone();
    m2.segments.clear();
    m2.version += 1;
    m2.commit(dir.path()).unwrap();
    let e = Engine::open(opts(dir.path())).unwrap();
    let r = e.open_report();
    assert_eq!(r.orphan_segments_removed, 1);
    assert_eq!(r.segments_quarantined, 1);
    assert_eq!(r.tmp_files_removed, 1);
    assert!(dir.path().join("quarantine").read_dir().unwrap().count() == 1);
}

#[test]
fn backpressure_rejects_when_flush_backlog_full() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    let mut o = opts(dir.path());
    o.max_frozen_memtables = 0; // every write sees a "full" backlog
    o.write_stall_timeout = Duration::from_millis(20);
    let e = Engine::open(o).unwrap();
    let err = e.write_batch(&t, vec![ev(&t, "x", 1)]).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResourceExhausted);
    assert!(err.is_retryable());
    assert_eq!(e.stats().write_rejections, 1);
}

#[test]
fn background_flush_by_row_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    let mut o = opts(dir.path());
    o.background = true;
    o.memtable_max_rows = 10;
    let e = Engine::open(o).unwrap();
    write_n(&e, &t, 0, 25);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while e.stats().segments == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(e.stats().segments > 0, "background flush did not run");
    assert_eq!(ids(&e, &t).len(), 25);
}

#[test]
fn async_mode_reports_weaker_commit_level() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    let mut o = opts(dir.path());
    o.wal.durability = Durability::Async;
    let e = Engine::open(o).unwrap();
    let r = e.write_batch(&t, vec![ev(&t, "x", 1)]).unwrap();
    assert_eq!(r.level.as_str(), "wal_written");
    e.sync_wal().unwrap();
}

/// Writers, a flusher, and readers run concurrently. Invariants checked:
/// * every acknowledged event is visible in every later snapshot;
/// * no snapshot ever contains a duplicate (flush transition is atomic);
/// * after restart, exactly the acknowledged set is present.
#[test]
fn concurrent_writes_flushes_and_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    let mut o = opts(dir.path());
    o.wal.durability = Durability::GroupCommit {
        max_delay: Duration::from_micros(200),
    };
    let e = Arc::new(Engine::open(o).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(parking_lot::Mutex::new(BTreeSet::<String>::new()));

    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (e, t, acked) = (Arc::clone(&e), t.clone(), Arc::clone(&acked));
            std::thread::spawn(move || {
                for i in 0..150 {
                    let batch: Vec<Event> = (0..3)
                        .map(|j| ev(&t, &format!("w{w}-{i}-{j}"), (i * 10 + j) as i64))
                        .collect();
                    let names: Vec<String> =
                        batch.iter().map(|e| e.event_id.clone().unwrap()).collect();
                    e.write_batch(&t, batch).unwrap();
                    acked.lock().extend(names);
                }
            })
        })
        .collect();
    let flusher = {
        let (e, stop) = (Arc::clone(&e), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                e.flush().unwrap();
                std::thread::sleep(Duration::from_millis(3));
            }
        })
    };
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let (e, t, stop, acked) = (
                Arc::clone(&e),
                t.clone(),
                Arc::clone(&stop),
                Arc::clone(&acked),
            );
            std::thread::spawn(move || {
                let mut checks = 0;
                while !stop.load(Ordering::Relaxed) {
                    let before: BTreeSet<String> = acked.lock().clone();
                    let rows = e.snapshot(&t).scan_all().unwrap();
                    let seen: Vec<String> = rows
                        .into_iter()
                        .map(|(_, _, e)| e.event_id.unwrap())
                        .collect();
                    let set: BTreeSet<String> = seen.iter().cloned().collect();
                    assert_eq!(set.len(), seen.len(), "duplicate row visible in snapshot");
                    assert!(
                        before.is_subset(&set),
                        "acknowledged row missing from snapshot"
                    );
                    checks += 1;
                }
                checks
            })
        })
        .collect();
    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    flusher.join().unwrap();
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
    let want = acked.lock().clone();
    assert_eq!(want.len(), 4 * 150 * 3);
    let e = Arc::try_unwrap(e).unwrap();
    drop(e);
    let e = Engine::open(opts(dir.path())).unwrap();
    let got: BTreeSet<String> = ids(&e, &t).into_iter().collect();
    assert_eq!(got, want);
}

#[test]
fn disk_reserve_pauses_writes_but_not_reads_or_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let t = tenant("a");
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        write_n(&e, &t, 0, 5);
    }
    let mut o = opts(dir.path());
    o.disk_reserve_bytes = u64::MAX; // simulate a nearly full volume
    let e = Engine::open(o).unwrap();
    let err = e.write_batch(&t, vec![ev(&t, "blocked", 1)]).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResourceExhausted, "{err}");
    assert!(err.is_retryable());
    assert!(
        e.failure().is_none(),
        "a paused write must not fail the engine"
    );
    assert_eq!(e.stats().disk_write_rejections, 1);
    // Reads and flushes still work (flushing frees WAL space).
    assert_eq!(ids(&e, &t).len(), 5);
    e.flush().unwrap();
    assert!(e.stats().disk_free_bytes > 0);
    drop(e);
    // Space "freed": writes resume.
    let e = Engine::open(opts(dir.path())).unwrap();
    e.write_batch(&t, vec![ev(&t, "ok", 2)]).unwrap();
}
