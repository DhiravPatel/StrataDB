//! Write-path I/O faults through the real engine (spec STRATA-SYS-04-002,
//! 04-009, 04-026; ADR-0027): short writes are completed, a WAL fsync
//! failure fails the engine closed without losing acknowledged data, a disk
//! full during flush is retried, and a failed manifest commit fails stop. In
//! every case reopening with a healthy device recovers exactly the
//! acknowledged data.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::sync::Arc;

use stratadb_common::io::{Fault, FaultyIo, IoEngine, PreadEngine};
use stratadb_common::{ErrorKind, TenantId};
use stratadb_schema::Event;
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("wf").unwrap()
}

fn opts(dir: &Path, io: Arc<dyn IoEngine>) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Sync;
    o.io = Some(io);
    o
}

fn batch(from: i64, n: i64) -> Vec<Event> {
    (from..from + n)
        .map(|i| Event::new(tenant(), 1_795_170_600_000_000_000 + i, 1))
        .collect()
}

/// Rows visible to a fresh snapshot (segments + memtable).
fn rows(e: &Engine) -> u64 {
    let s = e.snapshot(&tenant());
    s.segments()
        .iter()
        .map(|h| h.reader.footer().row_count)
        .sum::<u64>()
        + s.memory_row_count() as u64
}

fn reopen(dir: &Path) -> Engine {
    Engine::open(opts(dir, Arc::new(PreadEngine::new(64)))).unwrap()
}

#[test]
fn short_writes_everywhere_are_completed() {
    let dir = tempfile::tempdir().unwrap();
    let io = Arc::new(FaultyIo::new(Arc::new(PreadEngine::new(64))));
    let e = Engine::open(opts(dir.path(), io.clone())).unwrap();
    // WAL frames, segment files and manifests all see short writes.
    io.inject(None, Fault::ShortWrite, 10_000);
    for b in 0..10 {
        e.write_batch(&tenant(), batch(b * 50, 50)).unwrap();
        if b % 3 == 2 {
            e.flush().unwrap();
        }
    }
    assert_eq!(rows(&e), 500);
    assert!(e.failure().is_none());
    drop(e);
    assert_eq!(rows(&reopen(dir.path())), 500);
    // Recovery reads (manifest load, WAL replay) go through the engine too:
    // short reads there are completed, a persistent read error is reported.
    let rio = Arc::new(FaultyIo::new(Arc::new(PreadEngine::new(64))));
    rio.inject(Some(".log"), Fault::ShortRead, 1_000);
    rio.inject(Some("MANIFEST"), Fault::ShortRead, 1_000);
    let e = Engine::open(opts(dir.path(), rio.clone())).unwrap();
    assert_eq!(rows(&e), 500);
    assert!(rio.stats().short_reads_resubmitted > 0);
    drop(e);
    let bad = Arc::new(FaultyIo::new(Arc::new(PreadEngine::new(64))));
    bad.inject(Some("MANIFEST"), Fault::ReadEio, 1_000);
    let err = Engine::open(opts(dir.path(), bad)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io, "{err}");
    assert!(err.to_string().contains("MANIFEST"), "{err}");
}

#[test]
fn wal_fsync_failure_fails_closed_and_loses_no_acknowledged_data() {
    let dir = tempfile::tempdir().unwrap();
    let io = Arc::new(FaultyIo::new(Arc::new(PreadEngine::new(64))));
    let e = Engine::open(opts(dir.path(), io.clone())).unwrap();
    for b in 0..5 {
        e.write_batch(&tenant(), batch(b * 10, 10)).unwrap();
    }
    io.inject(Some(".log"), Fault::FsyncEio, 1);
    let err = e.write_batch(&tenant(), batch(1000, 10)).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Indeterminate, "{err}");
    // Fail-stop: no further acknowledgements (STRATA-SYS-04-026).
    assert!(e.failure().is_some());
    assert!(e.write_batch(&tenant(), batch(2000, 10)).is_err());
    drop(e);
    // Recovery: the 50 acknowledged rows; the failed batch may or may not
    // survive (it was written before the fsync failed), never partially.
    let n = rows(&reopen(dir.path()));
    assert!(n == 50 || n == 60, "{n}");
}

#[test]
fn disk_full_during_flush_is_retried_without_data_loss() {
    let dir = tempfile::tempdir().unwrap();
    let io = Arc::new(FaultyIo::new(Arc::new(PreadEngine::new(64))));
    let e = Engine::open(opts(dir.path(), io.clone())).unwrap();
    e.write_batch(&tenant(), batch(0, 100)).unwrap();
    io.inject(Some(".sseg.tmp"), Fault::Enospc, 1);
    let err = e.flush().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResourceExhausted, "{err}");
    // Not fail-stop: the rows are still served and the next flush works.
    assert!(e.failure().is_none(), "{:?}", e.failure());
    assert_eq!(rows(&e), 100);
    e.flush().unwrap();
    assert_eq!(e.stats().segments, 1);
    // No leftover temporary segment files.
    let leftovers: Vec<_> = walk(dir.path())
        .into_iter()
        .filter(|p| p.ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    drop(e);
    assert_eq!(rows(&reopen(dir.path())), 100);
}

#[test]
fn manifest_commit_failure_fails_stop_and_recovers_from_the_wal() {
    let dir = tempfile::tempdir().unwrap();
    let io = Arc::new(FaultyIo::new(Arc::new(PreadEngine::new(64))));
    let e = Engine::open(opts(dir.path(), io.clone())).unwrap();
    e.write_batch(&tenant(), batch(0, 40)).unwrap();
    e.flush().unwrap();
    e.write_batch(&tenant(), batch(100, 40)).unwrap();
    io.inject(Some("MANIFEST.tmp"), Fault::FsyncEio, 1);
    let err = e.flush().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Indeterminate, "{err}");
    assert!(e.failure().is_some(), "manifest state unknown: fail stop");
    drop(e);
    // The old manifest is authoritative; the second batch replays from the
    // WAL; the unpublished segment is removed as an orphan.
    let e = reopen(dir.path());
    assert_eq!(rows(&e), 80);
    e.verify().unwrap();
}

fn walk(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let p = entry.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p.to_string_lossy().into_owned());
        }
    }
    out
}
