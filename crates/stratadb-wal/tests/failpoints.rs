//! In-process error-injection tests. Failpoints are process-global, so every
//! test here serializes on `LOCK`.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::sync::Mutex;

use stratadb_common::{failpoint, ErrorKind};
use stratadb_wal::{Durability, RecordType, Wal, WalOptions};

static LOCK: Mutex<()> = Mutex::new(());

fn open(dir: &std::path::Path) -> (Wal, Vec<u64>) {
    let mut seqs = Vec::new();
    let (wal, _) = Wal::open(
        dir,
        WalOptions {
            durability: Durability::Sync,
            ..WalOptions::default()
        },
        0,
        |s, _, _| {
            seqs.push(s);
            Ok(())
        },
    )
    .unwrap();
    (wal, seqs)
}

#[test]
fn sync_failure_poisons_wal_and_reports_indeterminate() {
    let _g = LOCK.lock().unwrap();
    assert!(failpoint::enabled());
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(dir.path());
    wal.append(RecordType::EventBatch, b"one").unwrap();
    assert!(failpoint::set("wal::before_sync", "error"));
    let err = wal.append(RecordType::EventBatch, b"two").unwrap_err();
    failpoint::clear_all();
    assert_eq!(err.kind(), ErrorKind::Indeterminate, "{err}");
    assert!(err.is_retryable());
    assert!(wal.is_poisoned());
    // Poisoned WAL refuses all further work, even though the fault is gone.
    let err = wal.append(RecordType::EventBatch, b"three").unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Unavailable);
    assert!(wal.sync().is_err());
    drop(wal);
    // Recovery: record 2 reached the file, so it is replayed (at-least-once;
    // the client was told the outcome was indeterminate).
    let (_, seqs) = open(dir.path());
    assert_eq!(seqs, vec![1, 2]);
}

#[test]
fn write_failure_poisons_wal() {
    let _g = LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (wal, _) = open(dir.path());
    assert!(failpoint::set("wal::after_write", "error@2"));
    wal.append(RecordType::EventBatch, b"one").unwrap();
    let err = wal.append(RecordType::EventBatch, b"two").unwrap_err();
    failpoint::clear_all();
    assert_eq!(err.kind(), ErrorKind::Indeterminate);
    assert!(wal.append(RecordType::EventBatch, b"x").is_err());
}
