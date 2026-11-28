//! WAL segment lifecycle (spec IX.22.3, STRATA-ARCH-22-003): created segments
//! are PREALLOCATED then ACTIVE, rotation seals them, reclamation
//! checkpoints and deletes them, and a poisoned WAL fails its active segment.
//!
//! One test: the state counters are process-wide.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::sync::atomic::Ordering;

use stratadb_common::failpoint;
use stratadb_common::state::{State, WalSegmentState as W};
use stratadb_wal::{Durability, RecordType, Wal, WalOptions};

fn left(s: W) -> u64 {
    W::stats().seconds[s.index()].snapshot().count
}

fn now_in(s: W) -> i64 {
    W::stats().entities[s.index()].load(Ordering::Relaxed)
}

fn open(dir: &std::path::Path, covered: u64) -> Wal {
    Wal::open(
        dir,
        WalOptions {
            durability: Durability::Sync,
            segment_max_bytes: 4096,
            ..WalOptions::default()
        },
        covered,
        |_, _, _| Ok(()),
    )
    .unwrap()
    .0
}

#[test]
fn segments_walk_their_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let wal = open(dir.path(), 0);
    // A fresh WAL creates its first segment.
    assert_eq!((left(W::Preallocated), now_in(W::Active)), (1, 1));
    let payload = vec![7u8; 1500];
    let mut last = 0;
    for _ in 0..12 {
        last = wal.append(RecordType::EventBatch, &payload).unwrap();
    }
    let rotations = left(W::Active);
    assert!(rotations >= 3, "{rotations} rotations");
    assert_eq!(left(W::Preallocated), rotations + 1);
    assert_eq!(now_in(W::Sealed), rotations as i64);
    assert_eq!(now_in(W::Active), 1);
    // Everything flushed: every sealed segment is checkpointed and deleted.
    let n = wal.reclaim_through(last).unwrap() as u64;
    assert_eq!(n, rotations);
    assert_eq!((left(W::Sealed), left(W::Checkpointed)), (n, n));
    assert_eq!((now_in(W::Sealed), now_in(W::Checkpointed)), (0, 0));
    assert_eq!(now_in(W::Deleted), 0, "deleted segments are not entities");
    drop(wal);
    assert_eq!(now_in(W::Active), 0);

    // Reopen: the last segment is reused (ACTIVE); older ones come back
    // SEALED. (The reclaimed records are covered by the "manifest".)
    let wal = open(dir.path(), last);
    assert_eq!((now_in(W::Active), now_in(W::Sealed)), (1, 0));
    wal.append(RecordType::EventBatch, &payload).unwrap();
    wal.append(RecordType::EventBatch, &payload).unwrap();
    wal.append(RecordType::EventBatch, &payload).unwrap();
    assert!(now_in(W::Sealed) >= 1);
    drop(wal);
    let wal = open(dir.path(), last);
    assert!(
        now_in(W::Sealed) >= 1,
        "closed segments found at open are SEALED"
    );

    // A failed fsync poisons the WAL and fails its active segment.
    assert!(failpoint::set("wal::before_sync", "error"));
    assert!(wal.append(RecordType::EventBatch, b"x").is_err());
    failpoint::clear_all();
    assert_eq!((now_in(W::Active), now_in(W::Failed)), (0, 1));
    assert_eq!(W::stats().illegal.load(Ordering::Relaxed), 0);
}
