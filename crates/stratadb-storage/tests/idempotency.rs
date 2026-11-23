//! Idempotent writes through the engine (ticket G-006).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use stratadb_common::{ErrorKind, TenantId};
use stratadb_schema::Event;
use stratadb_storage::{Durability, Engine, EngineOptions, IdempotencyKey};

fn t() -> TenantId {
    TenantId::new("acme").unwrap()
}

fn opts(dir: &Path) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Sync;
    o.flush_on_close = false;
    o
}

fn batch(tag: &str, n: usize) -> Vec<Event> {
    (0..n)
        .map(|i| {
            let mut e = Event::new(t(), i as i64, 0);
            e.event_id = Some(format!("{tag}-{i}"));
            e
        })
        .collect()
}

fn key(k: &str, d: u8) -> Option<IdempotencyKey> {
    Some(IdempotencyKey::new(k, [d; 32]).unwrap())
}

fn rows(e: &Engine) -> usize {
    e.snapshot(&t()).scan_all().unwrap().len()
}

#[test]
fn retries_are_deduplicated_across_flush_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let first;
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        first = e
            .write_batch_idempotent(&t(), batch("a", 3), key("k1", 1))
            .unwrap();
        assert!(!first.duplicate);
        // Same key + digest → original receipt, nothing written.
        let again = e
            .write_batch_idempotent(&t(), batch("a", 3), key("k1", 1))
            .unwrap();
        assert!(again.duplicate);
        assert_eq!((again.commit_seq, again.accepted), (first.commit_seq, 3));
        assert_eq!(rows(&e), 3);
        // Different digest → conflict, not stored.
        let err = e
            .write_batch_idempotent(&t(), batch("x", 1), key("k1", 2))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AlreadyExists);
        assert!(!err.is_retryable());
        assert_eq!(e.stats().duplicates_suppressed, 1);
    }
    // Restart: dedup state recovered from the WAL record.
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        let r = e
            .write_batch_idempotent(&t(), batch("a", 3), key("k1", 1))
            .unwrap();
        assert!(r.duplicate && r.commit_seq == first.commit_seq);
        // Flush: WAL is reclaimed, metadata now lives in the segment footer.
        e.flush().unwrap();
        assert!(
            e.write_batch_idempotent(&t(), batch("a", 3), key("k1", 1))
                .unwrap()
                .duplicate
        );
        e.write_batch_idempotent(&t(), batch("b", 2), key("k2", 1))
            .unwrap();
        e.flush().unwrap();
    }
    // Restart again: recovered from segment footers only.
    let e = Engine::open(opts(dir.path())).unwrap();
    assert_eq!(e.open_report().rows_recovered_from_wal, 0);
    assert!(
        e.write_batch_idempotent(&t(), batch("a", 3), key("k1", 1))
            .unwrap()
            .duplicate
    );
    assert!(
        e.write_batch_idempotent(&t(), batch("b", 2), key("k2", 1))
            .unwrap()
            .duplicate
    );
    assert_eq!(rows(&e), 5);
    assert_eq!(e.stats().idempotency_keys, 2);
    // Keys are tenant-scoped.
    let other = TenantId::new("other").unwrap();
    let mut ev = Event::new(other.clone(), 0, 0);
    ev.event_id = Some("o".into());
    assert!(
        !e.write_batch_idempotent(&other, vec![ev], key("k1", 1))
            .unwrap()
            .duplicate
    );
}

#[test]
fn window_expiry_and_capacity_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.idempotency_window = Duration::from_millis(50);
    let e = Engine::open(o).unwrap();
    e.write_batch_idempotent(&t(), batch("a", 1), key("w", 1))
        .unwrap();
    std::thread::sleep(Duration::from_millis(80));
    // Expired: accepted as a new write (documented at-least-once beyond window).
    assert!(
        !e.write_batch_idempotent(&t(), batch("a", 1), key("w", 1))
            .unwrap()
            .duplicate
    );
    drop(e);

    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.idempotency_max_keys = 2;
    let e = Engine::open(o).unwrap();
    for k in ["k1", "k2", "k3"] {
        e.write_batch_idempotent(&t(), batch(k, 1), key(k, 1))
            .unwrap();
    }
    assert_eq!(e.stats().idempotency_evictions, 1);
    assert!(
        !e.write_batch_idempotent(&t(), batch("k1", 1), key("k1", 1))
            .unwrap()
            .duplicate
    );
    assert!(
        e.write_batch_idempotent(&t(), batch("k3", 1), key("k3", 1))
            .unwrap()
            .duplicate
    );
}

#[test]
fn concurrent_same_key_commits_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(dir.path());
    o.wal.durability = Durability::GroupCommit {
        max_delay: Duration::from_millis(2),
    };
    let e = Arc::new(Engine::open(o).unwrap());
    let hs: Vec<_> = (0..8)
        .map(|_| {
            let e = Arc::clone(&e);
            std::thread::spawn(move || loop {
                match e.write_batch_idempotent(&t(), batch("c", 4), key("same", 7)) {
                    Ok(r) => return r,
                    Err(err) if err.kind() == ErrorKind::Unavailable => std::thread::yield_now(),
                    Err(err) => panic!("{err}"),
                }
            })
        })
        .collect();
    let receipts: Vec<_> = hs.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(receipts.iter().filter(|r| !r.duplicate).count(), 1);
    assert!(receipts
        .windows(2)
        .all(|w| w[0].commit_seq == w[1].commit_seq));
    assert_eq!(rows(&e), 4);
}
