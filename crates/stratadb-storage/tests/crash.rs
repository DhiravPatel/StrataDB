//! Process-crash recovery tests (spec §26.5, ticket C-008).
//!
//! Each case re-executes this test binary as a child running `crash_child`
//! with one failpoint configured to terminate the process at a specific
//! transition. The child prints `ACK <batch>` after each acknowledged write.
//! The parent then recovers the data directory and checks the durability
//! contract:
//!
//! 1. every acknowledged batch is present, exactly once;
//! 2. batches are atomic (all events of a batch, or none);
//! 3. at most the single in-flight (unacknowledged) batch may additionally appear;
//! 4. recovery is idempotent (a second recovery yields the same rows);
//! 5. the recovered engine accepts writes, flushes, and passes full verification.
//!
//! Scope: these simulate **process crashes** — data handed to the kernel
//! survives. Power-loss simulation (dropping unsynced page-cache data) needs a
//! fault-injecting filesystem layer and is tracked in docs/project/KNOWN_LIMITATIONS.md.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::Command;

use stratadb_common::TenantId;
use stratadb_schema::{Event, Value};
use stratadb_storage::{Durability, Engine, EngineOptions, IdempotencyKey};

const DIR_ENV: &str = "STRATADB_CRASH_DIR";

/// Cases run serially: a child forked by one test thread briefly inherits the
/// parent's open lock-file description (until exec closes it), which can make
/// a concurrent in-process reopen observe the data-dir lock as held.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
const BATCHES: usize = 60;
const PER_BATCH: usize = 3;

fn opts(dir: &Path) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Sync;
    o.wal.segment_max_bytes = 2048; // frequent WAL rotation
    o.flush_on_close = false;
    o.compaction.min_inputs = 2;
    o
}

fn tenant() -> TenantId {
    TenantId::new("crash").unwrap()
}

/// Deterministic idempotency key for batch `b` (digest stands in for the
/// client's request hash).
fn key(b: usize) -> Option<IdempotencyKey> {
    Some(IdempotencyKey::new(format!("batch-{b}"), [b as u8; 32]).expect("key"))
}

fn event(batch: usize, j: usize) -> Event {
    let mut e = Event::new(tenant(), (batch * 10 + j) as i64, 0);
    e.event_id = Some(format!("b{batch}-{j}"));
    e.body = Some(Value::from(format!(
        "payload {batch} {j} {}",
        "x".repeat(40)
    )));
    e
}

/// Child workload. A no-op unless launched by `run_case`.
#[test]
fn crash_child() {
    let Ok(dir) = std::env::var(DIR_ENV) else {
        return;
    };
    let e = Engine::open(opts(Path::new(&dir))).expect("child open");
    for b in 0..BATCHES {
        let batch = (0..PER_BATCH).map(|j| event(b, j)).collect();
        e.write_batch_idempotent(&tenant(), batch, key(b))
            .expect("child write");
        println!("ACK {b}");
        if b % 7 == 6 {
            e.flush().expect("child flush");
        }
        if b % 14 == 13 {
            e.compact().expect("child compact");
        }
    }
    println!("DONE");
}

fn recovered(dir: &Path) -> BTreeMap<usize, BTreeSet<usize>> {
    let e = Engine::open(opts(dir)).expect("recovery open");
    let rows = e.snapshot(&tenant()).scan_all().expect("scan");
    let mut seen = BTreeSet::new();
    let mut by_batch: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    for (_, _, ev) in rows {
        let id = ev.event_id.expect("event id");
        assert!(
            seen.insert(id.clone()),
            "duplicate event {id} after recovery"
        );
        let (b, j) = id[1..].split_once('-').expect("id format");
        by_batch
            .entry(b.parse().expect("batch"))
            .or_default()
            .insert(j.parse().expect("idx"));
    }
    by_batch
}

fn run_case(failpoint: &str) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::current_exe().unwrap();
    let out = Command::new(exe)
        .args(["crash_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(DIR_ENV, dir.path())
        .env("STRATADB_FAILPOINTS", failpoint)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        !stdout.contains("DONE"),
        "{failpoint}: failpoint never fired (child completed)\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        out.status.code(),
        Some(stratadb_common::failpoint::EXIT_CODE),
        "{failpoint}: child did not exit via failpoint\nstderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let acked: BTreeSet<usize> = stdout
        .lines()
        // libtest may print "test crash_child ... " on the same line as the first ACK.
        .filter_map(|l| l.find("ACK ").map(|p| &l[p + 4..]))
        .map(|n| n.trim().parse().unwrap())
        .collect();
    let max_acked = acked.iter().max().copied();

    let got = recovered(dir.path());
    let full: BTreeSet<usize> = (0..PER_BATCH).collect();
    for b in &acked {
        assert_eq!(
            got.get(b),
            Some(&full),
            "{failpoint}: acknowledged batch {b} lost or partial"
        );
    }
    for (b, idxs) in &got {
        assert_eq!(
            idxs, &full,
            "{failpoint}: batch {b} is partial (atomicity violated)"
        );
        let in_flight = max_acked.map_or(0, |m| m + 1);
        assert!(
            acked.contains(b) || *b == in_flight,
            "{failpoint}: unexpected batch {b} (acked through {max_acked:?})"
        );
    }

    // Idempotent recovery.
    assert_eq!(
        recovered(dir.path()),
        got,
        "{failpoint}: second recovery differs"
    );

    // Client retries after the crash: every acknowledged batch must be
    // recognised as a duplicate (dedup state recovered from WAL/segments), and
    // retrying the in-flight batch must leave exactly one copy.
    let e = Engine::open(opts(dir.path())).unwrap();
    let upto = max_acked.map_or(0, |m| m + 1);
    for b in 0..=upto {
        let batch = (0..PER_BATCH).map(|j| event(b, j)).collect();
        let r = e.write_batch_idempotent(&tenant(), batch, key(b)).unwrap();
        if acked.contains(&b) || got.contains_key(&b) {
            assert!(
                r.duplicate,
                "{failpoint}: retry of committed batch {b} was not deduplicated"
            );
        }
    }
    e.flush().unwrap();
    e.verify().unwrap();
    drop(e);
    let after = recovered(dir.path());
    assert_eq!(
        after.len(),
        upto + 1,
        "{failpoint}: retries produced wrong batch set"
    );

    // The recovered database remains fully usable for new writes.
    let e = Engine::open(opts(dir.path())).unwrap();
    e.write_batch(&tenant(), vec![event(1000, 0)]).unwrap();
    e.flush().unwrap();
    assert_eq!(
        e.snapshot(&tenant()).scan_all().unwrap().len(),
        (upto + 1) * PER_BATCH + 1,
        "{failpoint}: row count after post-recovery write"
    );
}

macro_rules! crash_cases {
    ($($name:ident => $fp:expr,)*) => {$(
        #[test]
        fn $name() {
            run_case($fp);
        }
    )*};
}

crash_cases! {
    crash_before_wal_append      => "wal::before_write=exit@10",
    crash_during_wal_append      => "wal::torn_write=exit@10",
    crash_after_append_before_sync => "wal::before_sync=exit@10",
    crash_after_sync_before_ack  => "wal::after_sync=exit@10",
    crash_before_memtable_insert => "engine::after_wal_commit=exit@10",
    crash_during_wal_rotation    => "wal::rotate::after_create=exit@3",
    crash_after_freeze           => "flush::after_freeze=exit@2",
    crash_during_segment_write   => "segment::torn_write=exit@2",
    crash_after_segment_before_manifest => "flush::after_segment_write=exit@2",
    crash_during_manifest_update => "manifest::before_rename=exit@3",
    crash_after_manifest_rename_before_dir_sync => "manifest::after_rename=exit@3",
    crash_after_manifest_before_wal_reclaim => "flush::after_manifest_commit=exit@2",
    crash_during_wal_reclamation => "wal::reclaim::after_delete=exit@1",
    crash_first_flush_manifest   => "manifest::before_rename=exit@2",
    crash_late_wal_append        => "wal::torn_write=exit@55",
    crash_after_compaction_output_before_manifest => "compaction::after_output_write=exit@1",
    crash_after_compaction_manifest_commit => "compaction::after_manifest_commit=exit@1",
    crash_during_obsolete_deletion => "compaction::after_delete_obsolete=exit@1",
    crash_during_second_compaction => "compaction::after_output_write=exit@2",
}

/// Sanity check: without a failpoint the child completes normally, so a
/// failpoint that "never fired" is a real test failure, not a silent pass.
#[test]
fn child_completes_without_failpoints() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["crash_child", "--exact", "--nocapture", "--test-threads=1"])
        .env(DIR_ENV, dir.path())
        .env_remove("STRATADB_FAILPOINTS")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("DONE"));
    let got = recovered(dir.path());
    assert_eq!(got.len(), BATCHES);
}
