//! Compaction and retention (spec §16).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::path::Path;

use proptest::prelude::*;
use stratadb_common::{time, TenantId};
use stratadb_schema::Event;
use stratadb_storage::{Durability, Engine, EngineOptions, IdempotencyKey, RetentionPolicy};

fn t(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn opts(dir: &Path) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    o.flush_on_close = false;
    o.compaction.min_inputs = 2;
    o
}

fn ev(tenant: &TenantId, id: &str, time_ns: i64) -> Event {
    let mut e = Event::new(tenant.clone(), time_ns, time_ns);
    e.event_id = Some(id.to_owned());
    e
}

fn ids(e: &Engine, tenant: &TenantId) -> Vec<String> {
    e.snapshot(tenant)
        .scan_all()
        .unwrap()
        .into_iter()
        .map(|(_, _, ev)| ev.event_id.unwrap())
        .collect()
}

fn seg_files(dir: &Path, tenant: &str) -> usize {
    std::fs::read_dir(dir.join("segments").join(tenant)).map_or(0, |d| d.count())
}

/// Write `flushes` flushes of `per` events each for `tenant`.
fn load(e: &Engine, tenant: &TenantId, flushes: usize, per: usize) {
    for f in 0..flushes {
        let batch = (0..per)
            .map(|i| ev(tenant, &format!("{f}-{i}"), (f * per + i) as i64))
            .collect();
        e.write_batch(tenant, batch).unwrap();
        e.flush().unwrap();
    }
}

#[test]
fn compaction_merges_and_preserves_results() {
    let dir = tempfile::tempdir().unwrap();
    let a = t("a");
    let e = Engine::open(opts(dir.path())).unwrap();
    load(&e, &a, 6, 10);
    assert_eq!(e.stats().segments, 6);
    let before = ids(&e, &a);
    let r = e.compact().unwrap();
    assert_eq!((r.groups, r.inputs, r.outputs, r.rows), (1, 6, 1, 60));
    assert_eq!(r.obsolete_deleted, 6, "no snapshot holds the inputs");
    assert_eq!(e.stats().segments, 1);
    assert_eq!(ids(&e, &a), before);
    assert_eq!(seg_files(dir.path(), "a"), 1);
    e.verify().unwrap();
    let m = stratadb_storage::Manifest::load(dir.path())
        .unwrap()
        .unwrap();
    assert_eq!(m.segments[0].lineage.len(), 6);
    assert!(m.obsolete_segments.is_empty());
    // Nothing more to do.
    assert_eq!(e.compact().unwrap().groups, 0);
    drop(e);
    let e = Engine::open(opts(dir.path())).unwrap();
    assert_eq!(ids(&e, &a), before);
}

#[test]
fn snapshots_pin_obsolete_segments() {
    let dir = tempfile::tempdir().unwrap();
    let a = t("a");
    let e = Engine::open(opts(dir.path())).unwrap();
    load(&e, &a, 3, 5);
    let snap = e.snapshot(&a);
    let r = e.compact().unwrap();
    assert_eq!(
        r.obsolete_deleted, 0,
        "inputs are still referenced by a snapshot"
    );
    assert_eq!(e.stats().obsolete_segments, 3);
    assert_eq!(seg_files(dir.path(), "a"), 4);
    // The old snapshot still reads its (now obsolete) segments.
    assert_eq!(snap.scan_all().unwrap().len(), 15);
    // A crash here must not resurrect obsolete segments as visible data.
    let m = stratadb_storage::Manifest::load(dir.path())
        .unwrap()
        .unwrap();
    assert_eq!(m.obsolete_segments.len(), 3);
    drop(snap);
    assert_eq!(e.compact().unwrap().obsolete_deleted, 3);
    assert_eq!(seg_files(dir.path(), "a"), 1);
    assert_eq!(ids(&e, &a).len(), 15);
}

#[test]
fn recovery_deletes_recorded_obsolete_segments() {
    let dir = tempfile::tempdir().unwrap();
    let a = t("a");
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        load(&e, &a, 3, 5);
        let _pin = e.snapshot(&a);
        e.compact().unwrap();
        // "Crash" with obsolete files still on disk.
        std::mem::forget(_pin);
        std::mem::forget(e);
    }
    std::fs::remove_file(dir.path().join("LOCK")).ok();
    let e = Engine::open(opts(dir.path())).unwrap();
    assert_eq!(e.open_report().obsolete_deleted, 3);
    assert_eq!(e.open_report().segments_quarantined, 0);
    assert_eq!(seg_files(dir.path(), "a"), 1);
    assert_eq!(ids(&e, &a).len(), 15);
}

#[test]
fn idempotency_keys_survive_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let a = t("a");
    let key = |i: u8| Some(IdempotencyKey::new(format!("k{i}"), [i; 32]).unwrap());
    {
        let e = Engine::open(opts(dir.path())).unwrap();
        for i in 0..4u8 {
            e.write_batch_idempotent(&a, vec![ev(&a, &format!("x{i}"), i64::from(i))], key(i))
                .unwrap();
            e.flush().unwrap();
        }
        assert_eq!(e.compact().unwrap().inputs, 4);
    }
    let e = Engine::open(opts(dir.path())).unwrap();
    for i in 0..4u8 {
        let r = e
            .write_batch_idempotent(&a, vec![ev(&a, &format!("x{i}"), i64::from(i))], key(i))
            .unwrap();
        assert!(r.duplicate, "key k{i} forgotten after compaction");
    }
    assert_eq!(ids(&e, &a).len(), 4);
}

#[test]
fn retention_drops_rewrites_and_respects_hold() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (t("a"), t("b"));
    let hour_ns = 3_600_000_000_000i64;
    let now_ms = 1_000 * 3_600_000; // hour 1000
    let e = Engine::open(opts(dir.path())).unwrap();
    // Segment 1: hours 990..991 (expired), segment 2: hours 997..999 (straddles a 2h cutoff at 998).
    e.write_batch(
        &a,
        (0..2)
            .map(|i| ev(&a, &format!("old{i}"), (990 + i) * hour_ns))
            .collect(),
    )
    .unwrap();
    e.flush().unwrap();
    e.write_batch(
        &a,
        (0..3)
            .map(|i| ev(&a, &format!("mid{i}"), (997 + i) * hour_ns))
            .collect(),
    )
    .unwrap();
    e.flush().unwrap();
    e.write_batch(&b, vec![ev(&b, "b-old", 990 * hour_ns)])
        .unwrap();
    e.flush().unwrap();

    let policy = RetentionPolicy {
        max_age_ms: 2 * 3_600_000,
        hold: false,
    };
    e.set_retention(&a, Some(policy)).unwrap();
    let r = e.enforce_retention(now_ms).unwrap();
    assert_eq!(
        (r.segments_dropped, r.segments_rewritten, r.rows_deleted),
        (1, 1, 3)
    );
    assert_eq!(ids(&e, &a), vec!["mid1", "mid2"]);
    assert_eq!(
        ids(&e, &b),
        vec!["b-old"],
        "tenant without a policy is untouched"
    );
    e.verify().unwrap();

    // Legal hold stops deletion.
    e.write_batch(&a, vec![ev(&a, "old-again", 900 * hour_ns)])
        .unwrap();
    e.flush().unwrap();
    e.set_retention(
        &a,
        Some(RetentionPolicy {
            hold: true,
            ..policy
        }),
    )
    .unwrap();
    let r = e.enforce_retention(now_ms).unwrap();
    assert_eq!((r.tenants_on_hold, r.rows_deleted), (1, 0));
    assert!(ids(&e, &a).contains(&"old-again".to_owned()));
    drop(e);
    // Policies are durable.
    let e = Engine::open(opts(dir.path())).unwrap();
    assert!(e.retention_policies()[&a].hold);
    assert!(e
        .set_retention(
            &a,
            Some(RetentionPolicy {
                max_age_ms: 0,
                hold: false
            })
        )
        .is_err());
    let _ = time::now_unix_millis();
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    /// Compaction never changes the visible row set, for any flush pattern.
    #[test]
    fn compaction_is_result_preserving(sizes in proptest::collection::vec(1usize..20, 2..8), times in proptest::collection::vec(any::<i32>(), 200)) {
        let dir = tempfile::tempdir().unwrap();
        let a = t("a");
        let e = Engine::open(opts(dir.path())).unwrap();
        let mut n = 0;
        for s in &sizes {
            let batch = (0..*s).map(|i| { n += 1; ev(&a, &format!("e{n}"), i64::from(times[(n + i) % times.len()])) }).collect();
            e.write_batch(&a, batch).unwrap();
            e.flush().unwrap();
        }
        let before: BTreeSet<String> = ids(&e, &a).into_iter().collect();
        while e.compact().unwrap().groups > 0 {}
        let after: Vec<String> = ids(&e, &a);
        prop_assert_eq!(after.len(), before.len());
        prop_assert_eq!(after.into_iter().collect::<BTreeSet<_>>(), before);
        prop_assert_eq!(e.stats().segments, 1);
        e.verify().unwrap();
    }
}
