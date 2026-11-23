//! Backup / restore drills (spec §19.4). "A backup feature is not complete
//! until restore drills pass."

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use stratadb_common::{ErrorKind, TenantId};
use stratadb_schema::Event;
use stratadb_storage::{restore_backup, verify_backup, Durability, Engine, EngineOptions};

/// Failpoints are process-global; serialize every test in this binary.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn t(s: &str) -> TenantId {
    TenantId::new(s).unwrap()
}

fn opts(dir: &Path) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::GroupCommit {
        max_delay: std::time::Duration::from_micros(200),
    };
    o.compaction.min_inputs = 2;
    o
}

fn ev(tenant: &TenantId, id: String) -> Event {
    let mut e = Event::new(tenant.clone(), 0, 0);
    e.event_id = Some(id);
    e
}

fn ids(e: &Engine, tenant: &TenantId) -> BTreeSet<String> {
    e.snapshot(tenant)
        .scan_all()
        .unwrap()
        .into_iter()
        .map(|(_, _, ev)| ev.event_id.unwrap())
        .collect()
}

#[test]
fn backup_under_load_restores_consistent_point() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let (a, b) = (t("a"), t("b"));
    let e = Arc::new(Engine::open(opts(&root.path().join("live"))).unwrap());
    for i in 0..5 {
        e.write_batch(&a, (0..20).map(|j| ev(&a, format!("a{i}-{j}"))).collect())
            .unwrap();
        e.write_batch(&b, vec![ev(&b, format!("b{i}"))]).unwrap();
        e.flush().unwrap();
    }
    let acked_before: BTreeSet<String> = ids(&e, &a);
    // Concurrent writer + compaction while the backup runs.
    let stop = Arc::new(AtomicBool::new(false));
    let bg = {
        let (e, a, stop) = (Arc::clone(&e), a.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut n = 0;
            while !stop.load(Ordering::Relaxed) {
                e.write_batch(&a, vec![ev(&a, format!("late{n}"))]).unwrap();
                if n % 10 == 0 {
                    e.flush().unwrap();
                    e.compact().unwrap();
                }
                n += 1;
            }
        })
    };
    let dest = root.path().join("backup-1");
    let report = e.backup(&dest).unwrap();
    stop.store(true, Ordering::Relaxed);
    bg.join().unwrap();
    assert!(report.rows >= 105, "{report:?}");

    let bm = verify_backup(&dest).unwrap();
    assert_eq!(bm.manifest_version, report.manifest_version);
    assert!(!dest.join("auth").exists(), "tokens must not be in backups");

    // Restore to a clean directory ("different machine").
    let target = root.path().join("restored");
    let r = restore_backup(&dest, &target).unwrap();
    assert_eq!(r.rows, report.rows);
    let re = Engine::open(opts(&target)).unwrap();
    let restored = ids(&re, &a);
    assert!(
        acked_before.is_subset(&restored),
        "acknowledged-before-backup data missing"
    );
    assert_eq!(ids(&re, &b).len(), 5);
    assert_eq!(re.open_report().rows_recovered_from_wal, 0);
    re.verify().unwrap();
    // The restored database accepts writes and can itself be backed up.
    re.write_batch(&a, vec![ev(&a, "post-restore".into())])
        .unwrap();
    re.backup(&root.path().join("backup-2")).unwrap();
}

#[test]
fn corruption_and_tampering_are_detected() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let a = t("a");
    let e = Engine::open(opts(&root.path().join("live"))).unwrap();
    e.write_batch(&a, (0..50).map(|j| ev(&a, format!("x{j}"))).collect())
        .unwrap();
    let dest = root.path().join("bk");
    e.backup(&dest).unwrap();
    let seg = std::fs::read_dir(dest.join("segments/a"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let orig = std::fs::read(&seg).unwrap();

    // 1. Bit flip in a segment.
    let mut bad = orig.clone();
    bad[30] ^= 1;
    std::fs::write(&seg, &bad).unwrap();
    assert_eq!(
        verify_backup(&dest).unwrap_err().kind(),
        ErrorKind::Corruption
    );
    let target = root.path().join("r1");
    assert!(restore_backup(&dest, &target).is_err());
    assert!(
        !target.exists(),
        "failed restore must not leave a target behind"
    );
    std::fs::write(&seg, &orig).unwrap();
    verify_backup(&dest).unwrap();

    // 2. Missing file.
    std::fs::rename(&seg, root.path().join("moved")).unwrap();
    assert!(verify_backup(&dest).is_err());
    std::fs::rename(root.path().join("moved"), &seg).unwrap();

    // 3. Tampered backup manifest (path traversal attempt).
    let bpath = dest.join("BACKUP");
    let raw = std::fs::read_to_string(&bpath).unwrap();
    std::fs::write(&bpath, raw.replace("segments/a/", "../../etc/")).unwrap();
    assert_eq!(
        verify_backup(&dest).unwrap_err().kind(),
        ErrorKind::Corruption
    );
    std::fs::write(&bpath, raw).unwrap();
    verify_backup(&dest).unwrap();
}

#[test]
fn refuses_to_overwrite() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let a = t("a");
    let e = Engine::open(opts(&root.path().join("live"))).unwrap();
    e.write_batch(&a, vec![ev(&a, "1".into())]).unwrap();
    let dest = root.path().join("bk");
    e.backup(&dest).unwrap();
    assert_eq!(
        e.backup(&dest).unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    let occupied = root.path().join("occupied");
    std::fs::create_dir_all(&occupied).unwrap();
    std::fs::write(occupied.join("file"), b"x").unwrap();
    assert_eq!(
        restore_backup(&dest, &occupied).unwrap_err().kind(),
        ErrorKind::AlreadyExists
    );
    assert_eq!(std::fs::read(occupied.join("file")).unwrap(), b"x");
    // An existing empty directory is an acceptable target.
    let empty = root.path().join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    restore_backup(&dest, &empty).unwrap();
}

#[test]
fn interrupted_backup_is_not_published() {
    let _serial = serial();
    let root = tempfile::tempdir().unwrap();
    let a = t("a");
    let e = Engine::open(opts(&root.path().join("live"))).unwrap();
    e.write_batch(&a, vec![ev(&a, "1".into())]).unwrap();
    e.flush().unwrap();
    e.write_batch(&a, vec![ev(&a, "2".into())]).unwrap();
    assert!(stratadb_common::failpoint::set(
        "backup::after_segment_copy",
        "error@1"
    ));
    let dest = root.path().join("bk");
    let err = e.backup(&dest);
    stratadb_common::failpoint::clear_all();
    assert!(err.is_err());
    assert!(!dest.exists(), "a failed backup must not appear complete");
    // A retry succeeds (stale .tmp is replaced).
    e.backup(&dest).unwrap();
    verify_backup(&dest).unwrap();
}
