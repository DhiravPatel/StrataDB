//! Encryption at rest through the engine (spec §18.6, §19.4; ADR-0024).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::sync::Arc;

use stratadb_common::{ErrorKind, TenantId};
use stratadb_crypto::{KeyProvider, Keyring, SecretKey, StaticKeyProvider};
use stratadb_schema::{Event, Value};
use stratadb_storage::{
    restore_backup, verify_backup, CompactionOptions, Durability, Engine, EngineOptions,
};

/// A marker that must never appear in clear on disk once encrypted.
const SECRET: &str = "CARD-4111-1111-1111-1111";

fn tenant() -> TenantId {
    TenantId::new("enc").unwrap()
}

fn key(b: u8) -> Arc<dyn KeyProvider> {
    Arc::new(StaticKeyProvider(SecretKey::from_bytes([b; 32])))
}

fn opts(dir: &Path, k: Option<Arc<dyn KeyProvider>>) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.flush_on_close = false;
    o.wal.durability = Durability::Sync;
    o.wal.segment_max_bytes = 4096;
    o.encryption = k;
    o.compaction = CompactionOptions {
        min_inputs: 2,
        ..CompactionOptions::default()
    };
    o
}

fn batch(from: i64, n: i64) -> Vec<Event> {
    let now = stratadb_common::time::now_unix_nanos();
    (from..from + n)
        .map(|i| {
            let mut e = Event::new(tenant(), now - 1_000_000 * (1000 - i), now);
            e.event_id = Some(format!("e{i}"));
            e.body = Some(Value::from(format!("{SECRET} payment {i}")));
            e.attributes
                .insert("card".into(), Value::from(format!("{SECRET}-{i}")));
            e
        })
        .collect()
}

fn ids(e: &Engine) -> Vec<String> {
    let mut v: Vec<String> = e
        .snapshot(&tenant())
        .scan_all()
        .unwrap()
        .into_iter()
        .map(|(_, _, ev)| ev.event_id.unwrap())
        .collect();
    v.sort();
    v
}

/// Every file under `dir` whose bytes contain the marker.
fn plaintext_files(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if std::fs::read(&p)
                .unwrap()
                .windows(SECRET.len())
                .any(|w| w == SECRET.as_bytes())
            {
                out.push(p.display().to_string());
            }
        }
    }
    out
}

fn expect_kind(r: stratadb_common::Result<Engine>, kind: ErrorKind) -> String {
    let e = r.map(|_| ()).unwrap_err();
    assert_eq!(e.kind(), kind, "{e}");
    e.to_string()
}

#[test]
fn encrypted_database_never_stores_plaintext() {
    let d = tempfile::tempdir().unwrap();
    {
        let e = Engine::open(opts(d.path(), Some(key(1)))).unwrap();
        e.write_batch(&tenant(), batch(0, 40)).unwrap();
        e.flush().unwrap();
        e.write_batch(&tenant(), batch(40, 40)).unwrap();
        e.flush().unwrap();
        e.compact().unwrap();
        e.write_batch(&tenant(), batch(80, 20)).unwrap(); // stays in the WAL
    }
    assert!(Keyring::exists(d.path()));
    assert_eq!(plaintext_files(d.path()), Vec::<String>::new());
    let e = Engine::open(opts(d.path(), Some(key(1)))).unwrap();
    assert_eq!(e.open_report().rows_recovered_from_wal, 20);
    assert_eq!(ids(&e).len(), 100);
    drop(e);
    // Full checksum verification of every (sealed) segment.
    let mut o = opts(d.path(), Some(key(1)));
    o.verify_segments_on_open = true;
    drop(Engine::open(o).unwrap());
    // Without the key, or with another key: refused, never misread.
    let msg = expect_kind(
        Engine::open(opts(d.path(), None)),
        ErrorKind::PermissionDenied,
    );
    assert!(msg.contains("encrypted"), "{msg}");
    let msg = expect_kind(
        Engine::open(opts(d.path(), Some(key(2)))),
        ErrorKind::PermissionDenied,
    );
    assert!(msg.contains("not the key-encryption key"), "{msg}");
}

#[test]
fn enabling_encryption_on_existing_data_and_upgrading() {
    let d = tempfile::tempdir().unwrap();
    {
        let e = Engine::open(opts(d.path(), None)).unwrap();
        e.write_batch(&tenant(), batch(0, 30)).unwrap();
        e.flush().unwrap();
        e.write_batch(&tenant(), batch(30, 10)).unwrap();
    }
    assert!(!plaintext_files(d.path()).is_empty());
    let e = Engine::open(opts(d.path(), Some(key(1)))).unwrap();
    assert_eq!(ids(&e).len(), 40, "plaintext data stays readable");
    e.write_batch(&tenant(), batch(40, 10)).unwrap();
    e.flush().unwrap(); // reclaims the plaintext WAL segments
    let up = e.upgrade_segments(true).unwrap();
    assert_eq!(up.segments_upgraded, 0, "dry run");
    let up = e.upgrade_segments(false).unwrap();
    assert!(up.segments_upgraded >= 1, "{up:?}");
    assert_eq!(ids(&e).len(), 50);
    drop(e);
    assert_eq!(
        plaintext_files(d.path()),
        Vec::<String>::new(),
        "after upgrade no plaintext remains"
    );
    let e = Engine::open(opts(d.path(), Some(key(1)))).unwrap();
    assert_eq!(ids(&e).len(), 50);
}

#[test]
fn key_rotation() {
    let d = tempfile::tempdir().unwrap();
    {
        let e = Engine::open(opts(d.path(), Some(key(1)))).unwrap();
        e.write_batch(&tenant(), batch(0, 20)).unwrap();
        e.flush().unwrap();
    }
    // Offline rotation: new active DEK, then a new KEK.
    let mut k = Keyring::load(d.path(), key(1).as_ref()).unwrap().unwrap();
    assert_eq!(k.rotate_dek(d.path()).unwrap(), 2);
    k.rotate_kek(d.path(), key(3).as_ref()).unwrap();
    expect_kind(
        Engine::open(opts(d.path(), Some(key(1)))),
        ErrorKind::PermissionDenied,
    );
    let e = Engine::open(opts(d.path(), Some(key(3)))).unwrap();
    e.write_batch(&tenant(), batch(20, 20)).unwrap();
    e.flush().unwrap();
    assert_eq!(
        ids(&e).len(),
        40,
        "old (DEK 1) and new (DEK 2) segments readable"
    );
    e.compact().unwrap();
    assert_eq!(ids(&e).len(), 40);
    drop(e);
    assert_eq!(plaintext_files(d.path()), Vec::<String>::new());
}

#[test]
fn encrypted_backup_verify_and_restore() {
    let d = tempfile::tempdir().unwrap();
    let data = d.path().join("data");
    let bk = d.path().join("bk");
    {
        let e = Engine::open(opts(&data, Some(key(1)))).unwrap();
        e.write_batch(&tenant(), batch(0, 30)).unwrap();
        e.backup(&bk).unwrap();
    }
    assert_eq!(plaintext_files(&bk), Vec::<String>::new());
    assert!(bk.join("KEYRING").exists());
    let err = verify_backup(&bk, None).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");
    assert!(verify_backup(&bk, Some(key(2).as_ref())).is_err());
    verify_backup(&bk, Some(key(1).as_ref())).unwrap();
    assert!(restore_backup(&bk, &d.path().join("r0"), None).is_err());
    assert!(restore_backup(&bk, &d.path().join("r1"), Some(key(2))).is_err());
    let r = restore_backup(&bk, &d.path().join("r2"), Some(key(1))).unwrap();
    assert_eq!(r.rows, 30);
    let e = Engine::open(opts(&d.path().join("r2"), Some(key(1)))).unwrap();
    assert_eq!(ids(&e).len(), 30);
}

#[test]
fn tampering_with_sealed_files_is_detected() {
    let d = tempfile::tempdir().unwrap();
    {
        let e = Engine::open(opts(d.path(), Some(key(1)))).unwrap();
        e.write_batch(&tenant(), batch(0, 50)).unwrap();
        e.flush().unwrap();
    }
    let seg_dir = d.path().join("segments").join("enc");
    let seg = std::fs::read_dir(&seg_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let orig = std::fs::read(&seg).unwrap();
    for at in [100, orig.len() / 2, orig.len() - 3] {
        let mut x = orig.clone();
        x[at] ^= 0x10;
        std::fs::write(&seg, &x).unwrap();
        let mut o = opts(d.path(), Some(key(1)));
        o.verify_segments_on_open = true;
        let r = Engine::open(o);
        expect_kind(r, ErrorKind::Corruption);
    }
    std::fs::write(&seg, &orig).unwrap();
    let man = d.path().join("MANIFEST");
    let morig = std::fs::read(&man).unwrap();
    let mut x = morig.clone();
    let n = x.len();
    x[n - 5] ^= 1;
    std::fs::write(&man, &x).unwrap();
    expect_kind(
        Engine::open(opts(d.path(), Some(key(1)))),
        ErrorKind::Corruption,
    );
    std::fs::write(&man, &morig).unwrap();
    assert_eq!(
        ids(&Engine::open(opts(d.path(), Some(key(1)))).unwrap()).len(),
        50
    );
}

/// Key of the golden encrypted fixture (test-only; never use elsewhere).
const FIXTURE_KEY: [u8; 32] = [0x5a; 32];

fn fixture_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/encrypted-v1")
}

fn fixture_batch(from: i64, n: i64) -> Vec<Event> {
    // Fixed times (the fixture is read years later): ingest = event time.
    (from..from + n)
        .map(|i| {
            let t = 1_795_170_600_000_000_000 + i * 1_000_000_000;
            let mut e = Event::new(tenant(), t, t);
            e.event_id = Some(format!("f{i}"));
            e.body = Some(Value::from(format!("{SECRET} fixture {i}")));
            e
        })
        .collect()
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            copy_tree(&e.path(), &to.join(e.file_name()));
        } else {
            std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

/// Golden encrypted data directory: KEYRING, sealed MANIFEST, one sealed
/// segment (events f0..f20) and an encrypted WAL holding f20..f25.
/// Never regenerate it to make a failure go away.
#[test]
fn reads_encrypted_v1_fixture() {
    let src = fixture_dir();
    assert!(
        std::fs::read(src.join("MANIFEST"))
            .unwrap()
            .starts_with(b"STRENC\r\n"),
        "fixture manifest must be sealed"
    );
    assert!(
        !plaintext_files(&src).iter().any(|_| true),
        "fixture holds plaintext"
    );
    let d = tempfile::tempdir().unwrap();
    copy_tree(&src, d.path());
    let k: Arc<dyn KeyProvider> = Arc::new(StaticKeyProvider(SecretKey::from_bytes(FIXTURE_KEY)));
    let mut o = opts(d.path(), Some(k));
    o.verify_segments_on_open = true;
    let e = Engine::open(o).unwrap();
    assert_eq!(e.open_report().rows_recovered_from_wal, 5);
    let evs = e.snapshot(&tenant()).scan_all().unwrap();
    assert_eq!(evs.len(), 25);
    let want = fixture_batch(0, 25);
    for (got, w) in evs.iter().zip(&want) {
        assert_eq!(got.2.event_id, w.event_id);
        assert_eq!(got.2.body, w.body);
        assert_eq!(got.2.event_time, w.event_time);
    }
}

#[test]
#[ignore = "writes tests/fixtures/encrypted-v1; run once when introducing the format"]
fn generate_encrypted_v1_fixture() {
    let out = fixture_dir();
    assert!(!out.exists(), "fixture exists; never regenerate it");
    let tmp = tempfile::tempdir().unwrap();
    let k: Arc<dyn KeyProvider> = Arc::new(StaticKeyProvider(SecretKey::from_bytes(FIXTURE_KEY)));
    {
        let mut o = opts(tmp.path(), Some(k));
        o.wal.segment_max_bytes = 1 << 20;
        let e = Engine::open(o).unwrap();
        e.write_batch(&tenant(), fixture_batch(0, 20)).unwrap();
        e.flush().unwrap();
        e.write_batch(&tenant(), fixture_batch(20, 5)).unwrap();
    }
    std::fs::remove_file(tmp.path().join("LOCK")).unwrap();
    copy_tree(tmp.path(), &out);
}
