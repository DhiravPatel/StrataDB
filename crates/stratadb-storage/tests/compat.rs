//! Format compatibility against committed golden files (ticket B-006, ADR-0015).
//!
//! `tests/fixtures/storage-v1/` is a data directory written by format v1
//! (WAL v1, segment v1, manifest v1, event encoding v1) containing flushed
//! segments for two tenants plus unflushed WAL records. Readers must keep
//! decoding it exactly. **Never regenerate this fixture to make a failing test
//! pass**: a failure means a compatibility break, which needs a version bump,
//! a migration path, and a new fixture alongside this one.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use stratadb_common::TenantId;
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/storage-v1")
}

fn opts(dir: &Path) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Sync;
    o.flush_on_close = false;
    o
}

fn event(t: &str, i: i64) -> Event {
    let mut e = Event::new(
        TenantId::new(t).unwrap(),
        1_700_000_000_000_000_000 + i,
        1_700_000_000_500_000_000 + i,
    );
    e.event_id = Some(format!("{t}-{i}"));
    e.service = Some(format!("svc{}", i % 2));
    e.severity = if i % 3 == 0 {
        Severity::Error
    } else {
        Severity::Info
    };
    e.severity_text = (i == 4).then(|| "warning-ish".to_owned());
    e.body = match i % 4 {
        0 => Some(Value::from(format!("text body {i}"))),
        1 => Some(Value::Map(BTreeMap::from([(
            "k".to_owned(),
            Value::List(vec![Value::Int(-i), Value::Bool(true)]),
        )]))),
        2 => None,
        _ => Some(Value::Null),
    };
    e.trace_id = (i % 2 == 0).then(|| format!("{i:032x}"));
    e.span_id = (i % 2 == 0).then(|| format!("{i:016x}"));
    e.attributes.insert("int".into(), Value::Int(i * 1000));
    e.attributes
        .insert("uint".into(), Value::UInt(u64::MAX - i as u64));
    e.attributes
        .insert("float".into(), Value::Float(i as f64 / 3.0));
    e.attributes
        .insert("bytes".into(), Value::Bytes(vec![0, 255, i as u8]));
    e.attributes.insert("ts".into(), Value::Timestamp(-i));
    e.attributes.insert("null".into(), Value::Null);
    e.attributes.insert("empty".into(), Value::from(""));
    e.resource_attributes
        .insert("host".into(), Value::from("fixture-host"));
    e
}

fn expected() -> Vec<Event> {
    let mut v: Vec<Event> = (0..12).map(|i| event("alpha", i)).collect();
    v.extend((0..5).map(|i| event("beta", i)));
    v
}

#[test]
#[ignore = "writes the golden fixture; run only when intentionally creating a NEW format version"]
fn regenerate_storage_v1_fixture() {
    let dir = fixture_dir();
    let _ = fs::remove_dir_all(&dir);
    let e = Engine::open(opts(&dir)).unwrap();
    let all = expected();
    let (alpha, beta): (Vec<_>, Vec<_>) = all
        .into_iter()
        .partition(|e| e.tenant_id.as_str() == "alpha");
    let a = TenantId::new("alpha").unwrap();
    let b = TenantId::new("beta").unwrap();
    e.write_batch(&a, alpha[..8].to_vec()).unwrap();
    e.write_batch(&b, beta[..3].to_vec()).unwrap();
    e.flush().unwrap(); // -> segments for both tenants
    e.write_batch(&a, alpha[8..].to_vec()).unwrap(); // -> remains in the WAL only
    e.write_batch(&b, beta[3..].to_vec()).unwrap();
    drop(e);
    fs::remove_file(dir.join("LOCK")).unwrap();
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let t = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &t);
        } else {
            fs::copy(e.path(), t).unwrap();
        }
    }
}

#[test]
fn reads_storage_v1_fixture() {
    let src = fixture_dir();
    assert!(
        src.join("MANIFEST").exists(),
        "golden fixture missing at {}",
        src.display()
    );
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&src, tmp.path());
    let e = Engine::open(opts(tmp.path())).unwrap();
    assert_eq!(e.open_report().segments, 2);
    assert_eq!(e.open_report().rows_recovered_from_wal, 6);
    assert_eq!(e.verify().unwrap(), 2);
    for t in ["alpha", "beta"] {
        let tid = TenantId::new(t).unwrap();
        let mut got: Vec<Event> = e
            .snapshot(&tid)
            .scan_all()
            .unwrap()
            .into_iter()
            .map(|(_, _, ev)| ev)
            .collect();
        let mut want: Vec<Event> = expected()
            .into_iter()
            .filter(|ev| ev.tenant_id == tid)
            .collect();
        got.sort_by_key(|e| e.event_id.clone());
        want.sort_by_key(|e| e.event_id.clone());
        assert_eq!(got, want, "tenant {t}: v1 fixture decoded differently");
    }
}

/// v1 and current-version segments coexist; compaction upgrades v1 segments
/// to the current format (RFC-0001/0002 rollout).
#[test]
fn v1_fixture_upgrades_through_compaction() {
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&fixture_dir(), tmp.path());
    let alpha = TenantId::new("alpha").unwrap();
    let mut o = opts(tmp.path());
    o.compaction.min_inputs = 2;
    let e = Engine::open(o.clone()).unwrap();
    e.flush().unwrap(); // WAL rows → a new v2 segment next to the v1 segment
    let m = stratadb_storage::Manifest::load(tmp.path())
        .unwrap()
        .unwrap();
    let versions: BTreeMap<u64, u16> = m
        .segments
        .iter()
        .map(|s| (s.id, s.segment_format_version))
        .collect();
    assert!(
        versions.values().any(|v| *v == 1)
            && versions
                .values()
                .any(|v| *v == stratadb_segment::FORMAT_VERSION),
        "{versions:?}"
    );
    let before = e.snapshot(&alpha).scan_all().unwrap().len();
    assert!(e.compact().unwrap().groups >= 1);
    drop(e);
    let e = Engine::open(o).unwrap();
    let m = stratadb_storage::Manifest::load(tmp.path())
        .unwrap()
        .unwrap();
    assert!(m
        .segments
        .iter()
        .filter(|s| s.tenant == alpha)
        .all(|s| s.segment_format_version == stratadb_segment::FORMAT_VERSION));
    let mut got: Vec<Event> = e
        .snapshot(&alpha)
        .scan_all()
        .unwrap()
        .into_iter()
        .map(|(_, _, ev)| ev)
        .collect();
    assert_eq!(got.len(), before);
    let mut want: Vec<Event> = expected()
        .into_iter()
        .filter(|ev| ev.tenant_id == alpha)
        .collect();
    got.sort_by_key(|e| e.event_id.clone());
    want.sort_by_key(|e| e.event_id.clone());
    assert_eq!(got, want);
    e.verify().unwrap();
}
