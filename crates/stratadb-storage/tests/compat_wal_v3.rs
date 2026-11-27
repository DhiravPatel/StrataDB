//! Columnar WAL payloads (WAL format v3, spec STRATA-SYS-07-002) against a
//! committed golden data directory, plus the upgrade and rollback paths.
//!
//! `tests/fixtures/storage-wal-v3/` holds one flushed segment and three
//! unflushed WAL v3 records:
//! - a small plain columnar batch;
//! - a large LZ4-compressed batch;
//! - an idempotent columnar batch.
//!
//! **Never regenerate it to make a failing test pass**: a failure is a
//! compatibility break.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods)] // test code; reads WAL headers directly

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use stratadb_common::TenantId;
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::{Durability, Engine, EngineOptions, IdempotencyKey};
use stratadb_wal::format::{self, RecordType};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/storage-wal-v3")
}

fn opts(dir: &Path) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Sync;
    o.flush_on_close = false;
    o
}

fn tenant() -> TenantId {
    TenantId::new("gamma").unwrap()
}

/// Deterministic events covering every field and value type.
fn event(i: i64) -> Event {
    let mut e = Event::new(
        tenant(),
        1_790_000_000_000_000_000 + i * 1_000_003,
        1_790_000_000_500_000_000 + i,
    );
    e.event_id = Some(format!("gamma-{i}"));
    e.service = Some(["api", "db", "worker"][(i % 3) as usize].to_owned());
    e.severity = [Severity::Info, Severity::Warn, Severity::Error][(i % 3) as usize];
    e.severity_text = (i % 7 == 0).then(|| "custom-level".to_owned());
    e.trace_id = (i % 2 == 0).then(|| format!("{i:032x}"));
    e.span_id = (i % 4 == 0).then(|| format!("{i:016x}"));
    e.body = match i % 5 {
        0 => Some(Value::from(format!("request {i} served in {} ms", i % 113))),
        1 => Some(Value::Map(BTreeMap::from([(
            "nested".to_owned(),
            Value::List(vec![Value::Int(-i), Value::Bool(i % 2 == 0), Value::Null]),
        )]))),
        2 => None,
        3 => Some(Value::Null),
        _ => Some(Value::Bytes(vec![0, 255, i as u8])),
    };
    e.attributes.insert("int".into(), Value::Int(i * 1000));
    e.attributes
        .insert("uint".into(), Value::UInt(u64::MAX - i as u64));
    e.attributes
        .insert("float".into(), Value::Float(i as f64 / 7.0));
    e.attributes.insert("ts".into(), Value::Timestamp(-i));
    e.attributes
        .insert("route".into(), Value::from("/api/v1/items/{id}"));
    e.resource_attributes
        .insert("host".into(), Value::from("fixture-host"));
    e.schema_version = 1 + (i % 2) as u32;
    e
}

/// (flushed, unflushed) rows of the fixture, in write order.
fn expected() -> (Vec<Event>, Vec<Event>) {
    let flushed = (0..20).map(event).collect();
    let unflushed = (100..105)
        .chain(1000..1400)
        .chain(5000..5010)
        .map(event)
        .collect();
    (flushed, unflushed)
}

#[test]
#[ignore = "writes the golden fixture; run only when intentionally creating a NEW format version"]
fn regenerate_storage_wal_v3_fixture() {
    let dir = fixture_dir();
    assert!(!dir.exists(), "fixture exists; never regenerate it");
    let e = Engine::open(opts(&dir)).unwrap();
    let (flushed, unflushed) = expected();
    e.write_batch(&tenant(), flushed).unwrap();
    e.flush().unwrap();
    e.write_batch(&tenant(), unflushed[..5].to_vec()).unwrap();
    e.write_batch(&tenant(), unflushed[5..405].to_vec())
        .unwrap();
    let key = IdempotencyKey::new("fixture-key-1", [7; 32]).unwrap();
    e.write_batch_idempotent(&tenant(), unflushed[405..].to_vec(), Some(key))
        .unwrap();
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

/// (version, record types) of every WAL segment, in order.
fn wal_contents(dir: &Path) -> Vec<(u16, Vec<RecordType>)> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    files.sort();
    files
        .iter()
        .map(|p| {
            let data = fs::read(p).unwrap();
            let h = format::decode_header(&data, "t").unwrap();
            let mut pos = format::HEADER_LEN;
            let mut types = Vec::new();
            while pos < data.len() {
                let f = format::parse_frame(&data[pos..], h.version).unwrap();
                types.push(RecordType::from_u8(f.record_type).unwrap());
                pos += f.encoded_len();
            }
            (h.version, types)
        })
        .collect()
}

fn rows(e: &Engine) -> Vec<Event> {
    let mut v: Vec<Event> = e
        .snapshot(&tenant())
        .scan_all()
        .unwrap()
        .into_iter()
        .map(|(_, _, ev)| ev)
        .collect();
    v.sort_by_key(|e| e.event_id.clone());
    v
}

fn sorted(mut v: Vec<Event>) -> Vec<Event> {
    v.sort_by_key(|e| e.event_id.clone());
    v
}

#[test]
fn reads_storage_wal_v3_fixture() {
    let src = fixture_dir();
    assert!(
        src.join("MANIFEST").exists(),
        "golden fixture missing at {}",
        src.display()
    );
    assert_eq!(
        wal_contents(&src).last().unwrap(),
        &(
            3,
            vec![
                // Flushed (skipped by replay; the segment is not truncated).
                RecordType::ColumnarEventBatch,
                RecordType::ColumnarEventBatch,
                RecordType::ColumnarEventBatch,
                RecordType::IdempotentColumnarEventBatch
            ]
        ),
        "the fixture's WAL holds the flushed record and three unflushed columnar records"
    );
    let tmp = tempfile::tempdir().unwrap();
    copy_dir(&src, tmp.path());
    let e = Engine::open(opts(tmp.path())).unwrap();
    assert_eq!(e.open_report().segments, 1);
    assert_eq!(e.open_report().rows_recovered_from_wal, 415);
    let (flushed, unflushed) = expected();
    assert_eq!(
        rows(&e),
        sorted(flushed.into_iter().chain(unflushed).collect())
    );
    // And they survive a flush to segments.
    e.flush().unwrap();
    drop(e);
    let e = Engine::open(opts(tmp.path())).unwrap();
    assert_eq!(rows(&e).len(), 435);
    e.verify().unwrap();
}

/// The large record of the fixture really is LZ4-compressed, and columnar
/// records are much smaller than the row encoding of the same events.
#[test]
fn columnar_records_are_compressed_and_smaller() {
    let (_, unflushed) = expected();
    let big = &unflushed[5..405];
    let col = stratadb_schema::columnar::encode_batch(big).unwrap();
    assert_eq!(col[1] & 1, 1, "LZ4 flag");
    let row = stratadb_schema::codec::encode_batch(big);
    assert!(
        col.len() * 2 < row.len(),
        "columnar {} vs row {}",
        col.len(),
        row.len()
    );
}

/// Upgrading a v1 data directory, and the v2 rollback window: with
/// `format_version = 2` only row records and v2 segments are written;
/// switching to 3 starts a v3 segment; every row is read either way.
#[test]
fn upgrade_and_rollback_window() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let mut v2 = opts(dir);
    v2.wal.format_version = 2;
    let e = Engine::open(v2.clone()).unwrap();
    e.write_batch(&tenant(), (0..10).map(event).collect())
        .unwrap();
    drop(e);
    assert_eq!(wal_contents(dir), vec![(2, vec![RecordType::EventBatch])]);
    let e = Engine::open(opts(dir)).unwrap();
    e.write_batch(&tenant(), (10..20).map(event).collect())
        .unwrap();
    let key = IdempotencyKey::new("k", [1; 32]).unwrap();
    e.write_batch_idempotent(&tenant(), (20..25).map(event).collect(), Some(key.clone()))
        .unwrap();
    assert!(
        e.write_batch_idempotent(&tenant(), (20..25).map(event).collect(), Some(key.clone()))
            .unwrap()
            .duplicate
    );
    drop(e);
    assert_eq!(
        wal_contents(dir),
        vec![
            (2, vec![RecordType::EventBatch]),
            (
                3,
                vec![
                    RecordType::ColumnarEventBatch,
                    RecordType::IdempotentColumnarEventBatch
                ]
            )
        ]
    );
    // Columnar records replay; the idempotency key is remembered across restart.
    let e = Engine::open(opts(dir)).unwrap();
    assert_eq!(rows(&e), sorted((0..25).map(event).collect()));
    assert!(
        e.write_batch_idempotent(&tenant(), (20..25).map(event).collect(), Some(key))
            .unwrap()
            .duplicate
    );
    drop(e);
    // v1 directory (WAL v1 records) upgrades too.
    let v1 = tempfile::tempdir().unwrap();
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/storage-v1"),
        v1.path(),
    );
    let e = Engine::open(opts(v1.path())).unwrap();
    let alpha = TenantId::new("alpha").unwrap();
    let before = e.snapshot(&alpha).scan_all().unwrap().len();
    let mut ev = event(1);
    ev.tenant_id = alpha.clone();
    e.write_batch(&alpha, vec![ev]).unwrap();
    drop(e);
    let e = Engine::open(opts(v1.path())).unwrap();
    assert_eq!(e.snapshot(&alpha).scan_all().unwrap().len(), before + 1);
    let versions: Vec<u16> = wal_contents(v1.path()).iter().map(|s| s.0).collect();
    assert_eq!(versions, vec![1, 3]);
}
