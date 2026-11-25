//! Stable-toolchain mutation fuzzing of every untrusted-input decoder.
//! Deterministic (seeded) so failures reproduce; iterations per target via
//! `STRATADB_FUZZ_ITERS` (default 1500). Coverage-guided campaigns use /fuzz.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};

use serde_json::json;
use stratadb_common::TenantId;
use stratadb_schema::{Event, Value};
use stratadb_storage::{Durability, Engine, EngineOptions, IdempotencyKey};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
}

const INTERESTING: &[u8] = &[0x00, 0xff, 0x7f, 0x80, 0x01, 0xfe, b'\n', b'"', b'\'', b'['];

fn mutate(r: &mut Rng, seeds: &[Vec<u8>]) -> Vec<u8> {
    let mut d = seeds[r.below(seeds.len())].clone();
    for _ in 0..1 + r.below(4) {
        let len = d.len();
        match r.below(9) {
            0 if len > 0 => {
                let i = r.below(len);
                d[i] ^= 1 << r.below(8);
            }
            1 if len > 0 => {
                let i = r.below(len);
                d[i] = r.next() as u8;
            }
            2 if len > 0 => {
                let i = r.below(len);
                d[i] = INTERESTING[r.below(INTERESTING.len())];
            }
            3 => {
                let i = r.below(len + 1);
                d.insert(i, r.next() as u8);
            }
            4 if len > 0 => {
                let i = r.below(len);
                let n = 1 + r.below((len - i).min(16));
                d.drain(i..i + n);
            }
            5 if len > 0 => {
                let i = r.below(len);
                let n = 1 + r.below((len - i).min(32));
                let chunk = d[i..i + n].to_vec();
                let j = r.below(d.len() + 1);
                d.splice(j..j, chunk);
            }
            6 => d.truncate(r.below(len + 1)),
            7 => {
                let other = &seeds[r.below(seeds.len())];
                if !other.is_empty() {
                    let i = r.below(other.len());
                    let cut = r.below(d.len() + 1);
                    d.truncate(cut);
                    d.extend_from_slice(&other[i..]);
                }
            }
            _ => {
                // Repeat a byte many times (stresses length fields / nesting).
                let b = INTERESTING[r.below(INTERESTING.len())];
                let i = r.below(len + 1);
                d.splice(i..i, std::iter::repeat_n(b, 1 + r.below(64)));
            }
        }
    }
    d
}

/// Real artifacts written by the current code, used as mutation seeds.
fn seeds() -> Vec<(&'static str, Vec<Vec<u8>>)> {
    let dir = tempfile::tempdir().unwrap();
    let t = TenantId::new("fuzz").unwrap();
    let mut o = EngineOptions::new(dir.path());
    o.background = false;
    o.wal.durability = Durability::Async;
    o.flush_on_close = false;
    let e = Engine::open(o).unwrap();
    let mk = |i: i64| {
        let mut ev = Event::new(t.clone(), i, i);
        ev.trace_id = Some(format!("{i:032x}"));
        ev.body = Some(Value::from(format!("body {i}")));
        ev.attributes
            .insert("request_id".into(), Value::String(format!("r{i}")));
        ev.attributes.insert("n".into(), Value::Int(i));
        ev
    };
    for b in 0..3u8 {
        let k = IdempotencyKey::new(format!("k{b}"), [b; 32]).unwrap();
        e.write_batch_idempotent(
            &t,
            (0..5).map(|i| mk(i64::from(b) * 10 + i)).collect(),
            Some(k),
        )
        .unwrap();
        if b < 2 {
            e.flush().unwrap();
        }
    }
    let backup = dir.path().join("bk");
    e.backup(&backup).unwrap();
    let read_all = |sub: &str| -> Vec<Vec<u8>> {
        std::fs::read_dir(dir.path().join(sub))
            .unwrap()
            .map(|f| std::fs::read(f.unwrap().path()).unwrap())
            .collect()
    };
    let wal = read_all("wal");
    let segs = read_all("segments/fuzz");
    let manifest = std::fs::read(dir.path().join("MANIFEST")).unwrap();
    let bman = std::fs::read(backup.join("BACKUP")).unwrap();
    let batch = stratadb_schema::codec::encode_batch(&[mk(1), mk(2)]);

    let sec = tempfile::tempdir().unwrap();
    let mut store = stratadb_server::auth::TokenStore::load(sec.path()).unwrap();
    store
        .create(
            t.clone(),
            BTreeSet::from([stratadb_server::auth::Permission::Query]),
            None,
            None,
        )
        .unwrap();
    let log = stratadb_server::audit::AuditLog::open(sec.path()).unwrap();
    for i in 0..3 {
        log.record("e", Some(("id", "fuzz")), json!({"i": i}))
            .unwrap();
    }
    let tokens = std::fs::read(stratadb_server::auth::TokenStore::path_for(sec.path())).unwrap();
    let audit = std::fs::read(stratadb_server::audit::AuditLog::path_for(sec.path())).unwrap();

    let queries: Vec<Vec<u8>> = [
        "SELECT * FROM logs WHERE event_time >= TIMESTAMP '1970-01-01T00:00:01Z' ORDER BY event_time DESC LIMIT 5",
        "SELECT service, count(*) AS n, min(attributes['n']), max(attributes['f']), sum(attributes['n']), avg(attributes['f']) FROM logs GROUP BY service ORDER BY n DESC",
        "SELECT time_bucket(INTERVAL '10 s', event_time) AS b, count(*) FROM logs GROUP BY time_bucket(INTERVAL '10 s', event_time) ORDER BY b",
        "EXPLAIN ANALYZE SELECT body FROM logs WHERE trace_id IN ('00000000000000000000000000000001', 'x') AND body ILIKE '%mess_ge%' AND NOT (attributes['z'] IS NOT NULL) AND attributes['q'] IS MISSING",
        "SELECT attributes['nested'], resource_attributes['h'] FROM logs WHERE severity_number >= 9 OR service != 'a' ORDER BY 1",
    ]
    .iter()
    .map(|s| s.as_bytes().to_vec())
    .collect();
    let json_seeds: Vec<Vec<u8>> = vec![
        json!({"event_time": "2026-11-20T10:30:00.123Z", "service": "s", "severity": "ERROR", "body": {"a": [1, 2.5, null, true]},
               "trace_id": "0123456789abcdef0123456789abcdef", "attributes": {"k": "v"}, "extra": 1}).to_string().into_bytes(),
        json!({"resourceLogs": [{"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "x"}}]},
               "scopeLogs": [{"scope": {"name": "n"}, "logRecords": [{"timeUnixNano": "1795170600000000000", "severityNumber": 17,
               "body": {"kvlistValue": {"values": [{"key": "a", "value": {"arrayValue": {"values": [{"intValue": "1"}, {"bytesValue": "AAE="}]}}}]}},
               "traceId": "5B8EFFF798038103D269B633813FC60C", "attributes": [{"key": "d", "value": {"doubleValue": 1.5}}]}]}]}]}).to_string().into_bytes(),
    ];
    // Real cursors of the `cursor` target's queries (selector byte + rest).
    let mut cursors: Vec<Vec<u8>> = Vec::new();
    for (sel, sql) in [
        (1u8, "SELECT body FROM logs ORDER BY event_time DESC"),
        (0, "SELECT body FROM logs"),
    ] {
        let p = stratadb_query::execute_page(
            &e.snapshot(&t),
            sql,
            &stratadb_query::QueryOptions::default(),
            &stratadb_query::PageRequest {
                page_size: 2,
                cursor: None,
            },
        )
        .unwrap();
        let text = p.next_cursor.unwrap();
        let mut seed = vec![sel];
        seed.extend(stratadb_query::page::fuzz_hooks::cursor_rest(&text).unwrap());
        cursors.push(seed);
        let mut raw = vec![sel];
        raw.extend_from_slice(text.as_bytes());
        cursors.push(raw);
    }
    // Sealed files, KEYRING and an encrypted WAL from the golden fixture.
    let fx =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/encrypted-v1");
    let sealed: Vec<Vec<u8>> = [
        "MANIFEST",
        "KEYRING",
        "segments/enc/seg-00000000000000000001.sseg",
        "wal/wal-00000000000000000001.log",
    ]
    .iter()
    .map(|f| std::fs::read(fx.join(f)).unwrap())
    .collect();
    // Map chunks: selector byte (key mask, rows) + encoded maps.
    let maps = {
        use stratadb_common::codec::Encoder;
        let mut out = Vec::new();
        for sel in [0b1_1111u8, 0b0_0110 | (2 << 5)] {
            let mut enc = Encoder::new();
            for i in 0..3i64 {
                let mut m = std::collections::BTreeMap::new();
                m.insert("a".to_owned(), Value::Int(i));
                m.insert(
                    "hot".to_owned(),
                    Value::List(vec![Value::from("x"), Value::Null]),
                );
                if i == 1 {
                    m.insert(
                        "nested".to_owned(),
                        Value::Map(std::collections::BTreeMap::from([(
                            "k".to_owned(),
                            Value::Float(1.5),
                        )])),
                    );
                }
                stratadb_schema::codec::encode_map(&mut enc, &m);
            }
            let mut seed = vec![sel];
            seed.extend(enc.into_inner());
            out.push(seed);
        }
        out
    };
    let config = b"config_version = 1\ndata_dir = \"/d\"\nlisten = \"127.0.0.1:1\"\n[limits]\nmax_query_rows = 5\n[storage]\nbloom_attribute_keys = [\"a\"]\ndisk_reserve_ratio = 0.1\n[tls]\ncert = \"/c\"\nkey = \"/k\"\n".to_vec();
    drop(e);
    vec![
        ("wal_frames", wal.clone()),
        ("wal_recovery", wal),
        ("segment", segs.clone()),
        ("segment_internals", segment_internal_seeds(&segs)),
        ("manifest", vec![manifest, bman]),
        ("event_codec", vec![batch]),
        ("query", queries),
        ("json_event", json_seeds),
        (
            "otlp_proto",
            vec![std::fs::read(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../tests/fixtures/otlp/request.binpb"
            ))
            .unwrap()],
        ),
        ("config", vec![config]),
        ("security_files", vec![tokens, audit]),
        ("cursor", cursors),
        ("sealed", sealed),
        ("map_projection", maps),
    ]
}

/// Footer bytes and (selector ‖ chunk) inputs extracted from real segments.
fn segment_internal_seeds(segs: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for seg in segs {
        let n = seg.len();
        let flen =
            u32::from_le_bytes([seg[n - 16], seg[n - 15], seg[n - 14], seg[n - 13]]) as usize;
        out.push(seg[n - 16 - flen..n - 16].to_vec());
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("s.sseg");
        std::fs::write(&p, seg).unwrap();
        let r = stratadb_segment::SegmentReader::open(&p).unwrap();
        for c in &r.footer().columns {
            let chunk = &seg[c.offset as usize..(c.offset + c.len) as usize];
            let mut v = vec![
                c.id as u16 as u8,
                c.encoding as u8,
                r.footer().row_count.min(255) as u8,
                (c.raw_len / 64).min(255) as u8,
            ];
            v.extend_from_slice(chunk);
            out.push(v);
        }
    }
    out
}

#[test]
fn no_decoder_panics_on_mutated_inputs() {
    let iters: usize = std::env::var("STRATADB_FUZZ_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1500);
    let seeds = seeds();
    let mut failures = Vec::new();
    // Silence the default panic printer; failures are reported below.
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    for (name, f) in stratadb_fuzz::TARGETS {
        let s = &seeds.iter().find(|(n, _)| n == name).unwrap().1;
        assert!(!s.is_empty(), "no seeds for {name}");
        // Seeds themselves must be handled.
        for input in s {
            if catch_unwind(AssertUnwindSafe(|| f(input))).is_err() {
                failures.push(format!("{name}: panicked on an unmodified seed"));
            }
        }
        let mut r = Rng(0x5eed ^ name.len() as u64);
        let n = if matches!(
            *name,
            "wal_recovery" | "segment" | "security_files" | "sealed" | "cursor"
        ) {
            iters / 3
        } else {
            iters
        };
        for i in 0..n {
            let input = mutate(&mut r, s);
            if catch_unwind(AssertUnwindSafe(|| f(&input))).is_err() {
                failures.push(format!(
                    "{name}: panic on iteration {i} (input {} bytes, first 64: {:02x?})",
                    input.len(),
                    &input[..input.len().min(64)]
                ));
                if failures.len() > 20 {
                    break;
                }
            }
        }
    }
    std::panic::set_hook(prev);
    assert!(
        failures.is_empty(),
        "decoder panics:\n{}",
        failures.join("\n")
    );
}

/// Write the seed artifacts to `fuzz/corpus/<target>/` for libFuzzer:
/// `cargo test -p stratadb-fuzz -- --ignored export_corpus`.
#[test]
#[ignore = "writes files under fuzz/corpus"]
fn export_corpus() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus");
    for (name, seeds) in seeds() {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        for (i, s) in seeds.iter().enumerate() {
            std::fs::write(dir.join(format!("seed-{i}")), s).unwrap();
        }
    }
}
