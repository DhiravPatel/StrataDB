//! Fuzz entry points (spec §26.3, ticket H-011).
//!
//! Each `fuzz_*` function feeds arbitrary bytes to one untrusted-input parser
//! or decoder. Contract: **never panic, never allocate unboundedly** — errors
//! are fine. They are driven by (a) `tests/mutation.rs` on the stable
//! toolchain in the default test suite, and (b) the libFuzzer targets in
//! `/fuzz` (nightly + cargo-fuzz) for coverage-guided campaigns.

#![allow(clippy::unwrap_used, clippy::expect_used)] // harness setup only; inputs never reach these

use std::sync::OnceLock;

use stratadb_common::codec::Decoder;
use stratadb_common::TenantId;
use stratadb_schema::codec::{decode_batch, decode_event, DecodeLimits};
use stratadb_schema::Limits;

fn tenant() -> TenantId {
    TenantId::new("fuzz").unwrap()
}

pub fn fuzz_wal_frames(data: &[u8]) {
    let _ = stratadb_wal::format::decode_header(data, "fuzz");
    for version in [1, 2] {
        let mut pos = 0;
        while pos < data.len() {
            match stratadb_wal::format::parse_frame(&data[pos..], version) {
                Ok(f) => pos += f.encoded_len(),
                Err(_) => break,
            }
        }
        let _ = stratadb_wal::format::find_later_valid_frame(data, 1, version);
        let _ = stratadb_wal::format::classify_invalid_tail(data, 1, version);
    }
}

/// Full WAL recovery over an arbitrary final segment file.
pub fn fuzz_wal_recovery(data: &[u8]) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join(stratadb_wal::format::segment_file_name(1)),
        data,
    )
    .unwrap();
    let mut replayed = 0usize;
    let _ = stratadb_wal::Wal::open(
        dir.path(),
        stratadb_wal::WalOptions::default(),
        0,
        |_, _, p| {
            replayed += p.len();
            let _ = decode_batch(p, &DecodeLimits::default());
            Ok(())
        },
    );
}

/// Segment open, verify, full read and Bloom decoding.
pub fn fuzz_segment(data: &[u8]) {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("s.sseg");
    std::fs::write(&p, data).unwrap();
    if let Ok(r) = stratadb_segment::SegmentReader::open(&p) {
        let _ = r.verify();
        let _ = r.read_rows();
        if let Some(x) = r.footer().extension(stratadb_segment::bloom::EXT_BLOOM) {
            if let Ok(b) = stratadb_segment::bloom::BloomSet::decode(&x.data) {
                for f in b.fields() {
                    let _ = b.get(f).map(|f| f.may_contain("x"));
                }
            }
        }
        if let Some(x) = r.footer().extension(stratadb_segment::ngram::EXT_NGRAM) {
            if let Ok(n) = stratadb_segment::ngram::NgramSet::decode(&x.data) {
                let probes = stratadb_segment::ngram::probes("%abc%");
                let _ = n.may_match(&stratadb_segment::ngram::NgramField::Body, &probes);
            }
        }
    }
}

/// Segment decoders that sit behind checksums (footer, columns, LZ4), driven
/// directly so mutations are not all rejected by CRC checks.
pub fn fuzz_segment_internals(data: &[u8]) {
    use stratadb_segment::fuzz_hooks as h;
    let _ = h::decode_footer(data, 1);
    let _ = h::decode_footer(data, 2);
    let _ = h::decode_footer(data, 3);
    if data.len() >= 4 {
        let (col, enc, rows) = (u16::from(data[0] % 16), data[1] % 8, usize::from(data[2]));
        let _ = h::decode_column(col, enc, &data[3..], rows);
        let raw = usize::from(data[3]) * 64;
        if let Ok(out) = h::decompress(&data[4..], raw) {
            let _ = h::decode_column(col, enc, &out, rows);
        }
    }
}

pub fn fuzz_manifest(data: &[u8]) {
    let _ = stratadb_storage::Manifest::decode(data);
    let _ = stratadb_storage::BackupManifest::decode(data);
}

pub fn fuzz_event_codec(data: &[u8]) {
    let lim = DecodeLimits::default();
    let _ = decode_batch(data, &lim);
    let mut d = Decoder::new(data, "fuzz");
    let _ = decode_event(&mut d, &lim);
    let _ = stratadb_storage::idempotency::decode_record(data);
    let _ = stratadb_storage::idempotency::decode_extension(data);
    let _ = stratadb_segment::bloom::BloomSet::decode(data);
    let _ = stratadb_segment::ngram::NgramSet::decode(data);
    let _ = stratadb_crypto::container::Header::decode(data, "fuzz");
    let _ = stratadb_crypto::Keyring::check_format(data);
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = stratadb_segment::ngram::probes(s);
    }
}

/// A small fixed dataset so fuzzed queries reach the executor.
fn query_engine() -> &'static (tempfile::TempDir, stratadb_storage::Engine) {
    static E: OnceLock<(tempfile::TempDir, stratadb_storage::Engine)> = OnceLock::new();
    E.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        let mut o = stratadb_storage::EngineOptions::new(dir.path());
        o.background = false;
        o.wal.durability = stratadb_storage::Durability::Async;
        let e = stratadb_storage::Engine::open(o).unwrap();
        let t = tenant();
        for chunk in 0..3 {
            let evs = (0..20)
                .map(|i| {
                    let v = serde_json::json!({"service": (["a", "b"][i % 2]), "severity": (["INFO", "ERROR"][i % 2]),
                        "body": format!("message {i}"), "trace_id": format!("{i:032x}"),
                        "attributes": {"n": i, "f": i as f64 / 2.0, "s": "x", "nested": {"k": [1, 2]}, "z": null}});
                    stratadb_schema::json::event_from_json(&v, &t, (chunk * 20 + i as i64) * 1_000_000_000, &Limits::default()).unwrap()
                })
                .collect();
            e.write_batch(&t, evs).unwrap();
            if chunk < 2 {
                e.flush().unwrap();
            }
        }
        (dir, e)
    })
}

/// Parse, plan and execute (with tight budgets) arbitrary query text.
pub fn fuzz_query(data: &[u8]) {
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };
    let (_, e) = query_engine();
    let opts = stratadb_query::QueryOptions {
        timeout: std::time::Duration::from_millis(200),
        max_rows_scanned: 10_000,
        max_result_rows: 1_000,
        max_memory_bytes: 8 << 20,
        ..Default::default()
    };
    let _ = stratadb_query::execute(&e.snapshot(&tenant()), sql, &opts);
}

pub fn fuzz_json_event(data: &[u8]) {
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(data) {
        let _ = stratadb_schema::json::event_from_json(
            &v,
            &tenant(),
            1_795_170_600_000_000_000,
            &Limits::default(),
        );
        let _ = stratadb_schema::otlp::map_export_request(
            &v,
            &tenant(),
            1_795_170_600_000_000_000,
            &Limits::default(),
        );
    }
}

/// OTLP/HTTP protobuf decoding + mapping.
pub fn fuzz_otlp_proto(data: &[u8]) {
    if let Ok(v) = stratadb_schema::otlp_proto::decode_export_request(data) {
        let _ = stratadb_schema::otlp::map_export_request(
            &v,
            &tenant(),
            1_795_170_600_000_000_000,
            &Limits::default(),
        );
    }
}

pub fn fuzz_config(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    if let Ok(f) = toml::from_str::<stratadb_server::config::FileConfig>(text) {
        let o = stratadb_server::config::Overrides {
            data_dir: Some("/nonexistent".into()),
            ..Default::default()
        };
        let _ = stratadb_server::config::resolve(&f, &o);
    }
}

/// Token store and audit log readers over arbitrary file contents.
pub fn fuzz_security_files(data: &[u8]) {
    let dir = tempfile::tempdir().unwrap();
    let tp = stratadb_server::auth::TokenStore::path_for(dir.path());
    std::fs::create_dir_all(tp.parent().unwrap()).unwrap();
    std::fs::write(&tp, data).unwrap();
    if let Ok(s) = stratadb_server::auth::TokenStore::load(dir.path()) {
        let _ = s.verify(&String::from_utf8_lossy(data));
    }
    let ap = stratadb_server::audit::AuditLog::path_for(dir.path());
    std::fs::create_dir_all(ap.parent().unwrap()).unwrap();
    std::fs::write(&ap, data).unwrap();
    let _ = stratadb_server::audit::verify(dir.path());
}

/// A fuzz entry point.
pub type Target = fn(&[u8]);

/// All targets with a name, for the mutation runner.
/// Queries whose cursors the `cursor` target exercises (byte 0 selects one).
const CURSOR_QUERIES: [&str; 4] = [
    "SELECT body FROM logs",
    "SELECT body FROM logs ORDER BY event_time DESC",
    "SELECT service, attributes['n'] FROM logs ORDER BY service, attributes['f'] DESC LIMIT 7",
    "SELECT body FROM logs WHERE attributes['nested'] IS NOT MISSING ORDER BY attributes['nested'], body",
];

/// Pagination cursors (client-supplied tokens): raw text, raw bodies, and
/// bodies wrapped with a valid version/fingerprint/CRC so fuzzed key values
/// reach the executor's cursor comparisons.
pub fn fuzz_cursor(data: &[u8]) {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    let _ = stratadb_query::page::fuzz_hooks::decode_body(rest);
    let (_, e) = query_engine();
    let t = tenant();
    let sql = CURSOR_QUERIES[usize::from(sel) % CURSOR_QUERIES.len()];
    let opts = stratadb_query::QueryOptions {
        max_rows_scanned: 10_000,
        max_result_rows: 100,
        timeout: std::time::Duration::from_secs(2),
        parallelism: 1,
        ..Default::default()
    };
    let snap = e.snapshot(&t);
    let page = |cursor: String| {
        let _ = stratadb_query::execute_page(
            &snap,
            sql,
            &opts,
            &stratadb_query::PageRequest {
                page_size: 1 + usize::from(sel >> 4),
                cursor: Some(cursor),
            },
        );
    };
    page(String::from_utf8_lossy(rest).into_owned());
    page(stratadb_query::page::fuzz_hooks::cursor_text(&t, sql, rest));
}

/// Key of the golden encrypted fixture (`tests/fixtures/encrypted-v1`).
const FIXTURE_KEY: [u8; 32] = [0x5a; 32];

fn fixture_keyring() -> &'static std::sync::Arc<stratadb_crypto::Keyring> {
    static K: OnceLock<std::sync::Arc<stratadb_crypto::Keyring>> = OnceLock::new();
    K.get_or_init(|| {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/encrypted-v1");
        let p =
            stratadb_crypto::StaticKeyProvider(stratadb_crypto::SecretKey::from_bytes(FIXTURE_KEY));
        std::sync::Arc::new(stratadb_crypto::Keyring::load(&dir, &p).unwrap().unwrap())
    })
}

/// Encryption-at-rest decoders: sealed container (header, length checks,
/// block authentication, random-access reads), KEYRING framing, and recovery
/// of an encrypted WAL segment — all with the fixture's real keys.
pub fn fuzz_sealed(data: &[u8]) {
    use stratadb_crypto::container;
    let keys = fixture_keyring();
    let _ = container::Header::decode(data, "fuzz");
    let _ = container::open_bytes(keys, data, "fuzz");
    let _ = stratadb_crypto::Keyring::check_format(data);
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("f");
    std::fs::write(&p, data).unwrap();
    if let Ok(f) = std::fs::File::open(&p) {
        if let Ok(s) = container::SealedFile::open(f, keys, &p) {
            let len = s.len();
            for (off, n) in [(0, 16), (len / 2, 100), (len.saturating_sub(16), 16)] {
                let mut buf = vec![0u8; n.min(4096)];
                let _ = s.read_exact_at(&mut buf, off);
            }
        }
    }
    // The same bytes as an encrypted WAL segment.
    let wal = tempfile::tempdir().unwrap();
    std::fs::write(
        wal.path().join(stratadb_wal::format::segment_file_name(1)),
        data,
    )
    .unwrap();
    let o = stratadb_wal::WalOptions {
        keys: Some(std::sync::Arc::clone(keys)),
        ..Default::default()
    };
    let _ = stratadb_wal::Wal::open(wal.path(), o, 0, |_, _, p| {
        let _ = stratadb_schema::codec::decode_batch(p, &DecodeLimits::default());
        Ok(())
    });
}

/// Attribute key projection over a map chunk and value skipping.
pub fn fuzz_map_projection(data: &[u8]) {
    let Some((&sel, rest)) = data.split_first() else {
        return;
    };
    const KEYS: [&str; 5] = ["", "a", "hot", "n", "zz"];
    let keys: Vec<String> = KEYS
        .iter()
        .enumerate()
        .filter(|(i, _)| sel & (1 << i) != 0)
        .map(|(_, k)| (*k).to_owned())
        .collect();
    let rows = 1 + usize::from(sel >> 5);
    let _ = stratadb_segment::fuzz_hooks::project_map_keys(rest, rows, &keys);
    let lim = DecodeLimits::default();
    let mut d = Decoder::new(rest, "fuzz");
    let _ = stratadb_schema::codec::skip_value(&mut d, &lim);
}

pub const TARGETS: &[(&str, Target)] = &[
    ("wal_frames", fuzz_wal_frames),
    ("wal_recovery", fuzz_wal_recovery),
    ("segment", fuzz_segment),
    ("segment_internals", fuzz_segment_internals),
    ("manifest", fuzz_manifest),
    ("event_codec", fuzz_event_codec),
    ("query", fuzz_query),
    ("json_event", fuzz_json_event),
    ("otlp_proto", fuzz_otlp_proto),
    ("config", fuzz_config),
    ("security_files", fuzz_security_files),
    ("cursor", fuzz_cursor),
    ("sealed", fuzz_sealed),
    ("map_projection", fuzz_map_projection),
];
