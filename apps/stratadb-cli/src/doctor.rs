//! `strata doctor` (spec §22.3): read-only diagnostics of a data directory.
//!
//! Never opens the engine (that would run recovery and could truncate a WAL
//! tail) and never prints secrets. Each check reports `ok`, `warn` or `fail`.

use std::collections::BTreeSet;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use serde_json::{json, Value};
use stratadb_common::time;
use stratadb_storage::Manifest;
use stratadb_wal::format;

pub struct Report {
    pub checks: Vec<Value>,
}

impl Report {
    fn add(&mut self, name: &str, status: &str, detail: impl Into<String>) {
        self.checks
            .push(json!({"check": name, "status": status, "detail": detail.into()}));
    }

    pub fn worst(&self) -> &'static str {
        let has = |s: &str| self.checks.iter().any(|c| c["status"] == s);
        if has("fail") {
            "fail"
        } else if has("warn") {
            "warn"
        } else {
            "ok"
        }
    }
}

fn mode(p: &Path) -> Option<u32> {
    std::fs::metadata(p)
        .ok()
        .map(|m| m.permissions().mode() & 0o777)
}

fn walk_perms(dir: &Path, bad: &mut Vec<String>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        let Ok(ft) = e.file_type() else { continue };
        if ft.is_symlink() {
            bad.push(format!(
                "{} is a symlink (ignored by StrataDB)",
                p.display()
            ));
        } else if ft.is_dir() {
            if mode(&p).is_some_and(|m| m & 0o077 != 0) {
                bad.push(format!("{} is group/other accessible", p.display()));
            }
            walk_perms(&p, bad);
        } else if mode(&p).is_some_and(|m| m & 0o077 != 0) {
            bad.push(format!("{} is group/other accessible", p.display()));
        }
    }
}

/// Read-only scan of the WAL directory.
fn check_wal(dir: &Path, flushed: u64, r: &mut Report) {
    let wal = dir.join("wal");
    let mut segs: Vec<(u64, std::path::PathBuf)> = std::fs::read_dir(&wal)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    format::parse_segment_file_name(&name).map(|b| (b, e.path()))
                })
                .collect()
        })
        .unwrap_or_default();
    segs.sort();
    let (mut frames, mut bytes) = (0u64, 0u64);
    let mut problems = Vec::new();
    let mut last_seq = 0u64;
    for (i, (base, p)) in segs.iter().enumerate() {
        let Ok(data) = std::fs::read(p) else {
            problems.push(format!("{} unreadable", p.display()));
            continue;
        };
        bytes += data.len() as u64;
        let version = match format::decode_header(&data, &p.display().to_string()) {
            Err(e) => {
                problems.push(format!("FAIL {e}"));
                continue;
            }
            Ok(h) => {
                if h.base_seq != *base {
                    problems.push(format!("FAIL {}: header/file name mismatch", p.display()));
                }
                h.version
            }
        };
        let mut pos = format::HEADER_LEN;
        let mut next = *base;
        while pos < data.len() {
            match format::parse_frame(&data[pos..], version) {
                Ok(f) if f.seq == next => {
                    frames += 1;
                    last_seq = f.seq;
                    next += 1;
                    pos += f.encoded_len();
                }
                Ok(f) => {
                    problems.push(format!(
                        "FAIL {}: sequence {} where {next} expected",
                        p.display(),
                        f.seq
                    ));
                    break;
                }
                Err(reason) => {
                    // Same classification as WAL recovery.
                    let kind = format::classify_invalid_tail(&data[pos + 1..], next, version);
                    let last = i + 1 == segs.len();
                    if last && kind != format::InvalidTail::PossiblyDurableDamage {
                        problems.push(format!(
                            "WARN {}: {} at offset {pos} ({reason}); next open will truncate {} un-synced bytes",
                            p.display(),
                            if kind == format::InvalidTail::Torn {
                                "torn tail"
                            } else {
                                "un-synced tail persisted out of order (power loss)"
                            },
                            data.len() - pos
                        ));
                    } else {
                        problems.push(format!(
                            "FAIL {}: {reason} at offset {pos} with valid data after it (mid-log corruption; restore or inspect)",
                            p.display()
                        ));
                    }
                    break;
                }
            }
        }
    }
    if let Some((base, _)) = segs.first() {
        if base.saturating_sub(1) > flushed {
            problems.push(format!(
                "FAIL WAL starts at {base} but the manifest covers only through {flushed} (gap)"
            ));
        }
    }
    let detail = format!(
        "{} segments, {frames} frames, {bytes} bytes, last seq {last_seq}, {} frames above flushed boundary {flushed}",
        segs.len(),
        frames.saturating_sub(flushed.min(frames))
    );
    let status = if problems.iter().any(|p| p.starts_with("FAIL")) {
        "fail"
    } else if problems.is_empty() {
        "ok"
    } else {
        "warn"
    };
    let mut d = detail;
    for p in problems {
        d.push_str("; ");
        d.push_str(&p);
    }
    r.add("wal", status, d);
}

pub fn run(dir: &Path) -> Report {
    let mut r = Report { checks: Vec::new() };
    r.add(
        "version",
        "ok",
        format!(
            "strata {} reads WAL v{}, segment v{}..=v{}, manifest v{}",
            env!("CARGO_PKG_VERSION"),
            format::FORMAT_VERSION,
            stratadb_segment::MIN_READ_VERSION,
            stratadb_segment::FORMAT_VERSION,
            stratadb_storage::manifest::MANIFEST_FORMAT_VERSION
        ),
    );
    if !dir.is_dir() {
        r.add(
            "data_dir",
            "fail",
            format!("{} does not exist", dir.display()),
        );
        return r;
    }
    match mode(dir) {
        Some(m) if m & 0o077 == 0 => {
            r.add("data_dir", "ok", format!("{} mode {m:o}", dir.display()))
        }
        Some(m) => r.add(
            "data_dir",
            "warn",
            format!("{} mode {m:o}: should be 0700", dir.display()),
        ),
        None => r.add("data_dir", "fail", "cannot stat data directory"),
    }
    let mut bad = Vec::new();
    walk_perms(dir, &mut bad);
    if bad.is_empty() {
        r.add("permissions", "ok", "all files owner-only, no symlinks");
    } else {
        r.add("permissions", "warn", bad.join("; "));
    }

    // Lock: is a server/CLI using the directory right now?
    match std::fs::File::open(dir.join("LOCK")) {
        Ok(f) => match f.try_lock() {
            Ok(()) => {
                let _ = f.unlock();
                r.add("lock", "ok", "not in use");
            }
            Err(_) => r.add(
                "lock",
                "warn",
                "in use by another process (results may be a moving target)",
            ),
        },
        Err(_) => r.add("lock", "ok", "no LOCK file (never opened)"),
    }

    match (fs4::available_space(dir), fs4::total_space(dir)) {
        (Ok(free), Ok(total)) => {
            let reserve = (512u64 << 20).max((total as f64 * 0.02) as u64);
            let status = if free < reserve {
                "fail"
            } else if free < reserve * 4 {
                "warn"
            } else {
                "ok"
            };
            r.add(
                "disk",
                status,
                format!("{free} B free of {total} B (default write reserve {reserve} B)"),
            );
        }
        _ => r.add("disk", "warn", "could not measure free space"),
    }

    let man = match Manifest::load(dir) {
        Ok(Some(m)) => {
            r.add(
                "manifest",
                "ok",
                format!(
                    "version {}, {} segments, flushed through {}",
                    m.version,
                    m.segments.len(),
                    m.flushed_through_seq
                ),
            );
            m
        }
        Ok(None) => {
            r.add(
                "manifest",
                "fail",
                "no MANIFEST (not a StrataDB data directory, or it was deleted)",
            );
            return r;
        }
        Err(e) => {
            r.add("manifest", "fail", e.to_string());
            return r;
        }
    };

    // Segments: full verification + Bloom decoding, against manifest entries.
    let (mut ok, mut rows, mut bytes, mut blooms) = (0usize, 0u64, 0u64, 0usize);
    let mut fails = Vec::new();
    let mut unsupported = BTreeSet::new();
    for e in &man.segments {
        if e.segment_format_version > stratadb_segment::FORMAT_VERSION {
            unsupported.insert(e.segment_format_version);
        }
        let p = stratadb_storage::manifest::segment_path(dir, &e.tenant, e.id);
        match stratadb_segment::SegmentReader::open(&p).and_then(|rd| rd.verify().map(|()| rd)) {
            Ok(rd) if rd.footer_crc() == e.footer_crc && rd.footer().row_count == e.row_count => {
                ok += 1;
                rows += e.row_count;
                bytes += e.file_bytes;
                if let Some(x) = rd.footer().extension(stratadb_segment::bloom::EXT_BLOOM) {
                    match stratadb_segment::bloom::BloomSet::decode(&x.data) {
                        Ok(_) => blooms += 1,
                        Err(err) => {
                            fails.push(format!("segment {} bloom filters undecodable: {err}", e.id))
                        }
                    }
                }
            }
            Ok(_) => fails.push(format!(
                "segment {} does not match its manifest entry",
                e.id
            )),
            Err(err) => fails.push(format!("segment {}: {err}", e.id)),
        }
    }
    if !unsupported.is_empty() {
        r.add(
            "format_compatibility",
            "fail",
            format!("segment format versions {unsupported:?} are newer than this binary"),
        );
    } else {
        r.add(
            "format_compatibility",
            "ok",
            "all formats readable by this binary",
        );
    }
    r.add(
        "segments",
        if fails.is_empty() { "ok" } else { "fail" },
        format!(
            "{ok}/{} verified ({rows} rows, {bytes} bytes, {blooms} with Bloom filters){}",
            man.segments.len(),
            if fails.is_empty() {
                String::new()
            } else {
                format!("; {}", fails.join("; "))
            }
        ),
    );

    // Unreferenced files / quarantine / obsolete.
    let referenced: BTreeSet<(String, u64)> = man
        .segments
        .iter()
        .map(|s| (s.tenant.to_string(), s.id))
        .collect();
    let mut orphans = 0;
    if let Ok(td) = std::fs::read_dir(dir.join("segments")) {
        for t in td.flatten() {
            let tn = t.file_name().to_string_lossy().into_owned();
            for f in std::fs::read_dir(t.path()).into_iter().flatten().flatten() {
                if let Some(id) =
                    stratadb_segment::parse_segment_file_name(&f.file_name().to_string_lossy())
                {
                    if !referenced.contains(&(tn.clone(), id)) {
                        orphans += 1;
                    }
                }
            }
        }
    }
    let quarantined = std::fs::read_dir(dir.join("quarantine"))
        .map(|d| d.count())
        .unwrap_or(0);
    let status = if quarantined > 0 { "warn" } else { "ok" };
    r.add("unreferenced", status, format!(
        "{orphans} unreferenced segment files (cleaned on next open), {} obsolete pending deletion, {quarantined} quarantined",
        man.obsolete_segments.len()
    ));

    check_wal(dir, man.flushed_through_seq, &mut r);

    let now = time::now_unix_millis();
    match man.segments.iter().map(|s| s.created_ms).max() {
        Some(t) if t > now + 60_000 => r.add(
            "clock",
            "warn",
            format!(
                "newest segment created {} ms in the future: clock moved backwards?",
                t - now
            ),
        ),
        _ => r.add("clock", "ok", "no segment creation times in the future"),
    }

    let tok = dir.join("auth").join("tokens.json");
    if tok.exists() {
        match mode(&tok) {
            Some(0o600) => r.add(
                "tokens",
                "ok",
                "token store present, mode 600 (verifiers only)",
            ),
            Some(m) => r.add(
                "tokens",
                "warn",
                format!("token store mode {m:o}; should be 600"),
            ),
            None => r.add("tokens", "warn", "cannot stat token store"),
        }
    }
    match stratadb_server::audit::verify(dir) {
        Ok(0) => r.add("audit", "ok", "no audit log"),
        Ok(n) => r.add("audit", "ok", format!("hash chain intact ({n} records)")),
        Err(e) => r.add("audit", "fail", e.to_string()),
    }
    r
}
