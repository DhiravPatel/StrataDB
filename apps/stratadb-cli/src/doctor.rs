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
        let (version, header_len) = match format::decode_header(&data, &p.display().to_string()) {
            Err(e) => {
                problems.push(format!("FAIL {e}"));
                continue;
            }
            Ok(h) => {
                if h.base_seq != *base {
                    problems.push(format!("FAIL {}: header/file name mismatch", p.display()));
                }
                (h.version, h.len())
            }
        };
        let mut pos = header_len;
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

/// Optional inputs of `strata doctor`.
#[derive(Debug, Default)]
pub struct Options {
    /// Server configuration file: enables TLS, backup and memory checks.
    pub config: Option<std::path::PathBuf>,
    /// Read every segment and check index filters against the rows.
    pub deep: bool,
    /// A backup older than this is reported as a warning.
    pub max_backup_age: std::time::Duration,
}

const DAY_MS: i64 = 86_400_000;
/// Per-query memory budget used by the server (QueryOptions default).
const QUERY_MEMORY: u64 = 256 << 20;

fn check_fd(segments: usize, wal_segments: usize, r: &mut Report) {
    // Segment files are cached in an LRU limited to half the soft limit
    // (spec STRATA-SYS-02-037): a low limit causes reopen churn, not
    // failures. Hard floor: WAL files, sockets, the lock file, logs and a
    // minimal segment cache. The server raises the soft limit to the hard
    // limit at startup (STRATA-SYS-02-036).
    let floor = (wal_segments + 128) as u64;
    let comfortable = 2 * (segments + wal_segments + 256) as u64;
    match crate::hostcheck::open_file_limit() {
        None => r.add(
            "fd_limit",
            "warn",
            "could not determine the open-file limit",
        ),
        Some(l) => {
            let shown = if l == u64::MAX {
                "unlimited".to_owned()
            } else {
                l.to_string()
            };
            let status = if l < floor {
                "fail"
            } else if l < comfortable {
                "warn"
            } else {
                "ok"
            };
            r.add(
                "fd_limit",
                status,
                format!(
                    "soft limit {shown} for processes started from this environment (the server raises it to the hard limit); {segments} segments share a file cache of half the limit, {wal_segments} WAL files stay open (fail below {floor}; below {comfortable} cold segment files are closed and reopened). The service manager may set its own limit (systemd unit: LimitNOFILE=65536)"
                ),
            );
        }
    }
}

fn check_memory(cfg: Option<&stratadb_server::config::Resolved>, r: &mut Report) {
    let Some(total) = crate::hostcheck::total_memory() else {
        return r.add("memory", "warn", "could not determine physical memory");
    };
    let (memtable, queries) = cfg.map_or((64u64 << 20, 16u64), |c| {
        (
            c.engine.memtable_max_bytes as u64,
            c.server.max_concurrent_queries as u64,
        )
    });
    // Active + one frozen memtable, plus every concurrent query at its budget.
    let worst = memtable * 2 + queries * QUERY_MEMORY;
    let status = if worst > total { "warn" } else { "ok" };
    r.add(
        "memory",
        status,
        format!(
            "{} MiB physical; worst case {} MiB ({} MiB memtables + {queries} concurrent queries × {} MiB budget){}",
            total >> 20,
            worst >> 20,
            (memtable * 2) >> 20,
            QUERY_MEMORY >> 20,
            if worst > total { " — lower limits.max_concurrent_queries" } else { "" }
        ),
    );
}

fn check_tls(cfg: &stratadb_server::config::Resolved, r: &mut Report) {
    let Some(tls) = &cfg.tls else {
        return r.add(
            "tls",
            "ok",
            "TLS not configured (loopback or behind a TLS proxy)",
        );
    };
    if let Err(e) = stratadb_server::tls::load_server_config(&tls.cert, &tls.key) {
        return r.add("tls", "fail", e.to_string());
    }
    let key_mode = mode(&tls.key);
    let leaf = stratadb_server::tls::read_leaf_certificate(&tls.cert).ok();
    let Some(not_after) = leaf.as_deref().and_then(crate::hostcheck::cert_not_after) else {
        return r.add(
            "tls",
            "warn",
            "certificate loads, but its expiry date could not be read",
        );
    };
    let days = (not_after * 1000 - time::now_unix_millis()).div_euclid(DAY_MS);
    let expiry = stratadb_common::time::format_rfc3339(not_after * 1_000_000_000);
    let (status, what) = if days < 0 {
        ("fail", format!("certificate EXPIRED on {expiry}"))
    } else if days < 30 {
        (
            "warn",
            format!("certificate expires in {days} days ({expiry}); renew it"),
        )
    } else {
        (
            "ok",
            format!("certificate valid for {days} more days ({expiry})"),
        )
    };
    let key_note = match key_mode {
        Some(m) if m & 0o077 != 0 => {
            format!("; private key mode {m:o} is group/other accessible (chmod 600)")
        }
        _ => String::new(),
    };
    let status = if !key_note.is_empty() && status == "ok" {
        "warn"
    } else {
        status
    };
    r.add("tls", status, format!("{what}{key_note}"));
}

fn check_backups(
    cfg: &stratadb_server::config::Resolved,
    max_age: std::time::Duration,
    r: &mut Report,
) {
    let Some(root) = &cfg.server.backup_root else {
        return r.add(
            "backups",
            "warn",
            "no backup directory configured ([backup] dir); a single node cannot survive loss of its disk without backups",
        );
    };
    let mut newest: Option<(i64, String)> = None;
    let mut bad = Vec::new();
    for e in std::fs::read_dir(root).into_iter().flatten().flatten() {
        let p = e.path().join("BACKUP");
        let Ok(raw) = std::fs::read(&p) else { continue };
        match stratadb_storage::BackupManifest::decode(&raw) {
            Ok(m) => {
                let name = e.file_name().to_string_lossy().into_owned();
                if newest.as_ref().is_none_or(|(t, _)| m.created_ms > *t) {
                    newest = Some((m.created_ms, name));
                }
            }
            Err(err) => bad.push(format!("{}: {err}", e.path().display())),
        }
    }
    let Some((t, name)) = newest else {
        return r.add(
            "backups",
            "warn",
            format!("no backups in {}", root.display()),
        );
    };
    let age_ms = time::now_unix_millis() - t;
    let max_ms = i64::try_from(max_age.as_millis()).unwrap_or(i64::MAX);
    let status = if !bad.is_empty() || age_ms > max_ms {
        "warn"
    } else {
        "ok"
    };
    r.add(
        "backups",
        status,
        format!(
            "newest backup `{name}` is {:.1} days old (warn after {:.1}){}; verify restores with `strata backup verify`",
            age_ms as f64 / DAY_MS as f64,
            max_ms as f64 / DAY_MS as f64,
            if bad.is_empty() { String::new() } else { format!("; unreadable: {}", bad.join("; ")) }
        ),
    );
}

/// Read every segment and check its filters for false negatives.
fn check_indexes_deep(
    dir: &Path,
    man: &Manifest,
    keys: Option<&stratadb_crypto::Keyring>,
    r: &mut Report,
) {
    let mut bad = Vec::new();
    let mut checked = 0;
    for e in &man.segments {
        let p = stratadb_storage::manifest::segment_path(dir, &e.tenant, e.id);
        let res = (|| -> stratadb_common::Result<Vec<String>> {
            let rd = stratadb_segment::SegmentReader::open_with(&p, keys)?;
            let rows = rd.read_rows()?;
            let refs: Vec<stratadb_segment::RowRef<'_>> = rows
                .iter()
                .map(|x| stratadb_segment::RowRef {
                    seq: x.seq,
                    idx: x.idx,
                    event: &x.event,
                })
                .collect();
            let mut fns = Vec::new();
            let f = rd.footer();
            if let Some(x) = f.extension(stratadb_segment::bloom::EXT_BLOOM) {
                fns.extend(
                    stratadb_segment::bloom::BloomSet::decode(&x.data)?.false_negatives(&refs),
                );
            }
            if let Some(x) = f.extension(stratadb_segment::ngram::EXT_NGRAM) {
                fns.extend(
                    stratadb_segment::ngram::NgramSet::decode(&x.data)?.false_negatives(&refs),
                );
            }
            Ok(fns)
        })();
        checked += 1;
        match res {
            Ok(f) if f.is_empty() => {}
            Ok(f) => bad.push(format!(
                "segment {}: {} miss stored values",
                e.id,
                f.join(", ")
            )),
            Err(err) => bad.push(format!("segment {}: {err}", e.id)),
        }
    }
    if bad.is_empty() {
        r.add(
            "indexes",
            "ok",
            format!("{checked} segments: no filter misses a stored value"),
        );
    } else {
        r.add(
            "indexes",
            "fail",
            format!("{}; run `strata indexes rebuild`", bad.join("; ")),
        );
    }
}

pub fn run(dir: &Path, key: Option<&dyn stratadb_crypto::KeyProvider>, opts: &Options) -> Report {
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

    // Server configuration (optional): validated with the server's own resolver.
    let cfg = match &opts.config {
        None => None,
        Some(path) => match stratadb_server::config::FileConfig::load(path).and_then(|f| {
            stratadb_server::config::resolve(
                &f,
                &stratadb_server::config::Overrides {
                    data_dir: Some(dir.to_path_buf()),
                    ..Default::default()
                },
            )
        }) {
            Ok(c) => {
                r.add("config", "ok", format!("{} is valid", path.display()));
                Some(c)
            }
            Err(e) => {
                r.add("config", "fail", format!("{}: {e}", path.display()));
                None
            }
        },
    };
    check_memory(cfg.as_ref(), &mut r);
    if let Some(c) = &cfg {
        check_tls(c, &mut r);
        check_backups(c, opts.max_backup_age, &mut r);
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

    // Encryption at rest (ADR-0024). WAL framing and checksums are checked
    // without the key; manifest and segment contents need it.
    let keys = match (stratadb_crypto::Keyring::exists(dir), key) {
        (false, None) => {
            r.add("encryption", "ok", "not enabled");
            None
        }
        (false, Some(_)) => {
            r.add(
                "encryption",
                "warn",
                "a key was given but the directory is not encrypted (the next open enables \
                 encryption for new files)",
            );
            None
        }
        (true, None) => {
            r.add(
                "encryption",
                "warn",
                "encrypted (KEYRING present); pass --key-file to verify the manifest and segments",
            );
            return r;
        }
        (true, Some(k)) => match stratadb_crypto::Keyring::load(dir, k) {
            Ok(Some(ring)) => {
                r.add(
                    "encryption",
                    "ok",
                    format!(
                        "enabled; key-encryption key {}, data keys {:?}, active {}",
                        ring.kek_id(),
                        ring.dek_ids(),
                        ring.active().0
                    ),
                );
                Some(ring)
            }
            Ok(None) => None,
            Err(e) => {
                r.add("encryption", "fail", e.to_string());
                return r;
            }
        },
    };
    let man = match Manifest::load(dir, keys.as_ref()) {
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
        match stratadb_segment::SegmentReader::open_with(&p, keys.as_ref())
            .and_then(|rd| rd.verify().map(|()| rd))
        {
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
                if let Some(x) = rd.footer().extension(stratadb_segment::ngram::EXT_NGRAM) {
                    if let Err(err) = stratadb_segment::ngram::NgramSet::decode(&x.data) {
                        fails.push(format!(
                            "segment {} trigram filters undecodable: {err}",
                            e.id
                        ));
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
    let wal_segments = std::fs::read_dir(dir.join("wal"))
        .map(|d| d.count())
        .unwrap_or(0);
    check_fd(man.segments.len(), wal_segments, &mut r);
    if opts.deep {
        check_indexes_deep(dir, &man, keys.as_ref(), &mut r);
    }

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
