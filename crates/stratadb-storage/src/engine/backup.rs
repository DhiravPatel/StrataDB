//! Backup, verification and restore (spec §19, ADR-0010).
//!
//! # Backup layout (`<dest>/`)
//! ```text
//! BACKUP                       envelope: "STRATADB-BACKUP v1 sha256=<hex> len=<n>\n" + JSON
//! MANIFEST                     the data-directory manifest at the backup point
//! segments/<tenant>/seg-*.sseg byte-for-byte copies of committed segments
//! KEYRING                      encrypted databases only: the wrapped data keys
//! ```
//! # Encryption (ADR-0024)
//! A backup of an encrypted database stays encrypted: segments and MANIFEST
//! are copied as sealed files and the KEYRING (data keys wrapped by the KEK)
//! is included. Verifying or restoring it requires the KEK that wrapped that
//! KEYRING — keep retired KEKs until backups made with them have expired.
//! # Consistency
//! The engine flushes, then captures the manifest and pins every committed
//! segment under the manifest lock. The backup point is that manifest's
//! `flushed_through_seq`: every write acknowledged before the backup call
//! started is included; later writes may or may not be. Segments are
//! immutable and pinned, so they are copied while ingestion continues.
//! Auth tokens are **not** included (secrets stay out of backups).
//!
//! # Atomicity
//! The backup is assembled in `<dest>.tmp` and renamed into place after every
//! file and directory is fsynced; an interrupted backup never looks complete.

use std::fs;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::*;

const BACKUP_FILE: &str = "BACKUP";
const BACKUP_PREFIX: &str = "STRATADB-BACKUP v1 ";
pub const BACKUP_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupFile {
    /// Relative path inside the backup (validated on read).
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    pub format_version: u32,
    pub created_ms: i64,
    pub stratadb_version: String,
    pub manifest_version: u64,
    pub flushed_through_seq: u64,
    pub segments: usize,
    pub rows: u64,
    pub files: Vec<BackupFile>,
}

#[derive(Debug, Clone, Default)]
pub struct BackupReport {
    pub path: PathBuf,
    pub manifest_version: u64,
    pub flushed_through_seq: u64,
    pub segments: usize,
    pub rows: u64,
    pub bytes: u64,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct RestoreReport {
    pub data_dir: PathBuf,
    pub manifest_version: u64,
    pub flushed_through_seq: u64,
    pub segments: usize,
    pub rows: u64,
    pub elapsed: Duration,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Copy `from` to `to` (new file, 0600), fsync it, return (bytes, sha256 hex).
fn copy_hashed(from: &Path, to: &Path) -> Result<(u64, String)> {
    if let Some(p) = to.parent() {
        fsutil::create_dir_durable(p)?;
    }
    let io = stratadb_common::io::default_io();
    let src = stratadb_common::io::IoFile::open(std::sync::Arc::clone(&io), from)?;
    let dst = io.create_new(to)?;
    let mut h = Sha256::new();
    let mut total = 0u64;
    for (off, n) in chunks(stratadb_common::io::RandomRead::len(&src)) {
        let buf = src.read_vec(off, n)?;
        h.update(&buf);
        stratadb_common::io::write_all_at(&*dst, &buf, off)?;
        total += n as u64;
    }
    dst.sync(false)?;
    io.forget(src.file());
    Ok((total, hex(&h.finalize())))
}

/// `(offset, len)` pieces of at most 1 MiB covering `len` bytes.
fn chunks(len: u64) -> impl Iterator<Item = (u64, usize)> {
    const STEP: u64 = 1 << 20;
    (0..len.div_ceil(STEP)).map(move |i| {
        let off = i * STEP;
        (off, (len - off).min(STEP) as usize)
    })
}

fn hash_file(p: &Path) -> Result<(u64, String)> {
    let io = stratadb_common::io::default_io();
    let f = stratadb_common::io::IoFile::open(std::sync::Arc::clone(&io), p)?;
    let mut h = Sha256::new();
    let mut total = 0u64;
    for (off, n) in chunks(stratadb_common::io::RandomRead::len(&f)) {
        h.update(&f.read_vec(off, n)?);
        total += n as u64;
    }
    io.forget(f.file());
    Ok((total, hex(&h.finalize())))
}

fn fsync_tree(dir: &Path) -> Result<()> {
    for d in fsutil::list_dirs(dir)? {
        fsync_tree(&d)?;
    }
    fsutil::fsync_dir(dir)
}

fn ensure_absent_or_empty(p: &Path, what: &str) -> Result<()> {
    if p.exists() {
        let empty = p.is_dir()
            && fs::read_dir(p)
                .ctx(|| format!("listing {}", p.display()))?
                .next()
                .is_none();
        if !empty {
            return Err(Error::new(
                ErrorKind::AlreadyExists,
                format!(
                    "{what} {} already exists and is not empty; refusing to overwrite",
                    p.display()
                ),
            ));
        }
    }
    Ok(())
}

impl BackupManifest {
    fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_vec_pretty(self).map_err(|e| Error::internal(e.to_string()))?;
        let mut out = format!(
            "{BACKUP_PREFIX}sha256={} len={}\n",
            hex(&Sha256::digest(&json)),
            json.len()
        )
        .into_bytes();
        out.extend_from_slice(&json);
        Ok(out)
    }

    /// Parse and validate a `BACKUP` file (public for fuzzing and tooling).
    pub fn decode(b: &[u8]) -> Result<Self> {
        let loc = BACKUP_FILE;
        let nl = b
            .iter()
            .position(|&c| c == b'\n')
            .filter(|&p| p < 200)
            .ok_or_else(|| Error::corruption(loc, "missing header line"))?;
        let header = std::str::from_utf8(&b[..nl])
            .map_err(|_| Error::corruption(loc, "header not UTF-8"))?;
        let rest = header
            .strip_prefix(BACKUP_PREFIX)
            .ok_or_else(|| Error::corruption(loc, "not a StrataDB v1 backup"))?;
        let mut sha = None;
        let mut len = None;
        for part in rest.split(' ') {
            if let Some(v) = part.strip_prefix("sha256=") {
                sha = Some(v.to_owned());
            } else if let Some(v) = part.strip_prefix("len=") {
                len = v.parse::<usize>().ok();
            }
        }
        let body = &b[nl + 1..];
        if Some(body.len()) != len || sha.as_deref() != Some(hex(&Sha256::digest(body)).as_str()) {
            return Err(Error::corruption(
                loc,
                "backup manifest checksum/length mismatch",
            ));
        }
        let m: Self =
            serde_json::from_slice(body).map_err(|e| Error::corruption(loc, e.to_string()))?;
        if m.format_version != BACKUP_FORMAT_VERSION {
            return Err(Error::FormatVersion {
                what: "backup",
                found: m.format_version,
                supported: BACKUP_FORMAT_VERSION.to_string(),
            });
        }
        // Paths must be exactly the derived segment paths (no traversal).
        for f in &m.files {
            let ok = f.path == crate::manifest::MANIFEST_FILE
                || f.path == stratadb_crypto::KEYRING_FILE
                || {
                    let parts: Vec<&str> = f.path.split('/').collect();
                    parts.len() == 3
                        && parts[0] == "segments"
                        && TenantId::new(parts[1]).is_ok()
                        && stratadb_segment::parse_segment_file_name(parts[2]).is_some()
                };
            if !ok {
                return Err(Error::corruption(
                    loc,
                    format!("illegal file path {:?}", f.path),
                ));
            }
        }
        Ok(m)
    }
}

/// A backup's manifest (envelope checksum only; no file verification). For
/// listings; use [`verify_backup`] before trusting the data.
pub fn read_backup_manifest(dir: &Path) -> Result<BackupManifest> {
    let raw = stratadb_common::io::read_file(
        &*stratadb_common::io::default_io(),
        &dir.join(BACKUP_FILE),
    )?;
    BackupManifest::decode(&raw)
}

/// Verify a backup without modifying it: envelope checksum, every file's
/// size and SHA-256, the data manifest, and full verification of every segment.
/// An encrypted backup needs the KEK that wrapped its KEYRING.
pub fn verify_backup(
    dir: &Path,
    key: Option<&dyn stratadb_crypto::KeyProvider>,
) -> Result<BackupManifest> {
    let raw = stratadb_common::io::read_file(
        &*stratadb_common::io::default_io(),
        &dir.join(BACKUP_FILE),
    )?;
    let bm = BackupManifest::decode(&raw)?;
    for f in &bm.files {
        let p = dir.join(&f.path);
        let (bytes, sha) = hash_file(&p)?;
        if bytes != f.bytes || sha != f.sha256 {
            return Err(Error::corruption(
                p.display().to_string(),
                "backup file does not match its recorded SHA-256/size",
            ));
        }
    }
    let keys = match (stratadb_crypto::Keyring::exists(dir), key) {
        (false, _) => None,
        (true, None) => {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "this backup is encrypted; its key-encryption key is required",
            ))
        }
        (true, Some(p)) => stratadb_crypto::Keyring::load(dir, p)?,
    };
    let k = keys.as_ref();
    if k.is_some()
        && !bm
            .files
            .iter()
            .any(|f| f.path == stratadb_crypto::KEYRING_FILE)
    {
        return Err(Error::corruption(
            dir.display().to_string(),
            "KEYRING is not listed in BACKUP",
        ));
    }
    let man = Manifest::load(dir, k)?
        .ok_or_else(|| Error::corruption(dir.display().to_string(), "backup has no MANIFEST"))?;
    if man.version != bm.manifest_version || man.segments.len() != bm.segments {
        return Err(Error::corruption(
            dir.display().to_string(),
            "backup MANIFEST does not match BACKUP",
        ));
    }
    let listed: BTreeSet<&str> = bm.files.iter().map(|f| f.path.as_str()).collect();
    for e in &man.segments {
        let rel = format!(
            "segments/{}/{}",
            e.tenant,
            stratadb_segment::segment_file_name(e.id)
        );
        if !listed.contains(rel.as_str()) {
            return Err(Error::corruption(
                rel,
                "segment referenced by MANIFEST is missing from the backup",
            ));
        }
        open_segment(dir, e, true, k, &stratadb_common::io::default_io())?;
    }
    Ok(bm)
}

/// Restore `backup` into `target` (must be absent or empty), then open it
/// with full verification. The engine at `target` is closed before returning.
pub fn restore_backup(
    backup: &Path,
    target: &Path,
    key: Option<Arc<dyn stratadb_crypto::KeyProvider>>,
) -> Result<RestoreReport> {
    let started = Instant::now();
    ensure_absent_or_empty(target, "restore target")?;
    let bm = verify_backup(backup, key.as_deref())?;
    let tmp = fsutil::tmp_path(target);
    if tmp.exists() {
        fs::remove_dir_all(&tmp).ctx(|| format!("removing stale {}", tmp.display()))?;
    }
    fsutil::create_dir_durable(&tmp)?;
    for f in &bm.files {
        let (bytes, sha) = copy_hashed(&backup.join(&f.path), &tmp.join(&f.path))?;
        if bytes != f.bytes || sha != f.sha256 {
            return Err(Error::corruption(
                f.path.clone(),
                "file changed while restoring",
            ));
        }
    }
    fsutil::create_dir_durable(&tmp.join(WAL_DIR))?;
    fsync_tree(&tmp)?;
    if target.exists() {
        fs::remove_dir(target).ctx(|| format!("removing empty {}", target.display()))?;
    }
    fs::rename(&tmp, target).ctx(|| format!("renaming {} into place", tmp.display()))?;
    if let Some(parent) = target.parent() {
        fsutil::fsync_dir(parent)?;
    }
    // Consistency check: a full open with segment verification.
    let mut o = EngineOptions::new(target);
    o.background = false;
    o.verify_segments_on_open = true;
    o.flush_on_close = false;
    o.encryption = key;
    let e = Engine::open(o)?;
    let st = e.stats();
    e.close()?;
    Ok(RestoreReport {
        data_dir: target.to_path_buf(),
        manifest_version: bm.manifest_version,
        flushed_through_seq: bm.flushed_through_seq,
        segments: st.segments,
        rows: st.segment_rows,
        elapsed: started.elapsed(),
    })
}

impl Shared {
    pub(super) fn backup(&self, dest: &Path) -> Result<BackupReport> {
        let started = Instant::now();
        ensure_absent_or_empty(dest, "backup destination")?;
        self.flush()?;
        // Capture the manifest and pin its segments atomically w.r.t. maintenance.
        let (man, pinned): (Manifest, Vec<Arc<SegmentHandle>>) = {
            let man = self.manifest.lock();
            let segs = self.state.lock().all_segments();
            (man.clone(), segs.iter().cloned().collect())
        };
        let tmp = fsutil::tmp_path(dest);
        if tmp.exists() {
            fs::remove_dir_all(&tmp).ctx(|| format!("removing stale {}", tmp.display()))?;
        }
        fsutil::create_dir_durable(&tmp)?;
        let needed: u64 = pinned.iter().map(|h| h.entry.file_bytes).sum::<u64>() + (1 << 20);
        let free = fs4::available_space(&tmp)
            .ctx(|| format!("measuring free space at {}", tmp.display()))?;
        if free < needed {
            let _ = fs::remove_dir_all(&tmp);
            return Err(Error::resource_exhausted(format!(
                "backup needs ~{needed} B but only {free} B are free at the destination"
            )));
        }
        let mut files = Vec::new();
        let mut bytes_total = 0u64;
        for h in &pinned {
            let rel = format!(
                "segments/{}/{}",
                h.entry.tenant,
                stratadb_segment::segment_file_name(h.entry.id)
            );
            let (bytes, sha256) = copy_hashed(h.reader.path(), &tmp.join(&rel))?;
            failpoint::hit("backup::after_segment_copy")?;
            // Verify the copy, not just the source.
            open_segment(
                &tmp,
                &h.entry,
                true,
                self.keys.as_deref(),
                &self.opts.io_engine(),
            )?;
            bytes_total += bytes;
            files.push(BackupFile {
                path: rel,
                bytes,
                sha256,
            });
        }
        let mut bman = man.clone();
        bman.obsolete_segments.clear();
        bman.segments
            .retain(|s| pinned.iter().any(|h| h.entry.id == s.id));
        bman.commit_with(&tmp, self.keys.as_deref(), &*self.opts.io_engine())?;
        let (mbytes, msha) = hash_file(&tmp.join(crate::manifest::MANIFEST_FILE))?;
        files.push(BackupFile {
            path: crate::manifest::MANIFEST_FILE.into(),
            bytes: mbytes,
            sha256: msha,
        });
        if self.keys.is_some() {
            // The wrapped data keys; useless without the KEK.
            let (kbytes, ksha) = copy_hashed(
                &stratadb_crypto::Keyring::path(&self.dir),
                &tmp.join(stratadb_crypto::KEYRING_FILE),
            )?;
            files.push(BackupFile {
                path: stratadb_crypto::KEYRING_FILE.into(),
                bytes: kbytes,
                sha256: ksha,
            });
        }
        let rows = bman.segments.iter().map(|s| s.row_count).sum();
        let bm = BackupManifest {
            format_version: BACKUP_FORMAT_VERSION,
            created_ms: time::now_unix_millis(),
            stratadb_version: env!("CARGO_PKG_VERSION").to_owned(),
            manifest_version: bman.version,
            flushed_through_seq: bman.flushed_through_seq,
            segments: bman.segments.len(),
            rows,
            files,
        };
        fsutil::atomic_write(&tmp.join(BACKUP_FILE), &bm.encode()?, "backup")?;
        fsync_tree(&tmp)?;
        if dest.exists() {
            fs::remove_dir(dest).ctx(|| format!("removing empty {}", dest.display()))?;
        }
        fs::rename(&tmp, dest).ctx(|| format!("publishing backup {}", dest.display()))?;
        if let Some(parent) = dest.parent() {
            fsutil::fsync_dir(parent)?;
        }
        drop(pinned);
        Ok(BackupReport {
            path: dest.to_path_buf(),
            manifest_version: bm.manifest_version,
            flushed_through_seq: bm.flushed_through_seq,
            segments: bm.segments,
            rows,
            bytes: bytes_total,
            elapsed: started.elapsed(),
        })
    }
}
