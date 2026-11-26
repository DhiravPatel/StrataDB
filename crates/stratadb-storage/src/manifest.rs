//! The manifest: the single authoritative list of committed segments and the
//! WAL boundary they cover (spec §10.5, ticket D-006).
//!
//! On disk (`<data_dir>/MANIFEST`), replaced atomically via write-tmp, fsync,
//! rename, directory fsync:
//!
//! ```text
//! STRATADB-MANIFEST v1 crc32c=<8 hex> len=<decimal>\n
//! <len bytes of JSON>
//! ```
//!
//! JSON is used for inspectability; the checksum and length make truncation
//! and bit rot detectable. Unknown JSON fields are rejected (fail closed).
//! Segment file paths are *derived* from (tenant, id) — never read from the
//! manifest — so a tampered manifest cannot redirect reads outside the data dir.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use stratadb_common::checksum::crc32c;
use stratadb_common::{fsutil, Error, Result, TenantId};

pub const MANIFEST_FILE: &str = "MANIFEST";
pub const MANIFEST_FORMAT_VERSION: u32 = 1;
const MAGIC_PREFIX: &str = "STRATADB-MANIFEST v1 ";
const MAX_MANIFEST_BYTES: usize = 256 * 1024 * 1024;

/// Visibility state of a segment in the manifest. Only `Committed` segments
/// are query-visible. (`building`/`prepared` exist only as files not yet in
/// the manifest; `obsolete`/`deleted` are represented by removal.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentState {
    Committed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentEntry {
    pub id: u64,
    pub tenant: TenantId,
    pub segment_format_version: u16,
    pub min_schema_version: u32,
    pub max_schema_version: u32,
    pub min_event_time: i64,
    pub max_event_time: i64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub row_count: u64,
    pub file_bytes: u64,
    pub footer_crc: u32,
    pub created_ms: i64,
    pub state: SegmentState,
    /// Segment IDs this segment was compacted from (empty for flush output).
    pub lineage: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format_version: u32,
    /// Incremented on every commit.
    pub version: u64,
    pub next_segment_id: u64,
    /// Every WAL record with `seq <= flushed_through_seq` is durably
    /// represented in committed segments.
    pub flushed_through_seq: u64,
    pub segments: Vec<SegmentEntry>,
    /// Segments removed from the visible set (compaction/retention) whose
    /// files may still exist. Recovery deletes these files; they are never
    /// query-visible. (Spec §10.4 `obsolete` state.)
    #[serde(default)]
    pub obsolete_segments: Vec<ObsoleteSegment>,
    /// Per-tenant retention policies (spec §16.4), keyed by tenant id.
    #[serde(default)]
    pub retention: BTreeMap<TenantId, RetentionPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObsoleteSegment {
    pub id: u64,
    pub tenant: TenantId,
}

/// Time-based retention for one tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPolicy {
    /// Rows with `event_time < now - max_age_ms` become eligible for deletion.
    pub max_age_ms: i64,
    /// Legal hold: when true, nothing is deleted for this tenant.
    #[serde(default)]
    pub hold: bool,
}

impl Manifest {
    pub fn initial() -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            version: 1,
            next_segment_id: 1,
            flushed_through_seq: 0,
            segments: Vec::new(),
            obsolete_segments: Vec::new(),
            retention: BTreeMap::new(),
        }
    }

    /// Check structural invariants (unique IDs, IDs below next_segment_id).
    pub fn validate(&self) -> Result<()> {
        let loc = MANIFEST_FILE;
        if self.format_version != MANIFEST_FORMAT_VERSION {
            return Err(Error::FormatVersion {
                what: "manifest",
                found: self.format_version,
                supported: MANIFEST_FORMAT_VERSION.to_string(),
            });
        }
        let mut ids = std::collections::BTreeSet::new();
        for s in &self.segments {
            if !ids.insert(s.id) {
                return Err(Error::corruption(
                    loc,
                    format!("duplicate segment id {}", s.id),
                ));
            }
            if s.id >= self.next_segment_id {
                return Err(Error::corruption(
                    loc,
                    format!(
                        "segment id {} >= next_segment_id {}",
                        s.id, self.next_segment_id
                    ),
                ));
            }
            if s.min_seq > s.max_seq || s.max_seq > self.flushed_through_seq {
                return Err(Error::corruption(
                    loc,
                    format!("segment {} sequence range exceeds flushed boundary", s.id),
                ));
            }
        }
        for o in &self.obsolete_segments {
            if ids.contains(&o.id) || o.id >= self.next_segment_id {
                return Err(Error::corruption(
                    loc,
                    format!(
                        "obsolete segment {} is still referenced or was never allocated",
                        o.id
                    ),
                ));
            }
        }
        for (t, r) in &self.retention {
            if r.max_age_ms <= 0 {
                return Err(Error::corruption(
                    loc,
                    format!("retention for {t} must be positive"),
                ));
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let json = serde_json::to_vec_pretty(self)
            .map_err(|e| Error::internal(format!("serializing manifest: {e}")))?;
        let mut out = format!(
            "{MAGIC_PREFIX}crc32c={:08x} len={}\n",
            crc32c(&json),
            json.len()
        )
        .into_bytes();
        out.extend_from_slice(&json);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let loc = MANIFEST_FILE;
        let nl = bytes
            .iter()
            .position(|&b| b == b'\n')
            .filter(|&p| p < 128)
            .ok_or_else(|| Error::corruption(loc, "missing manifest header line"))?;
        let header = std::str::from_utf8(&bytes[..nl])
            .map_err(|_| Error::corruption(loc, "manifest header is not UTF-8"))?;
        let rest = header
            .strip_prefix(MAGIC_PREFIX)
            .ok_or_else(|| Error::corruption(loc, "bad manifest magic/version"))?;
        let mut crc = None;
        let mut len = None;
        for part in rest.split(' ') {
            if let Some(v) = part.strip_prefix("crc32c=") {
                crc = u32::from_str_radix(v, 16).ok();
            } else if let Some(v) = part.strip_prefix("len=") {
                len = v.parse::<usize>().ok();
            }
        }
        let (crc, len) = match (crc, len) {
            (Some(c), Some(l)) if l <= MAX_MANIFEST_BYTES => (c, l),
            _ => return Err(Error::corruption(loc, "malformed manifest header")),
        };
        let body = &bytes[nl + 1..];
        if body.len() != len {
            return Err(Error::corruption(
                loc,
                format!("manifest body is {} bytes, header says {len}", body.len()),
            ));
        }
        if crc32c(body) != crc {
            return Err(Error::corruption(loc, "manifest checksum mismatch"));
        }
        let m: Manifest = serde_json::from_slice(body)
            .map_err(|e| Error::corruption(loc, format!("manifest JSON invalid: {e}")))?;
        m.validate()?;
        Ok(m)
    }

    /// Load `<dir>/MANIFEST`; `Ok(None)` if it does not exist. A sealed
    /// (encrypted) manifest needs `keys`.
    pub fn load(dir: &Path, keys: Option<&stratadb_crypto::Keyring>) -> Result<Option<Self>> {
        Self::load_with(dir, keys, &*stratadb_common::io::default_io())
    }

    /// [`Manifest::load`] reading through `io`.
    pub fn load_with(
        dir: &Path,
        keys: Option<&stratadb_crypto::Keyring>,
        io: &dyn stratadb_common::io::IoEngine,
    ) -> Result<Option<Self>> {
        let p = dir.join(MANIFEST_FILE);
        match stratadb_common::io::read_file(io, &p) {
            Ok(b) => Self::decode_any(&b, keys).map(Some),
            Err(e) if stratadb_common::io::is_not_found(&e) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Decode plain or sealed manifest bytes.
    pub fn decode_any(bytes: &[u8], keys: Option<&stratadb_crypto::Keyring>) -> Result<Self> {
        if stratadb_crypto::container::is_sealed(bytes) {
            let keys = keys.ok_or_else(|| {
                Error::new(
                    stratadb_common::ErrorKind::PermissionDenied,
                    "the MANIFEST is encrypted; configure the data directory's key",
                )
            })?;
            Self::decode(&stratadb_crypto::container::open_bytes(
                keys,
                bytes,
                MANIFEST_FILE,
            )?)
        } else {
            Self::decode(bytes)
        }
    }

    /// Durably and atomically publish this manifest (sealed when `keys`).
    pub fn commit(&self, dir: &Path, keys: Option<&stratadb_crypto::Keyring>) -> Result<()> {
        self.validate()?;
        let mut bytes = self.encode()?;
        if let Some(k) = keys {
            bytes = stratadb_crypto::container::seal(
                k,
                stratadb_crypto::container::KIND_MANIFEST,
                &bytes,
            )?;
        }
        fsutil::atomic_write(&dir.join(MANIFEST_FILE), &bytes, "manifest")
    }
}

/// `<data_dir>/segments/<tenant>/seg-<id>.sseg`. Tenant IDs are validated to a
/// path-safe alphabet, so this cannot escape the data directory.
pub fn segment_path(data_dir: &Path, tenant: &TenantId, id: u64) -> PathBuf {
    tenant_dir(data_dir, tenant).join(stratadb_segment::segment_file_name(id))
}

pub fn tenant_dir(data_dir: &Path, tenant: &TenantId) -> PathBuf {
    data_dir.join("segments").join(tenant.as_str())
}

/// Read a manifest file's raw bytes (diagnostics).
pub fn read_raw(dir: &Path) -> Result<Vec<u8>> {
    let p = dir.join(MANIFEST_FILE);
    stratadb_common::io::read_file(&*stratadb_common::io::default_io(), &p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Manifest {
        let mut m = Manifest::initial();
        m.next_segment_id = 3;
        m.flushed_through_seq = 10;
        m.segments.push(SegmentEntry {
            id: 1,
            tenant: TenantId::new("t").unwrap(),
            segment_format_version: 1,
            min_schema_version: 1,
            max_schema_version: 1,
            min_event_time: 0,
            max_event_time: 5,
            min_seq: 1,
            max_seq: 10,
            row_count: 4,
            file_bytes: 100,
            footer_crc: 7,
            created_ms: 0,
            state: SegmentState::Committed,
            lineage: vec![],
        });
        m
    }

    #[test]
    fn roundtrip_and_commit() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Manifest::load(dir.path(), None).unwrap().is_none());
        let m = sample();
        m.commit(dir.path(), None).unwrap();
        assert_eq!(Manifest::load(dir.path(), None).unwrap().unwrap(), m);
    }

    #[test]
    fn every_byte_flip_or_truncation_detected() {
        let bytes = sample().encode().unwrap();
        for i in 0..bytes.len() {
            let mut b = bytes.clone();
            b[i] ^= 0x01;
            assert!(Manifest::decode(&b).is_err(), "flip at {i} undetected");
        }
        for cut in 0..bytes.len() {
            assert!(Manifest::decode(&bytes[..cut]).is_err());
        }
    }

    #[test]
    fn invariants_enforced() {
        let mut m = sample();
        m.segments.push(m.segments[0].clone());
        assert!(m.validate().is_err());
        let mut m = sample();
        m.next_segment_id = 1;
        assert!(m.validate().is_err());
        let mut m = sample();
        m.flushed_through_seq = 5;
        assert!(m.validate().is_err());
    }

    #[test]
    fn unknown_fields_rejected() {
        let mut v: serde_json::Value = serde_json::to_value(sample()).unwrap();
        v["surprise"] = serde_json::json!(1);
        let json = serde_json::to_vec(&v).unwrap();
        let mut bytes = format!(
            "{MAGIC_PREFIX}crc32c={:08x} len={}\n",
            crc32c(&json),
            json.len()
        )
        .into_bytes();
        bytes.extend(json);
        assert!(Manifest::decode(&bytes).is_err());
    }
}
