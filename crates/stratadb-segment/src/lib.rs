//! Immutable segment format v1 (spec §10, epic D). Normative description:
//! `docs/storage-format/segment-v1.md`. Status: **provisional** — see ADR-0003;
//! the format is versioned so it can be replaced after the Parquet evaluation.
//!
//! ```text
//! header  (16 B): magic b"STRSEG\r\n" | version u16 | flags u16 | reserved u32
//! column chunks : back to back, each independently CRC-32C checksummed
//! footer        : metadata + column directory + statistics + extensions
//! trailer (16 B): footer_len u32 | footer_crc u32 | magic b"STRSEG\r\n"
//! ```
//!
//! Invariants:
//! * A segment holds rows of exactly one tenant (storage-level isolation).
//! * Rows are sorted by `(event_time, seq, idx)`.
//! * Every byte read from disk is covered by a checksum before it is decoded.
//! * Readers reject unknown *required* extensions and ignore unknown optional ones.

pub mod bloom;
mod columns;
pub mod ngram;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use stratadb_common::checksum::crc32c;
use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{failpoint, fsutil, Error, IoResultExt, Result, TenantId};
use stratadb_schema::{Event, Severity};

pub use columns::{ColumnData, ColumnId, Encoding};

pub const MAGIC: [u8; 8] = *b"STRSEG\r\n";
/// Segment format written by this build (RFC-0001 v2 compression, RFC-0002 v3
/// shredded attributes). Readers accept 1..=3.
pub const FORMAT_VERSION: u16 = 3;
/// A key is shredded when present in at least 1/SHRED_MIN_FRACTION_DIV of rows.
pub const SHRED_MIN_FRACTION_DIV: usize = 10;
pub const MAX_SHREDDED_PER_SCOPE: usize = 64;
/// Oldest segment format this build can read.
pub const MIN_READ_VERSION: u16 = 1;
/// Upper bound on a decompressed chunk (decompression-bomb guard).
pub const MAX_CHUNK_RAW_BYTES: u64 = 1 << 30;

/// Chunk compression codes (persisted; never renumber).
pub mod compression {
    pub const NONE: u8 = 0;
    pub const LZ4: u8 = 1;
}

/// Writer settings.
#[derive(Debug, Clone, Copy)]
pub struct WriteOptions {
    /// Compress chunks with LZ4 when it saves at least 1/8 of the chunk.
    pub compress: bool,
    /// Store hot attribute keys as separate columns (format v3, RFC-0002).
    pub shred: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            compress: true,
            shred: true,
        }
    }
}

/// Which attribute map a shredded column belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Scope {
    Attributes = 0,
    Resource = 1,
}

/// Directory entry for one shredded attribute key (format v3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShreddedColumn {
    pub scope: Scope,
    pub key: String,
    pub offset: u64,
    pub len: u64,
    pub crc: u32,
    pub missing: u64,
    pub compression: u8,
    pub raw_len: u64,
}
pub const HEADER_LEN: usize = 16;
pub const TRAILER_LEN: usize = 16;
/// Upper bound on footer size accepted by the reader.
pub const MAX_FOOTER_BYTES: usize = 64 * 1024 * 1024;
/// Distinct attribute keys recorded in the footer before giving up (overflow).
pub const MAX_TRACKED_KEYS: usize = 1024;

/// A row to be written: WAL sequence number, index within that WAL record,
/// and the event.
#[derive(Debug, Clone, Copy)]
pub struct RowRef<'a> {
    pub seq: u64,
    pub idx: u32,
    pub event: &'a Event,
}

/// A row read back from a segment.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredRow {
    pub seq: u64,
    pub idx: u32,
    pub event: Event,
}

/// Directory entry for one column chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMeta {
    pub id: ColumnId,
    pub encoding: Encoding,
    pub offset: u64,
    pub len: u64,
    pub crc: u32,
    /// Rows where the value is missing (absent optional field / empty map).
    pub missing: u64,
    /// `compression::NONE` or `compression::LZ4` (always NONE in v1 files).
    pub compression: u8,
    /// Decompressed chunk length (equals `len` when uncompressed).
    pub raw_len: u64,
}

/// Distinct attribute keys present in the segment, for pruning predicates on
/// attributes that no row has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySet {
    Exact(BTreeSet<String>),
    /// Too many distinct keys; no pruning possible.
    Overflow,
}

impl KeySet {
    pub fn may_contain(&self, key: &str) -> bool {
        match self {
            KeySet::Exact(s) => s.contains(key),
            KeySet::Overflow => true,
        }
    }
}

/// An extension section (forward compatibility).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extension {
    pub tag: u16,
    pub required: bool,
    pub data: Vec<u8>,
}

/// Segment metadata stored in the footer.
#[derive(Debug, Clone, PartialEq)]
pub struct Footer {
    pub segment_id: u64,
    pub tenant_id: TenantId,
    pub row_count: u64,
    pub min_event_time: i64,
    pub max_event_time: i64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub min_schema_version: u32,
    pub max_schema_version: u32,
    pub created_ms: i64,
    pub columns: Vec<ColumnMeta>,
    /// Shredded attribute keys (v3; empty for v1/v2).
    pub shredded: Vec<ShreddedColumn>,
    pub attribute_keys: KeySet,
    pub resource_keys: KeySet,
    pub severity_counts: BTreeMap<u8, u64>,
    pub extensions: Vec<Extension>,
}

impl Footer {
    pub fn column(&self, id: ColumnId) -> Option<&ColumnMeta> {
        self.columns.iter().find(|c| c.id == id)
    }

    pub fn shredded(&self, scope: Scope, key: &str) -> Option<&ShreddedColumn> {
        self.shredded
            .iter()
            .find(|c| c.scope == scope && c.key == key)
    }

    /// The first extension with `tag`, if present.
    pub fn extension(&self, tag: u16) -> Option<&Extension> {
        self.extensions.iter().find(|x| x.tag == tag)
    }
}

/// Information about a written segment file.
#[derive(Debug, Clone)]
pub struct WrittenSegment {
    pub path: PathBuf,
    pub file_bytes: u64,
    pub footer_crc: u32,
    pub footer: Footer,
}

fn key_set(keys: BTreeSet<String>) -> KeySet {
    if keys.len() > MAX_TRACKED_KEYS {
        KeySet::Overflow
    } else {
        KeySet::Exact(keys)
    }
}

fn encode_keyset(e: &mut Encoder, k: &KeySet) {
    match k {
        KeySet::Overflow => e.put_u8(1),
        KeySet::Exact(s) => {
            e.put_u8(0);
            e.put_uvarint(s.len() as u64);
            for key in s {
                e.put_str(key);
            }
        }
    }
}

fn decode_keyset(d: &mut Decoder<'_>) -> Result<KeySet> {
    match d.get_u8()? {
        1 => Ok(KeySet::Overflow),
        0 => {
            let n = d.get_len(MAX_TRACKED_KEYS, 1)?;
            let mut s = BTreeSet::new();
            for _ in 0..n {
                s.insert(d.get_string(64 * 1024)?);
            }
            Ok(KeySet::Exact(s))
        }
        other => Err(Error::corruption(
            "segment footer",
            format!("bad keyset marker {other}"),
        )),
    }
}

fn encode_footer(f: &Footer) -> Vec<u8> {
    let mut e = Encoder::with_capacity(512);
    e.put_u64(f.segment_id);
    e.put_str(f.tenant_id.as_str());
    e.put_uvarint(f.row_count);
    e.put_ivarint(f.min_event_time);
    e.put_ivarint(f.max_event_time);
    e.put_uvarint(f.min_seq);
    e.put_uvarint(f.max_seq);
    e.put_uvarint(u64::from(f.min_schema_version));
    e.put_uvarint(u64::from(f.max_schema_version));
    e.put_ivarint(f.created_ms);
    e.put_uvarint(f.columns.len() as u64);
    for c in &f.columns {
        e.put_u16(c.id as u16);
        e.put_u8(c.encoding as u8);
        e.put_uvarint(c.offset);
        e.put_uvarint(c.len);
        e.put_u32(c.crc);
        e.put_uvarint(c.missing);
        e.put_u8(c.compression);
        e.put_uvarint(c.raw_len);
    }
    e.put_uvarint(f.shredded.len() as u64);
    for c in &f.shredded {
        e.put_u8(c.scope as u8);
        e.put_str(&c.key);
        e.put_uvarint(c.offset);
        e.put_uvarint(c.len);
        e.put_u32(c.crc);
        e.put_uvarint(c.missing);
        e.put_u8(c.compression);
        e.put_uvarint(c.raw_len);
    }
    encode_keyset(&mut e, &f.attribute_keys);
    encode_keyset(&mut e, &f.resource_keys);
    e.put_uvarint(f.severity_counts.len() as u64);
    for (s, n) in &f.severity_counts {
        e.put_u8(*s);
        e.put_uvarint(*n);
    }
    e.put_uvarint(f.extensions.len() as u64);
    for x in &f.extensions {
        e.put_u16(x.tag);
        e.put_u8(u8::from(x.required));
        e.put_bytes(&x.data);
    }
    e.into_inner()
}

fn decode_footer(b: &[u8], loc: &str, version: u16) -> Result<Footer> {
    let mut d = Decoder::new(b, "segment footer");
    let segment_id = d.get_u64()?;
    let tenant_id = TenantId::new(d.get_str(64)?)
        .map_err(|e| Error::corruption(loc, format!("invalid tenant id in footer: {e}")))?;
    let row_count = d.get_uvarint()?;
    let min_event_time = d.get_ivarint()?;
    let max_event_time = d.get_ivarint()?;
    let min_seq = d.get_uvarint()?;
    let max_seq = d.get_uvarint()?;
    let sv =
        |v: u64| u32::try_from(v).map_err(|_| Error::corruption(loc, "schema version overflow"));
    let min_schema_version = sv(d.get_uvarint()?)?;
    let max_schema_version = sv(d.get_uvarint()?)?;
    let created_ms = d.get_ivarint()?;
    let ncols = d.get_len(256, 8)?;
    let mut columns = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let raw_id = d.get_u16()?;
        let id = ColumnId::from_u16(raw_id)
            .ok_or_else(|| Error::unsupported(format!("{loc}: unknown column id {raw_id}")))?;
        let raw_enc = d.get_u8()?;
        let encoding = Encoding::from_u8(raw_enc)
            .ok_or_else(|| Error::unsupported(format!("{loc}: unknown encoding {raw_enc}")))?;
        let offset = d.get_uvarint()?;
        let len = d.get_uvarint()?;
        let crc = d.get_u32()?;
        let missing = d.get_uvarint()?;
        let (compression, raw_len) = if version >= 2 {
            let c = d.get_u8()?;
            let raw = d.get_uvarint()?;
            if c != compression::NONE && c != compression::LZ4 {
                return Err(Error::unsupported(format!(
                    "{loc}: unknown chunk compression {c}"
                )));
            }
            if raw > MAX_CHUNK_RAW_BYTES || (c == compression::NONE && raw != len) {
                return Err(Error::corruption(
                    loc,
                    format!("column {id:?}: implausible raw length {raw}"),
                ));
            }
            (c, raw)
        } else {
            (compression::NONE, len)
        };
        columns.push(ColumnMeta {
            id,
            encoding,
            offset,
            len,
            crc,
            missing,
            compression,
            raw_len,
        });
    }
    let mut shredded = Vec::new();
    if version >= 3 {
        let n = d.get_len(2 * MAX_SHREDDED_PER_SCOPE, 12)?;
        for _ in 0..n {
            let scope = match d.get_u8()? {
                0 => Scope::Attributes,
                1 => Scope::Resource,
                s => {
                    return Err(Error::corruption(
                        loc,
                        format!("unknown shredded scope {s}"),
                    ))
                }
            };
            let key = d.get_string(64 * 1024)?;
            let (offset, len, crc, missing) = (
                d.get_uvarint()?,
                d.get_uvarint()?,
                d.get_u32()?,
                d.get_uvarint()?,
            );
            let compression = d.get_u8()?;
            let raw_len = d.get_uvarint()?;
            if compression != compression::NONE && compression != compression::LZ4 {
                return Err(Error::unsupported(format!(
                    "{loc}: unknown chunk compression {compression}"
                )));
            }
            if raw_len > MAX_CHUNK_RAW_BYTES || (compression == compression::NONE && raw_len != len)
            {
                return Err(Error::corruption(
                    loc,
                    format!("shredded {key:?}: implausible raw length"),
                ));
            }
            if shredded
                .iter()
                .any(|c: &ShreddedColumn| c.scope == scope && c.key == key)
            {
                return Err(Error::corruption(
                    loc,
                    format!("duplicate shredded key {key:?}"),
                ));
            }
            shredded.push(ShreddedColumn {
                scope,
                key,
                offset,
                len,
                crc,
                missing,
                compression,
                raw_len,
            });
        }
    }
    let attribute_keys = decode_keyset(&mut d)?;
    let resource_keys = decode_keyset(&mut d)?;
    let nsev = d.get_len(256, 2)?;
    let mut severity_counts = BTreeMap::new();
    for _ in 0..nsev {
        let s = d.get_u8()?;
        severity_counts.insert(s, d.get_uvarint()?);
    }
    let next = d.get_len(1024, 4)?;
    let mut extensions = Vec::with_capacity(next);
    for _ in 0..next {
        let tag = d.get_u16()?;
        let required = d.get_u8()? != 0;
        let data = d.get_bytes(MAX_FOOTER_BYTES)?.to_vec();
        if required {
            return Err(Error::unsupported(format!(
                "{loc}: segment requires unknown extension {tag}"
            )));
        }
        extensions.push(Extension {
            tag,
            required,
            data,
        });
    }
    d.expect_end()?;
    if min_seq > max_seq || (row_count > 0 && min_event_time > max_event_time) {
        return Err(Error::corruption(loc, "footer bounds are inconsistent"));
    }
    Ok(Footer {
        segment_id,
        tenant_id,
        row_count,
        min_event_time,
        max_event_time,
        min_seq,
        max_seq,
        min_schema_version,
        max_schema_version,
        created_ms,
        columns,
        shredded,
        attribute_keys,
        resource_keys,
        severity_counts,
        extensions,
    })
}

/// Fuzzing hooks that bypass checksums so decoders behind them get coverage.
/// Not part of the stable API.
#[doc(hidden)]
pub mod fuzz_hooks {
    use super::*;

    /// Decode footer bytes as segment format `version` (no CRC check).
    pub fn decode_footer(bytes: &[u8], version: u16) -> Result<Footer> {
        super::decode_footer(bytes, "fuzz", version)
    }

    /// Decode a column chunk for (`column_id`, `encoding`) and `rows` (no CRC check).
    pub fn decode_column(
        column_id: u16,
        encoding: u8,
        bytes: &[u8],
        rows: usize,
    ) -> Result<ColumnData> {
        let id = ColumnId::from_u16(column_id).ok_or_else(|| Error::unsupported("column id"))?;
        let enc = Encoding::from_u8(encoding).ok_or_else(|| Error::unsupported("encoding"))?;
        columns::decode_column(id, enc, bytes, rows)
    }

    /// Project a `Map`-encoded chunk onto `keys` (no CRC check).
    pub fn project_map_keys(
        bytes: &[u8],
        rows: usize,
        keys: &[String],
    ) -> Result<Vec<Vec<Option<stratadb_schema::Value>>>> {
        columns::project_map_keys(bytes, rows, keys)
    }

    /// LZ4 block decompression with the reader's bound (no CRC check).
    pub fn decompress(bytes: &[u8], raw_len: usize) -> Result<Vec<u8>> {
        if raw_len as u64 > MAX_CHUNK_RAW_BYTES {
            return Err(Error::corruption("fuzz", "raw length too big"));
        }
        lz4_flex::block::decompress(bytes, raw_len)
            .map_err(|e| Error::corruption("fuzz", e.to_string()))
    }
}

/// File name of segment `id`.
pub fn segment_file_name(id: u64) -> String {
    format!("seg-{id:020}.sseg")
}

pub fn parse_segment_file_name(name: &str) -> Option<u64> {
    let d = name.strip_prefix("seg-")?.strip_suffix(".sseg")?;
    if d.len() != 20 || !d.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    d.parse().ok()
}

/// Write a segment durably to `path` (via `path.tmp`, fsync, rename, dir fsync).
///
/// After return the file is in the `prepared` state: durable but not yet
/// visible. It becomes `committed` only when a manifest referencing it is
/// durably published. `rows` must all belong to `tenant` and be non-empty.
pub fn write_segment(
    path: &Path,
    segment_id: u64,
    tenant: &TenantId,
    rows: &mut [RowRef<'_>],
    created_ms: i64,
) -> Result<WrittenSegment> {
    write_segment_with_extensions(path, segment_id, tenant, rows, created_ms, Vec::new())
}

/// Like [`write_segment_opts`] with default options (compression on).
pub fn write_segment_with_extensions(
    path: &Path,
    segment_id: u64,
    tenant: &TenantId,
    rows: &mut [RowRef<'_>],
    created_ms: i64,
    extensions: Vec<Extension>,
) -> Result<WrittenSegment> {
    write_segment_opts(
        path,
        segment_id,
        tenant,
        rows,
        created_ms,
        extensions,
        &WriteOptions::default(),
    )
}

/// Write a segment with footer extension sections (tags in
/// `docs/storage-format/segment-v1.md`) and writer options.
pub fn write_segment_opts(
    path: &Path,
    segment_id: u64,
    tenant: &TenantId,
    rows: &mut [RowRef<'_>],
    created_ms: i64,
    extensions: Vec<Extension>,
    opts: &WriteOptions,
) -> Result<WrittenSegment> {
    write_segment_sealed(
        path, segment_id, tenant, rows, created_ms, extensions, opts, None,
    )
}

/// Like [`write_segment_opts`]; with `keys`, the file is written as a sealed
/// container (encryption at rest, ADR-0024) of the same segment bytes.
#[allow(clippy::too_many_arguments)]
pub fn write_segment_sealed(
    path: &Path,
    segment_id: u64,
    tenant: &TenantId,
    rows: &mut [RowRef<'_>],
    created_ms: i64,
    extensions: Vec<Extension>,
    opts: &WriteOptions,
    keys: Option<&stratadb_crypto::Keyring>,
) -> Result<WrittenSegment> {
    if rows.is_empty() {
        return Err(Error::internal("refusing to write an empty segment"));
    }
    if let Some(r) = rows.iter().find(|r| &r.event.tenant_id != tenant) {
        return Err(Error::internal(format!(
            "segment for tenant {tenant} received a row of tenant {}",
            r.event.tenant_id
        )));
    }
    rows.sort_by_key(|r| (r.event.event_time, r.seq, r.idx));

    let mut body = Encoder::with_capacity(rows.len() * 128);
    body.put_raw(&MAGIC);
    body.put_u16(FORMAT_VERSION);
    body.put_u16(0);
    body.put_u32(0);

    // Appends one chunk; returns (offset, stored len, crc, compression, raw len).
    let put_chunk = |body: &mut Encoder, raw: Vec<u8>| -> (u64, u64, u32, u8, u64) {
        let raw_len = raw.len() as u64;
        // Compress only when it saves at least 1/8 (RFC-0001).
        let (compression, bytes) = match opts.compress {
            true => {
                let c = lz4_flex::block::compress(&raw);
                if c.len() + raw.len() / 8 <= raw.len() {
                    (compression::LZ4, c)
                } else {
                    (compression::NONE, raw)
                }
            }
            false => (compression::NONE, raw),
        };
        let offset = body.len() as u64;
        body.put_raw(&bytes);
        (
            offset,
            bytes.len() as u64,
            crc32c(&bytes),
            compression,
            raw_len,
        )
    };
    // Hot keys per scope (RFC-0002).
    let hot = |resource: bool| -> BTreeSet<String> {
        if !opts.shred {
            return BTreeSet::new();
        }
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        for r in rows.iter() {
            let m = if resource {
                &r.event.resource_attributes
            } else {
                &r.event.attributes
            };
            for k in m.keys() {
                *counts.entry(k.as_str()).or_insert(0) += 1;
            }
        }
        let mut v: Vec<(&str, usize)> = counts
            .into_iter()
            .filter(|(_, c)| c * SHRED_MIN_FRACTION_DIV >= rows.len())
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        v.into_iter()
            .take(MAX_SHREDDED_PER_SCOPE)
            .map(|(k, _)| k.to_owned())
            .collect()
    };
    let (hot_attr, hot_res) = (hot(false), hot(true));

    let mut metas = Vec::new();
    for id in ColumnId::ALL {
        let (encoding, raw, missing) = match id {
            ColumnId::Attributes => columns::encode_map_excluding(rows, false, &hot_attr),
            ColumnId::ResourceAttributes => columns::encode_map_excluding(rows, true, &hot_res),
            _ => columns::encode_column(id, rows),
        };
        let (offset, len, crc, compression, raw_len) = put_chunk(&mut body, raw);
        metas.push(ColumnMeta {
            id,
            encoding,
            offset,
            len,
            crc,
            missing,
            compression,
            raw_len,
        });
    }
    let mut shredded = Vec::new();
    for (scope, keys) in [(Scope::Attributes, &hot_attr), (Scope::Resource, &hot_res)] {
        for key in keys {
            let (raw, missing) = columns::encode_shredded(rows, scope == Scope::Resource, key);
            let (offset, len, crc, compression, raw_len) = put_chunk(&mut body, raw);
            shredded.push(ShreddedColumn {
                scope,
                key: key.clone(),
                offset,
                len,
                crc,
                missing,
                compression,
                raw_len,
            });
        }
    }

    let mut attr_keys = BTreeSet::new();
    let mut res_keys = BTreeSet::new();
    let mut severity_counts = BTreeMap::new();
    for r in rows.iter() {
        if attr_keys.len() <= MAX_TRACKED_KEYS {
            attr_keys.extend(r.event.attributes.keys().cloned());
        }
        if res_keys.len() <= MAX_TRACKED_KEYS {
            res_keys.extend(r.event.resource_attributes.keys().cloned());
        }
        *severity_counts
            .entry(r.event.severity.as_u8())
            .or_insert(0u64) += 1;
    }
    let footer = Footer {
        segment_id,
        tenant_id: tenant.clone(),
        row_count: rows.len() as u64,
        min_event_time: rows.iter().map(|r| r.event.event_time).min().unwrap_or(0),
        max_event_time: rows.iter().map(|r| r.event.event_time).max().unwrap_or(0),
        min_seq: rows.iter().map(|r| r.seq).min().unwrap_or(0),
        max_seq: rows.iter().map(|r| r.seq).max().unwrap_or(0),
        min_schema_version: rows
            .iter()
            .map(|r| r.event.schema_version)
            .min()
            .unwrap_or(0),
        max_schema_version: rows
            .iter()
            .map(|r| r.event.schema_version)
            .max()
            .unwrap_or(0),
        created_ms,
        columns: metas,
        shredded,
        attribute_keys: key_set(attr_keys),
        resource_keys: key_set(res_keys),
        severity_counts,
        extensions,
    };
    let fbytes = encode_footer(&footer);
    let footer_crc = crc32c(&fbytes);
    body.put_raw(&fbytes);
    body.put_u32(u32::try_from(fbytes.len()).map_err(|_| Error::internal("footer too large"))?);
    body.put_u32(footer_crc);
    body.put_raw(&MAGIC);
    let mut bytes = body.into_inner();
    if let Some(k) = keys {
        bytes =
            stratadb_crypto::container::seal(k, stratadb_crypto::container::KIND_SEGMENT, &bytes)?;
    }

    let tmp = fsutil::tmp_path(path);
    fsutil::remove_if_exists(&tmp)?;
    let mut f = fsutil::create_new_file(&tmp)?;
    if let Some(action) = failpoint::eval("segment::torn_write") {
        let _ = f.write_all(&bytes[..bytes.len() / 2]);
        let _ = f.sync_all();
        failpoint::fire("segment::torn_write", action)?;
    }
    f.write_all(&bytes)
        .ctx(|| format!("writing segment {}", tmp.display()))?;
    fsutil::sync_file(&f, &tmp, false).ctx(|| format!("fsyncing {}", tmp.display()))?;
    drop(f);
    std::fs::rename(&tmp, path).ctx(|| format!("renaming {}", tmp.display()))?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::internal("segment path has no parent"))?;
    fsutil::fsync_dir(parent)?;
    Ok(WrittenSegment {
        path: path.to_path_buf(),
        file_bytes: bytes.len() as u64,
        footer_crc,
        footer,
    })
}

/// Plain file or sealed (encrypted) container.
#[derive(Debug)]
enum Source {
    Plain(File),
    Sealed(Box<stratadb_crypto::container::SealedFile>),
}

impl Source {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64, loc: &str) -> Result<()> {
        match self {
            Source::Plain(f) => f
                .read_exact_at(buf, offset)
                .ctx(|| format!("reading {loc}")),
            Source::Sealed(s) => s.read_exact_at(buf, offset),
        }
    }
}

/// Read access to one segment file. Opening validates header, trailer and
/// footer checksum; column chunks are checksummed when read.
#[derive(Debug)]
pub struct SegmentReader {
    path: PathBuf,
    file: Source,
    /// Logical (plaintext) segment length.
    file_len: u64,
    footer: Footer,
    footer_crc: u32,
}

impl SegmentReader {
    /// Open a plaintext segment (a sealed one needs [`SegmentReader::open_with`]).
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with(path, None)
    }

    /// Open a segment; sealed (encrypted) segments are decrypted with `keys`.
    pub fn open_with(path: &Path, keys: Option<&stratadb_crypto::Keyring>) -> Result<Self> {
        let loc = path.display().to_string();
        let raw = File::open(path).ctx(|| format!("opening segment {loc}"))?;
        let mut magic = [0u8; 8];
        let sealed = raw.read_exact_at(&mut magic, 0).is_ok()
            && stratadb_crypto::container::is_sealed(&magic);
        let file = if sealed {
            let keys = keys.ok_or_else(|| {
                Error::new(
                    stratadb_common::ErrorKind::PermissionDenied,
                    format!("{loc} is encrypted; configure the data directory's key"),
                )
            })?;
            Source::Sealed(Box::new(stratadb_crypto::container::SealedFile::open(
                raw, keys, path,
            )?))
        } else {
            Source::Plain(raw)
        };
        let file_len = match &file {
            Source::Plain(f) => f.metadata().ctx(|| format!("stat {loc}"))?.len(),
            Source::Sealed(s) => s.len(),
        };
        if file_len < (HEADER_LEN + TRAILER_LEN) as u64 {
            return Err(Error::corruption(&loc, "file too short to be a segment"));
        }
        let mut header = [0u8; HEADER_LEN];
        file.read_exact_at(&mut header, 0, &loc)?;
        if header[0..8] != MAGIC {
            return Err(Error::corruption(&loc, "bad segment header magic"));
        }
        let version = u16::from_le_bytes([header[8], header[9]]);
        if !(MIN_READ_VERSION..=FORMAT_VERSION).contains(&version) {
            return Err(Error::FormatVersion {
                what: "segment",
                found: u32::from(version),
                supported: format!("{MIN_READ_VERSION}..={FORMAT_VERSION}"),
            });
        }
        if header[10..16] != [0u8; 6] {
            return Err(Error::unsupported(format!(
                "{loc}: unknown segment header flags"
            )));
        }
        let mut trailer = [0u8; TRAILER_LEN];
        file.read_exact_at(&mut trailer, file_len - TRAILER_LEN as u64, &loc)?;
        if trailer[8..16] != MAGIC {
            return Err(Error::corruption(
                &loc,
                "bad segment trailer magic (truncated file?)",
            ));
        }
        let footer_len =
            u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]) as usize;
        let footer_crc = u32::from_le_bytes([trailer[4], trailer[5], trailer[6], trailer[7]]);
        let max_footer = (file_len as usize).saturating_sub(HEADER_LEN + TRAILER_LEN);
        if footer_len > MAX_FOOTER_BYTES || footer_len > max_footer {
            return Err(Error::corruption(
                &loc,
                format!("footer length {footer_len} invalid"),
            ));
        }
        let footer_off = file_len - (TRAILER_LEN + footer_len) as u64;
        let mut fb = vec![0u8; footer_len];
        file.read_exact_at(&mut fb, footer_off, &loc)?;
        if crc32c(&fb) != footer_crc {
            return Err(Error::corruption(&loc, "footer checksum mismatch"));
        }
        let footer = decode_footer(&fb, &loc, version)?;
        for c in &footer.columns {
            let end = c.offset.checked_add(c.len);
            if c.offset < HEADER_LEN as u64 || end.is_none_or(|e| e > footer_off) {
                return Err(Error::corruption(
                    &loc,
                    format!("column {:?} chunk out of bounds", c.id),
                ));
            }
        }
        for c in &footer.shredded {
            let end = c.offset.checked_add(c.len);
            if c.offset < HEADER_LEN as u64 || end.is_none_or(|e| e > footer_off) {
                return Err(Error::corruption(
                    &loc,
                    format!("shredded {:?} chunk out of bounds", c.key),
                ));
            }
        }
        Ok(Self {
            path: path.to_path_buf(),
            file,
            file_len,
            footer,
            footer_crc,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn footer(&self) -> &Footer {
        &self.footer
    }

    pub fn footer_crc(&self) -> u32 {
        self.footer_crc
    }

    /// Logical (plaintext) segment length.
    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// Whether the file is a sealed (encrypted) container.
    pub fn is_sealed(&self) -> bool {
        matches!(self.file, Source::Sealed(_))
    }

    /// Read one stored chunk, verify its CRC, and decompress it (bounded).
    fn read_chunk(
        &self,
        what: &str,
        offset: u64,
        len: u64,
        crc: u32,
        comp: u8,
        raw_len: u64,
    ) -> Result<Vec<u8>> {
        let loc = self.path.display().to_string();
        let len = usize::try_from(len).map_err(|_| Error::corruption(&loc, "chunk too big"))?;
        let mut buf = vec![0u8; len];
        self.file.read_exact_at(&mut buf, offset, &loc)?;
        if crc32c(&buf) != crc {
            return Err(Error::corruption(&loc, format!("{what} checksum mismatch")));
        }
        if comp == compression::LZ4 {
            let raw_len = usize::try_from(raw_len)
                .map_err(|_| Error::corruption(&loc, "raw length too big"))?;
            let raw = lz4_flex::block::decompress(&buf, raw_len).map_err(|e| {
                Error::corruption(&loc, format!("{what}: decompression failed: {e}"))
            })?;
            if raw.len() != raw_len {
                return Err(Error::corruption(
                    &loc,
                    format!("{what}: decompressed length mismatch"),
                ));
            }
            buf = raw;
        }
        Ok(buf)
    }

    /// Values of one shredded key, or `None` if the key is not shredded here.
    pub fn read_shredded(
        &self,
        scope: Scope,
        key: &str,
    ) -> Result<Option<Vec<Option<stratadb_schema::Value>>>> {
        let Some(c) = self.footer.shredded(scope, key) else {
            return Ok(None);
        };
        let what = format!("shredded {key:?}");
        let buf = self.read_chunk(&what, c.offset, c.len, c.crc, c.compression, c.raw_len)?;
        columns::decode_column(
            ColumnId::Body,
            Encoding::OptValue,
            &buf,
            self.footer.row_count as usize,
        )
        .and_then(ColumnData::into_values)
        .map(Some)
        .map_err(|e| Error::corruption(self.path.display().to_string(), format!("{what}: {e}")))
    }

    /// Values of `keys` (sorted, deduplicated) of one attribute scope, one
    /// column per key — the projection `read_column(map)[row].get(key)` without
    /// building the maps: shredded keys come from their own chunks, the others
    /// from the residual map chunk, whose other values are skipped. Returns the
    /// columns and the stored bytes read.
    pub fn read_map_keys(
        &self,
        scope: Scope,
        keys: &[String],
    ) -> Result<(Vec<Vec<Option<stratadb_schema::Value>>>, u64)> {
        let loc = self.path.display().to_string();
        let rows = self.footer.row_count as usize;
        let mut out: Vec<Option<Vec<Option<stratadb_schema::Value>>>> = vec![None; keys.len()];
        let mut bytes = 0u64;
        let mut residual: Vec<String> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            match self.read_shredded(scope, k)? {
                Some(col) => {
                    bytes += self.footer.shredded(scope, k).map_or(0, |c| c.len);
                    out[i] = Some(col);
                }
                None => residual.push(k.clone()),
            }
        }
        if !residual.is_empty() {
            let id = match scope {
                Scope::Attributes => ColumnId::Attributes,
                Scope::Resource => ColumnId::ResourceAttributes,
            };
            let meta = self
                .footer
                .column(id)
                .ok_or_else(|| Error::corruption(&loc, format!("missing column {id:?}")))?;
            if meta.encoding != Encoding::Map {
                return Err(Error::corruption(
                    &loc,
                    format!("column {id:?} is not a map column"),
                ));
            }
            let what = format!("column {id:?}");
            let buf = self.read_chunk(
                &what,
                meta.offset,
                meta.len,
                meta.crc,
                meta.compression,
                meta.raw_len,
            )?;
            bytes += meta.len;
            let cols = columns::project_map_keys(&buf, rows, &residual)
                .map_err(|e| Error::corruption(&loc, format!("{what}: {e}")))?;
            let mut it = cols.into_iter();
            for (i, k) in keys.iter().enumerate() {
                if out[i].is_none() && residual.binary_search(k).is_ok() {
                    out[i] = it.next();
                }
            }
        }
        Ok((
            out.into_iter().map(Option::unwrap_or_default).collect(),
            bytes,
        ))
    }

    /// Read, checksum, and decode one column chunk. Attribute map columns are
    /// returned **complete** (shredded keys merged back in).
    pub fn read_column(&self, id: ColumnId) -> Result<ColumnData> {
        let loc = self.path.display().to_string();
        let meta = self
            .footer
            .column(id)
            .ok_or_else(|| Error::corruption(&loc, format!("missing column {id:?}")))?;
        let what = format!("column {id:?}");
        let buf = self.read_chunk(
            &what,
            meta.offset,
            meta.len,
            meta.crc,
            meta.compression,
            meta.raw_len,
        )?;
        let data = columns::decode_column(id, meta.encoding, &buf, self.footer.row_count as usize)
            .map_err(|e| Error::corruption(&loc, format!("{what}: {e}")))?;
        let scope = match id {
            ColumnId::Attributes => Scope::Attributes,
            ColumnId::ResourceAttributes => Scope::Resource,
            _ => return Ok(data),
        };
        let mut maps = data.into_maps()?;
        for c in self.footer.shredded.iter().filter(|c| c.scope == scope) {
            let vals = self.read_shredded(scope, &c.key)?.unwrap_or_default();
            for (m, v) in maps.iter_mut().zip(vals) {
                if let Some(v) = v {
                    if m.insert(c.key.clone(), v).is_some() {
                        return Err(Error::corruption(
                            &loc,
                            format!("key {:?} both shredded and in the map", c.key),
                        ));
                    }
                }
            }
        }
        Ok(ColumnData::Maps(maps))
    }

    /// Materialize every row (all columns). Used by recovery tooling, verify,
    /// compaction and the v0 query path.
    pub fn read_rows(&self) -> Result<Vec<StoredRow>> {
        let n = self.footer.row_count as usize;
        let take = |id| self.read_column(id);
        let ints = |c: ColumnData| c.into_ints();
        let event_time = ints(take(ColumnId::EventTime)?)?;
        let ingest_time = ints(take(ColumnId::IngestTime)?)?;
        let seq = ints(take(ColumnId::Seq)?)?;
        let idx = ints(take(ColumnId::Idx)?)?;
        let schema = ints(take(ColumnId::SchemaVersion)?)?;
        let severity = take(ColumnId::Severity)?.into_u8s()?;
        let service = take(ColumnId::Service)?.into_strs()?;
        let sev_text = take(ColumnId::SeverityText)?.into_strs()?;
        let trace = take(ColumnId::TraceId)?.into_strs()?;
        let span = take(ColumnId::SpanId)?.into_strs()?;
        let event_id = take(ColumnId::EventId)?.into_strs()?;
        let body = take(ColumnId::Body)?.into_values()?;
        let attrs = take(ColumnId::Attributes)?.into_maps()?;
        let res = take(ColumnId::ResourceAttributes)?.into_maps()?;
        let loc = || self.path.display().to_string();
        let mut out = Vec::with_capacity(n);
        let mut service = service.into_iter();
        let mut sev_text = sev_text.into_iter();
        let mut trace = trace.into_iter();
        let mut span = span.into_iter();
        let mut event_id = event_id.into_iter();
        let mut body = body.into_iter();
        let mut attrs = attrs.into_iter();
        let mut res = res.into_iter();
        for i in 0..n {
            let sev = Severity::from_u8(severity[i])
                .ok_or_else(|| Error::corruption(loc(), format!("invalid severity at row {i}")))?;
            let missing = || Error::corruption(loc(), "column shorter than row_count");
            out.push(StoredRow {
                seq: u64::try_from(seq[i]).map_err(|_| Error::corruption(loc(), "negative seq"))?,
                idx: u32::try_from(idx[i]).map_err(|_| Error::corruption(loc(), "bad idx"))?,
                event: Event {
                    event_time: event_time[i],
                    ingest_time: ingest_time[i],
                    tenant_id: self.footer.tenant_id.clone(),
                    service: service.next().ok_or_else(missing)?,
                    severity: sev,
                    severity_text: sev_text.next().ok_or_else(missing)?,
                    body: body.next().ok_or_else(missing)?,
                    trace_id: trace.next().ok_or_else(missing)?,
                    span_id: span.next().ok_or_else(missing)?,
                    event_id: event_id.next().ok_or_else(missing)?,
                    attributes: attrs.next().ok_or_else(missing)?,
                    resource_attributes: res.next().ok_or_else(missing)?,
                    schema_version: u32::try_from(schema[i])
                        .map_err(|_| Error::corruption(loc(), "bad schema version"))?,
                },
            });
        }
        Ok(out)
    }

    /// Full integrity verification: every chunk checksum, every decode, and
    /// footer statistics recomputed from data.
    pub fn verify(&self) -> Result<()> {
        let rows = self.read_rows()?;
        let loc = self.path.display().to_string();
        let f = &self.footer;
        let bad = |what: &str| {
            Err(Error::corruption(
                &loc,
                format!("footer {what} does not match data"),
            ))
        };
        if rows.len() as u64 != f.row_count {
            return bad("row_count");
        }
        let min_t = rows.iter().map(|r| r.event.event_time).min().unwrap_or(0);
        let max_t = rows.iter().map(|r| r.event.event_time).max().unwrap_or(0);
        if min_t != f.min_event_time || max_t != f.max_event_time {
            return bad("time bounds");
        }
        let min_s = rows.iter().map(|r| r.seq).min().unwrap_or(0);
        let max_s = rows.iter().map(|r| r.seq).max().unwrap_or(0);
        if min_s != f.min_seq || max_s != f.max_seq {
            return bad("sequence bounds");
        }
        if !rows.windows(2).all(|w| {
            (w[0].event.event_time, w[0].seq, w[0].idx)
                < (w[1].event.event_time, w[1].seq, w[1].idx)
        }) {
            return Err(Error::corruption(
                &loc,
                "rows not strictly sorted by (event_time, seq, idx)",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
