//! Column chunk encodings for segment format v1.

use std::collections::{BTreeMap, HashMap};

use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{Error, Result};
use stratadb_schema::codec::{decode_map, decode_value, encode_map, encode_value, DecodeLimits};
use stratadb_schema::{Event, Value};

use crate::RowRef;

/// Stable column identifiers (persisted; never renumber).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u16)]
pub enum ColumnId {
    EventTime = 1,
    IngestTime = 2,
    Seq = 3,
    Idx = 4,
    Severity = 5,
    Service = 6,
    SeverityText = 7,
    TraceId = 8,
    SpanId = 9,
    EventId = 10,
    Body = 11,
    Attributes = 12,
    ResourceAttributes = 13,
    SchemaVersion = 14,
}

impl ColumnId {
    pub const ALL: [ColumnId; 14] = [
        ColumnId::EventTime,
        ColumnId::IngestTime,
        ColumnId::Seq,
        ColumnId::Idx,
        ColumnId::Severity,
        ColumnId::Service,
        ColumnId::SeverityText,
        ColumnId::TraceId,
        ColumnId::SpanId,
        ColumnId::EventId,
        ColumnId::Body,
        ColumnId::Attributes,
        ColumnId::ResourceAttributes,
        ColumnId::SchemaVersion,
    ];

    pub fn from_u16(v: u16) -> Option<Self> {
        Self::ALL.iter().copied().find(|c| *c as u16 == v)
    }

    pub fn name(self) -> &'static str {
        match self {
            ColumnId::EventTime => "event_time",
            ColumnId::IngestTime => "ingest_time",
            ColumnId::Seq => "_seq",
            ColumnId::Idx => "_idx",
            ColumnId::Severity => "severity",
            ColumnId::Service => "service",
            ColumnId::SeverityText => "severity_text",
            ColumnId::TraceId => "trace_id",
            ColumnId::SpanId => "span_id",
            ColumnId::EventId => "event_id",
            ColumnId::Body => "body",
            ColumnId::Attributes => "attributes",
            ColumnId::ResourceAttributes => "resource_attributes",
            ColumnId::SchemaVersion => "schema_version",
        }
    }
}

/// Chunk encodings (persisted; never renumber).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    /// Zig-zag varint of successive differences (first value relative to 0).
    DeltaVarint = 1,
    /// One byte per row.
    RawU8 = 2,
    /// Per row: uvarint(len + 1) then bytes, or 0 for missing.
    OptStr = 3,
    /// Dictionary: uvarint n, n strings, then per row uvarint code (0 = missing, k = dict[k-1]).
    DictStr = 4,
    /// Per row: u8 presence then encoded value.
    OptValue = 5,
    /// Per row: encoded map (empty map = no attributes).
    Map = 6,
}

impl Encoding {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => Encoding::DeltaVarint,
            2 => Encoding::RawU8,
            3 => Encoding::OptStr,
            4 => Encoding::DictStr,
            5 => Encoding::OptValue,
            6 => Encoding::Map,
            _ => return None,
        })
    }
}

/// Decoded column values, one entry per row.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Ints(Vec<i64>),
    U8s(Vec<u8>),
    Strs(Vec<Option<String>>),
    Values(Vec<Option<Value>>),
    Maps(Vec<BTreeMap<String, Value>>),
}

fn wrong(expected: &str) -> Error {
    Error::internal(format!("column data is not {expected}"))
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Ints(v) => v.len(),
            ColumnData::U8s(v) => v.len(),
            ColumnData::Strs(v) => v.len(),
            ColumnData::Values(v) => v.len(),
            ColumnData::Maps(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_ints(self) -> Result<Vec<i64>> {
        match self {
            ColumnData::Ints(v) => Ok(v),
            _ => Err(wrong("ints")),
        }
    }
    pub fn into_u8s(self) -> Result<Vec<u8>> {
        match self {
            ColumnData::U8s(v) => Ok(v),
            _ => Err(wrong("u8s")),
        }
    }
    pub fn into_strs(self) -> Result<Vec<Option<String>>> {
        match self {
            ColumnData::Strs(v) => Ok(v),
            _ => Err(wrong("strings")),
        }
    }
    pub fn into_values(self) -> Result<Vec<Option<Value>>> {
        match self {
            ColumnData::Values(v) => Ok(v),
            _ => Err(wrong("values")),
        }
    }
    pub fn into_maps(self) -> Result<Vec<BTreeMap<String, Value>>> {
        match self {
            ColumnData::Maps(v) => Ok(v),
            _ => Err(wrong("maps")),
        }
    }
}

fn delta(rows: &[RowRef<'_>], f: impl Fn(&RowRef<'_>) -> i64) -> Vec<u8> {
    let mut e = Encoder::with_capacity(rows.len() * 2);
    let mut prev = 0i64;
    for r in rows {
        let v = f(r);
        e.put_ivarint(v.wrapping_sub(prev));
        prev = v;
    }
    e.into_inner()
}

fn opt_str(rows: &[RowRef<'_>], f: impl Fn(&Event) -> &Option<String>) -> (Vec<u8>, u64) {
    let mut e = Encoder::new();
    let mut missing = 0;
    for r in rows {
        match f(r.event) {
            None => {
                e.put_uvarint(0);
                missing += 1;
            }
            Some(s) => {
                e.put_uvarint(s.len() as u64 + 1);
                e.put_raw(s.as_bytes());
            }
        }
    }
    (e.into_inner(), missing)
}

fn dict_str(rows: &[RowRef<'_>], f: impl Fn(&Event) -> &Option<String>) -> (Vec<u8>, u64) {
    let mut dict: Vec<&str> = Vec::new();
    let mut codes: HashMap<&str, u64> = HashMap::new();
    let mut per_row = Vec::with_capacity(rows.len());
    let mut missing = 0;
    for r in rows {
        match f(r.event) {
            None => {
                per_row.push(0);
                missing += 1;
            }
            Some(s) => {
                let next = dict.len() as u64 + 1;
                let code = *codes.entry(s.as_str()).or_insert_with(|| {
                    dict.push(s.as_str());
                    next
                });
                per_row.push(code);
            }
        }
    }
    let mut e = Encoder::new();
    e.put_uvarint(dict.len() as u64);
    for s in &dict {
        e.put_str(s);
    }
    for c in per_row {
        e.put_uvarint(c);
    }
    (e.into_inner(), missing)
}

/// Encode column `id` for `rows`. Returns (encoding, bytes, missing count).
pub(crate) fn encode_column(id: ColumnId, rows: &[RowRef<'_>]) -> (Encoding, Vec<u8>, u64) {
    match id {
        ColumnId::EventTime => (
            Encoding::DeltaVarint,
            delta(rows, |r| r.event.event_time),
            0,
        ),
        ColumnId::IngestTime => (
            Encoding::DeltaVarint,
            delta(rows, |r| r.event.ingest_time),
            0,
        ),
        // Sequence numbers are < 2^63 by construction (dense u64 from 1).
        ColumnId::Seq => (Encoding::DeltaVarint, delta(rows, |r| r.seq as i64), 0),
        ColumnId::Idx => (Encoding::DeltaVarint, delta(rows, |r| i64::from(r.idx)), 0),
        ColumnId::SchemaVersion => (
            Encoding::DeltaVarint,
            delta(rows, |r| i64::from(r.event.schema_version)),
            0,
        ),
        ColumnId::Severity => (
            Encoding::RawU8,
            rows.iter().map(|r| r.event.severity.as_u8()).collect(),
            0,
        ),
        ColumnId::Service => {
            let (b, m) = dict_str(rows, |e| &e.service);
            (Encoding::DictStr, b, m)
        }
        ColumnId::SeverityText => {
            let (b, m) = dict_str(rows, |e| &e.severity_text);
            (Encoding::DictStr, b, m)
        }
        ColumnId::TraceId => {
            let (b, m) = opt_str(rows, |e| &e.trace_id);
            (Encoding::OptStr, b, m)
        }
        ColumnId::SpanId => {
            let (b, m) = opt_str(rows, |e| &e.span_id);
            (Encoding::OptStr, b, m)
        }
        ColumnId::EventId => {
            let (b, m) = opt_str(rows, |e| &e.event_id);
            (Encoding::OptStr, b, m)
        }
        ColumnId::Body => {
            let mut e = Encoder::new();
            let mut missing = 0;
            for r in rows {
                match &r.event.body {
                    None => {
                        e.put_u8(0);
                        missing += 1;
                    }
                    Some(v) => {
                        e.put_u8(1);
                        encode_value(&mut e, v);
                    }
                }
            }
            (Encoding::OptValue, e.into_inner(), missing)
        }
        ColumnId::Attributes | ColumnId::ResourceAttributes => {
            let mut e = Encoder::new();
            let mut missing = 0;
            for r in rows {
                let m = if id == ColumnId::Attributes {
                    &r.event.attributes
                } else {
                    &r.event.resource_attributes
                };
                if m.is_empty() {
                    missing += 1;
                }
                encode_map(&mut e, m);
            }
            (Encoding::Map, e.into_inner(), missing)
        }
    }
}

/// Map column for one scope, omitting `exclude` (keys stored as shredded columns).
pub(crate) fn encode_map_excluding(
    rows: &[RowRef<'_>],
    resource: bool,
    exclude: &std::collections::BTreeSet<String>,
) -> (Encoding, Vec<u8>, u64) {
    let mut e = Encoder::new();
    let mut missing = 0;
    for r in rows {
        let m = if resource {
            &r.event.resource_attributes
        } else {
            &r.event.attributes
        };
        let n = m.keys().filter(|k| !exclude.contains(*k)).count();
        if n == 0 {
            missing += 1;
        }
        e.put_uvarint(n as u64);
        // BTreeMap iteration keeps keys strictly increasing (canonical encoding).
        for (k, v) in m.iter().filter(|(k, _)| !exclude.contains(*k)) {
            e.put_str(k);
            encode_value(&mut e, v);
        }
    }
    (Encoding::Map, e.into_inner(), missing)
}

/// One shredded key as an `OptValue` column (0 = missing, 1 = value incl. null).
pub(crate) fn encode_shredded(rows: &[RowRef<'_>], resource: bool, key: &str) -> (Vec<u8>, u64) {
    let mut e = Encoder::new();
    let mut missing = 0;
    for r in rows {
        let m = if resource {
            &r.event.resource_attributes
        } else {
            &r.event.attributes
        };
        match m.get(key) {
            None => {
                e.put_u8(0);
                missing += 1;
            }
            Some(v) => {
                e.put_u8(1);
                encode_value(&mut e, v);
            }
        }
    }
    (e.into_inner(), missing)
}

/// Decode a chunk. `rows` comes from the (checksummed) footer.
pub(crate) fn decode_column(
    id: ColumnId,
    enc: Encoding,
    bytes: &[u8],
    rows: usize,
) -> Result<ColumnData> {
    let expected = expected_encoding(id);
    if enc != expected {
        return Err(Error::corruption(
            "column",
            format!("column {id:?} has encoding {enc:?}, expected {expected:?}"),
        ));
    }
    let lim = DecodeLimits::default();
    let mut d = Decoder::new(bytes, "column chunk");
    // Every encoding uses >= 1 byte per row; reject impossible row counts early.
    if rows > bytes.len() {
        return Err(Error::corruption("column", "row count exceeds chunk size"));
    }
    let data = match enc {
        Encoding::DeltaVarint => {
            let mut v = Vec::with_capacity(rows);
            let mut prev = 0i64;
            for _ in 0..rows {
                prev = prev.wrapping_add(d.get_ivarint()?);
                v.push(prev);
            }
            ColumnData::Ints(v)
        }
        Encoding::RawU8 => ColumnData::U8s(d.get_raw(rows)?.to_vec()),
        Encoding::OptStr => {
            let mut v = Vec::with_capacity(rows);
            for _ in 0..rows {
                let n = d.get_uvarint()?;
                if n == 0 {
                    v.push(None);
                } else {
                    let len = usize::try_from(n - 1)
                        .map_err(|_| Error::corruption("column", "length overflow"))?;
                    let raw = d.get_raw(len)?;
                    let s = std::str::from_utf8(raw)
                        .map_err(|e| Error::corruption("column", format!("invalid UTF-8: {e}")))?;
                    v.push(Some(s.to_owned()));
                }
            }
            ColumnData::Strs(v)
        }
        Encoding::DictStr => {
            let n = d.get_len(bytes.len(), 1)?;
            let mut dict = Vec::with_capacity(n);
            for _ in 0..n {
                dict.push(d.get_string(bytes.len())?);
            }
            let mut v = Vec::with_capacity(rows);
            for _ in 0..rows {
                let code = d.get_uvarint()?;
                if code == 0 {
                    v.push(None);
                } else {
                    let s = usize::try_from(code - 1)
                        .ok()
                        .and_then(|i| dict.get(i))
                        .ok_or_else(|| {
                            Error::corruption("column", "dictionary code out of range")
                        })?;
                    v.push(Some(s.clone()));
                }
            }
            ColumnData::Strs(v)
        }
        Encoding::OptValue => {
            let mut v = Vec::with_capacity(rows);
            for _ in 0..rows {
                match d.get_u8()? {
                    0 => v.push(None),
                    1 => v.push(Some(decode_value(&mut d, &lim)?)),
                    other => {
                        return Err(Error::corruption(
                            "column",
                            format!("invalid presence marker {other}"),
                        ))
                    }
                }
            }
            ColumnData::Values(v)
        }
        Encoding::Map => {
            let mut v = Vec::with_capacity(rows);
            for _ in 0..rows {
                v.push(decode_map(&mut d, &lim)?);
            }
            ColumnData::Maps(v)
        }
    };
    d.expect_end()?;
    Ok(data)
}

fn expected_encoding(id: ColumnId) -> Encoding {
    match id {
        ColumnId::EventTime
        | ColumnId::IngestTime
        | ColumnId::Seq
        | ColumnId::Idx
        | ColumnId::SchemaVersion => Encoding::DeltaVarint,
        ColumnId::Severity => Encoding::RawU8,
        ColumnId::Service | ColumnId::SeverityText => Encoding::DictStr,
        ColumnId::TraceId | ColumnId::SpanId | ColumnId::EventId => Encoding::OptStr,
        ColumnId::Body => Encoding::OptValue,
        ColumnId::Attributes | ColumnId::ResourceAttributes => Encoding::Map,
    }
}
