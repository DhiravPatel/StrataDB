//! Column chunk encodings for segment format v1.

use std::collections::HashMap;

use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{Error, Result};
use stratadb_schema::codec::{decode_map, decode_value, encode_map, encode_value, DecodeLimits};
use stratadb_schema::{Event, Value};

pub(crate) use stratadb_schema::column::index_opt_values;
pub use stratadb_schema::column::{ColumnData, ColumnId, RawValues};
use stratadb_schema::column::{StrBuilder, StrColumn};

use crate::RowRef;

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

/// Project a `Map`-encoded chunk onto `keys` (sorted, deduplicated): one
/// column per key, `None` where the row's map lacks it. Values of other keys
/// are skipped without allocation (validated like a full decode).
// `out` is indexed [key][row]; `row` selects within the matched key's column.
#[allow(clippy::needless_range_loop)]
pub(crate) fn project_map_keys(
    bytes: &[u8],
    rows: usize,
    keys: &[String],
) -> Result<Vec<Vec<Option<Value>>>> {
    let lim = DecodeLimits::default();
    let mut d = Decoder::new(bytes, "column chunk");
    if rows > bytes.len() {
        return Err(Error::corruption("column", "row count exceeds chunk size"));
    }
    let mut out: Vec<Vec<Option<Value>>> = keys.iter().map(|_| vec![None; rows]).collect();
    for row in 0..rows {
        let n = d.get_len(lim.max_items, 2)?;
        let mut prev: Option<&str> = None;
        for _ in 0..n {
            let k = d.get_str(lim.max_string_bytes)?;
            if prev.is_some_and(|p| k <= p) {
                return Err(Error::corruption(
                    "map",
                    "keys not strictly increasing (duplicate or unsorted key)",
                ));
            }
            prev = Some(k);
            match keys.binary_search_by(|x| x.as_str().cmp(k)) {
                Ok(i) => out[i][row] = Some(decode_value(&mut d, &lim)?),
                Err(_) => stratadb_schema::codec::skip_value(&mut d, &lim)?,
            }
        }
    }
    d.expect_end()?;
    Ok(out)
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
            // Straight into one byte buffer: no allocation per row.
            let mut v = StrBuilder::with_capacity(rows, bytes.len());
            for _ in 0..rows {
                let n = d.get_uvarint()?;
                if n == 0 {
                    v.push(None)?;
                } else {
                    let len = usize::try_from(n - 1)
                        .map_err(|_| Error::corruption("column", "length overflow"))?;
                    let raw = d.get_raw(len)?;
                    let s = std::str::from_utf8(raw)
                        .map_err(|e| Error::corruption("column", format!("invalid UTF-8: {e}")))?;
                    v.push(Some(s))?;
                }
            }
            ColumnData::Strs(v.finish())
        }
        Encoding::DictStr => {
            let n = d.get_len(bytes.len(), 1)?;
            let mut dict = Vec::with_capacity(n);
            for _ in 0..n {
                dict.push(d.get_string(bytes.len())?);
            }
            // Kept dictionary-encoded: filters evaluate once per entry.
            let mut codes = Vec::with_capacity(rows);
            for _ in 0..rows {
                let code = d.get_uvarint()?;
                let code = u32::try_from(code)
                    .ok()
                    .filter(|c| *c as usize <= dict.len())
                    .ok_or_else(|| Error::corruption("column", "dictionary code out of range"))?;
                codes.push(code);
            }
            ColumnData::Strs(StrColumn::from_dict(dict, codes)?)
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
