//! Logical column and batch types shared by every layer above L1 (spec
//! STRATA-ARCH-11-001): column identifiers, decoded column vectors, attribute
//! scopes and undecoded value columns. Storage produces them and the executor
//! consumes them; neither defines its own column representation.

use std::collections::BTreeMap;

use stratadb_common::codec::Decoder;
use stratadb_common::{Error, Result};

use crate::codec::{decode_value, DecodeLimits};
use crate::Value;

/// Which attribute map a shredded column belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Scope {
    Attributes = 0,
    Resource = 1,
}

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

/// Decoded column values, one entry per row.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData {
    Ints(Vec<i64>),
    U8s(Vec<u8>),
    /// Typed string column (no allocation per row; dictionary-encoded when
    /// the source was).
    Strs(StrColumn),
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
            ColumnData::Strs(v) => Ok(v.to_options()),
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

/// Validity bitmap: bit `i` set = row `i` present.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Bitmap {
    words: Vec<u64>,
    len: usize,
}

impl Bitmap {
    fn push(&mut self, v: bool) {
        if self.len.is_multiple_of(64) {
            self.words.push(0);
        }
        if v {
            if let Some(w) = self.words.last_mut() {
                *w |= 1u64 << (self.len % 64);
            }
        }
        self.len += 1;
    }

    fn get(&self, i: usize) -> bool {
        self.words[i / 64] & (1u64 << (i % 64)) != 0
    }
}

#[derive(Debug, Clone)]
enum StrRepr {
    /// Row `i` is `bytes[offsets[i]..offsets[i + 1]]` when `valid[i]`.
    Plain {
        offsets: std::sync::Arc<Vec<u32>>,
        /// All values back to back; offsets fall on char boundaries.
        text: std::sync::Arc<String>,
        valid: std::sync::Arc<Bitmap>,
    },
    /// Row `i` is `dict[codes[i] - 1]`, missing when `codes[i] == 0`.
    Dict {
        dict: std::sync::Arc<Vec<String>>,
        codes: std::sync::Arc<Vec<u32>>,
    },
}

/// A column of optional UTF-8 strings, stored without a heap allocation per
/// row (spec STRATA-ARCH-03-005, ARCH-11-001).
/// - **Plain:** one byte buffer with offsets and a validity bitmap.
/// - **Dictionary-encoded:** distinct values plus one code per row, kept as
///   decoded from `DictStr` chunks, so filters can evaluate a predicate once
///   per distinct value.
///
/// Buffers are reference-counted and immutable: cloning a column is O(1).
/// Equality is by value, whatever the representation.
#[derive(Debug, Clone)]
pub struct StrColumn {
    repr: StrRepr,
}

/// Builds a plain [`StrColumn`].
#[derive(Debug, Default)]
pub struct StrBuilder {
    offsets: Vec<u32>,
    text: String,
    valid: Bitmap,
}

impl StrBuilder {
    pub fn with_capacity(rows: usize, bytes: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        Self {
            offsets,
            text: String::with_capacity(bytes),
            valid: Bitmap {
                words: Vec::with_capacity(rows.div_ceil(64)),
                len: 0,
            },
        }
    }

    /// Append a row. Fails when the column would exceed 4 GiB of text.
    pub fn push(&mut self, v: Option<&str>) -> Result<()> {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        if let Some(s) = v {
            self.text.push_str(s);
        }
        let end = u32::try_from(self.text.len())
            .map_err(|_| Error::invalid_input("string column larger than 4 GiB"))?;
        self.offsets.push(end);
        self.valid.push(v.is_some());
        Ok(())
    }

    pub fn finish(mut self) -> StrColumn {
        if self.offsets.is_empty() {
            self.offsets.push(0);
        }
        StrColumn {
            repr: StrRepr::Plain {
                offsets: std::sync::Arc::new(self.offsets),
                text: std::sync::Arc::new(self.text),
                valid: std::sync::Arc::new(self.valid),
            },
        }
    }
}

impl StrColumn {
    /// A plain column from optional strings.
    pub fn from_options<I, S>(values: I) -> Result<Self>
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let mut b = StrBuilder::default();
        for v in values {
            b.push(v.as_ref().map(AsRef::as_ref))?;
        }
        Ok(b.finish())
    }

    /// A dictionary-encoded column: `codes[i] == 0` is missing, `k` is
    /// `dict[k - 1]`. Fails on an out-of-range code.
    pub fn from_dict(dict: Vec<String>, codes: Vec<u32>) -> Result<Self> {
        if codes.iter().any(|c| *c as usize > dict.len()) {
            return Err(Error::corruption("column", "dictionary code out of range"));
        }
        Ok(Self {
            repr: StrRepr::Dict {
                dict: std::sync::Arc::new(dict),
                codes: std::sync::Arc::new(codes),
            },
        })
    }

    pub fn len(&self) -> usize {
        match &self.repr {
            StrRepr::Plain { offsets, .. } => offsets.len() - 1,
            StrRepr::Dict { codes, .. } => codes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Row `i` (`None` = missing). Panics if `i >= len`.
    pub fn get(&self, i: usize) -> Option<&str> {
        match &self.repr {
            StrRepr::Plain {
                offsets,
                text,
                valid,
            } => {
                if !valid.get(i) {
                    return None;
                }
                // Offsets are recorded after whole `&str` pushes, so they
                // are char boundaries: `get` only checks the two ends.
                text.get(offsets[i] as usize..offsets[i + 1] as usize)
            }
            StrRepr::Dict { dict, codes } => match codes[i] {
                0 => None,
                c => dict.get(c as usize - 1).map(String::as_str),
            },
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&str>> + '_ {
        (0..self.len()).map(move |i| self.get(i))
    }

    /// Owned values (for row materialization).
    pub fn to_options(&self) -> Vec<Option<String>> {
        self.iter().map(|v| v.map(str::to_owned)).collect()
    }

    /// `(dictionary, codes)` when dictionary-encoded: code 0 = missing,
    /// `k` = `dictionary[k - 1]`.
    pub fn dict(&self) -> Option<(&[String], &[u32])> {
        match &self.repr {
            StrRepr::Dict { dict, codes } => Some((dict, codes)),
            StrRepr::Plain { .. } => None,
        }
    }
}

impl PartialEq for StrColumn {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

/// An `OptValue` column kept in its encoded form: row `i` is missing or the
/// `encode_value` bytes `buf[start..end]`. Decoding is deferred to
/// [`RawValues::value`]; the bytes are validated on construction exactly as
/// the segment reader's `decode_column` validates them (presence markers, tags, lengths, UTF-8,
/// nesting, map key order — via `skip_value`), so a chunk is accepted or
/// rejected the same way in either form.
#[derive(Debug, Clone, PartialEq)]
pub struct RawValues {
    buf: Vec<u8>,
    spans: Vec<Option<(u32, u32)>>,
}

impl RawValues {
    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    /// Encoded value of row `i` (`None` = missing). Panics if `i >= len`.
    pub fn bytes(&self, i: usize) -> Option<&[u8]> {
        self.spans[i].map(|(s, e)| &self.buf[s as usize..e as usize])
    }

    /// Decoded value of row `i`.
    pub fn value(&self, i: usize) -> Result<Option<Value>> {
        match self.bytes(i) {
            None => Ok(None),
            Some(b) => {
                let mut d = Decoder::new(b, "raw value");
                let v = decode_value(&mut d, &DecodeLimits::default())?;
                d.expect_end()?;
                Ok(Some(v))
            }
        }
    }
}

/// Validate an `OptValue` chunk and index its values without decoding them.
pub fn index_opt_values(buf: Vec<u8>, rows: usize) -> Result<RawValues> {
    if rows > buf.len() {
        return Err(Error::corruption("column", "row count exceeds chunk size"));
    }
    let end = u32::try_from(buf.len())
        .map_err(|_| Error::corruption("column", "value chunk larger than 4 GiB"))?;
    let lim = DecodeLimits::default();
    let mut spans = Vec::with_capacity(rows);
    {
        let mut d = Decoder::new(&buf, "column chunk");
        let pos = |d: &Decoder<'_>| end - d.remaining() as u32;
        for _ in 0..rows {
            match d.get_u8()? {
                0 => spans.push(None),
                1 => {
                    let start = pos(&d);
                    crate::codec::skip_value(&mut d, &lim)?;
                    spans.push(Some((start, pos(&d))));
                }
                other => {
                    return Err(Error::corruption(
                        "column",
                        format!("invalid presence marker {other}"),
                    ))
                }
            }
        }
        d.expect_end()?;
    }
    Ok(RawValues { buf, spans })
}

/// The events of one tenant, column by column (one entry per row). The
/// row-free form shared by the columnar WAL decoder and the memtable
/// (spec STRATA-SYS-07-002, STRATA-ARCH-02-012).
#[derive(Debug, Clone, PartialEq)]
pub struct EventColumns {
    pub tenant: stratadb_common::TenantId,
    pub event_time: Vec<i64>,
    pub ingest_time: Vec<i64>,
    pub severity: Vec<crate::Severity>,
    pub schema_version: Vec<u32>,
    pub service: Vec<Option<String>>,
    pub severity_text: Vec<Option<String>>,
    pub trace_id: Vec<Option<String>>,
    pub span_id: Vec<Option<String>>,
    pub event_id: Vec<Option<String>>,
    pub body: Vec<Option<Value>>,
    pub attributes: Vec<BTreeMap<String, Value>>,
    pub resource_attributes: Vec<BTreeMap<String, Value>>,
}

impl EventColumns {
    pub fn len(&self) -> usize {
        self.event_time.len()
    }

    pub fn is_empty(&self) -> bool {
        self.event_time.is_empty()
    }

    /// Split events (non-empty, one tenant) into columns, moving their
    /// fields.
    pub fn from_events(events: Vec<crate::Event>) -> Result<Self> {
        let tenant = events
            .first()
            .map(|e| e.tenant_id.clone())
            .ok_or_else(|| Error::invalid_input("a batch needs at least one event"))?;
        let n = events.len();
        let mut c = Self::with_capacity(tenant, n);
        for e in events {
            if e.tenant_id != c.tenant {
                return Err(Error::invalid_input("a batch holds events of one tenant"));
            }
            c.event_time.push(e.event_time);
            c.ingest_time.push(e.ingest_time);
            c.severity.push(e.severity);
            c.schema_version.push(e.schema_version);
            c.service.push(e.service);
            c.severity_text.push(e.severity_text);
            c.trace_id.push(e.trace_id);
            c.span_id.push(e.span_id);
            c.event_id.push(e.event_id);
            c.body.push(e.body);
            c.attributes.push(e.attributes);
            c.resource_attributes.push(e.resource_attributes);
        }
        Ok(c)
    }

    /// Empty columns with room for `n` rows.
    pub fn with_capacity(tenant: stratadb_common::TenantId, n: usize) -> Self {
        Self {
            tenant,
            event_time: Vec::with_capacity(n),
            ingest_time: Vec::with_capacity(n),
            severity: Vec::with_capacity(n),
            schema_version: Vec::with_capacity(n),
            service: Vec::with_capacity(n),
            severity_text: Vec::with_capacity(n),
            trace_id: Vec::with_capacity(n),
            span_id: Vec::with_capacity(n),
            event_id: Vec::with_capacity(n),
            body: Vec::with_capacity(n),
            attributes: Vec::with_capacity(n),
            resource_attributes: Vec::with_capacity(n),
        }
    }

    /// Back to rows (moving the values).
    pub fn into_events(self) -> Vec<crate::Event> {
        let mut out = Vec::with_capacity(self.len());
        let mut it = (
            self.event_time.into_iter(),
            self.ingest_time.into_iter(),
            self.severity.into_iter(),
            self.schema_version.into_iter(),
            self.service.into_iter(),
            self.severity_text.into_iter(),
            self.trace_id.into_iter(),
            self.span_id.into_iter(),
            self.event_id.into_iter(),
            self.body.into_iter(),
            self.attributes.into_iter(),
            self.resource_attributes.into_iter(),
        );
        while let (
            Some(event_time),
            Some(ingest_time),
            Some(severity),
            Some(schema_version),
            Some(service),
            Some(severity_text),
            Some(trace_id),
            Some(span_id),
            Some(event_id),
            Some(body),
            Some(attributes),
            Some(resource_attributes),
        ) = (
            it.0.next(),
            it.1.next(),
            it.2.next(),
            it.3.next(),
            it.4.next(),
            it.5.next(),
            it.6.next(),
            it.7.next(),
            it.8.next(),
            it.9.next(),
            it.10.next(),
            it.11.next(),
        ) {
            out.push(crate::Event {
                event_time,
                ingest_time,
                tenant_id: self.tenant.clone(),
                service,
                severity,
                severity_text,
                body,
                trace_id,
                span_id,
                event_id,
                attributes,
                resource_attributes,
                schema_version,
            });
        }
        out
    }

    /// `Event::approx_size` of row `i` (same formula, without building it).
    pub fn row_approx_size(&self, i: usize) -> usize {
        let opt = |s: &Option<String>| s.as_ref().map_or(0, String::len);
        std::mem::size_of::<crate::Event>()
            + self.tenant.as_str().len()
            + opt(&self.service[i])
            + opt(&self.severity_text[i])
            + opt(&self.trace_id[i])
            + opt(&self.span_id[i])
            + opt(&self.event_id[i])
            + self.body[i].as_ref().map_or(0, Value::approx_size)
            + self.attributes[i]
                .iter()
                .chain(self.resource_attributes[i].iter())
                .map(|(k, v)| k.len() + 32 + v.approx_size())
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;

    fn dict_of(vals: &[Option<String>]) -> StrColumn {
        let mut dict: Vec<String> = Vec::new();
        let codes = vals
            .iter()
            .map(|v| match v {
                None => 0,
                Some(s) => match dict.iter().position(|d| d == s) {
                    Some(k) => k as u32 + 1,
                    None => {
                        dict.push(s.clone());
                        dict.len() as u32
                    }
                },
            })
            .collect();
        StrColumn::from_dict(dict, codes).unwrap()
    }

    proptest! {
        #[test]
        fn plain_and_dict_columns_hold_the_values(vals in proptest::collection::vec(proptest::option::of(".{0,12}"), 0..300)) {
            let plain = StrColumn::from_options(vals.clone()).unwrap();
            let dict = dict_of(&vals);
            prop_assert_eq!(plain.len(), vals.len());
            prop_assert_eq!(plain.to_options(), vals.clone());
            prop_assert_eq!(dict.to_options(), vals.clone());
            prop_assert_eq!(&plain, &dict);
            prop_assert!(plain.dict().is_none() && dict.dict().is_some());
            for (i, v) in vals.iter().enumerate() {
                prop_assert_eq!(plain.get(i), v.as_deref());
            }
        }
    }

    #[test]
    fn edges() {
        let e = StrColumn::from_options(Vec::<Option<&str>>::new()).unwrap();
        assert!(e.is_empty() && e.to_options().is_empty());
        assert_eq!(StrBuilder::default().finish().len(), 0);
        // Empty strings are present, not missing; bitmap crosses a word.
        let vals: Vec<Option<&str>> = (0..130).map(|i| (i % 3 != 0).then_some("")).collect();
        let c = StrColumn::from_options(vals.clone()).unwrap();
        assert_eq!(c.iter().collect::<Vec<_>>(), vals);
        assert!(StrColumn::from_dict(vec!["a".into()], vec![0, 1, 2]).is_err());
        assert_ne!(
            StrColumn::from_options([Some("a")]).unwrap(),
            StrColumn::from_options([Some("b")]).unwrap()
        );
        let shared = c.clone();
        assert_eq!(shared, c);
    }
}
