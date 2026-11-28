//! The scan seam between storage (L2) and the executor (L3), spec
//! STRATA-ARCH-01-003: the executor never sees a file format. It asks a
//! [`ScanSource`] whether a pruning predicate excludes it and for the
//! columns a plan needs, and receives decoded [`ColumnBatch`]es of the shared
//! column types (`stratadb_schema::column`, STRATA-ARCH-11-001).
//!
//! Implemented here for committed segments ([`SegmentHandle`]) and for
//! memtable chunks ([`MemChunk`]), so the same operators run over both.
//! Index structures (Bloom and trigram filters), footers, chunk directories
//! and shredded-column layout stay behind this interface.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use stratadb_common::{Error, Result, TenantId};
use stratadb_schema::column::{ColumnData, ColumnId, RawValues, Scope, StrBuilder, StrColumn};
use stratadb_schema::Value;
use stratadb_segment::bloom::BloomField;
use stratadb_segment::ngram::NgramField;

use crate::engine::SegmentHandle;
use crate::memtable::Chunk;

/// A string-valued field a pruning predicate can name. Storage decides which
/// fields it actually indexes, and how.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexField {
    TraceId,
    SpanId,
    EventId,
    Body,
    Attr(String),
    Resource(String),
}

impl IndexField {
    fn bloom(&self) -> Option<BloomField> {
        Some(match self {
            IndexField::TraceId => BloomField::TraceId,
            IndexField::SpanId => BloomField::SpanId,
            IndexField::EventId => BloomField::EventId,
            IndexField::Attr(k) => BloomField::Attr(k.clone()),
            IndexField::Resource(k) => BloomField::Resource(k.clone()),
            IndexField::Body => return None,
        })
    }

    fn ngram(&self) -> Option<NgramField> {
        Some(match self {
            IndexField::Body => NgramField::Body,
            IndexField::Attr(k) => NgramField::Attr(k.clone()),
            IndexField::Resource(k) => NgramField::Resource(k.clone()),
            _ => return None,
        })
    }
}

/// Sound per-source pruning predicates: each is implied by the query filter,
/// so a source that fails one has no matching row. Built by the planner;
/// evaluated by storage against whatever statistics and indexes a source has.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Pruning {
    /// Inclusive `event_time` bounds.
    pub time_lo: Option<i64>,
    pub time_hi: Option<i64>,
    /// Attribute keys that must be present for any row to match.
    pub required_attrs: BTreeSet<String>,
    pub required_resource: BTreeSet<String>,
    /// Severity numbers one of which a matching row must have.
    pub severities: Option<BTreeSet<u8>>,
    /// `field = one of values` (string values only), compiled to Bloom probes.
    equals: Vec<(BloomField, Vec<String>)>,
    /// `field LIKE pattern`, compiled to the folded trigrams every match
    /// contains.
    like: Vec<(NgramField, Vec<String>)>,
}

impl Pruning {
    /// Record that a matching row has one of `values` in `field`. Returns
    /// whether storage can use it (the field is indexable for equality).
    pub fn add_equals(&mut self, field: &IndexField, values: Vec<String>) -> bool {
        match field.bloom() {
            Some(f) => {
                self.equals.push((f, values));
                true
            }
            None => false,
        }
    }

    /// Record that a matching row's `field` matches the `LIKE`/`ILIKE`
    /// `pattern`. Returns whether storage can use it (the field is indexable
    /// for substrings and the pattern has a literal run long enough to probe).
    pub fn add_like(&mut self, field: &IndexField, pattern: &str) -> bool {
        let probes = stratadb_segment::ngram::probes(pattern);
        match field.ngram() {
            Some(f) if !probes.is_empty() => {
                self.like.push((f, probes));
                true
            }
            _ => false,
        }
    }

    /// Whether any index-backed predicate was recorded.
    pub fn has_index_candidates(&self) -> bool {
        !self.equals.is_empty() || !self.like.is_empty()
    }

    /// Human-readable index-backed predicates, for `EXPLAIN`.
    pub fn index_candidates(&self) -> Vec<String> {
        self.equals
            .iter()
            .map(|(f, v)| format!("{} ({} value(s))", f.describe(), v.len()))
            .chain(
                self.like
                    .iter()
                    .map(|(f, g)| format!("{} ({} trigram(s))", f.describe(), g.len())),
            )
            .collect()
    }

    /// The index that pruned a source, for statistics (`None` for pruning
    /// by zone statistics).
    pub fn index_name(&self, why: Pruned) -> Option<String> {
        match why {
            Pruned::Equals(i) => self.equals.get(i).map(|(f, _)| f.describe()),
            Pruned::Like(i) => self.like.get(i).map(|(f, _)| f.describe()),
            Pruned::Time | Pruned::Keys | Pruned::Severity => None,
        }
    }
}

/// Why a source was pruned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pruned {
    /// Its `event_time` range is outside the predicate's.
    Time,
    /// A required attribute key is absent from every row.
    Keys,
    /// No row has a required severity.
    Severity,
    /// The equality predicate at this index matches no row (Bloom filter).
    Equals(usize),
    /// The `LIKE` predicate at this index matches no row (trigram filter).
    Like(usize),
}

/// What a scan needs from a source. Attribute scopes referenced only through
/// specific keys can be served per key instead of as whole maps.
#[derive(Debug, Clone, Copy)]
pub struct ColumnRequest<'a> {
    pub columns: &'a [ColumnId],
    /// Keys referenced individually (`attributes['k']`), and whether the
    /// whole map is referenced. Same for resource attributes.
    pub attr_keys: &'a BTreeSet<String>,
    pub whole_attributes: bool,
    pub resource_keys: &'a BTreeSet<String>,
    pub whole_resource: bool,
    /// Keys needed only as encoded group keys: may be returned undecoded.
    pub raw_attr_keys: &'a BTreeSet<String>,
    pub raw_resource_keys: &'a BTreeSet<String>,
    /// Serve key-only scopes per key even when some keys are not stored as
    /// separate columns (projection of the residual map).
    pub projection: bool,
}

/// Decoded columns of one source, one entry per row.
#[derive(Debug, Default)]
pub struct ColumnBatch {
    pub rows: usize,
    pub columns: HashMap<ColumnId, ColumnData>,
    /// Per-key attribute columns served instead of whole maps.
    pub keys: HashMap<(Scope, String), Vec<Option<Value>>>,
    /// Per-key attribute columns kept encoded (`ColumnRequest::raw_*_keys`).
    pub raw: HashMap<(Scope, String), RawValues>,
    /// Stored bytes read to produce the batch.
    pub bytes_read: u64,
    /// Attribute scopes whose keys were all served from dedicated columns.
    pub dedicated_key_reads: u64,
    /// Columns served from the decoded column cache (no read, no decode).
    pub cached_columns: u64,
}

/// A source of rows for the executor. Trait objects are used at this
/// boundary only (one call per source, not per row; STRATA-ARCH-11-003).
pub trait ScanSource: Send + Sync + std::fmt::Debug {
    /// Whether every row belongs to `tenant` (checked before any read, and
    /// again by [`ScanSource::read`]).
    fn owned_by(&self, tenant: &TenantId) -> bool;
    /// Number of rows.
    fn rows(&self) -> u64;
    /// Inclusive `event_time` range of the rows.
    fn event_time_range(&self) -> (i64, i64);
    /// Inclusive commit sequence range of the rows.
    fn seq_range(&self) -> (u64, u64);
    /// `Some` when `p` proves that no row of this source can match.
    fn prune(&self, p: &Pruning) -> Option<Pruned>;
    /// Read the requested columns for `tenant`. Fails (and never returns a
    /// partial batch) if the source belongs to another tenant or cannot be
    /// read.
    fn read(&self, tenant: &TenantId, req: &ColumnRequest<'_>) -> Result<ColumnBatch>;
    /// Estimated memory of the batch `read` would return, so callers can
    /// reserve it before reading (STRATA-SYS-05-005). Computed from
    /// metadata only; no data is read.
    fn estimated_read_bytes(&self, req: &ColumnRequest<'_>) -> u64;
    /// A process-unique identity of this source's rows when they can never
    /// change (a flushed segment): two sources with the same identity hold
    /// identical rows, and a rewrite (compaction, retention) yields a new
    /// one. `None` for sources that can still change (memtable windows).
    /// Caches of per-source results key on it (spec STRATA-ARCH-09-003).
    fn identity(&self) -> Option<u64> {
        None
    }

    /// Exact row count per severity code, from metadata, when the source
    /// records it (aggregates answered without reading, STRATA-SYS-09-006).
    /// `None` when unknown.
    fn severity_counts(&self) -> Option<Vec<(u8, u64)>> {
        None
    }
}

/// Per-row size of a decoded column slot (the vector element itself).
fn slot_bytes(id: ColumnId) -> u64 {
    use std::mem::size_of;
    (match id {
        ColumnId::EventTime
        | ColumnId::IngestTime
        | ColumnId::Seq
        | ColumnId::Idx
        | ColumnId::SchemaVersion => size_of::<i64>(),
        ColumnId::Severity => 1,
        ColumnId::Service
        | ColumnId::SeverityText
        | ColumnId::TraceId
        | ColumnId::SpanId
        | ColumnId::EventId => size_of::<Option<String>>(),
        ColumnId::Body => size_of::<Option<Value>>(),
        ColumnId::Attributes | ColumnId::ResourceAttributes => {
            size_of::<std::collections::BTreeMap<String, Value>>()
        }
    }) as u64
}

/// Decoded heap bytes per encoded byte: map entries carry B-tree node and
/// `String`/`Value` headers; other variable-size values about one header.
fn payload_factor(id: ColumnId) -> u64 {
    match id {
        ColumnId::Attributes | ColumnId::ResourceAttributes => 4,
        ColumnId::Body => 2,
        _ => 1,
    }
}

fn foreign() -> Error {
    Error::internal("scan source belongs to another tenant")
}

impl ScanSource for SegmentHandle {
    fn identity(&self) -> Option<u64> {
        Some(self.uid())
    }

    fn owned_by(&self, tenant: &TenantId) -> bool {
        &self.entry.tenant == tenant && &self.reader.footer().tenant_id == tenant
    }

    fn rows(&self) -> u64 {
        self.reader.footer().row_count
    }

    fn severity_counts(&self) -> Option<Vec<(u8, u64)>> {
        let f = self.reader.footer();
        let counts: Vec<(u8, u64)> = f.severity_counts.iter().map(|(k, v)| (*k, *v)).collect();
        // Trust the map only when it accounts for every row (a writer that
        // did not record it leaves it empty).
        (counts.iter().map(|(_, n)| n).sum::<u64>() == f.row_count).then_some(counts)
    }

    fn event_time_range(&self) -> (i64, i64) {
        (self.entry.min_event_time, self.entry.max_event_time)
    }

    fn seq_range(&self) -> (u64, u64) {
        (self.entry.min_seq, self.entry.max_seq)
    }

    fn prune(&self, p: &Pruning) -> Option<Pruned> {
        let e = &self.entry;
        if p.time_lo.is_some_and(|lo| e.max_event_time < lo)
            || p.time_hi.is_some_and(|hi| e.min_event_time > hi)
        {
            return Some(Pruned::Time);
        }
        let f = self.reader.footer();
        if p.required_attrs
            .iter()
            .any(|k| !f.attribute_keys.may_contain(k))
            || p.required_resource
                .iter()
                .any(|k| !f.resource_keys.may_contain(k))
        {
            return Some(Pruned::Keys);
        }
        if let Some(sevs) = &p.severities {
            if !sevs
                .iter()
                .any(|s| f.severity_counts.get(s).is_some_and(|n| *n > 0))
            {
                return Some(Pruned::Severity);
            }
        }
        if !p.equals.is_empty() {
            if let Some(bs) = self.bloom() {
                for (i, (field, vals)) in p.equals.iter().enumerate() {
                    if let Some(b) = bs.get(field) {
                        if !vals.iter().any(|v| b.may_contain(v)) {
                            return Some(Pruned::Equals(i));
                        }
                    }
                }
            }
        }
        if !p.like.is_empty() {
            if let Some(ns) = self.ngram() {
                for (i, (field, probes)) in p.like.iter().enumerate() {
                    if !ns.may_match(field, probes) {
                        return Some(Pruned::Like(i));
                    }
                }
            }
        }
        None
    }

    fn read(&self, tenant: &TenantId, req: &ColumnRequest<'_>) -> Result<ColumnBatch> {
        if !self.owned_by(tenant) {
            return Err(foreign());
        }
        let footer = self.reader.footer();
        let mut out = ColumnBatch {
            rows: usize::try_from(footer.row_count)
                .map_err(|_| Error::internal("segment row count exceeds usize"))?,
            ..ColumnBatch::default()
        };
        // A scope referenced only through specific keys is read per key
        // (shredded chunks, or a projection of the residual map chunk that
        // skips every other value) instead of materializing whole maps.
        // Without projection, only scopes whose keys are all shredded here.
        let per_key = |scope: Scope, keys: &BTreeSet<String>, whole: bool| {
            !whole
                && !keys.is_empty()
                && (req.projection || keys.iter().all(|k| footer.shredded(scope, k).is_some()))
        };
        let attr = per_key(Scope::Attributes, req.attr_keys, req.whole_attributes);
        let res = per_key(Scope::Resource, req.resource_keys, req.whole_resource);
        for id in req.columns {
            if (*id == ColumnId::Attributes && attr) || (*id == ColumnId::ResourceAttributes && res)
            {
                continue;
            }
            if let Some(c) = self
                .column_cache()
                .and_then(|cc| cc.get(self.entry.id, *id))
            {
                out.columns.insert(*id, c);
                out.cached_columns += 1;
                continue;
            }
            let col = self.reader.read_column(*id)?;
            if let Some(cc) = self.column_cache() {
                cc.put(self.entry.id, *id, &col);
            }
            out.columns.insert(*id, col);
            out.bytes_read += footer.column(*id).map_or(0, |c| c.len);
            // Full maps also read their shredded chunks.
            let scope = match id {
                ColumnId::Attributes => Some(Scope::Attributes),
                ColumnId::ResourceAttributes => Some(Scope::Resource),
                _ => None,
            };
            if let Some(scope) = scope {
                out.bytes_read += footer
                    .shredded
                    .iter()
                    .filter(|c| c.scope == scope)
                    .map(|c| c.len)
                    .sum::<u64>();
            }
        }
        for (scope, keys, raw_keys, on) in [
            (Scope::Attributes, req.attr_keys, req.raw_attr_keys, attr),
            (
                Scope::Resource,
                req.resource_keys,
                req.raw_resource_keys,
                res,
            ),
        ] {
            if !on {
                continue;
            }
            if keys.iter().all(|k| footer.shredded(scope, k).is_some()) {
                out.dedicated_key_reads += 1;
            }
            let mut decode: Vec<String> = Vec::with_capacity(keys.len());
            for k in keys {
                match footer.shredded(scope, k) {
                    // Keys needed only as group keys stay encoded.
                    Some(c) if raw_keys.contains(k) => {
                        if let Some(r) = self.reader.read_shredded_raw(scope, k)? {
                            out.bytes_read += c.len;
                            out.raw.insert((scope, k.clone()), r);
                            continue;
                        }
                        decode.push(k.clone());
                    }
                    _ => decode.push(k.clone()),
                }
            }
            if decode.is_empty() {
                continue;
            }
            let (vals, bytes) = self.reader.read_map_keys(scope, &decode)?;
            out.bytes_read += bytes;
            for (k, col) in decode.into_iter().zip(vals) {
                out.keys.insert((scope, k), col);
            }
        }
        Ok(out)
    }

    fn estimated_read_bytes(&self, req: &ColumnRequest<'_>) -> u64 {
        let f = self.reader.footer();
        let rows = f.row_count;
        let per_key = |scope: Scope, keys: &BTreeSet<String>, whole: bool| {
            !whole
                && !keys.is_empty()
                && (req.projection || keys.iter().all(|k| f.shredded(scope, k).is_some()))
        };
        let attr = per_key(Scope::Attributes, req.attr_keys, req.whole_attributes);
        let res = per_key(Scope::Resource, req.resource_keys, req.whole_resource);
        let mut total = 0u64;
        for id in req.columns {
            let scope = match id {
                ColumnId::Attributes if attr => continue,
                ColumnId::ResourceAttributes if res => continue,
                ColumnId::Attributes => Some(Scope::Attributes),
                ColumnId::ResourceAttributes => Some(Scope::Resource),
                _ => None,
            };
            let mut raw = f.column(*id).map_or(0, |c| c.raw_len);
            if let Some(scope) = scope {
                raw += f
                    .shredded
                    .iter()
                    .filter(|c| c.scope == scope)
                    .map(|c| c.raw_len)
                    .sum::<u64>();
            }
            total = total
                .saturating_add(rows.saturating_mul(slot_bytes(*id)))
                .saturating_add(raw.saturating_mul(payload_factor(*id)));
        }
        let value_slot = std::mem::size_of::<Option<Value>>() as u64;
        for (scope, keys, on, map) in [
            (Scope::Attributes, req.attr_keys, attr, ColumnId::Attributes),
            (
                Scope::Resource,
                req.resource_keys,
                res,
                ColumnId::ResourceAttributes,
            ),
        ] {
            if !on {
                continue;
            }
            for k in keys {
                let raw = match f.shredded(scope, k) {
                    Some(c) => c.raw_len,
                    // A key projected from the residual map: bounded by it.
                    None => f.column(map).map_or(0, |c| c.raw_len),
                };
                total = total
                    .saturating_add(rows.saturating_mul(value_slot))
                    .saturating_add(raw.saturating_mul(2));
            }
        }
        total
    }
}

/// A window of one memtable column chunk, scanned like a tiny unsorted
/// segment (spec STRATA-ARCH-02-013): pruned by the chunk's zone map (time,
/// severity), read straight from its columns.
#[derive(Debug, Clone, Copy)]
pub struct MemChunk<'a> {
    tenant: &'a TenantId,
    chunk: &'a Chunk,
    from: usize,
    to: usize,
}

impl<'a> MemChunk<'a> {
    pub(crate) fn new(tenant: &'a TenantId, chunk: &'a Chunk, from: usize, to: usize) -> Self {
        Self {
            tenant,
            chunk,
            from,
            to,
        }
    }

    fn ids(&self) -> std::ops::Range<usize> {
        self.from..self.to
    }

    fn strs(&self, f: impl Fn(&Chunk, usize) -> Result<&Option<String>>) -> Result<StrColumn> {
        let mut b = StrBuilder::with_capacity(self.to - self.from, 0);
        for i in self.ids() {
            b.push(f(self.chunk, i)?.as_deref())?;
        }
        Ok(b.finish())
    }

    /// Column `id` of this window.
    fn column(&self, id: ColumnId) -> Result<ColumnData> {
        let c = self.chunk;
        let ints =
            |f: &dyn Fn(usize) -> i64| ColumnData::Ints(Arc::new(self.ids().map(f).collect()));
        Ok(match id {
            ColumnId::EventTime => ints(&|i| c.event_time(i)),
            ColumnId::IngestTime => ints(&|i| c.ingest_time(i)),
            ColumnId::SchemaVersion => ints(&|i| i64::from(c.schema_version(i))),
            ColumnId::Seq => ints(&|i| c.seq(i) as i64),
            ColumnId::Idx => ints(&|i| i64::from(c.idx(i))),
            ColumnId::Severity => {
                ColumnData::U8s(Arc::new(self.ids().map(|i| c.severity(i)).collect()))
            }
            ColumnId::Service => ColumnData::Strs(self.strs(Chunk::service)?),
            ColumnId::SeverityText => ColumnData::Strs(self.strs(Chunk::severity_text)?),
            ColumnId::TraceId => ColumnData::Strs(self.strs(Chunk::trace_id)?),
            ColumnId::SpanId => ColumnData::Strs(self.strs(Chunk::span_id)?),
            ColumnId::EventId => ColumnData::Strs(self.strs(Chunk::event_id)?),
            ColumnId::Body => ColumnData::Values(Arc::new(
                self.ids()
                    .map(|i| c.body(i).cloned())
                    .collect::<Result<_>>()?,
            )),
            ColumnId::Attributes => ColumnData::Maps(Arc::new(
                self.ids()
                    .map(|i| c.attributes(i).cloned())
                    .collect::<Result<_>>()?,
            )),
            ColumnId::ResourceAttributes => ColumnData::Maps(Arc::new(
                self.ids()
                    .map(|i| c.resource_attributes(i).cloned())
                    .collect::<Result<_>>()?,
            )),
        })
    }

    /// Heap bytes of column `id` in this window (for the read estimate).
    fn payload(&self, id: ColumnId) -> usize {
        let c = self.chunk;
        let opt =
            |r: Result<&Option<String>>| r.ok().and_then(|s| s.as_ref()).map_or(0, String::len);
        let map = |r: Result<&std::collections::BTreeMap<String, Value>>| {
            r.map_or(0, |m| {
                m.iter().map(|(k, v)| k.len() + 32 + v.approx_size()).sum()
            })
        };
        self.ids()
            .map(|i| match id {
                ColumnId::Service => opt(c.service(i)),
                ColumnId::SeverityText => opt(c.severity_text(i)),
                ColumnId::TraceId => opt(c.trace_id(i)),
                ColumnId::SpanId => opt(c.span_id(i)),
                ColumnId::EventId => opt(c.event_id(i)),
                ColumnId::Body => c
                    .body(i)
                    .ok()
                    .and_then(Option::as_ref)
                    .map_or(0, Value::approx_size),
                ColumnId::Attributes => map(c.attributes(i)),
                ColumnId::ResourceAttributes => map(c.resource_attributes(i)),
                _ => 0,
            })
            .sum()
    }
}

impl ScanSource for MemChunk<'_> {
    fn owned_by(&self, tenant: &TenantId) -> bool {
        self.tenant == tenant
    }

    fn rows(&self) -> u64 {
        (self.to - self.from) as u64
    }

    fn event_time_range(&self) -> (i64, i64) {
        let t = self.ids().map(|i| self.chunk.event_time(i));
        (t.clone().min().unwrap_or(0), t.max().unwrap_or(0))
    }

    fn seq_range(&self) -> (u64, u64) {
        let s = self.ids().map(|i| self.chunk.seq(i));
        (s.clone().min().unwrap_or(0), s.max().unwrap_or(0))
    }

    /// Zone-map pruning: the chunk's time range and severity set cover all
    /// its rows (also those not yet visible), so a miss is sound. Required
    /// attribute keys are not tracked in memtable chunks.
    fn prune(&self, p: &Pruning) -> Option<Pruned> {
        let ((lo, hi), _, sevs) = self.chunk.zone();
        if lo > hi {
            return None;
        }
        if p.time_lo.is_some_and(|t| hi < t) || p.time_hi.is_some_and(|t| lo > t) {
            return Some(Pruned::Time);
        }
        if let Some(want) = &p.severities {
            if !want.iter().any(|s| *s < 32 && sevs & (1u32 << s) != 0) {
                return Some(Pruned::Severity);
            }
        }
        None
    }

    /// Whole columns only: attribute maps are in memory already, so per-key
    /// requests are served from the maps by the executor.
    fn read(&self, tenant: &TenantId, req: &ColumnRequest<'_>) -> Result<ColumnBatch> {
        if !self.owned_by(tenant) {
            return Err(foreign());
        }
        Ok(ColumnBatch {
            rows: self.to - self.from,
            columns: req
                .columns
                .iter()
                .map(|id| Ok((*id, self.column(*id)?)))
                .collect::<Result<_>>()?,
            ..ColumnBatch::default()
        })
    }

    /// Cloned column data: the slots plus the payload of the requested
    /// columns only.
    fn estimated_read_bytes(&self, req: &ColumnRequest<'_>) -> u64 {
        let rows = (self.to - self.from) as u64;
        req.columns
            .iter()
            .map(|id| {
                rows.saturating_mul(slot_bytes(*id))
                    .saturating_add(self.payload(*id) as u64)
            })
            .fold(0u64, u64::saturating_add)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn only_indexable_predicates_are_recorded() {
        let mut p = Pruning::default();
        assert!(p.add_equals(&IndexField::TraceId, vec!["t".into()]));
        assert!(!p.add_equals(&IndexField::Body, vec!["b".into()]));
        assert!(p.add_like(&IndexField::Body, "%timeout%"));
        assert!(!p.add_like(&IndexField::TraceId, "%timeout%"));
        // Too short to contain a trigram.
        assert!(!p.add_like(&IndexField::Body, "%ab%"));
        assert!(p.has_index_candidates());
        assert_eq!(
            p.index_name(Pruned::Equals(0)).as_deref(),
            Some("bloom(trace_id)")
        );
        assert_eq!(
            p.index_name(Pruned::Like(0)).as_deref(),
            Some("ngram(body)")
        );
        assert_eq!(p.index_name(Pruned::Time), None);
        assert_eq!(p.index_candidates().len(), 2);
    }
}
