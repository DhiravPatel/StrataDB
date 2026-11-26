//! Budgeted executor over a tenant-scoped storage [`Snapshot`] (spec §13.5, §13.6).
//!
//! Data flows as columnar batches: segments contribute only the column chunks
//! the plan needs (each checksummed on read); memtable rows are converted into
//! the same representation. The filter, projection, grouping and sort keys are
//! evaluated per row over a batch.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use stratadb_common::codec::Encoder;
use stratadb_common::{Error, ErrorKind, Result, TenantId};
use stratadb_schema::{Event, Severity, Value};
use stratadb_segment::{ColumnData, ColumnId, RawValues, Scope};
use stratadb_storage::{SegmentHandle, Snapshot};

use crate::ast::{AggFunc, CmpOp, ColumnRef, ExplainMode, Expr};
use crate::eval::{self, Datum, Truth};
use crate::page::{self, Cursor};
use crate::plan::{OutputSource, Plan, Pruning, SortSource};

/// Per-query resource budget and switches. Defaults are conservative.
#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub timeout: Duration,
    pub max_rows_scanned: u64,
    pub max_result_rows: usize,
    /// Approximate bound on memory held by result rows and aggregation groups.
    pub max_memory_bytes: usize,
    pub cancel: Option<Arc<AtomicBool>>,
    /// Segment pruning by statistics. Disabling it must never change results.
    pub enable_pruning: bool,
    /// Vectorized filter evaluation (typed kernels, selection vectors).
    /// Disabling it must never change results.
    pub enable_vectorized: bool,
    /// Read only the referenced attribute keys (shredded chunks or a
    /// projection of the residual map chunk) instead of whole maps when a
    /// query names specific keys. Disabling it must never change results.
    pub enable_map_projection: bool,
    pub parse_limits: crate::parser::ParseLimits,
    /// Worker threads for scanning segments (sorted and aggregate queries;
    /// spec §13.5). 1 = sequential. Results are merged in segment order, so
    /// rows and groups match a sequential scan; float `sum`/`avg` may differ
    /// in the last bits (different addition order), deterministically.
    pub parallelism: usize,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_rows_scanned: 100_000_000,
            max_result_rows: 100_000,
            max_memory_bytes: 256 * 1024 * 1024,
            cancel: None,
            enable_pruning: true,
            enable_vectorized: true,
            enable_map_projection: true,
            parse_limits: Default::default(),
            parallelism: std::thread::available_parallelism()
                .map_or(1, |n| n.get())
                .min(8),
        }
    }
}

/// Execution statistics (EXPLAIN ANALYZE and API responses).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct QueryStats {
    pub segments_total: usize,
    pub segments_pruned_time: usize,
    pub segments_pruned_keys: usize,
    pub segments_pruned_severity: usize,
    pub segments_pruned_bloom: usize,
    /// Segments skipped because a trigram filter excludes a `LIKE` pattern.
    pub segments_pruned_ngram: usize,
    /// Segments skipped because a pagination cursor excludes all their rows.
    pub segments_pruned_cursor: usize,
    /// Segment-scope reads served from shredded attribute columns.
    pub shredded_reads: u64,
    /// Indexes that pruned at least one segment (e.g. `bloom(trace_id)`).
    pub indexes_used: Vec<String>,
    pub segments_scanned: usize,
    pub memory_rows_scanned: u64,
    pub rows_scanned: u64,
    pub rows_matched: u64,
    pub bytes_read: u64,
    pub peak_memory_bytes: usize,
    pub early_stop: bool,
    /// Execution time (scan to result rows), excluding parse and plan.
    pub elapsed: Duration,
    /// Parse time (spec VIII.1.4 / STRATA-SYS-01-007: reported separately).
    pub parse: Duration,
    /// Plan (bind + optimize) time, reported separately from execution.
    pub plan: Duration,
}

/// Result of a query. `rows[i][j]` is `None` when the value is missing.
#[derive(Debug, Clone)]
pub struct QueryOutput {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Datum>>,
    pub stats: QueryStats,
    /// Plan text for EXPLAIN / EXPLAIN ANALYZE (rows are empty then).
    pub explain: Option<String>,
}

/// Parse, plan, and run `sql` against `snapshot`.
pub fn execute(snapshot: &Snapshot, sql: &str, opts: &QueryOptions) -> Result<QueryOutput> {
    let t = Instant::now();
    let stmt = crate::parser::parse(sql, &opts.parse_limits)?;
    let parsed = t.elapsed();
    let plan = Plan::new(stmt)?;
    let planned = t.elapsed() - parsed;
    let mut res = run(snapshot, &plan, opts, None)?;
    res.output.stats.parse = parsed;
    res.output.stats.plan = planned;
    if res.output.explain.is_none() && res.rows.len() > opts.max_result_rows {
        return Err(Error::resource_exhausted(format!(
            "result has more than {} rows; add a LIMIT or paginate",
            opts.max_result_rows
        )));
    }
    res.output.rows = res.rows.into_iter().map(|r| r.out).collect();
    Ok(res.output)
}

/// One page of a row query (spec §2.3, ADR-0023).
#[derive(Debug, Clone)]
pub struct PageRequest {
    /// Maximum rows in this page (>= 1, <= `QueryOptions::max_result_rows`).
    pub page_size: usize,
    /// `next_cursor` of the previous page; `None` for the first page.
    pub cursor: Option<String>,
}

/// A page of results and the cursor for the next one.
#[derive(Debug, Clone)]
pub struct Page {
    pub output: QueryOutput,
    /// `None` when there are no more rows.
    pub next_cursor: Option<String>,
}

/// Run one page of a paginated row query with keyset semantics:
///
/// * rows are ordered by the query's `ORDER BY` keys, then by row identity
///   `(seq, idx)`; without `ORDER BY` the order is ingest (commit) order;
/// * a page returns the first `page_size` rows strictly after the cursor, so
///   pages never repeat a row;
/// * every page reads only rows visible to the *first* page's snapshot
///   (`as_of_seq` in the cursor): each row acknowledged before the first page
///   and not since removed by retention is returned exactly once, and rows
///   committed after the first page never appear;
/// * the query's `LIMIT` bounds the total across all pages.
///
/// The cursor is bound to the tenant and the query text. Aggregate and
/// `EXPLAIN` queries cannot be paginated.
pub fn execute_page(
    snapshot: &Snapshot,
    sql: &str,
    opts: &QueryOptions,
    req: &PageRequest,
) -> Result<Page> {
    let t = Instant::now();
    let stmt = crate::parser::parse(sql, &opts.parse_limits)?;
    let parsed = t.elapsed();
    let plan = Plan::new(stmt)?;
    let planned = t.elapsed() - parsed;
    if plan.explain != ExplainMode::None {
        return Err(Error::invalid_input("EXPLAIN queries cannot be paginated"));
    }
    if plan.aggregate {
        return Err(Error::invalid_input(
            "aggregate queries cannot be paginated; their result size is bounded by max_rows",
        ));
    }
    if req.page_size == 0 {
        return Err(Error::invalid_input("page_size must be at least 1"));
    }
    if req.page_size > opts.max_result_rows {
        return Err(Error::resource_exhausted(format!(
            "page_size {} exceeds the result row limit {}",
            req.page_size, opts.max_result_rows
        )));
    }
    let fp = page::fingerprint(snapshot.tenant(), sql);
    let cursor = match &req.cursor {
        None => None,
        Some(text) => {
            let c = Cursor::decode(text)?;
            if c.fingerprint != fp {
                return Err(Error::invalid_input(
                    "pagination cursor belongs to a different query or tenant",
                ));
            }
            if c.keys.len() != plan.order.len() {
                return Err(Error::invalid_input(
                    "pagination cursor does not match the query's ORDER BY",
                ));
            }
            Some(c)
        }
    };
    let returned = cursor.as_ref().map_or(0, |c| c.returned);
    let as_of = cursor
        .as_ref()
        .map_or_else(|| snapshot.max_visible_seq(), |c| c.as_of_seq);
    let remaining = plan.limit.map(|l| l.saturating_sub(returned));
    let take = remaining.map_or(req.page_size, |r| {
        req.page_size.min(usize::try_from(r).unwrap_or(usize::MAX))
    });
    if take == 0 {
        // LIMIT 0, or the LIMIT was already satisfied by earlier pages.
        return Ok(Page {
            output: QueryOutput {
                columns: plan.output_names.clone(),
                rows: Vec::new(),
                stats: QueryStats::default(),
                explain: None,
            },
            next_cursor: None,
        });
    }
    let pctx = PageCtx {
        as_of,
        after: cursor.map(|c| (c.keys, (c.seq, c.idx))),
        // One extra row tells whether another page exists.
        k: take.saturating_add(1),
    };
    let mut res = run(snapshot, &plan, opts, Some(pctx))?;
    res.output.stats.parse = parsed;
    res.output.stats.plan = planned;
    let more = res.rows.len() > take && remaining.is_none_or(|r| (take as u64) < r);
    res.rows.truncate(take);
    let next_cursor = match (more, res.rows.last()) {
        (true, Some(last)) => Some(
            Cursor {
                fingerprint: fp,
                as_of_seq: as_of,
                returned: returned + res.rows.len() as u64,
                keys: last.keys.clone(),
                seq: last.id.0,
                idx: last.id.1,
            }
            .encode(),
        ),
        _ => None,
    };
    res.output.rows = res.rows.into_iter().map(|r| r.out).collect();
    Ok(Page {
        output: res.output,
        next_cursor,
    })
}

/// Pagination state for one execution.
#[derive(Debug, Clone)]
struct PageCtx {
    /// Rows with a larger WAL sequence number are invisible.
    as_of: u64,
    /// Position of the previous page's last row: (order keys, (seq, idx)).
    after: Option<(Vec<Datum>, (u64, u32))>,
    /// Rows to keep (top-k).
    k: usize,
}

/// A result row: output values, `ORDER BY` key values (one per
/// `Plan::order` entry), and the row identity `(seq, idx)` — `(0, 0)` unless
/// paginating.
#[derive(Debug, Clone)]
struct Row {
    out: Vec<Datum>,
    keys: Vec<Datum>,
    id: (u64, u32),
}

/// Total order of result rows: order keys (each ASC/DESC), then identity.
fn cmp_position(
    order: &[(SortSource, bool)],
    a: (&[Datum], (u64, u32)),
    b: (&[Datum], (u64, u32)),
) -> std::cmp::Ordering {
    for (j, (_, desc)) in order.iter().enumerate() {
        let o = eval::sort_cmp(&a.0[j], &b.0[j]);
        let o = if *desc { o.reverse() } else { o };
        if o.is_ne() {
            return o;
        }
    }
    a.1.cmp(&b.1)
}

/// Sort `rows` by `order` and keep the first `keep`. With a bound smaller
/// than the row count this selects the top `k` (O(n + k log k)) instead of
/// sorting everything; ties are broken by the rows' current positions, so the
/// result equals a stable sort followed by truncation exactly.
fn sort_keep(rows: &mut Vec<Row>, order: &[(SortSource, bool)], keep: Option<usize>) {
    let k = match keep {
        Some(k) if k < rows.len() => k,
        _ => {
            rows.sort_by(|a, b| cmp_position(order, (&a.keys, a.id), (&b.keys, b.id)));
            return;
        }
    };
    let all = std::mem::take(rows);
    rows.extend(top_k(all, k, |a, b| {
        cmp_position(order, (&a.keys, a.id), (&b.keys, b.id))
    }));
}

/// The first `k` items of `items` stably sorted by `cmp`, in that order,
/// without sorting the rest: select by (`cmp`, position), then sort the `k`.
fn top_k<T>(
    items: Vec<T>,
    k: usize,
    cmp: impl Fn(&T, &T) -> std::cmp::Ordering,
) -> impl Iterator<Item = T> {
    let mut t: Vec<(usize, T)> = items.into_iter().enumerate().collect();
    let total = |a: &(usize, T), b: &(usize, T)| cmp(&a.1, &b.1).then(a.0.cmp(&b.0));
    if k == 0 {
        t.clear();
    } else if k < t.len() {
        t.select_nth_unstable_by(k - 1, total);
        t.truncate(k);
    }
    t.sort_unstable_by(total);
    t.into_iter().map(|(_, x)| x)
}

/// Raw result of [`run`] before rows are reduced to output values.
struct RunResult {
    output: QueryOutput,
    rows: Vec<Row>,
}

// ---------------------------------------------------------------------------
// Batches

pub(crate) struct Batch<'a> {
    pub(crate) len: usize,
    pub(crate) tenant: &'a TenantId,
    pub(crate) cols: HashMap<ColumnId, ColumnData>,
    /// Shredded attribute columns read instead of whole maps (RFC-0002).
    pub(crate) shredded: HashMap<(Scope, String), Vec<Datum>>,
    /// Shredded attribute columns kept encoded (keys used only as group
    /// keys, `Plan::raw_group_attr_keys`).
    pub(crate) raw: HashMap<(Scope, String), RawValues>,
}

/// Where one group key's values come from in a batch — resolved once per
/// batch, so per-row encoding does no map lookups or key clones.
enum KeySrc<'b> {
    Strs(&'b [Option<String>]),
    Values(&'b [Datum]),
    /// Already encoded: copied verbatim (no decode, no allocation).
    Raw(&'b RawValues),
    Map(&'b [BTreeMap<String, Value>], &'b str),
    Eval(&'b Expr),
}

impl KeySrc<'_> {
    /// Append the `eval::group_key_bytes` encoding of row `i`.
    fn encode(&self, b: &Batch<'_>, i: usize, enc: &mut Encoder) -> Result<()> {
        use stratadb_schema::codec::{encode_string_value, encode_value};
        let val = |enc: &mut Encoder, v: Option<&Value>| match v {
            Some(v) => encode_value(enc, v),
            None => enc.put_u8(eval::MISSING_KEY_TAG),
        };
        match self {
            KeySrc::Strs(v) => match &v[i] {
                Some(s) => encode_string_value(enc, s),
                None => enc.put_u8(eval::MISSING_KEY_TAG),
            },
            KeySrc::Values(v) => val(enc, v[i].as_ref()),
            KeySrc::Raw(r) => match r.bytes(i) {
                Some(bytes) => enc.put_raw(bytes),
                None => enc.put_u8(eval::MISSING_KEY_TAG),
            },
            KeySrc::Map(m, k) => val(enc, m[i].get(*k)),
            KeySrc::Eval(e) => val(enc, eval_expr(e, b, i)?.as_ref()),
        }
        Ok(())
    }
}

fn opt_str(e: &Event, id: ColumnId) -> Option<String> {
    match id {
        ColumnId::Service => e.service.clone(),
        ColumnId::SeverityText => e.severity_text.clone(),
        ColumnId::TraceId => e.trace_id.clone(),
        ColumnId::SpanId => e.span_id.clone(),
        _ => e.event_id.clone(),
    }
}

fn column_from_events(id: ColumnId, evs: &[&Event]) -> ColumnData {
    match id {
        ColumnId::EventTime => ColumnData::Ints(evs.iter().map(|e| e.event_time).collect()),
        ColumnId::IngestTime => ColumnData::Ints(evs.iter().map(|e| e.ingest_time).collect()),
        ColumnId::SchemaVersion => {
            ColumnData::Ints(evs.iter().map(|e| i64::from(e.schema_version)).collect())
        }
        // Filled from the memtable rows by `memory_column`.
        ColumnId::Seq | ColumnId::Idx => ColumnData::Ints(vec![0; evs.len()]),
        ColumnId::Severity => ColumnData::U8s(evs.iter().map(|e| e.severity.as_u8()).collect()),
        ColumnId::Service
        | ColumnId::SeverityText
        | ColumnId::TraceId
        | ColumnId::SpanId
        | ColumnId::EventId => ColumnData::Strs(evs.iter().map(|e| opt_str(e, id)).collect()),
        ColumnId::Body => ColumnData::Values(evs.iter().map(|e| e.body.clone()).collect()),
        ColumnId::Attributes => {
            ColumnData::Maps(evs.iter().map(|e| e.attributes.clone()).collect())
        }
        ColumnId::ResourceAttributes => {
            ColumnData::Maps(evs.iter().map(|e| e.resource_attributes.clone()).collect())
        }
    }
}

fn memory_column(id: ColumnId, rows: &[stratadb_storage::MemRow], evs: &[&Event]) -> ColumnData {
    match id {
        ColumnId::Seq => ColumnData::Ints(rows.iter().map(|r| r.seq as i64).collect()),
        ColumnId::Idx => ColumnData::Ints(rows.iter().map(|r| i64::from(r.idx)).collect()),
        _ => column_from_events(id, evs),
    }
}

impl Batch<'_> {
    pub(crate) fn col(&self, id: ColumnId) -> Result<&ColumnData> {
        self.cols
            .get(&id)
            .ok_or_else(|| Error::internal(format!("column {id:?} not loaded")))
    }

    /// Append the group-key encoding of `e` at row `i` to `enc`: identical to
    /// `eval::group_key_bytes(&eval_expr(e, self, i)?)`, but column
    /// references are encoded from the batch by reference (no value clone).
    #[cfg(test)]
    pub(crate) fn group_key_into(&self, e: &Expr, i: usize, enc: &mut Encoder) -> Result<()> {
        self.key_source(e)?.encode(self, i, enc)
    }

    /// Resolve where the values of group key `e` come from in this batch.
    fn key_source<'b>(&'b self, e: &'b Expr) -> Result<KeySrc<'b>> {
        let Expr::Column(c) = e else {
            return Ok(KeySrc::Eval(e));
        };
        Ok(match c {
            ColumnRef::Service
            | ColumnRef::SeverityText
            | ColumnRef::TraceId
            | ColumnRef::SpanId
            | ColumnRef::EventId => {
                let id = match c {
                    ColumnRef::Service => ColumnId::Service,
                    ColumnRef::SeverityText => ColumnId::SeverityText,
                    ColumnRef::TraceId => ColumnId::TraceId,
                    ColumnRef::SpanId => ColumnId::SpanId,
                    _ => ColumnId::EventId,
                };
                match self.col(id)? {
                    ColumnData::Strs(v) => KeySrc::Strs(v),
                    _ => return Err(Error::internal("expected string column")),
                }
            }
            ColumnRef::Attr(k) | ColumnRef::Resource(k) => {
                let (scope, id) = if matches!(c, ColumnRef::Attr(_)) {
                    (Scope::Attributes, ColumnId::Attributes)
                } else {
                    (Scope::Resource, ColumnId::ResourceAttributes)
                };
                let key = (scope, k.clone());
                if let Some(col) = self.shredded.get(&key) {
                    KeySrc::Values(col)
                } else if let Some(r) = self.raw.get(&key) {
                    KeySrc::Raw(r)
                } else {
                    match self.col(id)? {
                        ColumnData::Maps(v) => KeySrc::Map(v, k),
                        _ => return Err(Error::internal("expected map column")),
                    }
                }
            }
            ColumnRef::Body => match self.col(ColumnId::Body)? {
                ColumnData::Values(v) => KeySrc::Values(v),
                _ => return Err(Error::internal("expected value column")),
            },
            // Cheap scalars (no heap) and whole maps: evaluate normally.
            _ => KeySrc::Eval(e),
        })
    }

    /// Row identity `(seq, idx)`; requires the Seq and Idx columns.
    fn id(&self, i: usize) -> Result<(u64, u32)> {
        match (self.col(ColumnId::Seq)?, self.col(ColumnId::Idx)?) {
            (ColumnData::Ints(s), ColumnData::Ints(x)) => Ok((
                u64::try_from(s[i]).map_err(|_| Error::internal("negative seq"))?,
                u32::try_from(x[i]).map_err(|_| Error::internal("idx out of range"))?,
            )),
            _ => Err(Error::internal("expected int identity columns")),
        }
    }

    fn datum(&self, c: &ColumnRef, i: usize) -> Result<Datum> {
        let ints = |id| -> Result<i64> {
            match self.col(id)? {
                ColumnData::Ints(v) => Ok(v[i]),
                _ => Err(Error::internal("expected int column")),
            }
        };
        let strs = |id| -> Result<Datum> {
            match self.col(id)? {
                ColumnData::Strs(v) => Ok(v[i].clone().map(Value::String)),
                _ => Err(Error::internal("expected string column")),
            }
        };
        let maps = |id| -> Result<&BTreeMap<String, Value>> {
            match self.col(id)? {
                ColumnData::Maps(v) => Ok(&v[i]),
                _ => Err(Error::internal("expected map column")),
            }
        };
        let sev = || -> Result<u8> {
            match self.col(ColumnId::Severity)? {
                ColumnData::U8s(v) => Ok(v[i]),
                _ => Err(Error::internal("expected u8 column")),
            }
        };
        Ok(match c {
            ColumnRef::EventTime => Some(Value::Timestamp(ints(ColumnId::EventTime)?)),
            ColumnRef::IngestTime => Some(Value::Timestamp(ints(ColumnId::IngestTime)?)),
            ColumnRef::SchemaVersion => Some(Value::Int(ints(ColumnId::SchemaVersion)?)),
            ColumnRef::Service => strs(ColumnId::Service)?,
            ColumnRef::SeverityText => strs(ColumnId::SeverityText)?,
            ColumnRef::TraceId => strs(ColumnId::TraceId)?,
            ColumnRef::SpanId => strs(ColumnId::SpanId)?,
            ColumnRef::EventId => strs(ColumnId::EventId)?,
            ColumnRef::Severity => {
                let s = Severity::from_u8(sev()?).unwrap_or_default();
                Some(Value::String(s.as_str().to_owned()))
            }
            ColumnRef::SeverityNumber => Some(Value::Int(i64::from(sev()?))),
            ColumnRef::Body => match self.col(ColumnId::Body)? {
                ColumnData::Values(v) => v[i].clone(),
                _ => return Err(Error::internal("expected value column")),
            },
            ColumnRef::TenantId => Some(Value::String(self.tenant.as_str().to_owned())),
            ColumnRef::Attributes => Some(Value::Map(maps(ColumnId::Attributes)?.clone())),
            ColumnRef::ResourceAttributes => {
                Some(Value::Map(maps(ColumnId::ResourceAttributes)?.clone()))
            }
            ColumnRef::Attr(k) | ColumnRef::Resource(k) => {
                let (scope, id) = if matches!(c, ColumnRef::Attr(_)) {
                    (Scope::Attributes, ColumnId::Attributes)
                } else {
                    (Scope::Resource, ColumnId::ResourceAttributes)
                };
                let key = (scope, k.clone());
                match (self.shredded.get(&key), self.raw.get(&key)) {
                    (Some(col), _) => col[i].clone(),
                    (None, Some(r)) => r.value(i)?,
                    (None, None) => maps(id)?.get(k).cloned(),
                }
            }
        })
    }
}

fn truth_datum(t: Truth) -> Datum {
    Some(t.map_or(Value::Null, Value::Bool))
}

pub(crate) fn eval_expr(e: &Expr, b: &Batch<'_>, i: usize) -> Result<Datum> {
    Ok(match e {
        Expr::Column(c) => b.datum(c, i)?,
        Expr::Literal(v) => Some(v.clone()),
        Expr::TimeBucket { width_ns, expr } => match eval_expr(expr, b, i)? {
            Some(Value::Timestamp(t)) => Some(Value::Timestamp(t.div_euclid(*width_ns) * width_ns)),
            Some(_) => Some(Value::Null),
            None => None,
        },
        Expr::Agg { .. } => return Err(Error::internal("aggregate evaluated per row")),
        _ => truth_datum(eval_truth(e, b, i)?),
    })
}

pub(crate) fn eval_truth(e: &Expr, b: &Batch<'_>, i: usize) -> Result<Truth> {
    Ok(match e {
        Expr::Cmp(l, op, r) => {
            let ord = eval::compare(&eval_expr(l, b, i)?, &eval_expr(r, b, i)?);
            ord.map(|o| match op {
                CmpOp::Eq => o.is_eq(),
                CmpOp::Ne => o.is_ne(),
                CmpOp::Lt => o.is_lt(),
                CmpOp::Le => o.is_le(),
                CmpOp::Gt => o.is_gt(),
                CmpOp::Ge => o.is_ge(),
            })
        }
        Expr::And(l, r) => {
            let a = eval_truth(l, b, i)?;
            if a == Some(false) {
                Some(false)
            } else {
                eval::and(a, eval_truth(r, b, i)?)
            }
        }
        Expr::Or(l, r) => {
            let a = eval_truth(l, b, i)?;
            if a == Some(true) {
                Some(true)
            } else {
                eval::or(a, eval_truth(r, b, i)?)
            }
        }
        Expr::Not(x) => eval::not(eval_truth(x, b, i)?),
        Expr::IsNull { expr, negated } => {
            Some(matches!(eval_expr(expr, b, i)?, Some(Value::Null)) != *negated)
        }
        Expr::IsMissing { expr, negated } => Some(eval_expr(expr, b, i)?.is_none() != *negated),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let d = eval_expr(expr, b, i)?;
            let mut unknown = false;
            let mut found = false;
            for v in list {
                match eval::compare(&d, &Some(v.clone())) {
                    Some(o) if o.is_eq() => {
                        found = true;
                        break;
                    }
                    Some(_) => {}
                    None => unknown = true,
                }
            }
            let t = if found {
                Some(true)
            } else if unknown {
                None
            } else {
                Some(false)
            };
            if *negated {
                eval::not(t)
            } else {
                t
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => match eval_expr(expr, b, i)? {
            Some(Value::String(s)) => Some(eval::like(&s, pattern, *case_insensitive) != *negated),
            _ => None,
        },
        other => eval::truth_of(&eval_expr(other, b, i)?),
    })
}

// ---------------------------------------------------------------------------
// Aggregation

#[derive(Debug, Clone)]
enum AggState {
    Count(u64),
    Sum {
        int: i128,
        float: f64,
        saw_float: bool,
        any: bool,
    },
    Avg {
        sum: f64,
        n: u64,
    },
    Min(Datum),
    Max(Datum),
}

impl AggState {
    fn new(f: AggFunc) -> Self {
        match f {
            AggFunc::Count => AggState::Count(0),
            AggFunc::Sum => AggState::Sum {
                int: 0,
                float: 0.0,
                saw_float: false,
                any: false,
            },
            AggFunc::Avg => AggState::Avg { sum: 0.0, n: 0 },
            AggFunc::Min => AggState::Min(None),
            AggFunc::Max => AggState::Max(None),
        }
    }

    fn update(&mut self, star: bool, d: Datum) -> Result<()> {
        let present = !matches!(d, None | Some(Value::Null));
        match self {
            AggState::Count(n) => {
                if star || present {
                    *n += 1;
                }
            }
            _ if !present => {}
            AggState::Sum {
                int,
                float,
                saw_float,
                any,
            } => {
                *any = true;
                match d {
                    Some(Value::Int(v)) => *int += i128::from(v),
                    Some(Value::UInt(v)) => *int += i128::from(v),
                    Some(Value::Float(v)) => {
                        *float += v;
                        *saw_float = true;
                    }
                    Some(v) => return Err(type_err("sum", &v)),
                    None => {}
                }
            }
            AggState::Avg { sum, n } => {
                *sum += match d {
                    Some(Value::Int(v)) => v as f64,
                    Some(Value::UInt(v)) => v as f64,
                    Some(Value::Float(v)) => v,
                    Some(v) => return Err(type_err("avg", &v)),
                    None => 0.0,
                };
                *n += 1;
            }
            AggState::Min(cur) => min_max(cur, d, true)?,
            AggState::Max(cur) => min_max(cur, d, false)?,
        }
        Ok(())
    }

    /// Combine a partial state of the same function (parallel scans).
    fn merge(&mut self, other: &AggState) -> Result<()> {
        match (self, other) {
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (
                AggState::Sum {
                    int,
                    float,
                    saw_float,
                    any,
                },
                AggState::Sum {
                    int: i2,
                    float: f2,
                    saw_float: s2,
                    any: a2,
                },
            ) => {
                *int += i2;
                *float += f2;
                *saw_float |= s2;
                *any |= a2;
            }
            (AggState::Avg { sum, n }, AggState::Avg { sum: s2, n: n2 }) => {
                *sum += s2;
                *n += n2;
            }
            (AggState::Min(cur), AggState::Min(d)) => {
                if d.is_some() {
                    min_max(cur, d.clone(), true)?;
                }
            }
            (AggState::Max(cur), AggState::Max(d)) => {
                if d.is_some() {
                    min_max(cur, d.clone(), false)?;
                }
            }
            _ => return Err(Error::internal("merging different aggregate states")),
        }
        Ok(())
    }

    fn finish(&self) -> Datum {
        Some(match self {
            AggState::Count(n) => Value::Int(*n as i64),
            AggState::Sum { any: false, .. } => Value::Null,
            AggState::Sum {
                int,
                float,
                saw_float,
                ..
            } => {
                if *saw_float {
                    Value::Float(*int as f64 + float)
                } else if let Ok(v) = i64::try_from(*int) {
                    Value::Int(v)
                } else if let Ok(v) = u64::try_from(*int) {
                    Value::UInt(v)
                } else {
                    Value::Float(*int as f64)
                }
            }
            AggState::Avg { n: 0, .. } => Value::Null,
            AggState::Avg { sum, n } => Value::Float(sum / *n as f64),
            AggState::Min(d) | AggState::Max(d) => return d.clone().or(Some(Value::Null)),
        })
    }
}

fn min_max(cur: &mut Datum, d: Datum, is_min: bool) -> Result<()> {
    let replace = match cur {
        None => true,
        Some(_) => match eval::compare(cur, &d) {
            Some(o) => (is_min && o.is_gt()) || (!is_min && o.is_lt()),
            None => {
                return Err(Error::invalid_input(format!(
                    "{} over incomparable values ({} and {})",
                    if is_min { "min" } else { "max" },
                    cur.as_ref().map_or("missing", Value::type_name),
                    d.as_ref().map_or("missing", Value::type_name)
                )))
            }
        },
    };
    if replace {
        *cur = d;
    }
    Ok(())
}

fn type_err(f: &str, v: &Value) -> Error {
    Error::invalid_input(format!(
        "{f} requires numeric values, found {}",
        v.type_name()
    ))
}

fn datum_size(d: &Datum) -> usize {
    d.as_ref().map_or(8, Value::approx_size)
}

// ---------------------------------------------------------------------------
// Execution

struct Ctx<'a> {
    plan: &'a Plan,
    opts: &'a QueryOptions,
    deadline: Instant,
    stats: QueryStats,
    mem: usize,
    rows: Vec<Row>,
    groups: crate::groups::GroupTable<AggState>,
    page: Option<PageCtx>,
    /// Scratch buffer for group keys (reused across rows).
    key_buf: Vec<u8>,
    /// Compiled vectorized filter (`None`: no filter or vectorization off).
    vfilter: Option<crate::vector::Filter>,
    /// Rows to keep after sorting (top-k): the page size + 1 when paginating,
    /// else the query's LIMIT.
    keep: Option<usize>,
    /// Query-wide budget shared with parallel workers.
    budget: Arc<SharedBudget>,
    /// Rows and memory already added to `budget`.
    reported_rows: u64,
    reported_mem: i64,
}

/// Rows scanned and memory held across all workers of one query, so budgets
/// stay query-wide under parallel scans; `stop` makes workers give up after
/// another worker failed.
#[derive(Debug, Default)]
struct SharedBudget {
    rows: std::sync::atomic::AtomicU64,
    mem: std::sync::atomic::AtomicI64,
    stop: AtomicBool,
}

impl<'a> Ctx<'a> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        plan: &'a Plan,
        opts: &'a QueryOptions,
        deadline: Instant,
        page: Option<PageCtx>,
        keep: Option<usize>,
        vfilter: Option<crate::vector::Filter>,
        budget: Arc<SharedBudget>,
    ) -> Self {
        Self {
            plan,
            opts,
            deadline,
            stats: QueryStats::default(),
            mem: 0,
            rows: Vec::new(),
            groups: crate::groups::GroupTable::new(plan.aggregates.len()),
            page,
            keep,
            key_buf: Vec::new(),
            vfilter,
            budget,
            reported_rows: 0,
            reported_mem: 0,
        }
    }

    /// A fresh context for one parallel work item (same plan and budget).
    /// A context for one parallel work item. Its groups hash with this
    /// context's seed so [`crate::groups::GroupTable::merge_into`] can reuse
    /// the hashes.
    fn partial(&self) -> Self {
        let mut p = Self::new(
            self.plan,
            self.opts,
            self.deadline,
            self.page.clone(),
            self.keep,
            self.vfilter.clone(),
            Arc::clone(&self.budget),
        );
        p.groups = crate::groups::GroupTable::with_hasher(
            self.plan.aggregates.len(),
            self.groups.hasher(),
        );
        p
    }
}

impl Ctx<'_> {
    fn check_budget(&mut self) -> Result<()> {
        if self
            .opts
            .cancel
            .as_ref()
            .is_some_and(|c| c.load(AtomicOrdering::Relaxed))
        {
            return Err(Error::new(ErrorKind::Cancelled, "query cancelled"));
        }
        if self.budget.stop.load(AtomicOrdering::Relaxed) {
            return Err(Error::new(
                ErrorKind::Cancelled,
                "another scan worker failed",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(Error::new(
                ErrorKind::Timeout,
                format!("query exceeded its {:?} time limit", self.opts.timeout),
            ));
        }
        let b = &self.budget;
        let rows = b.rows.fetch_add(
            self.stats.rows_scanned - self.reported_rows,
            AtomicOrdering::Relaxed,
        ) + (self.stats.rows_scanned - self.reported_rows);
        self.reported_rows = self.stats.rows_scanned;
        let dm = self.mem as i64 - self.reported_mem;
        let mem = (b.mem.fetch_add(dm, AtomicOrdering::Relaxed) + dm).max(0) as usize;
        self.reported_mem = self.mem as i64;
        if rows > self.opts.max_rows_scanned {
            return Err(Error::resource_exhausted(format!(
                "query scanned more than {} rows; narrow the time range or add filters",
                self.opts.max_rows_scanned
            )));
        }
        self.stats.peak_memory_bytes = self.stats.peak_memory_bytes.max(mem);
        if mem > self.opts.max_memory_bytes {
            return Err(Error::resource_exhausted(format!(
                "query memory exceeded {} bytes",
                self.opts.max_memory_bytes
            )));
        }
        Ok(())
    }

    /// Rows are sorted before output (explicit ORDER BY or pagination).
    fn sorted(&self) -> bool {
        !self.plan.order.is_empty() || self.page.is_some()
    }

    fn limit_reached(&self) -> bool {
        !self.plan.aggregate && !self.sorted() && self.keep.is_some_and(|l| self.rows.len() >= l)
    }

    /// Sort and keep the first `keep` rows. With a bound smaller than the
    /// row count this selects the top `k` (O(n + k log k)) instead of sorting
    /// everything; ties are broken by the rows' current positions, so the
    /// result equals the stable sort followed by truncation exactly.
    fn sort_keep(&mut self) {
        sort_keep(&mut self.rows, &self.plan.order, self.keep);
    }

    /// Keep memory bounded for sorted queries with a row bound (top-k).
    fn maybe_compact(&mut self) {
        if let (true, Some(k)) = (self.sorted(), self.keep) {
            if self.rows.len() > k.saturating_mul(2).max(1024) {
                self.sort_keep();
                self.mem = self.rows.iter().map(row_size).sum();
            }
        }
    }

    fn process(&mut self, b: &Batch<'_>) -> Result<()> {
        let plan = self.plan;
        let key_srcs: Vec<KeySrc<'_>> = if plan.aggregate {
            plan.group_by
                .iter()
                .map(|g| b.key_source(g))
                .collect::<Result<_>>()?
        } else {
            Vec::new()
        };
        // Vectorized filter results for rows chunk_start..chunk_end.
        let (mut chunk_start, mut chunk_end) = (0usize, 0usize);
        let mut truth: Vec<Truth> = Vec::new();
        for i in 0..b.len {
            if i % 1024 == 0 {
                self.check_budget()?;
            }
            if self.limit_reached() {
                self.stats.early_stop = true;
                return Ok(());
            }
            self.stats.rows_scanned += 1;
            let id = match &self.page {
                Some(p) => {
                    let id = b.id(i)?;
                    if id.0 > p.as_of {
                        continue; // committed after the first page
                    }
                    id
                }
                None => (0, 0),
            };
            if let Some(f) = &self.plan.filter {
                let t = match &self.vfilter {
                    Some(vf) => {
                        if i >= chunk_end {
                            chunk_start = i;
                            chunk_end = (i + crate::vector::CHUNK).min(b.len);
                            truth = vf.eval(b, chunk_start, chunk_end)?;
                        }
                        truth[i - chunk_start]
                    }
                    None => eval_truth(f, b, i)?,
                };
                if t != Some(true) {
                    continue;
                }
            }
            self.stats.rows_matched += 1;
            if self.plan.aggregate {
                // Group key: per key a u32 length then its group_key_bytes,
                // built in a reused buffer straight from the batch.
                let mut enc = Encoder::from_vec(std::mem::take(&mut self.key_buf));
                for src in &key_srcs {
                    let at = enc.len();
                    enc.put_u32(0);
                    src.encode(b, i, &mut enc)?;
                    let n = (enc.len() - at - 4) as u32;
                    let mut buf = enc.into_inner();
                    buf[at..at + 4].copy_from_slice(&n.to_le_bytes());
                    enc = Encoder::from_vec(buf);
                }
                let mut kb = enc.into_inner();
                let h = self.groups.hash(&kb);
                let idx = match self.groups.find(h, &kb) {
                    Some(g) => g,
                    None => {
                        // New group: only its key bytes are stored; key
                        // values are decoded from them once, at output.
                        self.mem += kb.len() * 3 + 64 + 48 * self.plan.aggregates.len();
                        let states = self.plan.aggregates.iter().map(|(f, _)| AggState::new(*f));
                        self.groups.insert(h, &kb, states)
                    }
                };
                kb.clear();
                self.key_buf = kb;
                for (j, (_, arg)) in self.plan.aggregates.iter().enumerate() {
                    let d = match arg {
                        Some(a) => eval_expr(a, b, i)?,
                        None => None,
                    };
                    self.groups.states_mut(idx)[j].update(arg.is_none(), d)?;
                }
            } else {
                let out: Vec<Datum> = self
                    .plan
                    .outputs
                    .iter()
                    .map(|o| match o {
                        OutputSource::Row(e) => eval_expr(e, b, i),
                        _ => Err(Error::internal("non-row output in row query")),
                    })
                    .collect::<Result<_>>()?;
                let keys: Vec<Datum> = self
                    .plan
                    .order
                    .iter()
                    .map(|(s, _)| match s {
                        SortSource::Hidden(e) => eval_expr(e, b, i),
                        SortSource::Output(j) => Ok(out[*j].clone()),
                    })
                    .collect::<Result<_>>()?;
                if let Some((ak, aid)) = self.page.as_ref().and_then(|p| p.after.as_ref()) {
                    if cmp_position(&self.plan.order, (&keys, id), (ak, *aid)).is_le() {
                        continue; // at or before the cursor: returned already
                    }
                }
                let r = Row { out, keys, id };
                self.mem += row_size(&r);
                self.rows.push(r);
                self.maybe_compact();
            }
        }
        Ok(())
    }
}

fn row_size(r: &Row) -> usize {
    64 + r
        .out
        .iter()
        .chain(r.keys.iter())
        .map(datum_size)
        .sum::<usize>()
}

/// Why a segment was pruned, if it was.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Pruned {
    Time,
    Keys,
    Severity,
    Bloom(usize),
    Ngram(usize),
}

fn prune(seg: &SegmentHandle, p: &Pruning) -> Option<Pruned> {
    let e = &seg.entry;
    if p.time_lo.is_some_and(|lo| e.max_event_time < lo)
        || p.time_hi.is_some_and(|hi| e.min_event_time > hi)
    {
        return Some(Pruned::Time);
    }
    let f = seg.reader.footer();
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
    if let Some(bs) = seg.bloom() {
        for (i, (field, vals)) in p.bloom.iter().enumerate() {
            if let Some(b) = bs.get(field) {
                if !vals.iter().any(|v| b.may_contain(v)) {
                    return Some(Pruned::Bloom(i));
                }
            }
        }
    }
    if !p.ngram.is_empty() {
        if let Some(ns) = seg.ngram() {
            for (i, (field, probes)) in p.ngram.iter().enumerate() {
                if !ns.may_match(field, probes) {
                    return Some(Pruned::Ngram(i));
                }
            }
        }
    }
    None
}

/// True when the first `ORDER BY` key is the plain `event_time` column.
fn first_key_is_event_time(plan: &Plan) -> bool {
    let is_et = |e: &Expr| matches!(e, Expr::Column(ColumnRef::EventTime));
    match plan.order.first() {
        Some((SortSource::Hidden(e), _)) => is_et(e),
        Some((SortSource::Output(i), _)) => {
            matches!(plan.outputs.get(*i), Some(OutputSource::Row(e)) if is_et(e))
        }
        None => false,
    }
}

/// Whether the pagination state excludes every row of `seg`. Sound because a
/// row is returned only if `seq <= as_of` and its position sorts strictly
/// after the cursor, which requires (ingest order) `seq >= cursor seq`, or
/// (first key `event_time`) `event_time` on the far side of the cursor's.
fn cursor_prunes(seg: &SegmentHandle, plan: &Plan, p: &PageCtx) -> bool {
    let e = &seg.entry;
    if e.min_seq > p.as_of {
        return true;
    }
    let Some((keys, (seq, _))) = &p.after else {
        return false;
    };
    if plan.order.is_empty() {
        return e.max_seq < *seq;
    }
    if let (true, Some(Some(Value::Timestamp(t)))) = (first_key_is_event_time(plan), keys.first()) {
        let desc = plan.order[0].1;
        return if desc {
            e.min_event_time > *t
        } else {
            e.max_event_time < *t
        };
    }
    false
}

/// Read the columns a plan needs from one segment (shredded attribute
/// columns when they cover every referenced key of a scope).
fn load_batch<'t>(
    seg: &SegmentHandle,
    plan: &Plan,
    needed: &[ColumnId],
    tenant: &'t TenantId,
    stats: &mut QueryStats,
    projection: bool,
) -> Result<Batch<'t>> {
    let mut cols = HashMap::new();
    let mut shredded = HashMap::new();
    let mut raw = HashMap::new();
    let footer = seg.reader.footer();
    // When a scope is referenced only through specific keys, read just those
    // keys (shredded chunks, or a projection of the residual map chunk that
    // skips every other value) instead of materializing whole maps.
    // With projection disabled, only scopes whose referenced keys are all
    // shredded in this segment are read per key (the pre-projection rule).
    let project = |scope: Scope, keys: &std::collections::BTreeSet<String>, whole: bool| {
        !whole
            && !keys.is_empty()
            && (projection || keys.iter().all(|k| footer.shredded(scope, k).is_some()))
    };
    let proj_attr = project(Scope::Attributes, &plan.attr_keys, plan.whole_attributes);
    let proj_res = project(Scope::Resource, &plan.resource_keys, plan.whole_resource);
    for id in needed {
        let skip = (*id == ColumnId::Attributes && proj_attr)
            || (*id == ColumnId::ResourceAttributes && proj_res);
        if skip {
            continue;
        }
        cols.insert(*id, seg.reader.read_column(*id)?);
        stats.bytes_read += footer.column(*id).map_or(0, |c| c.len);
        // Full maps also read their shredded chunks.
        let scope = match id {
            ColumnId::Attributes => Some(Scope::Attributes),
            ColumnId::ResourceAttributes => Some(Scope::Resource),
            _ => None,
        };
        if let Some(scope) = scope {
            stats.bytes_read += footer
                .shredded
                .iter()
                .filter(|c| c.scope == scope)
                .map(|c| c.len)
                .sum::<u64>();
        }
    }
    for (scope, keys, on) in [
        (Scope::Attributes, &plan.attr_keys, proj_attr),
        (Scope::Resource, &plan.resource_keys, proj_res),
    ] {
        if !on {
            continue;
        }
        if keys.iter().all(|k| footer.shredded(scope, k).is_some()) {
            stats.shredded_reads += 1;
        }
        // Shredded keys needed only as group keys stay encoded.
        let raw_keys = match scope {
            Scope::Attributes => &plan.raw_group_attr_keys,
            Scope::Resource => &plan.raw_group_resource_keys,
        };
        let mut decode: Vec<String> = Vec::with_capacity(keys.len());
        for k in keys {
            match footer.shredded(scope, k) {
                Some(c) if raw_keys.contains(k) => {
                    if let Some(r) = seg.reader.read_shredded_raw(scope, k)? {
                        stats.bytes_read += c.len;
                        raw.insert((scope, k.clone()), r);
                        continue;
                    }
                    decode.push(k.clone());
                }
                _ => decode.push(k.clone()),
            }
        }
        let keys = decode;
        if keys.is_empty() {
            continue;
        }
        let (vals, bytes) = seg.reader.read_map_keys(scope, &keys)?;
        stats.bytes_read += bytes;
        for (k, col) in keys.into_iter().zip(vals) {
            shredded.insert((scope, k), col);
        }
    }
    Ok(Batch {
        len: footer.row_count as usize,
        tenant,
        cols,
        shredded,
        raw,
    })
}

/// Result of scanning one segment in a parallel work item.
struct Partial {
    stats: QueryStats,
    rows: Vec<Row>,
    /// Groups in first-seen order: (key bytes, key values, states).
    groups: crate::groups::GroupTable<AggState>,
    mem: usize,
}

/// Scan `segs` with `workers` threads, one partial result per segment, then
/// merge the partials into `ctx` **in segment order** — so result rows (after
/// a stable sort) and group order are exactly those of a sequential scan.
/// The first failing segment (lowest index) determines the error.
fn scan_parallel(
    ctx: &mut Ctx<'_>,
    segs: &[&Arc<SegmentHandle>],
    needed: &[ColumnId],
    tenant: &TenantId,
    workers: usize,
) -> Result<()> {
    let n = segs.len();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let slots: Vec<std::sync::Mutex<Option<Result<Partial>>>> =
        (0..n).map(|_| std::sync::Mutex::new(None)).collect();
    let template = &*ctx;
    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| loop {
                let i = next.fetch_add(1, AtomicOrdering::Relaxed);
                if i >= n || template.budget.stop.load(AtomicOrdering::Relaxed) {
                    break;
                }
                let mut p = template.partial();
                let r = (|| -> Result<Partial> {
                    p.check_budget()?;
                    p.stats.segments_scanned += 1;
                    let batch = load_batch(
                        segs[i],
                        template.plan,
                        needed,
                        tenant,
                        &mut p.stats,
                        template.opts.enable_map_projection,
                    )?;
                    p.process(&batch)?;
                    p.check_budget()?;
                    let empty = crate::groups::GroupTable::new(0);
                    Ok(Partial {
                        stats: std::mem::take(&mut p.stats),
                        rows: std::mem::take(&mut p.rows),
                        groups: std::mem::replace(&mut p.groups, empty),
                        mem: p.mem,
                    })
                })();
                if r.is_err() {
                    template.budget.stop.store(true, AtomicOrdering::Relaxed);
                }
                *slots[i].lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
            });
        }
    });
    let results: Vec<Option<Result<Partial>>> = slots
        .into_iter()
        .map(|m| m.into_inner().unwrap_or_else(|e| e.into_inner()))
        .collect();
    // Report the first real error in segment order; a "worker failed"
    // cancellation is only a symptom of another worker's error.
    let user_cancel = ctx
        .opts
        .cancel
        .as_ref()
        .is_some_and(|c| c.load(AtomicOrdering::Relaxed));
    let err_at = |real_only: bool| {
        results.iter().position(|r| match r {
            Some(Err(e)) => !real_only || e.kind() != ErrorKind::Cancelled || user_cancel,
            _ => false,
        })
    };
    if let Some(i) = err_at(true).or_else(|| err_at(false)) {
        let mut results = results;
        if let Some(Err(e)) = results.swap_remove(i) {
            return Err(e);
        }
        return Err(Error::internal("parallel scan error vanished"));
    }
    for p in results.into_iter().flatten() {
        let p = p?;
        let s = &mut ctx.stats;
        s.segments_scanned += p.stats.segments_scanned;
        s.rows_scanned += p.stats.rows_scanned;
        s.rows_matched += p.stats.rows_matched;
        s.bytes_read += p.stats.bytes_read;
        s.shredded_reads += p.stats.shredded_reads;
        s.peak_memory_bytes = s.peak_memory_bytes.max(p.stats.peak_memory_bytes);
        ctx.mem += p.mem;
        ctx.rows.extend(p.rows);
        ctx.maybe_compact();
        // Partial groups in their first-seen order → global first-seen order.
        p.groups.merge_into(&mut ctx.groups, |a, b| a.merge(&b))?;
    }
    // The merged context now owns the workers' rows/groups: rebase the
    // shared accounting on it before the memtable is processed.
    ctx.reported_rows = ctx.stats.rows_scanned;
    ctx.budget
        .rows
        .store(ctx.stats.rows_scanned, AtomicOrdering::Relaxed);
    ctx.budget
        .mem
        .store(ctx.mem as i64, AtomicOrdering::Relaxed);
    ctx.reported_mem = ctx.mem as i64;
    ctx.check_budget()
}

fn run(
    snapshot: &Snapshot,
    plan: &Plan,
    opts: &QueryOptions,
    page: Option<PageCtx>,
) -> Result<RunResult> {
    let started = Instant::now();
    let keep = match &page {
        Some(p) => Some(p.k),
        None => plan.limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
    };
    let mut ctx = Ctx::new(
        plan,
        opts,
        started + opts.timeout,
        page,
        keep,
        plan.filter
            .as_ref()
            .filter(|_| opts.enable_vectorized)
            .map(crate::vector::Filter::new),
        Arc::new(SharedBudget::default()),
    );
    let tenant = snapshot.tenant();
    let mut to_scan = Vec::new();
    for seg in snapshot.segments() {
        ctx.stats.segments_total += 1;
        if &seg.entry.tenant != tenant || &seg.reader.footer().tenant_id != tenant {
            return Err(Error::internal(
                "snapshot contains another tenant's segment",
            ));
        }
        if ctx
            .page
            .as_ref()
            .is_some_and(|p| cursor_prunes(seg, plan, p))
        {
            ctx.stats.segments_pruned_cursor += 1;
            continue;
        }
        match opts
            .enable_pruning
            .then(|| prune(seg, &plan.pruning))
            .flatten()
        {
            Some(Pruned::Time) => ctx.stats.segments_pruned_time += 1,
            Some(Pruned::Keys) => ctx.stats.segments_pruned_keys += 1,
            Some(Pruned::Severity) => ctx.stats.segments_pruned_severity += 1,
            Some(Pruned::Bloom(i)) => {
                ctx.stats.segments_pruned_bloom += 1;
                let name = plan.pruning.bloom[i].0.describe();
                if !ctx.stats.indexes_used.contains(&name) {
                    ctx.stats.indexes_used.push(name);
                }
            }
            Some(Pruned::Ngram(i)) => {
                ctx.stats.segments_pruned_ngram += 1;
                let name = plan.pruning.ngram[i].0.describe();
                if !ctx.stats.indexes_used.contains(&name) {
                    ctx.stats.indexes_used.push(name);
                }
            }
            None => to_scan.push(seg),
        }
    }
    if plan.explain == ExplainMode::Plan {
        ctx.stats.elapsed = started.elapsed();
        let text = explain_text(plan, snapshot, &ctx.stats, to_scan.len(), false);
        return Ok(RunResult {
            output: QueryOutput {
                columns: vec!["plan".into()],
                rows: Vec::new(),
                stats: ctx.stats,
                explain: Some(text),
            },
            rows: Vec::new(),
        });
    }
    let mut needed: Vec<ColumnId> = plan.columns.iter().copied().collect();
    if ctx.page.is_some() {
        for id in [ColumnId::Seq, ColumnId::Idx] {
            if !needed.contains(&id) {
                needed.push(id);
            }
        }
    }
    // Parallel scans only where every row must be seen anyway (sorted or
    // aggregate queries); unsorted LIMIT queries keep sequential early stop.
    let workers = opts.parallelism.min(to_scan.len());
    if workers > 1 && (plan.aggregate || ctx.sorted()) {
        scan_parallel(&mut ctx, &to_scan, &needed, tenant, workers)?;
    } else {
        for seg in to_scan {
            if ctx.limit_reached() {
                ctx.stats.early_stop = true;
                break;
            }
            ctx.check_budget()?;
            ctx.stats.segments_scanned += 1;
            let batch = load_batch(
                seg,
                plan,
                &needed,
                tenant,
                &mut ctx.stats,
                opts.enable_map_projection,
            )?;
            ctx.process(&batch)?;
        }
    }
    for chunk in snapshot.memory_rows().chunks(4096) {
        if ctx.limit_reached() {
            ctx.stats.early_stop = true;
            break;
        }
        ctx.check_budget()?;
        let evs: Vec<&Event> = chunk.iter().map(|r| r.event.as_ref()).collect();
        if evs.iter().any(|e| &e.tenant_id != tenant) {
            return Err(Error::internal("snapshot contains another tenant's row"));
        }
        let cols = needed
            .iter()
            .map(|id| (*id, memory_column(*id, chunk, &evs)))
            .collect();
        ctx.stats.memory_rows_scanned += evs.len() as u64;
        ctx.process(&Batch {
            len: evs.len(),
            tenant,
            cols,
            shredded: HashMap::new(),
            raw: HashMap::new(),
        })?;
    }
    ctx.check_budget()?;

    if plan.aggregate {
        if ctx.groups.is_empty() && plan.group_by.is_empty() {
            let states = plan.aggregates.iter().map(|(f, _)| AggState::new(*f));
            let h = ctx.groups.hash(&[]);
            ctx.groups.insert(h, &[], states);
        }
        let groups = std::mem::replace(&mut ctx.groups, crate::groups::GroupTable::new(0));
        let sort_keys = |out: &[Datum]| -> Vec<Datum> {
            plan.order
                .iter()
                .map(|(s, _)| match s {
                    SortSource::Output(j) => out[*j].clone(),
                    // The planner rejects hidden sort keys in aggregate queries.
                    SortSource::Hidden(_) => None,
                })
                .collect()
        };
        let output = |g: usize, keys: Option<&[Datum]>| -> Vec<Datum> {
            let states = groups.group(g).1;
            plan.outputs
                .iter()
                .map(|o| match o {
                    OutputSource::Group(i) => keys.and_then(|k| k[*i].clone()),
                    OutputSource::Agg(i) => states[*i].finish(),
                    OutputSource::Row(_) => None,
                })
                .collect()
        };
        // Top-k by aggregate values only (e.g. `ORDER BY count(*) DESC LIMIT
        // 10`): rank groups on their states, then decode keys and build rows
        // only for the k selected — not for every group.
        let by_aggs_only = plan.order.iter().all(|(s, _)| match s {
            SortSource::Output(j) => !matches!(plan.outputs[*j], OutputSource::Group(_)),
            SortSource::Hidden(_) => true,
        });
        let selected: Vec<usize> = match ctx.keep {
            Some(k) if ctx.sorted() && by_aggs_only && k < groups.len() => {
                let ranked: Vec<(usize, Vec<Datum>)> = (0..groups.len())
                    .map(|g| (g, sort_keys(&output(g, None))))
                    .collect();
                let order = &plan.order;
                top_k(ranked, k, |a, b| {
                    cmp_position(order, (&a.1, (0, 0)), (&b.1, (0, 0)))
                })
                .map(|(g, _)| g)
                .collect()
            }
            _ => (0..groups.len()).collect(),
        };
        for g in selected {
            let keys = crate::groups::decode_keys(groups.group(g).0, plan.group_by.len())?;
            let out = output(g, Some(&keys));
            let keys = sort_keys(&out);
            ctx.rows.push(Row {
                out,
                keys,
                id: (0, 0),
            });
        }
    }
    if ctx.sorted() {
        ctx.sort_keep();
    }
    if let Some(k) = ctx.keep {
        ctx.rows.truncate(k);
    }
    ctx.stats.peak_memory_bytes = ctx.stats.peak_memory_bytes.max(ctx.mem);
    ctx.stats.elapsed = started.elapsed();
    if plan.explain == ExplainMode::Analyze {
        let text = explain_text(plan, snapshot, &ctx.stats, ctx.stats.segments_scanned, true);
        return Ok(RunResult {
            output: QueryOutput {
                columns: vec!["plan".into()],
                rows: Vec::new(),
                stats: ctx.stats,
                explain: Some(text),
            },
            rows: Vec::new(),
        });
    }
    Ok(RunResult {
        output: QueryOutput {
            columns: plan.output_names.clone(),
            rows: Vec::new(),
            stats: ctx.stats,
            explain: None,
        },
        rows: ctx.rows,
    })
}

fn explain_text(
    plan: &Plan,
    snap: &Snapshot,
    s: &QueryStats,
    to_scan: usize,
    analyzed: bool,
) -> String {
    let mut t = String::new();
    let fmt_t = |v: Option<i64>| {
        v.map_or(
            "unbounded".to_owned(),
            stratadb_common::time::format_rfc3339,
        )
    };
    if let Some(l) = plan.limit {
        let _ = writeln!(t, "Limit: {l}");
    }
    if !plan.order.is_empty() {
        let keys: Vec<String> = plan
            .order
            .iter()
            .map(|(src, desc)| {
                let k = match src {
                    SortSource::Output(i) => plan.output_names[*i].clone(),
                    SortSource::Hidden(e) => e.to_string(),
                };
                format!("{k} {}", if *desc { "DESC" } else { "ASC" })
            })
            .collect();
        let _ = writeln!(
            t,
            "Sort: {}{}",
            keys.join(", "),
            if plan.limit.is_some() { " (top-k)" } else { "" }
        );
    }
    if plan.aggregate {
        let g: Vec<String> = plan.group_by.iter().map(ToString::to_string).collect();
        let a: Vec<String> = plan
            .aggregates
            .iter()
            .map(|(f, e)| {
                Expr::Agg {
                    func: *f,
                    arg: e.clone().map(Box::new),
                }
                .to_string()
            })
            .collect();
        let _ = writeln!(
            t,
            "Aggregate: group by [{}] compute [{}]",
            g.join(", "),
            a.join(", ")
        );
    }
    let _ = writeln!(t, "Project: {}", plan.output_names.join(", "));
    if let Some(f) = &plan.filter {
        let _ = writeln!(t, "Filter: {f}");
    }
    let cols: Vec<&str> = plan.columns.iter().map(|c| c.name()).collect();
    let _ = writeln!(t, "Scan: logs (tenant scoped by authentication)");
    let _ = writeln!(t, "  columns read: [{}]", cols.join(", "));
    let p = &plan.pruning;
    let _ = writeln!(
        t,
        "  time range: [{}, {}]",
        fmt_t(p.time_lo),
        fmt_t(p.time_hi)
    );
    if !p.required_attrs.is_empty() || !p.required_resource.is_empty() {
        let _ = writeln!(
            t,
            "  required keys: attributes {:?}, resource {:?}",
            p.required_attrs, p.required_resource
        );
    }
    if let Some(sv) = &p.severities {
        let names: Vec<&str> = sv
            .iter()
            .map(|c| Severity::from_u8(*c).map_or("<none>", Severity::as_str))
            .collect();
        let _ = writeln!(t, "  severity in: {names:?}");
    }
    let _ = writeln!(
        t,
        "  segments: {} total, pruned {} by time, {} by keys, {} by severity, {} by bloom, {} by ngram, {} to scan",
        s.segments_total,
        s.segments_pruned_time,
        s.segments_pruned_keys,
        s.segments_pruned_severity,
        s.segments_pruned_bloom,
        s.segments_pruned_ngram,
        to_scan
    );
    let _ = writeln!(t, "  memtable rows: {}", snap.memory_rows().len());
    if !p.bloom.is_empty() || !p.ngram.is_empty() {
        let c: Vec<String> = p
            .bloom
            .iter()
            .map(|(f, v)| format!("{} ({} value(s))", f.describe(), v.len()))
            .collect();
        let c: Vec<String> = c
            .into_iter()
            .chain(
                p.ngram
                    .iter()
                    .map(|(f, g)| format!("{} ({} trigram(s))", f.describe(), g.len())),
            )
            .collect();
        let _ = writeln!(t, "  index candidates: [{}]", c.join(", "));
    }
    let _ = writeln!(
        t,
        "  indexes used: {}",
        if s.indexes_used.is_empty() {
            "none".to_owned()
        } else {
            s.indexes_used.join(", ")
        }
    );
    if analyzed {
        let _ = writeln!(
            t,
            "Actual: rows scanned {}, matched {}, segment bytes read {}, peak memory ~{} B, early stop {}, elapsed {:?}",
            s.rows_scanned, s.rows_matched, s.bytes_read, s.peak_memory_bytes, s.early_stop, s.elapsed
        );
    }
    t
}

#[cfg(test)]
mod parallel_budget_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use stratadb_storage::{Durability, Engine, EngineOptions};

    /// Workers must stop soon after the query-wide row budget is exceeded,
    /// not scan every segment and fail at the end.
    #[test]
    fn row_budget_stops_parallel_workers_early() {
        let dir = tempfile::tempdir().unwrap();
        let t = TenantId::new("b").unwrap();
        let mut o = EngineOptions::new(dir.path());
        o.background = false;
        o.wal.durability = Durability::Async;
        let e = Engine::open(o).unwrap();
        for s in 0..40i64 {
            let evs: Vec<Event> = (0..150)
                .map(|j| Event::new(t.clone(), 1_795_170_600_000_000_000 + s * 150 + j, 1))
                .collect();
            e.write_batch(&t, evs).unwrap();
            e.flush().unwrap();
        }
        let snap = e.snapshot(&t);
        let opts = QueryOptions {
            max_rows_scanned: 1000,
            parallelism: 4,
            ..QueryOptions::default()
        };
        let plan = Plan::new(
            crate::parser::parse("SELECT count(*) FROM logs", &opts.parse_limits).unwrap(),
        )
        .unwrap();
        let budget = Arc::new(SharedBudget::default());
        let mut ctx = Ctx::new(
            &plan,
            &opts,
            Instant::now() + opts.timeout,
            None,
            None,
            None,
            Arc::clone(&budget),
        );
        let segs: Vec<&Arc<SegmentHandle>> = snap.segments().iter().collect();
        assert_eq!(segs.len(), 40);
        let needed: Vec<ColumnId> = plan.columns.iter().copied().collect();
        let err = scan_parallel(&mut ctx, &segs, &needed, &t, 4).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ResourceExhausted);
        let scanned = budget.rows.load(AtomicOrdering::Relaxed);
        // Budget + at most one in-flight segment per worker (plus slack).
        assert!(
            scanned <= 1000 + 4 * 150 + 150,
            "scanned {scanned} of 6000 rows"
        );
    }
}

#[cfg(test)]
mod sort_keep_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Top-k selection equals a stable full sort followed by truncation,
        /// including the order of ties (small key domains force many ties)
        /// and mixed ASC/DESC keys with missing values.
        #[test]
        fn top_k_equals_stable_sort_then_truncate(
            rows in proptest::collection::vec(
                (proptest::collection::vec(proptest::option::of(0i64..4), 2), 0u64..3),
                0..80),
            desc in proptest::collection::vec(any::<bool>(), 2),
            keep in proptest::option::of(0usize..90),
        ) {
            let order: Vec<(SortSource, bool)> =
                desc.iter().enumerate().map(|(j, d)| (SortSource::Output(j), *d)).collect();
            let make = || -> Vec<Row> {
                rows.iter()
                    .enumerate()
                    .map(|(n, (k, id))| Row {
                        out: vec![Some(Value::UInt(n as u64))],
                        keys: k.iter().map(|x| x.map(Value::Int)).collect(),
                        id: (*id, 0),
                    })
                    .collect()
            };
            let mut want = make();
            want.sort_by(|a, b| cmp_position(&order, (&a.keys, a.id), (&b.keys, b.id)));
            if let Some(k) = keep {
                want.truncate(k);
            }
            let mut got = make();
            sort_keep(&mut got, &order, keep);
            let ids = |v: &[Row]| v.iter().map(|r| r.out.clone()).collect::<Vec<_>>();
            prop_assert_eq!(ids(&got), ids(&want));
        }
    }
}
