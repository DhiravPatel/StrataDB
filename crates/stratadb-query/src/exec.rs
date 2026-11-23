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

use stratadb_common::{Error, ErrorKind, Result, TenantId};
use stratadb_schema::{Event, Severity, Value};
use stratadb_segment::{ColumnData, ColumnId, Scope};
use stratadb_storage::{SegmentHandle, Snapshot};

use crate::ast::{AggFunc, CmpOp, ColumnRef, ExplainMode, Expr};
use crate::eval::{self, Datum, Truth};
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
    pub parse_limits: crate::parser::ParseLimits,
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
            parse_limits: Default::default(),
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
    pub elapsed: Duration,
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
    let stmt = crate::parser::parse(sql, &opts.parse_limits)?;
    let plan = Plan::new(stmt)?;
    run(snapshot, &plan, opts)
}

// ---------------------------------------------------------------------------
// Batches

struct Batch<'a> {
    len: usize,
    tenant: &'a TenantId,
    cols: HashMap<ColumnId, ColumnData>,
    /// Shredded attribute columns read instead of whole maps (RFC-0002).
    shredded: HashMap<(Scope, String), Vec<Datum>>,
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

impl Batch<'_> {
    fn col(&self, id: ColumnId) -> Result<&ColumnData> {
        self.cols
            .get(&id)
            .ok_or_else(|| Error::internal(format!("column {id:?} not loaded")))
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
            ColumnRef::Attr(k) => match self.shredded.get(&(Scope::Attributes, k.clone())) {
                Some(col) => col[i].clone(),
                None => maps(ColumnId::Attributes)?.get(k).cloned(),
            },
            ColumnRef::Resource(k) => match self.shredded.get(&(Scope::Resource, k.clone())) {
                Some(col) => col[i].clone(),
                None => maps(ColumnId::ResourceAttributes)?.get(k).cloned(),
            },
        })
    }
}

fn truth_datum(t: Truth) -> Datum {
    Some(t.map_or(Value::Null, Value::Bool))
}

fn eval_expr(e: &Expr, b: &Batch<'_>, i: usize) -> Result<Datum> {
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

fn eval_truth(e: &Expr, b: &Batch<'_>, i: usize) -> Result<Truth> {
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
    rows: Vec<(Vec<Datum>, Vec<Datum>)>,
    groups: HashMap<Vec<u8>, usize>,
    group_vals: Vec<(Vec<Datum>, Vec<AggState>)>,
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
        if Instant::now() >= self.deadline {
            return Err(Error::new(
                ErrorKind::Timeout,
                format!("query exceeded its {:?} time limit", self.opts.timeout),
            ));
        }
        if self.stats.rows_scanned > self.opts.max_rows_scanned {
            return Err(Error::resource_exhausted(format!(
                "query scanned more than {} rows; narrow the time range or add filters",
                self.opts.max_rows_scanned
            )));
        }
        self.stats.peak_memory_bytes = self.stats.peak_memory_bytes.max(self.mem);
        if self.mem > self.opts.max_memory_bytes {
            return Err(Error::resource_exhausted(format!(
                "query memory exceeded {} bytes",
                self.opts.max_memory_bytes
            )));
        }
        Ok(())
    }

    fn limit_reached(&self) -> bool {
        !self.plan.aggregate
            && self.plan.order.is_empty()
            && self.plan.limit.is_some_and(|l| self.rows.len() as u64 >= l)
    }

    fn sort_rows(&mut self) {
        let order = &self.plan.order;
        self.rows.sort_by(|(oa, ha), (ob, hb)| {
            let mut hidden = 0;
            for (src, desc) in order {
                let (x, y) = match src {
                    SortSource::Output(i) => (&oa[*i], &ob[*i]),
                    SortSource::Hidden(_) => {
                        hidden += 1;
                        (&ha[hidden - 1], &hb[hidden - 1])
                    }
                };
                let o = eval::sort_cmp(x, y);
                let o = if *desc { o.reverse() } else { o };
                if o.is_ne() {
                    return o;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    /// Keep memory bounded for ORDER BY ... LIMIT k (top-k).
    fn maybe_compact(&mut self) {
        if let (false, Some(k)) = (self.plan.order.is_empty(), self.plan.limit) {
            let k = usize::try_from(k).unwrap_or(usize::MAX);
            if self.rows.len() > k.saturating_mul(2).max(1024) {
                self.sort_rows();
                self.rows.truncate(k);
                self.mem = self.rows.iter().map(row_size).sum();
            }
        }
    }

    fn process(&mut self, b: &Batch<'_>) -> Result<()> {
        for i in 0..b.len {
            if i % 1024 == 0 {
                self.check_budget()?;
            }
            if self.limit_reached() {
                self.stats.early_stop = true;
                return Ok(());
            }
            self.stats.rows_scanned += 1;
            if let Some(f) = &self.plan.filter {
                if eval_truth(f, b, i)? != Some(true) {
                    continue;
                }
            }
            self.stats.rows_matched += 1;
            if self.plan.aggregate {
                let keys: Vec<Datum> = self
                    .plan
                    .group_by
                    .iter()
                    .map(|g| eval_expr(g, b, i))
                    .collect::<Result<_>>()?;
                let mut kb = Vec::new();
                for k in &keys {
                    let enc = eval::group_key_bytes(k);
                    kb.extend_from_slice(&(enc.len() as u32).to_le_bytes());
                    kb.extend_from_slice(&enc);
                }
                let idx = match self.groups.get(&kb) {
                    Some(i) => *i,
                    None => {
                        self.mem += kb.len() * 2
                            + 64
                            + keys.iter().map(datum_size).sum::<usize>()
                            + 48 * self.plan.aggregates.len();
                        let states = self
                            .plan
                            .aggregates
                            .iter()
                            .map(|(f, _)| AggState::new(*f))
                            .collect();
                        self.group_vals.push((keys, states));
                        self.groups.insert(kb, self.group_vals.len() - 1);
                        self.group_vals.len() - 1
                    }
                };
                for (j, (_, arg)) in self.plan.aggregates.iter().enumerate() {
                    let d = match arg {
                        Some(a) => eval_expr(a, b, i)?,
                        None => None,
                    };
                    self.group_vals[idx].1[j].update(arg.is_none(), d)?;
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
                let hidden: Vec<Datum> = self
                    .plan
                    .order
                    .iter()
                    .filter_map(|(s, _)| match s {
                        SortSource::Hidden(e) => Some(eval_expr(e, b, i)),
                        SortSource::Output(_) => None,
                    })
                    .collect::<Result<_>>()?;
                let r = (out, hidden);
                self.mem += row_size(&r);
                self.rows.push(r);
                self.maybe_compact();
            }
        }
        Ok(())
    }
}

fn row_size(r: &(Vec<Datum>, Vec<Datum>)) -> usize {
    48 + r.0.iter().chain(r.1.iter()).map(datum_size).sum::<usize>()
}

/// Why a segment was pruned, if it was.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Pruned {
    Time,
    Keys,
    Severity,
    Bloom(usize),
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
    None
}

fn run(snapshot: &Snapshot, plan: &Plan, opts: &QueryOptions) -> Result<QueryOutput> {
    let started = Instant::now();
    let mut ctx = Ctx {
        plan,
        opts,
        deadline: started + opts.timeout,
        stats: QueryStats::default(),
        mem: 0,
        rows: Vec::new(),
        groups: HashMap::new(),
        group_vals: Vec::new(),
    };
    let tenant = snapshot.tenant();
    let mut to_scan = Vec::new();
    for seg in snapshot.segments() {
        ctx.stats.segments_total += 1;
        if &seg.entry.tenant != tenant || &seg.reader.footer().tenant_id != tenant {
            return Err(Error::internal(
                "snapshot contains another tenant's segment",
            ));
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
            None => to_scan.push(seg),
        }
    }
    if plan.explain == ExplainMode::Plan {
        ctx.stats.elapsed = started.elapsed();
        let text = explain_text(plan, snapshot, &ctx.stats, to_scan.len(), false);
        return Ok(QueryOutput {
            columns: vec!["plan".into()],
            rows: Vec::new(),
            stats: ctx.stats,
            explain: Some(text),
        });
    }
    let needed: Vec<ColumnId> = plan.columns.iter().copied().collect();
    for seg in to_scan {
        if ctx.limit_reached() {
            ctx.stats.early_stop = true;
            break;
        }
        ctx.check_budget()?;
        ctx.stats.segments_scanned += 1;
        let mut cols = HashMap::new();
        let mut shredded = HashMap::new();
        let footer = seg.reader.footer();
        // Use shredded columns when every referenced key of a scope is shredded
        // in this segment and the whole map is not needed; else read full maps.
        let use_shredded = |scope: Scope,
                            keys: &std::collections::BTreeSet<String>,
                            whole: bool| {
            !whole && !keys.is_empty() && keys.iter().all(|k| footer.shredded(scope, k).is_some())
        };
        let shred_attr = use_shredded(Scope::Attributes, &plan.attr_keys, plan.whole_attributes);
        let shred_res = use_shredded(Scope::Resource, &plan.resource_keys, plan.whole_resource);
        for id in &needed {
            let skip = (*id == ColumnId::Attributes && shred_attr)
                || (*id == ColumnId::ResourceAttributes && shred_res);
            if skip {
                continue;
            }
            cols.insert(*id, seg.reader.read_column(*id)?);
            ctx.stats.bytes_read += footer.column(*id).map_or(0, |c| c.len);
            // Full maps also read their shredded chunks.
            let scope = match id {
                ColumnId::Attributes => Some(Scope::Attributes),
                ColumnId::ResourceAttributes => Some(Scope::Resource),
                _ => None,
            };
            if let Some(scope) = scope {
                ctx.stats.bytes_read += footer
                    .shredded
                    .iter()
                    .filter(|c| c.scope == scope)
                    .map(|c| c.len)
                    .sum::<u64>();
            }
        }
        for (scope, keys, on) in [
            (Scope::Attributes, &plan.attr_keys, shred_attr),
            (Scope::Resource, &plan.resource_keys, shred_res),
        ] {
            if !on {
                continue;
            }
            for k in keys {
                if let Some(vals) = seg.reader.read_shredded(scope, k)? {
                    ctx.stats.bytes_read += footer.shredded(scope, k).map_or(0, |c| c.len);
                    shredded.insert((scope, k.clone()), vals);
                }
            }
            ctx.stats.shredded_reads += 1;
        }
        let batch = Batch {
            len: footer.row_count as usize,
            tenant,
            cols,
            shredded,
        };
        ctx.process(&batch)?;
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
            .map(|id| (*id, column_from_events(*id, &evs)))
            .collect();
        ctx.stats.memory_rows_scanned += evs.len() as u64;
        ctx.process(&Batch {
            len: evs.len(),
            tenant,
            cols,
            shredded: HashMap::new(),
        })?;
    }
    ctx.check_budget()?;

    if plan.aggregate {
        if ctx.group_vals.is_empty() && plan.group_by.is_empty() {
            let states = plan
                .aggregates
                .iter()
                .map(|(f, _)| AggState::new(*f))
                .collect();
            ctx.group_vals.push((Vec::new(), states));
        }
        let groups = std::mem::take(&mut ctx.group_vals);
        for (keys, states) in groups {
            let out = plan
                .outputs
                .iter()
                .map(|o| match o {
                    OutputSource::Group(i) => keys[*i].clone(),
                    OutputSource::Agg(i) => states[*i].finish(),
                    OutputSource::Row(_) => None,
                })
                .collect();
            ctx.rows.push((out, Vec::new()));
        }
    }
    if !plan.order.is_empty() {
        ctx.sort_rows();
    }
    if let Some(l) = plan.limit {
        ctx.rows.truncate(usize::try_from(l).unwrap_or(usize::MAX));
    }
    if ctx.rows.len() > opts.max_result_rows {
        return Err(Error::resource_exhausted(format!(
            "result has more than {} rows; add a LIMIT",
            opts.max_result_rows
        )));
    }
    ctx.stats.peak_memory_bytes = ctx.stats.peak_memory_bytes.max(ctx.mem);
    ctx.stats.elapsed = started.elapsed();
    if plan.explain == ExplainMode::Analyze {
        let text = explain_text(plan, snapshot, &ctx.stats, ctx.stats.segments_scanned, true);
        return Ok(QueryOutput {
            columns: vec!["plan".into()],
            rows: Vec::new(),
            stats: ctx.stats,
            explain: Some(text),
        });
    }
    Ok(QueryOutput {
        columns: plan.output_names.clone(),
        rows: ctx.rows.into_iter().map(|(o, _)| o).collect(),
        stats: ctx.stats,
        explain: None,
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
        "  segments: {} total, pruned {} by time, {} by keys, {} by severity, {} by bloom, {} to scan",
        s.segments_total,
        s.segments_pruned_time,
        s.segments_pruned_keys,
        s.segments_pruned_severity,
        s.segments_pruned_bloom,
        to_scan
    );
    let _ = writeln!(t, "  memtable rows: {}", snap.memory_rows().len());
    if !p.bloom.is_empty() {
        let c: Vec<String> = p
            .bloom
            .iter()
            .map(|(f, v)| format!("{} ({} value(s))", f.describe(), v.len()))
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
