//! Budgeted executor over a tenant-scoped storage [`Snapshot`] (spec §13.5, §13.6).
//!
//! Data flows as columnar batches from storage [`ScanSource`]s (segments and
//! memtable chunks alike, spec STRATA-ARCH-01-003): the executor states which
//! columns and attribute keys it needs and never sees a file format. The filter, projection, grouping and sort keys are
//! evaluated per row over a batch.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stratadb_common::codec::Encoder;
use stratadb_common::memory::{Accountant, MemoryTracker, DEFAULT_BATCH_BYTES};
use stratadb_common::{Error, ErrorKind, Result, TenantId};
use stratadb_schema::column::{ColumnData, ColumnId, RawValues, Scope, StrColumn};
use stratadb_schema::{Severity, Value};
use stratadb_storage::scan::{ColumnRequest, Pruned, ScanSource};
use stratadb_storage::Snapshot;

use crate::ast::{AggFunc, CmpOp, ColumnRef, ExplainMode, Expr, Table};
use crate::eval::{self, Datum, Truth};
use crate::page::{self, Cursor};
use crate::plan::{OutputSource, Plan, SortSource};

/// Per-query resource budget and switches. Defaults are conservative.
#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub timeout: Duration,
    pub max_rows_scanned: u64,
    pub max_result_rows: usize,
    /// Memory limit of the query: result rows, aggregation groups and the
    /// column batches being scanned (reserved before they are read).
    pub max_memory_bytes: usize,
    /// Parent tracker of the query's memory (tenant / query pool, spec
    /// VIII.5.2). `None`: the query is accounted on its own.
    pub memory: Option<Arc<MemoryTracker>>,
    /// How long a reservation may wait for memory to be released before the
    /// query fails with `MEMORY_LIMIT_EXCEEDED` (0 = fail fast).
    pub memory_wait: Duration,
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
    /// Answer global `count(*)` / `min` / `max(event_time)` from segment and
    /// chunk metadata where the filter provably covers a source (spec
    /// STRATA-SYS-09-006/007). Disabling it must never change results.
    pub enable_metadata_aggregates: bool,
    /// Aggregate through dense code arrays when every group key is a
    /// dictionary or byte code column (STRATA-SYS-09-008). Disabling it must
    /// never change results (rows or their order).
    pub enable_dense_groups: bool,
    /// Sort through normalized keys (radix or pdqsort over a permutation),
    /// sort each segment's rows on its scan worker and merge the sorted
    /// runs with a loser tree (STRATA-SYS-14-001/002/006). Disabling it
    /// must never change results (rows or their order).
    pub enable_normalized_sort: bool,
    /// `ORDER BY ... LIMIT k`: once `k` rows are held, skip rows that sort
    /// strictly after the k-th before building their outputs; the bound is
    /// shared by all scan workers (STRATA-SYS-14-003). Disabling it must
    /// never change results.
    pub enable_topk_bound: bool,
    /// Per-segment partial aggregate cache shared across queries (spec
    /// STRATA-ARCH-09-002/003; `None`: off). With a cache, aggregates are
    /// always computed as per-segment partials merged in segment order (as
    /// parallel scans are), and cached partials merge exactly like fresh
    /// ones: results do not depend on hits or misses.
    pub partial_cache: Option<Arc<crate::partial_cache::PartialCache>>,
    /// `ORDER BY event_time [ASC|DESC] LIMIT k`: read sources best-first by
    /// their time ranges and stop when the k-th row beats every remaining
    /// source (STRATA-SYS-14-004). Disabling it must never change results.
    pub enable_ordered_scan: bool,
    pub parse_limits: crate::parser::ParseLimits,
    /// Worker threads for scanning segments (sorted and aggregate queries;
    /// spec §13.5). 1 = sequential. Results are merged in segment order, so
    /// rows and groups match a sequential scan; float `sum`/`avg` may differ
    /// in the last bits (different addition order), deterministically.
    /// This is an upper bound: the cost-based degree of parallelism
    /// (`target_ms_per_worker`) usually picks fewer for small scans.
    pub parallelism: usize,
    /// Estimated single-core CPU milliseconds of scan work per worker
    /// (STRATA-SYS-11-008: 5 ms for interactive queries). `0` disables the
    /// estimate and always uses `parallelism` workers.
    pub target_ms_per_worker: f64,
    /// The query's work class (spec VIII.11.1), shared with its scan-pool
    /// tasks so a demotion applies at their next morsel boundary. `None`:
    /// `normal`.
    pub class: Option<Arc<stratadb_common::workload::ClassCell>>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_rows_scanned: 100_000_000,
            max_result_rows: 100_000,
            max_memory_bytes: 256 * 1024 * 1024,
            memory: None,
            memory_wait: Duration::ZERO,
            cancel: None,
            enable_pruning: true,
            enable_vectorized: true,
            enable_map_projection: true,
            enable_metadata_aggregates: true,
            enable_dense_groups: true,
            enable_normalized_sort: true,
            enable_topk_bound: true,
            partial_cache: None,
            enable_ordered_scan: true,
            parse_limits: Default::default(),
            parallelism: std::thread::available_parallelism()
                .map_or(1, |n| n.get())
                .min(8),
            target_ms_per_worker: TARGET_MS_PER_WORKER,
            class: None,
        }
    }
}

/// The aggregation hash table of a query (STRATA-SYS-09-011).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HashTableStats {
    pub variant: &'static str,
    pub groups: usize,
    /// Groups held before the next growth (load factor 7/8).
    pub capacity: usize,
    /// Capacity doublings of the final table (per-segment partial tables of
    /// parallel scans are not counted).
    pub resizes: u64,
    /// Heap bytes of the table, its key arena and state arrays.
    pub bytes: usize,
    /// Batches aggregated through a dense code array (all workers).
    pub dense_batches: u64,
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
    /// Memtable chunk windows skipped by their zone maps (time, severity).
    pub memory_chunks_pruned: u64,
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
    /// Workers that scanned at least one segment (this thread + pool tasks;
    /// 0 = no segment scan, 1 = sequential).
    pub scan_workers: usize,
    /// Pool tasks requested for this scan (the pool grants fewer when busy).
    pub scan_tasks_queued: usize,
    /// Estimated single-core CPU ms of the segment scan (drives the degree
    /// of parallelism, STRATA-SYS-11-008).
    pub est_cpu_ms: f64,
    /// CPU time of the query: the executing thread (parse, plan, scan,
    /// merge) plus the scan-pool workers that helped (STRATA-SYS-13-009),
    /// minus `cpu_lent`.
    pub cpu: Duration,
    /// CPU the executing thread spent running higher-class queries' morsels
    /// at its own morsel boundaries (spec VIII.11.1 preemption); not part
    /// of this query's `cpu`.
    pub cpu_lent: Duration,
    /// The final aggregation hash table (EXPLAIN ANALYZE,
    /// STRATA-SYS-09-011); `None` for row queries.
    pub group_table: Option<HashTableStats>,
    /// Column batches aggregated through a dense code array (dictionary or
    /// byte group keys, STRATA-SYS-09-008).
    pub dense_group_batches: u64,
    /// Segment columns served from the decoded column cache (ADR-0042).
    pub column_cache_hits: u64,
    /// Sources (segments, memtable windows) never read because the k-th row
    /// of an `ORDER BY event_time ... LIMIT k` already beat their whole time
    /// range (STRATA-SYS-14-004).
    pub sources_skipped_by_order: u64,
    /// Sorted runs (one per scanned segment) merged by the loser tree
    /// instead of being concatenated and re-sorted (STRATA-SYS-14-001).
    pub sort_runs_merged: u64,
    /// Rows of an `ORDER BY ... LIMIT k` query skipped before their outputs
    /// were built, because they sort after the query-wide k-th row
    /// (STRATA-SYS-14-003; all workers).
    pub topk_rows_skipped: u64,
    /// Segments whose aggregate partial came from the partial cache instead
    /// of a scan, and the rows those scans would have read
    /// (STRATA-ARCH-09-002; not included in `rows_scanned`).
    pub partials_from_cache: u64,
    pub rows_from_cache: u64,
    /// Times a conjunction of the vectorized filter changed its evaluation
    /// order from observed cost and selectivity (STRATA-SYS-08-019; all
    /// workers).
    pub filter_reorders: u64,
    /// Sources (segments, memtable chunk windows) whose contribution was
    /// answered from metadata without reading columns (STRATA-SYS-09-006).
    pub sources_from_metadata: u64,
    /// Morsel boundaries at which the executing thread yielded to
    /// higher-class work instead of scanning (spec VIII.11.1).
    pub scan_yields: u64,
    /// The table read (`logs`, `system.query_log`).
    pub table: &'static str,
    /// Peak memory reserved by the query on its tracker (operator state and
    /// column batches, in reservation batches).
    pub memory_reserved_peak: u64,
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

/// Resolves the tables a query names to tenant-scoped snapshots (spec §17).
/// A bare [`Snapshot`] is a catalog holding only `logs`.
pub trait Catalog {
    /// The tenant every resolved snapshot must belong to.
    fn tenant(&self) -> &TenantId;
    /// A snapshot of `table` for [`Catalog::tenant`].
    fn snapshot(&self, table: Table) -> Result<std::borrow::Cow<'_, Snapshot>>;
}

impl Catalog for Snapshot {
    fn tenant(&self) -> &TenantId {
        Snapshot::tenant(self)
    }

    fn snapshot(&self, table: Table) -> Result<std::borrow::Cow<'_, Snapshot>> {
        match table {
            Table::Logs => Ok(std::borrow::Cow::Borrowed(self)),
            t => Err(Error::unsupported(format!(
                "table {} is not available here",
                t.name()
            ))),
        }
    }
}

/// The plan's table from `catalog`, checked to be the catalog's tenant
/// (defense in depth: a query never reads another tenant's snapshot).
fn resolve<'c, C: Catalog + ?Sized>(
    catalog: &'c C,
    plan: &Plan,
) -> Result<std::borrow::Cow<'c, Snapshot>> {
    let snap = catalog.snapshot(plan.table)?;
    if snap.tenant() != catalog.tenant() {
        return Err(Error::internal(
            "catalog returned another tenant's snapshot",
        ));
    }
    Ok(snap)
}

/// Parse, plan, and run `sql` against the tables of `catalog` (a
/// [`Snapshot`] for `logs` only).
pub fn execute<C: Catalog + ?Sized>(
    catalog: &C,
    sql: &str,
    opts: &QueryOptions,
) -> Result<QueryOutput> {
    let prepared = parse(sql, opts)?.plan()?;
    execute_prepared(catalog, &prepared, opts)
}

/// A parsed query (spec IX.22.1 `PARSED`).
#[derive(Debug, Clone)]
pub struct Parsed {
    sql: String,
    stmt: crate::ast::Statement,
    parse: Duration,
    cpu: Duration,
}

/// A planned query, ready to run (spec IX.22.1 `PLANNED`).
#[derive(Debug, Clone)]
pub struct Prepared {
    sql: String,
    /// Shared: a cached prepared query is cloned per execution in O(1).
    plan: Arc<Plan>,
    parse: Duration,
    plan_time: Duration,
    /// CPU spent parsing and planning (added to the run's CPU).
    cpu: Duration,
}

impl Prepared {
    /// Whether pagination applies: row queries only (not aggregates, not
    /// EXPLAIN).
    pub fn paginable(&self) -> bool {
        !self.plan.aggregate && self.plan.explain == ExplainMode::None
    }

    /// This prepared query as served from a plan cache: the lookup is its
    /// parse time, it took no planning or parse CPU (spec STRATA-SYS-16-004).
    pub fn cached(&self, lookup: Duration) -> Prepared {
        Prepared {
            sql: self.sql.clone(),
            plan: Arc::clone(&self.plan),
            parse: lookup,
            plan_time: Duration::ZERO,
            cpu: Duration::ZERO,
        }
    }

    /// Approximate heap size, for cache budgets.
    pub fn approx_bytes(&self) -> usize {
        // The plan holds expression trees about the size of the text.
        512 + self.sql.len() * 4
    }

    /// The table the query reads.
    pub fn table(&self) -> Table {
        self.plan.table
    }

    /// Plan-time cost estimate from metadata only (STRATA-SYS-11-003): the
    /// rows of segments that survive pruning plus the visible memtable
    /// rows, times the plan's per-row cost. Reads no column data; used to
    /// classify the query before admission.
    pub fn estimate(&self, catalog: &dyn Catalog) -> Result<Estimate> {
        let snapshot = catalog.snapshot(self.plan.table)?;
        let mut e = Estimate::default();
        for seg in snapshot.sources() {
            if seg.prune(&self.plan.pruning).is_some() {
                e.segments_pruned += 1;
            } else {
                e.segments += 1;
                e.rows += seg.rows();
            }
        }
        e.memory_rows = snapshot.memory_chunks(1 << 16).map(|c| c.rows()).sum();
        e.est_cpu_ms = (e.rows + e.memory_rows) as f64 * est_ns_per_row(&self.plan) / 1e6;
        Ok(e)
    }
}

/// A query's estimated work (see [`Prepared::estimate`]).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Estimate {
    /// Segments to scan after pruning.
    pub segments: usize,
    pub segments_pruned: usize,
    /// Rows in those segments.
    pub rows: u64,
    /// Visible memtable rows (not pruned by the estimate).
    pub memory_rows: u64,
    /// Estimated single-core CPU milliseconds.
    pub est_cpu_ms: f64,
}

/// Parse `sql`; a failure here is a syntax error.
pub fn parse(sql: &str, opts: &QueryOptions) -> Result<Parsed> {
    let cpu = stratadb_common::cpu::thread_cpu_time();
    let t = Instant::now();
    let stmt = crate::parser::parse(sql, &opts.parse_limits)?;
    Ok(Parsed {
        sql: sql.to_owned(),
        stmt,
        parse: t.elapsed(),
        cpu: stratadb_common::cpu::thread_cpu_time().saturating_sub(cpu),
    })
}

impl Parsed {
    /// Bind and plan; a failure here is a semantic error.
    pub fn plan(self) -> Result<Prepared> {
        let cpu = stratadb_common::cpu::thread_cpu_time();
        let t = Instant::now();
        let plan = Plan::new(self.stmt)?;
        Ok(Prepared {
            sql: self.sql,
            plan: Arc::new(plan),
            parse: self.parse,
            plan_time: t.elapsed(),
            cpu: self.cpu + stratadb_common::cpu::thread_cpu_time().saturating_sub(cpu),
        })
    }
}

/// Run a prepared query against the tables of `catalog`.
pub fn execute_prepared<C: Catalog + ?Sized>(
    catalog: &C,
    prepared: &Prepared,
    opts: &QueryOptions,
) -> Result<QueryOutput> {
    let cpu = stratadb_common::cpu::thread_cpu_time();
    let plan = &prepared.plan;
    let snapshot = resolve(catalog, plan)?;
    let mut res = run(&snapshot, plan, opts, None)?;
    res.output.stats.parse = prepared.parse;
    res.output.stats.plan = prepared.plan_time;
    res.output.stats.cpu +=
        prepared.cpu + stratadb_common::cpu::thread_cpu_time().saturating_sub(cpu);
    res.output.stats.cpu = res
        .output
        .stats
        .cpu
        .saturating_sub(res.output.stats.cpu_lent);
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
pub fn execute_page<C: Catalog + ?Sized>(
    catalog: &C,
    sql: &str,
    opts: &QueryOptions,
    req: &PageRequest,
) -> Result<Page> {
    let prepared = parse(sql, opts)?.plan()?;
    execute_page_prepared(catalog, &prepared, opts, req)
}

/// [`execute_page`] for a prepared query.
pub fn execute_page_prepared<C: Catalog + ?Sized>(
    catalog: &C,
    prepared: &Prepared,
    opts: &QueryOptions,
    req: &PageRequest,
) -> Result<Page> {
    let cpu = stratadb_common::cpu::thread_cpu_time();
    let plan = &prepared.plan;
    let sql = prepared.sql.as_str();
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
    let snapshot = resolve(catalog, plan)?;
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
    let mut res = run(&snapshot, plan, opts, Some(pctx))?;
    res.output.stats.parse = prepared.parse;
    res.output.stats.plan = prepared.plan_time;
    res.output.stats.cpu +=
        prepared.cpu + stratadb_common::cpu::thread_cpu_time().saturating_sub(cpu);
    res.output.stats.cpu = res
        .output
        .stats
        .cpu
        .saturating_sub(res.output.stats.cpu_lent);
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

/// Top-k selections keeping at least `1 / NORMALIZED_MIN_KEEP_DIVISOR` of
/// the rows sort through normalized keys; smaller ones select by comparison.
const NORMALIZED_MIN_KEEP_DIVISOR: usize = 4;

/// Sort `rows` by `order` and keep the first `keep`. With a bound smaller
/// than the row count this selects the top `k` (O(n + k log k)) instead of
/// sorting everything; ties are broken by the rows' current positions, so the
/// result equals a stable sort followed by truncation exactly.
fn sort_keep(
    rows: &mut Vec<Row>,
    order: &[(SortSource, bool)],
    keep: Option<usize>,
    normalized: bool,
) {
    // Normalized keys pay an encoding pass over every row; that wins when
    // most rows get sorted, not when a few are selected out of many
    // (measured: sort_kernel_ab, ADR-0043).
    if normalized
        && keep.is_none_or(|k| k.saturating_mul(NORMALIZED_MIN_KEEP_DIVISOR) >= rows.len())
    {
        let desc: Vec<bool> = order.iter().map(|(_, d)| *d).collect();
        let keys = crate::sort::NormKeys::build(rows, &desc, |r| &r.keys);
        let all: &[Row] = rows;
        let full = |a: usize, b: usize| {
            cmp_position(order, (&all[a].keys, all[a].id), (&all[b].keys, all[b].id))
        };
        // Exact keys and no row identities: equal keys are equal rows.
        let tie: Option<&dyn Fn(usize, usize) -> std::cmp::Ordering> =
            if keys.exact() && all.iter().all(|r| r.id == (0, 0)) {
                None
            } else {
                Some(&full)
            };
        let perm = crate::sort::order(&keys, tie, keep);
        let mut slots: Vec<Option<Row>> = std::mem::take(rows).into_iter().map(Some).collect();
        rows.extend(perm.iter().filter_map(|i| slots[*i as usize].take()));
        return;
    }
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
    Strs(&'b StrColumn),
    /// A dictionary-encoded string column: each distinct value's key
    /// encoding computed once (index = code; 0 = missing).
    Dict(Vec<Vec<u8>>, &'b [u32]),
    /// A one-byte code column (severity): key encoding per code value.
    Byte(Box<[Vec<u8>]>, &'b [u8]),
    Values(&'b [Datum]),
    /// Already encoded: copied verbatim (no decode, no allocation).
    Raw(&'b RawValues),
    Map(&'b [BTreeMap<String, Value>], &'b str),
    Eval(&'b Expr),
}

/// Estimated heap of one aggregate state beyond its inline size (a min/max
/// string value, say); the inline part is in the table's own accounting.
const STATE_HEAP_BYTES: usize = 16;

/// Largest combined code space aggregated through a dense array.
const DENSE_GROUP_CODES: usize = 1 << 16;

impl KeySrc<'_> {
    /// The size of this key's code space when it is a dictionary or byte
    /// code column (codes index the pre-encoded keys).
    fn domain(&self) -> Option<usize> {
        match self {
            KeySrc::Dict(encoded, _) => Some(encoded.len()),
            KeySrc::Byte(encoded, _) => Some(encoded.len()),
            _ => None,
        }
    }

    /// Row `i`'s code (only for [`KeySrc::domain`] sources).
    fn code(&self, i: usize) -> usize {
        match self {
            KeySrc::Dict(_, codes) => codes[i] as usize,
            KeySrc::Byte(_, codes) => usize::from(codes[i]),
            _ => usize::MAX,
        }
    }

    /// Append the `eval::group_key_bytes` encoding of row `i`.
    fn encode(&self, b: &Batch<'_>, i: usize, enc: &mut Encoder) -> Result<()> {
        use stratadb_schema::codec::{encode_string_value, encode_value};
        let val = |enc: &mut Encoder, v: Option<&Value>| match v {
            Some(v) => encode_value(enc, v),
            None => enc.put_u8(eval::MISSING_KEY_TAG),
        };
        match self {
            KeySrc::Dict(encoded, codes) => enc.put_raw(&encoded[codes[i] as usize]),
            KeySrc::Byte(encoded, codes) => enc.put_raw(&encoded[codes[i] as usize]),
            KeySrc::Strs(v) => match v.get(i) {
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
                    ColumnData::Strs(v) => match v.dict() {
                        Some((dict, codes)) => {
                            use stratadb_schema::codec::encode_string_value;
                            let mut encoded = Vec::with_capacity(dict.len() + 1);
                            encoded.push(vec![eval::MISSING_KEY_TAG]);
                            for s in dict {
                                let mut e = Encoder::new();
                                encode_string_value(&mut e, s);
                                encoded.push(e.into_inner());
                            }
                            KeySrc::Dict(encoded, codes)
                        }
                        None => KeySrc::Strs(v),
                    },
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
            // The severity name, encoded once per code (exactly what
            // evaluating the column per row would encode).
            ColumnRef::Severity => match self.col(ColumnId::Severity)? {
                ColumnData::U8s(v) => {
                    use stratadb_schema::codec::encode_value;
                    let encoded: Box<[Vec<u8>]> = (0..=255u8)
                        .map(|c| {
                            let name = Severity::from_u8(c).unwrap_or_default().as_str();
                            let mut e = Encoder::new();
                            encode_value(&mut e, &Value::String(name.to_owned()));
                            e.into_inner()
                        })
                        .collect();
                    KeySrc::Byte(encoded, v)
                }
                _ => return Err(Error::internal("expected severity column")),
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
                ColumnData::Strs(v) => Ok(v.get(i).map(|s| Value::String(s.to_owned()))),
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
pub(crate) enum AggState {
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

    /// Add `n` to a count.
    fn update_count(&mut self, n: u64) {
        if let AggState::Count(c) = self {
            *c += n;
        }
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
    /// The first `sorted_rows` of `rows` are in final order (merged sorted
    /// runs); rows are only ever appended after them or truncated.
    sorted_rows: usize,
    /// This worker's copy of the top-k bound and the shared version it
    /// reflects.
    bound: Option<Arc<Bound>>,
    bound_version: u64,
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
    /// This worker's reservation on the query's memory tracker.
    acct: Accountant,
    /// Estimated bytes of the column batch being scanned.
    batch_mem: u64,
    /// Set when sources can be answered from metadata (see [`MetaPlan`]).
    meta: Option<MetaPlan>,
    /// Ordered scans: the position of the source being read in the default
    /// scan order, so a row's identity `(pos, row)` breaks ties exactly as
    /// the default (position) order would.
    pos_base: Option<u64>,
}

/// A global aggregate answerable from source metadata: every aggregate is
/// `count(*)`, `count(event_time)`, `min(event_time)` or `max(event_time)`,
/// and the filter is a [`MetaFilter`] (STRATA-SYS-09-006/007).
#[derive(Debug, Clone)]
struct MetaPlan {
    filter: crate::plan::MetaFilter,
    aggs: Vec<MetaAgg>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetaAgg {
    Count,
    MinTime,
    MaxTime,
}

impl MetaPlan {
    fn of(plan: &Plan, opts: &QueryOptions, paged: bool) -> Option<Self> {
        if !opts.enable_metadata_aggregates
            || !plan.aggregate
            || !plan.group_by.is_empty()
            || plan.aggregates.is_empty()
            || paged
        {
            return None;
        }
        let et = |a: &Option<Expr>| matches!(a, Some(Expr::Column(ColumnRef::EventTime)));
        let aggs = plan
            .aggregates
            .iter()
            .map(|(f, a)| match f {
                AggFunc::Count if a.is_none() || et(a) => Some(MetaAgg::Count),
                AggFunc::Min if et(a) => Some(MetaAgg::MinTime),
                AggFunc::Max if et(a) => Some(MetaAgg::MaxTime),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            filter: crate::plan::MetaFilter::of(plan.filter.as_ref())?,
            aggs,
        })
    }
}

/// Rows scanned and memory held across all workers of one query, so budgets
/// stay query-wide under parallel scans; `stop` makes workers give up after
/// another worker failed.
#[derive(Debug)]
struct SharedBudget {
    rows: std::sync::atomic::AtomicU64,
    mem: std::sync::atomic::AtomicI64,
    stop: AtomicBool,
    /// The best (smallest) k-th row position any worker has held, and a
    /// version bumped on every improvement so workers re-read it only
    /// when it changed (STRATA-SYS-14-003).
    topk: Mutex<Option<Arc<Bound>>>,
    topk_version: std::sync::atomic::AtomicU64,
    /// The query's node in the memory hierarchy (limit `max_memory_bytes`).
    tracker: Arc<MemoryTracker>,
}

/// A row position (order keys, identity) that `k` held rows sort at or
/// before: any row sorting strictly after it cannot be in the top `k`.
type Bound = (Vec<Datum>, (u64, u32));

impl SharedBudget {
    /// Offer `b` as the query-wide bound; kept when it is tighter.
    fn offer_bound(&self, order: &[(SortSource, bool)], b: &Arc<Bound>) {
        let mut cur = self.topk.lock().unwrap_or_else(|e| e.into_inner());
        let tighter = cur.as_ref().is_none_or(|c| {
            cmp_position(order, (&b.0, b.1), (&c.0, c.1)) == std::cmp::Ordering::Less
        });
        if tighter {
            *cur = Some(Arc::clone(b));
            self.topk_version.fetch_add(1, AtomicOrdering::Release);
        }
    }

    fn new(opts: &QueryOptions) -> Self {
        let limit = opts.max_memory_bytes as u64;
        Self {
            rows: Default::default(),
            mem: Default::default(),
            stop: Default::default(),
            topk: Mutex::new(None),
            topk_version: Default::default(),
            tracker: match &opts.memory {
                Some(parent) => parent.child("exec", limit),
                None => MemoryTracker::root("query", limit),
            },
        }
    }
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
            sorted_rows: 0,
            bound: None,
            bound_version: 0,
            groups: crate::groups::GroupTable::new(plan.aggregates.len()),
            page,
            keep,
            key_buf: Vec::new(),
            vfilter,
            acct: Accountant::new(
                Some(Arc::clone(&budget.tracker)),
                DEFAULT_BATCH_BYTES,
                opts.memory_wait,
            ),
            budget,
            reported_rows: 0,
            reported_mem: 0,
            batch_mem: 0,
            // Set by `run` on its top-level context only: it answers
            // segments before any worker starts.
            meta: None,
            pos_base: None,
        }
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
        // This worker's operator state plus the batch it holds, reserved on
        // the query tracker (and so on every ancestor pool).
        self.acct
            .set_usage(self.mem as u64 + self.batch_mem)
            .map_err(|e| self.memory_error(e))
    }

    /// A reservation failure, with the query's own limit spelled out.
    fn memory_error(&self, e: Error) -> Error {
        Error::resource_exhausted(format!(
            "{} (query limit {} bytes)",
            e.to_string().trim_start_matches("resource_exhausted: "),
            self.opts.max_memory_bytes
        ))
    }

    /// Add `src`'s contribution from its metadata alone, when the plan
    /// allows it and the filter provably covers the source: its time range
    /// lies inside the filter's bounds, and with a severity filter its exact
    /// per-severity counts are known. `min`/`max(event_time)` need every row
    /// to match. Returns false when the source must be read.
    fn answer_from_metadata(&mut self, src: &dyn ScanSource) -> Result<bool> {
        let Some(m) = &self.meta else {
            return Ok(false);
        };
        let rows = src.rows();
        if rows == 0 {
            return Ok(true);
        }
        let range = src.event_time_range();
        if !m.filter.covers_time(range) {
            return Ok(false);
        }
        let n = match &m.filter.severities {
            None => rows,
            Some(set) => match src.severity_counts() {
                Some(c) => c
                    .iter()
                    .filter(|(k, _)| set.contains(k))
                    .map(|(_, n)| n)
                    .sum(),
                None => return Ok(false),
            },
        };
        if n > 0 && n < rows && m.aggs.iter().any(|a| *a != MetaAgg::Count) {
            return Ok(false); // min/max over a subset: unknown
        }
        let aggs = m.aggs.clone();
        self.stats.sources_from_metadata += 1;
        self.stats.rows_matched += n;
        if n == 0 {
            return Ok(true);
        }
        let h = self.groups.hash(&[]);
        let idx = match self.groups.find(h, &[]) {
            Some(g) => g,
            None => {
                self.mem += 64 + 48 * self.plan.aggregates.len();
                let states = self.plan.aggregates.iter().map(|(f, _)| AggState::new(*f));
                self.groups.insert(h, &[], states)
            }
        };
        for (st, a) in self.groups.states_mut(idx).iter_mut().zip(aggs) {
            match a {
                MetaAgg::Count => st.update_count(n),
                MetaAgg::MinTime => st.update(false, Some(Value::Timestamp(range.0)))?,
                MetaAgg::MaxTime => st.update(false, Some(Value::Timestamp(range.1)))?,
            }
        }
        Ok(true)
    }

    /// Read `src` (reserving its estimated batch memory first,
    /// STRATA-SYS-05-005) and process its rows, unless metadata answers it.
    /// Returns whether the source was read.
    fn scan(
        &mut self,
        src: &dyn ScanSource,
        needed: &[ColumnId],
        tenant: &TenantId,
        projection: bool,
    ) -> Result<bool> {
        if self.answer_from_metadata(src)? {
            return Ok(false);
        }
        let req = column_request(self.plan, needed, projection);
        self.batch_mem = src.estimated_read_bytes(&req);
        self.acct
            .set_usage(self.mem as u64 + self.batch_mem)
            .map_err(|e| self.memory_error(e))?;
        let b = src.read(tenant, &req)?;
        self.stats.bytes_read += b.bytes_read;
        self.stats.shredded_reads += b.dedicated_key_reads;
        self.stats.column_cache_hits += b.cached_columns;
        let batch = Batch {
            len: b.rows,
            tenant,
            cols: b.columns,
            shredded: b.keys,
            raw: b.raw,
        };
        let r = self.process(&batch);
        drop(batch);
        self.batch_mem = 0;
        r.map(|()| true)
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
        let normalized = self.opts.enable_normalized_sort;
        let p = self.sorted_rows.min(self.rows.len());
        if p == self.rows.len() {
            // Already in order (merged runs): only the bound applies.
            if let Some(k) = self.keep {
                self.rows.truncate(k);
            }
        } else if p > 0 && normalized {
            // A sorted prefix (merged runs) and an unsorted tail (memtable
            // rows): sort the tail and merge the two, stably.
            let mut tail = self.rows.split_off(p);
            sort_keep(&mut tail, &self.plan.order, self.keep, true);
            let head = std::mem::take(&mut self.rows);
            let order = &self.plan.order;
            self.rows = crate::sort::merge_runs(vec![head, tail], self.keep, |a, b| {
                cmp_position(order, (&a.keys, a.id), (&b.keys, b.id))
            });
        } else {
            sort_keep(&mut self.rows, &self.plan.order, self.keep, normalized);
        }
        self.sorted_rows = self.rows.len();
        self.note_bound();
    }

    /// Whether rows can be skipped against a top-k bound.
    fn uses_bound(&self) -> bool {
        self.opts.enable_topk_bound
            && !self.plan.aggregate
            && self.sorted()
            && self.keep.is_some_and(|k| k > 0)
    }

    /// After sorting: with `k` rows held, the k-th is a bound for this
    /// worker and, when tighter, for the whole query.
    fn note_bound(&mut self) {
        let Some(k) = self.keep else { return };
        if !self.uses_bound() || self.rows.len() < k {
            return;
        }
        let kth = &self.rows[k - 1];
        let order = &self.plan.order;
        let tighter = self.bound.as_ref().is_none_or(|c| {
            cmp_position(order, (&kth.keys, kth.id), (&c.0, c.1)) == std::cmp::Ordering::Less
        });
        if tighter {
            let b = Arc::new((kth.keys.clone(), kth.id));
            self.budget.offer_bound(order, &b);
            self.bound = Some(b);
        }
    }

    /// The first `k` rows held: the last of them in sort order is a bound
    /// already (`k` rows sort at or before it), found in one pass instead of
    /// waiting for the first compaction.
    fn first_bound(&mut self) {
        if !self.uses_bound() {
            return;
        }
        let order = &self.plan.order;
        let last = self
            .rows
            .iter()
            .max_by(|a, b| cmp_position(order, (&a.keys, a.id), (&b.keys, b.id)));
        if let Some(r) = last {
            let b = Arc::new((r.keys.clone(), r.id));
            self.budget.offer_bound(order, &b);
            self.bound = Some(b);
        }
    }

    /// Adopt a tighter query-wide bound published by another worker.
    fn refresh_bound(&mut self) {
        if !self.uses_bound() {
            return;
        }
        let v = self.budget.topk_version.load(AtomicOrdering::Acquire);
        if v == self.bound_version {
            return;
        }
        self.bound_version = v;
        let shared = self
            .budget
            .topk
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(s) = shared {
            let order = &self.plan.order;
            let tighter = self.bound.as_ref().is_none_or(|c| {
                cmp_position(order, (&s.0, s.1), (&c.0, c.1)) == std::cmp::Ordering::Less
            });
            if tighter {
                self.bound = Some(s);
            }
        }
    }

    /// Keep memory bounded for sorted queries with a row bound (top-k).
    fn maybe_compact(&mut self) {
        if let (true, Some(k)) = (self.sorted(), self.keep) {
            // With a top-k bound, compacting tightens it and rows reaching
            // this point are rare, so compact early; without one, batch the
            // work.
            let floor = if self.bound.is_some() { 64 } else { 1024 };
            if self.rows.len() > k.saturating_mul(2).max(floor) {
                self.sort_keep();
                self.mem = self.rows.iter().map(row_size).sum();
            }
        }
    }

    /// Whether every row of a batch is kept (no filter, no page bound): the
    /// selection is then the dense range, never materialized.
    fn keeps_all(&self) -> bool {
        self.plan.filter.is_none() && self.page.is_none()
    }

    /// The rows of `b[start..end)` that the query keeps: visible to the
    /// page (`seq <= as_of`) and matching the filter. A selection, not a
    /// compacted copy: later work reads only these rows (spec
    /// STRATA-ARCH-03-006).
    fn select(
        &mut self,
        b: &Batch<'_>,
        start: usize,
        end: usize,
        sel: &mut Vec<u32>,
    ) -> Result<()> {
        sel.clear();
        let truth = match (&self.plan.filter, &mut self.vfilter) {
            (Some(_), Some(vf)) => Some(vf.eval(b, start, end)?),
            _ => None,
        };
        if let (None, Some(t)) = (&self.page, &truth) {
            // Common case: vectorized filter, no page bound.
            sel.extend(
                t.iter()
                    .enumerate()
                    .filter(|(_, v)| **v == Some(true))
                    .map(|(j, _)| (start + j) as u32),
            );
            return Ok(());
        }
        for i in start..end {
            if let Some(p) = &self.page {
                if b.id(i)?.0 > p.as_of {
                    continue; // committed after the first page
                }
            }
            let keep = match (&self.plan.filter, &truth) {
                (None, _) => true,
                (Some(_), Some(t)) => t[i - start] == Some(true),
                (Some(f), None) => eval_truth(f, b, i)? == Some(true),
            };
            if keep {
                sel.push(i as u32);
            }
        }
        Ok(())
    }

    /// Whether the plan is a global `count(*)`-only aggregate: counted from
    /// selection sizes, without visiting rows.
    fn count_only(&self) -> bool {
        let p = self.plan;
        p.aggregate
            && p.group_by.is_empty()
            && !p.aggregates.is_empty()
            && p.aggregates
                .iter()
                .all(|(f, a)| *f == AggFunc::Count && a.is_none())
    }

    /// The group of row `i` (created on first sight, keeping first-seen
    /// order): its key bytes, built in a reused buffer straight from the
    /// batch, are hashed and probed.
    #[inline]
    fn group_index(&mut self, b: &Batch<'_>, i: usize, key_srcs: &[KeySrc<'_>]) -> Result<usize> {
        // Per key a u32 length then its group_key_bytes.
        let mut enc = Encoder::from_vec(std::mem::take(&mut self.key_buf));
        for src in key_srcs {
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
                // New group: only its key bytes are stored; key values are
                // decoded from them once, at output. Growth of the table or
                // its arrays is reserved before it is allocated
                // (STRATA-SYS-09-002); then the actual heap change is
                // charged, plus the states' own heap (estimated).
                let grow = self.groups.growth_bytes(kb.len());
                if grow > 0 {
                    self.acct
                        .set_usage(self.mem as u64 + grow as u64 + self.batch_mem)
                        .map_err(|e| self.memory_error(e))?;
                }
                let before = self.groups.heap_bytes();
                let states = self.plan.aggregates.iter().map(|(f, _)| AggState::new(*f));
                let g = self.groups.insert(h, &kb, states);
                self.mem += self.groups.heap_bytes().saturating_sub(before)
                    + STATE_HEAP_BYTES * self.plan.aggregates.len();
                g
            }
        };
        kb.clear();
        self.key_buf = kb;
        Ok(idx)
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
        // Aggregation on codes (STRATA-SYS-09-008): when every key is a
        // dictionary or byte code column and their combined code space is
        // small, a dense array maps a row's combined code to its group, so
        // each distinct key is hashed and probed once per batch. With only
        // `count(*)`, rows are counted per code and added at the end.
        let domains: Option<Vec<usize>> =
            (plan.aggregate && !key_srcs.is_empty() && self.opts.enable_dense_groups)
                .then(|| {
                    key_srcs
                        .iter()
                        .map(KeySrc::domain)
                        .collect::<Option<Vec<_>>>()
                })
                .flatten()
                .filter(|d| {
                    d.iter()
                        .try_fold(1usize, |acc, x| acc.checked_mul(*x))
                        .is_some_and(|n| n <= DENSE_GROUP_CODES)
                });
        let star_only = plan
            .aggregates
            .iter()
            .all(|(f, a)| *f == AggFunc::Count && a.is_none());
        let space = domains.as_ref().map_or(0, |d| d.iter().product());
        let mut dense_group: Vec<u32> = vec![u32::MAX; space];
        let mut dense_count: Vec<u64> = if star_only {
            vec![0; space]
        } else {
            Vec::new()
        };
        if space > 0 {
            self.stats.dense_group_batches += 1;
        }
        let r = self.process_rows(
            b,
            &key_srcs,
            domains.as_deref(),
            &mut dense_group,
            &mut dense_count,
        );
        // Counts per code into their groups.
        for (c, n) in dense_count.iter().enumerate() {
            if *n > 0 {
                let g = dense_group[c] as usize;
                for st in self.groups.states_mut(g) {
                    st.update_count(*n);
                }
            }
        }
        r
    }

    #[inline]
    fn process_rows(
        &mut self,
        b: &Batch<'_>,
        key_srcs: &[KeySrc<'_>],
        domains: Option<&[usize]>,
        dense_group: &mut [u32],
        dense_count: &mut [u64],
    ) -> Result<()> {
        let plan = self.plan;
        let count_only = self.count_only();
        let mut sel: Vec<u32> = Vec::with_capacity(crate::vector::CHUNK);
        let mut start = 0usize;
        while start < b.len {
            let end = (start + crate::vector::CHUNK).min(b.len);
            self.check_budget()?;
            self.refresh_bound();
            if self.limit_reached() {
                self.stats.early_stop = true;
                return Ok(());
            }
            let dense = self.keeps_all();
            if !dense {
                self.select(b, start, end, &mut sel)?;
            }
            let selected = if dense { end - start } else { sel.len() };
            if count_only {
                self.stats.rows_scanned += (end - start) as u64;
                self.stats.rows_matched += selected as u64;
                if selected > 0 {
                    let h = self.groups.hash(&[]);
                    let idx = match self.groups.find(h, &[]) {
                        Some(g) => g,
                        None => {
                            self.mem += 64 + 48 * plan.aggregates.len();
                            let states = plan.aggregates.iter().map(|(f, _)| AggState::new(*f));
                            self.groups.insert(h, &[], states)
                        }
                    };
                    for st in self.groups.states_mut(idx) {
                        if let AggState::Count(n) = st {
                            *n += selected as u64;
                        }
                    }
                }
                start = end;
                continue;
            }
            let mut scanned_to = end;
            let rows = if dense {
                Selected::Dense(start..end)
            } else {
                Selected::Rows(sel.iter())
            };
            for i in rows {
                if self.limit_reached() {
                    // Count rows up to the stopping point, as a row-at-a-time
                    // scan would.
                    scanned_to = i;
                    self.stats.early_stop = true;
                    break;
                }
                let id = match (&self.page, self.pos_base) {
                    (Some(_), _) => b.id(i)?,
                    (None, Some(base)) => (base, i as u32),
                    (None, None) => (0, 0),
                };
                self.stats.rows_matched += 1;
                if self.plan.aggregate {
                    let mut dense_at = None;
                    if let Some(doms) = domains {
                        let mut c = 0usize;
                        let mut inside = true;
                        for (k, d) in key_srcs.iter().zip(doms) {
                            let code = k.code(i);
                            if code >= *d {
                                inside = false; // let the regular path report it
                                break;
                            }
                            c = c * d + code;
                        }
                        if inside {
                            dense_at = Some(c);
                        }
                    }
                    let idx = match dense_at {
                        Some(c) if dense_group[c] != u32::MAX => dense_group[c] as usize,
                        Some(c) => {
                            let g = self.group_index(b, i, key_srcs)?;
                            dense_group[c] =
                                u32::try_from(g).map_err(|_| Error::internal("too many groups"))?;
                            g
                        }
                        None => self.group_index(b, i, key_srcs)?,
                    };
                    if let (Some(c), false) = (dense_at, dense_count.is_empty()) {
                        dense_count[c] += 1;
                        continue;
                    }
                    for (j, (_, arg)) in self.plan.aggregates.iter().enumerate() {
                        let d = match arg {
                            Some(a) => eval_expr(a, b, i)?,
                            None => None,
                        };
                        self.groups.states_mut(idx)[j].update(arg.is_none(), d)?;
                    }
                } else {
                    if let Some(bound) = &self.bound {
                        // Keys first: a row sorting strictly after the
                        // bound has `k` rows ahead of it in any order of
                        // ties, so its outputs are never built.
                        let keys: Vec<Datum> = self
                            .plan
                            .order
                            .iter()
                            .map(|(s, _)| match s {
                                SortSource::Hidden(e) => eval_expr(e, b, i),
                                SortSource::Output(j) => match &self.plan.outputs[*j] {
                                    OutputSource::Row(e) => eval_expr(e, b, i),
                                    _ => Err(Error::internal("non-row output in row query")),
                                },
                            })
                            .collect::<Result<_>>()?;
                        if cmp_position(&self.plan.order, (&keys, id), (&bound.0, bound.1))
                            == std::cmp::Ordering::Greater
                        {
                            self.stats.topk_rows_skipped += 1;
                            continue;
                        }
                    }
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
                    if self.bound.is_none() && self.keep == Some(self.rows.len()) {
                        self.first_bound();
                    }
                    self.maybe_compact();
                }
            }
            self.stats.rows_scanned += (scanned_to - start) as u64;
            if self.stats.early_stop {
                return Ok(());
            }
            start = end;
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
fn cursor_prunes(seg: &dyn ScanSource, plan: &Plan, p: &PageCtx) -> bool {
    let (min_seq, max_seq) = seg.seq_range();
    if min_seq > p.as_of {
        return true;
    }
    let Some((keys, (seq, _))) = &p.after else {
        return false;
    };
    if plan.order.is_empty() {
        return max_seq < *seq;
    }
    if let (true, Some(Some(Value::Timestamp(t)))) = (first_key_is_event_time(plan), keys.first()) {
        let (min_t, max_t) = seg.event_time_range();
        let desc = plan.order[0].1;
        return if desc { min_t > *t } else { max_t < *t };
    }
    false
}

/// What a plan needs from a source. Attribute scopes the plan references
/// only through specific keys are requested per key.
fn column_request<'p>(
    plan: &'p Plan,
    needed: &'p [ColumnId],
    projection: bool,
) -> ColumnRequest<'p> {
    ColumnRequest {
        columns: needed,
        attr_keys: &plan.attr_keys,
        whole_attributes: plan.whole_attributes,
        resource_keys: &plan.resource_keys,
        whole_resource: plan.whole_resource,
        raw_attr_keys: &plan.raw_group_attr_keys,
        raw_resource_keys: &plan.raw_group_resource_keys,
        projection,
    }
}

/// The rows of a chunk that a query keeps (spec STRATA-ARCH-03-006): the
/// whole range when nothing filters, otherwise the selected indices. Rows
/// are never copied or compacted.
enum Selected<'a> {
    Dense(std::ops::Range<usize>),
    Rows(std::slice::Iter<'a, u32>),
}

impl Iterator for Selected<'_> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        match self {
            Selected::Dense(r) => r.next(),
            Selected::Rows(it) => it.next().map(|&i| i as usize),
        }
    }
}

/// Result of scanning one segment in a parallel work item.
struct Partial {
    stats: QueryStats,
    /// A cached partial used instead of `groups` (partial cache hit).
    cached: Option<Arc<crate::partial_cache::CachedPartial>>,
    rows: Vec<Row>,
    /// Groups in first-seen order: (key bytes, key values, states).
    groups: crate::groups::GroupTable<AggState>,
    mem: usize,
    /// The worker's reservation for `rows` and `groups`, handed over with
    /// them.
    acct: Accountant,
}

/// What a cached per-segment partial depends on, besides the segment
/// (spec STRATA-ARCH-09-002; see `partial_cache`).
struct PartialKey {
    /// Tenant, table, `GROUP BY`, aggregates and the non-time conjuncts.
    canon: Arc<str>,
    split: crate::plan::TimeSplit,
}

impl PartialKey {
    /// `Some` for aggregate queries with a partial cache (not EXPLAIN, not
    /// paginated).
    fn of(plan: &Plan, opts: &QueryOptions, paged: bool, tenant: &TenantId) -> Option<Self> {
        opts.partial_cache.as_ref()?;
        if !plan.aggregate || paged || plan.explain != crate::ast::ExplainMode::None {
            return None;
        }
        let split = crate::plan::TimeSplit::of(plan.filter.as_ref());
        let canon = format!(
            "v1|{}|{:?}|{:?}|{:?}|{:?}",
            tenant.as_str(),
            plan.table,
            plan.group_by,
            plan.aggregates,
            split.residual
        );
        Some(PartialKey {
            canon: Arc::from(canon),
            split,
        })
    }
}

/// Everything a scan task needs, owned so tasks can run on the shared pool
/// (`'static`) and outlive the query harmlessly: a late task finds `next`
/// past the end and returns.
struct ScanShared {
    plan: Plan,
    opts: QueryOptions,
    deadline: Instant,
    page: Option<PageCtx>,
    keep: Option<usize>,
    vfilter: Option<crate::vector::Filter>,
    budget: Arc<SharedBudget>,
    hasher: ahash::RandomState,
    segs: Vec<Arc<dyn ScanSource>>,
    needed: Vec<ColumnId>,
    tenant: TenantId,
    /// Default-order positions of `segs` (ordered scans), see `Ctx::pos_base`.
    positions: Option<Vec<u64>>,
    /// The query's partial cache key, when its partials can be cached.
    partial_key: Option<PartialKey>,
    /// Next segment index to claim.
    next: std::sync::atomic::AtomicUsize,
    /// Workers that claimed at least one segment.
    participants: std::sync::atomic::AtomicUsize,
    /// Per-segment results; every claimed index is filled exactly once.
    slots: std::sync::Mutex<(usize, Vec<Option<Result<Partial>>>)>,
    filled: std::sync::Condvar,
}

impl ScanShared {
    /// Every segment has been claimed by some worker.
    fn all_claimed(&self) -> bool {
        self.next.load(AtomicOrdering::Relaxed) >= self.segs.len()
    }

    /// Sleep until a segment result is filled in or `max` passes.
    fn wait_progress(&self, max: Duration) {
        let st = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let before = st.0;
        let _ = self
            .filled
            .wait_timeout_while(st, max, |s| s.0 == before)
            .unwrap_or_else(|e| e.into_inner());
    }

    /// Claim and scan one segment (one morsel). Returns false when no
    /// segment is left or the query stopped. `helper`: running on a pool
    /// worker, whose CPU time is recorded in its partial results (the
    /// calling thread measures its own).
    fn step(&self, helper: bool, first: &mut bool) -> bool {
        {
            if self.budget.stop.load(AtomicOrdering::Relaxed) {
                return false;
            }
            let i = self.next.fetch_add(1, AtomicOrdering::Relaxed);
            if i >= self.segs.len() {
                return false;
            }
            if std::mem::take(first) {
                self.participants.fetch_add(1, AtomicOrdering::Relaxed);
            }
            // From here slot `i` must be filled, whatever happens (a panic
            // fills it through the guard).
            let mut guard = SlotGuard {
                shared: self,
                i,
                done: false,
            };
            let cpu = helper.then(stratadb_common::cpu::thread_cpu_time);
            let mut r = self.scan_one(i);
            if let (Some(c), Ok(p)) = (cpu, r.as_mut()) {
                p.stats.cpu = stratadb_common::cpu::thread_cpu_time().saturating_sub(c);
            }
            if r.is_err() {
                self.budget.stop.store(true, AtomicOrdering::Relaxed);
            }
            guard.fill(r);
        }
        self.next.load(AtomicOrdering::Relaxed) < self.segs.len()
    }

    fn scan_one(&self, i: usize) -> Result<Partial> {
        let seg = &self.segs[i];
        let cacheable = match (&self.opts.partial_cache, &self.partial_key, seg.identity()) {
            (Some(cache), Some(key), Some(uid)) => {
                let time = if key.split.covers(seg.event_time_range()) {
                    None
                } else {
                    Some((key.split.lo, key.split.hi))
                };
                Some((cache, key, time, uid))
            }
            _ => None,
        };
        if let Some((cache, key, time, uid)) = &cacheable {
            // Storage re-checks ownership on every read; a cache hit reads
            // nothing, so check it here.
            if !seg.owned_by(&self.tenant) {
                return Err(Error::internal("segment of another tenant in a scan"));
            }
            if let Some(hit) = cache.get(&key.canon, *time, *uid) {
                let stats = QueryStats {
                    partials_from_cache: 1,
                    rows_from_cache: hit.rows_scanned,
                    ..QueryStats::default()
                };
                return Ok(Partial {
                    stats,
                    mem: hit.bytes,
                    cached: Some(hit),
                    rows: Vec::new(),
                    groups: crate::groups::GroupTable::new(0),
                    acct: Accountant::new(None, DEFAULT_BATCH_BYTES, Duration::ZERO),
                });
            }
        }
        let mut p = Ctx::new(
            &self.plan,
            &self.opts,
            self.deadline,
            self.page.clone(),
            self.keep,
            self.vfilter.clone(),
            Arc::clone(&self.budget),
        );
        p.pos_base = self.positions.as_ref().and_then(|v| v.get(i).copied());
        p.groups =
            crate::groups::GroupTable::with_hasher(self.plan.aggregates.len(), self.hasher.clone());
        p.check_budget()?;
        if p.scan(
            &*self.segs[i],
            &self.needed,
            &self.tenant,
            self.opts.enable_map_projection,
        )? {
            p.stats.segments_scanned += 1;
        }
        p.check_budget()?;
        p.stats.filter_reorders = p.vfilter.as_ref().map_or(0, |f| f.reorders());
        if p.sorted() && p.opts.enable_normalized_sort && !p.rows.is_empty() {
            // Sort this segment's rows here, on the scan worker; the query
            // thread only merges the sorted runs (STRATA-SYS-14-001).
            p.sort_keep();
            p.mem = p.rows.iter().map(row_size).sum();
        }
        let empty = crate::groups::GroupTable::new(0);
        if let Some((cache, key, time, uid)) = cacheable {
            // Partials answered from metadata are cheaper than a cache hit.
            if p.stats.sources_from_metadata == 0 {
                if let Some(frozen) = p.groups.freeze() {
                    cache.put(&key.canon, time, uid, frozen, p.stats.rows_scanned);
                }
            }
        }
        Ok(Partial {
            stats: std::mem::take(&mut p.stats),
            cached: None,
            rows: std::mem::take(&mut p.rows),
            groups: std::mem::replace(&mut p.groups, empty),
            mem: p.mem,
            acct: std::mem::replace(
                &mut p.acct,
                Accountant::new(None, DEFAULT_BATCH_BYTES, Duration::ZERO),
            ),
        })
    }
}

/// Fills a claimed slot exactly once, also when the scan panics.
struct SlotGuard<'s> {
    shared: &'s ScanShared,
    i: usize,
    done: bool,
}

impl SlotGuard<'_> {
    fn fill(&mut self, r: Result<Partial>) {
        let mut st = self.shared.slots.lock().unwrap_or_else(|e| e.into_inner());
        st.1[self.i] = Some(r);
        st.0 += 1;
        self.done = true;
        self.shared.filled.notify_all();
    }
}

impl Drop for SlotGuard<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.shared.budget.stop.store(true, AtomicOrdering::Relaxed);
            self.fill(Err(Error::internal("scan task panicked")));
        }
    }
}

/// Estimated single-core scan cost per row scanned, by query shape.
/// Calibrated on HW-REF-7 (performance cores, one worker; report
/// 2026-11-26-scan-pool): filters 34–49 ns, GROUP BY 48–72 ns, LIKE 63 ns per
/// row. Sorting an aggregate's groups is negligible next to the scan; row
/// queries with ORDER BY pay for sort keys of matched rows. The VIII.12 cost
/// model replaces this.
fn est_ns_per_row(plan: &Plan) -> f64 {
    let mut ns = 30.0;
    if !plan.group_by.is_empty() {
        ns += 40.0;
    }
    if plan.filter.as_ref().is_some_and(contains_like) {
        ns += 30.0;
    }
    if !plan.aggregate && !plan.order.is_empty() {
        ns += 100.0;
    }
    ns
}

fn contains_like(e: &Expr) -> bool {
    let mut found = false;
    e.walk(&mut |x| {
        if matches!(x, Expr::Like { .. }) {
            found = true;
        }
    });
    found
}

/// The query's class cell (`normal` when none was given).
fn query_class(opts: &QueryOptions) -> Arc<stratadb_common::workload::ClassCell> {
    opts.class.clone().unwrap_or_else(|| {
        Arc::new(stratadb_common::workload::ClassCell::new(
            stratadb_common::workload::WorkClass::Normal,
        ))
    })
}

/// The k-th kept row's `event_time` once `k` rows are kept (rows sorted).
fn kth_event_time(ctx: &mut Ctx<'_>, k: usize) -> Option<i64> {
    if ctx.rows.len() < k || k == 0 {
        return None;
    }
    ctx.sort_keep();
    ctx.mem = ctx.rows.iter().map(row_size).sum();
    match ctx.rows.get(k - 1).and_then(|r| r.keys.first()) {
        Some(Some(Value::Timestamp(t))) => Some(*t),
        _ => None,
    }
}

/// `ORDER BY event_time [ASC|DESC] ... LIMIT k` (STRATA-SYS-14-003/004):
/// visit sources (segments, then memtable windows, in the default scan
/// order numbered as positions) best-first by their time range, newest
/// maximum first for `DESC`, oldest minimum first for `ASC`. Segments are
/// read in waves of 1, 2, 4, ... up to `workers` on the pool. Before each source, once
/// `k` rows are kept, a source whose best possible `event_time` is strictly
/// worse than the k-th kept row cannot contribute, and neither can any
/// later one: the scan stops. Rows carry their default-order position, so
/// ties break exactly as in the default scan, and the result is identical.
#[allow(clippy::too_many_arguments)]
fn scan_ordered(
    ctx: &mut Ctx<'_>,
    snapshot: &Snapshot,
    to_scan: &[&Arc<dyn ScanSource>],
    needed: &[ColumnId],
    tenant: &TenantId,
    workers: usize,
    pool: &crate::pool::ScanPool,
) -> Result<()> {
    let plan = ctx.plan;
    let opts = ctx.opts;
    let desc = plan.order.first().is_some_and(|(_, d)| *d);
    let k = ctx.keep.unwrap_or(usize::MAX);
    let mut chunks = Vec::new();
    for chunk in snapshot.memory_chunks(4096) {
        if !chunk.owned_by(tenant) {
            return Err(Error::internal("snapshot contains another tenant's row"));
        }
        if opts.enable_pruning && chunk.prune(&plan.pruning).is_some() {
            ctx.stats.memory_chunks_pruned += 1;
            continue;
        }
        chunks.push(chunk);
    }
    // (position, time range, segment index or memtable chunk index)
    enum Src {
        Seg(usize),
        Mem(usize),
    }
    let n = to_scan.len();
    let mut order: Vec<(u64, (i64, i64), Src)> = to_scan
        .iter()
        .enumerate()
        .map(|(i, s)| (i as u64, s.event_time_range(), Src::Seg(i)))
        .chain(
            chunks
                .iter()
                .enumerate()
                .map(|(j, c)| ((n + j) as u64, c.event_time_range(), Src::Mem(j))),
        )
        .collect();
    if desc {
        order.sort_by(|a, b| b.1 .1.cmp(&a.1 .1).then(a.0.cmp(&b.0)));
    } else {
        order.sort_by(|a, b| a.1 .0.cmp(&b.1 .0).then(a.0.cmp(&b.0)));
    }
    let hopeless = |range: (i64, i64), kth: i64| {
        if desc {
            range.1 < kth
        } else {
            range.0 > kth
        }
    };
    let mut kth = None;
    let mut i = 0;
    let mut used_workers = 0usize;
    // Waves grow 1, 2, 4, ... up to `workers`: the first sources usually
    // settle the k-th row, and harder cases still reach full parallelism.
    let mut wave_cap = 1usize;
    while i < order.len() {
        ctx.check_budget()?;
        if kth.is_some_and(|t| hopeless(order[i].1, t)) {
            ctx.stats.sources_skipped_by_order += (order.len() - i) as u64;
            ctx.stats.early_stop = true;
            break;
        }
        match order[i].2 {
            Src::Mem(j) => {
                ctx.pos_base = Some(order[i].0);
                let c = &chunks[j];
                if ctx.scan(c, needed, tenant, false)? {
                    ctx.stats.memory_rows_scanned += c.rows();
                }
                ctx.pos_base = None;
                used_workers = used_workers.max(1);
                i += 1;
            }
            Src::Seg(_) => {
                // A wave of consecutive segments still worth reading.
                let mut wave: Vec<&Arc<dyn ScanSource>> = Vec::new();
                let mut pos: Vec<u64> = Vec::new();
                while i < order.len() && wave.len() < wave_cap.min(workers.max(1)) {
                    let Src::Seg(si) = order[i].2 else { break };
                    if kth.is_some_and(|t| hopeless(order[i].1, t)) {
                        break;
                    }
                    wave.push(to_scan[si]);
                    pos.push(order[i].0);
                    i += 1;
                }
                if wave.len() > 1 {
                    scan_parallel(ctx, &wave, needed, tenant, wave.len(), pool, Some(&pos))?;
                    used_workers = used_workers.max(ctx.stats.scan_workers);
                } else {
                    for (s, p) in wave.iter().zip(&pos) {
                        ctx.pos_base = Some(*p);
                        if ctx.scan(&***s, needed, tenant, opts.enable_map_projection)? {
                            ctx.stats.segments_scanned += 1;
                        }
                        ctx.pos_base = None;
                    }
                    used_workers = used_workers.max(1);
                }
                wave_cap = wave_cap.saturating_mul(2);
            }
        }
        kth = kth_event_time(ctx, k).or(kth);
    }
    ctx.stats.scan_workers = used_workers;
    Ok(())
}

/// Per-worker CPU target for interactive queries (STRATA-SYS-11-008).
pub const TARGET_MS_PER_WORKER: f64 = 5.0;

/// Below this estimated CPU time a query never uses more than
/// [`SMALL_QUERY_MAX_WORKERS`] workers (STRATA-SYS-01-006).
pub const SMALL_QUERY_MS: f64 = 2.0;
pub const SMALL_QUERY_MAX_WORKERS: usize = 4;

/// Degree of parallelism for scanning `segs`, given `idle_workers` (pool
/// workers not running or holding queued tasks).
///
/// * Base (STRATA-SYS-11-008): `ceil(est_cpu_ms / target_ms_per_worker)`,
///   clamped to `1..=min(parallelism, segments)`. This is what a query gets
///   when the pool is busy, so concurrent queries share workers instead of
///   each fanning out.
/// * When workers are idle, a query may also use them, up to
///   `min(parallelism, segments)`. Queries under 2 ms of estimated work stay
///   at no more than 4 workers (STRATA-SYS-01-006). On this engine fan-out
///   costs little extra CPU and cuts latency 2–4× for 1–5 ms queries, which
///   the base formula alone would scan on one worker (report
///   2026-11-26-scan-pool; ADR-0006).
///
/// `target_ms_per_worker = 0` always uses `min(parallelism, segments)`.
pub(crate) fn scan_dop(
    plan: &Plan,
    segs: &[&Arc<dyn ScanSource>],
    opts: &QueryOptions,
    idle_workers: usize,
) -> (usize, f64) {
    let rows: u64 = segs.iter().map(|s| s.rows()).sum();
    let est_ms = rows as f64 * est_ns_per_row(plan) / 1e6;
    let max = opts.parallelism.max(1).min(segs.len().max(1));
    if opts.target_ms_per_worker <= 0.0 {
        return (max, est_ms);
    }
    let base = ((est_ms / opts.target_ms_per_worker).ceil() as usize).clamp(1, max);
    let cap = if est_ms < SMALL_QUERY_MS {
        SMALL_QUERY_MAX_WORKERS.min(max)
    } else {
        max
    };
    let idle = (1 + idle_workers).min(cap);
    (base.max(idle).min(max), est_ms)
}

/// Scan `segs` with up to `workers` workers: this thread plus up to
/// `workers - 1` tasks on the shared pool (fewer when the pool is busy;
/// STRATA-SYS-11-008). One partial result per segment is merged into `ctx`
/// **in segment order**, so result rows (after a stable sort) and group
/// order are exactly those of a sequential scan. The first failing segment
/// (lowest index) determines the error.
fn scan_parallel(
    ctx: &mut Ctx<'_>,
    segs: &[&Arc<dyn ScanSource>],
    needed: &[ColumnId],
    tenant: &TenantId,
    workers: usize,
    pool: &crate::pool::ScanPool,
    positions: Option<&[u64]>,
) -> Result<()> {
    let n = segs.len();
    let shared = Arc::new(ScanShared {
        partial_key: PartialKey::of(ctx.plan, ctx.opts, ctx.page.is_some(), tenant),
        positions: positions.map(<[u64]>::to_vec),
        plan: ctx.plan.clone(),
        opts: ctx.opts.clone(),
        deadline: ctx.deadline,
        page: ctx.page.clone(),
        keep: ctx.keep,
        vfilter: ctx.vfilter.clone(),
        budget: Arc::clone(&ctx.budget),
        hasher: ctx.groups.hasher(),
        segs: segs.iter().map(|s| Arc::clone(*s)).collect(),
        needed: needed.to_vec(),
        tenant: tenant.clone(),
        next: std::sync::atomic::AtomicUsize::new(0),
        participants: std::sync::atomic::AtomicUsize::new(0),
        slots: std::sync::Mutex::new((0, (0..n).map(|_| None).collect())),
        filled: std::sync::Condvar::new(),
    });
    let tag = crate::pool::Tag {
        class: query_class(ctx.opts),
        tenant: crate::pool::tenant_key(tenant),
        est_step_ns: (ctx.stats.est_cpu_ms * 1e6 / n.max(1) as f64).max(1e4),
    };
    let helpers = workers
        .saturating_sub(1)
        .min(pool.free_slots_for(tag.class.get()));
    let mut queued = 0;
    for _ in 0..helpers {
        let s = Arc::clone(&shared);
        let mut first = true;
        let task: crate::pool::Task = Box::new(move || {
            if s.step(true, &mut first) {
                crate::pool::Step::Again
            } else {
                crate::pool::Step::Done
            }
        });
        match pool.try_submit_task(tag.clone(), task) {
            Ok(()) => queued += 1,
            Err(_) => break, // saturated: this thread does the rest
        }
    }
    // This thread's share, preempted at its morsel boundaries by
    // higher-class work (spec VIII.11.1):
    // - a waiting higher-class morsel runs here first (CPU lent);
    // - while higher-class work is busy in the pool and this query's own
    //   helper tasks hold its scan, this thread sleeps instead of scanning,
    //   so the rest of the scan runs at its class's weighted share of the
    //   pool. A query without helpers always scans itself (progress).
    let mut first = true;
    let mut lent = Duration::ZERO;
    loop {
        let class = tag.class.get();
        if let Some(d) = pool.help_higher(class) {
            lent += d;
            continue;
        }
        if queued > 0 && !shared.all_claimed() && pool.higher_busy(class) {
            ctx.stats.scan_yields += 1;
            shared.wait_progress(Duration::from_millis(2));
            continue;
        }
        if !shared.step(false, &mut first) {
            break;
        }
    }
    ctx.stats.cpu_lent += lent;
    // No index is claimed after this; every claimed index gets filled.
    let claimed = shared.next.fetch_max(n, AtomicOrdering::Relaxed).min(n);
    let results: Vec<Option<Result<Partial>>> = {
        let mut st = shared.slots.lock().unwrap_or_else(|e| e.into_inner());
        while st.0 < claimed {
            st = shared.filled.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        std::mem::take(&mut st.1)
    };
    ctx.stats.scan_workers = shared.participants.load(AtomicOrdering::Relaxed).max(1);
    ctx.stats.scan_tasks_queued = queued;
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
    let merge = ctx.sorted() && ctx.opts.enable_normalized_sort;
    let mut runs: Vec<Vec<Row>> = Vec::new();
    if merge && !ctx.rows.is_empty() {
        // Rows already held come first in the concatenation order.
        ctx.sort_keep();
        runs.push(std::mem::take(&mut ctx.rows));
    }
    for p in results.into_iter().flatten() {
        let p = p?;
        let s = &mut ctx.stats;
        s.segments_scanned += p.stats.segments_scanned;
        s.sources_from_metadata += p.stats.sources_from_metadata;
        s.dense_group_batches += p.stats.dense_group_batches;
        s.filter_reorders += p.stats.filter_reorders;
        s.column_cache_hits += p.stats.column_cache_hits;
        s.topk_rows_skipped += p.stats.topk_rows_skipped;
        s.partials_from_cache += p.stats.partials_from_cache;
        s.rows_from_cache += p.stats.rows_from_cache;
        s.rows_scanned += p.stats.rows_scanned;
        s.rows_matched += p.stats.rows_matched;
        s.bytes_read += p.stats.bytes_read;
        s.shredded_reads += p.stats.shredded_reads;
        s.cpu += p.stats.cpu;
        s.peak_memory_bytes = s.peak_memory_bytes.max(p.stats.peak_memory_bytes);
        ctx.mem += p.mem;
        ctx.acct.absorb(p.acct);
        if merge {
            if !p.rows.is_empty() {
                runs.push(p.rows);
            }
        } else {
            ctx.rows.extend(p.rows);
            ctx.maybe_compact();
        }
        // Partial groups in their first-seen order → global first-seen order.
        match &p.cached {
            Some(c) => c.groups.merge_into(&mut ctx.groups, |a, b| a.merge(b))?,
            None => p.groups.merge_into(&mut ctx.groups, |a, b| a.merge(&b))?,
        }
    }
    if merge && !runs.is_empty() {
        // Row queries only (aggregates hold groups, not rows).
        ctx.stats.sort_runs_merged += runs.len() as u64;
        let order = &ctx.plan.order;
        ctx.rows = crate::sort::merge_runs(runs, ctx.keep, |a, b| {
            cmp_position(order, (&a.keys, a.id), (&b.keys, b.id))
        });
        ctx.sorted_rows = ctx.rows.len();
        ctx.mem = ctx.rows.iter().map(row_size).sum();
        ctx.note_bound();
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
        Arc::new(SharedBudget::new(opts)),
    );
    ctx.stats.table = plan.table.name();
    ctx.meta = MetaPlan::of(plan, opts, ctx.page.is_some());
    let tenant = snapshot.tenant();
    let sources = snapshot.sources();
    let mut to_scan = Vec::new();
    for seg in &sources {
        ctx.stats.segments_total += 1;
        if !seg.owned_by(tenant) {
            return Err(Error::internal(
                "snapshot contains another tenant's segment",
            ));
        }
        if ctx
            .page
            .as_ref()
            .is_some_and(|p| cursor_prunes(&**seg, plan, p))
        {
            ctx.stats.segments_pruned_cursor += 1;
            continue;
        }
        let why = opts
            .enable_pruning
            .then(|| seg.prune(&plan.pruning))
            .flatten();
        match why {
            Some(Pruned::Time) => ctx.stats.segments_pruned_time += 1,
            Some(Pruned::Keys) => ctx.stats.segments_pruned_keys += 1,
            Some(Pruned::Severity) => ctx.stats.segments_pruned_severity += 1,
            Some(Pruned::Equals(_)) => ctx.stats.segments_pruned_bloom += 1,
            Some(Pruned::Like(_)) => ctx.stats.segments_pruned_ngram += 1,
            None => to_scan.push(seg),
        }
        if let Some(name) = why.and_then(|w| plan.pruning.index_name(w)) {
            if !ctx.stats.indexes_used.contains(&name) {
                ctx.stats.indexes_used.push(name);
            }
        }
    }
    // Segments the metadata answers are never read (STRATA-SYS-09-006/007).
    if ctx.meta.is_some() {
        let mut rest = Vec::with_capacity(to_scan.len());
        for seg in to_scan {
            if !ctx.answer_from_metadata(&**seg)? {
                rest.push(seg);
            }
        }
        to_scan = rest;
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
    let pool = crate::pool::scan_pool();
    // Count this query as executing in its class while it scans, so lower
    // classes yield to it at their morsel boundaries (spec VIII.11.1).
    let _executing = pool.enter(query_class(opts));
    let idle = pool.free_slots().saturating_sub(pool.workers());
    let (workers, est_ms) = scan_dop(plan, &to_scan, opts, idle);
    ctx.stats.est_cpu_ms = est_ms;
    let ordered = opts.enable_ordered_scan
        && !plan.aggregate
        && ctx.page.is_none()
        && ctx.keep.is_some()
        && first_key_is_event_time(plan);
    if ordered {
        scan_ordered(&mut ctx, snapshot, &to_scan, &needed, tenant, workers, pool)?;
    } else if (workers > 1 && (plan.aggregate || ctx.sorted()))
        || (plan.aggregate && opts.partial_cache.is_some() && !to_scan.is_empty())
    {
        scan_parallel(&mut ctx, &to_scan, &needed, tenant, workers, pool, None)?;
    } else {
        if !to_scan.is_empty() {
            ctx.stats.scan_workers = 1;
        }
        for seg in to_scan {
            if ctx.limit_reached() {
                ctx.stats.early_stop = true;
                break;
            }
            ctx.check_budget()?;
            if ctx.scan(&**seg, &needed, tenant, opts.enable_map_projection)? {
                ctx.stats.segments_scanned += 1;
            }
        }
    }
    for chunk in snapshot.memory_chunks(4096).filter(|_| !ordered) {
        if ctx.limit_reached() {
            ctx.stats.early_stop = true;
            break;
        }
        ctx.check_budget()?;
        if !chunk.owned_by(tenant) {
            return Err(Error::internal("snapshot contains another tenant's row"));
        }
        if opts.enable_pruning && chunk.prune(&plan.pruning).is_some() {
            ctx.stats.memory_chunks_pruned += 1;
            continue;
        }
        if ctx.scan(&chunk, &needed, tenant, false)? {
            ctx.stats.memory_rows_scanned += chunk.rows();
        }
    }
    ctx.check_budget()?;
    ctx.stats.filter_reorders += ctx.vfilter.as_ref().map_or(0, |f| f.reorders());

    if plan.aggregate {
        if ctx.groups.is_empty() && plan.group_by.is_empty() {
            let states = plan.aggregates.iter().map(|(f, _)| AggState::new(*f));
            let h = ctx.groups.hash(&[]);
            ctx.groups.insert(h, &[], states);
        }
        let t = ctx.groups.stats();
        ctx.stats.group_table = Some(HashTableStats {
            variant: if ctx.stats.dense_group_batches > 0 {
                "dense codes over serialized-key SwissTable"
            } else {
                "serialized-key SwissTable"
            },
            groups: t.groups,
            capacity: t.capacity,
            resizes: t.resizes,
            bytes: t.bytes,
            dense_batches: ctx.stats.dense_group_batches,
        });
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
    ctx.stats.memory_reserved_peak = ctx.budget.tracker.peak();
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
    let _ = writeln!(
        t,
        "  memtable rows: {}, chunk windows pruned: {}",
        snap.memory_row_count(),
        s.memory_chunks_pruned
    );
    if s.sources_from_metadata > 0 {
        let _ = writeln!(
            t,
            "  answered from metadata (no columns read): {} source(s)",
            s.sources_from_metadata
        );
    }
    if p.has_index_candidates() {
        let _ = writeln!(
            t,
            "  index candidates: [{}]",
            p.index_candidates().join(", ")
        );
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
        let _ = writeln!(
            t,
            "Parallelism: estimated scan CPU {:.3} ms, {} scan worker(s), {} pool task(s) requested",
            s.est_cpu_ms, s.scan_workers, s.scan_tasks_queued
        );
        if s.sources_skipped_by_order > 0 {
            let _ = writeln!(
                t,
                "Ordered scan: {} source(s) skipped (their time range cannot beat the k-th row)",
                s.sources_skipped_by_order
            );
        }
        if s.sort_runs_merged > 0 || s.topk_rows_skipped > 0 {
            let _ = writeln!(
                t,
                "Sort: {} sorted segment run(s) merged, {} row(s) skipped by the top-k bound",
                s.sort_runs_merged, s.topk_rows_skipped
            );
        }
        if s.filter_reorders > 0 {
            let _ = writeln!(
                t,
                "Filter: conjunction order adapted {} time(s) from observed cost and selectivity",
                s.filter_reorders
            );
        }
        if let Some(h) = &s.group_table {
            let _ = writeln!(
                t,
                "Hash table: {}, {} group(s), capacity {}, {} resize(s), ~{} B, {} dense batch(es), 0 spilled partitions",
                h.variant, h.groups, h.capacity, h.resizes, h.bytes, h.dense_batches
            );
        }
    }
    t
}

#[cfg(test)]
mod parallel_budget_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use stratadb_schema::Event;
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
            // Scan every segment (metadata would answer this count).
            enable_metadata_aggregates: false,
            ..QueryOptions::default()
        };
        let plan = Plan::new(
            crate::parser::parse("SELECT count(*) FROM logs", &opts.parse_limits).unwrap(),
        )
        .unwrap();
        let budget = Arc::new(SharedBudget::new(&opts));
        let mut ctx = Ctx::new(
            &plan,
            &opts,
            Instant::now() + opts.timeout,
            None,
            None,
            None,
            Arc::clone(&budget),
        );
        let srcs = snap.sources();
        let segs: Vec<&Arc<dyn ScanSource>> = srcs.iter().collect();
        assert_eq!(segs.len(), 40);
        let needed: Vec<ColumnId> = plan.columns.iter().copied().collect();
        let err = scan_parallel(
            &mut ctx,
            &segs,
            &needed,
            &t,
            4,
            crate::pool::scan_pool(),
            None,
        )
        .unwrap_err();
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
mod pool_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use stratadb_schema::Event;
    use stratadb_storage::{Durability, Engine, EngineOptions};

    fn engine(dir: &std::path::Path, segments: i64, rows: i64) -> (Engine, TenantId) {
        let t = TenantId::new("p").unwrap();
        let mut o = EngineOptions::new(dir);
        o.background = false;
        o.wal.durability = Durability::Async;
        let e = Engine::open(o).unwrap();
        for s in 0..segments {
            let evs: Vec<Event> = (0..rows)
                .map(|j| {
                    let mut ev = Event::new(t.clone(), 1_795_170_600_000_000_000 + s * rows + j, 1);
                    ev.service = Some(format!("svc{}", j % 7));
                    ev
                })
                .collect();
            e.write_batch(&t, evs).unwrap();
            e.flush().unwrap();
        }
        (e, t)
    }

    fn plan(sql: &str) -> Plan {
        Plan::new(crate::parser::parse(sql, &Default::default()).unwrap()).unwrap()
    }

    #[test]
    fn cpu_time_includes_pool_workers_and_tables_resolve_through_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let (e, t) = engine(dir.path(), 8, 4000);
        let snap = e.snapshot(&t);
        let sql = "SELECT service, count(*) FROM logs GROUP BY service ORDER BY service";
        let seq = execute(
            &snap,
            sql,
            &QueryOptions {
                parallelism: 1,
                ..QueryOptions::default()
            },
        )
        .unwrap();
        let par = execute(
            &snap,
            sql,
            &QueryOptions {
                parallelism: 4,
                target_ms_per_worker: 0.0,
                ..QueryOptions::default()
            },
        )
        .unwrap();
        assert_eq!(seq.rows, par.rows);
        assert_eq!(seq.stats.table, "logs");
        assert!(seq.stats.cpu > Duration::ZERO);
        // Work done by pool helpers is counted: the same scan costs about the
        // same CPU however it is split. Without helper accounting, a 4-way
        // scan would report about a quarter of it.
        assert!(par.stats.scan_workers >= 1);
        assert!(
            par.stats.cpu.as_secs_f64() >= 0.4 * seq.stats.cpu.as_secs_f64(),
            "parallel {:?} vs sequential {:?} (workers {})",
            par.stats.cpu,
            seq.stats.cpu,
            par.stats.scan_workers
        );
        // A bare snapshot only holds `logs`.
        let err = execute(
            &snap,
            "SELECT count(*) FROM system.query_log",
            &QueryOptions::default(),
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported, "{err}");
        // A catalog must not hand out another tenant's snapshot.
        struct Wrong<'a>(&'a Snapshot, TenantId);
        impl Catalog for Wrong<'_> {
            fn tenant(&self) -> &TenantId {
                &self.1
            }
            fn snapshot(&self, _: Table) -> Result<std::borrow::Cow<'_, Snapshot>> {
                Ok(std::borrow::Cow::Borrowed(self.0))
            }
        }
        let other = TenantId::new("other").unwrap();
        let err = execute(&Wrong(&snap, other), sql, &QueryOptions::default()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Internal, "{err}");
    }

    #[test]
    fn degree_of_parallelism_follows_estimated_work_and_idle_capacity() {
        let dir = tempfile::tempdir().unwrap();
        let (e, t) = engine(dir.path(), 8, 1000);
        let snap = e.snapshot(&t);
        let srcs = snap.sources();
        let segs: Vec<&Arc<dyn ScanSource>> = srcs.iter().collect();
        let opts = |par: usize, target: f64| QueryOptions {
            parallelism: par,
            target_ms_per_worker: target,
            ..QueryOptions::default()
        };
        let count = plan("SELECT count(*) FROM logs");
        let group = plan("SELECT service, count(*) AS n FROM logs GROUP BY service ORDER BY n");
        let rows = plan("SELECT event_id FROM logs ORDER BY event_time");
        // Shape-dependent cost per row: group-by > filter; row sort > both.
        let est = |p: &Plan| scan_dop(p, &segs, &opts(8, 5.0), 0).1;
        assert!(est(&count) < est(&group) && est(&group) < est(&rows));
        // Busy pool (no idle workers): the base formula. 8,000 rows is far
        // below 5 ms, so one worker.
        assert!(est(&count) < SMALL_QUERY_MS);
        assert_eq!(scan_dop(&count, &segs, &opts(8, 5.0), 0).0, 1);
        // ceil(est / target), capped by parallelism and segment count.
        let e_rows = est(&rows);
        assert_eq!(scan_dop(&rows, &segs, &opts(8, e_rows / 3.0), 0).0, 3);
        assert_eq!(
            scan_dop(&rows, &segs, &opts(2, e_rows / 3.0), 0).0,
            2,
            "parallelism caps"
        );
        assert_eq!(
            scan_dop(&rows, &segs[..2], &opts(8, e_rows / 9.0), 0).0,
            2,
            "segments cap"
        );
        // Idle workers: fan out, but a < 2 ms query never beyond 4
        // (STRATA-SYS-01-006) ...
        assert_eq!(
            scan_dop(&count, &segs, &opts(8, 5.0), 100).0,
            SMALL_QUERY_MAX_WORKERS
        );
        assert_eq!(
            scan_dop(&count, &segs, &opts(8, 5.0), 1).0,
            2,
            "only as many as are idle"
        );
        // ... while a query above 2 ms may use every idle worker up to the
        // caps (24,000 rows of a sorted row query).
        let dir2 = tempfile::tempdir().unwrap();
        let (e2, t2) = engine(dir2.path(), 8, 3000);
        let snap2 = e2.snapshot(&t2);
        let srcs2 = snap2.sources();
        let segs2: Vec<&Arc<dyn ScanSource>> = srcs2.iter().collect();
        let (d, est2) = scan_dop(&rows, &segs2, &opts(8, 5.0), 100);
        assert!(est2 >= SMALL_QUERY_MS, "{est2}");
        assert_eq!(d, 8);
        assert_eq!(
            scan_dop(&rows, &segs2, &opts(8, 5.0), 0).0,
            1,
            "busy: base formula"
        );
        // 0 = no estimate: always min(parallelism, segments).
        assert_eq!(scan_dop(&count, &segs, &opts(6, 0.0), 0).0, 6);
        assert_eq!(scan_dop(&count, &[], &opts(6, 5.0), 100).0, 1);
        // End to end: reported in stats.
        let scan_all = QueryOptions {
            enable_metadata_aggregates: false,
            ..QueryOptions::default()
        };
        let out = execute(&snap, "SELECT count(*) FROM logs", &scan_all).unwrap();
        assert!((1..=SMALL_QUERY_MAX_WORKERS).contains(&out.stats.scan_workers));
        assert!(out.stats.est_cpu_ms > 0.0);
    }

    #[test]
    fn saturated_pool_degrades_to_caller_runs_without_deadlock() {
        let dir = tempfile::tempdir().unwrap();
        let (e, t) = engine(dir.path(), 12, 500);
        let snap = e.snapshot(&t);
        let opts = QueryOptions {
            parallelism: 8,
            target_ms_per_worker: 0.0,
            ..QueryOptions::default()
        };
        let p = plan("SELECT service, count(*) FROM logs GROUP BY service");
        // A private pool (the global one is shared with concurrent tests),
        // saturated with blocked tasks: 1 running + 1 queued for 1 worker.
        let pool = crate::pool::ScanPool::start(1);
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let deadline = Instant::now() + Duration::from_secs(10);
        // Saturated for the scan's class (`normal`: 3/4 of 2 × workers).
        while pool.free_slots_for(stratadb_common::workload::WorkClass::Normal) > 0 {
            assert!(Instant::now() < deadline, "pool never saturated");
            let g = Arc::clone(&gate);
            let _ = pool.try_submit(Box::new(move || {
                let (m, cv) = &*g;
                let mut open = m.lock().unwrap();
                while !*open {
                    open = cv.wait(open).unwrap();
                }
            }));
        }
        let srcs = snap.sources();
        let segs: Vec<&Arc<dyn ScanSource>> = srcs.iter().collect();
        let needed: Vec<ColumnId> = p.columns.iter().copied().collect();
        let budget = Arc::new(SharedBudget::new(&opts));
        let mut ctx = Ctx::new(
            &p,
            &opts,
            Instant::now() + opts.timeout,
            None,
            None,
            None,
            budget,
        );
        // The scan completes on this thread alone: no wait on the pool.
        scan_parallel(&mut ctx, &segs, &needed, &t, 8, &pool, None).unwrap();
        assert_eq!(ctx.stats.scan_workers, 1);
        assert_eq!(ctx.stats.scan_tasks_queued, 0);
        assert_eq!(ctx.stats.segments_scanned, 12);
        assert_eq!(ctx.stats.rows_scanned, 12 * 500);
        assert_eq!(ctx.groups.len(), 7, "every service group");
        let (m, cv) = &*gate;
        *m.lock().unwrap() = true;
        cv.notify_all();
    }

    #[test]
    fn concurrent_queries_share_a_fixed_set_of_threads() {
        let dir = tempfile::tempdir().unwrap();
        let (e, t) = engine(dir.path(), 10, 300);
        let e = Arc::new(e);
        let workers = crate::pool::scan_pool().workers();
        let attempts = |s: crate::pool::PoolStats| s.submitted + s.rejected;
        let before = attempts(crate::pool::scan_pool().stats());
        let opts = QueryOptions {
            parallelism: 8,
            target_ms_per_worker: 0.0,
            ..QueryOptions::default()
        };
        let sql = "SELECT service, count(*) FROM logs GROUP BY service ORDER BY service";
        let want = execute(&e.snapshot(&t), sql, &opts).unwrap().rows;
        std::thread::scope(|s| {
            for _ in 0..8 {
                let (e, t, opts, want) = (&e, &t, &opts, &want);
                s.spawn(move || {
                    for _ in 0..25 {
                        let out = execute(&e.snapshot(t), sql, opts).unwrap();
                        assert_eq!(&out.rows, want);
                    }
                });
            }
        });
        let st = crate::pool::scan_pool().stats();
        assert_eq!(st.workers, workers, "no threads created per query");
        // Tasks were offered to the pool (another test may hold it saturated
        // for a moment, in which case the offers were declined).
        assert!(attempts(st) > before, "pool used");
        assert_eq!(st.panicked, 0);
    }
}

#[cfg(test)]
mod sort_keep_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;

    /// Sort kernel timing, comparison vs normalized keys. Run with `cargo
    /// test --release -p stratadb-query --lib sort_kernel_ab -- --ignored
    /// --nocapture`.
    #[test]
    #[ignore = "timing measurement"]
    fn sort_kernel_ab() {
        let mut s = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        type Gen<'a> = &'a mut dyn FnMut(&mut dyn FnMut() -> u64) -> Vec<Datum>;
        let mut ts =
            |r: &mut dyn FnMut() -> u64| vec![Some(Value::Timestamp((r() % 3_600_000_000) as i64))];
        let mut st = |r: &mut dyn FnMut() -> u64| {
            vec![Some(Value::String(format!(
                "GET /api/v1/items/{}",
                r() % 100_000
            )))]
        };
        let mut two = |r: &mut dyn FnMut() -> u64| {
            let x = r();
            vec![
                Some(Value::String(format!("svc-{:02}", x % 40))),
                Some(Value::Timestamp((x >> 8) as i64 % 3_600_000)),
            ]
        };
        let mut int = |r: &mut dyn FnMut() -> u64| vec![Some(Value::Int((r() % 10_000) as i64))];
        let cases: [(&str, Gen<'_>, Vec<bool>); 4] = [
            ("timestamp", &mut ts, vec![false]),
            ("string", &mut st, vec![true]),
            ("str+time", &mut two, vec![false, true]),
            ("int", &mut int, vec![false]),
        ];
        for (name, gen, desc) in cases {
            for n in [2_000usize, 12_000, 100_000] {
                let rows: Vec<Row> = (0..n)
                    .map(|i| Row {
                        out: vec![Some(Value::UInt(i as u64))],
                        keys: gen(&mut next),
                        id: (0, 0),
                    })
                    .collect();
                let order: Vec<(SortSource, bool)> = desc
                    .iter()
                    .enumerate()
                    .map(|(j, d)| (SortSource::Output(j), *d))
                    .collect();
                for keep in [None, Some(100), Some(n / 8), Some(n / 2)] {
                    let mut t = [Vec::new(), Vec::new()];
                    for round in 0..11 {
                        for k in 0..2 {
                            let normalized = (round + k) % 2 == 0;
                            let mut v = rows.clone();
                            let started = std::time::Instant::now();
                            sort_keep(&mut v, &order, keep, normalized);
                            t[usize::from(normalized)]
                                .push(started.elapsed().as_secs_f64() * 1e6 / n as f64);
                            std::hint::black_box(v.len());
                        }
                    }
                    for v in &mut t {
                        v.sort_by(f64::total_cmp);
                    }
                    println!(
                        "{name:9} n={n:6} keep={keep:?}: cmp {:6.1} ns/row, normalized {:6.1} ns/row ({:.2}x)",
                        t[0][5] * 1e3,
                        t[1][5] * 1e3,
                        t[0][5] / t[1][5]
                    );
                }
            }
        }
    }

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
            sort_keep(&mut got, &order, keep, false);
            let ids = |v: &[Row]| v.iter().map(|r| r.out.clone()).collect::<Vec<_>>();
            prop_assert_eq!(ids(&got), ids(&want));
            let mut norm = make();
            sort_keep(&mut norm, &order, keep, true);
            prop_assert_eq!(ids(&norm), ids(&want));
        }
    }
}
