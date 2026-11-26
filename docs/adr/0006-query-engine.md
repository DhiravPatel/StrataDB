# ADR-0006: Query engine integration

- **Status:** Accepted (provisional)
- **Spec references:** §13

## Options
1. Apache DataFusion over Arrow record batches.
2. Custom engine over StrataDB column data.

## Decision
Start with a **custom, deliberately small** engine (`stratadb-query`) for the
spec's limited SQL subset, because: (a) tenant scoping, budgets, and EXPLAIN of
StrataDB-specific pruning must be first-class and are simpler to guarantee in
code we own; (b) DataFusion + Arrow add a very large dependency and build
footprint (critical on the 27 GB-free dev machine); (c) the spec forbids
exposing SQL we do not implement correctly — a narrow grammar makes this easy.

The engine is structured as parser → logical plan → (optimizer) → executor
over a `Snapshot`, so a DataFusion-backed executor could replace the executor
stage later.

### Update 2026-11-25: vectorized filters
`WHERE` clauses are compiled once per query into typed column kernels over
selection vectors (`crates/stratadb-query/src/vector.rs`); unsupported shapes
fall back to the scalar evaluator per row. Equality with the scalar path is
property-tested row for row; `QueryOptions::enable_vectorized` disables it.
Evidence: `benchmarks/reports/2026-11-25-vectorized-ab.md` (1.2–3.5× on the
filtered queries of the suite). Projection, grouping and sort keys are still
evaluated per matched row.

### Update 2026-11-25: allocation-free grouping
Group keys are encoded from column data by reference into a reused buffer and
looked up by slice; owned key values are built only for new groups. Byte
equality with `group_key_bytes` is property-tested. Evidence:
`benchmarks/reports/2026-11-25-grouping.md` (1.3–3.0×). Remaining: whole-map
decoding for non-shredded keys (needs late materialization), parallel segment
scans with partial aggregation.

### Update 2026-11-25: parallel segment scans
Sorted and aggregate queries scan segments on up to `QueryOptions::parallelism`
(default min(CPUs, 8)) scoped threads, one partial result per segment, merged
in segment order (rows then stable-sorted; groups merged in first-seen order;
`AggState::merge`). Row/memory budgets are query-wide (shared atomics) and
workers stop as soon as they are exceeded or another worker fails. Unsorted
`LIMIT` queries stay sequential (early stop). Float `sum`/`avg` may differ from
a sequential scan in the last bits (addition order), deterministically.
Evidence: `benchmarks/reports/2026-11-25-parallel-ab.md` (2.2–2.6× with 3
segments). `parallelism = 1` disables it.

### Update 2026-11-25: attribute key projection
Queries that reference specific attribute keys read only those keys per
segment (shredded chunks, or a projection of the residual map chunk that skips
other values without allocating). Query 10 of the suite: 22× faster, 26× fewer
segment bytes (`benchmarks/reports/2026-11-25-map-projection-ab.md`).
`QueryOptions::enable_map_projection` disables it.

### Update 2026-11-25: flat aggregation groups
Groups live in a flat `GroupTable` (hashbrown `HashTable` of indices, key-byte
arena, flat key/state arrays) hashed with aHash (random per-table seed).
High-cardinality `GROUP BY` uses 2.2× less CPU; low-cardinality unchanged
(`benchmarks/reports/2026-11-25-grouping-flat.md`). The remaining dominant cost
for high-cardinality string keys is decoding each value into an owned string.

### Update 2026-11-25: undecoded group keys and top-k
- **Raw group keys.** A shredded attribute key that the query uses only as a
  whole `GROUP BY` key (`Plan::raw_group_attr_keys`) is read as `RawValues`:
  encoded bytes plus spans, validated with `skip_value` so a chunk is accepted
  or rejected exactly as decoding would. Its bytes are copied into the group
  key.
- **Key bytes only.** Group tables store only key bytes. The key encoding is
  bit-exact, so values are decoded once per output group.
- **Shared hash seed.** Partials share the query's hash seed, so the merge
  reuses hashes and moves states.
- **Top-k selection.** `ORDER BY … LIMIT k` uses top-k selection, which is
  exactly equal to a stable sort followed by truncation. Aggregate queries
  ordered only by aggregates are ranked before any rows are built.
- **Result.** Query 08 uses 2.95× less CPU
  (`benchmarks/reports/2026-11-25-raw-group-keys.md`). No persistent format
  changed.

### Update 2026-11-26: shared scan worker pool and cost-based parallelism
Parallel scans no longer create threads per query (STRATA-SYS-01-010):
- **Shared pool.** One process-wide pool of `sx-work-NN` threads
  (`[limits] scan_threads`, default one per logical CPU) runs the scan tasks
  of all queries.
- **Bounded queue.** It holds at most `workers` tasks, so at most 2 × workers
  are active.
- **Caller runs.** The query's thread always scans too, and takes over any
  share the pool cannot accept. A busy pool lowers parallelism instead of
  queueing or blocking.
- **Owned task state.** Tasks own their inputs (`Arc`). A task that starts
  after its query finished finds no work.
- **Order unchanged.** Results are still merged in segment order.
- **Degree of parallelism.** The base is `ceil(est_cpu_ms / 5 ms)`, clamped
  to `1..=min(parallelism, segments)` (STRATA-SYS-11-008). It applies when
  no pool worker is idle.
  - When workers are idle, a query may also use them, up to
    `min(parallelism, segments)`; queries under 2 ms of estimated work stay
    at 4 or fewer (STRATA-SYS-01-006).
  - The estimate is rows to scan × a per-row cost by query shape,
    calibrated on HW-REF-7.
  - *Deviation, with evidence:* the base formula alone scanned 1.7–3.8 ms
    queries on one worker and made them 2–4× slower on an idle machine,
    while fan-out costs this engine only 0–13% extra CPU. Under contention
    the formula still applies.
- **Visibility.** `stats.scan_workers`, `stats.est_cpu_ms` and EXPLAIN
  ANALYZE report it; `QueryOptions::target_ms_per_worker = 0` restores the
  fixed fan-out.
- **Evidence:** `benchmarks/reports/2026-11-26-scan-pool.md`.

## Revisit conditions
If the supported SQL surface grows to joins/window functions, or benchmarks
show vectorized Arrow execution is materially faster on Class B/C workloads.
