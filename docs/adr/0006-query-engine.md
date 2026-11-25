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

## Revisit conditions
If the supported SQL surface grows to joins/window functions, or benchmarks
show vectorized Arrow execution is materially faster on Class B/C workloads.
