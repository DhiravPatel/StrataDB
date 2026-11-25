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

## Revisit conditions
If the supported SQL surface grows to joins/window functions, or benchmarks
show vectorized Arrow execution is materially faster on Class B/C workloads.
