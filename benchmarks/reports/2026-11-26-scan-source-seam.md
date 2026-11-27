# ScanSource seam and layer check (spec STRATA-ARCH-01-001/002/003) on HW-REF-7

## Summary
The executor no longer reads segment files. It asks storage
`ScanSource`s, which cover segments and memtable chunks, to prune themselves
and to return column batches. `stratadb-query` no longer depends on
`stratadb-segment`, and the layer check forbids that edge from returning.

This A/B checks two things:
- the extra indirection (one trait call per source, not per row) costs
  nothing measurable;
- every query plan is unchanged.

**Setup:** 3 interleaved foreground pairs of
`benchmarks/scenarios/standard.toml` (200k events, 4 tenants, 12 queries, 10
repetitions each).
- **Baseline:** this tree with only the seam change reverted. Its sources
  differ only in the files this change touches.
- **Candidate:** this tree.

## Plans
In all 6 runs every query gave identical results and statistics:
- the result rows;
- the segments in total and the segments scanned;
- the rows scanned;
- the indexes used (`bloom(trace_id)` and others).

Every plan assertion in the scenario passed in both arms: Bloom, n-gram, time
and severity pruning, shredded reads and early stop.

## CPU time
Process CPU in ms per query. Each figure is the median of 10 repetitions,
best of 3 runs:

| Query | Baseline | Seam | Ratio |
|---|---|---|---|
| q01 trace_id lookup | 3.005 | 2.883 | 0.96 |
| q02 request_id lookup | 1.700 | 1.632 | 0.96 |
| q03 recent errors | 1.531 | 1.481 | 0.97 |
| q04 service + severity | 5.083 | 4.992 | 0.98 |
| q05 time range | 1.496 | 1.367 | 0.91 |
| q06 message token search | 12.991 | 11.302 | 0.87 |
| q07 attribute equality | 6.473 | 6.326 | 0.98 |
| q08 high-cardinality GROUP BY | 4.578 | 4.684 | 1.02 |
| q09 service time bucket | 4.851 | 4.408 | 0.91 |
| q10 top error codes | 36.346 | 37.784 | 1.04 |
| q12 historical wide range | 3.193 | 3.256 | 1.02 |
| q13 absent substring (fully pruned) | 0.004 | 0.004 | — |

The geometric mean ratio is 0.96 (excluding q13, which runs in 4 µs). In
each arm, one of the three runs was about 1.5× slower across the board, a
laptop effect. Within the baseline, the run-to-run spread (max/min) had a
median of 1.46 over queries.

The difference is well inside that noise. **No speedup is claimed:** the
data shows only that the seam has no measurable cost.

These are developer-laptop measurements (Apple M4, macOS), not capacity
claims.

## Correctness evidence
- **Existing query suites, unchanged and passing:**
  - `crates/stratadb-query/tests/*`, including the pruning-on/off and
    vectorized-on/off property equivalence;
  - pagination, the parallel and sequential equivalence suites;
  - the read faults that name the segment file (`io_faults.rs`).
- **`crates/stratadb-storage/tests/scan_source.rs`:**
  - A segment and a memtable chunk holding the same rows serve identical
    columns.
  - A key-only attribute request is served per key by the segment. It equals
    the memtable map's projection.
  - Both kinds of source refuse to read for another tenant. Storage
    re-checks ownership on every read, not only the executor.
- **`stratadb_storage::scan` unit test:** only indexable predicates are
  recorded, and index names are reported as before.
- **`scripts/check_layers.py --self-test`:** 10 cases, among them a
  deliberate upward dependency, a skip-level dependency, a forbidden edge, a
  dependency on a composition root, an unlisted crate and a stale exception.
  Each one fails the check.
- **Workspace layer check:** run against the old graph with
  `stratadb-query → stratadb-segment` re-added, it fails with the
  ARCH-01-003 reason.

## Reproduction
```bash
# Baseline: copy the tree and revert the seam change (query crate sources and
# Cargo.toml, segment columns.rs and schema lib.rs from HEAD; drop
# schema/src/column.rs and storage/src/scan.rs; remove the Snapshot
# sources/memory_chunks methods and the Scope re-export), then:
CARGO_TARGET_DIR=<copy>/target cargo build --release -p stratadb-bench
cargo build --release -p stratadb-bench
for r in 1 2 3; do
  <copy>/target/release/strata-bench scenario benchmarks/scenarios/standard.toml --out ab/base-$r --hw-profile HW-REF-7
  target/release/strata-bench scenario benchmarks/scenarios/standard.toml --out ab/seam-$r --hw-profile HW-REF-7
done
python3 scripts/check_layers.py --self-test && python3 scripts/check_layers.py
```
Raw results: `2026-11-26-scan-source-seam/{base,seam}-{1,2,3}/`, with the
summary table in `summary.txt`.
