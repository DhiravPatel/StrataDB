# StrataDB benchmark report — parallel segment scans A/B

## Summary
Same release build, `--no-parallel` (sequential) vs default (up to
min(CPUs, 8) workers), interleaved (off, on × 3). Developer laptop (macOS,
aarch64, 10 CPUs); **not a capacity claim**. All other optimizations on.

## Dataset
200 000 `app-logs` events (seed 1, 4 tenants, `group_commit`), queries on
`tenant-0`, which has **3 segments** — so at most 3 workers have work; the
speed-up ceiling here is 3×. 5 repetitions per query per run, warm cache.

## Results (median of the 3 runs' p50)
| Query | Segments scanned | Off | On | Speed-up |
|-------|------------------|-----|----|----------|
| 04 service + severity count | 3 | 1.54 ms | 0.63 ms | **2.4×** |
| 06 `LIKE '%TIMEOUT%'` count | 3 | 2.90 ms | 1.33 ms | **2.2×** |
| 07 attribute equality count | 3 | 2.29 ms | 0.92 ms | **2.5×** |
| 09 group by service, time bucket | 3 | 4.65 ms | 1.79 ms | **2.6×** |
| 12 group by severity | 3 | 2.01 ms | 0.78 ms | **2.6×** |
| 10 top error codes (map decoding) | 3 | 14.16 ms | 8.07 ms | 1.75× |
| 08 ≈50 k groups, top 10 | 3 | 10.15 ms | 8.27 ms | 1.23× |
| 01–03, 05 (one segment after pruning) | 1 | — | — | 1.00× |

Result rows and `rows_matched` were identical in all six runs. Query 08 is
limited by the sequential merge of ≈50 k groups from each partial and by
memtable rows (scanned after the merge).

## Correctness evidence
`crates/stratadb-query/tests/parallel.rs`: parallel (8) vs sequential (1) over
12 segments + memtable with heavy ties — identical rows/order, group order,
integer aggregates, stats and pagination pages; float `sum`/`avg` equal within
1e-9 relative and deterministic run to run; row and memory budgets enforced
query-wide; data errors surface (not worker-cancellation symptoms); user
cancellation reported. `row_budget_stops_parallel_workers_early` checks that
workers stop near the budget (≤ budget + one segment per worker of 6 000 rows).
Mutation-checked: wrong count merge, unmerged groups, out-of-order merge and a
per-worker-only budget are each caught. The whole query suite (equivalence
proptests, pagination) runs with parallelism on by default.

## Raw data
`benchmarks/reports/2026-11-25-parallel-ab/{off,on}-{1,2,3}.json`

## Reproduction
```bash
cargo build --release -p stratadb-bench
for r in 1 2 3; do
  ./target/release/strata-bench --events 200000 --reps 5 --no-parallel --out off-$r.json
  ./target/release/strata-bench --events 200000 --reps 5 --out on-$r.json
done
```
