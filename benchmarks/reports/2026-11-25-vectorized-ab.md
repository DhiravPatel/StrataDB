# StrataDB benchmark report — vectorized filter evaluation A/B

## Summary
Same release build, `--no-vectorize` (scalar per-row filter) vs default
(vectorized: typed column kernels + selection vectors, `crates/stratadb-query/src/vector.rs`),
interleaved (off, on × 3), one session. Developer laptop (macOS, aarch64,
10 CPUs); **not a capacity claim**.

## Environment / dataset / durability
As in `2026-11-20-baseline-dev.md`: 200 000 `app-logs` events (seed 1,
4 tenants, batch 1 000, `group_commit`), query suite on `tenant-0`, 5
repetitions per query per run, warm cache (same process, OS page cache not
dropped). LZ4 compression, Bloom filters and attribute shredding on in both arms.

## Results (median of the 3 runs' p50)
| Query | Off | On | Speed-up |
|-------|-----|----|----------|
| 01 trace_id lookup | 2.20 ms | 1.49 ms | 1.5× |
| 02 request_id lookup | 2.49 ms | 1.51 ms | 1.6× |
| 03 recent errors (time + severity, top 100) | 1.49 ms | 1.12 ms | 1.3× |
| 04 service = … AND severity = … | 4.47 ms | 1.53 ms | **2.9×** |
| 05 time-range count | 1.11 ms | 0.47 ms | **2.4×** |
| 06 `body LIKE '%TIMEOUT%'` | 10.20 ms | 2.88 ms | **3.5×** |
| 07 attribute equality (region) | 5.60 ms | 2.29 ms | **2.4×** |
| 10 top error codes (severity IN …, group by) | 17.96 ms | 14.66 ms | 1.2× |
| 08, 09, 12 (no `WHERE` clause) | — | — | 0.99–1.01× (unchanged, as expected) |

Per-run p50s were within ±4 % of the median in every cell. Result row
counts and `rows_matched` were identical in all six runs. Ingest throughput is
unaffected (the change is query-side only): 41.7–42.5 k ev/s off vs
41.4–42.2 k ev/s on.

## Where the time went (why it helps)
The scalar path materialized a `Value` per row and operand: a fresh `String`
for every `service`/`trace_id`/severity cell, a clone of the literal on every
comparison, a cloned attribute value per lookup, and `LIKE` recompiled its
pattern and built a `Vec<char>` per row. The vectorized path compares borrowed
column cells against the literal, answers severity predicates from a
256-entry table built once per query, compiles `LIKE` once (plain
`str::contains` for `%literal%`), and evaluates the right side of `AND`/`OR`
only on rows the left side left undecided. Queries dominated by grouping or
projection (08, 09, 12, most of 10) are not affected; projection/grouping
still materialize values per matched row (a follow-up).

## Correctness evidence
- `vector::tests::vectorized_equals_scalar`: 2 000 random expressions × random
  typed batches (nulls, NaN, mixed numeric types, missing values, unknown
  severity codes, shredded and map attributes, literals on either side,
  `LIKE`/`ILIKE`, `IN`, `IS [NOT] NULL/MISSING`, fallbacks), row-for-row
  equality with the scalar evaluator, including chunks starting mid-batch.
  Mutation-checked: seven injected kernel bugs were each caught.
- `eval::tests::like_fast_path_equals_general` (proptest).
- `optimized_equals_unoptimized_equals_reference` now also compares
  `enable_vectorized = false`.

## Raw data
`benchmarks/reports/2026-11-25-vectorized-ab/{off,on}-{1,2,3}.json`

## Limitations
Warm cache, one profile, one machine, 3 runs, single-threaded scans.

## Reproduction
```bash
cargo build --release -p stratadb-bench
for r in 1 2 3; do
  ./target/release/strata-bench --events 200000 --reps 5 --no-vectorize --out off-$r.json
  ./target/release/strata-bench --events 200000 --reps 5 --out on-$r.json
done
```
