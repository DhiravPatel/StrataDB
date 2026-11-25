# StrataDB benchmark report — attribute key projection A/B

## Summary
Same release build, `--no-map-projection` (decode whole attribute maps unless
every referenced key is shredded — the previous behaviour) vs default,
interleaved (off, on × 5), 15 repetitions per query per run. Developer laptop
(macOS, aarch64, 10 CPUs); **not a capacity claim**.

## Measurement conditions (read this first)
The first two attempts ran while the machine was busy (load average 9–10:
Spotlight indexing, a browser, an IDE). Queries whose code path is identical in
both arms then differed by up to 2×, so those runs were discarded and not
published. The published runs were taken at load average 3–5, and use the
**minimum** latency of each run's 15 repetitions (robust to scheduling noise for
CPU-bound work), then the median over the 5 runs. Queries that do not use the
changed path stayed within 0.92–1.04× — that is the noise floor here.

## Change
When a query references specific attribute keys (not whole maps), each segment
now yields just those keys: shredded keys from their own chunks, others by
projecting the residual map chunk — values of other keys are skipped without
allocation (`skip_value`, validating exactly like a full decode). Previously a
single non-shredded key forced decoding every row's full map *and* every
shredded chunk of the scope.

## Results
| Query | Off (ms) | On (ms) | Speed-up | Segment bytes read |
|-------|----------|---------|----------|--------------------|
| 10 top error codes (`error.code` not shredded) | 13.18 [10.74–16.59] | **0.60** [0.55–0.67] | **22×** | 1 377 618 → 53 214 |
| all other suite queries (path unchanged) | — | — | 0.92–1.04× (noise) | unchanged |

Brackets: range over the 5 runs. Result rows and `rows_matched` identical in all
10 runs. In this dataset only query 10 references a non-shredded key; the gain
applies to any query naming rare attribute keys.

## Correctness evidence
* `read_map_keys_equals_full_maps` (segment proptest): projection ≡ full map
  lookup for shredded, residual and absent keys, both scopes, with/without
  shredding, nested values.
* `skip_value_validates_like_decode`: every single-byte corruption is accepted
  or rejected identically by `skip_value` and `decode_value`.
* Engine equivalence proptest now also compares `enable_map_projection = false`.

## Raw data
`benchmarks/reports/2026-11-25-map-projection-ab/{off,on}-{1..5}.json`

## Reproduction
```bash
cargo build --release -p stratadb-bench
for r in 1 2 3 4 5; do
  ./target/release/strata-bench --events 200000 --reps 15 --no-map-projection --out off-$r.json
  ./target/release/strata-bench --events 200000 --reps 15 --out on-$r.json
done
```
