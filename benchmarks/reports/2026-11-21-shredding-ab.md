# StrataDB benchmark report — attribute shredding A/B (segment v3, RFC-0002)

## Summary
Same build, `--no-shredding` vs default, interleaved (off, on × 3), one
session, quiet machine (tight ranges). Developer laptop; not a capacity claim.

## Environment / dataset / durability
As in `2026-11-20-baseline-dev.md`; LZ4 compression and Bloom filters on in
both arms. 200 000 `app-logs` events, seed 1, query suite on `tenant-0`.

## Results (median of 3 runs' p50; segment bytes read from run 1)
| Query | Off | On | Speed-up | Segment bytes read |
|-------|-----|----|----------|--------------------|
| 02 request_id lookup | 10.46 ms | **2.48 ms** | **4.2×** | 764 676 → 231 083 |
| 07 attribute equality (region) | 25.17 ms | **5.54 ms** | **4.5×** | 1 646 077 → 132 435 |
| 08 high-cardinality group-by (user_id) | 33.87 ms | **12.85 ms** | **2.6×** | 1 646 077 → 255 528 |
| 10 top error codes | 25.13 ms | 18.33 ms | 1.4× | 1 674 204 → 1 377 618 |
| Other 7 queries | — | — | 0.99–1.12× | unchanged |

| Metric | Off | On |
|--------|-----|----|
| Ingest (wall) | 40 975 ev/s | 40 716 ev/s (within noise) |
| Segment bytes | 14.10 MB | 12.35 MB (−12 %) |
| Final flush | 163 ms | 186 ms (+14 %) |

Query 10 benefits less because `error.code` occurs in ~7 % of rows (below the
10 % shredding threshold), so it is served from the residual maps.

## Raw data
`benchmarks/reports/2026-11-21-shredding-ab/{off,on}-{1,2,3}.json`

## Limitations
Warm cache, one profile, one machine, 3 runs. Threshold (10 %) not tuned.

## Reproduction
```bash
cargo build --release -p stratadb-bench
for r in 1 2 3; do
  ./target/release/strata-bench --events 200000 --reps 5 --no-shredding --out off-$r.json
  ./target/release/strata-bench --events 200000 --reps 5 --out on-$r.json
done
```
