# StrataDB benchmark report — segment v2 LZ4 compression A/B (developer laptop)

## Summary
RFC-0001 evidence: same build, `--no-compression` vs default, interleaved
(off, on × 3) in one session. Not a capacity claim; no competitor comparison.

## Environment / dataset / durability
As in `2026-11-20-baseline-dev.md` (Apple M4, 24 GiB, APFS, release build,
`app-logs`, seed 1, 200 000 events, 4 tenants, `group_commit`, warm cache).
Bloom filters on in both arms. Machine was noisier than earlier runs (wide ranges).

## Results (medians of 3 runs)
| Metric | LZ4 off | LZ4 on | Change |
|--------|---------|--------|--------|
| Segment bytes | 41.08 MB | **14.10 MB** | **−65.7 % (2.91× smaller)** |
| Encoded-event bytes / segment bytes | 1.13 | **3.29** | |
| Ingest throughput (wall) | 30 008 ev/s | 30 333 ev/s | within noise |
| Final flush (200 k rows in memtable) | 392 ms | 476 ms | +21 % |
| Query p50, all 11 queries | — | — | within run-to-run noise (ranges overlap) |

Query costs are dominated by decoding attribute maps, not I/O, on a warm
cache, so smaller chunks neither help nor hurt measurably here; cold-cache and
larger-than-RAM datasets are expected to benefit and remain to be measured.

## Raw data
`benchmarks/reports/2026-11-21-compression-ab/{off,on}-{1,2,3}.json`

## Limitations
One machine, warm cache, 3 runs, one profile; zstd not evaluated (C toolchain).

## Reproduction
```bash
cargo build --release -p stratadb-bench
for r in 1 2 3; do
  ./target/release/strata-bench --events 200000 --reps 5 --no-compression --out off-$r.json
  ./target/release/strata-bench --events 200000 --reps 5 --out on-$r.json
done
```
