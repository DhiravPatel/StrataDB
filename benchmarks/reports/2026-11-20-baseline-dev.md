# StrataDB benchmark report — baseline (developer laptop)

## Summary
First internal baseline to guide Phase 3 work. **Not a performance claim**, no
competitor comparison, single machine, warm cache, one run of 5 query repetitions.

## Hardware
CPU: Apple M4 (10 cores) · RAM: 24 GiB · Storage: internal SSD · Filesystem: APFS ·
OS: macOS 27.0.1 · Network: n/a (in-process).

## Software versions
StrataDB 0.0.1 (uncommitted working tree, 2026-11-20), release profile
(`lto = "thin"`, `codegen-units = 1`), rustc 1.95.0. No competitors.

## Dataset
Generator `stratadb-testkit`, profile `app-logs`, seed 1, 200 000 events,
4 tenants (round-robin), ~230 B encoded/event (46 MB encoded total).
Timestamps monotonic, 1 ms apart. Query suite runs against `tenant-0` (50 000 events).

## Durability
`group_commit` (max_delay 2 ms), F_FULLFSYNC per group; no replication.

## Query workload
Spec §27.4 queries 1–10 and 12 (11 — concurrent ingest+query — not yet
implemented in the harness). Concurrency 1. Cache: warm.

## Results (from the raw JSON)
| Metric | Value |
|--------|-------|
| Ingest throughput (wall) | ≈ 41 000 events/s (batches of 1000 split per tenant → 250 events/commit) |
| Ack latency p50 / p99 | 5.9 ms / 10.0 ms |
| Segment bytes / encoded bytes | 40.7 MB / 46 MB (ratio 1.14: no general-purpose compression yet) |
| Reopen after flush | 13.9 ms |

| Query | p50 | Rows scanned | Segments pruned |
|-------|-----|--------------|-----------------|
| 01 trace_id lookup | 5.9 ms | 50 000 | 0/3 |
| 02 request_id lookup | 29.3 ms | 50 000 | 0/3 |
| 03 recent errors (last 5 %) | 1.7 ms | 12 500 | 2/3 |
| 04 service+severity count | 4.5 ms | 50 000 | 0/3 |
| 05 time-range scan (10 %) | 1.1 ms | 18 750 | 2/3 |
| 06 body LIKE | 16.0 ms | 50 000 | 0/3 |
| 07 attribute equality | 27.0 ms | 50 000 | 0/3 |
| 08 high-cardinality group-by | 36.2 ms | 50 000 | 0/3 |
| 09 service × 1-min bucket | 15.7 ms | 50 000 | 0/3 |
| 10 top error codes | 27.3 ms | 50 000 | 0/3 |
| 12 severity histogram | 7.8 ms | 50 000 | 0/3 |

## Observations (engineering input, not claims)
1. Attribute predicates (02, 07, 08, 10) dominate: the attributes chunk is
   decoded as whole per-row maps. Candidate fix: shred hot attribute keys into
   columns (segment v2) — needs an RFC and benchmark.
2. Exact-ID lookups scan all rows: bloom filters / exact-match postings (F-003/F-004) are the expected win.
3. Compression ratio ≈ 1.1: evaluate zstd/LZ4 per chunk (spec §11.4).
4. Ingest latency is bounded by F_FULLFSYNC; larger groups or `async` trade durability for throughput (ADR-0009).

## Repetitions and variance
5 repetitions per query; only one process run — variance across runs not yet measured.

## Raw data
`benchmarks/reports/2026-11-20-baseline-app-logs-200k.json`

## Limitations
Warm cache; single run; one profile; tenant-0 only; debug info present in release build.

## Reproduction commands
```bash
cargo build --release -p stratadb-bench
./target/release/strata-bench --events 200000 --reps 5 --profile app-logs --out result.json
```
