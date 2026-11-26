# P0 measurement system: first runs and noise floor (HW-REF-7)

## Summary
This report establishes and validates the spec phase P0 measurement system:
TOML scenarios, run manifests, open-loop load, regression detection, the
cost ledger and INSERT/SELECT stage metrics. It is **not a performance
claim**. Every number comes from HW-REF-7, a shared developer laptop (Apple
M4, 10 cores, macOS), and is informational. Its main finding is the noise
floor of this machine, which decides what the gates can and cannot do here.

## Setup
- Build: release profile (fat LTO, `codegen-units = 1`, `panic = "abort"`),
  commit `4dac459e` plus the working-tree changes of this phase. The manifest
  records `git_dirty: true`.
- Fat-LTO build of `strata`, `strata-server` and `strata-bench`: 98.7 s wall
  (310.9 s user). Binary sizes: `strata` 4.18 MB, `strata-server` 4.65 MB,
  `strata-bench` 2.30 MB.
- Scenario: `benchmarks/scenarios/standard.toml`:
  - 200,000 app-log events, 4 tenants, seed 1, 1,000-event batches;
  - deterministic layout of 4 segments per tenant;
  - durability `group_commit` (2 ms delay);
  - warm cache, 2 warmup runs and 10 repetitions per query;
  - open-loop ingest at 200 req/s (100 events per request) and open-loop
    trace lookup at 100 req/s, each for 10 s with 4 workers;
  - closed-loop dashboard query for 5 s with 4 workers.
- Three consecutive runs. Load average was 3.5–5.1 at the start of each run
  (recorded in each manifest).

## Results
All three runs passed every plan assertion. None was flagged `CLIENT_BOUND`.

### Dataset load (closed loop, burst; not sustained ingest)
| Run | events/s | CPU ns/event |
|---|---|---|
| 1 | 32,807 | 6,250 |
| 2 | 33,197 | 6,489 |
| 3 | 33,281 | 6,555 |

### INSERT stages (engine; 800 batches of about 250 events per tenant)
These are means across the three runs. The p50 and p99 columns are histogram
bucket upper bounds. The budgets are VIII.1.2 targets for HW-REF-1 with
1,000-row batches, so they are not directly comparable.

| Stage | Mean (µs) | p50 ≤ | p99 ≤ | Budget p50 / p99 |
|---|---|---|---|---|
| admission | 0.0 | 1 | 1 | — |
| wal_encode | 63 | 100 | 250 | 20 / 60 |
| wal_append | 32–41 | 50 | 250 | 3 / 15 |
| group_commit_wait | 5,700–5,860 | 10,000 | 10,000 | 250 / 1,500 |
| memtable_insert | 55–58 | 50–100 | 250 | 30 / 100 |

Reading:
- `group_commit_wait` is the 2 ms group delay plus this laptop's fsync
  latency.
- `wal_encode` reflects the row-oriented event codec, where the spec expects
  a columnar WAL payload (STRATA-SYS-07-002).
- Both are tracked gaps in docs/project/PERF_INVENTORY.md.

### Queries: median wall ms (median process-CPU ms)
| Query | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| q01 trace_id lookup | 3.25 (3.25) | 4.07 (4.08) | 3.82 (3.82) |
| q04 service + severity | 1.72 (5.99) | 1.79 (6.90) | 2.57 (7.93) |
| q06 LIKE `%TIMEOUT%` | 3.38 (13.19) | 4.64 (17.92) | 5.34 (20.85) |
| q08 high-cardinality GROUP BY | 2.94 (5.53) | 3.77 (6.96) | 3.54 (6.68) |
| q10 top error codes | 12.24 (43.41) | 13.51 (53.64) | 14.07 (49.14) |
| q12 GROUP BY severity | 1.17 (3.83) | 1.58 (5.39) | 0.96 (3.65) |

The full per-query samples, CIs and plan statistics are in `standard-*/results.json`.

### Open-loop loads (latency from intended send time, ms)
| Load | Run | p50 | p99 | p99.9 | achieved/s |
|---|---|---|---|---|---|
| ingest 200/s | 1 / 2 / 3 | 7.18 / 7.00 / 7.38 | 19.45 / 12.92 / 14.61 | 40.66 / 17.34 / 39.79 | 200 |
| trace lookup 100/s | 1 / 2 / 3 | 11.34 / 12.68 / 11.34 | 15.30 / 20.43 / 77.41 | 22.32 / 44.80 / 108.16 | 100 |

At 100 req/s with 4 workers, trace-lookup latency (p50 about 11 ms) is 3× the
single-query time (q01, about 3.5 ms). Concurrent lookups compete for cores
because every query spawns its own scan threads (STRATA-SYS-01-010, a
known violation scheduled for ADR-0026 phase 10). The open-loop generator
makes this visible; a closed-loop benchmark would hide it.

## Noise floor: what the gates can do on this machine
- **Run to run, same binary.** Medians of the same query differ by up to 58%
  between consecutive runs (q04 wall: 1.72 → 2.57 ms; q06 process CPU:
  13.2 → 20.9 ms).
- **A/A regression test.** Comparing run 3 against the baseline of runs 1–2
  (`aa-compare.json`, `aa-compare.txt`) reports **5 "regressions" and 4
  "improvements" out of 27 metrics**, all of them false.
  - The pooled Mann-Whitney test treats samples as independent.
  - Run-level effects (load, thermals, frequency) violate that.
  - This is the failure mode STRATA-SYS-19-006 guards against by requiring
    dedicated runners with isolated cores.
- **Cost ledger.** Three ledger sessions within about 10 minutes moved every
  entry by up to ±40% (for example `crc32c` 46.6–80.5 CPU ns/KiB), far
  beyond the 5% + noise-band limit.

**Conclusion.** On HW-REF-7 the regression gate and the ledger check produce
false alarms, so CI runs only the smoke scenario (harness correctness, plan
assertions, client-bound detection). Performance gating waits for a
dedicated runner (RELEASE_DECLARATION.md). Before/after evidence on this
machine must keep using interleaved runs and CPU time, with the noise floor
stated, as in the earlier reports.

## Harness bugs found while validating it
- Closed-loop runs were unbounded: a no-op workload produced millions of
  samples, and bootstrapping them took minutes. Fixed with a cap of 10⁶
  requests and an order-statistic CI above 2,000 samples.
- Dispatch lag from macOS timer coalescing (sleep overshoot of 0.5–4 ms)
  produced false `CLIENT_BOUND` flags. Fixed with a sleep-then-spin dispatch
  whose spin window adapts to the observed overshoot (lag p50 now about
  4 µs). Spin time is excluded from client CPU.
- Requests delayed while the dispatcher was blocked on busy workers were
  counted as client lag. They are now charged only as latency, since the
  system under test caused the delay.
- Background flush timing made segment layouts, and therefore pruning
  assertions, nondeterministic: one smoke run in three failed. Fixed with
  `flush_every`, which flushes explicitly and then reopens with background
  flushing.
- On macOS the manifest named the sealed system volume instead of the data
  volume (firmlinks). Fixed by resolving the mount point with `df -P`.

## Reproduction
```bash
cargo build --release -p stratadb-bench
for i in 1 2 3; do
  target/release/strata-bench scenario benchmarks/scenarios/standard.toml --out runs/standard-$i
done
target/release/strata-bench compare runs/standard-3/results.json --baseline runs
target/release/strata-bench ledger --runs 20      # against perf/cpr_ledger.toml
```
