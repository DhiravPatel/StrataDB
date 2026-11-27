# Benchmark harness (`strata-bench`, spec VIII.19)

```
strata-bench scenario FILE.toml --out DIR [--hw-profile HW-REF-n] [--data-dir D]
strata-bench compare DIR/results.json --baseline RUNS_DIR [--window 10 --alpha 0.01 --min-effect 0.03]
strata-bench ledger [--write] [--runs 20] [--path perf/cpr_ledger.toml]
strata-bench [legacy flags]          # the fixed §27.4 suite (raw JSON, unchanged)
```

`--hw-profile` defaults to `HW-REF-7` (an unclassified or laptop machine).
Results from different profiles are never compared.

**Run benchmarks in the foreground.** macOS may schedule a background
(low-priority) process on efficiency cores. In one session a background A/B
ran 3–30× slower than the same scenario in the foreground, in both arms
(report 2026-11-26-scan-pool).

## Scenarios
Scenarios are TOML files in `benchmarks/scenarios/`:
- `smoke.toml` is the CI smoke test: 20k events, a deterministic layout and
  plan assertions.
- `standard.toml` is the §27.4 suite plus open- and closed-loop loads.

Unknown keys are rejected.

| Section | Keys |
|---|---|
| top | `name`, `description` |
| `[dataset]` | `profile` (testkit profile), `events`, `tenants`, `seed`, `batch_size`, `flush_every` (optional: deterministic segment layout) |
| `[engine]` | `durability` = `async` \| `sync` \| `group_commit` (**required**, Part V.8), `group_commit_delay_ms`, `bloom`, `ngram`, `compression`, `shredding`, `memtable_max_bytes`, `adaptive_group_delay`, `memory_accounting` (default true: a server-like, unlimited memory tracker tree; `false` for A/B) |
| `[query_options]` | `parallelism` (0 = default), `vectorized`, `map_projection`, `target_ms_per_worker` (0 = always `parallelism` workers) |
| `[run]` | `warmup`, `repetitions` (≥ 5, STRATA-SYS-19-001), `cache` (only `"warm"`: a valid cold run needs a dropped page cache and an idle device, STRATA-SYS-01-009) |
| `[[query]]` | `id`, `sql` (placeholders `{trace_id}`, `{request_id}`, `{t_start}`, `{t_end}`, `{t_recent}`, `{t_mid_lo}`, `{t_mid_hi}`), `tenant`, `[query.expect]` |
| `[[load]]` | `id`, `kind` = `ingest` \| `query`, `mode` = `open` \| `closed`, `rate_per_s` (open), `duration_s`, `workers`, `batch_size` (ingest), `query` (a `[[query]]` id) |

### Plan assertions (STRATA-SYS-25-002)
`[query.expect]` maps a statistic to a condition, for example
`segments_pruned_bloom = ">= 1"` or `indexes_used = "contains bloom(trace_id)"`.
Any failed assertion fails the run: a query that stops using the mechanism
that makes it fast is a regression even if it still returns the right rows.

The statistics are:
- `result_rows`, `rows_scanned`, `rows_matched`, `bytes_read`, `early_stop`;
- `segments_total`, `segments_scanned`, `segments_pruned`;
- `segments_pruned_time`, `segments_pruned_keys`, `segments_pruned_severity`,
  `segments_pruned_bloom`, `segments_pruned_ngram`;
- `shredded_reads`, `memory_rows_scanned`, `scan_workers`.

Assertions that depend on the number of segments need `flush_every`.
Background flush timing is not deterministic, and a run with a different
segment layout can legitimately prune differently. After a deterministic
load, the engine is reopened with background flushing for the query and
load phases.

## What a run produces
`DIR/manifest.json` (STRATA-SYS-19-004) records:
- git SHA and dirty flag, build profile, opt level, target, rustc version and
  allocator;
- CPU model and microcode, logical CPUs, memory, kernel;
- governor, THP, SMT, turbo, selected sysctls;
- storage model and firmware, the filesystem of the data directory (`df` plus
  the mount table), load average;
- dataset spec and generator version, scenario SHA-256, engine settings;
- `runner_fingerprint`, a hash of hardware and OS facts with no hostname or
  user data.

Values the host does not expose are `"unknown"`. macOS exposes neither
storage firmware nor governor.

`DIR/results.json` contains:
- **`ingest`**: the dataset load. It is closed-loop and burst, so it is
  labelled as not publishable as sustained ingest (STRATA-SYS-01-004/005).
  Fields: events/s, CPU ns per event, ack latency summary.
- **`insert_stages`**: per engine stage (`admission`, `wal_encode`,
  `wal_append`, `group_commit_wait`, `memtable_insert`), the count, the mean,
  and the p50/p99 bucket upper bounds, next to the VIII.1.2 budgets.
  `gate_eligible` is true only on HW-REF-1.
- **`queries`**: per query, the raw `wall_ms` and `process_cpu_ms` samples,
  summaries (median, p95, p99, p99.9, mean, stddev, 95% CI of the median),
  CPU ns per row scanned, median `plan_us`, and assertion results.
- **`loads`**: per load block, the open/closed label, requests, errors,
  achieved rate, latency and service-time summaries, raw latency samples (at
  most 20,000), dispatch lag, dispatcher CPU fraction and flags.
- **`verdict`**: `ok`, `failures`, `flags`, `excluded_client_bound`.

The exit code is 0 when the verdict is ok and 2 when assertions fail or the
client is bound.

## Open-loop load (STRATA-SYS-19-002/003)
Requests follow a fixed timeline. Latency is measured from the **intended**
send time, so waiting caused by a slow system is counted and there is no
coordinated omission. The dispatcher sleeps until shortly before each send
time, then spins. The spin window adapts to the OS's observed sleep
overshoot, because macOS timer coalescing overshoots by milliseconds.

A run is flagged `CLIENT_BOUND` when either of these holds:
- the dispatcher used more than 70% of a CPU, excluding deliberate spinning;
- the p99 lateness of requests a free worker could have taken exceeds
  max(2 ms, 10% of the interval).

Client-bound runs are excluded from comparisons. Requests that fell due
while every worker was busy are charged in latency, not as client lag. When
workers stay busy the run is flagged `SATURATED`, which is informational.
Closed-loop mode is labelled "throughput only".

## Regression detection (STRATA-SYS-19-005)
`compare` selects the last `window` runs (default 10) with the same scenario,
`runner_fingerprint` and durability that are not client-bound. For each
metric it tests the candidate's raw samples against the pooled baseline with
a two-sided Mann-Whitney U test (normal approximation, tie and continuity
corrections). The metrics are:
- `query.<id>.wall_ms`;
- `query.<id>.process_cpu_ms`;
- `load.<id>.latency_ms`.

A metric regresses when `p < alpha` and the median rose by at least
`min_effect`. It improves symmetrically when the median fell. The exit code
is 2 when any metric regresses. Every verdict also reports
`baseline_run_medians`, the spread of the per-run medians.

**Limitation (measured):** the pooled test treats samples as independent,
but on a shared machine run-to-run effects (load, thermals, frequency)
dominate. In an A/A test on HW-REF-7 (same binary, three runs) the
comparator reported 5 "regressions" and 4 "improvements" out of 27 metrics
(benchmarks/reports/2026-11-26-p0-measurement-system.md). The gate is only
meaningful on a dedicated runner with isolated cores (STRATA-SYS-19-006). On
other machines, treat its output as a hint and check `baseline_run_medians`.

## Operator cost ledger (VIII.1.8)
`perf/cpr_ledger.toml` records, per operator, the median **CPU nanoseconds
per unit** over 20 runs and a noise band (half the 10–90% range relative to
the median). It does not record cycles: STRATA-SYS-01-014 requires hardware
counters, which are BLOCKED here.

Each sample repeats the operation until it has used at least 50 ms of CPU.
`ledger` (without `--write`) fails when a median exceeds
`measured × (1 + 0.05 + noise_band)`, the STRATA-SYS-01-015 rule. A ledger
recorded on a different profile or CPU model is reported as not comparable,
not failed. On HW-REF-7, session-to-session drift reaches 40%, so CI does
not run the check.

## Statistics (`apps/stratadb-bench/src/stats.rs`)
- Quantiles use linear interpolation (type 7).
- The standard deviation is the sample standard deviation.
- The 95% CI of the median uses a percentile bootstrap (2,000 resamples,
  fixed seed) for up to 2,000 samples, and order statistics
  (`n/2 ∓ 1.96·√n/2`) above that.
- Mann-Whitney U is two-sided, with a normal approximation and tie and
  continuity corrections.
