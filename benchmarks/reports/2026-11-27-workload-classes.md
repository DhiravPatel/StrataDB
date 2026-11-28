# Workload classes, fair scan scheduling and admission (spec VIII.11) on HW-REF-7

## Summary
- **What changed** (ADR-0036):
  - the scan pool schedules one morsel at a time through a weighted fair
    queue (class weight, then tenant, then FIFO, with aging);
  - lower-class queries yield to higher-class work at their morsel
    boundaries;
  - the server admits queries per class through a bounded fair queue, with
    `TOO_MANY_QUERIES` / `SERVER_OVERLOADED` refusals, demotion and an AIMD
    controller on batch slots.
- **No regression** on the scan-heavy concurrency scenario:
  - single-query CPU geometric mean 1.00×;
  - loads at the same throughput, and open-loop p99 equal or lower.
- **No measurable latency gain** for interactive queries next to a batch
  flood in the embedded benchmark on this 10-core laptop: classed and
  unclassed arms are within run-to-run noise. The reasons are below; the
  scheduling guarantees themselves are checked by deterministic tests, not
  by this benchmark.

## Setup
- **Binaries:** `strata-bench`, release builds.
  - **Base:** HEAD (`56e0782`, typed columns). Its query path equals the tree
    before this change: A1-3 touched only storage and server code, plus a
    parse/plan wrapper.
  - **New:** this change.
- **Ordering:** 3 interleaved foreground pairs of
  `benchmarks/scenarios/concurrency.toml`.
- **Machine:** Apple M4 (10 cores), macOS. Load averages were 4–8 during the
  runs, from external `node` and WindowServer processes. These are
  developer-laptop measurements, not capacity claims.

## Overhead check: concurrency scenario
Process CPU per query in ms (median of 10 repetitions, best of 3 runs):

| Query | Base | New | Ratio |
|---|---|---|---|
| q04 service/severity count | 3.880 | 3.894 | 1.00× |
| q06 `LIKE '%TIMEOUT%'` count | 10.583 | 10.585 | 1.00× |
| q07 attribute equality | 6.196 | 6.043 | 1.03× |
| q08 high-cardinality GROUP BY | 3.906 | 3.914 | 1.00× |
| q12 GROUP BY severity | 0.865 | 0.883 | 0.98× |
| q01 trace id lookup | 2.779 | 2.780 | 1.00× |

The geometric mean is 1.00×, and wall times are within 1%.

Loads (median of 3 runs):

| Load | Metric | Base | New |
|---|---|---|---|
| LIKE, 8 closed-loop clients | rate/s | 362 | 369 |
| GROUP BY, 8 closed-loop clients | rate/s | 4,705 | 4,630 |
| filter, open loop 300/s | p50 / p99 ms | 3.97 / 8.03 | 4.00 / 7.13 |
| trace lookup, open loop 200/s | p50 / p99 ms | 6.12 / 10.16 | 6.13 / 7.18 |

The per-morsel cost of fair scheduling, one pool lock and one thread-CPU
clock read per segment step, is not visible at these morsel sizes.

## Mixed workload: `benchmarks/scenarios/workload.toml`
- **Measured load:** interactive `GROUP BY service, time bucket` queries,
  open loop at 100/s, alone and next to closed-loop `LIKE` scan clients.
- **Interference:** either 8 clients (no admission bound) or 3 clients
  (about what the server's batch slots would admit).
- **Arms:** "unclassed" runs both loads as `normal`; "classed" runs
  interactive against batch. Pairs alternate.

p50 / p99 of the interactive load in ms:

| Load | Run 1 (load avg 5–8) | Run 2 (load avg 4–6) |
|---|---|---|
| interactive alone | 9.81 / 36.6 | 5.52 / 6.57 |
| 8 batch clients, unclassed | 14.9 / 45.1, 14.4 / 33.3 | 12.9 / 17.0, 14.1 / 34.7 |
| 8 batch clients, classed | 13.9 / 18.2, 14.5 / 38.4 | 14.2 / 29.6, 13.4 / 30.1 |
| 3 batch clients, unclassed | 11.8 / 18.3, 10.5 / 19.6 | 9.03 / 10.4, 9.08 / 9.69 |
| 3 batch clients, classed | 11.1 / 21.6, 10.5 / 16.8 | 8.48 / 10.1, 8.98 / 9.64 |

Several 8-client runs are flagged `CLIENT_BOUND`: the open-loop dispatcher
lagged because the machine was saturated. Differences between the arms are
smaller than the spread between repeats.

### Why classes do not show here
In the embedded benchmark each query runs on its client's thread, and the
pool only adds helpers to it.
- **Unbounded batch threads:** with 8 batch clients there are 8 batch
  threads, the pool's 10 workers and up to 8 interactive client threads, on
  10 cores. The OS scheduler shares CPU among them regardless of the pool.
- **Yielding needs helpers:** a lower-class thread can only yield once its
  own helper tasks hold its scan. Batch work may hold at most `workers`
  pool tasks, so most of the 8 batch queries get no helper and scan on their
  own thread, which is what guarantees their progress.

**What bounds this in the server:**
- Batch slots: a quarter of `max_concurrent_queries`, adapted by AIMD.
- The queue in front of them.

These were exercised only functionally (`apps/stratadb-server/tests/workload.rs`),
not under load. An HTTP mixed-load benchmark, and a host with more cores
than runnable threads (HW-REF-1), remain to be run.

## Correctness evidence
- **`stratadb_common::workload` (`FairQueue`):**
  - CPU shares follow the weights 8:4:1, both for equal and unequal step
    costs;
  - tenants alternate and stay FIFO within a tenant;
  - an idle class does not bank credit;
  - `system` goes first;
  - aging lets a starved class through;
  - eligibility, remove and position work.
- **`stratadb_query::pool`:**
  - interactive steps preempt a running batch task at morsel boundaries
    with about an 8:1 share;
  - a demoted task's remaining steps run in the lower class;
  - two tenants of one class alternate;
  - batch work leaves room in the pool for interactive work;
  - the existing saturation, panic and thread-name tests pass.
- **`apps/stratadb-server/src/admission.rs`:**
  - fair dispatch across classes, tenants and arrival order;
  - per-class slots under the global limit;
  - refusal when the queue is full, on queue timeout, and on cancel while
    queued;
  - no slot leak when a waiter disconnects;
  - demotion after 2× the class budget frees the class slot;
  - the AIMD floor and ceiling;
  - PSI memory pressure and memory headroom gating;
  - tenant and request class overrides, where a request can never raise
    its class;
  - PSI parsing.
- **`apps/stratadb-server/tests/workload.rs`** (over HTTP):
  - classes are reported in responses;
  - `workload_class` lowers the class, and an invalid value gives 400;
  - a queued query reports `queued`, `queue_position: 0` and its class;
  - a full queue gives 429 `TOO_MANY_QUERIES` with `retry_after_ms` and
    `Retry-After`;
  - a queued query runs once a slot frees, and reports `queue_us`;
  - a query cancelled while queued fails with `cancelled`;
  - the `/api/v1/admin/workload` counters and the `strata_workload_*` and
    `strata_scan_pool_class_cpu_seconds_total` series are present.
- **Unchanged and passing:** all query equivalence suites (parallel against
  sequential), the lifecycle and query-log suites.

## Reproduction
```bash
# base: strata-bench built from 56e0782; new: this tree
for r in 1 2 3; do
  base/strata-bench scenario benchmarks/scenarios/concurrency.toml --out ab/conc-base-$r --hw-profile HW-REF-7
  target/release/strata-bench scenario benchmarks/scenarios/concurrency.toml --out ab/conc-new-$r --hw-profile HW-REF-7
done
target/release/strata-bench scenario benchmarks/scenarios/workload.toml --out ab/workload --hw-profile HW-REF-7
```
Raw results are in `2026-11-27-workload-classes/`, with the summaries.
