# Partial aggregate cache: 2026-11-28 (ADR-0045)

**Command:** `cargo test --release -p stratadb-query --test partial_cache dashboard_ab -- --ignored --nocapture`

**Conditions.**
- **Machine:** the development laptop (Apple Silicon), **on battery**.
- **Load:** a load average of 27–55 from external processes. Wall times are
  noisy; the CPU ratios are the more reliable figure.
- **Method:** one process, cache off and warm cache interleaved over 12
  rounds. The first 2 rounds (cache warm-up) are dropped and the median of
  the rest is reported.

**Data.**
- 24 flushed segments of 20,000 rows each, one per hour.
- One memtable batch of 20,000 rows.

**Queries.** Each run asks for "the last 23 hours", one second later than
the run before, as a refreshing dashboard does. Segment 1 is therefore on
the edge of the window, segments 2–23 are fully covered, and segment 0 is
pruned.

| query | wall off | wall warm | CPU off | CPU warm | CPU saving | segments from cache |
|---|---|---|---|---|---|---|
| `service, count, avg, max … GROUP BY service` | 16.20 ms | 7.46 ms (2.2×) | 87.21 ms | 7.48 ms | **11.7×** | 22 of 24 |
| `attributes['user'], count … ORDER BY c DESC LIMIT 10` | 7.04 ms | 3.87 ms (1.8×) | 32.15 ms | 3.88 ms | **8.3×** | 22 of 24 |
| `count, sum … severity = 'ERROR' AND body LIKE …` | 9.53 ms | 4.47 ms (2.1×) | 48.58 ms | 4.55 ms | **10.7×** | 22 of 24 |

**Reading.**
- **CPU:** this is the headline. A refresh reads 2 of 25 row sets, the
  edge segment and the memtable, instead of 24, so it costs about a tenth
  of the CPU. Throughput for concurrent dashboards follows the CPU figure.
- **Wall time:** it improves less, because without the cache the 24
  segment scans run in parallel on the scan pool. The warm path is
  dominated by the edge segment and the memtable, scanned on one thread.
  Parallelizing the memtable scan would close more of the gap.

**Correctness.**
- **Results:** identical with the cache off and warm, which
  `tests/partial_cache.rs` asserts for every query shape, hit or miss.
- **Mutations caught:** ignoring the time part, ignoring the non-time
  conjuncts, and dropping a hit's merge.

**To do:** rerun on AC power with an idle machine.
