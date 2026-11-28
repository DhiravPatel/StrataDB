# Normalized-key sort and shared top-k bound: 2026-11-28 (ADR-0043)

**Conditions.**
- **Machine:** the development laptop (Apple Silicon), **on battery** with
  a load average of about 3.6, so absolute numbers are inflated and noisy.
- **Method:** each comparison runs both arms inside one process,
  interleaved with the order alternating, and reports the median. These
  ratios are the evidence; absolute times are not.
- **Noise floor:** `group_topk` does not touch the changed code, and its
  ratios varied between 0.93× and 1.00×. Treat changes within about ±7% as
  noise.

## 1. Sort kernel: `sort_keep` comparison vs normalized
`cargo test --release -p stratadb-query --lib sort_kernel_ab -- --ignored --nocapture`.
Synthetic rows, 11 rounds each, in ns per input row:
- timestamp: random within 1 h;
- string: `GET /api/v1/items/N`, with N up to 100k, DESC;
- str+time: 40 services, then time DESC;
- int: 0..10k.

This is the final version, with common-prefix stripping and radix on large
selections. Rows with a small `keep` (100, n/8) use comparison selection on
both arms, and measured 0.99–1.02×.

| keys | n | keep | comparison | normalized | speedup |
|---|---|---|---|---|---|
| timestamp | 2,000 | all | 209.8 | 118.5 | 1.77× |
| timestamp | 12,000 | all | 143.8 | 17.2 | 8.35× |
| timestamp | 100,000 | all | 186.7 | 39.1 | 4.77× |
| timestamp | 100,000 | 50,000 | 89.2 | 58.9 | 1.51× |
| string | 2,000 | all | 167.2 | 61.1 | 2.74× |
| string | 12,000 | all | 232.0 | 32.9 | 7.05× |
| string | 100,000 | all | 305.1 | 48.9 | 6.24× |
| string | 100,000 | 50,000 | 135.3 | 81.5 | 1.66× |
| str+time | 12,000 | all | 274.1 | 92.3 | 2.97× |
| str+time | 100,000 | all | 356.0 | 136.1 | 2.62× |
| str+time | 100,000 | 50,000 | 187.6 | 119.7 | 1.57× |
| int | 12,000 | all | 143.6 | 21.9 | 6.57× |
| int | 100,000 | all | 167.3 | 36.7 | 4.56× |
| int | 2,000 | 1,000 | 57.5 | 66.4 | 0.87× |

**Findings that shaped the design** (earlier runs in this session):
- **Small selections lose.** Before the keep-ratio rule, normalized
  selections of 100 rows out of n measured 0.59–0.91×, because encoding
  every row costs more than `select_nth`'s comparisons. Such selections
  now stay on comparison selection (`NORMALIZED_MIN_KEEP_DIVISOR = 4`).
- **Shared string prefixes.** Before common-prefix stripping, the string
  case was 0.91–1.20×: every key tied on `GET /api/`. It is now 2.7–7×.
- **Remaining regression:** n = 2,000 with keep = 1,000 on integers is
  0.87×. That is below the radix threshold, and pdqsort over the
  permutation plus the gather costs more than sorting the rows directly.

## 2. Queries: the three arms
`cargo test --release -p stratadb-query --test sort sort_ab -- --ignored --nocapture`.
- **Data:** 8 batches of 60k rows (7 flushed segments plus the memtable),
  480k rows.
- **Settings:** default parallelism, ordered scan off.
- **Measure:** the median of 9 rounds, in ms.
- **Arms:** both off, normalized sort only, normalized sort plus the bound.

| query | off | sort | sort + bound | rows skipped |
|---|---|---|---|---|
| `ORDER BY service, event_time DESC LIMIT 100` | 59.68 | 56.62 (1.05×) | 27.43 (**2.18×**) | 470,562 |
| `ORDER BY body LIMIT 1000` | 95.37 | 83.47 (1.14×) | 36.66 (**2.60×**) | 449,870 |
| `ORDER BY attributes['latency_ms'] DESC LIMIT 10000` | 132.14 | 94.40 (1.40×) | 61.21 (**2.16×**) | 248,035 |
| `WHERE service = … ORDER BY event_time` (about 12k rows) | 12.32 | 13.25 (0.93×) | 11.89 (1.04×) | 0 |
| `WHERE … ORDER BY attributes['latency_ms'], event_time` | 33.77 | 33.44 (1.01×) | 31.93 (1.06×) | 0 |
| group top-k (unchanged path) | 61.19 | 62.40 (0.98×) | 61.04 (1.00×) | 0 |

**Reading.**
- **Top-k:** the bound is the large win. More than 90% of rows are
  rejected on their keys alone, before any output value is built.
- **Full sorts:** these are scan-bound at these sizes, so the faster kernel
  is within noise end to end.
- **Pending:** a rerun on AC power with an idle machine, together with the
  pending P2-3/P2-4/P3 A/B. The ledger now includes `top_k_order_by_string`.
