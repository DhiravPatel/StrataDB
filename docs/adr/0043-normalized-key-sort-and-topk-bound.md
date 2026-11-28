# ADR-0043: Normalized-key sorting, sorted segment runs, and a shared top-k bound

- **Status:** Accepted
- **Spec references:** VIII.14 (STRATA-SYS-14-001, 14-002, 14-003, 14-006)
- **Evidence:** `benchmarks/reports/2026-11-28-normalized-sort.md`

## Context
- **Comparison cost:** every `ORDER BY` sort compared rows with
  `cmp_position`, which walks each key's `Option<Value>` and dispatches on
  type for every comparison.
- **Concentrated sorting:** each segment's rows were concatenated on the
  query thread and sorted there.
- **Wasted materialization:** `ORDER BY ... LIMIT k` built the output values
  of every matching row, even the ones a later compaction threw away.
- **Not a total order:** `sort_cmp` compared an integer with a float
  through `as f64`, so `Int(2^53+1) = Float(2^53) = Int(2^53)` while
  `Int(2^53+1) > Int(2^53)`. Rust's sort may panic on such an order, and no
  key encoding can match it.

## Decision
### Total order for numbers
- `sort_cmp` compares integers and floats exactly: `int_float_cmp` compares
  the integer part, then the sign of the fraction.
- Predicates (`compare`) are unchanged; only ORDER BY uses it.

### Normalized keys (`stratadb_query::sort`, 14-001)
- **Layout:** 9 bytes per sort column, a tag byte then an 8-byte payload.
  - Tags follow the type ranks; numbers share one tag, and missing comes
    after null.
  - **Payloads:**
    - integers and floats: the IEEE total-order transform of the value as
      an `f64`, with `-0.0` folded into `0.0` and NaN placed last;
    - timestamps: big-endian with the sign bit flipped;
    - strings and bytes: 7 bytes plus `min(len, 8)`.
  - **DESC:** bitwise NOT of the whole column.
- **Shared prefixes:** strings and bytes leave out the prefix that every
  value of the column shares in the rows being sorted (`common_prefixes`).
  Dropping a shared prefix keeps byte order, and log values such as URLs
  and `service-NN` otherwise tie on every prefix.
- **Keys agree with the full order, but do not decide it:**
  - if `a` sorts before `b` then `key(a) <= key(b)`;
  - equal keys fall back to the full comparator;
  - a value is *exact* when equal keys prove equality: integers with
    magnitude up to 2^53, floats, timestamps, booleans, null, missing,
    and strings of up to 7 bytes after the shared prefix.
- **Inexact columns:** comparison stops at the first column that holds an
  inexact value. Bytes of later columns could contradict an undecided
  earlier one.
- **The result** is exactly the stable sort by the full comparator, ties by
  input position (property tests, boundary values pairwise, mutation
  checked).

### Algorithms (14-002, 14-006)
- **Indices, not rows, are sorted.** Each row moves once at the end.
- **Radix:** a stable LSD radix sort, skipping digits that are the same in
  every key, when n >= 4096 and the compared key is at most 16 bytes; a
  pdqsort over the permutation otherwise. Equal-key runs are then sorted by
  the full comparator when it can matter.
- **Small selections:** a top-k that keeps under a quarter of its rows
  (`NORMALIZED_MIN_KEEP_DIVISOR`) stays on comparison selection.
  - **Why:** encoding every row costs more than the roughly 2n
    comparisons of `select_nth` (measured at 0.6–0.9×).
  - **Large selections:** these select by key and radix-sort the kept part.

### Sorted runs and the loser tree (14-001)
- Each scan task sorts its segment's rows on the pool worker.
- The query thread merges the runs with a loser tree (`merge_runs`): log2 k
  comparisons per row, and ties go to the earlier run, so the result equals
  the stable sort of the concatenation.
- `Ctx::sorted_rows` marks the merged prefix. Memtable rows appended after
  it are sorted alone and merged with it.

### Shared top-k bound (14-003)
- **Invariant:** with `k` rows held, the k-th row's position (keys, id) is
  a bound. A row sorting *strictly* after it has `k` rows ahead of it under
  any tie order, so it cannot be in the result. Strictly, because ties
  across segments are decided by concatenation order.
- **Where the bound comes from:** the first `k` rows held give one at once
  (their maximum). Each compaction tightens it, and workers then compact
  from `2k` (at least 64) rows instead of 1024.
- **Sharing:** the tightest bound is published in `SharedBudget`, with a
  version so that workers re-read it under the mutex only when it changed.
  They check the version once per morsel.
- **Skipping:** a row's keys are evaluated first. A row beaten by the bound
  is skipped before any output is built (`QueryStats.topk_rows_skipped`).

### Switches and observability
- `QueryOptions::enable_normalized_sort` and `enable_topk_bound` are both
  default on; turning them off must never change results (tested in every
  combination).
- **Reporting:**
  - `sort_runs_merged` and `topk_rows_skipped` in query stats, the HTTP
    response and EXPLAIN ANALYZE;
  - a ledger operator, `top_k_order_by_string`;
  - a scenario query, `q17_top_k_by_string_keys`.

## Consequences
- **Measured** in-process, interleaved, on battery (see the report):
  - top-k queries 2.2–2.6× faster, almost all of it from the bound;
  - the sort kernel 1.8–9.4× on full sorts of 2k–100k rows, and 1.24–1.66×
    on large selections.
- **End to end:** full-sort queries are flat (0.93–1.06×) at 12k result
  rows, where the scan dominates.
- **Behaviour change:** ORDER BY over mixed integer and float values beyond
  2^53 now follows exact numeric order. It was ill-defined before.
- **Not done:**
  - an external sort (14-005; results are bounded by `max_result_rows`);
  - granule skipping against the bound for keys other than `event_time`;
  - normalized keys for the group top-k path (k is small there);
  - a width-based choice to carry payloads with the keys (14-006).
