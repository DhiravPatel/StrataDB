# Metadata-answered aggregates and aggregation on codes (spec VIII.9) on HW-REF-7

## Summary
Three changes (ADR-0037):
1. **Metadata answers (STRATA-SYS-09-006/007).**
   - **Which queries:** global `count(*)`, `count(event_time)`,
     `min(event_time)` and `max(event_time)`.
   - **Which sources:** any segment or memtable window whose metadata
     proves that the filter keeps all of its rows (time bounds), or exactly
     a known number of them (a severity filter, from the segment's exact
     severity counts). Such a source is not read.
2. **Aggregation on codes (STRATA-SYS-09-008).** When every `GROUP BY` key
   is a dictionary or one-byte code column, a dense per-batch array maps
   codes to groups, so each distinct key is hashed once per batch. With
   only `count(*)`, rows are counted per code.
3. **Group table accounting (STRATA-SYS-09-002/011).**
   - Table growth is reserved against the query's memory limit before it
     is allocated, and memory is then charged by capacity.
   - EXPLAIN ANALYZE reports the hash table.

| Measurement | Before | After |
|---|---|---|
| `SELECT count(*), min(event_time), max(event_time) FROM logs` (q14, process CPU) | 3.43 ms | 0.003 ms |
| `GROUP BY severity` (q12, process CPU) | 0.84–0.91 ms | 0.24–0.25 ms (3.4–3.8×) |
| ledger `group_by_low_cardinality` | 14.20 ns/row | 2.56 ns/row (5.5×) |
| ledger `count_from_metadata` (new) | 1.68 ns/row scanned | 0.034 ns/row answered |
| memtable m05 FATAL count | 2.90 ms | 2.43 ms (1.19×) |
| ledger `group_by_high_cardinality` | 61.3 ns/row | 63.4 ns/row (0.97×) |

- **Other queries:** within ±2% after one fix (see "Regression found and
  fixed").
- **High-cardinality GROUP BY:** consistently 2–3% slower in the ledger
  (0.97–0.98× across four rounds). Operators with untouched code moved
  0.97–1.02× between the same two builds, so this is close to the
  code-layout noise band, but it is reported as a possible cost.

## Setup
- **Binaries:** `strata-bench`, release builds.
  - **Base:** the P2-1 tree (workload classes, before this change).
  - **New:** this change.
- **Scenario A/B:** interleaved foreground pairs of `standard` (3 pairs
  base-first, plus 2 new-first as an order check) and `memtable` (3 pairs).
- **Ledger:** 3 interleaved rounds (`strata-bench ledger --path X --write`,
  20 runs each).
- **Machine:** Apple M4, macOS, load average 2–3.
- **q14:** measured from a copy of the scenario without the new assertion
  keys, because the base binary rejects keys it does not know.

These are developer-laptop measurements, not capacity claims.

## Standard scenario
Process CPU in ms (median of 10 repetitions, best of the pairs):

| Query | Base | New (first build) | New (fixed) |
|---|---|---|---|
| q04 service/severity count | 3.82 | 4.03 (0.95×) | 4.04 vs 4.00 (0.99×) |
| q06 LIKE count | 10.42 | 11.15 (0.93×) | 11.25 vs 10.79 (0.96×) |
| q07 attribute equality | 5.88 | 6.03 (0.97×) | 5.95 vs 6.45 (1.08×) |
| q08 high-cardinality GROUP BY | 3.82 | 3.99 (0.96×) | 4.04 vs 3.97 (0.98×) |
| q09 GROUP BY service, bucket | 3.43 | 3.60 (0.95×) | 3.58 vs 3.60 (1.01×) |
| q12 GROUP BY severity | 0.88 | 0.24 (3.61×) | 0.25 vs 0.84 (3.35×) |

"Fixed" is two further pairs after the fix; each cell shows new against
that session's base. The other queries were within 1% in every run.

**q05** (`count(*)` over a time range) is not answered from metadata here.
In this dataset every segment spans the whole time range, so no segment
is covered; it reads the same 25,000 rows as before.

### Regression found and fixed
- **The symptom:** the first build was 4–5% slower on parallel
  aggregates (q04, q06, q08, q09), in both run orders.
- **The cause:** every per-segment worker context computed the metadata
  plan (a filter walk and allocations) even though the top-level context
  had already answered every segment it could.
- **The fix:** the plan is computed in `run` only, and the split row
  loop's helpers are `#[inline]`.
- **Tried and reverted:** `#[inline(always)]` made the dense path slower
  (2.54 → 2.99 ns/row) without helping the high-cardinality path.

## Memtable scenario (best of 3)
| Query | Base | New |
|---|---|---|
| m04 GROUP BY service | 1.558 | 1.516 (1.03×) |
| m05 `severity = 'FATAL'` count | 2.899 | 2.426 (1.19×) |
| others | — | 0.96–1.01× |

Memtable strings are plain, not dictionary, so m04 cannot use code
aggregation. m05 gains because flushed segments answer from their severity
counts.

## Ledger (median of 3 rounds, CPU ns per unit)
| Operator | Base | New |
|---|---|---|
| `group_by_low_cardinality` | 14.20 | 2.56 (5.54×) |
| `count_from_metadata` (new) | — | 0.034 / row answered |
| `scan_filter_count` | 1.68 | 1.68 |
| `like_substring_count` | 55.45 | 55.20 |
| `top_k_order_by_time` | 139.1 | 140.2 (0.99×) |
| `group_by_high_cardinality` | 61.34 | 63.35 (0.97×) |
| codec and CRC operators (unchanged code) | — | 0.98–1.00× |

The ledger's kernel operators now run with metadata answers off, so they
still measure the kernels.

## Correctness evidence
- **`tests/metadata_aggregates.rs`:**
  - no-filter, covering time-filter and severity-count answers, with
    rows and bytes read checked;
  - min/max only when all of a source's rows match;
  - a non-canonical severity spelling is not answered;
  - non-qualifying shapes are not answered;
  - EXPLAIN reports the answered sources;
  - a 128-case property: random aggregates with time and severity filters
    give identical results and row counts with the optimization on and off.
  - A mutation of the time-coverage check fails it.
- **`tests/aggregation.rs`:**
  - `GROUP BY service, severity` equals an independent reference;
  - a 48-case property: dense equals hashed aggregation for random key
    sets, aggregates and filters, with and without `ORDER BY`, rows *and*
    order. A mutation of the combined-code computation fails it.
  - EXPLAIN ANALYZE reports the table's variant, groups, capacity and
    resizes;
  - 21,000 groups under a 256 KiB limit fail with `MEMORY_LIMIT_EXCEEDED`.
- **`groups::tests`:** across 50,000 inserts, what an insert allocates
  never exceeds the growth bound reserved for it.
- **Unchanged and passing:** query equivalence, parallel, pagination,
  memory and index suites, plus server and bench tests. Their expectations
  were updated where a plain `count(*)` now reads no rows.

## Reproduction
```bash
for r in 1 2 3; do
  for s in standard memtable; do
    base/strata-bench scenario benchmarks/scenarios/$s.toml --out ab/$s-base-$r --hw-profile HW-REF-7
    target/release/strata-bench scenario benchmarks/scenarios/$s.toml --out ab/$s-new-$r --hw-profile HW-REF-7
  done
  base/strata-bench ledger --path ab/lb$r.toml --write
  target/release/strata-bench ledger --path ab/ln$r.toml --write
done
```
Raw results are in `2026-11-27-hash-aggregation/` (`standard-base.toml` is
the copy without the new assertion keys), with `ledger-summary.txt`.
