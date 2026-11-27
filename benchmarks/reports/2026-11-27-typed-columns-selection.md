# Typed string columns and selection vectors (spec STRATA-ARCH-03-005/006, ARCH-11-001) on HW-REF-7

## Summary
Four changes to how columns are held and how the executor walks a batch:
1. **`StrColumn`** replaces `Vec<Option<String>>`. It is either plain (one
   text buffer, offsets and a validity bitmap) or dictionary-encoded (the
   distinct values plus a `u32` code per row), and its buffers are
   reference-counted and immutable.
   - Segment `OptStr` chunks decode without an allocation per row.
   - `DictStr` chunks (`service`, `severity_text`) stay dictionary-encoded
     instead of cloning a `String` per row.
2. **Dictionary-aware kernels:**
   - **Filters:** a predicate on a dictionary column (`=`, `IN`, `LIKE`,
     ...) is evaluated once per distinct value, then looked up per row by
     code.
   - **GROUP BY:** a dictionary column, or the one-byte `severity`, encodes
     each distinct value's group key once and copies those bytes per row.
3. **Selections:** each 1,024-row chunk is reduced to a selection, either
   the dense range (no filter, no page bound, nothing materialized) or the
   selected row indices. Downstream work visits only selected rows, and rows
   are never copied or compacted.
4. **Global `count(*)`** (no GROUP BY) adds selection sizes instead of
   updating an aggregation state row by row.

**Setup:**
- **Binaries:** the parent tree (the columnar memtable) against this
  change, both in release builds.
- **Ordering:** interleaved foreground pairs; the third standard pair ran in
  reversed order.
- **Conditions:** A/A spread within the baseline was 1.06 for the standard
  scenario. Two earlier rounds, taken under heavier load (spread 1.3–1.9),
  showed the same direction and were used to find two regressions, which are
  fixed (see below).

## Standard scenario (segments)
Process CPU in ms. Each figure is the median of 10 repetitions, best of 3
runs:

| Query | Before | After | Speed-up |
|---|---|---|---|
| q01 trace_id lookup | 2.777 | 2.758 | 1.01× |
| q02 request_id lookup | 1.648 | 1.621 | 1.02× |
| q03 recent errors (prints `service`) | 1.462 | 1.216 | 1.20× |
| q04 `service = 'auth' AND severity = 'WARN'` | 4.502 | 3.853 | 1.17× |
| q05 time range count | 1.316 | 1.244 | 1.06× |
| q06 `count(*) WHERE body LIKE` | 10.384 | 10.352 | 1.00× |
| q07 `count(*) WHERE attributes['region'] = …` | 5.883 | 5.518 | 1.07× |
| q08 high-cardinality GROUP BY | 3.831 | 3.768 | 1.02× |
| q09 GROUP BY service, time bucket | 4.284 | 3.298 | **1.30×** |
| q10 top error codes | 26.264 | 25.697 | 1.02× |
| q12 GROUP BY severity | 2.417 | 0.832 | **2.91×** |

The geometric mean speed-up is **1.183×**. Plans and results were identical
in all runs.

## Memtable scenario (200k unflushed rows)
Memtable string columns are plain (built from the memtable's cells), so the
dictionary kernels do not apply. The gains come from selections, count, and
the severity and string group keys.

| Query | Before | After | Speed-up |
|---|---|---|---|
| m01 trace_id lookup | 5.293 | 5.083 | 1.04× |
| m02 recent errors | 1.508 | 1.258 | 1.20× |
| m03 older time range | 0.860 | 0.787 | 1.09× |
| m04 GROUP BY service | 2.184 | 1.565 | **1.40×** |
| m05 severity = FATAL count | 2.784 | 2.843 | 0.98× |
| m06 LIMIT 20 | 0.559 | 0.555 | 1.01× |

- **Geometric mean:** 1.111×.
- **Dataset load** varied in both arms (4.0–6.2 µs CPU per event) with no
  consistent difference. Ingest code is unchanged.

## Operator cost ledger (single thread, CPU ns per row, 20 runs each, 3 interleaved rounds)
| Op | Before | After | Speed-up |
|---|---|---|---|
| `count(*) WHERE severity = 'ERROR'` | 3.3 | 1.7 | **2.01×** |
| GROUP BY severity | 37.3 | 13.5 | **2.77×** |
| ORDER BY event_time LIMIT (top-k) | 151.9 | 138.1 | 1.10× |
| `count(*) WHERE body LIKE` | 55.6 | 54.0 | 1.03× |
| high-cardinality GROUP BY | 60.6 | 59.8 | 1.02× |
| codecs and CRC | — | — | 0.98–1.01× |

`perf/cpr_ledger.toml` is updated.

## Regressions found and fixed during measurement
1. **UTF-8 re-validation:** plain string cells checked UTF-8 on every
   access (`from_utf8`).
   - The text is now stored as one `String` with char-boundary offsets, so
     an access checks only the two ends, in O(1) and without `unsafe`.
   - Seen as q12, which reads row strings, running 0.87–0.92×.
2. **A materialized selection with no filter:** a filter-less chunk still
   built an index vector and read it back, about 4 ns/row. GROUP BY severity
   measured 0.904× in the ledger before the fix.
   - Dense chunks now iterate the row range directly.
   - The severity group key was also pre-encoded, which previously allocated
     a `String` per row. Together these turned the op into a 2.77×
     improvement.
3. **Page checks:** the selection loop checked the page bound per row even
   without pagination, leaving q06 at 0.95×. A tight loop over the filter's
   truth vector handles the common case.

## Correctness evidence
- **`stratadb_schema::column` tests:**
  - plain and dictionary columns hold the input values for random data;
  - equality holds across the two representations;
  - edges: empty columns, empty strings vs missing, bitmap word boundaries,
    out-of-range codes.
- **Vectorized vs scalar filter property** (`vector.rs`): odd string columns
  are now dictionary-encoded and even ones plain, so the dictionary filter
  kernel is checked row for row against the scalar evaluator.
- **Group-key property** (1,000 cases): the borrowed group-key encoder is
  byte-identical to encoding the evaluated value, for every column. This
  covers the dictionary keys and the severity key, including invalid
  severity codes.
- **Unchanged and passing:**
  - the query suites: random-query equivalence with pruning,
    vectorization and parallelism on and off; pagination, which exercises
    page-bounded selections; memory accounting;
  - storage, segment and server suites.

These are developer-laptop measurements (Apple M4, macOS), not capacity
claims.

## Reproduction
```bash
# base: the parent tree's strata-bench
for r in 1 2 3; do
  base/strata-bench scenario benchmarks/scenarios/standard.toml --out ab/standard-base-$r --hw-profile HW-REF-7
  target/release/strata-bench scenario benchmarks/scenarios/standard.toml --out ab/standard-new-$r --hw-profile HW-REF-7
done
base/strata-bench ledger --path lb.toml --write; target/release/strata-bench ledger --path ln.toml --write
```
Raw results are in `2026-11-27-typed-columns-selection/`, with the summaries.
