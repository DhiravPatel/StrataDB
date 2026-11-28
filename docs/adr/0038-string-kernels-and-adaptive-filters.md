# ADR-0038: String search kernels, adaptive conjunctions, constant folding, no-null columns

- **Status:** Accepted
- **Spec references:** spec VIII.8 (STRATA-SYS-08-011, 08-012, 08-013,
  08-014, 08-015, 08-016, 08-018, 08-019); ADR-0034 (typed string columns)

## Context
- **ILIKE** always took the general matcher. It builds a `Vec<char>` per
  row with Unicode lowercasing, which costs about 330 ns per 45-byte log
  body.
- **`AND`** evaluated its operands in written order, so `body LIKE … AND
  severity = 'FATAL'` ran the expensive predicate on every row.
- **The planner** did no constant folding.
- **Plain string columns** always allocated and checked a validity bitmap.
- **UTF-8 validation** on ingest used std, with no measured throughput.

The workspace forbids `unsafe`, so SIMD must come from dependencies.

## Decision
### Substring search (`eval::LikePattern`)
- **Case-sensitive literal patterns** (`x`, `x%`, `%x`, `%x%`): exact,
  prefix and suffix compare lengths before bytes. `%x%` uses a prebuilt
  `memchr::memmem::Finder`.
  - `memchr` is already in the lock file through aho-corasick and regex,
    and has no dependencies. Making it direct adds no crate.
  - Measured: 7.7–7.9 ns per short row, against 8.1–8.2 for
    `str::contains`; 15–20 GB/s on 1 KiB rows.
- **Case-insensitive patterns whose literal is ASCII,** when the text is
  ASCII:
  - exact, prefix and suffix use `eq_ignore_ascii_case`;
  - `%x%` scans in place for the needle's first byte in either case
    (`memchr2`) for text up to 256 bytes;
  - longer text is lowercased into a per-thread scratch buffer
    (vectorized), then searched with `memmem`. 256 bytes is the measured
    crossover: the copy costs more than it saves on short text.
- **Non-ASCII text, or a non-ASCII literal,** takes the general matcher,
  so Unicode folding is unchanged; for example, the Kelvin sign folds to
  `k` as before.
- **Correctness:** property tests (2,000 cases, plus long texts) compare
  every fast path with the general matcher, and are mutation-checked.

### Adaptive conjunctions (`vector::Conjunction`, STRATA-SYS-08-019)
- **Flattening:** an `AND` chain compiles to one node. Each conjunct runs
  on the rows the earlier ones did not reject (false).
- **Ranking:** per conjunct, the filter measures ns/row (exponential
  average) and the fraction of rows kept, and ranks by
  `cost / (1 − kept)`. The order is recomputed every 8 batches.
- **Initial order:** a static per-node cost estimate (severity table 0.5,
  int compare 1, plain string 2, body LIKE 30, scalar fallback 60, ...)
  with an assumed half of rows kept.
- **Correctness:** three-valued `AND` is commutative, so order never
  changes a result. The vector-against-scalar property test now evaluates
  each filter 24 times, across re-rankings, and compares every row.
- **Mutability:** `Filter::eval` takes `&mut self`. Each worker holds its
  own clone, so there are no locks and statistics are per worker.
- **Reporting:** `QueryStats.filter_reorders`, and a line in EXPLAIN
  ANALYZE.

### Constant folding (`plan::fold`, STRATA-SYS-08-018)
- **What folds:** comparisons, `IN`, `LIKE`, `IS [NOT] NULL` /
  `IS [NOT] MISSING` and `time_bucket` over literals; `AND`/`OR`
  absorbing and identity elements; `NOT` of a literal.
- **Duplicates:** repeated operands of an `AND`/`OR` chain are kept once.
- **Where it applies:** it runs on the filter before pruning, metadata
  answers and execution. A filter that folds to `true` is removed.
- **Correctness:** a property test checks that a folded filter evaluates
  identically to the original on every row of random batches; it is
  mutation-checked.

### No-null string columns (STRATA-SYS-08-011)
- `StrBuilder` allocates the validity bitmap only at the first missing row,
  back-filling earlier rows as present.
- `StrColumn::null_count()` is exposed. The vector filter's
  plain-string leaf picks a no-presence-check loop once per batch when it
  is 0.

### UTF-8 validation (STRATA-SYS-08-016)
- std's `str::from_utf8` is measured at 45–46 GB/s on ASCII JSON and
  10.3–10.7 GB/s with about 2% multi-byte text (Apple M4). This meets the
  floor without a new dependency.
- It is a word-at-a-time ASCII path, not a SIMD validator.

## Consequences
- **Measured** (in-process microbenchmarks, report
  2026-11-27-string-kernels):
  - ILIKE substring search: about 20× faster on short rows, and 5–11 GB/s
    on 1 KiB rows (from 0.2–0.5);
  - a misordered conjunction costs about 6× less (3.6 against 22 ns/row
    of filter);
  - case-sensitive LIKE is unchanged.
- **Query level:** LIKE queries are dominated by reading the `body`
  column, so the filter savings show less there. End-to-end A/B was taken
  on battery power, where results were unusable, and is pending on AC
  power.
- **Not done:**
  - expression bytecode (08-017);
  - ISA dispatch and SIMD kernels of our own (08-002/003), which need
    `unsafe` or a vetted crate;
  - selection bitmaps (08-005/006);
  - German-string views (08-012, a MAY);
  - a SIMD UTF-8 validator (`simdutf8`), to adopt if HW-REF-1 measures std
    below 10 GB/s.
