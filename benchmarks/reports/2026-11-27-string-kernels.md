# String search kernels and adaptive conjunctions (spec VIII.8) on HW-REF-7: kernel measurements

## Summary
| Kernel | Before | After |
|---|---|---|
| ILIKE `'%timeout%'`, 45-byte rows | 316–392 ns/row (general matcher) | 14.6–16.8 ns/row (about 20×) |
| ILIKE `'%timeout%'`, 1 KiB rows | 0.21–0.55 GB/s | 4.8–11.0 GB/s |
| LIKE `'%TIMEOUT%'`, 45-byte rows, direct | 8.1–8.2 ns/row (`str::contains`) | 7.7–7.9 ns/row (`memmem`) |
| LIKE `'%TIMEOUT%'`, 1 KiB rows | 12–34 GB/s | 13–36 GB/s (equal) |
| `NOT ILIKE … AND severity = 'FATAL'`, filter cost | 21.4–23.0 ns/row (written order) | 3.4–3.8 ns/row (adaptive; about 6×) |
| UTF-8 validation (std), ASCII JSON / ~2% multi-byte | — | 45–46 / 10.3–10.7 GB/s |

These are **kernel-level** measurements, taken in one process so old and
new code run back to back. The end-to-end scenario A/B is **pending**.

## Why the end-to-end A/B is pending
The scenario and ledger A/B were run three times, interleaved and in
alternating order, against a baseline tree. That tree is this one with
the P2-3 hot-path changes reverted: the LIKE fast paths, the n-ary
adaptive `AND`, plan-time folding and the lazy bitmap.

The results are unusable:
- the per-round ratios for unchanged queries ranged from 0.3 to 1.7;
- `crc32c` read 64 ns/KiB, against 33 in earlier sessions.

The laptop was on battery power (54%, discharging), and macOS throttles
CPU unpredictably on battery. Only the ILIKE gain was clear through the
noise (ledger 256 → 137 ns/row, 1.87×; scenario q16 1.35×). The A/B must
be repeated on AC power before any query-level claim, and NEXT_SESSION
lists it first.

**Why query-level effects will be smaller than the kernel numbers:** LIKE
queries are dominated by reading and decoding the `body` column.
- `count(body)`, which reads and clones bodies, costs more per row than
  the LIKE query itself.
- q15 (`body LIKE … AND severity = 'FATAL'`) still decodes every body.

## Measurements (Apple M4, macOS, on battery, release build)
- **Substring search:**
  `cargo test --release -p stratadb-query eval::tests::substring_throughput -- --ignored --nocapture`.
  Three runs; rows are synthetic log lines (about 45 bytes) or 1 KiB
  lines, with 2% matching.
- **Conjunctions:**
  `cargo test --release -p stratadb-query vector::tests::conjunction_order_cost -- --ignored --nocapture`.
  1,024-row batches with 1% FATAL rows and 45-byte bodies, 2,000
  evaluations, three runs. The filter is written expensive-first.
- **UTF-8:**
  `cargo test --release -p stratadb-common codec::tests::utf8_validation_throughput -- --ignored --nocapture`.
  Two runs over 1 MiB buffers.

### Corrections made while measuring
- **A confounded comparison:** a first comparison suggested `memmem` was
  25% slower than `str::contains` on short rows. It had compared `memmem`
  inside `LikePattern::matches` (call overhead about 2 ns/row) with a
  direct `contains`. Measured directly, `memmem` is slightly faster, and
  it is kept.
- **ILIKE on short rows:** lowercasing a copy of every row cost 29–39
  ns/row on short rows. A copy-free first-byte scan costs about 10.5
  ns/row there, but only about 2 GB/s on 1 KiB rows, so ILIKE now switches
  at 256 bytes.

## Correctness evidence
- **LIKE fast paths:**
  - `eval::tests::like_fast_path_equals_general` (Unicode alphabet
    including É and the Kelvin sign);
  - `substring_search_equals_general` (2,000 cases, mutation-checked);
  - `long_text_folded_search_equals_general` (the lowercased-copy path);
  - `ascii_folding_edges`.
- **Adaptive order:**
  - `vector::tests::vectorized_equals_scalar` now evaluates each random
    filter 24 times, across re-rankings, and compares every row (2,000
    cases);
  - `conjunctions_learn_to_run_cheap_selective_predicates_first`:
    measurement corrects a wrong static guess, and the expensive conjunct
    then runs only on rows that survive.
- **Folding:**
  - `vector::tests::folding_preserves_truth` (2,000 cases with literal
    leaves and duplicates, mutation-checked);
  - `plan::tests::filters_are_folded_and_deduplicated`.
- **No-null columns:** `column::tests::no_missing_rows_means_no_bitmap`,
  plus the existing plain/dictionary column properties.
