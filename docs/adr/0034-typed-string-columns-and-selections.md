# ADR-0034: Typed string columns, dictionary kernels and selection vectors

- **Status:** Accepted
- **Spec references:** STRATA-ARCH-03-005, STRATA-ARCH-03-006,
  STRATA-ARCH-11-001, STRATA-ARCH-00-002 ("do less work"); ADR-0006
  (executor), ADR-0033 (columnar memtable)

## Context
- **String columns** were `Vec<Option<String>>`:
  - decoding a segment allocated one `String` per row;
  - dictionary chunks (`DictStr`) were expanded by cloning the dictionary
    value into every row;
  - filters compared every row's string.
- **The batch loop** walked every row, even when only a few matched, and a
  plain `count(*)` updated an aggregation state per matching row.

## Decision
### `StrColumn` (`stratadb_schema::column`, the shared types)
- **Plain representation:** one text buffer (`String`), `u32` offsets on
  char boundaries, and a validity bitmap. Reading a row checks only the two
  boundaries, with no re-validation and no `unsafe`.
- **Dictionary representation:** `Vec<String>` plus one `u32` code per row,
  where 0 is missing.
- **Buffers:** reference-counted (`Arc`) and immutable, so a clone is O(1).
- **Equality** is by value, across representations.
- **Producers:**
  - segment `OptStr` chunks decode to plain columns, with one copy of the
    bytes and no allocation per row;
  - `DictStr` chunks stay dictionary-encoded;
  - memtable windows build plain columns.
- **Consumers:**
  - `ColumnData::Strs(StrColumn)` replaces the old vector;
  - `into_strs()` still yields owned values, for row materialization.

### Dictionary kernels
- **Vectorized filters:** a dictionary column's predicate is evaluated once
  per distinct value, plus once for missing, into a truth table indexed by
  code. This is used when the dictionary is no larger than the rows being
  evaluated.
- **GROUP BY keys:** dictionary columns, and the one-byte `severity` column
  through a 256-entry table, encode each distinct value's group key once.
  Rows then copy those bytes, byte-identical to the generic encoding.

### Selections (`exec.rs`)
- **Per chunk:** each 1,024-row chunk yields a selection, never a compacted
  copy:
  - `Dense(range)` when nothing can drop rows (no filter, no page bound);
  - otherwise `Rows(indices)`, built from the vectorized filter's truth
    vector, or the scalar evaluator, plus the page's `seq <= as_of` bound.
- **Downstream work** (aggregation, projection, sort keys) visits only the
  selected rows.
- **`rows_scanned`** keeps its row-at-a-time meaning, including when a
  `LIMIT` stops mid-chunk.
- **Global `count(*)` without GROUP BY** adds selection sizes.

## Consequences
Measured in interleaved foreground A/B pairs (report
`2026-11-27-typed-columns-selection`):
- **Standard scenario:** 1.18× geometric mean.
  - GROUP BY severity 2.91×;
  - GROUP BY service and time bucket 1.30×;
  - `service` filters 1.17–1.20×;
  - nothing slower.
- **Memtable scenario:** 1.11×.
- **Ledger:** global count 2.01×, low-cardinality GROUP BY 2.77×, top-k
  1.10×.
- **Regressions caught on the way:** three, by measuring (UTF-8
  re-validation, materialized dense selections, per-row page checks), all
  fixed and recorded in the report.
- **Not yet:**
  - numeric and value columns are still `Vec<i64>` / `Vec<Option<Value>>`;
  - there is no null bitmap for values, and no vector pool;
  - zero-copy slicing is not used: no operator slices yet;
  - the selection does not travel between operators, because there is no
    multi-operator pipeline yet (ARCH-03-001);
  - memtable string columns are not dictionary-encoded;
  - dictionary filter tables are rebuilt per 1,024-row call.
