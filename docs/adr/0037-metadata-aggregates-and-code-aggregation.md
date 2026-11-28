# ADR-0037: Aggregates from metadata, aggregation on codes, group-table accounting

- **Status:** Accepted
- **Spec references:** spec VIII.9 (STRATA-SYS-09-002, 09-006, 09-007,
  09-008, 09-011); ADR-0034 (typed string columns), ADR-0030 (memory
  accounting)

## Context
- **Global aggregates read every column chunk they touched**, although
  segment footers record exact row counts, the `event_time` range and
  per-severity row counts.
- **`GROUP BY` on dictionary columns** (`service`, `severity_text` in
  segments) or on the one-byte `severity` still built key bytes, hashed and
  probed once per row. Their keys were only pre-encoded per distinct value
  (ADR-0034).
- **The group table** (hashbrown's SwissTable: load factor 7/8, growth
  ×2) grew without reserving memory first. Its memory was charged as a
  per-group estimate, and nothing reported it.

## Decision
### Metadata answers (`exec::MetaPlan`, `plan::MetaFilter`)
- **Which queries:**
  - no `GROUP BY` and no pagination;
  - every aggregate is `count(*)`, `count(event_time)`, `min(event_time)`
    or `max(event_time)`;
  - the filter is a `MetaFilter`: `AND` of `event_time` comparisons with
    timestamp literals (not `!=`) and `severity = 'NAME'` /
    `severity IN (...)` with canonical names. Evaluation compares names
    exactly, so `'error'` is not canonical and is not answered.
- **Per source** (a segment, or a memtable window):
  - if the source's exact time range lies inside the filter's bounds, the
    matching count is its row count. With a severity filter, the count is
    the sum of its exact `severity_counts` for the listed severities.
    Segments provide those counts, and only when they add up to
    `row_count`; memtable windows do not, so they are read;
  - `min`/`max(event_time)` come from the time range, and only when every
    row matches. A source with no matching row contributes nothing;
  - anything else is read.
- **When it runs:** `run` answers segments before choosing parallelism, so
  workers never start for them, and memtable windows are checked as they
  are scanned. Per-segment worker contexts carry no metadata plan.
- **Reporting:** `QueryStats.sources_from_metadata`, an EXPLAIN line, and
  `stats.sources_from_metadata` in HTTP responses.
- **Switch:** `QueryOptions.enable_metadata_aggregates`; disabling it
  never changes results.
- **Not answered: `GROUP BY severity` from metadata.** Groups without
  `ORDER BY` come out in first-seen row order, and ties under `ORDER BY`
  are broken by it, and metadata cannot reproduce that order.

### Aggregation on codes
- **When it applies:** every group key is a dictionary column
  (`KeySrc::Dict`) or a byte code column (`KeySrc::Byte`), and the product
  of their code spaces is at most 65,536.
- **How it works:** a per-batch dense array maps the combined code to a
  group index. A code's group is looked up, or created, the first time the
  code appears in the batch, which keeps first-seen order; later rows
  reuse it without hashing.
  - With only `count(*)`, rows are counted per code and added at the end
    of the batch.
  - A code outside its dictionary falls back to the regular path, which
    reports the error.
- **Reporting and switch:** `QueryStats.dense_group_batches`, and
  `QueryOptions.enable_dense_groups` (rows and order are unchanged with it
  off).

### Group-table accounting and statistics
- **Reserve before growing:** `GroupTable::growth_bytes(key_len)` is an
  upper bound on what the next insert allocates: the table's buckets and
  every array at capacity doubling. It is reserved through the query's
  accountant before the insert. A query whose groups exceed its memory
  limit fails with `MEMORY_LIMIT_EXCEEDED` before allocating past it.
- **Charge by capacity:** memory is then charged as the actual change in
  capacity-based heap bytes, plus 16 bytes per aggregate state for the
  states' own heap, instead of a per-group guess.
- **Statistics:** EXPLAIN ANALYZE and `QueryStats.group_table` report the
  variant, groups, capacity, resizes, bytes, dense batches and 0 spilled
  partitions.

## Consequences
- **Measured** (report 2026-11-27-hash-aggregation):
  - a plain `count(*), min, max` over 50,000 rows costs 0.003 ms of CPU
    instead of 3.4 ms;
  - `GROUP BY severity` is 3.4–3.8× faster (ledger 5.5×);
  - the metadata count path costs 0.034 ns per row answered;
  - high-cardinality `GROUP BY` may be 2–3% slower. This is near the
    noise band but consistent.
- **Not done:**
  - **Probe statistics:** probe length and the probe-vs-insert time
    fraction (09-011) would need instrumented probes; hashbrown does not
    expose probe lengths.
  - **Incremental rehash (09-002):** hashbrown rehashes all at once, not
    in batches.
  - **TwoLevel tables and parallel merge by sub-table (09-003).**
  - **Batched probes with prefetch (09-004):** the prefetch intrinsics
    need `unsafe`, which the workspace forbids.
  - **Streaming aggregation on sort-key prefixes (09-009).**
  - **Abandoning partial pre-aggregation (09-010).**
- **API change:** `QueryStats` gains `sources_from_metadata`,
  `dense_group_batches` and `group_table`; `QueryOptions` gains
  `enable_metadata_aggregates` and `enable_dense_groups`. A plain
  `count(*)` now reports `rows_scanned` 0.
