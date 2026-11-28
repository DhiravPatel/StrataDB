# ADR-0045: Per-segment partial aggregate cache

- **Status:** Accepted
- **Spec references:** STRATA-ARCH-09-002 (partial result caching per
  part), STRATA-ARCH-09-003 (invalidation by part identity only)
- **Evidence:** `benchmarks/reports/2026-11-28-partial-cache.md`

## Context
- **Repeated work:** dashboards rerun the same aggregate ("count by service
  over the last 24 h") every few seconds, and each run rescans every
  segment in the window.
- **Why it can be reused:**
  - aggregates already run as one partial per segment (a group table of
    aggregate states), merged in segment order;
  - flushed segments are immutable.
- **The goal:** a partial computed once can be reused until the segment is
  rewritten, turning a refresh from O(data) into O(new data).

## Decision
### Segment identity (`ScanSource::identity`)
- `SegmentHandle` gets a process-unique `uid` from a global counter when it
  is opened. Snapshots share the handle, so the uid is stable for the
  segment's lifetime.
- **Rewrites:** compaction, retention and reopening yield new handles, so
  new uids (09-003: identity, never time).
- **Memtable windows** can change, so they have no identity and are always
  scanned.

### Key (`partial_cache`)
- **Query part:** a canonical text of tenant, table, `GROUP BY`, aggregates,
  and the filter's conjuncts other than `event_time` bounds
  (`plan::TimeSplit`). It is stored in the entry and compared on every hit,
  so a 64-bit slot collision can never return another query's state.
- **Time part:**
  - **Covered:** when the segment's time range lies inside the filter's
    `event_time` bounds, every time conjunct is true for every row
    (`TRUE AND x = x`). The time part is then empty, so a window that
    moves keeps hitting every segment it fully covers.
  - **Edge segments:** they key on the exact bounds.
- **Segment uid.**
- **Not cached:** EXPLAIN, paginated queries, non-aggregates, and partials
  answered from metadata (cheaper than a hit).

### Value and merge
- **Value:** `FrozenGroups`, the group table's key arena, spans and states
  in first-seen order, without the seeded hash index. Each query has a
  random hash seed.
- **Merge on a hit:** the groups are rehashed into the query's table and
  merged with `AggState::merge` in the slot's segment order, exactly where
  the scanned partial would have merged. Results are therefore identical
  hit or miss, including float sums and group order (tested in every
  combination; three mutations caught).
- **Sequential scans:** with a cache, aggregates always use per-segment
  partials, even at one worker. Without one, the cost model may scan small
  aggregates sequentially, whose float sums can differ in the last bits;
  this is the same documented property as changing `parallelism`.

### Bounds and accounting
- **Bounds:** `ShardedLru` (SLRU, byte bounded); entries over 1/16 of the
  capacity are skipped.
- **Accounting:** bytes are charged to the cache memory region before
  insertion (skipped when it is full) and released on eviction and when the
  cache drops (tested).
- **Ownership:** a hit reads no data, so the scan task re-checks
  `owned_by(tenant)` itself before using one (the non-negotiable that
  storage re-checks tenant ownership).

### Configuration and observability
- **Configuration:**
  - `QueryOptions::partial_cache` (default `None`);
  - the server's `[limits] partial_cache_bytes` (default 0, capped at the
    cache memory region);
  - the bench's `[engine] partial_cache_bytes`.
- **Observability:**
  - `partials_from_cache` and `rows_from_cache` in query stats and the
    HTTP response;
  - a `partial_cache` entry in `/api/v1/admin/caches`;
  - the `strata_partial_cache_*` metrics.

## Consequences
- **Speed:** repeated dashboards rescan only the memtable, edge segments
  and new segments (measured in the report).
- **Accounting:** `rows_scanned` excludes cached rows. The plan-time row
  estimate does not, so the Q-error of cached queries reads high.
- **Not done:**
  - a whole-query result cache (09-001);
  - caching for row queries;
  - sharing partials between queries that differ only in their output
    expressions.
