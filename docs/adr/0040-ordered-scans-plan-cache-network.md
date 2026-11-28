# ADR-0040: Ordered scans with early stop, plan cache, connection options, estimate Q-error

- **Status:** Accepted
- **Spec references:** STRATA-SYS-14-003/004 (sort), 16-001/004 (caches),
  06-009/012 (network), 12-004 (estimates)

## Context
- **"Latest rows" queries** (`... ORDER BY event_time DESC LIMIT k`, the
  most common log query) read every unpruned segment, then kept the top k.
  Segments carry exact time ranges, and the memtable holds the newest rows.
- **Repeated queries** were parsed and planned again every time.
- **Client sockets** used default options.
- **Estimates:** the planner's row estimate was never compared with what
  queries actually did.

## Decision
### Ordered scans (`exec::scan_ordered`)
- **Eligibility:** non-aggregate queries with a LIMIT, no pagination, and
  `event_time` as the first ORDER BY key.
- **Visit order:** sources (segments and memtable windows) are visited
  best-first, newest maximum first for DESC and oldest minimum first for
  ASC.
- **Waves:** segments are read in waves of 1, 2, 4, ... up to the degree of
  parallelism, on the scan pool. The first source usually settles the
  k-th row; harder cases still reach full parallelism.
- **Stopping:** before each source, once k rows are kept, a source whose
  best possible time is *strictly* worse than the k-th kept row's is
  skipped, and so is every later one. Sources holding a tied value are
  still read, because a tie may win.
- **Identical results:** rows carry their position in the default scan
  order (source index, row index) as their identity, so ties break exactly
  as before.
  - Checked by a property test: random k, direction, secondary keys,
    filters, parallelism; data with heavy ties, overlapping and disjoint
    segment ranges, and memtable rows.
  - Mutation-checked: a non-strict skip condition fails it.
- **Reporting and switch:** `QueryStats.sources_skipped_by_order`, an
  EXPLAIN ANALYZE line and `stats.sources_skipped_by_order` in responses;
  `QueryOptions.enable_ordered_scan`.

### Plan cache (`stratadb_common::cache::ShardedLru`, server `plan_cache`)
- **The cache:** a generic sharded LRU with 64 shards, each with its own
  lock and counters.
  - Bounded by entries and bytes, with LRU by a per-shard logical clock.
  - Metrics: hits, misses, insertions, evictions, entries, bytes and a
    hit-latency histogram.
- **The plan cache:** maps the exact query text to an `Arc`'d `Prepared`
  (the `Plan` is shared, so a hit clones in O(1)). Texts over 64 KiB are
  not cached (the parser's limit).
  - **Sharing:** plans depend only on the text; tenant scoping happens at
    execution through the tenant's snapshot, so plans are shared across
    tenants safely.
  - **On a hit:** `parse_us` reports the lookup and `plan_us` is 0. The
    lifecycle still records PARSED and PLANNED.
  - **Per fingerprint:** hit/miss counters in 64 bounded shards export the
    top 100 by lookups.
- **Exposure:** `GET /api/v1/admin/caches`, `strata_plan_cache_*`, and
  `[limits] plan_cache_entries` (10,000) / `plan_cache_bytes` (64 MiB);
  0 disables.

### Connection options (`server::net::tune`)
- Accepted connections, plain and TLS, get `TCP_NODELAY` and TCP keepalive
  (60 s idle, 10 s interval, 6 probes), via `socket2`, already a tokio
  dependency.

### Estimate Q-error
- Every query records `est_rows` (unpruned segment rows plus memtable rows,
  from metadata) and `q_error = max(est/actual, actual/est)` against rows
  actually scanned. These go in the response stats and as `system.query_log`
  attributes (`est_rows`, `est_cpu_ms`, `q_error`).
- They also feed a histogram, `strata_query_estimate_q_error`.

## Consequences
- **Ordered scans:** "latest N" queries over time-ordered segments read
  only the newest sources. With overlapping ranges they cannot stop early
  but stay correct.
- **Q-error is high by design** for early-stopping and
  metadata-answered queries: the estimate is the rows a full scan would
  read. That makes the metric a measure of the work avoided as much as of
  estimation error. It is documented as such; per-operator estimates come
  with the cost model.
- **Not done:**
  - normalized-key radix sorts and loser-tree merges (14-001/002);
  - external sort (14-005);
  - a block cache and result cache (16-002/003);
  - `SO_REUSEPORT` listeners (06-008);
  - a columnar wire protocol (06-001).
