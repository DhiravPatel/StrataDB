# ADR-0029: `system.query_log`, query fingerprints and per-query CPU time

- **Status:** Accepted
- **Spec references:** §17 (system tables), STRATA-SYS-01-007 (`plan_us`),
  STRATA-SYS-11-006 (`queue_us` in the log and response),
  STRATA-SYS-13-009 (fingerprint, top-N by CPU), `obs.query_log*`
  settings; ADR-0028 (Catalog and ScanSource)

## Context
Each query already reported `queue_us`, `parse_us`, `plan_us` and `exec_us`
in its response body and in a `query` log line. The spec asks for more:
- a queryable `system.query_log`;
- a normalized 64-bit fingerprint per query, so that the workload can be
  ranked by total CPU per query shape;
- `queue_us` in the response headers as well as the body.

Per-query CPU time was not measured at all.

## Decision
### Storage
- The log lives in a second storage engine at `<data_dir>/system/query_log`.
  It shares the main engine's I/O engine, encryption key and disk reserve.
- Each query becomes one event, stored under the tenant that ran it:
  - `event_time`: arrival time;
  - `event_id`: the query id;
  - `severity`: INFO for success, ERROR for failure;
  - `body`: the normalized text;
  - `attributes`: the measurements, as typed integers.
- The table therefore has the event schema, and everything already built
  applies: the executor, pruning, retention (default 7 days), Bloom filters on
  `query_id` and `fingerprint`, and tenant scoping.
- A dedicated system-table store with its own schema was rejected: it would
  need a second executor path before there is a typed-schema table engine.

### Querying
- The parser accepts `FROM logs | system.query_log`.
- `Plan.table` is resolved through a `Catalog` trait, `execute<C: Catalog>`.
  The resolved snapshot must belong to the catalog's tenant, or the query
  fails.
- A bare `Snapshot` remains a catalog that holds only `logs`, so embedded
  callers are unchanged. The server's catalog adds the query log when it is
  enabled.
- Tenants read only their own entries. There is no cross-tenant admin view
  yet.

### Recording
- Every query is recorded once its request parses: success, failure, a
  concurrency rejection and a duplicate id.
- The entry goes into a bounded `sync_channel` (`query_log_queue`, default
  10,000).
- A full queue drops the entry and counts it in
  `strata_query_log_dropped_total`. Recording never blocks or fails a query.
- One writer thread (`sx-maint-02`) drains the queue in batches of at most
  `min(queue, 4096)`. Every `query_log_flush_ms` (default 7.5 s) it writes a
  batch with one `Sync` commit per tenant.
- A crash therefore loses at most one interval of log entries. No
  acknowledged user data is involved.
- Write errors are counted and logged. On shutdown the writer drains the
  queue before the engines close.

### Fingerprint
- **Normalization** works on lexer tokens:
  - literals become `?`, and `- 5` becomes one `?`;
  - comma-separated `?` runs collapse to one `?`, so `IN` lists of any length
    match;
  - subscript keys are kept, because `attributes['a']` and
    `attributes['b']` read different columns;
  - unquoted identifiers and keywords are lowercased, and a trailing `;` is
    dropped.
- **Hash:** FNV-1a 64 of the normalized text, pinned by tests. Changing
  normalization changes grouping with stored entries, so it needs a changelog
  entry.
- **Text that does not lex** is whitespace-collapsed, lowercased and hashed,
  so failed queries still group.
- **Storage:** the normalized text, truncated to 4 KiB, is stored instead of
  the SQL, so literal values never enter the log.

### CPU time
- `QueryStats.cpu` is the executing thread's CPU from parse to result
  (`CLOCK_THREAD_CPUTIME_ID`).
- Each scan-pool helper adds its own CPU for the segments it scanned. It
  records this inside the partial result before filling its slot, so there is
  no race with the merge and nothing is counted twice.

### Headers
- On every query response, errors included: `x-stratadb-query-id`,
  `x-stratadb-fingerprint`, `x-stratadb-queue-us`.
- On success, additionally `-parse-us`, `-plan-us`, `-exec-us` and
  `-cpu-us`.

### Threads
- The thread plan's fixed count rises from 4 to 6: the query log engine's
  `sx-maint-01` and the writer. `EngineOptions::maint_thread_index` names a
  second engine's background thread.

## Consequences
- **Evidence:**
  - **Top-N by CPU** per fingerprint is a plain query (`GROUP BY
    attributes['fingerprint']`), tested end to end over HTTP together with
    tenant isolation, failure entries, headers, the absence of literals, and
    entries that survive a restart (`tests/query_log.rs`).
  - **CPU accounting:** a unit test fails when helper CPU is not merged. A
    mutation check showed it reporting 3.2 ms instead of 12.7 ms.
- **Measured cost per query** (release build, Apple M4): about 2.6 µs.
  - fingerprint: 1.75 µs, mostly re-tokenizing;
  - entry plus queue send: 0.85 µs;
  - four thread-CPU clock reads: 88 ns each;
  - reproduce with `cargo test --release -p stratadb-server --lib
    query_log::cost -- --ignored --nocapture`.
  - That is about 0.2% of the fastest non-trivial queries in the standard
    scenario (1.4 ms or more).
- **Not yet:**
  - a cross-tenant (cluster admin) view;
  - per-operator estimated vs actual rows (STRATA-SYS-12-004);
  - spill counters (STRATA-SYS-05-021);
  - reusing the parser's tokens for the fingerprint;
  - live reload of `obs.*` settings: a restart is required, like every
    setting.
  - Backups do not include the query log, which is diagnostic data.

## Alternatives rejected
- **Logging the raw SQL:** it can carry tenant values (identifiers, secrets in
  literals).
- **Blocking when the queue is full:** it would make observability slow down
  the workload.
- **Writing every entry synchronously:** that is one fsync per query.
