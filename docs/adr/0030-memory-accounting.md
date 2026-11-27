# ADR-0030: Memory accounting hierarchy

- **Status:** Accepted
- **Spec references:** VIII.5.2 (STRATA-SYS-05-005/006/007/009),
  STRATA-SYS-02-029/035, STRATA-ARCH-01-006; `memory.*` settings

## Context
Before this change, memory limits were scattered:
- **Queries:** a per-query estimate of result rows and groups, checked
  against `max_memory_bytes`. Column batches, often the largest allocation,
  were not counted.
- **Ingest:** bounded only by the memtable size and the frozen-memtable
  count.
- **Process:** nothing tied these to the process's memory, so the OOM killer
  was the only global limit.

The spec requires a hierarchy of trackers under a process budget, with
separate regions. A reservation must be checked from leaf to root. Updates
must be batched. A failed reservation must wait or fail with a diagnostic
error, and resident memory must be compared with tracked memory.

## Decision
### Trackers (`stratadb_common::memory`, L1)
- **`MemoryTracker`:** a node with a name, an optional limit, `used`,
  `peak` and weak child links.
- **Reservation:** `try_reserve` adds to every node from the leaf to the
  root. If any node would exceed its limit, the nodes already added are
  undone and the call fails. So a successful reservation never puts a node
  over its limit, even under concurrency (tested with 8 threads).
- **Failure:** the error is `resource_exhausted` with `MEMORY_LIMIT_EXCEEDED`,
  naming the refusing node's path, its limit and usage, the request and the
  requester, and the top 5 consumers under the refusing node.
- **Tenant privacy:** tenant nodes are private. Outside their own path they
  are named `<other>`, so a tenant never learns another tenant's identity.
- **`Reservation`:** an RAII guard. `Reservation::forced` tracks memory
  already held without a limit check.
- **Waiting:** `reserve_wait` sleeps on a tree-wide condition variable,
  woken by releases, up to a timeout.
- **`Accountant`** (per worker, SYS-05-006): the owner declares its usage,
  and the accountant reserves ahead in 1 MiB batches.
  - When a whole batch does not fit, it reserves exactly what is needed, so
    limits apply to usage rather than to batch rounding.
  - It keeps one batch of slack when shrinking.
  - `absorb` hands a worker's reservation to the merging thread without
    touching shared counters.
  - It lives in the worker's context rather than a thread-local, which gives
    the same error bound (1 MiB × workers) without a per-thread registry.
- **Host facts:**
  - `detect_memory_limit` reads the cgroup v2 `memory.max` or the v1
    `memory.limit_in_bytes`, falling back to physical memory;
  - `resident_bytes` reads `/proc/self/statm`, or `ps` on development
    hosts.

### Regions (server)
- **Budget:** `max_server_memory_bytes`, or `max_server_memory_ratio`
  (default 0.80) of the detected limit.
- **Shares:**
  - control: a fixed 2%;
  - ingest: `ingest_memory_ratio`, default 0.10;
  - cache: `cache_memory_ratio`, default 0.30;
  - background: `merge_memory_ratio`, default 0.10;
  - query pool: the remainder, which must be at least 10%.
- **Query pool:** one private node per tenant, limited by its
  `query_memory_bytes` quota. Under it, one node per query, then the
  executor's `exec` node limited by `max_query_memory_bytes` (default
  min(10 GiB, the pool)).

### Who reserves what
- **Queries:**
  - each worker's `Accountant` covers its operator state (result rows,
    groups) plus the column batch it is about to read;
  - the batch is reserved before the read, from
    `ScanSource::estimated_read_bytes`: footer lengths for segments,
    requested-column payloads for memtable chunks;
  - failure: wait up to `memory_wait_timeout_ms` (default 0), then
    `MEMORY_LIMIT_EXCEEDED`.
- **Ingest:**
  - `write_batch` reserves the batch's memtable bytes before the WAL
    append. That is the same `row_bytes` the memtable charges, so release
    is exact;
  - when the region is full: request a flush, wait up to
    `write_stall_timeout`, then fail with a retryable `resource_exhausted`.
    The batch is never committed;
  - flushing a frozen memtable releases its bytes;
  - rows replayed from the WAL at startup are force-tracked;
  - the server caps `memtable_max_bytes` at a quarter of the ingest region,
    so flushes start well before writers would stall.
- **Cache:** decoded Bloom and trigram filters are force-tracked when first
  used and released with their segment. They cannot be evicted yet, so the
  cache share is a reported target, not a limit.
- **Background:** compaction, retention and upgrade rewrites reserve their
  input rows before reading them. Compaction is postponed when the region is
  full (`CompactionReport::deferred_for_memory`).
- **Query log engine:** accounted as `query_log` children of the same
  regions.

### Visibility
- `GET /api/v1/admin/memory` (cluster.read): the plan and the live tree.
- `strata_memory_{tracked,limit,peak}_bytes{region}`,
  `strata_memory_budget_bytes`, `strata_memory_resident_bytes` and
  `strata_memory_untracked_bytes`.
- A 10 s RSS sampler warns, at most every 5 minutes, when untracked memory
  exceeds `untracked_warn_ratio` (default 0.10) of RSS, or 4 GiB.
  - A 64 MiB floor keeps a small process's fixed overhead (binary, stacks,
    allocator) from warning forever.
- The query log records `memory_reserved_peak`.

## Consequences
- **Measured cost:**
  - **Hierarchy:** 3 interleaved foreground pairs of the standard scenario
    with the server-like tree on and off gave a geometric mean CPU ratio of
    1.001 against a 1.8% A/A spread. Plans were identical and dataset-load
    throughput unchanged (report `2026-11-26-memory-accounting`).
  - **Primitives:** a 4-level reservation plus release takes 23 ns, and an
    in-batch update 1 ns.
- **Behaviour changes:**
  - A query's memory limit now includes the column batch being scanned, not
    only results and groups. A very small `max_memory_bytes` now refuses
    wide reads.
  - The thread budget is unchanged, since the sampler is a tokio task.
- **Not yet:**
  - spilling (step 1 of SYS-05-007);
  - an evictable cache that shrinks first under pressure (SYS-05-008);
  - accounting for the control region;
  - allocator-exact accounting: sizes are estimates, and the untracked gap is
    measured to keep them honest;
  - reserving segment-encoding buffers during flush;
  - NUMA (SYS-05-010..014).

## Alternatives rejected
- **A counting global allocator:** exact, but it needs a custom
  `GlobalAlloc`, which is `unsafe` and forbidden here (ADR-0026). It also
  cannot attribute memory to a query or tenant.
- **Thread-local batching:** it needs a registry of per-thread counters per
  tracker. Per-worker accountants give the same bound with ownership.
- **Failing ingest immediately when the region is full:** a flush frees the
  memory within milliseconds, so stalling first (as for the frozen-memtable
  backlog) avoids needless client retries.
