# Known limitations (current state, 0.0.1)

## Not implemented
- Encryption at rest (ADR-0024) uses a key *file* provider only (no KMS/HSM
  integration yet); rotation is offline (CLI, server stopped); the token store
  and audit log are not encrypted (no event data); one extra WAL segment per
  process start when encrypted; encryption cannot be disabled once enabled.
- HTTP server (`strata-server`) has API-token auth, TLS (rustls), and an audit
  log. No mTLS, no certificate hot-reload, no audit-log rotation, no gRPC/OTLP,
  SDKs are synchronous/blocking (Go uses contexts). Docker/compose/systemd files exist but the
  image build is unverified.
- OTLP: HTTP with protobuf or JSON (no gRPC transport); interop-tested with the official
  OpenTelemetry Go exporter, not with a Collector binary.
- The auth-failure rate limit is a single global window (not per client/IP).
  Per-tenant rate buckets are in memory (they reset on restart).
- Logs are JSON on stderr; no OpenTelemetry trace export yet.
- Compaction is single-threaded with a simple size-tiered policy.
- Retention is applied periodically (default 60 s) at segment granularity; expired
  rows stay queryable until the next run. No per-row delete API.
- Backups are full (no incremental), encrypted only when the database is
  (ADR-0024), and must be copied to another
  failure domain by the operator. Restore is offline only.
- Indexes: footer statistics + per-segment Bloom filters for id fields and
  selected string attributes; trigram filters prune `LIKE`/`ILIKE` segments
  (literal runs ≥ 3 characters, top-level conjuncts only). No exact-match
  postings (deferred: no row groups) and no token/phrase/prefix search
  operators. Changing Bloom or ngram settings affects only new segments.
- Idempotency is bounded (24 h / 100 000 keys by default); retries outside
  that bound, or retries without a key, are at-least-once.
- Disk reserve (512 MiB / 2 %) pauses writes before the volume fills; if
  the disk fills anyway (e.g. another process), a WAL write error still puts
  the engine into the failed state (restart required).
- Format upgrades (`strata upgrade`) cannot be rolled back except by restoring a backup.

- State machines (ADR-0035): the `connection` machine is exported but not
  driven, since HTTP authenticates per request. Insert `FLUSHED`, merge
  `PAUSED`/`CANCELLED`, WAL `RECYCLED` and table DDL states are never
  entered. The spec tables have no `DEGRADED → DRAINING`, `UNKNOWN →
  FAILED` (device) or `RECEIVED → REJECTED` (insert) edges. A degraded server
  therefore stops from `DEGRADED`, and undecodable or quota-rejected inserts
  are not tracked. Device health is for one device (the data directory),
  judged from WAL fsync latency only.

- Workload classes (ADR-0036):
  - **What is scheduled:** fair scheduling covers scan-pool morsels, and
    query threads that lend to or yield to higher classes. Sequential scans
    and work outside the pool (merge, ingest) are not scheduled. There is no
    background minimum share (SYS-11-002).
  - **Admission inputs:** PSI-based overload refusal works only on Linux,
    and admission does not budget estimated CPU.
  - **Workload stats:** they are an HTTP endpoint and metrics, not a
    `system.workload_stats` table.
  - **Measured effect:** in the embedded mixed benchmark on a 10-core
    laptop, classes give no measurable interactive-latency gain.

- Aggregation (ADR-0037):
  - **Metadata answers:** only global `count`/`min`/`max(event_time)` with
    time or canonical-severity filters are answered from metadata.
    `GROUP BY severity` is not, because of the first-seen output order.
    Memtable windows have no severity counts.
  - **Dense aggregation:** it needs dictionary or byte keys, so it covers
    segments, not the memtable's plain strings.
  - **Hash tables:** there are no two-level tables, no streaming
    aggregation, and no probe-length statistics.

- String kernels (ADR-0038):
  - **Fast paths:** only literal patterns (`x`, `x%`, `%x`, `%x%`) have
    them; patterns with `_` or inner `%` use the general matcher. ILIKE is
    fast only for ASCII literals on ASCII text.
  - **Filters:** they compile to a node tree, not bytecode.
  - **Unverified:** the query-level gains, pending an A/B on AC power.

- Snapshots (ADR-0039):
  - **Deletion delay:** replaced segment files stay on disk for
    `old_segments_lifetime` (8 min by default) after their last reader, so
    disk is freed later than before.
  - **Large tenants:** segment lists are per-tenant vectors, so very large
    single tenants copy their list on publish while a snapshot holds it.
  - **`system.snapshots`:** it is an HTTP endpoint and metrics, not a SQL
    table.

- P3 (ADR-0040):
  - **Ordered scans:** they need `event_time` as the first ORDER BY key and
    a LIMIT without pagination.
  - **Plan cache:** it keys on exact text, so queries differing only in a
    literal plan separately.
  - **Q-error:** it is per query, not per operator.
  - **Wire protocol:** there is no native or columnar protocol.

- Query responses (ADR-0044):
  - **Not streamed:** they are built whole. Peak memory holds the result
    rows and their JSON (06-002 is open), bounded by `max_result_rows`.
- Sorting (ADR-0043):
  - **No external sort:** results are bounded by `max_result_rows`.
  - **Granule skipping:** the top-k bound skips rows, not granules, for
    keys other than `event_time`.
  - **Group top-k:** it still uses comparison selection.
  - **Small sorts:** normalized sorts of fewer than 4,096 rows that keep
    half of them measured 0.87× on integer keys.
- Column cache and StrataStudio (ADR-0041, ADR-0042):
  - **Column cache:** it is off by default and needs `[storage]
    column_cache_bytes`. Shredded keys and projected map reads are not
    cached.
  - **StrataStudio limitations:** see `docs/studio/STATUS.md`. Each
    connection uses one token (one tenant), and sessions are held in
    memory.
  - **`/api/v1/schema`:** it lists keys from flushed segments only.

## Durability / testing gaps
- Power loss is tested by **simulation** (simfs recorder: lost/torn/reordered
  un-synced sectors, lost un-synced directory operations), sampling 2–6 states
  per crash point rather than enumerating all of them. Devices that ignore
  flush requests ("lying hardware") are out of scope. Damage to the last
  acknowledged WAL record with no later frame is indistinguishable from a torn
  tail (truncated).
- Fsync failures poison the WAL by design; recovery requires restart.

## Performance
- **Typed columns (ADR-0034):**
  - Only string columns are typed (plain or dictionary). Numeric and value
    columns are still vectors, with no null bitmaps for values.
  - Memtable strings are plain, so the dictionary kernels apply to segments
    only.
  - Selections live within the single scan operator; there is no
    multi-operator pipeline.
- **Commit wake-ups and in-flight I/O (ADR-0032):**
  - Group-commit wake-ups are handed off writer to writer (about 0.3 ms
    mean at 128 writers on HW-REF-7); a single futex word is not
    implemented.
  - The per-device in-flight default (128) is not derived from a measured
    queue depth.
- **WAL v3 (ADR-0031):**
  - A directory with a v3 WAL segment cannot be opened by releases older
    than v3; use `wal_format_version = 2` for a rollback window.
  - Columnar encoding and LZ4 add about 100 ns of ingest CPU per event.
  - Replay still decodes into row events, because the memtable is row-based.
- **Memory accounting (ADR-0030):**
  - Sizes are estimates from metadata and approximate sizes, not exact
    allocator figures; the RSS gap metric measures the difference.
  - Queries cannot spill, so a query that needs more memory than its
    limits fails.
  - Decoded filters are tracked but cannot be evicted, so the cache share
    is not enforced.
  - Nothing is accounted to the control region.
  - Segment-encoding buffers during flush are not reserved.
  - Outside Linux, RSS is read with `ps`.
- **Query log (ADR-0029):**
  - Each tenant sees only its own entries; there is no cluster-admin view.
  - Entries buffered within `query_log_flush_ms` are lost in a crash, and
    entries are dropped (and counted) when the queue is full.
  - The query log is not included in backups.
  - The fingerprint re-tokenizes the query, about 1.75 µs.
  - There are no per-operator row estimates (SYS-12-004).
- **Architecture (ADR-0028):**
  - `stratadb-query` still holds both the executor (L3) and the
    parser/planner (L4).
  - Column batches are row-major `Value` vectors: no physical-typed
    vectors, null bitmaps or selection vectors.
  - Memtable chunks are never pruned.
  - There are no standalone L1 microbenchmarks.
- Only developer-laptop (HW-REF-7) measurements exist (`benchmarks/reports/`). **No performance claims are made**, and there are no competitor comparisons. No spec Part VIII level is achieved (RELEASE_DECLARATION.md).
- Of the 416 spec Part VIII–IX requirements, most are MISSING
  (PERF_INVENTORY.md; phase plan in ADR-0026).
- The regression gate and the cost ledger work, but on a shared machine
  they give false verdicts: the A/A test found 9 of 27. CI runs only the
  smoke scenario until a dedicated runner exists. The ledger records CPU ns,
  not cycles.
- `strata doctor host` reports but never enforces. The server still starts
  on NFS/overlay data directories (no `allow_unsafe_fs` production mode).
  NIC/IRQ placement, NVMe power-loss protection and C-states are not
  checked, and the checks apply to Linux only.
- The thread inventory is an HTTP endpoint, not a SQL `system.threads`
  table. CPU time is recorded by each thread itself (after tasks and when
  parking), threads are not pinned to cores, and the OS thread
  cross-check is Linux-only.
- Scans run on a shared, fixed pool with cost-based parallelism.
  - The unit of parallel work is a whole segment, not a granule morsel
    (STRATA-SYS-11-007).
  - There are no interactive/batch scheduling classes.
  - The per-row cost estimate is a fixed calibration from HW-REF-7, not the
    VIII.12 cost model.
  - Idle workers are used beyond the STRATA-SYS-11-008 base formula
    (ADR-0006).
  - Very short queries (under about 1 ms) pay up to about 45% for the
    handoff to pool threads (report 2026-11-26-scan-pool).
- Segments use LZ4 chunk compression (v2); no zstd cold tier yet. Hot attribute keys (≥ 10 % of
  rows) are shredded into columns (v3); rarer keys stay in per-row maps.
- `WHERE` filters are vectorized and group keys are encoded without copies
  (ADR-0006 updates); shredded keys used only as `GROUP BY` keys are not
  decoded at all, but other group keys from per-row maps still are.
  Projection and sort keys are still materialized per matched row (top-k
  `ORDER BY … LIMIT` bounds how many are kept). Queries naming specific attribute keys decode only those keys;
  queries using whole maps (`attributes`, `SELECT *`) decode every row's map. Segment scans of sorted and
  aggregate queries are parallel (ADR-0006); unsorted `LIMIT` scans, the
  memtable and the final merge are sequential; float `sum`/`avg` may differ in
  the last bits from a sequential scan. Whole-map predicates (`attributes = …`) use the scalar path. Memtable
  snapshots copy chunk pointers (O(chunks), ADR-0033). Memtable chunks prune by time and
  severity only (no Bloom or key filters), and flush materializes each tenant's rows for the
  segment writer.
- Group commit is adaptive (one fsync in flight per WAL). Waking the
  writers of a group uses a condition variable's `notify_all`, not O(1)
  syscalls (STRATA-SYS-07-006). The fsync latency and arrival-rate
  estimates are per process and start cold after a restart.
- Storage file data (segments, WAL, manifest, backups) is read and written
  through the I/O engine (ADR-0027). Only the portable `pread`/`pwrite`
  backend exists: no io_uring and no in-flight I/O bound
  (STRATA-SYS-04-008). Namespace operations (rename, directory fsync,
  unlink) go through `fsutil`. Every open WAL file keeps a descriptor.
- `strata doctor` has no cluster connectivity or peer clock-skew checks
  (single node); its memory check is an upper bound on engine budgets, not a
  measurement; `--deep` costs a full scan.
- Manifest is rewritten in full on every flush.

## Semantics
- Without `ORDER BY`, unpaginated result order is unspecified (paginated
  queries use commit order).
- Pagination (ADR-0023) is keyset-based: each page rescans candidate segments
  (pruned by commit sequence, and by time when ordered by `event_time`), so deep
  pages of other orders cost about as much as the first. Aggregates and EXPLAIN
  cannot be paginated. Rows removed by retention between pages are skipped.
- Query memory budget is an estimate, not exact accounting.
- `strata` CLI requires exclusive access to the data directory (no server
  must be running) and trusts the operator-supplied `--tenant`; this includes
  `export`, `ingest` and `shell`. There is no HTTP export endpoint yet.
- Export formats are NDJSON, CSV and Parquet (ADR-0025: CLI only, events only,
  uncompressed, no resume). Parquet import reads uncompressed/snappy/gzip/lz4,
  not zstd or brotli. CSV cannot distinguish
  missing from null and is not formula-escaped.
- Resumable imports rely on the idempotency window (24 h / 100 000 keys).
