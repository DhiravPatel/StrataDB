# Changelog

## Unreleased (0.0.1)
### Added
- **StrataStudio (ADR-0041).** A pgAdmin-style web UI, `strata-studio
  serve`. It provides:
  - a connection registry with masked tokens;
  - a query tool with paging, cancel, EXPLAIN and export;
  - a live dashboard;
  - a query log browser;
  - admin actions with confirmations.

  It proxies to StrataDB servers, so tokens never reach the browser. See
  `docs/studio/README.md`.
- **New server endpoints for clients and StrataStudio (ADR-0042):**
  `GET /api/v1/whoami`, `GET /api/v1/schema` (attribute keys from
  metadata), `GET /api/v1/queries` (the caller's queued and running
  queries) and `GET /api/v1/admin/backup` (backup list). A `page_size` on
  aggregate or EXPLAIN queries is now ignored (`"paginated": false`)
  instead of rejected.
- **Faster query responses (ADR-0044).**
  - **Encoding:** result rows are encoded to JSON column by column (587
    MB/s per core, 3.2× the previous path) with byte-identical output.
  - **Off the async threads:** encoding now happens on the blocking pool,
    not on the async threads.
  - **No extra copy:** the encoded rows are not copied again into the
    response body.
- **Faster ORDER BY (ADR-0043).**
  - **`LIMIT` queries:** `ORDER BY ... LIMIT k` skips rows that cannot
    reach the top k before building them, with one bound shared by all
    scan workers. Measured 2.2–2.6× faster.
  - **Full sorts:** they use normalized keys with radix sort (kernel
    1.8–9×), each segment's rows are sorted in parallel, and the runs are
    merged by a loser tree.
  - **New stats:** `sort_runs_merged` and `topk_rows_skipped`.
- **Changed: exact numeric ordering.** ORDER BY now compares integers and
  floats exactly, so the order is total even beyond 2^53, where it was
  ill-defined before.
- **Decoded column cache (ADR-0042).** An opt-in
  `[storage] column_cache_bytes` caches decoded segment columns,
  scan-resistant (SLRU) and charged to the cache memory region. Metrics:
  `strata_column_cache_*`, and `column_cache_hits` in query stats.
- **Faster latest-rows queries, plan cache, connection options (ADR-0040).**
  - **Latest rows:** `ORDER BY event_time [DESC] LIMIT k` reads the newest
    (or oldest) segments first and stops as soon as the k-th row beats
    every remaining segment, with the same results.
  - **Plan cache:** repeated queries skip parsing and planning. It is
    bounded and sharded, with stats at `/api/v1/admin/caches` and
    `strata_plan_cache_*`.
  - **Connections:** client connections use `TCP_NODELAY` and TCP
    keepalive.
  - **Estimates:** query stats and `system.query_log` record estimated vs
    actual rows (`est_rows`, `q_error`), with the histogram
    `strata_query_estimate_q_error`.
- **Snapshot catalog (ADR-0039).**
  - **Cost:** queries take their snapshot from a lock-free published view,
    in constant time whatever the number of segments (about 0.25 µs,
    against 5.8 µs at 1,500 segments).
  - **Visibility:** `GET /api/v1/admin/snapshots` lists open snapshots
    with the query holding each and the replaced-segment bytes it keeps on
    disk. New metrics: `strata_snapshots_open`,
    `strata_snapshot_oldest_age_seconds` and `strata_obsolete_retained_*`.
    A warning is logged above 10% of the disk.
  - **Deletion delay:** replaced segments are deleted only after
    `[storage] old_segments_lifetime_s` (default 480 s) without readers.
- **Faster string filters (ADR-0038).**
  - **ILIKE:** literal patterns on ASCII text use ASCII case folding,
    about 20× faster per row, with Unicode folding unchanged for other
    text.
  - **LIKE:** `'%x%'` uses a prebuilt SIMD finder.
  - **AND:** conditions are evaluated in an adaptive order, cheapest and
    most selective first.
  - **Planner:** constant comparisons and duplicate conditions in `WHERE`
    are folded at plan time.
  - **Columns:** string columns without missing values hold no validity
    bitmap.
  - **EXPLAIN ANALYZE:** it reports conjunction reorders.
- **Aggregates from metadata and on dictionary codes (ADR-0037).**
  - **Metadata answers:** `SELECT count(*), min(event_time),
    max(event_time)` skips reading any segment or memtable window that the
    filter fully covers. That includes unfiltered queries, time ranges
    containing the whole segment, and severity filters, using each
    segment's exact severity counts. A total count fell from 3.4 ms of CPU
    to 0.003 ms.
  - **Code aggregation:** `GROUP BY` on dictionary or severity columns
    aggregates on codes (3.4–3.8× faster for `GROUP BY severity`).
  - **Memory:** aggregation reserves hash-table growth against the query
    memory limit before allocating.
  - **EXPLAIN ANALYZE:** it shows the hash table (variant, groups,
    capacity, resizes, bytes).
  - **API:** responses report `stats.sources_from_metadata`.
- **Workload classes and admission (ADR-0036, spec VIII.11).**
  - **Classes:** queries are `interactive`, `normal` or `batch`, chosen
    from a plan-time cost estimate. Operators can set a class per tenant,
    and a request's `workload_class` can only lower it.
  - **Queueing:** a query without a free slot waits in a bounded fair
    queue by class, then tenant, then arrival. Its position is visible
    through `GET /api/v1/query/{id}`, and it can be cancelled while it
    waits.
  - **Refusals:** a full queue or a timed-out wait gets 429
    `TOO_MANY_QUERIES`; memory pressure gets 503 `SERVER_OVERLOADED`. Both
    carry `retry_after_ms`.
  - **Long queries:** a query running past twice its class budget is
    demoted one class.
  - **Batch slots:** an AIMD controller shrinks them when interactive p95
    is over target under CPU saturation.
  - **Scan pool:** it schedules morsels by class weight (8:4:1) and tenant,
    and lower-class queries lend their thread to, or yield to,
    higher-class work.
  - **Observability:** new `[workload]` config section,
    `/api/v1/admin/workload`, `strata_workload_*` and
    `strata_scan_pool_class_cpu_seconds_total`.
- **System state machines (ADR-0035, spec IX.22).**
  - **Machines:** query, insert, WAL segment, merge task, table, server and
    device lifecycles are tracked against the spec's transition tables.
  - **Metrics:** `strata_state_seconds{machine,state}`,
    `strata_state_entities` and `strata_state_illegal_transitions_total`
    in `/metrics`.
  - **Readiness:** `/api/v1/ready` reports the server state and returns 503
    while starting or draining.
  - **Device health:** a device health monitor marks the server `DEGRADED`
    when WAL fsync p99 stays above 4× its baseline for 30 s. Disk below the
    reserve makes the table `READ_ONLY`.

### Changed
- `stats.elapsed_ms` of query responses is measured from the request's arrival, so `queue_us + parse_us + plan_us + exec_us <= elapsed_ms` holds again (parse, plan and admission run before the worker; the old start point excluded them).
- **Typed string columns and selection vectors (ADR-0034).**
  - **String columns:** they are now `StrColumn`, either plain (one text
    buffer) or dictionary-encoded as decoded from segments. There is no
    per-row allocation.
  - **Dictionary kernels:** filters on dictionary columns evaluate once per
    distinct value. `GROUP BY` on dictionary columns and on `severity` uses
    pre-encoded keys.
  - **Selections:** the executor walks per-chunk selections, and a global
    `count(*)` counts selection sizes.
  - **Effect:** 1.18× on the standard query suite and 2.9× on GROUP BY
    severity; no query is slower.
  - **API:** `ColumnData::Strs` now holds a `StrColumn`.
- **Columnar memtable (ADR-0033).**
  - **Storage:** unflushed rows are stored as per-tenant column chunks with
    a published row count, instead of a vector of `Arc<Event>`.
  - **Snapshots:** they copy chunk pointers (O(chunks)), where they used to
    clone every row.
  - **Pruning:** queries prune memtable chunks by their time and severity
    zone maps (`memory_chunks_pruned` in query stats and `EXPLAIN`).
  - **Freeze and replay:** freezing is O(1), and WAL replay decodes
    columnar records straight into columns.
  - **Effect:** 3.1–3.2× faster time- and severity-filtered queries over
    unflushed data, at about +4–9% ingest CPU.
  - **API:** `Snapshot::memory_rows()` is replaced by `memory_row_count()`
    and `memory_views()`.
### Fixed
- **Concurrent fsync during rotation.** A WAL segment rotation could fsync
  the old segment while a group-commit leader's fsync was still running.
  Rotation now waits, so one fsync is ever in flight per WAL (spec
  STRATA-SYS-07-005, ADR-0032).
### Added
- **Per-device in-flight I/O bound.** Set with `[storage]
  io_max_inflight_per_device` (default 128), with `strata_io_inflight*`
  metrics.
- **Commit wake-up histogram.** `strata_wal_commit_wake_seconds`.
- **Wake-up alternatives measured.** Broadcast and sharded designs were
  measured slower than the existing hand-off, which stays (ADR-0032).
- **WAL v3: columnar batch payloads (ADR-0031, spec STRATA-SYS-07-002).**
  - **Format:** WAL records now carry columnar batches: the tenant is stored
    once, timestamps as deltas, `service` and `severity_text` as
    dictionaries, and the body is LZ4-compressed from 16 KiB. This is new
    record types 3–4 in WAL format v3.
  - **Effect:** WAL bytes per event fall about 3.3× (232 → 70 B on the
    app-logs profile), at about 19% more ingest CPU.
  - **Compatibility:** v1–v2 WALs replay unchanged.
  - **Rollback:** `[storage] wal_format_version = 2` keeps writing the row
    format for a rollback window (see runbooks/upgrade.md).
  - **Metrics:** `strata_wal_bytes_total` and `strata_wal_records_total`.
  - **Upgrade note:** releases older than this cannot open a data directory
    once it holds a v3 WAL segment.
- **Memory accounting hierarchy (ADR-0030).**
  - **Budget and regions:** a process budget (0.80 of the cgroup or
    physical memory limit, `[memory]` settings) is split into control,
    ingest, cache, background and query regions, with a node for each tenant
    and each query.
  - **Queries:** they reserve operator state, and column batches before
    reading them. A refused query fails with `MEMORY_LIMIT_EXCEEDED`, naming
    the tracker path and the top 5 consumers, with other tenants shown as
    `<other>`. Queries can wait for memory (`memory_wait_timeout_ms`).
  - **Ingest:** writes reserve memtable memory before the WAL append, stall
    for a flush when the region is full, then fail retryably.
  - **Compaction:** it is postponed when background memory is full.
  - **Monitoring:** RSS is compared with tracked memory every 10 s
    (`strata_memory_untracked_bytes`). New: `GET /api/v1/admin/memory`, the
    `strata_memory_*` metrics, a `query_memory_bytes` tenant quota, and
    `memory_reserved_peak` in `system.query_log`.
  - **Behaviour change:** a query's memory limit now also covers the column
    batch being scanned.
- **`system.query_log` (ADR-0029).**
  - **Recording:** every query, failed and rejected ones included, is
    recorded under its tenant. Each entry holds a normalized 64-bit
    fingerprint, `queue_us`, `parse_us`, `plan_us`, `exec_us`, `cpu_us`
    (counting the scan-pool helpers), rows and bytes read, memory peak and
    status.
  - **Querying:** read it with `SELECT ... FROM system.query_log`. Each
    tenant sees only its own entries, and top-N by CPU per fingerprint is an
    ordinary `GROUP BY`.
  - **No impact on queries:** recording is bounded and never blocks or fails
    a query.
  - **Settings:** `[observability] query_log`, `query_log_flush_ms`,
    `query_log_retention_days` and `query_log_queue`.
  - **Headers and response body:** query responses carry
    `x-stratadb-query-id`, `-fingerprint`, `-queue-us`, `-parse-us`,
    `-plan-us`, `-exec-us` and `-cpu-us`. The body gains `fingerprint` and
    `stats.cpu_us`.
  - **Library API:** `stratadb_query::execute` now takes a `Catalog`, and a
    `Snapshot` still works.
  - **Thread budget:** the fixed thread count rises from 4 to 6.
- **Layer check and ScanSource seam (ADR-0028).**
  - **Layer check:** `scripts/check_layers.py` compares the crate graph with
    `docs/architecture/layers.toml`. It fails on upward, skip-level and
    forbidden dependencies, on unlisted crates and on stale rules, and runs
    in `scripts/check.sh` and CI (STRATA-ARCH-01-001/002).
  - **ScanSource seam:** the executor now reads segments and memtable
    chunks only through `stratadb_storage::ScanSource`. Storage does the
    pruning, including Bloom and trigram filters, and serves column batches.
    The query crate no longer depends on the segment crate
    (STRATA-ARCH-01-003).
  - **Column types:** they moved to `stratadb_schema::column`; the segment
    crate re-exports them.
  - **Unchanged:** query results, statistics and `EXPLAIN` output. Plans were
    identical and there was no measurable CPU cost (report
    2026-11-26-scan-source-seam).
- **Write path through the I/O engine (ADR-0027).** These now go through
  `stratadb_common::io`, so read and write faults can be injected under the
  real engine:
  - WAL appends, fsyncs, rotation, torn-tail truncation and replay;
  - segment writes;
  - manifest commits;
  - backup copies.

  New engine-level fault tests cover WAL fsync EIO (fail-stop, no
  acknowledged data lost), ENOSPC during flush (retried), manifest fsync
  EIO (fail-stop, recovered from the WAL) and short reads/writes. The
  crash matrix and power-loss simulation are unchanged (0 violations).
  Direct file reads and writes are now banned in the data-path crates.
- **Adaptive group commit** (spec STRATA-SYS-07-004).
  - The WAL sync leader no longer waits when no other writer is expected.
    Under load it waits at most `min(fsync latency, max delay)` and stops as
    soon as its group is complete.
  - On the development laptop, single-writer and 8-writer ingest throughput
    rose by about 50%, low-rate commit latency fell from about 6.1 ms to
    3.8 ms, and the bulk load went from 140k to 217k events/s
    (`benchmarks/reports/2026-11-26-adaptive-group-commit.md`).
  - New settings `[storage] group_commit_max_delay_ms` and
    `group_commit_adaptive`; new metric `strata_wal_group_delay_seconds`.
- `strata doctor storage --dir`: fdatasync p50/p99 of 4 KiB writes and the
  implied commit rate (STRATA-SYS-07-008).
### Fixed
- `strata-bench` closed-loop ingest held its batch-pool lock across
  `write_batch` once the pre-generated batches ran out. That serialised the
  writers and produced second-long false tail latencies. Open-loop ingest,
  used by earlier reports, was not affected.
- **`strata doctor host`** (spec VIII.2): host readiness report.
  - Checks: kernel, transparent huge pages, the 27-value sysctl baseline,
    CPU governor and clocksource, rlimits and cgroup limits, and, with
    `--data-dir`, the data filesystem, mount options and block device.
  - `--json` gives stable keys; the exit code is non-zero on FAIL.
  - Ships `packaging/sysctl.d/90-stratadb.conf` and a `tuned` profile.
  - The server logs host facts at startup.
- **Storage I/O engine (ADR-0027).** Segment, manifest and backup reads go
  through `stratadb_common::io`:
  - Open segment files are cached in an LRU under a descriptor budget
    (`[storage] max_open_files`, default 50% of `RLIMIT_NOFILE`), so the
    segment count is no longer limited by open files.
  - `EMFILE` is recovered by closing cold files, and reopens are
    identity-checked.
  - Short reads are resubmitted. A failed read is retried once and then
    fails the query naming the segment file.
  - The server and CLI raise the soft `RLIMIT_NOFILE` to the hard limit.
  - New `strata_io_*` metrics.
  - A deterministic `SimIo` backend injects short reads/writes, EIO, ENOSPC,
    latency and crash reordering/tearing for tests.
  - Direct file reads are banned by a lint in the data-path crates.
- `strata doctor`'s descriptor check accounts for the segment file cache:
  a low limit now warns about reopen churn instead of failing.
- **Thread inventory and budget (spec STRATA-SYS-03-014/015).**
  - Every engine thread is named `sx-net-NN`, `sx-work-NN`, `sx-blk-NN` or
    `sx-maint` and registered with its role and CPU time.
  - `GET /api/v1/admin/threads` lists them, along with the budget and pool
    state.
  - New metrics: `strata_threads{role}`, `strata_thread_budget`,
    `strata_thread_cpu_seconds{role}` and `strata_blocking_pool_*`.
  - New settings `[limits] net_threads` and `blocking_threads`. The server
    refuses thread plans above `2 × logical_cpus + 32`.
### Changed
- Blocking request work (commits, queries, admin operations) runs on a
  bounded `sx-blk` pool that starts threads on demand up to its maximum,
  instead of tokio's `spawn_blocking` (formerly up to 64 extra threads). A
  full queue fails fast with `resource_exhausted`.
- **Shared scan worker pool.** Queries no longer create scan threads.
  - Pool: one fixed pool of `sx-work-NN` threads (`[limits] scan_threads`)
    with a bounded queue (≤ 2 × workers active). When the pool is busy, the
    query runs the work on its own thread.
  - Contention-aware degree of parallelism: `ceil(est_cpu_ms / 5 ms)` when
    the pool is busy; idle workers are used otherwise, with at most 4 for
    queries under 2 ms (spec STRATA-SYS-01-006/010, 11-008).
  - Under 8 concurrent clients: lower tail latency and up to 1.6× GROUP BY
    throughput (`benchmarks/reports/2026-11-26-scan-pool.md`).
  - Visibility: `stats.scan_workers`, `stats.est_cpu_ms`, EXPLAIN ANALYZE
    "Parallelism", and `strata_scan_pool_*` metrics.
  - The background thread is now named `sx-maint`.
- **Performance measurement system (spec P0, ADR-0026).** These subcommands
  are new; `strata-bench [flags]` (the fixed suite) is unchanged:
  - `strata-bench scenario` runs TOML scenarios
    (`benchmarks/scenarios/`) with a run manifest, raw samples, bootstrap
    CIs, plan assertions and open-loop load (latency from the intended send
    time; `CLIENT_BOUND` detection).
  - `strata-bench compare` detects regressions (Mann-Whitney U, p < 0.01,
    at least a 3% median change, rolling window of 10 runs).
  - `strata-bench ledger` maintains the operator cost ledger
    `perf/cpr_ledger.toml` (CPU ns per unit, 5% + noise rule).
  - New metric families `strata_insert_stage_seconds{stage}` (9 stages) and
    `strata_query_stage_seconds{stage}`.
  - Query responses report `queue_us`, `parse_us`, `plan_us` and `exec_us`,
    and the server logs a structured `query` line.
  - Requirement inventory `docs/project/PERF_INVENTORY.md` for all 416 Part
    VIII–IX requirements, checked by `scripts/check.sh`.
  - `docs/project/RELEASE_DECLARATION.md`.
### Changed
- Release profile: fat LTO, `opt-level = 3`, `panic = "abort"` (spec
  STRATA-SYS-18-001). A panic now exits the process; the service manager
  restarts it and recovery replays the WAL. New `release-checked` profile and
  a nightly job running the tests with it.
- High-cardinality `GROUP BY` of shredded attributes uses 2.95× less CPU
  (query 08, `benchmarks/reports/2026-11-25-raw-group-keys.md`): group-only
  keys are read as validated raw bytes, without per-row allocation; group
  tables keep key bytes only, and keys are decoded once per output group;
  `ORDER BY … LIMIT k` selects the top k instead of sorting every row (and
  ranks aggregate groups before building rows).
- `strata doctor` completes spec §22.3 for a single node. New checks:
  open-file limit vs segment and WAL count, worst-case memory budget vs RAM,
  `--config` validation, TLS certificate expiry (fail when expired, warn
  under 30 days) and key mode, backup age (`--max-backup-age-hours`), and
  `--deep` Bloom/n-gram false-negative verification
  (docs/operations/doctor.md).
- Index lifecycle: `Engine::indexes`/`rebuild_indexes` and `strata indexes
  list|verify|rebuild` — verification checks filters against the rows (no
  false negatives; current vs stale settings), rebuild rewrites stale or
  unsound segments.
- Metrics: latency histograms for ingest acknowledgement (per endpoint),
  queries, WAL fsync and flushes; WAL group-commit size; `queries_total` by
  outcome; query rows/bytes/segments counters (docs/operations/metrics.md).
  No tenant labels (bounded cardinality, no tenant-name disclosure).
- Flat aggregation groups (`hashbrown::HashTable` + key arena, aHash):
  high-cardinality `GROUP BY` uses 2.2× less CPU
  (`benchmarks/reports/2026-11-25-grouping-flat.md`).
- Fuzz targets `cursor`, `sealed` (encryption formats, encrypted WAL recovery)
  and `map_projection`; 10-minute libFuzzer campaigns found no crashes
  (≈ 8.2 M runs); CI fuzz smoke covers all 14 targets.
- Attribute key projection: queries naming specific attribute keys no longer
  decode whole maps (`QueryOptions::enable_map_projection`,
  `strata-bench --no-map-projection`); query 10 of the suite 22× faster.
### Fixed
- Parquet import of a file using a codec this build lacks looped forever
  (every row returned the same read error); read errors now abort the import
  with a clear `unsupported` error. Nested Parquet columns are imported as
  JSON structure instead of debug strings.
### Added
- Parquet import of snappy/gzip/lz4 files (pure-Rust codecs) and independent
  interop verification against pyarrow and DuckDB (`scripts/parquet_interop.sh`,
  CI job `parquet-interop`, fixtures `tests/fixtures/parquet-interop`).
- Parquet export/import in `strata` (ADR-0025): `export --format parquet`
  (events, row group per page, atomic rename), `ingest` of `*.parquet` (exact
  timestamp conversion, aliases, extra columns → attributes); optional
  `parquet` cargo feature (default on, CLI only).
- Parallel segment scans for sorted and aggregate queries with partial
  aggregation, ordered merge and query-wide budgets
  (`QueryOptions::parallelism`, `strata-bench --no-parallel`); 2.2–2.6× on
  multi-segment queries (`benchmarks/reports/2026-11-25-parallel-ab.md`).
- Encryption at rest (ADR-0024): `stratadb-crypto` (ring AES-256-GCM +
  HKDF), KEK file provider, KEYRING of wrapped data keys, sealed segment and
  manifest containers, encrypted WAL segments, KEK/DEK rotation
  (`strata keys generate|status|rotate-kek|rotate-dek`), encrypted backups,
  `--key-file` / `STRATADB_KEY_FILE` / `[encryption] key_file`, `strata
  upgrade` encrypts existing plaintext segments. Formats:
  docs/storage-format/encryption-v1.md; runbook docs/runbooks/encryption.md.
- Allocation-free grouping: group keys encoded from column data by reference
  into a reused buffer (byte-identical keys, property-tested). Group-by queries
  1.3–3.0× faster (`benchmarks/reports/2026-11-25-grouping.md`).
- `strata export` (NDJSON/CSV, events or row query, paginated snapshot,
  fsynced checkpoints and `--resume`, never overwrites), idempotent resumable
  `strata ingest --import-id` with `--report`, and `strata shell`
  (docs/operations/export-import.md). `stratadb_schema::json::event_to_json`
  is the property-tested inverse of the ingest mapping.
- Trigram (ngram) filters for substring search (F-005, ADR-0005): segment
  footer extension tag 3 over `body` (and `storage.ngram_attribute_keys`)
  prunes segments for `LIKE`/`ILIKE`; `ngram(body)` in `indexes_used`;
  `strata-bench --no-ngram`. A/B: absent substring 3.0 ms → 3 µs, +0.17 %
  storage, +11 % flush (`benchmarks/reports/2026-11-25-ngram-ab.md`).
- Power-loss simulation (`stratadb_common::simfs`, feature `simfs`): crash
  images around every fsync, materialized as lost/torn/reordered sector and
  lost-rename states; `stratadb-storage/tests/power_loss_sim.rs` recovers
  2 428 states across `sync`/`group_commit`/`async` with zero violations.
### Changed
- **WAL format v2** (reads v1): frames carry `synced_through`, letting recovery
  truncate an un-synced tail that power loss persisted out of order instead of
  refusing to open (found by the simulation). v1 segments are replayed and never
  appended to. All data-file fsyncs go through `fsutil::sync_file`.
- Vectorized `WHERE` evaluation: typed column kernels over selection vectors,
  `LIKE` compiled once, severity predicates via lookup table; scalar fallback
  for other shapes; `QueryOptions::enable_vectorized` and `strata-bench
  --no-vectorize`. A/B: 1.2–3.5× on filtered suite queries
  (`benchmarks/reports/2026-11-25-vectorized-ab.md`).
- Stable keyset pagination for row queries (ADR-0023): `execute_page`, HTTP
  `page_size` / `cursor` request fields and `next_cursor` in responses, SDK
  helpers (`query_page`/`query_pages`, Python `query_pages`, Go `QueryPage` /
  `QueryAllPages`). Cursor-based segment pruning (`segments_pruned_cursor`).
- Workspace, toolchain pin, lint policy (`unsafe` forbidden).
- `stratadb-common`: error model with kinds/retryability, CRC-32C, bounded
  binary codec, durable filesystem helpers, tenant IDs, RFC 3339 time, failpoints.
- `stratadb-schema`: canonical event, typed values, validation limits, JSON
  mapping, event encoding v1.
- `stratadb-wal`: WAL format v1, async/sync/group-commit durability,
  recovery with fail-closed corruption policy, safe reclamation.
- `stratadb-segment`: segment format v1 (provisional), per-chunk checksums, verify.
- `stratadb-storage`: engine with memtable, flush protocol, manifest v1,
  orphan cleanup/quarantine, tenant-scoped snapshots, backpressure.
- `stratadb-query`: StrataQL v0 parser, planner (projection + segment
  pruning), executor with budgets, EXPLAIN/EXPLAIN ANALYZE.
- `strata` CLI: init, ingest, query, flush, segments list/verify, status.
- Process-crash test matrix (15 points), power-loss model tests, golden v1
  compatibility fixture, property tests, ADRs 0001–0017.
- `strata-server`: HTTP API v1 (ingest, query, health, ready, version,
  metrics) with API-token auth, RBAC permission checks, tenant-from-token,
  bounded concurrency, body limits, loopback-by-default (ADR-0018).
- Idempotency keys (ADR-0019), compaction and retention with durable obsolete
  lifecycle (ADR-0020), verified backup/restore (ADR-0010); CLI and HTTP admin endpoints.
- TLS for the HTTP listener (ADR-0021), hash-chained audit log (ADR-0022),
  disk free-space reserve admission control.
- `strata upgrade`: dry run, verified pre-upgrade backup, crash-safe segment rewrite.
- Token hot reload, per-tenant ingest-rate and storage quotas, JSON structured logs with request ids.
- OTLP/HTTP protobuf (hand-written bounded decoder, protoc-verified) and interop with the official OTel Go exporter.
- Segment format v3: shredded hot attribute columns (RFC-0002): attribute queries 2.6–4.5× faster, segments −12 %.
- `strata doctor`: read-only diagnostics (formats, permissions, disk, WAL, segments, Bloom, audit chain).
- Segment format v2: per-chunk LZ4 compression (RFC-0001); v1 remains readable and is upgraded by compaction.
- Query status/cancellation endpoints, OpenAPI 3.1 document, fuzz harness (stable mutation + cargo-fuzz).
- Python and Go SDKs (standard library only) with conformance tests against a real server.
- Rust SDK `stratadb-client` (retries with stable idempotency keys, gzip, TLS CA, cancellation, metrics hook).
- TOML configuration with env/flag precedence and `strata-server config`; `healthcheck`; Docker/compose/systemd files.
- OTLP/HTTP JSON logs endpoint (`/v1/logs`) and gzip request bodies with decompression-bomb limits.
- Per-segment Bloom filters (F-003) with EXPLAIN/`indexes_used` reporting and an A/B benchmark report.
- `stratadb-testkit` seeded dataset generator (7 profiles) and `strata-bench`
  harness; first internal baseline report (no claims).
