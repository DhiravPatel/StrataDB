# Changelog

## Unreleased (0.0.1)
### Added
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
