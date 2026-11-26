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

## Durability / testing gaps
- Power loss is tested by **simulation** (simfs recorder: lost/torn/reordered
  un-synced sectors, lost un-synced directory operations), sampling 2–6 states
  per crash point rather than enumerating all of them. Devices that ignore
  flush requests ("lying hardware") are out of scope. Damage to the last
  acknowledged WAL record with no later frame is indistinguishable from a torn
  tail (truncated).
- Fsync failures poison the WAL by design; recovery requires restart.

## Performance
- Only developer-laptop (HW-REF-7) measurements exist (`benchmarks/reports/`). **No performance claims are made**, and there are no competitor comparisons. No spec Part VIII level is achieved (RELEASE_DECLARATION.md).
- Of the 416 spec Part VIII–IX requirements, most are MISSING
  (PERF_INVENTORY.md; phase plan in ADR-0026).
- The regression gate and the cost ledger work, but on a shared machine
  they give false verdicts: the A/A test found 9 of 27. CI runs only the
  smoke scenario until a dedicated runner exists. The ledger records CPU ns,
  not cycles.
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
  snapshots copy `Arc` handles of the tenant's rows (O(memtable rows) per query).
- Every committed segment keeps an open file descriptor (`strata doctor`
  checks the open-file limit against segment + WAL count).
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
