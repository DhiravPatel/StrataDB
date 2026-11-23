# Changelog

## Unreleased (0.0.1)
### Added
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
