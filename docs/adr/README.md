# Architecture Decision Records

Every major decision (spec §4.4) has an ADR. An ADR is never edited to change
its decision; it is superseded by a new ADR. Status meanings:

- **Accepted** — implemented and backed by tests.
- **Accepted (provisional)** — implemented, but the spec requires benchmark
  evidence that has not been gathered yet; the decision is explicitly revisitable
  and the affected persistent formats are versioned.
- **Proposed** — direction agreed for planning; not implemented.
- **Deferred** — intentionally not decided until its phase begins.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-storage-format-strategy.md) | Storage format strategy (custom v1 vs Parquet) | Accepted (provisional) |
| [0002](0002-wal-record-format.md) | WAL record format and recovery policy | Accepted |
| [0003](0003-segment-layout.md) | Segment layout v1 | Accepted (provisional) |
| [0004](0004-timestamp-partitioning.md) | Timestamp partition strategy | Proposed |
| [0005](0005-index-architecture.md) | Index architecture | Accepted (Bloom) / Proposed (postings, inverted) |
| [0006](0006-query-engine.md) | Query engine integration (custom vs DataFusion) | Accepted (provisional) |
| [0007](0007-catalog-format.md) | Catalog / manifest format | Accepted |
| [0008](0008-schema-evolution.md) | Schema evolution and type conflicts | Accepted |
| [0009](0009-durability-modes.md) | Durability modes and acknowledgement semantics | Accepted |
| [0010](0010-backup-format.md) | Backup format and procedure | Accepted |
| [0011](0011-distributed-metadata.md) | Distributed metadata and consensus | Deferred |
| [0012](0012-replication.md) | Replication semantics | Deferred |
| [0013](0013-authn-authz.md) | Authentication and authorization | Proposed |
| [0014](0014-object-storage.md) | Object storage interface | Deferred |
| [0015](0015-compatibility-policy.md) | Compatibility and upgrade policy | Accepted |
| [0016](0016-license.md) | Project license | Proposed (owner sign-off required) |
| [0017](0017-concurrency-model.md) | Storage engine concurrency model | Accepted |
| [0018](0018-http-server.md) | HTTP server framework and execution model | Accepted |
| [0019](0019-idempotency.md) | Bounded durable idempotency | Accepted |
| [0020](0020-compaction-retention.md) | Compaction and retention | Accepted |
| [0021](0021-tls.md) | TLS for the HTTP listener | Accepted |
| [0022](0022-audit-log.md) | Tamper-evident audit log | Accepted |
| [0023](0023-pagination.md) | Stable keyset pagination for row queries | Accepted |
| [0024](0024-encryption-at-rest.md) | Encryption at rest | Accepted |
| [0025](0025-parquet-import-export.md) | Parquet import/export (CLI) | Accepted |
| [0026](0026-performance-architecture-adoption.md) | Adopting spec Parts IV–IX: phase plan, conflicts, hardware honesty | Accepted |
| [0027](0027-io-engine.md) | Storage I/O engine (pread + sim backends), segment file cache under a descriptor budget | Accepted (read and write paths) |
| [0028](0028-layering-and-scan-source.md) | Mechanical layer check; ScanSource seam between storage and executor | Accepted |
| [0029](0029-query-log.md) | `system.query_log`, query fingerprints, per-query CPU time | Accepted |
| [0030](0030-memory-accounting.md) | Memory accounting hierarchy: budget, regions, tenant/query trackers, RSS gap | Accepted |
| [0031](0031-columnar-wal.md) | WAL v3: columnar, LZ4-compressed batch payloads; v2 rollback window | Accepted |
| [0032](0032-commit-wakeups-and-inflight-io.md) | One fsync per WAL; group-commit wake-up measured (parking_lot kept); per-device in-flight I/O bound | Accepted |
| [0033](0033-columnar-memtable.md) | Columnar memtable: column chunks, published row counts, zone maps, O(1) freeze, replay into columns | Accepted |
| [0034](0034-typed-string-columns-and-selections.md) | Typed string columns (plain/dictionary), dictionary filter and group-key kernels, selection vectors, count fast path | Accepted |
| [0035](0035-state-machine-metrics.md) | Spec IX.22 state machines: generic tracker, `strata_state_*` metrics, spec-conformance test, device health monitor | Accepted |
| [0044](0044-columnar-json-serializer.md) | Columnar JSON result serializer (per-column blocks, itoa/zmij, byte-identical to serde_json), encoded on the blocking pool, exact-length chunked body | Accepted |
| [0043](0043-normalized-key-sort-and-topk-bound.md) | Normalized-key sort (radix/pdqsort over a permutation), per-segment sorted runs merged by a loser tree, shared top-k bound; exact numeric ORDER BY | Accepted |
| [0042](0042-column-cache-and-studio-endpoints.md) | Decoded column cache with SLRU scan resistance and shared column buffers; whoami/schema/queries/backup-list endpoints for StrataStudio | Accepted |
| [0041](0041-strata-studio.md) | StrataStudio: pgAdmin-style web UI as a separate proxying binary with an embedded no-build-step UI | Accepted |
| [0040](0040-ordered-scans-plan-cache-network.md) | Ordered `ORDER BY event_time LIMIT` scans with early stop, sharded plan cache, TCP_NODELAY/keepalive, estimate Q-error | Accepted |
| [0039](0039-snapshot-catalog.md) | Snapshot catalog: arc-swap published read view, per-tenant copy-on-write segment lists, old segment lifetime, snapshot registry and `/admin/snapshots` | Accepted |
| [0038](0038-string-kernels-and-adaptive-filters.md) | SIMD substring search (memmem) and ASCII-folding ILIKE, adaptive conjunction order, constant folding, no-null string columns, std UTF-8 validation measured | Accepted |
| [0037](0037-metadata-aggregates-and-code-aggregation.md) | Aggregates answered from source metadata, dense aggregation on dictionary/byte codes, group-table growth reserved before allocation and reported in EXPLAIN ANALYZE | Accepted |
| [0036](0036-workload-classes-and-admission.md) | Workload classes, `FairQueue` (class/tenant/FIFO stride scheduling), morsel-stepped scan pool, admission with queue/refusals/demotion/AIMD | Accepted |
