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
| [0027](0027-io-engine.md) | Storage I/O engine (pread + sim backends), segment file cache under a descriptor budget | Accepted (read path) |
