# StrataDB: Master Engineering Specification

## A production-oriented, open-source database purpose-built for structured logs

**Document type:** Master product, architecture, implementation,
security, operations, and release specification\
**Status:** Proposed engineering baseline --- decisions must be
validated by RFCs and benchmarks\
**Version:** 0.1\
**Date:** 20 November 2026\
**License strategy:** Select an OSI-approved license after dependency
and patent review\
**Primary implementation language:** Rust\
**Initial deployment:** Single-node, self-hosted\
**Long-term deployment:** Distributed, multi-node, multi-tenant\
**Primary workload:** High-volume structured application and
infrastructure logs

------------------------------------------------------------------------------------------------------------------------------------------------

# 0. Executive direction

## 0.1 Product definition

StrataDB is an independent database management system designed to ingest,
durably store, index, search, and analyze structured log events. It is
not merely a logging library, dashboard, log shipper, or wrapper around
ClickHouse, PostgreSQL, Elasticsearch, or another database.

The product owns the database responsibilities:

-   A durable write path and recovery protocol.
-   A storage layout and segment lifecycle.
-   A catalog and metadata model.
-   Index structures and index lifecycle.
-   A query language and execution pipeline.
-   Resource governance and isolation.
-   Retention, deletion, backup, and restore.
-   Optional distributed placement, replication, and query coordination.
-   Operational interfaces, metrics, diagnostics, and upgrade
    procedures.

StrataDB may use mature open-source libraries for bounded responsibilities.
Using Arrow arrays, Parquet interoperability, DataFusion operators,
Tokio, or cryptographic libraries does not make StrataDB a wrapper. The
differentiating database behavior must remain owned, documented, tested,
and replaceable by the StrataDB project.

## 0.2 Product positioning

**Positioning:** A self-hostable, log-native analytical database
optimized for durable ingestion, high-cardinality structured search,
time-bounded investigation, and cost-efficient retention.

The project must not claim to be universally faster than ClickHouse,
BigQuery, Loki, VictoriaLogs, Elasticsearch, or any other system before
comparable benchmarks establish the claim.

The strategy is to win defined workloads, disclose trade-offs, and
publish reproducible results.

## 0.3 Product principles

1.  Correctness before speed.
2.  Durability semantics must be explicit.
3.  No acknowledged write may disappear under the failure model promised
    by the configured durability mode.
4.  A query must never return data from another tenant.
5.  Resource limits must be enforceable, not merely documented.
6.  Every persistent format must be versioned and recoverable.
7.  Storage and index structures must be inspectable and verifiable.
8.  Performance claims must be reproducible.
9.  Single-node operation must be useful and simple.
10. Distributed mode must not be required for a useful first release.
11. No mandatory paid API, hosted control plane, or proprietary cloud
    service.
12. Open-source dependencies must be reviewed for license, security,
    maintenance, and supply-chain risk.
13. Operators must be able to back up, restore, upgrade, and diagnose
    the system without vendor assistance.
14. A failed optimization must be safely disableable.
15. Compatibility and migration are product features.

## 0.4 Non-goals for the initial release

The first release is not intended to be:

-   A general OLTP relational database.
-   A replacement for PostgreSQL for transactional business data.
-   A general-purpose warehouse with arbitrary joins as its primary
    workload.
-   A metrics TSDB with every Prometheus feature.
-   A full-text search engine for arbitrary documents with all
    Elasticsearch semantics.
-   A managed SaaS service.
-   A Kubernetes-only product.
-   A system that promises unlimited ingestion or retention on finite
    hardware.
-   A system that claims exactly-once end-to-end delivery without
    defining the client, transport, storage, and acknowledgement
    boundaries.

## 0.5 Release strategy

  -----------------------------------------------------------------------------------------------------------------------------------------------
  Release                 Scope                   Exit condition
  -----------------------------------------------------------------------------------------------------------------------------------------------
  Research prototypes     WAL, segment, index,    Measured and reviewed
                          query experiments       design decisions

  0.1                     Single-node durable     Crash recovery and
                          database                basic queries pass

  0.2                     Useful single-node      SDKs, retention,
                          product                 backup, security, docs

  0.3                     Multi-process /         Failure tests and
                          multi-node experimental recovery protocol

  0.5                     Distributed beta        Replication, routing,
                                                  rebalancing, operations

  0.9                     Release candidate       Security review,
                                                  compatibility, load and
                                                  chaos tests

  1.0                     Stable self-hosted      All release gates in
                          release                 this document pass
  -----------------------------------------------------------------------------------------------------------------------------------------------

Do not skip release gates to reach a marketing date.

------------------------------------------------------------------------------------------------------------------------------------------------

# 1. Users, use cases, and workload contract

## 1.1 Primary users

-   Backend and platform engineers investigating production incidents.
-   SRE and DevOps teams operating services.
-   SaaS operators collecting tenant application logs.
-   Security teams searching audit and security events.
-   Developers needing local or self-hosted log analytics.
-   Infrastructure teams retaining large volumes of structured events.

## 1.2 Primary use cases

1.  Find all error events for a service during a time interval.
2.  Find records by exact trace ID, request ID, correlation ID, or event
    ID.
3.  Search structured fields such as tenant, user, region, status code,
    and error code.
4.  Search message text within a bounded time range.
5.  Aggregate error counts by service, version, region, or time bucket.
6.  Investigate a trace across multiple services.
7.  Compare error rates before and after a deployment.
8.  Retain logs for a configured period and delete expired data.
9.  Export a query result for offline analysis.
10. Operate an isolated database for multiple tenants.
11. Replicate data across storage nodes in a cluster.
12. Recover from process, machine, disk, and network failures within
    documented guarantees.

## 1.3 Workload dimensions

Every benchmark and capacity plan must specify:

-   Events per second and bytes per second.
-   Average, p50, p95, and maximum event size.
-   Number of fields per event.
-   Cardinality per field.
-   Null and missing-field rates.
-   Data type distribution.
-   Timestamp disorder and late-arriving events.
-   Duplicate rate.
-   Tenant count and tenant skew.
-   Service count.
-   Query mix and concurrency.
-   Hot versus cold query distribution.
-   Retention period.
-   Compression and encryption settings.
-   Durability and replication mode.
-   Hardware and filesystem.

A benchmark result without this workload description is incomplete.

## 1.4 Workload classes

### Class A: Recent incident search

-   Time range: seconds to minutes.
-   Typical filters: tenant, service, severity, trace ID, request ID.
-   Desired behavior: low latency and predictable resource usage.
-   Optimization: partition pruning, exact-match indexes, hot segment
    cache.

### Class B: Historical investigation

-   Time range: hours to days.
-   Typical filters: service, deployment, error code, message text.
-   Desired behavior: scan only relevant data and columns.
-   Optimization: segment statistics, dictionaries, inverted indexes,
    vectorized scans.

### Class C: Aggregation

-   Group by service, level, region, deployment, or time bucket.
-   Desired behavior: efficient column scans and partial aggregation.
-   Optimization: vectorized execution, parallel scans, pre-aggregation
    only when proven useful.

### Class D: High-cardinality lookup

-   Trace IDs, request IDs, user IDs, session IDs, arbitrary
    identifiers.
-   Desired behavior: avoid unbounded per-value metadata and
    uncontrolled index growth.
-   Optimization: selective inverted indexes, compact postings, hash
    indexes, dictionary decisions based on measured distributions.

### Class E: Full-text search

-   Search message and selected text fields.
-   Desired behavior: bounded, documented semantics.
-   Optimization: token indexes, n-gram indexes, or scan fallback
    depending on field and query.
-   Initial limitation: do not promise arbitrary regex search to be
    constant-time.

### Class F: Continuous ingestion with concurrent queries

-   Sustained writes while historical reads and compaction run.
-   Desired behavior: ingestion backpressure, bounded memory, fair
    resource scheduling, no starvation.

------------------------------------------------------------------------------------------------------------------------------------------------

# 2. Functional requirements

## 2.1 Ingestion

The system must support:

-   Single-event and batch ingestion.
-   HTTP ingestion endpoint.
-   A documented protobuf/gRPC endpoint when stable.
-   OpenTelemetry Protocol (OTLP) logs.
-   Rust, Go, and Python client libraries.
-   Authentication and tenant routing.
-   Schema inference with safe limits.
-   Explicit schema registration as an optional mode.
-   Event timestamp validation.
-   Server receive timestamp.
-   Idempotency key support.
-   Partial batch error reporting.
-   Configurable maximum event and batch sizes.
-   Compression for network payloads where supported.
-   Backpressure and retry hints.
-   Rate limiting and quotas.
-   Graceful shutdown and in-flight write draining.

The ingestion contract must distinguish:

-   Received by the HTTP process.
-   Validated.
-   Appended to WAL.
-   WAL fsynced or committed under the selected durability policy.
-   Visible to queries.
-   Flushed to immutable segments.
-   Replicated to the configured number of nodes.

The API must never use the ambiguous phrase "stored successfully"
without defining which state has been reached.

## 2.2 Data model

An event contains:

-   `event_time`: event timestamp supplied by the producer.
-   `ingest_time`: time StrataDB accepted the event.
-   `tenant_id`: derived from authenticated identity, not trusted from
    arbitrary payload.
-   `service`: optional normalized service identity.
-   `severity`: normalized severity plus original representation if
    needed.
-   `body`: message text or structured body.
-   `attributes`: typed key-value map.
-   `resource_attributes`: service/process/host/container context.
-   `trace_id`, `span_id`: optional trace correlation fields.
-   `event_id`: optional producer event identifier.
-   `schema_version`: schema compatibility marker.

The physical representation may normalize common fields into dedicated
columns and preserve arbitrary attributes in a typed map or nested
representation.

Do not silently coerce incompatible values. Define type conflict
behavior, such as variant columns, schema evolution, rejection, or
explicit casts.

## 2.3 Query capabilities

The initial query language should support:

-   Projection.
-   Time range filters.
-   Equality and inequality.
-   Numeric comparisons.
-   AND / OR / NOT.
-   IN.
-   IS NULL / IS MISSING semantics.
-   LIKE or documented text matching.
-   ORDER BY on supported fields.
-   LIMIT.
-   GROUP BY.
-   COUNT, MIN, MAX, SUM, AVG.
-   Time bucket functions.
-   JSON or map field access.
-   Query timeout and cancellation.
-   Pagination with stable semantics.
-   EXPLAIN and EXPLAIN ANALYZE.

Do not expose SQL syntax that is not implemented correctly. Maintain a
grammar and compatibility tests.

## 2.4 Administration

-   Create and drop databases/namespaces.
-   Create and alter log tables or streams.
-   Manage tenants and credentials.
-   Inspect segments and index health.
-   Configure retention.
-   Run consistency checks.
-   Compact selected partitions.
-   Export and import data.
-   Create and verify backups.
-   Inspect active queries.
-   Inspect ingestion queues and WAL state.
-   Drain or decommission a node.
-   Inspect cluster membership and replication state.

## 2.5 Observability

StrataDB must expose its own health and telemetry without requiring StrataDB
to monitor itself.

Provide:

-   Prometheus metrics endpoint.
-   Structured internal logs.
-   OpenTelemetry traces for ingestion and query paths.
-   Health and readiness endpoints.
-   Build information and format versions.
-   Diagnostic bundle generation with secrets redacted.

------------------------------------------------------------------------------------------------------------------------------------------------

# 3. Non-functional requirements and service objectives

These are design objectives and acceptance targets to be calibrated
through benchmark results. They are not pre-existing performance claims.

## 3.1 Correctness

-   No silent data corruption.
-   WAL recovery is deterministic.
-   Segment checksums are verified.
-   Indexes can be rebuilt from authoritative data.
-   Query results are correct under concurrent ingestion.
-   Tenant isolation is enforced at every query and storage boundary.
-   Retention deletion follows the documented deletion model.
-   Backup restore is tested, not merely backup creation.

## 3.2 Performance objectives

Define workload-specific targets after the first baseline benchmark.
Record:

-   Sustained ingest events/s and bytes/s.
-   Burst ingest duration and recovery.
-   p50/p95/p99 write acknowledgement latency.
-   p50/p95/p99 query latency by query class.
-   Bytes scanned per query.
-   CPU cycles or CPU time per event where measurable.
-   Memory per active series/field/segment/tenant.
-   Compression ratio and bytes per event.
-   Compaction write amplification.
-   Recovery time after process crash.
-   Query concurrency before latency degradation.

No single "events per second" number is sufficient.

## 3.3 Availability and durability

Every deployment mode must document:

-   Failure model.
-   Acknowledgement policy.
-   Replication factor.
-   Quorum behavior.
-   Expected recovery time.
-   Data loss window, if any.
-   Behavior during network partitions.
-   Behavior when a replica is unavailable.
-   Behavior when disk is full.
-   Behavior during rolling upgrades.

For a single node, replication cannot protect against physical loss of
that node. Backups to a separate failure domain are required.

## 3.4 Resource governance

-   Per-query memory limit.
-   Per-query execution time limit.
-   Per-tenant ingestion quota.
-   Per-tenant storage quota.
-   Maximum batch size.
-   Maximum field count.
-   Maximum nesting depth.
-   Maximum string and body size.
-   Maximum concurrent queries.
-   Compaction CPU and I/O budget.
-   Ingestion queue bound.
-   Disk high-watermark behavior.

------------------------------------------------------------------------------------------------------------------------------------------------

# 4. System architecture

## 4.1 Logical architecture

``` text
Applications / Agents / OTLP
             |
             v
      Ingestion Gateway
             |
      Authentication / Tenant
             |
      Validation / Normalization
             |
      Batch Builder / Backpressure
             |
             v
       WAL Commit Layer
             |
             v
       Active Memtable
             |
       Flush / Segment Builder
             |
       Immutable Segment Store
        /       |        \
   Indexes   Metadata   Statistics
        \       |        /
             Query Engine
                  |
          API / SQL / CLI
```

Background services:

-   Compaction.
-   Retention.
-   Index build and repair.
-   Backup.
-   Scrubbing and integrity verification.
-   Replication.
-   Rebalancing.
-   Statistics collection.

## 4.2 Single-node architecture first

The first useful product is a single-node database. All modules should
have explicit interfaces, but they can run in one process.

Advantages:

-   Easier correctness reasoning.
-   No distributed consensus complexity during initial storage
    development.
-   Lower operational burden.
-   Easier local testing.
-   Simpler installation.
-   Faster iteration.

The single-node design must not hard-code assumptions that make
distributed operation impossible. However, do not introduce network RPC
abstractions everywhere without a concrete need.

## 4.3 Process boundaries

Initial process:

-   `stratadb-server`: API, WAL, memtable, catalog, query engine,
    background workers.

Later optional processes:

-   `stratadb-ingester`.
-   `stratadb-storage-node`.
-   `stratadb-query-node`.
-   `stratadb-coordinator`.
-   `stratadb-compactor`.

Process separation is a deployment option, not a requirement for the
first release.

## 4.4 Architecture decision records

Every major decision must have an ADR containing:

-   Context.
-   Problem.
-   Options considered.
-   Decision.
-   Consequences.
-   Alternatives rejected.
-   Benchmark or correctness evidence.
-   Revisit conditions.

Required ADRs:

1.  Storage format.
2.  WAL record format.
3.  Segment layout.
4.  Timestamp partition strategy.
5.  Index architecture.
6.  Query engine integration.
7.  Catalog format.
8.  Schema evolution.
9.  Durability modes.
10. Backup format.
11. Distributed metadata and consensus.
12. Replication semantics.
13. Authentication and authorization.
14. Object storage interface.
15. Compatibility and upgrade policy.

------------------------------------------------------------------------------------------------------------------------------------------------

# 5. Rust workspace and repository architecture

## 5.1 Proposed repository

``` text
stratadb/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── rustfmt.toml
├── clippy.toml
├── README.md
├── LICENSE
├── NOTICE
├── docs/CONTRIBUTING.md
├── docs/CODE_OF_CONDUCT.md
├── docs/SECURITY.md
├── docs/project/GOVERNANCE.md
├── CHANGELOG.md
├── .github/
│   ├── workflows/
│   ├── ISSUE_TEMPLATE/
│   └── PULL_REQUEST_TEMPLATE.md
├── crates/
│   ├── stratadb-common/
│   ├── stratadb-error/
│   ├── stratadb-config/
│   ├── stratadb-schema/
│   ├── stratadb-types/
│   ├── stratadb-wal/
│   ├── stratadb-memtable/
│   ├── stratadb-segment/
│   ├── stratadb-storage/
│   ├── stratadb-format/
│   ├── stratadb-parquet/
│   ├── stratadb-index/
│   ├── stratadb-catalog/
│   ├── stratadb-manifest/
│   ├── stratadb-query-parser/
│   ├── stratadb-query-planner/
│   ├── stratadb-query-execution/
│   ├── stratadb-ingestion/
│   ├── stratadb-otlp/
│   ├── stratadb-compaction/
│   ├── stratadb-retention/
│   ├── stratadb-backup/
│   ├── stratadb-security/
│   ├── stratadb-auth/
│   ├── stratadb-telemetry/
│   ├── stratadb-cluster/
│   ├── stratadb-replication/
│   ├── stratadb-object-store/
│   └── stratadb-testkit/
├── apps/
│   ├── stratadb-server/
│   ├── stratadb-cli/
│   ├── stratadb-bench/
│   ├── stratadb-doctor/
│   └── stratadb-console/
├── sdk/
│   ├── rust/
│   ├── go/
│   └── python/
├── integrations/
│   ├── opentelemetry/
│   ├── fluent-bit/
│   └── grafana/
├── proto/
├── sql/
├── docs/
│   ├── architecture/
│   ├── design/
│   ├── storage-format/
│   ├── query-language/
│   ├── security/
│   ├── operations/
│   ├── deployment/
│   ├── compatibility/
│   └── rfc/
├── tests/
│   ├── integration/
│   ├── crash-recovery/
│   ├── property/
│   ├── fuzz/
│   ├── concurrency/
│   ├── compatibility/
│   ├── distributed/
│   └── fixtures/
├── benchmarks/
│   ├── datasets/
│   ├── generators/
│   ├── workloads/
│   ├── baselines/
│   └── reports/
├── deployments/
│   ├── docker/
│   ├── compose/
│   ├── systemd/
│   └── examples/
└── scripts/
```

Create crates incrementally. Do not generate empty crates just to match
the final tree.

## 5.2 Dependency direction

-   Common types must not depend on server code.
-   Storage must not depend on HTTP.
-   Query execution must depend on abstract scan interfaces, not the
    server.
-   API must call service interfaces.
-   SDKs must not depend on server internals.
-   Security checks must be enforced in service boundaries, not only in
    the HTTP layer.
-   Background workers must use the same catalog and storage invariants
    as foreground operations.

## 5.3 Rust engineering standards

-   Use stable Rust unless an ADR justifies nightly.
-   Pin toolchain in `rust-toolchain.toml`.
-   Commit `Cargo.lock` for applications.
-   Run `cargo fmt --check`.
-   Run `cargo clippy --all-targets --all-features -- -D warnings` where
    feasible.
-   Use `cargo test --workspace`.
-   Use property testing for formats and parsers.
-   Use fuzzing for untrusted parsers and decoders.
-   Avoid `unsafe` unless justified by measured need and reviewed.
-   Document every unsafe block's invariants.
-   Avoid unbounded channels and unbounded task spawning.
-   Avoid holding locks across `.await`.
-   Define cancellation behavior.
-   Define error categories and retryability.
-   Keep dependency features minimal.

------------------------------------------------------------------------------------------------------------------------------------------------

# 6. Data model and schema system

## 6.1 Canonical event model

Conceptual event:

``` json
{
  "event_time": "2026-11-20T10:30:00.123Z",
  "ingest_time": "2026-11-20T10:30:00.456Z",
  "service": "payment-service",
  "severity": "ERROR",
  "body": "Payment authorization timed out",
  "trace_id": "0123456789abcdef0123456789abcdef",
  "span_id": "0123456789abcdef",
  "attributes": {
    "request_id": "req_123",
    "user_id": "user_456",
    "http.status_code": 504,
    "region": "ap-south-1"
  }
}
```

`tenant_id` is attached by the authenticated ingestion context. If the
payload contains a tenant identifier, it must not override the
authenticated tenant.

## 6.2 Field types

Initial supported types:

-   Boolean.
-   Signed and unsigned integers with explicit widths.
-   Float32 and Float64.
-   UTF-8 string.
-   Binary.
-   Timestamp with explicit unit and timezone policy.
-   Date.
-   Decimal where supported.
-   List.
-   Struct/map.
-   Null.
-   Variant for controlled mixed-type cases.

Do not make all attributes strings. Type preservation improves query
correctness and compression.

## 6.3 Missing versus null

Define distinct semantics:

-   Missing: field not present in event.
-   Null: field explicitly present with null value.
-   Empty string: field exists and contains zero-length text.

Queries and indexes must preserve this distinction where the schema
supports it.

## 6.4 Schema evolution

Rules:

-   Additive nullable fields should be straightforward.
-   Type changes require an explicit compatibility policy.
-   Incompatible changes must not silently reinterpret historical bytes.
-   Every segment records schema version.
-   Readers support the declared compatibility window.
-   Schema registry metadata is versioned.
-   A migration tool reports affected segments and estimated work.
-   Unknown fields must be handled according to explicit policy:
    preserve, reject, or place in an attributes map.

## 6.5 Event validation

Validate:

-   Maximum serialized event size.
-   Maximum attribute count.
-   Maximum nesting depth.
-   Maximum key length.
-   Maximum string length.
-   Timestamp range.
-   UTF-8 correctness.
-   Allowed numeric values.
-   Reserved field names.
-   Tenant identity.
-   Schema compatibility.

Reject or quarantine invalid records according to batch error semantics.
Never silently drop malformed records.

------------------------------------------------------------------------------------------------------------------------------------------------

# 7. Ingestion architecture

## 7.1 Ingestion pipeline

1.  Accept request.
2.  Authenticate caller.
3.  Resolve tenant and permissions.
4.  Enforce request and tenant quotas.
5.  Decode transport payload.
6.  Validate batch limits.
7.  Normalize timestamps and severity.
8.  Resolve schema.
9.  Construct canonical events.
10. Append records to WAL.
11. Commit according to durability policy.
12. Acknowledge with committed offset or batch identifier.
13. Publish committed records to memtable.
14. Flush to immutable segments asynchronously.

The implementation must ensure there is no gap where a write is
acknowledged but is neither recoverable from WAL nor durable in a
segment.

## 7.2 Batch semantics

A batch response must identify:

-   Accepted count.
-   Rejected count.
-   Duplicate count if deduplication is enabled.
-   Per-record errors where supported.
-   Commit identifier.
-   Durability level reached.
-   Retryable versus permanent failures.

Avoid ambiguous all-or-nothing behavior unless the WAL and API
explicitly implement atomic batch commits.

## 7.3 Idempotency

Support an optional producer-supplied event ID or idempotency key.

Document:

-   Scope of uniqueness.
-   Retention period of deduplication metadata.
-   Whether deduplication survives restart.
-   Behavior across replicas.
-   Cost of deduplication.
-   What happens when the same key is sent with different payload bytes.

Do not promise global exactly-once delivery. The defensible initial
contract is at-least-once ingestion with optional bounded deduplication
and idempotent client retries.

## 7.4 Backpressure

-   Bound all queues.
-   Return explicit retry guidance.
-   Support server-side rate limits.
-   Shed low-priority work only under documented policy.
-   Reserve capacity for WAL recovery and administrative operations.
-   Stop accepting writes before disk exhaustion.
-   Expose queue depth and saturation metrics.
-   Ensure compaction cannot starve ingestion indefinitely.
-   Ensure ingestion cannot starve compaction indefinitely.

## 7.5 Timestamp handling

Store both event time and ingest time.

Support late-arriving events. Define:

-   Partition assignment based on event time or ingest time.
-   Maximum accepted timestamp skew.
-   Behavior for extremely old events.
-   Behavior for future timestamps.
-   How late data enters sealed partitions.
-   Whether late data creates new segments or triggers rewrite.
-   Query semantics for event-time filters.

Do not use producer timestamps as a security or ordering guarantee.

## 7.6 OTLP

Implement OTLP logs with documented mapping:

-   Resource attributes.
-   Scope attributes.
-   Log record body.
-   Severity text and number.
-   Timestamp and observed timestamp.
-   Trace ID and span ID.
-   Event attributes.

Preserve fields that cannot be mapped losslessly. Provide protocol
conformance tests and interoperability fixtures.

------------------------------------------------------------------------------------------------------------------------------------------------

# 8. WAL and durability engine

## 8.1 WAL responsibilities

The WAL is the authoritative recovery source for committed records that
have not yet been safely represented in durable segments.

It must provide:

-   Append-only records.
-   Framing.
-   Checksums.
-   Segment rotation.
-   Commit markers or equivalent transaction boundaries.
-   Recovery scan.
-   Torn-write detection.
-   Truncation and corruption handling.
-   Sync policy.
-   Safe segment reclamation.
-   Format versioning.

## 8.2 WAL record format

A proposed framed record:

``` text
+--------------------+
| Magic              |
+--------------------+
| Format version     |
+--------------------+
| Record type        |
+--------------------+
| Sequence number   |
+--------------------+
| Payload length     |
+--------------------+
| Payload            |
+--------------------+
| Checksum           |
+--------------------+
```

The exact byte layout must be defined in a versioned storage-format
document. Specify endianness, integer widths, checksum algorithm,
maximum payload length, alignment, and compatibility rules.

## 8.3 Durability modes

Define explicit modes:

1.  `async`: acknowledge before fsync; possible loss window is
    documented.
2.  `sync`: acknowledge after local WAL sync.
3.  `group_commit`: batch multiple writes into one sync while preserving
    commit semantics.
4.  `replicated`: acknowledge after the configured replication/quorum
    condition in distributed mode.

Names may change, but semantics must not.

The default production mode should favor durability. A faster mode must
clearly disclose its risk.

## 8.4 Group commit

-   Collect concurrent append requests for a bounded time or byte
    threshold.
-   Write records in sequence.
-   Sync once per group according to configured policy.
-   Complete only the requests covered by the durable commit boundary.
-   Expose group size and sync latency.
-   Bound wait time.
-   Avoid starvation under low traffic.

## 8.5 WAL recovery

Recovery must:

1.  Load the last committed manifest/catalog state.
2.  Identify WAL segments not safely reclaimed.
3.  Scan records in order.
4.  Validate framing and checksums.
5.  Stop or quarantine at the first invalid tail according to policy.
6.  Reconstruct committed records.
7.  Ignore incomplete uncommitted records.
8.  Rebuild memtable state.
9.  Reconcile flush transactions.
10. Reclaim only segments proven unnecessary.

Test crashes at every transition, including during append, sync, flush,
manifest update, rename, and WAL deletion.

## 8.6 WAL reclamation

Never delete a WAL segment merely because a flush task returned success.

A safe protocol must establish:

-   Segment output files are durable.
-   Segment metadata is durable.
-   Manifest/catalog points to the segment.
-   Checksums are verified or covered by the durability protocol.
-   Recovery can discover the committed segment.
-   The WAL range is fully covered.
-   No active snapshot or recovery operation still needs the WAL.

Use atomic metadata transitions and filesystem-specific durability
practices. Document assumptions about rename and directory fsync on
supported platforms.

------------------------------------------------------------------------------------------------------------------------------------------------

# 9. Memtable and flush pipeline

## 9.1 Memtable goals

-   Fast append after WAL commit.
-   Bounded memory.
-   Concurrent readers.
-   Snapshot consistency.
-   Efficient conversion to columnar arrays.
-   Deterministic flush boundaries.

## 9.2 Candidate structures

Evaluate:

-   Append-only row batches.
-   Column-oriented mutable batches.
-   Per-column vectors.
-   Partitioned memtables.
-   Skiplist or ordered structures only where required.
-   Hash structures for exact lookups only where justified.

Do not select a complex concurrent structure before measuring the actual
workload.

## 9.3 Flush triggers

Trigger flush on:

-   Memory threshold.
-   Maximum event count.
-   Maximum age.
-   WAL pressure.
-   Shutdown.
-   Administrative request.

Use hysteresis to avoid rapid flush/reopen cycles.

## 9.4 Snapshot semantics

A query must see a consistent combination of:

-   Immutable segments committed before its snapshot.
-   Memtable records visible at the snapshot boundary.
-   No duplicate visibility during flush transition.

The flush protocol must prevent both missing records and double-counted
records.

------------------------------------------------------------------------------------------------------------------------------------------------

# 10. Segment and storage format

## 10.1 Storage design

Use immutable segments as the primary analytical storage unit.

A segment may contain:

-   Segment header.
-   Format version.
-   Tenant and table/stream identity.
-   Time bounds.
-   Sequence-number bounds.
-   Schema version.
-   Column chunks.
-   Per-column statistics.
-   Bloom filters or index references.
-   Row-group metadata.
-   Checksums.
-   Footer and commit metadata.

The exact design must be benchmarked against Parquet and other viable
formats.

## 10.2 Custom format versus Parquet

Do not assume a custom format is automatically faster.

Evaluate at least:

1.  Parquet as active segment format.
2.  Custom hot segment format with Parquet export/cold tier.
3.  Arrow IPC or another intermediate representation where useful.

Decision criteria:

-   Write throughput.
-   Read latency.
-   Compression ratio.
-   Random access.
-   Metadata overhead.
-   Recovery complexity.
-   Schema evolution.
-   Interoperability.
-   Implementation and maintenance cost.

A custom format creates a permanent compatibility and maintenance
obligation. Choose it only if benchmarks demonstrate a meaningful
advantage.

## 10.3 Row groups and column chunks

Benchmark row-group sizing across:

-   Small event batches.
-   Large batches.
-   Narrow projections.
-   Wide projections.
-   Exact-match filters.
-   Time-range scans.
-   Aggregations.

Small row groups can improve pruning but increase metadata overhead.
Large row groups can improve sequential throughput but increase read
amplification.

## 10.4 Segment lifecycle

States:

-   `building`
-   `prepared`
-   `committed`
-   `compacting`
-   `obsolete`
-   `deleted`
-   `quarantined`

State transitions must be durable and recoverable.

## 10.5 Manifest

Manifest must record:

-   Segment ID.
-   Tenant/table/partition.
-   Object or file location.
-   Format version.
-   Schema version.
-   Time range.
-   Sequence range.
-   Size.
-   Row count.
-   Checksums.
-   Index references.
-   Creation time.
-   Compaction lineage.
-   Visibility state.

Manifest updates must be atomic or use a journaled protocol. A partially
written manifest must never cause silent data loss.

## 10.6 Checksums and corruption

-   Verify segment footer and metadata.
-   Verify checksums during read or background scrubbing.
-   Expose corruption metrics.
-   Quarantine corrupt segments.
-   Rebuild derived indexes from authoritative segment data.
-   Recover from replica or backup where available.
-   Never silently return incomplete results without an explicit
    partial-result marker.

------------------------------------------------------------------------------------------------------------------------------------------------

# 11. Compression and encoding

## 11.1 Candidate encodings

-   Dictionary encoding.
-   Run-length encoding.
-   Delta encoding.
-   Delta-of-delta for monotonic timestamps.
-   Bit packing.
-   Frame-of-reference.
-   Boolean bitmaps.
-   Null bitmaps.
-   UTF-8 dictionary plus offsets.
-   Zstd.
-   LZ4.
-   Uncompressed path for data where compression is counterproductive.

## 11.2 Adaptive selection

For each column or chunk, collect sample statistics:

-   Cardinality estimate.
-   Average value length.
-   Null fraction.
-   Repetition.
-   Monotonicity.
-   Numeric range.
-   Entropy estimate.
-   CPU cost of encoding/decoding.

Choose an encoding based on measured cost model. Persist encoding
identifier and parameters in the format.

## 11.3 Compression policy

Evaluate compression levels by workload and CPU budget. Do not use the
highest compression level by default. Provide configurable policies for:

-   Ingest CPU constrained.
-   Balanced.
-   Storage constrained.
-   Cold archival.

## 11.4 Compression benchmarks

Measure:

-   Compression ratio.
-   Encode throughput.
-   Decode throughput.
-   CPU time per MB.
-   Query latency after decompression.
-   Memory usage.
-   Impact on compaction.

Report per-field-type results, not only aggregate ratios.

------------------------------------------------------------------------------------------------------------------------------------------------

# 12. Index architecture

## 12.1 Indexing principles

-   Indexes are derived structures unless explicitly part of
    authoritative metadata.
-   Every index must be rebuildable or recoverable.
-   Index memory and disk costs must be observable.
-   High-cardinality fields must not create unbounded in-memory
    metadata.
-   Index use must be visible in `EXPLAIN`.
-   The query engine must have a correct scan fallback.
-   Index corruption must not corrupt source data.

## 12.2 Primary pruning metadata

Every segment/row group should consider:

-   Minimum and maximum timestamp.
-   Minimum and maximum numeric values where useful.
-   Null counts.
-   Row count.
-   Column presence.
-   Dictionary metadata.
-   Bloom filters for selected fields.
-   Partition identity.

## 12.3 Exact-match indexes

Candidate structures:

-   Sorted postings.
-   Hash-based lookup.
-   Dictionary-to-row-id mapping.
-   Roaring bitmaps.
-   Compressed posting lists.

Choose based on cardinality, frequency, segment size, and query
distribution.

## 12.4 Inverted indexes

For selected string fields:

-   Tokenization rules.
-   Normalization.
-   Case sensitivity.
-   Unicode behavior.
-   Token position support if phrase search is supported.
-   Posting compression.
-   Segment-local indexing.
-   Index versioning.

Do not apply one tokenizer to IDs, URLs, stack traces, and
natural-language messages without a field policy.

## 12.5 Bloom filters

Use for probabilistic negative filtering, not exact results.

-   Define false-positive target.
-   Define hash algorithm.
-   Define seed/version.
-   Define memory and disk overhead.
-   Test adversarial inputs.
-   Never treat a positive Bloom result as proof of membership.

## 12.6 High-cardinality strategy

Separate:

-   Low-cardinality dimensions: service, level, environment, region.
-   Medium-cardinality dimensions: deployment, host, container.
-   High-cardinality identifiers: trace ID, request ID, user ID, session
    ID.

Use segment-local dictionaries and postings. Avoid global per-value
metadata unless justified.

## 12.7 Adaptive indexing

Potential policy:

1.  Observe query predicates.
2.  Aggregate field access patterns.
3.  Estimate selectivity and cost.
4.  Estimate index size and build cost.
5.  Recommend an index.
6.  Optionally build index in background after policy approval.
7.  Track benefit and overhead.
8.  Drop or downgrade unused indexes only under safe policy.

The first release should provide recommendations and manual index
management. Automatic index mutation should wait until the cost model is
trustworthy.

## 12.8 Index lifecycle

-   Build.
-   Validate.
-   Publish.
-   Query.
-   Rebuild.
-   Compact.
-   Drop.
-   Verify.

Index publication must be atomic with manifest visibility rules.

------------------------------------------------------------------------------------------------------------------------------------------------

# 13. Query language and execution engine

## 13.1 Query design

A SQL-like language is preferred, but the first release should support a
deliberately limited and documented subset.

Example:

``` sql
SELECT
    event_time,
    service,
    severity,
    body,
    trace_id
FROM logs
WHERE tenant_id = 'tenant-a'
  AND event_time >= TIMESTAMP '2026-11-20 10:00:00'
  AND event_time <  TIMESTAMP '2026-11-20 11:00:00'
  AND severity = 'ERROR'
ORDER BY event_time DESC
LIMIT 100;
```

Tenant identity should normally be injected from authenticated context
rather than accepted as a user-controlled predicate.

## 13.2 Parser

-   Define grammar in a version-controlled file.
-   Reject unsupported syntax with useful errors.
-   Enforce maximum query length and nesting.
-   Avoid catastrophic regex behavior.
-   Parameterize values.
-   Preserve source spans for diagnostics.
-   Fuzz parser and expression handling.

## 13.3 Logical plan

Represent:

-   Scan.
-   Filter.
-   Projection.
-   Limit.
-   Sort.
-   Aggregate.
-   Time bucket.
-   Unnest or JSON extraction only if supported.

## 13.4 Optimizer

Initial optimizations:

-   Predicate pushdown.
-   Projection pruning.
-   Constant folding.
-   Partition pruning.
-   Segment pruning.
-   Row-group pruning.
-   Index selection.
-   Filter reordering based on cost/selectivity.
-   Limit pushdown where semantically safe.
-   Partial aggregation.
-   Parallel scan scheduling.

Every optimization needs correctness tests comparing optimized and
unoptimized results.

## 13.5 Physical execution

-   Columnar batches.
-   Vectorized operators.
-   Bounded batch sizes.
-   Parallel segment scans.
-   Cooperative cancellation.
-   Memory accounting.
-   Spill-to-disk only after designed and tested.
-   Fair scheduling.
-   Backpressure between operators.

## 13.6 Query resource controls

-   Maximum execution time.
-   Maximum memory.
-   Maximum bytes scanned.
-   Maximum result rows.
-   Maximum result bytes.
-   Maximum concurrent queries.
-   Per-tenant concurrency.
-   Cancellation token.
-   Optional priority classes.

A query that exceeds a budget must fail with a clear resource-limit
error or return explicitly marked partial output if partial semantics
are intentionally supported.

## 13.7 EXPLAIN

Show:

-   Partitions considered and pruned.
-   Segments considered and pruned.
-   Indexes used.
-   Columns read.
-   Estimated rows.
-   Actual rows where analyzed.
-   Bytes scanned.
-   CPU and wall time.
-   Memory peak.
-   Operator tree.
-   Cache hits and misses.

------------------------------------------------------------------------------------------------------------------------------------------------

# 14. Full-text and message search

## 14.1 Search modes

Support distinct, documented modes:

-   Exact phrase.
-   Token search.
-   Prefix search.
-   Field equality.
-   Substring scan.
-   Regex only if safely bounded.

Do not make users believe all modes have equal performance.

## 14.2 Tokenization

Define:

-   Unicode normalization.
-   Case folding.
-   Token boundaries.
-   Punctuation.
-   CJK handling.
-   Stop-word policy, if any.
-   Minimum token length.
-   Maximum token length.
-   Versioning.

## 14.3 Regex safety

-   Apply execution time limits.
-   Apply memory limits.
-   Prefer a linear-time engine where compatible.
-   Disallow unsupported constructs rather than risk catastrophic
    backtracking.
-   Require time-range narrowing for expensive operations.
-   Expose scanned bytes and cancellation.

------------------------------------------------------------------------------------------------------------------------------------------------

# 15. Catalog and metadata

## 15.1 Catalog responsibilities

-   Database and table definitions.
-   Tenant metadata.
-   Schema versions.
-   Segment manifest references.
-   Index definitions.
-   Retention policies.
-   Node and replica metadata in distributed mode.
-   Format compatibility state.
-   Backup metadata.

## 15.2 Single-node catalog

SQLite may be used as a bootstrap metadata store if its transaction and
durability behavior meet the requirements. A custom metadata format is
also possible but increases maintenance burden.

Do not use PostgreSQL as a mandatory dependency for the single-node
product.

## 15.3 Metadata correctness

-   Transactional updates.
-   Schema versioning.
-   Backup inclusion.
-   Crash recovery.
-   Consistency checking.
-   Migration support.
-   Locking model.
-   Atomic manifest publication.

------------------------------------------------------------------------------------------------------------------------------------------------

# 16. Compaction and retention

## 16.1 Compaction goals

-   Merge small segments.
-   Improve row-group layout.
-   Rebuild or merge indexes.
-   Remove obsolete versions.
-   Improve compression where worthwhile.
-   Reduce metadata overhead.
-   Avoid write amplification and resource starvation.

## 16.2 Compaction policy

Candidate triggers:

-   Segment count.
-   Small-segment ratio.
-   Index fragmentation.
-   Overlapping time ranges.
-   Tombstone density.
-   Retention boundaries.

Policies must be configurable and observable.

## 16.3 Compaction safety

-   Inputs remain query-visible until output is committed.
-   Output is verified before publication.
-   Manifest switch is atomic.
-   Crash before switch leaves inputs valid.
-   Crash after switch leaves output discoverable.
-   Old inputs are deleted only after readers and snapshots no longer
    require them.
-   Compaction is idempotent or safely resumable.

## 16.4 Retention

Support retention by:

-   Database.
-   Table/stream.
-   Tenant.
-   Time.
-   Optional legal hold or protected partition.

Deletion must be explicit about whether it is:

-   Query-invisible.
-   Removed from active files.
-   Removed from replicas.
-   Removed from backups after backup expiry.
-   Cryptographically erased where supported.

A retention policy is not equivalent to immediate physical erasure.

## 16.5 Tombstones and late data

Define how deletes interact with:

-   Immutable segments.
-   Compaction.
-   Replicas.
-   Backups.
-   Late-arriving records.
-   Concurrent queries.

------------------------------------------------------------------------------------------------------------------------------------------------

# 17. Caching

## 17.1 Cache layers

-   Metadata cache.
-   Index page cache.
-   Segment footer cache.
-   Compressed block cache.
-   Query result cache only when semantics are safe.
-   OS page cache.

## 17.2 Cache policy

-   Bounded memory.
-   Tenant-aware fairness.
-   Admission policy.
-   Eviction policy.
-   Cache metrics.
-   Cache invalidation on schema/segment changes.
-   No correctness dependence on cache contents.

## 17.3 Cache benchmarking

Measure warm and cold behavior separately. Do not compare one database
with a warm cache to another with a cold cache.

------------------------------------------------------------------------------------------------------------------------------------------------

# 18. Security architecture

Security is a release-blocking requirement, not a future enhancement.

## 18.1 Threat model

Protect against:

-   Unauthenticated ingestion and queries.
-   Tenant data leakage.
-   Credential theft.
-   Malicious query payloads.
-   Parser and decoder vulnerabilities.
-   Resource exhaustion.
-   Disk exhaustion.
-   Path traversal.
-   Symlink attacks.
-   Malicious file uploads/imports.
-   Compromised dependencies.
-   Privilege escalation.
-   Unsafe administrative endpoints.
-   Backup exposure.
-   Network interception.
-   Supply-chain compromise.
-   Denial of service.
-   Log injection and forged audit records.

## 18.2 Authentication

Initial methods:

-   Local admin bootstrap.
-   API tokens.
-   Rotatable service credentials.
-   Optional mTLS for internal cluster traffic.
-   OIDC integration later if implemented and tested.

Requirements:

-   Store password/token verifiers securely.
-   Never log credentials.
-   Support token expiry and revocation.
-   Support rotation without downtime where possible.
-   Rate-limit authentication attempts.
-   Use constant-time comparison where relevant.
-   Do not ship a universal default password.

## 18.3 Authorization

Use role-based access control with permissions such as:

-   `ingest`
-   `query`
-   `schema.read`
-   `schema.write`
-   `index.manage`
-   `retention.manage`
-   `backup.create`
-   `backup.restore`
-   `user.manage`
-   `cluster.read`
-   `cluster.admin`
-   `security.audit`

Authorization must be checked in service methods and data access
boundaries, not only in routes.

## 18.4 Tenant isolation

-   Tenant identity derived from authenticated principal.
-   Every catalog lookup scoped by tenant.
-   Every segment lookup scoped by tenant.
-   Every index lookup scoped by tenant.
-   Query planner injects mandatory tenant filters.
-   Cache keys include tenant.
-   Object paths are tenant-safe.
-   Export and backup permissions are scoped.
-   Cross-tenant administrative operations require explicit privileged
    role.
-   Tests attempt malicious cross-tenant IDs and query manipulation.

## 18.5 Network security

-   TLS for external endpoints.
-   mTLS option for node-to-node communication.
-   Secure cipher configuration.
-   No unauthenticated admin listener by default.
-   Bind local development mode to loopback by default.
-   Explicit firewall and port documentation.
-   Certificate rotation.
-   Secure cluster identity.

## 18.6 Encryption at rest

Define:

-   Encryption scope.
-   Key management interface.
-   Data-key and key-encryption-key separation.
-   Key rotation.
-   Backup encryption.
-   WAL encryption.
-   Segment encryption.
-   Metadata encryption.
-   Recovery when key material is unavailable.

Do not invent cryptography. Use vetted libraries and standard
authenticated encryption constructions. Keep key management pluggable.

## 18.7 Audit logs

Record security-relevant actions:

-   Authentication success/failure.
-   Token creation/revocation.
-   Role changes.
-   Schema changes.
-   Retention changes.
-   Backup and restore.
-   Export.
-   Cluster membership changes.
-   Configuration changes.
-   Administrative query execution.

Audit records should be tamper-evident where practical and must not
include secrets or sensitive payloads by default.

## 18.8 Query security

-   Parameterized query values.
-   Parser limits.
-   Expression depth limits.
-   Regex limits.
-   Memory and CPU budgets.
-   File path isolation.
-   No arbitrary code execution.
-   No unrestricted filesystem access from SQL functions.
-   No unsafe extension loading by default.

## 18.9 Secure defaults

-   Authentication enabled for non-loopback production listeners.
-   No default production credentials.
-   TLS documented and strongly recommended; require it for remote
    cluster links.
-   Strict resource limits.
-   Restricted admin API.
-   Redacted diagnostics.
-   Least-privilege process user.
-   Data directory permissions.
-   Safe file creation flags.
-   Dependency scanning.

------------------------------------------------------------------------------------------------------------------------------------------------

# 19. Backup, restore, and disaster recovery

## 19.1 Backup requirements

Support:

-   Full backup.
-   Incremental backup if designed safely.
-   Metadata and schema backup.
-   Segment backup.
-   Index backup or rebuild metadata.
-   WAL checkpoint boundary.
-   Encryption.
-   Integrity manifest.
-   Backup verification.
-   Retention policy.

## 19.2 Backup consistency

A backup must represent a consistent database point or clearly
documented consistent snapshot boundary.

Do not copy live files arbitrarily while they are being rewritten.

## 19.3 Restore

Restore workflow:

1.  Verify backup manifest.
2.  Verify checksums and signatures where used.
3.  Validate format compatibility.
4.  Restore metadata.
5.  Restore segments.
6.  Restore or rebuild indexes.
7.  Replay WAL if included.
8.  Verify catalog-to-segment references.
9.  Run consistency checks.
10. Start in a safe state.
11. Report restored point and any missing data.

## 19.4 Disaster recovery tests

-   Restore to a clean directory.
-   Restore to a different machine.
-   Restore after simulated disk loss.
-   Restore encrypted backup with correct key.
-   Verify failure with wrong/missing key.
-   Verify old-version backup compatibility.
-   Measure RPO and RTO.
-   Document operator steps.

A backup feature is not complete until restore drills pass.

------------------------------------------------------------------------------------------------------------------------------------------------

# 20. Distributed architecture

Distributed mode is a later milestone. Do not build it before the
single-node durability and storage model is stable.

## 20.1 Distributed roles

-   Coordinator: cluster metadata and query routing.
-   Ingest gateway: receives and routes writes.
-   Storage node: stores WAL/segments and serves scans.
-   Query node: executes distributed plans.
-   Compactor: performs distributed segment maintenance.
-   Metadata quorum: protects cluster state.

## 20.2 Sharding

Candidate shard keys:

-   Tenant.
-   Tenant plus time partition.
-   Hash of tenant and partition.
-   Explicit user-defined partition key.

Avoid a design that creates one shard for every high-cardinality field.

Evaluate:

-   Tenant skew.
-   Hot partitions.
-   Query locality.
-   Rebalancing cost.
-   Write amplification.
-   Replica placement.
-   Small tenant overhead.

## 20.3 Replication

Define:

-   Replication factor.
-   Leader/follower or other protocol.
-   Write acknowledgement quorum.
-   Read consistency modes.
-   Replica lag.
-   Leader election.
-   Membership change.
-   Snapshot transfer.
-   Log truncation.
-   Split-brain prevention.
-   Fencing and epoch rules.

Never claim strong consistency unless the protocol and tests establish
it.

## 20.4 Consensus

If Raft is selected for metadata or shard leadership:

-   Use a reviewed implementation or a carefully isolated
    implementation.
-   Define membership transitions.
-   Define term and log semantics.
-   Define snapshot installation.
-   Test partitions and delayed messages.
-   Test node restarts.
-   Test leader changes during writes.
-   Test membership changes under failure.
-   Avoid reimplementing consensus merely for novelty.

## 20.5 Distributed query execution

-   Query coordinator creates logical plan.
-   Plan is split by partition/shard.
-   Storage nodes perform local pruning and scans.
-   Partial aggregates are returned.
-   Coordinator merges results.
-   Cancellation propagates to all workers.
-   Query budgets apply globally and per node.
-   Partial results must be explicitly marked.
-   Node failures follow documented retry semantics.

## 20.6 Rebalancing

-   Plan movement.
-   Throttle bandwidth and disk I/O.
-   Preserve replica count.
-   Verify checksums.
-   Publish new placement atomically.
-   Retire old copy only after new copy is committed.
-   Resume after coordinator restart.
-   Expose progress and failure reason.

## 20.7 Object storage

Support an abstract object-store interface only after local storage is
stable.

Potential backends:

-   Local filesystem.
-   S3-compatible APIs.
-   Self-hosted object storage systems.

Object storage is not automatically free. StrataDB software may be free
while hardware, disks, bandwidth, and hosted storage incur costs.

Define consistency, multipart upload, retry, checksum, object
versioning, and deletion semantics for every backend.

------------------------------------------------------------------------------------------------------------------------------------------------

# 21. API design

## 21.1 HTTP endpoints

Proposed endpoints; final names and versioning require an API RFC.

``` text
POST   /api/v1/ingest
POST   /api/v1/ingest/batch
POST   /api/v1/query
GET    /api/v1/query/{query_id}
DELETE /api/v1/query/{query_id}

GET    /api/v1/health
GET    /api/v1/ready
GET    /api/v1/version
GET    /api/v1/metrics

GET    /api/v1/databases
POST   /api/v1/databases
GET    /api/v1/databases/{database}
DELETE /api/v1/databases/{database}

GET    /api/v1/tables
POST   /api/v1/tables
GET    /api/v1/tables/{table}
PATCH  /api/v1/tables/{table}

GET    /api/v1/admin/segments
GET    /api/v1/admin/wal
GET    /api/v1/admin/compaction
POST   /api/v1/admin/compaction
POST   /api/v1/admin/backup
POST   /api/v1/admin/restore
```

Administrative endpoints must require privileged permissions and should
be separately configurable.

## 21.2 Query request

``` json
{
  "database": "observability",
  "query": "SELECT service, count(*) FROM logs WHERE severity = 'ERROR' GROUP BY service",
  "timeout_ms": 10000,
  "max_rows": 10000
}
```

The server derives tenant from authentication. Client-provided tenant
identifiers must not override identity.

## 21.3 Query response

Include:

-   Query ID.
-   Schema.
-   Rows or result stream.
-   Execution time.
-   Bytes scanned.
-   Segments scanned/pruned.
-   Indexes used.
-   Warnings.
-   Partial-result flag.
-   Resource usage where safe to expose.

## 21.4 API compatibility

-   Version public endpoints.
-   Maintain OpenAPI specification.
-   Add compatibility tests.
-   Define deprecation windows.
-   Never silently change field meanings.
-   Keep SDKs generated or tested against the canonical schema.

------------------------------------------------------------------------------------------------------------------------------------------------

# 22. CLI and developer experience

## 22.1 CLI commands

``` bash
strata init --data-dir ./data
strata start --config ./strata.toml
strata stop
strata status
strata version
strata health
strata doctor
strata shell
strata query --file query.sql
strata backup create --output ./backup
strata backup verify ./backup
strata backup restore ./backup --data-dir ./restored
strata segments list
strata segments verify
strata indexes list
strata compact --table logs
strata retention inspect
strata config validate
```

Commands must be safe by default. Destructive commands require explicit
confirmation or a non-interactive confirmation flag.

## 22.2 Configuration

Support:

-   TOML or YAML configuration.
-   Environment variable overrides.
-   CLI overrides.
-   Config validation.
-   Config schema version.
-   Secret references rather than plaintext secrets where possible.
-   Redacted config display.
-   Reloadable versus restart-required fields.

## 22.3 Diagnostics

`strata doctor` should inspect:

-   Version and format compatibility.
-   Data directory permissions.
-   Available disk space.
-   WAL health.
-   Segment manifest consistency.
-   Index consistency.
-   File descriptor limits.
-   Memory limits.
-   TLS certificate validity.
-   Cluster connectivity.
-   Clock skew.
-   Backup status.

It must not print secret values.

------------------------------------------------------------------------------------------------------------------------------------------------

# 23. SDKs and integrations

## 23.1 SDKs

Prioritize:

1.  Rust.
2.  Go.
3.  Python.

Each SDK must provide:

-   Authentication.
-   Batch ingestion.
-   Retry with bounded exponential backoff and jitter.
-   Idempotency support.
-   Compression.
-   Timeout configuration.
-   Cancellation.
-   Structured error types.
-   Connection reuse.
-   Metrics hooks.
-   Examples.
-   Compatibility tests.

## 23.2 OpenTelemetry

-   OTLP HTTP and/or gRPC.
-   Logs signal first.
-   Preserve trace and span identifiers.
-   Map resource and scope attributes.
-   Preserve severity number and text.
-   Test against official OTLP fixtures.
-   Document unsupported fields.

## 23.3 Fluent Bit and agents

An integration should be optional. StrataDB should not require a
proprietary shipper.

Provide examples for:

-   Fluent Bit.
-   OpenTelemetry Collector.
-   Vector or other compatible agents only after testing.
-   Direct SDK ingestion.

## 23.4 Grafana integration

Provide a data source or documented integration only when the query API
is stable. Do not claim full Grafana compatibility based on a simple
HTTP endpoint.

------------------------------------------------------------------------------------------------------------------------------------------------

# 24. Management console

The web console is useful but must not delay the database engine.

Initial screens:

-   Cluster overview.
-   Ingestion throughput.
-   Storage usage.
-   Query explorer.
-   Query history.
-   Schema browser.
-   Segment explorer.
-   Index status.
-   Retention policies.
-   Active queries.
-   Tenant and user administration.
-   Backup and restore status.
-   Node health.
-   Audit log.

Security:

-   No secrets in browser storage unless justified.
-   CSRF protection where cookie authentication is used.
-   Secure session cookies.
-   Strict CORS policy.
-   Content Security Policy.
-   Output escaping.
-   Server-side authorization on every operation.
-   No direct filesystem access from browser.

------------------------------------------------------------------------------------------------------------------------------------------------

# 25. Observability of StrataDB itself

## 25.1 Metrics

### Ingestion

-   Requests received.
-   Events accepted/rejected.
-   Bytes accepted.
-   Batch size.
-   Validation failures.
-   WAL append latency.
-   WAL sync latency.
-   Commit queue depth.
-   Backpressure count.
-   Rate-limit events.

### Storage

-   Active memtable bytes.
-   Segment count.
-   Segment bytes.
-   Flush duration.
-   Compaction duration.
-   Compaction backlog.
-   Write amplification.
-   Disk free bytes.
-   WAL bytes.
-   Index bytes.
-   Compression ratio.

### Query

-   Queries started/completed/failed.
-   Query latency histogram.
-   Bytes scanned.
-   Rows scanned.
-   Rows returned.
-   Segments pruned.
-   Index hit rate.
-   Cache hit/miss.
-   Query memory peak.
-   Query cancellation.
-   Resource-limit rejections.

### Distributed mode

-   Node status.
-   Replica lag.
-   Replication queue.
-   Leader changes.
-   Quorum failures.
-   Rebalance progress.
-   Network retry count.
-   Partial query failures.

## 25.2 Internal logs

Use structured logs with:

-   Timestamp.
-   Severity.
-   Component.
-   Operation.
-   Correlation ID.
-   Error category.
-   Safe context.

Never log raw credentials, tokens, private keys, or full sensitive log
payloads by default.

## 25.3 Health endpoints

-   Liveness: process is alive.
-   Readiness: can safely serve intended traffic.
-   Startup: recovery/initialization complete.

Do not make liveness fail merely because an optional dependency is
unavailable; otherwise orchestrators may create restart loops.

------------------------------------------------------------------------------------------------------------------------------------------------

# 26. Testing strategy

Testing is a first-class subsystem.

## 26.1 Unit tests

-   WAL framing.
-   Checksums.
-   Schema conversion.
-   Timestamp handling.
-   Encoding/decoding.
-   Index lookup.
-   Query expressions.
-   Retention calculations.
-   Authorization decisions.
-   Manifest transitions.

## 26.2 Property tests

-   Encode then decode preserves values.
-   Segment round trip preserves records.
-   Index results equal scan results.
-   Optimized query equals reference query.
-   WAL recovery returns exactly committed records under modeled crash
    points.
-   Compaction preserves query results.
-   Schema migration preserves supported semantics.

## 26.3 Fuzzing

Fuzz:

-   WAL parser.
-   Segment reader.
-   Footer parser.
-   Query parser.
-   SQL expression evaluator.
-   JSON decoder.
-   Protocol decoder.
-   Configuration parser.
-   Backup manifest parser.

Untrusted byte inputs must not panic, allocate unbounded memory, or
cause undefined behavior.

## 26.4 Integration tests

-   Ingest and query.
-   Restart and recover.
-   Flush and query.
-   Compact and query.
-   Retention and query.
-   Backup and restore.
-   SDK interoperability.
-   OTLP ingestion.
-   Authentication and authorization.
-   Tenant isolation.
-   Disk-full behavior.

## 26.5 Crash tests

Inject process termination:

-   Before WAL append.
-   During WAL append.
-   After append before sync.
-   After sync before response.
-   After response before memtable update.
-   During flush.
-   After segment write before manifest update.
-   During manifest update.
-   After manifest commit before WAL reclamation.
-   During compaction.
-   During retention deletion.
-   During backup.

The expected result for every crash point must be documented.

## 26.6 Concurrency tests

-   Concurrent writers.
-   Writer and flush races.
-   Writer and query races.
-   Multiple readers.
-   Compaction with active readers.
-   Retention with active readers.
-   Schema changes with ingestion.
-   Shutdown with in-flight operations.

## 26.7 Chaos tests

For distributed mode:

-   Kill nodes.
-   Partition network.
-   Add latency.
-   Drop packets.
-   Reorder packets.
-   Restart leaders.
-   Fill disks.
-   Corrupt a replica.
-   Delay clocks.
-   Rebalance under load.
-   Kill processes during snapshot transfer.

Use deterministic seeds and preserve failure traces.

------------------------------------------------------------------------------------------------------------------------------------------------

# 27. Benchmark laboratory

## 27.1 Benchmark principles

-   Publish exact hardware.
-   Publish OS and kernel.
-   Publish filesystem and mount options.
-   Publish database versions.
-   Publish configurations.
-   Publish dataset generator and seed.
-   Publish query definitions.
-   Publish warm/cold cache procedure.
-   Publish raw result files.
-   Run enough repetitions.
-   Report variance and percentiles.
-   Do not tune only one database.
-   Do not omit failed runs without explanation.

## 27.2 Competitors

Evaluate workload-appropriate systems, including:

-   ClickHouse.
-   Grafana Loki.
-   VictoriaLogs.
-   Elasticsearch or OpenSearch where relevant.
-   PostgreSQL for selected baseline queries.
-   BigQuery only for comparable managed analytical workloads and with
    transparent billing/configuration assumptions.

Do not compare unlike products as if they had identical semantics.

## 27.3 Dataset profiles

Create synthetic and public-data profiles:

-   Low-cardinality application logs.
-   High-cardinality trace/request identifiers.
-   JSON-heavy logs.
-   Large stack traces.
-   Repeated messages.
-   Random UUID fields.
-   Multi-tenant workloads.
-   Out-of-order timestamps.
-   Mixed severity levels.
-   Security audit events.

Never include real customer secrets or personally identifiable
information in benchmark fixtures.

## 27.4 Query suite

1.  Exact trace ID lookup.
2.  Exact request ID lookup.
3.  Recent error filter.
4.  Service and severity filter.
5.  Time range scan.
6.  Message token search.
7.  Structured JSON attribute equality.
8.  High-cardinality aggregation.
9.  Group by service and time bucket.
10. Top error codes.
11. Concurrent ingestion and query.
12. Historical wide-range query.

## 27.5 Performance metrics

-   Events/s.
-   Bytes/s.
-   p50/p95/p99 acknowledgement latency.
-   Query p50/p95/p99.
-   Bytes scanned.
-   CPU time.
-   Peak RSS.
-   Disk read/write.
-   Compression ratio.
-   Index size.
-   Write amplification.
-   Recovery time.
-   Compaction backlog.
-   Cost per stored TB on specified hardware, where relevant.

## 27.6 Benchmark release gate

No public claim such as "X times faster" without:

-   Same hardware.
-   Equivalent durability.
-   Equivalent data.
-   Equivalent query result.
-   Equivalent cache state.
-   Equivalent concurrency.
-   Reproducible scripts.
-   Raw results.
-   Clear version identifiers.

------------------------------------------------------------------------------------------------------------------------------------------------

# 28. Capacity planning

## 28.1 Storage estimate

A first-order estimate:

``` text
compressed_primary_bytes =
    raw_ingest_bytes_per_day
    * retention_days
    / measured_compression_ratio

replicated_bytes =
    compressed_primary_bytes
    * replication_factor

provisioned_bytes =
    replicated_bytes
    * operational_headroom_factor
    + wal_reserve
    + compaction_reserve
    + index_reserve
    + backup_reserve
```

This formula is only a planning approximation. Measure actual index,
WAL, metadata, compaction, and backup overhead.

## 28.2 Example

Assumptions:

-   Raw ingestion: 100 GB/day.
-   Retention: 30 days.
-   Measured compression ratio assumption: 5:1.
-   Replication factor: 3.
-   Headroom factor: 1.3.

Primary compressed data estimate:

`100 GB * 30 / 5 = 600 GB`

Replicated data:

`600 GB * 3 = 1,800 GB`

With 30% headroom:

`1,800 GB * 1.3 = 2,340 GB`

This excludes separately measured WAL, indexes, backup, and temporary
compaction overhead. The assumed 5:1 ratio is not guaranteed.

## 28.3 Hardware tiers

Provide sizing guides for:

-   Developer laptop.
-   Small single-node deployment.
-   Medium single-node deployment.
-   Three-node test cluster.
-   Small production cluster.
-   Larger production cluster.

Every sizing guide must state the workload and measured benchmark used.

## 28.4 Disk safety

-   Alert at configurable thresholds.
-   Stop or throttle ingestion before unrecoverable disk exhaustion.
-   Reserve space for WAL and recovery.
-   Reserve compaction temporary space.
-   Provide emergency read-only mode where possible.
-   Never delete data silently to recover disk space.

------------------------------------------------------------------------

# 29. Deployment and operations

## 29.1 Supported deployment methods

-   Standalone binary.
-   Docker.
-   Docker Compose.
-   systemd on supported Linux distributions.
-   Kubernetes later, after operational behavior is stable.

## 29.2 Container requirements

-   Non-root user.
-   Read-only root filesystem where practical.
-   Explicit data volume.
-   Resource limits.
-   Health checks.
-   Graceful shutdown.
-   No secret baked into image.
-   Minimal base image.
-   SBOM and image scan.
-   Pinned image digests for production examples.

## 29.3 Production topology

A small HA topology may eventually include:

-   Multiple ingestion gateways.
-   Multiple storage nodes.
-   Multiple query nodes.
-   Metadata quorum across failure domains.
-   Replicated storage.
-   Separate backup target.
-   Monitoring outside the database failure domain.

Exact node counts depend on the consensus design and failure-domain
model. Three processes on one host do not provide host-level HA.

## 29.4 Configuration management

-   Validate before start.
-   Print effective non-secret config.
-   Document defaults.
-   Document restart requirements.
-   Support config migration.
-   Provide sample configs.
-   Reject unknown critical settings rather than silently ignoring them.

## 29.5 Graceful shutdown

-   Stop accepting new requests.
-   Drain in-flight requests.
-   Commit or reject pending writes according to contract.
-   Flush or preserve recoverable state.
-   Stop background workers safely.
-   Persist shutdown metadata if needed.
-   Exit within configured timeout.
-   Leave recovery state valid if forced termination occurs.

------------------------------------------------------------------------

# 30. Upgrade and compatibility

## 30.1 Versioning

Version separately:

-   Server.
-   API.
-   WAL format.
-   Segment format.
-   Index format.
-   Catalog schema.
-   Backup format.
-   SDKs.

## 30.2 Upgrade rules

-   Support a documented upgrade path.
-   Check disk space before migration.
-   Backup before irreversible migration.
-   Provide dry-run.
-   Verify compatibility.
-   Support rolling upgrade only when protocol compatibility is proven.
-   Never allow mixed versions to write incompatible formats.
-   Provide rollback strategy or clearly state when rollback is
    impossible.

## 30.3 Data migration

Provide tools for:

-   Import from JSON/NDJSON.
-   Import from Parquet.
-   Export to Parquet.
-   Export to CSV/JSON.
-   Bulk load.
-   Schema mapping.
-   Validation report.
-   Checkpoint and resume.

Connectors to other databases can be added later. Do not make them a
dependency for core operation.

------------------------------------------------------------------------

# 31. Open-source governance and licensing

## 31.1 License

Choose an OSI-approved license after reviewing:

-   Desired adoption model.
-   Dependency licenses.
-   Patent clauses.
-   Contributor policy.
-   Trademark policy.
-   SDK licensing.
-   Documentation licensing.

Do not assume all dependencies use compatible licenses.

## 31.2 Community files

-   README.
-   CONTRIBUTING.
-   CODE_OF_CONDUCT.
-   SECURITY.
-   GOVERNANCE.
-   CHANGELOG.
-   Roadmap.
-   Support policy.
-   RFC process.
-   Release process.

## 31.3 Contribution requirements

-   Tests for behavior changes.
-   Benchmarks for performance-sensitive changes.
-   Documentation for public API changes.
-   No secrets in commits.
-   Signed-off or DCO policy if selected.
-   Review for unsafe Rust.
-   Dependency changes require justification.
-   Format compatibility changes require RFC.

## 31.4 Security reporting

Publish a private vulnerability reporting channel. Define response
targets, supported versions, disclosure policy, and patch release
process.

------------------------------------------------------------------------

# 32. CI/CD and supply-chain security

## 32.1 CI jobs

-   Format check.
-   Clippy.
-   Unit tests.
-   Integration tests.
-   Feature matrix.
-   Documentation build.
-   API schema validation.
-   Dependency audit.
-   License scan.
-   Secret scan.
-   Fuzz smoke tests.
-   Benchmark smoke tests.
-   Container build.
-   SBOM generation.

## 32.2 Release pipeline

1.  Tag release.
2.  Verify clean source and lockfile.
3.  Run full tests.
4.  Build supported targets.
5.  Generate checksums.
6.  Generate SBOM.
7.  Sign artifacts where feasible.
8.  Build and scan container images.
9.  Publish release notes.
10. Publish migration notes.
11. Verify download artifacts.
12. Preserve benchmark report.

## 32.3 Free infrastructure constraint

Development should not depend on paid CI or hosted services. Use local
tests as the complete source of truth and free CI allowances as an
additional automation layer. CI quotas can change and must be checked.

------------------------------------------------------------------------

# 33. Development roadmap and work breakdown

The schedule is a planning sequence, not a promise that a small team can
complete all work in a fixed number of weeks.

## Phase 0 --- Research and decisions

Deliverables:

-   Competitive workload study.
-   Benchmark dataset generator.
-   ADRs for WAL, segment, index, and query architecture.
-   Threat model.
-   Format version proposal.
-   Repository setup.

Exit gate:

-   Architecture reviewed.
-   Workload contract defined.
-   No unresolved critical durability decision.

## Phase 1 --- Durable local storage

Deliverables:

-   Rust workspace.
-   Canonical event model.
-   WAL.
-   Recovery.
-   Memtable.
-   Flush.
-   Segment writer.
-   Manifest.
-   Segment reader.
-   Basic integrity verification.

Exit gate:

-   Crash tests pass.
-   Acknowledged records recover.
-   Segment round-trip tests pass.
-   Corruption is detected.

## Phase 2 --- Useful query engine

Deliverables:

-   Parser.
-   Logical plan.
-   Scan.
-   Filter.
-   Projection.
-   Limit.
-   Aggregation.
-   Time filtering.
-   EXPLAIN.
-   Query budgets.

Exit gate:

-   Query results match reference implementation.
-   Cancellation works.
-   Resource limits are enforced.

## Phase 3 --- Indexes and optimization

Deliverables:

-   Segment statistics.
-   Timestamp pruning.
-   Bloom filters.
-   Exact-match index.
-   Selective inverted index.
-   Column pruning.
-   Predicate pushdown.
-   Benchmark suite.

Exit gate:

-   Index results equal scan results.
-   Measured benefit on target workloads.
-   Index overhead documented.

## Phase 4 --- Ingestion and interoperability

Deliverables:

-   HTTP batch API.
-   OTLP logs.
-   Rust SDK.
-   Go SDK.
-   Python SDK.
-   Retry and idempotency semantics.
-   Backpressure.
-   Authentication.

Exit gate:

-   SDK interoperability.
-   Load tests.
-   No unbounded queues.
-   Durability semantics verified.

## Phase 5 --- Operations and security

Deliverables:

-   RBAC.
-   Tenant isolation.
-   TLS.
-   Audit events.
-   Backup and restore.
-   Retention.
-   CLI. 
-   Metrics.
-   Docker.
-   Documentation.

Exit gate:

-   Security tests.
-   Restore drills.
-   Disk-full tests.
-   Operational runbooks.

## Phase 6 --- Single-node production candidate

Deliverables:

-   Performance tuning.
-   Compatibility policy.
-   Upgrade tool.
-   Fuzzing.
-   Chaos/crash tests.
-   Release automation.
-   Production documentation.

Exit gate:

-   All single-node acceptance criteria pass.

## Phase 7 --- Distributed design

Deliverables:

-   Failure model.
-   Metadata consensus decision.
-   Shard map.
-   Replication protocol.
-   Node identity.
-   Placement.
-   Query routing.
-   Distributed recovery RFC.

Exit gate:

-   Protocol model reviewed.
-   Deterministic simulation tests exist.

## Phase 8 --- Distributed implementation

Deliverables:

-   Cluster membership.
-   Replication.
-   Leader/quorum semantics.
-   Query coordination.
-   Rebalancing.
-   Node drain.
-   Distributed compaction.
-   Cluster backup and restore.

Exit gate:

-   Fault-injection and partition tests pass.
-   No acknowledged-write loss within stated failure model.
-   Recovery and rebalancing are observable.

## Phase 9 --- Stable release

Deliverables:

-   Security review.
-   Performance report.
-   Compatibility matrix.
-   Installation guides.
-   Upgrade guides.
-   Disaster recovery guide.
-   Release artifacts.
-   Known limitations.
-   Support policy.

Exit gate:

-   Production readiness review signed off.

------------------------------------------------------------------------

# 34. Production acceptance checklist

A release cannot be called production-ready merely because the happy
path works.

## 34.1 Data correctness

-   [ ] WAL framing and checksums validated.
-   [ ] Crash recovery tested at every transition.
-   [ ] Segment round trips pass.
-   [ ] Index results match scans.
-   [ ] Compaction preserves results.
-   [ ] Query optimizer equivalence tests pass.
-   [ ] Concurrent write/read semantics documented.
-   [ ] Schema evolution tested.
-   [ ] Timestamp edge cases tested.
-   [ ] Corrupt data detected and reported.

## 34.2 Durability

-   [ ] Acknowledgement semantics documented.
-   [ ] Sync behavior tested.
-   [ ] Group commit tested.
-   [ ] WAL reclamation proven safe.
-   [ ] Disk-full behavior tested.
-   [ ] Process crash recovery tested.
-   [ ] Machine-loss behavior documented.
-   [ ] Replica failure behavior tested in cluster mode.

## 34.3 Security

-   [ ] No production default credentials.
-   [ ] Tenant isolation tests pass.
-   [ ] RBAC tests pass.
-   [ ] TLS configuration tested.
-   [ ] Secrets redacted.
-   [ ] Query resource limits enforced.
-   [ ] Parser fuzzing runs.
-   [ ] Dependency audit runs.
-   [ ] Container runs as non-root.
-   [ ] Security reporting process exists.

## 34.4 Operations

-   [ ] Health/readiness endpoints work.
-   [ ] Metrics are documented.
-   [ ] Disk usage alerts exist.
-   [ ] Backup creation works.
-   [ ] Restore drills pass.
-   [ ] Upgrade path tested.
-   [ ] Rollback limitations documented.
-   [ ] Diagnostic tooling works.
-   [ ] Runbooks exist.

## 34.5 Performance

-   [ ] Ingestion benchmark published.
-   [ ] Query benchmark published.
-   [ ] High-cardinality benchmark published.
-   [ ] Compression benchmark published.
-   [ ] Concurrent workload benchmark published.
-   [ ] Resource usage documented.
-   [ ] Competitor comparison is fair and reproducible.
-   [ ] Regressions have thresholds and review process.

## 34.6 Release

-   [ ] License review complete.
-   [ ] SBOM generated.
-   [ ] Checksums published.
-   [ ] Release artifacts verified.
-   [ ] Documentation complete.
-   [ ] Known limitations published.
-   [ ] Version compatibility documented.

------------------------------------------------------------------------

# 35. Initial implementation tickets

Create issues in dependency order. Do not ask an AI coding agent to
implement all modules in one pass.

## Epic A: Repository foundation

-   A-001 Create workspace and toolchain.
-   A-002 Establish error model.
-   A-003 Establish logging and tracing.
-   A-004 Add CI checks.
-   A-005 Add architecture decision record template.
-   A-006 Add test fixtures and testkit.

## Epic B: Canonical data model

-   B-001 Define event schema.
-   B-002 Define typed attributes.
-   B-003 Define timestamp representation.
-   B-004 Define schema version.
-   B-005 Add validation.
-   B-006 Add serialization compatibility tests.

## Epic C: WAL

-   C-001 Define WAL format.
-   C-002 Implement append.
-   C-003 Implement checksum.
-   C-004 Implement segment rotation.
-   C-005 Implement sync policies.
-   C-006 Implement recovery.
-   C-007 Implement safe reclamation.
-   C-008 Add crash injection tests.

## Epic D: Segment storage

-   D-001 Define segment format RFC.
-   D-002 Implement segment writer.
-   D-003 Implement segment reader.
-   D-004 Implement metadata footer.
-   D-005 Implement checksums.
-   D-006 Implement manifest.
-   D-007 Implement flush transaction.
-   D-008 Add corruption tests.

## Epic E: Query

-   E-001 Define query grammar.
-   E-002 Implement parser.
-   E-003 Implement logical plan.
-   E-004 Implement scan.
-   E-005 Implement filter.
-   E-006 Implement projection.
-   E-007 Implement limit and sort.
-   E-008 Implement aggregation.
-   E-009 Implement EXPLAIN.
-   E-010 Implement resource budgets.

## Epic F: Indexes

-   F-001 Segment statistics.
-   F-002 Timestamp pruning.
-   F-003 Bloom filter prototype.
-   F-004 Exact-match index prototype.
-   F-005 Inverted index prototype.
-   F-006 Index manifest.
-   F-007 Index verification.
-   F-008 Index rebuild.

## Epic G: Ingestion

-   G-001 HTTP server.
-   G-002 Authentication middleware.
-   G-003 Batch endpoint.
-   G-004 WAL integration.
-   G-005 Backpressure.
-   G-006 Retry semantics.
-   G-007 OTLP mapping.
-   G-008 SDK conformance tests.

## Epic H: Production hardening

-   H-001 RBAC.
-   H-002 Tenant isolation.
-   H-003 TLS.
-   H-004 Audit events.
-   H-005 Retention.
-   H-006 Backup.
-   H-007 Restore.
-   H-008 Metrics.
-   H-009 CLI.
-   H-010 Docker deployment.
-   H-011 Fuzzing.
-   H-012 Security review.

## Epic I: Distributed mode

-   I-001 Failure model RFC.
-   I-002 Node identity.
-   I-003 Metadata consensus.
-   I-004 Shard map.
-   I-005 Replication protocol.
-   I-006 Snapshot transfer.
-   I-007 Query routing.
-   I-008 Rebalancing.
-   I-009 Distributed recovery.
-   I-010 Chaos test suite.

------------------------------------------------------------------------

# 36. Coding-agent execution protocol

This section defines how an AI coding agent or human contributor should
work on StrataDB.

## 36.1 Never implement the whole product in one prompt

Work one issue or one tightly coupled set of issues at a time.

For each task, the agent must:

1.  Read this specification.
2.  Read relevant ADRs.
3.  Inspect existing code.
4.  Identify invariants.
5.  Propose a minimal implementation plan.
6.  Implement.
7.  Add tests.
8.  Run formatting and lint.
9.  Run targeted and workspace tests.
10. Update docs.
11. Report changed files and test output.
12. Identify unresolved risks.

## 36.2 Required implementation report

Every completed task must report:

-   Requirement addressed.
-   Design chosen.
-   Files changed.
-   Public API changes.
-   Storage-format changes.
-   Tests added.
-   Commands run.
-   Results.
-   Performance impact if relevant.
-   Security impact.
-   Known limitations.
-   Follow-up issues.

## 36.3 Prohibited shortcuts

-   Do not use in-memory-only writes while claiming durability.
-   Do not acknowledge before the configured commit boundary.
-   Do not silently drop malformed events.
-   Do not ignore tenant scope in storage lookups.
-   Do not use unbounded channels.
-   Do not add arbitrary `unsafe`.
-   Do not catch and ignore storage errors.
-   Do not silently fall back from index to scan without reporting plan
    behavior.
-   Do not claim exactly-once delivery without end-to-end semantics.
-   Do not claim HA using multiple containers on one host.
-   Do not add distributed mode before single-node recovery is proven.
-   Do not publish benchmark claims from incomparable environments.
-   Do not generate fake performance numbers.
-   Do not introduce paid service dependencies.
-   Do not commit secrets or production data.
-   Do not make a persistent format change without versioning and
    migration tests.

## 36.4 Definition of done

A ticket is complete only when:

-   Acceptance criteria pass.
-   Tests cover normal and failure behavior.
-   Documentation is updated.
-   Error behavior is explicit.
-   Security implications are reviewed.
-   Performance-sensitive behavior is measured where appropriate.
-   No known data-loss or isolation defect remains.

------------------------------------------------------------------------

# 37. Risk register

  -----------------------------------------------------------------------
  Risk                    Impact                  Mitigation
  ----------------------- ----------------------- -----------------------
  Custom format           High                    Prototype against
  complexity                                      Parquet; version format

  WAL correctness defects Critical                Formal invariants,
                                                  crash injection

  High-cardinality index  High                    Selective indexing,
  growth                                          quotas, measurement

  Compaction write        High                    Budgeted compaction,
  amplification                                   benchmarks

  Query memory exhaustion Critical                Query budgets,
                                                  cancellation

  Tenant data leakage     Critical                Defense-in-depth
                                                  isolation tests

  Distributed consensus   Critical                Delay distributed mode;
  bugs                                            use simulation

  Backup not restorable   Critical                Regular restore drills

  Dependency              High                    Audits, pinned
  vulnerabilities                                 versions, SBOM

  Scope expansion         High                    Release gates and
                                                  non-goals

  Misleading performance  High                    Reproducible benchmark
  claims                                          policy

  Single developer        High                    Incremental milestones
  bandwidth                                       and limited scope

  Hardware limitations    Medium                  Local simulation and
                                                  workload-specific tests

  Cloud cost creep        Medium                  Self-host-first policy
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 38. Zero-cost development and operating model

## 38.1 Development

The initial developer machine can be the existing Apple Silicon Mac with
24 GB RAM. This is suitable for implementation, unit tests, local
single-node tests, and modest containerized cluster simulations. It is
not a substitute for production load testing on independent machines.

Use:

-   Rust toolchain.
-   Docker Engine/Desktop or compatible local container runtime.
-   Git.
-   GitHub public repository.
-   Local SSD.
-   Synthetic data generators.
-   Criterion.
-   k6 or another open-source load generator.
-   Prometheus and Grafana locally.
-   OpenTelemetry libraries.
-   Local backup targets.

## 38.2 No paid dependency rule

Every dependency proposal must answer:

-   Is it open source?
-   What is its license?
-   Does it require a paid hosted service?
-   Can it run locally?
-   Can the product operate if the service disappears?
-   Does it transmit data externally?
-   Is there a self-hosted alternative?

## 38.3 Production costs

The StrataDB software can be free and open source. Production
infrastructure is not free by definition. Customers must provide
compute, memory, storage, network, backups, and operational support.

Do not promise unlimited hosted storage or a free managed service
without a funded capacity plan.

------------------------------------------------------------------------

# 39. Product documentation required before v1.0

-   Quick start.
-   Installation.
-   Configuration reference.
-   Data model.
-   Ingestion API.
-   OTLP compatibility.
-   SDK guides.
-   Query language reference.
-   Indexing guide.
-   Storage architecture.
-   Durability guarantees.
-   Backup and restore.
-   Security model.
-   RBAC guide.
-   Multi-tenancy guide.
-   Capacity planning.
-   Benchmark methodology.
-   Production deployment.
-   Monitoring.
-   Troubleshooting.
-   Upgrade and migration.
-   Compatibility matrix.
-   Known limitations.
-   Disaster recovery runbook.

------------------------------------------------------------------------

# 40. Final engineering strategy

The most important strategic decision is to avoid trying to beat every
database at every workload.

StrataDB should become exceptional at:

1.  Durable structured-log ingestion.
2.  High-cardinality identifier lookup.
3.  Time-bounded incident investigation.
4.  Efficient storage and retention.
5.  Concurrent ingestion and analytical queries.
6.  Self-hosted operation.
7.  Predictable resource governance.

The project should proceed in this order:

**Correct single-node storage → useful query engine → measured indexes →
production security and operations → distributed replication →
horizontal scale.**

A distributed database built on an incorrect storage engine only
distributes the correctness problems.

The first milestone is therefore not a cluster, dashboard, or benchmark
headline. It is a single-node database that can acknowledge writes
according to a clear durability contract, survive crashes, recover its
data, execute correct queries, and prove its behavior through tests.

------------------------------------------------------------------------

# Appendix A. Official technical references

These are starting points for implementation research. Always verify
current versions, licenses, and compatibility before selecting
dependencies.

## Storage and query

-   Rust: https://www.rust-lang.org/
-   Tokio: https://tokio.rs/
-   Apache Arrow: https://arrow.apache.org/
-   Apache Parquet: https://parquet.apache.org/
-   Apache DataFusion: https://datafusion.apache.org/
-   SQLite: https://www.sqlite.org/docs.html
-   Zstandard: https://facebook.github.io/zstd/
-   LZ4: https://lz4.github.io/lz4/

## Observability and ingestion

-   OpenTelemetry: https://opentelemetry.io/docs/
-   OTLP specification: https://opentelemetry.io/docs/specs/otlp/
-   Prometheus: https://prometheus.io/docs/
-   Grafana: https://grafana.com/docs/

## Comparison systems

-   ClickHouse: https://clickhouse.com/docs
-   Grafana Loki: https://grafana.com/docs/loki/latest/
-   VictoriaLogs: https://docs.victoriametrics.com/victorialogs/
-   Google BigQuery: https://cloud.google.com/bigquery/docs
-   Elasticsearch: https://www.elastic.co/guide/index.html

## Security and software supply chain

-   OWASP: https://owasp.org/
-   RustSec: https://rustsec.org/
-   OpenSSF: https://openssf.org/
-   Sigstore: https://www.sigstore.dev/
-   CycloneDX: https://cyclonedx.org/

------------------------------------------------------------------------

# Appendix B. Mandatory RFC templates

Every significant architecture change must use this template.

``` markdown
# RFC-NNN: Title

## Status
Proposed | Accepted | Rejected | Superseded

## Authors

## Context
What problem are we solving?

## Requirements
Functional and non-functional requirements.

## Invariants
What must always remain true?

## Options
List alternatives and trade-offs.

## Decision
Selected approach.

## Failure behavior
What happens on process, disk, network, and node failures?

## Security implications

## Performance implications
Include measurements or a plan to measure.

## Compatibility
Storage, API, and upgrade implications.

## Testing plan

## Rollout plan

## Open questions
```

------------------------------------------------------------------------

# Appendix C. Mandatory benchmark report template

``` markdown
# StrataDB Benchmark Report

## Summary
Do not use unsupported superlatives.

## Hardware
CPU:
RAM:
Storage:
Filesystem:
OS:
Kernel:
Network:

## Software versions
StrataDB:
Competitor:
Compiler:
Dependencies:

## Dataset
Generator:
Seed:
Raw bytes:
Record count:
Average record size:
Cardinality:
Schema:
Timestamp distribution:

## Durability
Acknowledgement mode:
Replication:
Sync policy:

## Query workload
Query definitions:
Concurrency:
Cache state:

## Results
Ingestion:
Write latency:
Query latency:
Bytes scanned:
Compression:
CPU:
Memory:
Disk I/O:

## Repetitions and variance

## Raw data
Attach machine-readable result files.

## Limitations
List differences and caveats.

## Reproduction commands
Provide exact commands.
```

------------------------------------------------------------------------

# Appendix D. Release declaration template

``` markdown
# StrataDB vX.Y.Z Release

## Supported deployment modes

## Supported platforms

## Storage format versions

## API compatibility

## Durability guarantees

## Security changes

## Performance results
Link reproducible benchmark reports.

## Upgrade instructions

## Backup requirement

## Known limitations

## Breaking changes

## Checksums and signatures

## SBOM

## Security reporting instructions
```

------------------------------------------------------------------------

# Appendix E. Final non-negotiable rules

1.  Never trade correctness for an unmeasured speed claim.
2.  Never acknowledge a write before the configured durability boundary.
3.  Never expose data across tenant boundaries.
4.  Never silently ignore corruption.
5.  Never delete source data before replacement data is committed.
6.  Never claim backup support without restore testing.
7.  Never claim distributed high availability without independent
    failure-domain tests.
8.  Never claim a benchmark win without reproducible evidence.
9.  Never make a paid cloud service mandatory.
10. Never make a custom format change without versioning.
11. Never allow an unbounded queue or unbounded query.
12. Never ship a production default credential.
13. Never silently change query semantics.
14. Never let a dashboard become a substitute for database correctness.
15. Never call the product production-ready until its release gates
    pass.

**End of master specification.**
