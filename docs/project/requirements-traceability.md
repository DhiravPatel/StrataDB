# Requirements traceability matrix

Maps spec requirements → tasks → acceptance tests. Status: ✅ tested ·
◐ partial · ☐ not started · ⏸ deferred by spec. Test paths are relative to `crates/`.

| Spec | Requirement | Task | Acceptance test / evidence | Status |
|------|-------------|------|----------------------------|--------|
| §0.3.3, §8.3 | No acknowledged write lost under configured mode | C-005, C-008 | `stratadb-storage/tests/crash.rs`, `power_loss_sim.rs` (2 428 simulated power-loss states), WAL `concurrent_*` | ✅ process crash and simulated power loss (device honours flushes) |
| §0.3.4, §18.4 | No cross-tenant query results | H-002 | `tenant_isolation_in_storage`, `tenant_isolation_in_queries` | ◐ storage/query ✅, API ☐ |
| §0.3.6 | Persistent formats versioned | B-004, C-001, D-001 | version-rejection tests in wal/segment/schema/manifest | ✅ |
| §0.3.7 | Inspectable/verifiable storage | D-005 | `strata segments verify`, `SegmentReader::verify` | ✅ |
| §2.1 | Single + batch ingestion; partial batch errors | G-003 | CLI `end_to_end` (per-line rejections) | ◐ CLI only |
| §2.1 | HTTP ingestion endpoint, partial batch errors, auth + tenant routing | G-001..G-003 | `apps/stratadb-server/tests/http.rs` | ✅ |
| §2.1, §7.6, §23.2 | OTLP logs (HTTP protobuf + JSON, gzip), official-fixture & official-exporter interop | G-007 | `otlp_proto` tests, `otlp_http_json_logs`, `otlp_interop` | ◐ (no gRPC) |
| §2.1, §23.1 | Rust, Python, Go SDKs | G-008 | `sdk/README.md` conformance runs | ✅ |
| §22.2, §29.4 | Config file, env overrides, validation, redacted display | — | `config.rs` tests | ✅ |
| §29.1–29.2 | Docker, compose, systemd, non-root, healthcheck | H-010 | `deployments/` | ◐ (image build unverified) |
| §2.1 | Ingestion states distinguished | ADR-0009 | `WriteReceipt.level`, `async_mode_reports_weaker_commit_level` | ◐ (replicated/flushed states not exposed per write) |
| §2.1, §7.3 | Idempotency key support | G-006 | — | ☐ |
| §2.2, §6.1 | Canonical event model; tenant from auth | B-001 | `maps_spec_example`, `payload_cannot_override_tenant` | ✅ |
| §2.2, §6.4 | No silent coercion; type-conflict policy | B-002, ADR-0008 | schema json tests; planner `validation_errors` | ✅ |
| §2.3 | Projection, time filters, =/!=, numeric cmp, AND/OR/NOT, IN, IS NULL/MISSING, LIKE, ORDER BY, LIMIT, GROUP BY, COUNT/MIN/MAX/SUM/AVG, time bucket, map access, timeout/cancel, EXPLAIN [ANALYZE] | E-001..E-010 | `stratadb-query/tests/query.rs` | ✅ |
| §2.3 | Pagination with stable semantics | E-011 | `stratadb-query/tests/pagination.rs` (incl. proptest), server `http.rs::query_pagination_over_http`, Rust/Python/Go SDK conformance (ADR-0023) | ✅ |
| §2.4 | Administration (segments, verify, WAL state) | H-009 | `strata segments`, `strata status` | ◐ |
| §2.5, §25 | Prometheus, OTel traces, health endpoints | H-008 | `/metrics`, `/health`, `/ready` | ◐ (no traces, no histograms) |
| §3.1 | No silent corruption; deterministic recovery; checksums | C-003, C-006, D-005 | bit-flip/truncation tests, `crash.rs` idempotency | ✅ |
| §3.1 | Indexes rebuildable | F-008 | — | ☐ |
| §3.4, §13.6 | Query time/memory/rows/concurrency limits | E-010 | `budgets_and_cancellation_are_enforced` | ◐ (no concurrency limit / per-tenant quotas) |
| §3.4, §7.4 | Bounded queues, backpressure, disk reserve, per-tenant ingest + storage quotas | G-005 | backpressure, disk-reserve and quota tests | ✅ |
| §25.2 | Structured logs with correlation id, no secrets | A-003 | `request_ids_are_propagated_or_generated_safely` | ◐ (no OTel traces) |
| §18.2 | Token rotation/revocation without downtime | — | `tokens_hot_reload_without_restart` | ✅ |
| §5.3 | fmt, clippy -D warnings, tests, no unsafe | A-001, A-004 | `scripts/check.sh`, `unsafe_code = "forbid"` | ✅ |
| §6.3 | Missing vs null vs empty | B-002 | `missing_null_empty_distinct`, `null_missing_and_empty_are_distinct` | ✅ |
| §6.5 | Validation limits (size, fields, depth, keys, strings, timestamps, UTF-8, numbers) | B-005 | `validate.rs` tests | ✅ |
| §7.5 | Event + ingest time; skew limits | B-003 | `rejects_bad_fields` | ◐ partition policy ADR-0004 |
| §8.1–8.2 | WAL framing, checksum, rotation, torn-write detection, versioning | C-001..C-004 | wal tests | ✅ |
| §8.4 | Group commit bounded wait | C-005 | `concurrent_group_commit_appends` | ✅ |
| §8.5 | Recovery steps 1–10 | C-006 | wal recovery tests, engine open | ✅ |
| §8.6 | Safe WAL reclamation | C-007 | `rotation_and_reclamation`, crash `during_wal_reclamation` | ✅ |
| §9 | Memtable bounded, snapshot consistency, flush triggers | D-007 | `concurrent_writes_flushes_and_snapshots`, `background_flush_by_row_threshold` | ✅ (shutdown/age/admin triggers ✅; WAL-pressure trigger ☐) |
| §10.2 | Custom format vs Parquet benchmark | ADR-0001 | — | ☐ (provisional decision) |
| §10.4 | Segment lifecycle states durable | D-006 | manifest + orphan/quarantine tests | ◐ (compacting/obsolete states pending compaction) |
| §10.5 | Manifest atomic, records required fields | D-006 | manifest tests | ✅ |
| §10.6 | Detect/quarantine corruption; partial-result marker | D-008 | `corrupt_segment_detected_*`, quarantine test | ◐ (no partial results; queries fail) |
| §11 | Encodings + LZ4 chunk compression with uncompressed fallback | RFC-0001 | segment v2 tests, compression A/B report | ◐ (no zstd / policy tiers) |
| §12.5 | Bloom filters, EXPLAIN shows index use | F-003 | bloom tests + A/B report | ✅ |
| §12.3–12.4 | Exact-match postings, inverted indexes | F-004, F-005 | — | ☐ |
| §13.4 | Predicate/projection/partition pruning; optimizer equivalence | F-002 | `optimized_equals_unoptimized_equals_reference` | ✅ |
| §14 | Search modes, tokenization, regex safety | — | LIKE/ILIKE only | ◐ |
| §15 | Catalog | ADR-0007 | manifest | ◐ |
| §16 | Compaction, retention (+hold), obsolete lifecycle | H-005 | `tests/maintenance.rs`, crash matrix | ◐ (no tombstones/row deletes) |
| §17 | Caching | — | OS page cache only | ☐ |
| §18.2–18.4 | API tokens, RBAC, tenant from principal | H-001, H-002 | auth unit tests, `authentication_and_authorization_are_enforced`, `tenant_isolation_across_the_api` | ◐ (no rotation without restart, no OIDC/mTLS) |
| §18.5 | TLS | H-003 | `tests/tls.rs` | ◐ (no mTLS for cluster links yet) |
| §18.6 | Encryption at rest | — | `stratadb-crypto` tests, `stratadb-storage/tests/encryption.rs`, encrypted power-loss simulation, CLI `tests/encryption.rs`, fixture `encrypted-v1` (ADR-0024) | ✅ (file KEK provider; no KMS) |
| §18.7 | Audit log | H-004 | audit tests | ✅ |
| §28.4 | Disk safety (reserve, refuse before exhaustion) | — | `disk_reserve_pauses_writes_but_not_reads_or_flushes` | ◐ (no read-only emergency mode switch) |
| §19 | Backup/verify/restore + drills | H-006, H-007 | `tests/backup.rs`, CLI drill, runbook | ◐ (no incremental, no old-version backup yet; encrypted backups ✅) |
| §20 | Distributed | I-* | — | ⏸ |
| §21 | HTTP API incl. query status/cancel, OpenAPI | G-001 | `docs/api/openapi.json`, http tests | ✅ |
| §22 | CLI incl. `doctor` (§22.3) | H-009 | `apps/stratadb-cli/tests/{cli,doctor}.rs` | ◐ (no `shell`, no fd/TLS-expiry checks in doctor) |
| §26.2 | Property tests (codec, segment, WAL, query) | — | proptest in common/schema/wal/segment/query | ✅ |
| §26.3 | Fuzzing (WAL, segment, footer, manifest, backup, query+eval, JSON, OTLP, config, tokens, audit) | H-011 | `crates/stratadb-fuzz`, `fuzz/` | ✅ |
| §26.5 | Crash points incl. compaction/retention/backup | C-008 | `docs/testing/crash-matrix.md` | ◐ |
| §27 | Benchmark laboratory (generator, query suite, raw JSON) | A-006, BENCH-1 | `stratadb-testkit` tests, `bench_smoke`, baseline report | ◐ (no competitors, no concurrent workload, single run) |
| §30.2 | Upgrade tool: dry run, backup before irreversible migration, rollback statement | — | `upgrade.rs`, docs/runbooks/upgrade.md | ✅ |
| §31 | License, community files | ADR-0016 | files present; license pending owner | ◐ |
| §32 | CI/CD, audit, SBOM | A-004 | workflow file | ◐ |
