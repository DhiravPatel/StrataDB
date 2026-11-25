# Implementation status

States: NOT_STARTED · DESIGN · IN_PROGRESS · IMPLEMENTED · TESTED · VERIFIED · BLOCKED.
"TESTED" = automated tests cover normal and failure behaviour. "VERIFIED" is
reserved for features that additionally passed an independent review / release
gate; **nothing is VERIFIED yet.**

Last full validation: 2026-11-25 — `scripts/check.sh`: fmt ✅, clippy
`-D warnings` ✅, `cargo test --workspace` ✅ (222 tests, 0 failures).

| ID | Feature | State | Evidence | Known limitations |
|----|---------|-------|----------|-------------------|
| A-001 | Workspace, toolchain pin, lints | TESTED | `Cargo.toml`, `rust-toolchain.toml` | — |
| A-002 | Error model (kinds, retryability) | TESTED | `common/src/error.rs` tests | — |
| A-003 | Structured logging + request correlation | TESTED | JSON subscriber in server, `x-request-id` + access log, `request_ids_are_propagated_or_generated_safely` | no OTel trace export |
| — | `strata upgrade` (dry run, verified backup gate, crash-safe rewrite) | TESTED | `apps/stratadb-cli/tests/upgrade.rs`, runbook | |
| — | Token hot reload | TESTED | `tokens_hot_reload_without_restart` | ≤ 2 s propagation |
| — | Per-tenant ingest rate + storage quotas | TESTED | `per_tenant_rate_and_storage_quotas`, config tests | in-memory buckets (reset on restart) |
| A-004 | CI checks | IMPLEMENTED | `.github/workflows/ci.yml` (not yet run remotely) | no remote repo |
| A-005 | ADR template | TESTED | `docs/adr/` | — |
| A-006 | Testkit / fixtures | TESTED | `stratadb-testkit` (7 seeded profiles, determinism + validity tests); golden fixture | — |
| B-001..B-005 | Event schema, typed values, timestamps, schema version, validation | TESTED | schema crate (16 tests incl. proptest) | explicit schema registration not implemented |
| B-006 | Serialization compatibility tests | TESTED | golden v1 data dir `tests/fixtures/storage-v1` + `reads_storage_v1_fixture` | single version so far |
| C-001..C-007 | WAL format, append, checksum, rotation, sync policies, recovery, reclamation | TESTED | wal crate (21 tests: bit flips, torn tail at every offset, gaps, concurrency, poison) | power-loss not simulated |
| C-008 | Crash injection tests | TESTED | `storage/tests/crash.rs` (15 points, real process exit) + `power_loss.rs` (WAL-tail-loss model, 3 modes) | no fault-injecting FS (reordering, lost renames) |
| D-001..D-005 | Segment format v3 (LZ4 chunks + shredded attributes; reads v1–v3), writer, reader, footer, checksums | TESTED | segment crate (11 tests: every-byte flip v1+v2, truncation, proptest, compression fallback), `v1_fixture_upgrades_through_compaction` | provisional (ADR-0001, RFC-0001) |
| D-006 | Manifest | TESTED | manifest tests (flip/truncation/unknown fields/invariants) | O(segments) rewrite per commit |
| D-007 | Flush transaction | TESTED | engine + crash tests | single flush thread |
| D-008 | Corruption tests | TESTED | engine tests (segment corrupt/missing, manifest missing) | — |
| E-001..E-009 | Grammar, parser, plan, scan, filter, projection, limit/sort, aggregation, EXPLAIN | TESTED | query crate (27 tests incl. reference-equivalence proptest) | no arithmetic/joins (by design) |
| E-010 | Query budgets, timeout, cancellation | TESTED | `budgets_and_cancellation_are_enforced` | memory is an estimate |
| E-012 | Vectorized filter evaluation (typed kernels, selection vectors, compiled `LIKE`; `enable_vectorized` switch) | TESTED | `vector::tests::vectorized_equals_scalar` (2 000 random expr × batch cases, mutation-checked with 7 injected bugs), `like_fast_path_equals_general`, engine equivalence proptest with the switch off; A/B report `benchmarks/reports/2026-11-25-vectorized-ab.md` | projection/grouping still per row; single-threaded |
| E-011 | Stable keyset pagination (row queries; HTTP `page_size`/`cursor`; Rust/Python/Go SDK helpers) | TESTED | `query/tests/pagination.rs` (9 tests incl. proptest; mutation-checked: removing the tie-break, the `as_of` bound, the strict cursor comparison or loosening either pruning rule each fails tests), `query_pagination_over_http`, SDK conformance (ADR-0023) | aggregates/EXPLAIN not paginated; deep pages of non-time orders rescan |
| F-001 | Segment statistics | TESTED | footer stats + `verify` | — |
| F-002 | Timestamp pruning | TESTED | pruning tests + equivalence proptest | segments not time-partitioned |
| F-003 | Bloom filters (ids + selected attributes) | TESTED | bloom unit tests, `bloom_filters_prune_exact_id_lookups_without_false_negatives`, equivalence proptest, A/B report | rebuild requires compaction |
| F-004..F-008 | Exact-match postings, inverted index, index manifest/verify/rebuild | NOT_STARTED | — | — |
| G-001..G-003 | HTTP server, auth, batch ingest endpoint | TESTED | `apps/stratadb-server/tests/http.rs` (5) + auth unit tests (2) + manual curl smoke | no TLS; token reload needs restart |
| G-004 | WAL integration of API writes | TESTED | receipts carry commit_seq + durability | — |
| G-006 | Idempotency keys (bounded, durable) | TESTED | `storage/tests/idempotency.rs`, crash-matrix retries, `idempotent_ingest_over_http` (ADR-0019) | compaction must preserve extension tag 1 |
| G-007 | OTLP/HTTP logs: protobuf + JSON, gzip | TESTED | protoc-generated fixtures, `otlp_http_json_logs`, official OTel Go exporter interop (`otlp_interop`), fuzz target `otlp_proto` | no gRPC transport |
| G-008 | Rust SDK (`sdk/rust`) | TESTED | `sdk/rust/tests/conformance.rs` (real server, TLS custom CA, mock-server retries keep the idempotency key, cancellation, bounded retries) | blocking API only |
| G-008 | Python SDK (stdlib only) | TESTED | `sdk/python/tests`, driven by `python_sdk` test against a real server | sync API |
| G-008 | Go SDK (stdlib only) | TESTED | `sdk/go/client_test.go` (httptest retries) + `go_sdk` real-server run | |
| — | Config file (TOML) + env overrides + `config` validation | TESTED | `config.rs` tests, binary smoke | restart required for changes |
| G-005 | Backpressure (engine level) | TESTED | `backpressure_rejects_when_flush_backlog_full` | no disk watermark |
| H-001 | RBAC (spec permission set) | TESTED | handler checks + `authentication_and_authorization_are_enforced` | only ingest/query/cluster.read are used so far |
| H-002 | Tenant isolation (API → storage) | TESTED | `tenant_isolation_across_the_api`, storage/query isolation tests | — |
| H-003 | TLS (rustls, ring) | TESTED | `tests/tls.rs`, curl smoke (ADR-0021) | no mTLS / hot reload |
| H-004 | Audit log (hash-chained) | TESTED | `audit.rs` unit test, `security_events_are_audited_without_secrets` (ADR-0022) | no rotation; tamper-evident only |
| — | Disk reserve admission control | TESTED | `disk_reserve_pauses_writes_but_not_reads_or_flushes` | reserve measured every 200 ms |
| H-005 | Retention (per tenant, legal hold) | TESTED | `tests/maintenance.rs`, CLI + HTTP endpoints (ADR-0020) | periodic, not instantaneous |
| H-006, H-007 | Backup, verify, restore + drills | TESTED | `tests/backup.rs`, CLI drill, HTTP backup (ADR-0010) | full backups only; no encryption |
| H-008 | Metrics | IMPLEMENTED | `/api/v1/metrics` (Prometheus text, cluster.read) | no latency histograms; no query metrics |
| H-009 | CLI incl. `doctor` (read-only diagnostics) | TESTED | `apps/stratadb-cli/tests/cli.rs`, `tests/doctor.rs` (detects corruption, provably read-only) | local admin only |
| H-010 | Docker/compose/systemd + healthcheck | IMPLEMENTED | `deployments/`, `strata-server healthcheck` smoke | **image build not verified** (no Docker daemon) |
| H-011 | Fuzzing (9 targets) | TESTED | stable mutation test in default suite; 30k-iteration campaign; cargo-fuzz targets in `fuzz/` (docs/testing/fuzzing.md) | |
| — | Query status/cancel endpoints | TESTED | `running_queries_can_be_inspected_and_cancelled_by_their_tenant_only` | cancellation checked at batch boundaries |
| — | OpenAPI document | TESTED | `openapi_document_matches_router` | schemas are coarse |
| H-012 | External security review | NOT_STARTED | — | requires independent reviewer |
| BENCH-1 | Benchmark harness | TESTED | `apps/stratadb-bench` + smoke test; baseline report `benchmarks/reports/2026-11-20-baseline-dev.md` | query 11 (concurrent) not implemented; single-run variance |
| — | Compaction (+ obsolete lifecycle) | TESTED | `tests/maintenance.rs` (incl. equivalence proptest), 4 crash points | single-threaded, one group/tenant/pass |
| I-* | Distributed | NOT_STARTED (deferred by spec) | — | — |

## Task log (this session)
| Task | Summary | Tests added | Commands | Result |
|------|---------|-------------|----------|--------|
| Phase 0 | Repo init, ADRs 0001–0017, format specs, threat model, RTM | — | — | docs |
| C-001..C-008 | WAL v1 + recovery + group commit + crash matrix | 21 + 17 | `cargo test -p stratadb-wal`, `--test crash` | pass |
| D-001..D-008 | Segment v1, manifest, flush, engine | 7 + 5 + 14 | `cargo test -p stratadb-storage` | pass |
| E-001..E-010 | StrataQL v0 | 27 | `cargo test -p stratadb-query` | pass |
| H-009 | `strata` CLI | 1 (e2e) | `cargo test -p stratadb-cli` | pass |
| BENCH-1, A-006 | testkit + bench harness + baseline report | 3 | `strata-bench --events 200000` | pass |
| G-001..G-003, H-001, H-002 | `strata-server` HTTP API + tokens | 7 | `cargo test -p stratadb-server`; curl smoke | pass |
| Bug | Flaky `authentication_and_authorization_are_enforced` (forged token equalled the real one 1/16 of runs; test bug, not an auth bug) | guard `assert_ne!` | 40× loop | fixed |
