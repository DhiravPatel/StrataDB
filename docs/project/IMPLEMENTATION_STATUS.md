# Implementation status

States: NOT_STARTED · DESIGN · IN_PROGRESS · IMPLEMENTED · TESTED · VERIFIED · BLOCKED.
"TESTED" = automated tests cover normal and failure behaviour. "VERIFIED" is
reserved for features that additionally passed an independent review / release
gate; **nothing is VERIFIED yet.** Spec Parts VIII–IX (performance and
deep architecture, 416 requirements) are tracked separately in
PERF_INVENTORY.md (ADR-0026).

Last full validation: 2026-11-26 — `scripts/check.sh`: fmt ✅, clippy
`-D warnings` ✅, `cargo test --workspace` ✅ (318 tests, 0 failures; requirement inventory current).

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
| C-001..C-007 | WAL format v2 (reads v1), append, checksum, rotation, sync policies, recovery, reclamation | TESTED | wal crate (24 tests: bit flips v1+v2, torn tail at every offset, gaps, concurrency, poison, reordered un-synced tail, v1 read-not-append, golden `tests/fixtures/wal-v2`) | last acknowledged record damaged with nothing after it is indistinguishable from a torn tail |
| C-008 | Crash injection tests | TESTED | `storage/tests/crash.rs` (process exit at every failpoint) + `power_loss_sim.rs` (simfs recorder: 512 crash points, 2 428 recovered power-loss states incl. lost/torn/reordered sectors and lost un-synced renames; sensitivity-checked by removing 3 fsyncs) + `power_loss.rs` | lying hardware not modelled; states sampled, not exhaustive |
| D-001..D-005 | Segment format v3 (LZ4 chunks + shredded attributes; reads v1–v3), writer, reader, footer, checksums | TESTED | segment crate (11 tests: every-byte flip v1+v2, truncation, proptest, compression fallback), `v1_fixture_upgrades_through_compaction` | provisional (ADR-0001, RFC-0001) |
| D-006 | Manifest | TESTED | manifest tests (flip/truncation/unknown fields/invariants) | O(segments) rewrite per commit |
| D-007 | Flush transaction | TESTED | engine + crash tests | single flush thread |
| D-008 | Corruption tests | TESTED | engine tests (segment corrupt/missing, manifest missing) | — |
| E-001..E-009 | Grammar, parser, plan, scan, filter, projection, limit/sort, aggregation, EXPLAIN | TESTED | query crate (27 tests incl. reference-equivalence proptest) | no arithmetic/joins (by design) |
| E-010 | Query budgets, timeout, cancellation | TESTED | `budgets_and_cancellation_are_enforced` | memory is an estimate |
| E-012 | Vectorized filter evaluation (typed kernels, selection vectors, compiled `LIKE`; `enable_vectorized` switch) | TESTED | `vector::tests::vectorized_equals_scalar` (2 000 random expr × batch cases, mutation-checked with 7 injected bugs), `like_fast_path_equals_general`, engine equivalence proptest with the switch off; A/B report `benchmarks/reports/2026-11-25-vectorized-ab.md` | projection/grouping still per row; single-threaded |
| E-013 | Allocation-free grouping (borrowed group-key encoding, slice lookup) | TESTED | `group_key_into_equals_group_key_bytes` proptest (1 000 cases; injected tag bug caught), all query equivalence tests; report `2026-11-25-grouping.md` (1.3–3.0× on group-by queries) | map decoding dominates non-shredded keys |
| E-014 | Parallel segment scans with partial aggregation (ordered merge, query-wide budgets) | TESTED | `query/tests/parallel.rs` (2), `row_budget_stops_parallel_workers_early`, full query suite under parallelism; 4 injected bugs caught; report `2026-11-25-parallel-ab.md` (2.2–2.6×) | memtable + merge sequential; float sums not bit-identical to sequential |
| P1-1 | Shared scan worker pool (fixed `sx-work-NN` threads, bounded queue ≤ 2 × workers, caller-runs on saturation) and contention-aware degree of parallelism (base `ceil(est_cpu_ms / 5 ms)` when busy; idle workers used, ≤ 4 under 2 ms) | TESTED | `pool::tests` (bounded queue, named threads, panic isolation), `pool_tests` (DOP rules, saturated pool completes on the caller thread, concurrent queries keep the thread count), `parallel_equals_sequential` (asserts real fan-out), server metrics test; report `2026-11-26-scan-pool.md` | morsel = segment; no scheduling classes; fixed per-row cost calibration; sub-ms queries pay up to ~45% handoff |
| E-015 | Attribute key projection (no whole-map decode for key references) | TESTED | `read_map_keys_equals_full_maps`, `skip_value_validates_like_decode`, engine equivalence proptest with the switch off; report `2026-11-25-map-projection-ab.md` (22× on query 10) | whole-map references still decode every row |
| E-016 | Flat aggregation groups (HashTable + arena, aHash) | TESTED | `groups::tests`, full query suite (equivalence, parallel group order); report `2026-11-25-grouping-flat.md` (2.2× less CPU on high-cardinality GROUP BY) | — |
| P0 | Measurement system (spec VIII.19, Part VI): TOML scenarios with plan assertions, run manifests, open-loop load with client-bound detection, Mann-Whitney regression comparison, CPU-ns cost ledger, `strata_insert_stage_seconds` / `strata_query_stage_seconds`, `plan_us`/`exec_us`/`queue_us`, release profiles, release declaration | TESTED | `apps/stratadb-bench` unit tests (stats known values, open loop vs coordinated omission, manifest fields, scenario end to end + invalid scenarios, compare selection/verdicts, ledger rule); `metrics_histograms_and_query_outcomes`; `cpu` clock test; CI perf-smoke; report `2026-11-26-p0-measurement-system.md` | gates meaningful only on a dedicated runner (A/A on HW-REF-7: 9/27 false verdicts); CPU ns, not cycles; in-process client |
| E-017 | Undecoded group keys + top-k (shredded keys used only as `GROUP BY` keys read as validated raw bytes; group tables hold key bytes only, keys decoded once per output group; top-k selection for `ORDER BY … LIMIT`) | TESTED | `raw_values_agree_with_decoding` (proptest incl. corruptions) + fuzz differential in `map_projection`, `decode_keys_inverts_encoding`, `top_k_equals_stable_sort_then_truncate`, `merge_keeps_order_and_combines_states`, `raw_group_keys_only_when_used_solely_as_group_keys`, shredded-vs-plain engine equivalence; report `2026-11-25-raw-group-keys.md` (2.95× less CPU on query 08) | unshredded keys still decoded from maps; group keys computed by expressions still evaluated per row |
| E-011 | Stable keyset pagination (row queries; HTTP `page_size`/`cursor`; Rust/Python/Go SDK helpers) | TESTED | `query/tests/pagination.rs` (9 tests incl. proptest; mutation-checked: removing the tie-break, the `as_of` bound, the strict cursor comparison or loosening either pruning rule each fails tests), `query_pagination_over_http`, SDK conformance (ADR-0023) | aggregates/EXPLAIN not paginated; deep pages of non-time orders rescan |
| F-001 | Segment statistics | TESTED | footer stats + `verify` | — |
| F-002 | Timestamp pruning | TESTED | pruning tests + equivalence proptest | segments not time-partitioned |
| F-003 | Bloom filters (ids + selected attributes) | TESTED | bloom unit tests, `bloom_filters_prune_exact_id_lookups_without_false_negatives`, equivalence proptest, A/B report | rebuild requires compaction |
| F-005 | Trigram filters for `LIKE`/`ILIKE` segment pruning (body + configured attributes) | TESTED | `ngram` unit tests incl. no-false-negative proptest vs reference LIKE, `ngram_filters_prune_like_scans_without_changing_results`, engine equivalence proptest with LIKE/ILIKE, 2 injected bugs caught; A/B report `2026-11-25-ngram-ab.md` | runs < 3 chars, `NOT LIKE`, `OR` not pruned; rebuild requires compaction |
| F-004 | Exact-match postings | DESIGN (deferred, ADR-0005) | Bloom + vectorized scan; no row groups yet | revisit with row groups |
| F-006..F-008 | Index list/verify/rebuild (per-segment filters) | TESTED | `query/tests/indexes.rs` (stale settings detected, rebuild prunes, results unchanged, idempotent, durable), `false_negatives_are_detected`, CLI `indexes_list_verify_rebuild`; `strata indexes list|verify|rebuild` | rebuild rewrites whole segments; offline (CLI) |
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
| — | Export (NDJSON/CSV, checkpoint/resume), idempotent import with report, `strata shell` | TESTED | `apps/stratadb-cli/tests/export_import.rs` (lossless re-import round trip, torn-page resume == one-shot, snapshot excludes later writes, no overwrite, CSV quoting, duplicate import, changed-file conflict, shell script; 2 injected bugs caught), `event_json_round_trips` proptest | CLI needs exclusive data-dir access; no HTTP export endpoint |
| — | Parquet export/import (CLI, ADR-0025) | TESTED | `parquet_round_trip_is_lossless`, `imports_generic_parquet`, `imports_third_party_parquet_files` (pyarrow/DuckDB fixtures), `scripts/parquet_interop.sh` (pyarrow 26 + DuckDB 2 read the export exactly) | events only, uncompressed export, no resume; no zstd/brotli import |
| — | Encryption at rest (KEK provider, wrapped DEKs, sealed segments/manifest, encrypted WAL, rotation, encrypted backups) | TESTED | `stratadb-crypto` (8), WAL encryption tests (2), `storage/tests/encryption.rs` (6 + golden fixture), encrypted power-loss simulation (1 320 states), CLI workflow, server config test (ADR-0024) | file KEK provider only; offline rotation |
| H-005 | Retention (per tenant, legal hold) | TESTED | `tests/maintenance.rs`, CLI + HTTP endpoints (ADR-0020) | periodic, not instantaneous |
| H-006, H-007 | Backup, verify, restore + drills | TESTED | `tests/backup.rs`, CLI drill, HTTP backup (ADR-0010) | full backups only; no encryption |
| H-008 | Metrics | TESTED | `/api/v1/metrics`: engine counters, latency histograms (ingest ack, query, WAL fsync, flush), group-commit size, query outcomes and work; `metrics_histograms_and_query_outcomes` (exposition validity, cumulative buckets, counts), histogram unit tests; docs/operations/metrics.md | no OTel trace export; no per-tenant series (by design) |
| H-009 | CLI incl. `doctor` (read-only diagnostics: formats, permissions, disk, WAL, segments, deep index verification, fd limit, memory budget, config, TLS expiry, backup age, audit chain) | TESTED | `apps/stratadb-cli/tests/cli.rs`, `tests/doctor.rs` (detects corruption, provably read-only; real certificates via rcgen, `ulimit -n 40`), `src/hostcheck.rs` unit tests; docs/operations/doctor.md | local admin only; no cluster checks |
| H-010 | Docker/compose/systemd + healthcheck | IMPLEMENTED | `deployments/`, `strata-server healthcheck` smoke | **image build not verified** (no Docker daemon) |
| H-011 | Fuzzing (14 targets) | TESTED | stable mutation test in default suite; 30k-iteration campaign; cargo-fuzz targets in `fuzz/` (docs/testing/fuzzing.md) | |
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
