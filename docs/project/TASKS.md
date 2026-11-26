# Task backlog (dependency-ordered)

Legend: ✅ done (tested) · ▶ next · ☐ pending. IDs follow spec §35.

## Done
✅ A-001 A-002 A-005 · ✅ B-001..B-005 · ✅ C-001..C-008 · ✅ D-001..D-008 ·
✅ E-001..E-012 (E-011 stable pagination, ADR-0023; E-012 vectorized filters; E-013 allocation-free grouping; E-014 parallel scans; E-015 key projection; E-016 flat groups) · ✅ F-001 F-002 · ✅ H-009 (local CLI incl. export/import/shell) · ✅ engine backpressure

## Next (in order)
1. ✅ **T-P1-PL** Power-loss simulation (`simfs` recorder + `power_loss_sim.rs`);
   found and fixed WAL fail-closed on reordered un-synced tails (WAL v2).
2. ✅ **B-006** Golden v1 fixtures (`tests/fixtures/storage-v1`).
3. ✅ **A-006** `stratadb-testkit`: deterministic synthetic dataset generator
   (spec §27.3 profiles; seeded) shared by tests and benchmarks.
4. ✅ **BENCH-1** `apps/stratadb-bench`: ingest + query-suite (§27.4) harness
   emitting raw JSON results with hardware/version metadata.
5. ☐ **A-003** tracing subscriber + structured JSON logs in the CLI/server.
6. ✅ **H-001/H-002 + G-001..G-003** HTTP server (axum or hyper) **with**
   API-token auth, tenant-from-principal, batch endpoint, partial-batch
   errors, bounded request bodies, query endpoint. ADR for HTTP framework.
7. ✅ **G-006** idempotency keys with bounded dedup window (document scope).
8. ✅ Compaction (merge small segments; manifest lineage; delete inputs only
   after commit and when no snapshot holds them).
9. ✅ **H-005** retention; **H-006/H-007** backup/restore + drills.
10. ✅ F-003 bloom filters (A/B evidence). ✅ F-005 trigram filters for LIKE (A/B evidence). ⏸ F-004 postings deferred (ADR-0005: needs row groups).
11. ✅ **G-007** OTLP/HTTP JSON. ✅ **G-008** Rust, Python, Go SDKs; ☐ OTLP protobuf; ☐ real-Collector interop test.
12. ✅ **H-011** fuzz targets (WAL parser, segment reader, manifest, query parser).
13. ✅ Disk high-watermark admission control (spec §28.4).
14. ☐ Parquet evaluation per ADR-0001.
15. ✅ **H-003** TLS. ☐ token hot-reload; per-tenant quotas.
16. ✅ **H-004** audit log.
17. ✅ OpenAPI document; ☐ SDK generation from it.

## Performance engineering (spec Parts IV–IX, ADR-0026)
Requirement-level status: PERF_INVENTORY.md (generated; `scripts/spec_inventory.py`).

### Phase 9: P0 measurement system ✅
- ✅ **P0-1** TOML scenario harness: warmup, repetitions of at least 5, raw
  samples, median/p95/p99/p99.9/stddev, bootstrap CI (SYS-19-001).
- ✅ **P0-2** Open-loop load generator with client-bound and saturation
  detection (SYS-19-002/003).
- ✅ **P0-3** Run manifests (SYS-19-004).
- ✅ **P0-4** Regression comparison: Mann-Whitney plus a 3% floor
  (SYS-19-005).
- ✅ **P0-5** Plan assertions in scenarios (SYS-25-002).
- ✅ **P0-6** `strata_insert_stage_seconds`, `strata_query_stage_seconds`,
  `plan_us`/`exec_us`/`queue_us` (SYS-01-001/007).
- ✅ **P0-7** Cost ledger in CPU ns, with the 5%-beyond-noise rule
  (SYS-01-015; cycles BLOCKED).
- ✅ **P0-8** Release and `release-checked` profiles, nightly job
  (SYS-18-001/005).
- ✅ **P0-9** Release declaration (SYS-38-001/002) and CI perf smoke.
- ☐ **P0-10** Sustained-ingest mode (≥ 3 compaction cycles, sustained/first-5-min ratio; SYS-01-004/005).
- ☐ **P0-11** fio/STREAM baselines in manifests (SYS-01-008); startup-vs-part-count benchmark (SYS-01-013).
- ☐ **P0-12** Dedicated HW-REF-1 runner + CI gate (SYS-01-002, SYS-19-006). Owner action.

### Phase 10: P1 + A0 (next, in order)
1. ▶ **P1-1** Bounded scan worker pool shared by all queries. It replaces
   the per-query scoped threads (SYS-01-010) and adds DOP limits for small
   queries (SYS-01-006). The open-loop trace-lookup load shows the cost
   (report 2026-11-26).
2. ☐ **P1-2** Thread inventory and naming (SYS-03-014/015).
3. ☐ **P1-3** `IoEngine` trait with a portable backend and the `sim` fault
   backend (SYS-04-001/002); fd budget and LRU (SYS-02-037).
4. ☐ **P1-4** `strata doctor host` VIII.2 checklist (SYS-02-*), including
   raising RLIMIT_NOFILE (SYS-02-036).
5. ☐ **P1-5** Adaptive group-commit delay (SYS-07-004); columnar WAL
   payload (SYS-07-002, a format change: version bump and fixtures).
6. ☐ **A0-1** `xtask check-layers` (ARCH-01-001/002); `ScanSource` seam
   (ARCH-01-003).
7. ☐ **A0-2** Query log with fingerprint, `queue_us`, `plan_us`
   (SYS-01-007, SYS-13-009, SYS-11-006).
