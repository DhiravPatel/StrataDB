# Task backlog (dependency-ordered)

Legend: ✅ done (tested) · ▶ next · ☐ pending. IDs follow spec §35.

## Done
✅ A-001 A-002 A-005 · ✅ B-001..B-005 · ✅ C-001..C-008 · ✅ D-001..D-008 ·
✅ E-001..E-012 (E-011 stable pagination, ADR-0023; E-012 vectorized filters) · ✅ F-001 F-002 · ✅ H-009 (local CLI) · ✅ engine backpressure

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
10. ✅ F-003 bloom filters (A/B evidence). ☐ F-004 exact-match postings; + exact-match postings with benchmark evidence.
11. ✅ **G-007** OTLP/HTTP JSON. ✅ **G-008** Rust, Python, Go SDKs; ☐ OTLP protobuf; ☐ real-Collector interop test.
12. ✅ **H-011** fuzz targets (WAL parser, segment reader, manifest, query parser).
13. ✅ Disk high-watermark admission control (spec §28.4).
14. ☐ Parquet evaluation per ADR-0001.
15. ✅ **H-003** TLS. ☐ token hot-reload; per-tenant quotas.
16. ✅ **H-004** audit log.
17. ✅ OpenAPI document; ☐ SDK generation from it.
