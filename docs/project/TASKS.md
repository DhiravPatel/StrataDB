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
1. ✅ **P1-1** Shared scan worker pool (`sx-work-NN`, bounded queue,
   caller-runs when busy) and cost-based DOP (SYS-01-010/006, 11-008 partial);
   report 2026-11-26-scan-pool.
2. ✅ **P1-2** Thread inventory and naming (SYS-03-014/015):
   - all engine threads are `sx-*` and registered;
   - the thread plan is validated against `2 × cpus + 32`;
   - a bounded `sx-blk` pool replaces `spawn_blocking`;
   - `GET /api/v1/admin/threads` and `strata_threads` metrics.
3. ✅ **P1-3** (read path) `IoEngine` trait with a portable backend and the `sim` fault
   backend (SYS-04-001/002); fd budget and LRU (SYS-02-037). Done for reads
   (ADR-0027). ✅ **P1-3b** writes (segment writer, WAL, manifest, backup)
   and WAL replay routed; write-fault tests (report 2026-11-26-io-write-path).
   ☐ in-flight bounds (SYS-04-008); io_uring backend (Linux).
4. ✅ **P1-4** `strata doctor host` VIII.2 checklist (SYS-02-*), including
   raising RLIMIT_NOFILE (SYS-02-036).
5. ✅ **P1-5** Adaptive group-commit delay (SYS-07-004) and `strata doctor
   storage` (SYS-07-008); report 2026-11-26-adaptive-group-commit.
6. ✅ **A0-1** Layer check `scripts/check_layers.py` + `docs/architecture/layers.toml`
   in check.sh and CI (ARCH-01-001/002); `ScanSource` seam in
   `stratadb_storage::scan`, column types in `stratadb_schema::column`,
   query crate no longer depends on the segment crate (ARCH-01-003,
   ARCH-11-001 partial); ADR-0028, report 2026-11-26-scan-source-seam.
   ☐ Split `stratadb-query` into exec (L3) and sql/planner (L4) crates once a
   binder/optimizer exists; standalone L1 microbenchmarks (ARCH-01-004).
7. ✅ **A0-2** `system.query_log` (tenant-scoped, second engine, bounded
   non-blocking recording), fingerprints, per-query CPU time, timing headers
   (SYS-01-007, SYS-11-006, SYS-13-009); ADR-0029.
   ☐ Cluster-admin cross-tenant view; per-operator est/actual rows
   (SYS-12-004); fingerprint from the parser's tokens.
8. ✅ **P1-6** Memory accounting hierarchy (ADR-0030):
   - the process budget (0.80 of the cgroup or physical limit) splits into
     control, ingest, cache, background and query regions; the query pool
     has tenant and query nodes;
   - reservations go leaf to root, through batched per-worker accountants;
   - column batches are reserved before reads, and ingest before the WAL
     append; compaction is deferred when memory is full;
   - `MEMORY_LIMIT_EXCEEDED` names the path and top 5 consumers, with
     tenants redacted; queries can wait for memory;
   - RSS is compared with tracked memory every 10 s;
   - `/api/v1/admin/memory` and the `strata_memory_*` metrics.
   Report 2026-11-26-memory-accounting (no measurable cost).
   ☐ Spill (SYS-05-019..022, phase 12); an evictable cache (SYS-05-008);
   accounting for the control region; flush-buffer reservations.
9. ✅ **P1-7** Columnar WAL payload (ADR-0031):
   - WAL v3 adds record types 3–4, with columnar batch v1: the tenant once,
     delta timestamps, dictionaries, LZ4 at 16 KiB and above;
   - `[storage] wal_format_version = 2` gives a rollback window;
   - fixtures `wal-v3` and `storage-wal-v3`, format docs, fuzzing;
   - `strata_wal_bytes_total`.
   Report 2026-11-26-columnar-wal: 3.3× fewer WAL bytes per event, +19%
   ingest CPU (LZ4), replay slightly faster.
   ☐ Decode straight into a columnar memtable (phase 11, A1).
10. ✅ **P1-8** (ADR-0032):
    - a single fsync in flight per WAL (SYS-07-005): a rotation-during-sync
      bug was found and fixed;
    - wake-up designs measured, parking_lot kept, `strata_wal_commit_wake_seconds`
      added (SYS-07-006 partial);
    - a per-device in-flight I/O gate, `io_max_inflight_per_device`
      (SYS-04-008).
    Report 2026-11-26-commit-wakeup-and-inflight.
    ☐ Futex-word wake-ups with decoupled leader election, measured on HW-REF-1;
    a device default derived from measured saturation queue depth.

**Phase 10 is complete except the items marked ☐ above. Next: phase 11 (A1 + P2).**

### Phase 11: A1 + P2 (in order)
1. ✅ **A1-1** Columnar memtable (ADR-0033):
   - column chunks (64 → 65,536 rows), a published row count per tenant
     (Release/Acquire, whole batches), and snapshots at O(chunks);
   - zone-map pruning of memtable chunks, an O(1) freeze, and WAL replay
     straight into columns (ARCH-02-012/013/014, ARCH-04-002 partial,
     SYS-07-002 now implemented);
   - report 2026-11-27-columnar-memtable: 3.1–3.2× on time/severity filters
     over unflushed rows.
   ☐ Bloom/key filters per chunk; flush straight from columns.
2. ✅ **A1-2** Typed string columns and selection vectors (ADR-0034):
   - `StrColumn` (plain text, offsets and validity; or dictionary plus
     codes; shared immutable buffers);
   - dictionary filter and group-key kernels, plus pre-encoded severity
     keys;
   - per-chunk selections (dense or indices) and a global `count(*)` fast
     path;
   - report 2026-11-27-typed-columns-selection: standard 1.18×, memtable
     1.11×, GROUP BY severity 2.9×.
   ☐ Typed numeric and value columns with null bitmaps, and a vector pool;
   dictionary-encoded memtable strings; caching dictionary truth tables per
   batch.
3. ▶ **A1-3** State-machine metrics `strata_state_seconds` (ARCH-22-*).
4. ☐ **P2** Scheduler classes and admission, the hash-table family, the
   scalar kernel library.
