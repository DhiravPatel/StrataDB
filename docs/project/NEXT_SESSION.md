# docs/project/NEXT_SESSION.md

## Current phase
Single-node product (Phases 1–5) is implemented and tested end to end:
durable storage + crash recovery, query engine, Bloom indexes, HTTP API with
TLS / token auth / RBAC / audit log, idempotent ingest, OTLP/HTTP JSON,
compaction, retention, backup/restore, disk reserve, config file, Rust SDK,
deployment files, fuzz harnesses. Phase 6 (production-candidate hardening)
is in progress: fuzzing (stable + libFuzzer, ~8.6 M executions, no crashes),
segment v2 compression (2.9× smaller), query cancellation, OpenAPI, Python/Go
SDKs, `strata doctor` are done this session. Distributed mode (Phases 7–8) has not started, by design.

## State of the tree
- Branch `main`, git initialized, **no commits** (owner asked not to commit).
- Workspace: crates common, schema, wal, segment, storage, query, testkit, fuzz;
  apps cli (`strata`), bench (`strata-bench`), server (`strata-server`);
  sdk/rust (`stratadb-client`); `fuzz/` (cargo-fuzz, not a workspace member).
- Golden fixture `tests/fixtures/storage-v1/` (never regenerate).

## Last validation (2026-11-25)
`scripts/check.sh` → fmt ✅ clippy `-D warnings` ✅ tests ✅ (see
docs/project/IMPLEMENTATION_STATUS.md for the count). Stable mutation fuzzing runs in the
default suite.

## Known failures
None.

## Decisions this session (ADRs 0019–0022, 0005, 0010 updated)
Idempotency via WAL record type 2 + segment extension tag 1; compaction/retention
with durable `obsolete_segments`; full verified backups; rustls+ring TLS;
hash-chained audit log (admin actions fail closed); Bloom filters ext tag 2
(string values only) with A/B evidence; TOML config with flag > env > file precedence.

## Spec v0.4 (Parts IV–IX) adopted — read first
ADR-0026 maps the spec's phases P0–P4 and A0–A4 onto roadmap phases 9–14
and resolves the conflicts: `unsafe` stays forbidden; kernels are scalar
first; io_uring comes after a portable `IoEngine`; there is no
general-purpose table claim until typed tables exist.
`docs/project/PERF_INVENTORY.md` has a row for every requirement. To update
a status, edit `docs/project/perf-status.toml` and run
`scripts/spec_inventory.py`; `check.sh` fails if the inventory is stale.

**Phase 9 (P0, measurement system) is done.** It provides:
- `strata-bench scenario`, `compare` and `ledger`;
- `benchmarks/scenarios/{smoke,standard}.toml`;
- stage metrics;
- release profiles;
- `RELEASE_DECLARATION.md`.

The gates give false verdicts on this laptop (A/A: 9 of 27), so CI runs the
smoke scenario only. **Next: phase 10, task P1-1**: a bounded scan worker
pool replacing per-query threads (STRATA-SYS-01-010/006). **Done**: shared
`sx-work-NN` pool, contention-aware DOP, report 2026-11-26-scan-pool. P1-2 (thread
inventory, budget, `sx-blk` pool, `/api/v1/admin/threads`) is **done** too.
P1-3 (read path: I/O
engine, segment file cache, sim faults, ADR-0027) is **done**. P1-4 (`strata
doctor host`, shipped sysctl.d and tuned profiles) is **done**. Next is
P1-5: adaptive group-commit delay (STRATA-SYS-07-004; measure fsync latency
and arrival rate, flush immediately when idle). Then P1-3b: routing writes
through `IoEngine`. Run benchmarks in the foreground (efficiency-core pitfall).

## Exact next tasks (priority order)
1. Query execution follow-ups: intra-segment parallelism; row-selective
   decoding of whole maps; cheaper value validation for raw group keys (UTF-8
   checks now dominate query 08); ranking without per-group `Vec` allocation.
2. Run the Docker image build + an OpenTelemetry Collector binary (needs Docker).
3. Longer seeded libFuzzer campaigns (≥ 30 min/target) for all 14 targets.

Done since the last handoff: segment v3 attribute shredding (2.6–4.5× on attribute
queries), OTLP/HTTP protobuf + official OTel Go exporter interop, token hot reload,
per-tenant rate/storage quotas, JSON structured logs with request ids,
`strata upgrade` with verified backup gate; **stable keyset pagination**
(E-011, ADR-0023: `execute_page`, HTTP `page_size`/`cursor`/`next_cursor`,
SDK `query_pages`/`QueryAllPages`); **vectorized `WHERE` evaluation** (E-012,
1.2–3.5× on filtered suite queries, report `2026-11-25-vectorized-ab.md`);
**power-loss simulation** (simfs recorder, 2 428 recovered states, 0
violations) and **WAL format v2** (`synced_through`; fixes fail-closed after
reordered un-synced tails); **trigram filters for LIKE** (F-005; postings F-004
deferred, ADR-0005); **export/import/shell** (`strata export` with
checkpoint/resume, idempotent `ingest --import-id`, `strata shell`); **allocation-free grouping** (1.3–3.0× on group-by
queries); **encryption at rest** (ADR-0024); **parallel segment scans** (2.2–2.6×); flat
aggregation groups (2.2× less CPU); latency histograms and query metrics;
index list/verify/rebuild; **Parquet import/export** (ADR-0025); **doctor
completeness** (§22.3: fd limit, memory budget, config, TLS expiry, backup
age, `--deep` index verification — docs/operations/doctor.md); **undecoded
group keys + top-k** (2.95× less CPU on query 08, report
`2026-11-25-raw-group-keys.md`); 5-minute libFuzzer campaign over all 14
targets (40.9 M runs, no crashes). Gate: see IMPLEMENTATION_STATUS.md.

## Owner actions required
1. Confirm license (ADR-0016) and add LICENSE.
2. Security reporting channel + CoC contact (docs/SECURITY.md, docs/CODE_OF_CONDUCT.md).
3. Start Docker if you want the image build / Collector interop verified.
4. Disk: ~16 GB free; `cargo clean` frees ~6 GB of build artifacts when needed.
