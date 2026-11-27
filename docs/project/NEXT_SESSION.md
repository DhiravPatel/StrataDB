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
doctor host`, shipped sysctl.d and tuned profiles) is **done**. P1-5 (adaptive group commit, `strata doctor storage`) is **done**: +50%
ingest throughput, and commit wait close to fsync latency. P1-3b (the write path through `IoEngine`, write-fault tests) is **done**.
A0-1 is **done**:
- `scripts/check_layers.py` checks the crate graph against
  `docs/architecture/layers.toml` in check.sh and CI;
- the executor reads only through `stratadb_storage::ScanSource` (ADR-0028).

A0-2 is **done** (ADR-0029):
- `SELECT ... FROM system.query_log` returns the tenant's own queries;
- entries live in a second engine at `<data_dir>/system/query_log`, recorded
  without blocking (a bounded queue, writer `sx-maint-02`);
- each entry has a fingerprint, `cpu_us` and the phase timings;
- the timing headers are `x-stratadb-*-us`.

`execute` takes a `Catalog`, and a bare `Snapshot` still works.

P1-6 is **done** (ADR-0030):
- the memory tracker hierarchy lives in `stratadb_common::memory`;
- the server's regions are in `apps/stratadb-server/src/memory.rs`;
- `EngineOptions.memory` / `cache_memory` / `background_memory` attach the
  engine, and `QueryOptions.memory` / `memory_wait` attach queries;
- the bench's `[engine] memory_accounting` toggle is there for A/B runs.

P1-7 is **done** (ADR-0031): WAL v3 writes columnar, LZ4-compressed batch
payloads (record types 3–4; `stratadb_schema::columnar`).
- `[storage] wal_format_version = 2` keeps the row format for a rollback
  window.
- Fixtures: `tests/fixtures/wal-v3` and `storage-wal-v3`.
- Measured: 3.3× fewer WAL bytes and +19% ingest CPU, with LZ4 the bulk of
  it.

P1-8 is **done** (ADR-0032):
- **One fsync in flight per WAL:** a rotation-during-sync bug was found and
  fixed.
- **Wake-ups:** broadcast and sharded designs were measured slower, so
  parking_lot is kept and the wake histogram is new.
- **In-flight I/O:** a per-device gate (`io_max_inflight_per_device`).

The harness is `crates/stratadb-wal/tests/commit_wakeup.rs`. When
benchmarking, check `uptime` and the top processes first: external load
invalidated one A/B this session.

Phase 10 is complete apart from ☐ follow-ups.

A1-1 is **done** (ADR-0033): a columnar memtable.
- Column chunks with a published row count, snapshots at O(chunks), and
  zone-map pruning (`QueryStats.memory_chunks_pruned`).
- Replay goes straight into columns.
- Benchmark: `benchmarks/scenarios/memtable.toml`. The dataset knobs
  `max_flushes` and `final_flush` keep rows unflushed.

A1-2 is **done** (ADR-0034):
- **`StrColumn`:** plain or dictionary.
- **Kernels:** the dictionary filter and group-key kernels.
- **Selections:** per-chunk selections (`Selected`) and the `count(*)` fast
  path.

Evidence for changes like this one comes from the ledger (`strata-bench
ledger --path X --write`, interleaved base and new runs); it is far steadier
than the scenarios.

**Next is A1-3**: state-machine metrics `strata_state_seconds` (ARCH-22-*).

Before benchmarking, check `uptime`: this session saw external load as high
as 88 and a machine sleep. Disk is tight: `target/debug/incremental` grew to
20 GB and was cleared. Run benchmarks in the foreground (efficiency-core pitfall).

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
