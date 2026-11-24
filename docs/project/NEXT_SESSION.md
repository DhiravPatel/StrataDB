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

## Last validation (2026-11-21)
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

## Exact next tasks (priority order)
1. Stable pagination for row queries (keyset cursor on (event_time, seq, idx);
   spec §2.3) and `next_cursor` in the HTTP API + SDKs.
2. Vectorized predicate evaluation over typed columns (avoid per-row `Value` clones).
3. Fault-injecting filesystem shim for full power-loss simulation (reordering,
   lost un-fsynced renames).
4. Token inverted index for `body` (F-005) and exact-match postings (F-004).
5. CSV / Parquet export (§30.3), `strata shell` REPL (§22.1).
6. Encryption at rest (§18.6) behind a pluggable key provider.
7. Run the Docker image build + an OpenTelemetry Collector binary (needs Docker).
8. Longer seeded libFuzzer campaigns (≥ 30 min/target).

Done since the last handoff: segment v3 attribute shredding (2.6–4.5× on attribute
queries), OTLP/HTTP protobuf + official OTel Go exporter interop, token hot reload,
per-tenant rate/storage quotas, JSON structured logs with request ids,
`strata upgrade` with verified backup gate. Gate: 204 tests passing.

## Owner actions required
1. Confirm license (ADR-0016) and add LICENSE.
2. Security reporting channel + CoC contact (docs/SECURITY.md, docs/CODE_OF_CONDUCT.md).
3. Start Docker if you want the image build / Collector interop verified.
4. Disk: ~16 GB free; `cargo clean` frees ~6 GB of build artifacts when needed.
