# CLAUDE.md — persistent instructions for coding agents

Read before every session: this file, `docs/project/NEXT_SESSION.md`, `docs/project/IMPLEMENTATION_STATUS.md`,
`docs/project/TASKS.md`, and the relevant ADRs in `docs/adr/`. The master contract is
`docs/spec/StrataDB_Master_Engineering_Specification.md` (the "spec").

## Non-negotiables (spec Appendix E, §36.3)
- Never acknowledge a write before the configured durability boundary.
- Never delete source data (WAL segments, segments) before replacement data is
  durably committed in the manifest.
- Never change a persistent format without bumping its version, updating
  `docs/storage-format/`, and adding compatibility fixtures.
- Never bypass tenant scoping: queries take a tenant-scoped `Snapshot`; storage
  re-checks tenant ownership on write.
- No unbounded queues/channels, no unbounded query, no `unsafe` (workspace
  `unsafe_code = "forbid"`), no `unwrap`/`expect` outside tests.
- No fabricated benchmark numbers; no claims without reproducible evidence.
- Do not mark anything VERIFIED/production-ready without tests that prove it.

## Workflow
1. Pick the next dependency-ready task from `docs/project/TASKS.md`.
2. State invariants; implement the smallest complete unit with tests.
3. Run `scripts/check.sh` (fmt, clippy -D warnings, tests). All must pass.
4. Update `docs/project/IMPLEMENTATION_STATUS.md`, `docs/project/TASKS.md`, `docs/project/KNOWN_LIMITATIONS.md`,
   relevant docs/ADRs, and `docs/project/NEXT_SESSION.md`.

## Layout
- `crates/stratadb-common` errors, CRC-32C, bounded codec, durable fs ops, failpoints, time
- `crates/stratadb-schema` event model, typed values, validation, JSON mapping, event codec
- `crates/stratadb-wal` WAL format, group commit, recovery, reclamation
- `crates/stratadb-segment` segment format v1 writer/reader/verify
- `crates/stratadb-storage` engine: memtable, flush protocol, manifest, recovery, snapshots
- `crates/stratadb-query` StrataQL parser, planner (pruning), budgeted executor
- `crates/stratadb-testkit` seeded synthetic datasets (spec §27.3 profiles)
- `apps/stratadb-cli` `strata` binary (local admin)
- `apps/stratadb-bench` `strata-bench` harness → raw JSON (see docs/benchmarks)
- `apps/stratadb-server` `strata-server` HTTP API v1 + token management (ADR-0018, docs/api/http.md)

## Conventions
- Storage engine is synchronous (ADR-0017); async only at the network edge.
- Lock order: write_gate → state → memtable; manifest mutex serializes flushes.
- Failpoints: `stratadb_common::failpoint::hit("area::point")`; add a crash
  case in `crates/stratadb-storage/tests/crash.rs` and a row in
  `docs/testing/crash-matrix.md` for every new durability transition.
- Errors carry `ErrorKind`; storage errors are never swallowed.
- Disk on the dev machine is tight (~26 GB free): avoid heavy dependencies
  without an ADR; `target/` uses line-tables-only debug info.
