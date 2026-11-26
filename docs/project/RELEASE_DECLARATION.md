# Release declaration: performance level (spec VIII.38, Appendix D)

Required by STRATA-SYS-38-001 for every release. It states which spec Part
VIII compliance level is achieved, which targets are unmet and who owns them,
and where the archived benchmark artifacts are.

## Current state: 0.0.1 (unreleased)

| Item | Value |
|---|---|
| **Part VIII level achieved** | **none** |
| Highest level whose mechanisms exist | L0 (measurement system: harness, manifests, ledger, open-loop load and regression detection exist), *not achieved* |
| Why L0 is not achieved | L0 must pass on HW-REF-1. No HW-REF-1 runner exists, CI has no dedicated runner (STRATA-SYS-19-006), and "all VIII.1 stage metrics exported" is partial (no schema-resolution or partition-routing stages, because those stages do not exist yet) |
| Hardware used for all published numbers | HW-REF-7: shared developer laptop, Apple M4, 10 cores, macOS |
| Correctness, durability and isolation tests | `scripts/check.sh`: all passing, none skipped (see IMPLEMENTATION_STATUS.md for the count) |
| Archived artifacts | `benchmarks/reports/` (raw JSON next to every report); P0 runs: `benchmarks/reports/2026-11-26-p0-measurement-system/` |

### Unmet targets
Every VIII.1 numeric budget is unverified, because the budgets are defined
for HW-REF-1. The HW-REF-7 measurements in
`benchmarks/reports/2026-11-26-p0-measurement-system.md` are informational.
On the INSERT path they show:
- `wal_encode` is above its HW-REF-1 budget: the row-oriented event codec,
  where the spec expects a columnar WAL payload (STRATA-SYS-07-002).
- `group_commit_wait` is dominated by the laptop's fsync latency.

Requirement-level status, with the gaps and reasons, is in
`docs/project/PERF_INVENTORY.md`.

| Gap | Owner | Blocked on |
|---|---|---|
| HW-REF-1 runner, CI performance gate (SYS-01-002, SYS-19-006) | project owner | hardware |
| Cycle counters for the ledger (SYS-01-014) | project owner | Linux runner with `perf_event_open` |
| ISA-specific kernels (SYS-08-003) | project owner | decision on an `unsafe` boundary (ADR-0026) |
| Everything else marked MISSING | engineering, in the phase order of ADR-0026 | — |

## Rules for future declarations
- A level is claimed only if its VIII.38 gate passes on the required
  profiles **and** no correctness, durability or isolation test is failing
  or skipped (STRATA-SYS-38-002). Performance results never override this.
- Numbers from HW-REF-7 are never presented as evidence for a level.
- Every claim links to raw artifacts (manifest + results JSON) that reproduce
  it.
