# ADR-0026: Adopting spec Parts IV–IX (performance engineering and deep architecture)

- **Status:** Accepted (phase plan); individual mechanisms get their own ADRs
- **Spec references:** Parts IV–IX (spec v0.4, 26 Nov 2026), especially VIII.0.1
  (precedence), VIII.30 / IX.43 (work breakdowns), VIII.38 / IX.15 (release
  gates), VIII.39 and Part VI (agent directives)
- **Relates to:** ADR-0006 (query engine), ADR-0017 (synchronous storage
  engine), CLAUDE.md non-negotiables

## Context
Spec v0.4 added about 57,000 lines: Parts IV–IX, with 416 traceable
requirements (291 `STRATA-SYS-*`, 125 `STRATA-ARCH-*`; 333 MUST). They
specify host and kernel configuration, a thread-per-core runtime, io_uring
I/O, memory accounting, ISA-specific kernels, hash tables, codecs,
scheduling, an optimizer cost model, projections and materialized views,
indexing, data layout, result reuse, distributed execution, and the
measurement system that judges all of it. The title also changed from a
log-native database to a "general-purpose analytical database".

Part VI and VIII.39 prescribe the order:
1. inventory every requirement;
2. build the measurement system (phase P0) before optimizing anything;
3. then implement in dependency order, one measured bottleneck at a time,
   with a correct baseline and a fallback.

## Decision

### 1. Precedence and status vocabulary
- Correctness, durability, isolation and security (Parts I–II, CLAUDE.md)
  always win over a performance requirement (VIII.0.1).
- Every requirement has a row in `docs/project/PERF_INVENTORY.md`, generated
  by `scripts/spec_inventory.py` from the spec's work breakdowns and the
  hand-maintained `docs/project/perf-status.toml`.
- Statuses are `IMPLEMENTED`, `PARTIAL`, `MISSING`, `BLOCKED` and `DEFERRED`.
  An entry may claim `IMPLEMENTED` or `PARTIAL` only with evidence (code and
  test paths), and `PARTIAL`, `BLOCKED` and `DEFERRED` require a reason.
- `scripts/check.sh` fails if the inventory is stale or names unknown IDs.
- No requirement is `PERF-ACCEPTED`. That status needs archived evidence
  from the reference hardware, which does not exist yet (§4).

### 2. Phase plan
The spec's phases are mapped onto the roadmap (docs/project/ROADMAP.md) in
dependency order:

| Order | Spec phases | Scope (summary) | Prerequisite |
|---|---|---|---|
| 9 | **P0** | Measurement system: scenario harness, manifests, open-loop load, regression detection, stage metrics, ledger, build profiles, release declaration | — |
| 10 | **P1 + A0** | `doctor host`, thread inventory and naming, bounded worker pool (no thread per query), `IoEngine` trait with a portable backend and the `sim` fault backend, memory tracker hierarchy, WAL concrete design (adaptive group delay, columnar payload), layering check (`xtask check-layers`), `ScanSource` seam, query log with `plan_us`/`queue_us`/fingerprint | P0 |
| 11 | **A1 + P2** | Typed column vectors (`strata-types`), columnar memtable chunks, push pipelines and morsel scheduling, kernels (scalar oracle first), hash-table family, codec selection, scheduler classes and admission, catalog snapshot by atomic swap, state-machine metrics | P1/A0 |
| 12 | **A2 + P3** | Projections and MVs (after aggregate `merge` property tests), common `IndexReader`, layout features, optimizer cost model, sort/join/spill, caches, native protocol | A1/P2 |
| 13 | **A3** | Result reuse, prepared statements, accelerator interfaces | A2/P3 |
| 14 | **P4 + A4** | Distributed execution: spec Phases 7–8, still gated | single-node L2 |

Within a phase, each change targets one measured bottleneck and carries the
evidence listed in Part V.9.

### 3. Conflicts and how they are resolved
- **`unsafe` code.** IV.06 and VIII.39.8 allow audited `unsafe` in kernels;
  CLAUDE.md and the workspace lint forbid it (`unsafe_code = "forbid"`).
  - *Decision:* keep `forbid`.
  - Kernels start as safe scalar code, which the spec requires anyway as the
    differential oracle (STRATA-SYS-08-001), relying on autovectorization.
  - ISA tiers (AVX2/AVX-512/NEON) need either `unsafe` intrinsics or a
    vetted dependency. That is an owner decision recorded in a future ADR;
    until then those requirements are `BLOCKED`.
- **io_uring.** It is Linux-only (the development machine is macOS) and needs
  `unsafe` or a dependency with internal `unsafe`.
  - *Decision:* an `IoEngine` trait with a portable buffered `pread`/`pwrite`
    backend and the spec's `sim` fault-injection backend first (VIII.4 allows
    the fallback).
  - The io_uring backend follows on a Linux runner under its own ADR.
- **Runtime.** The engine stays synchronous (ADR-0017), which matches
  STRATA-SYS-03-001 (operators never await).
  - Today's per-query scoped scan threads violate STRATA-SYS-01-010 ("no OS
    thread per query") and are scheduled to be replaced by a bounded worker
    pool in phase 10.
- **Positioning.** The spec now describes a general-purpose analytical
  database. The engine today has one fixed table shape: the log event.
  - Typed tables, DDL and a catalog are new scope, planned under A1/A2.
  - Until then, nothing claims general-purpose table support.
- **Names.**
  - Crates stay `stratadb-*`. The spec's `strata-*` names map one to one
    (`strata-types` → a future `stratadb-types`); renaming adds churn and no
    value.
  - New metrics use the spec's names (`strata_insert_stage_seconds`,
    `strata_query_stage_seconds`); existing `stratadb_*` metrics are kept for
    compatibility.
- **Cycles per row.** STRATA-SYS-01-014 requires hardware performance
  counters on an isolated core.
  - The ledger (`perf/cpr_ledger.toml`) records CPU nanoseconds per unit from
    the CPU-time clock, labelled as such, until a Linux runner with
    `perf_event_open` exists.
  - The CPU-time clock uses `rustix` (already in the lockfile through
    `tempfile`, MIT/Apache-2.0), so the workspace stays free of `unsafe`.
- **Release profile.** Fat LTO, one codegen unit, `opt-level = 3`, and
  `panic = "abort"` (STRATA-SYS-18-001).
  - A panic exits the process. The service manager restarts it
    (`Restart=on-failure` in deployments/systemd) and recovery replays the
    WAL. Tests unwind (separate profile).
  - `release-checked` keeps debug assertions and overflow checks for nightly
    CI (STRATA-SYS-18-005).

### 4. Hardware honesty
- The only machine available is a shared developer laptop (Apple M4, macOS),
  which is **HW-REF-7**.
- Every number in this repository is labelled with its hardware profile and
  never compared with server results (VIII.39.7).
- The VIII.38 levels L0–L3 all require HW-REF-1. No level is claimed (see
  docs/project/RELEASE_DECLARATION.md).
- Shared machines show run-to-run and session-to-session drift of 20–40%
  (benchmarks/reports/2026-11-26-p0-measurement-system.md). The regression
  gate and the ledger check are therefore meaningful only on a dedicated
  runner (STRATA-SYS-19-006); CI runs smoke-level checks only.

## Consequences
- P0 ships first, and later optimization work is measured with it.
- 416 requirements are tracked mechanically. Most are `MISSING`, which is the
  honest starting point for a spec that grew by Parts IV–IX in one revision.
- Some requirements wait on an owner decision (`unsafe` kernels) or on
  hardware (HW-REF-1 and Linux runners). They are `BLOCKED` with that
  reason, not silently skipped.

## Revisit conditions
- A dedicated Linux benchmark runner becomes available: enable the CI gates
  and switch the ledger to cycle counters.
- The owner approves an `unsafe` boundary for kernels.
