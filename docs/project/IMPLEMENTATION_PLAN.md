# Implementation plan

Dependency graph of epics (spec §35). An arrow means "must exist before".

```text
A (foundation) ──► B (data model) ──► C (WAL) ──┐
                                   └─► D (segments, manifest, flush) ─┬─► E (query) ─► F (indexes)
                                                                      │                   │
                                                                      ├─► G (ingestion: HTTP/OTLP/SDK) ◄─ H1 auth
                                                                      └─► H (retention, backup, compaction, ops)
F + H + benchmarks ──► Phase 6 hardening ──► I (distributed, Phases 7–8) ──► 1.0
```

## Ordering rationale
1. **A→B→C→D** first: everything else relies on a durable, recoverable core
   (spec §40). Exit gate: crash matrix passes; corruption detected. **Met
   for process crashes** (power-loss simulation pending: T-P1-PL).
2. **E** next: queries are needed to measure anything (indexes, benchmarks).
3. **Benchmark generator + harness** before **F**: every index must show
   measured benefit (spec §12, §27).
4. **H1 (auth/RBAC/tenant from principal)** must land *with* **G** — no
   network listener ships without authentication (spec §18.9).
5. **Compaction, retention, backup** before any 0.2 claim; backup is not
   complete until restore drills pass.
6. Distributed work only after the single-node release gates pass.

## Acceptance criteria per phase
See `docs/project/requirements-traceability.md` (requirement → task → test).
