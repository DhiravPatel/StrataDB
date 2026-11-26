# Roadmap

Order (spec §40): correct single-node storage → useful query engine →
measured indexes → security and operations → performance engineering
(Parts IV–IX) → distributed replication → scale. Detailed state per
requirement: IMPLEMENTATION_STATUS.md (Parts I–III) and PERF_INVENTORY.md
(Parts VIII–IX).

| Release | Scope | State |
|---------|-------|-------|
| Phase 0 | ADRs, formats, threat model, test strategy, dataset generator | done |
| Phase 1 / 0.1 core | WAL, memtable, flush, segments, manifest, recovery, power-loss simulation | done (tested) |
| Phase 2 | query engine (StrataQL, pruning, budgets, pagination, vectorized filters, parallel scans) | done (tested) |
| Phase 3 | Bloom and trigram filters with A/B evidence; postings deferred (ADR-0005) | done, except postings |
| Phase 4 | HTTP and OTLP ingestion, SDKs, idempotency | done (tested) |
| Phase 5 / 0.2 | auth, RBAC, TLS, audit, retention, backup/restore, metrics, encryption at rest | done (tested); Docker image build unverified locally |
| Phase 6 | hardening: fuzzing, upgrade tool, compaction, doctor, index lifecycle | done; longer fuzz campaigns pending |
| **Phase 9 (spec P0)** | measurement system: scenario harness, manifests, open-loop load, regression detection, stage metrics, cost ledger, build profiles, release declaration | **done** (mechanisms; gates need a dedicated runner) |
| Phase 10 (P1 + A0) | `doctor host`, bounded worker pool (no thread per query), thread inventory, `IoEngine` + sim backend, memory tracker, adaptive group commit and columnar WAL payload, layering check, `ScanSource` seam, query log | next |
| Phase 11 (A1 + P2) | typed column vectors, columnar memtable, push pipelines and morsels, scalar kernel library, hash-table family, codec selection, scheduler classes and admission, catalog snapshots, state-machine metrics | planned |
| Phase 12 (A2 + P3) | projections and MVs, `IndexReader`, layout (typed JSON, LowCardinality), optimizer, sort/join/spill, caches, native protocol | planned |
| Phase 13 (A3) | result reuse, prepared statements, accelerator interfaces | planned |
| Phases 7–8 / 14 (P4 + A4) | distributed design and implementation | not started (by design; gated on single-node level L2) |
| 1.0 | all release gates | — |

The phase mapping and the conflicts it resolves (the `unsafe` boundary,
io_uring, naming, hardware) are in ADR-0026.
