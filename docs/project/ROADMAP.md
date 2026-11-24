# Roadmap

Order (spec §40): correct single-node storage → useful query engine →
measured indexes → security & operations → distributed replication → scale.

| Release | Scope | State |
|---------|-------|-------|
| Phase 0 | ADRs, formats, threat model, test strategy | mostly done (benchmark generator pending) |
| Phase 1 / 0.1 core | WAL, memtable, flush, segments, manifest, recovery | implemented & tested (power-loss sim pending) |
| Phase 2 | query engine | implemented & tested (v0 subset) |
| Phase 3 | bloom/exact/inverted indexes, benchmarks, Parquet evaluation | not started |
| Phase 4 | HTTP + OTLP ingestion, SDKs, idempotency | not started |
| Phase 5 / 0.2 | auth, RBAC, TLS, audit, retention, backup/restore, metrics, Docker | not started |
| Phase 6 | hardening, fuzzing, upgrade tool, compaction | not started |
| Phases 7–8 | distributed design and implementation | not started (by design) |
| 1.0 | all release gates | — |
