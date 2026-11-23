# ADR-0011: Distributed metadata and consensus

- **Status:** Deferred (spec §20: not before single-node durability is stable)

Direction only: use a reviewed Raft implementation (e.g. `openraft`) for
cluster metadata rather than a novel protocol; decide after the Phase 7
failure-model RFC and deterministic simulation harness exist.
