# Testing strategy

| Layer | Where | Run |
|-------|-------|-----|
| Unit + property tests | `src/**` `#[cfg(test)]` (proptest) | `cargo test --workspace` |
| WAL error injection | `crates/stratadb-wal/tests/failpoints.rs` | default suite |
| Engine integration | `crates/stratadb-storage/tests/engine.rs` | default suite |
| Process-crash matrix | `crates/stratadb-storage/tests/crash.rs` | default suite (~3 s) |
| Fuzzing | planned: `fuzz/` with cargo-fuzz (needs nightly; ADR pending) | — |
| Benchmarks | planned: `benchmarks/` | — |

Failpoints (`stratadb_common::failpoint`) compile only with the `failpoints`
feature, which is enabled for test builds via dev-dependencies and must never
be enabled in release builds. Configure with `STRATADB_FAILPOINTS="name=action[@n];…"`.
Actions: `exit`, `abort`, `error`, `panic`, `off`.

Every bug found becomes a regression test. Long stress tests must be
`#[ignore]` and run with `cargo test -- --ignored`.

See [crash-matrix.md](crash-matrix.md) for the expected result of every crash point.
