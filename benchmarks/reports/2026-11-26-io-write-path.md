# Write path through the I/O engine: overhead check (HW-REF-7)

## Change
These now go through `stratadb_common::io` (ADR-0027, update 2026-11-26):
- WAL appends (a positioned write at the tracked length instead of an
  `O_APPEND` write, under the WAL lock as before), WAL fsyncs, segment
  creation, rotation, torn-tail truncation and replay reads;
- segment file writes (flush, compaction, retention, upgrades);
- manifest commits;
- backup copies and verification reads.

Renames, directory fsyncs and unlinks stay in `stratadb_common::fsutil`.
The real backend creates files through `fsutil::create_new_file` and syncs
through `fsutil::sync_file`, so the power-loss recorder keeps observing
every create and fsync. Every failpoint of the crash matrix is unchanged.

## Overhead: same scenario, before → after, 3 interleaved foreground pairs
`benchmarks/scenarios/group_commit.toml` (durability `group_commit`,
adaptive delay):

| Load | Before | After |
|---|---|---|
| ingest, open loop 20/s: p50 ms | 3.92, 3.57, 3.42 | 2.59, 3.59, 3.85 |
| ingest, open loop 400/s: p50 ms | 4.57, 4.78, 4.56 | 4.87, 4.93, 5.39 |
| 1 closed-loop writer: throughput /s | 309, 303, 316 | 311, 293, 328 |
| 8 closed-loop writers: throughput /s | 2,514, 2,352, 2,440 | 2,331, 2,503, 2,546 |
| bulk load: events/s | 232k, 254k, 222k | 253k, 225k, 244k |
| final flush ms | 75.8, 79.2, 76.2 | 74.4, 80.3, 74.0 |
| `wal_append` stage, mean µs | 41.5, 43.1, 41.3 | 40.4, 42.7, 42.5 |

All within run-to-run noise; no regression is visible on this developer
laptop (no capacity claim). Raw data is in `2026-11-26-io-write-path/`.

## Correctness evidence
- **Crash matrix:** `crates/stratadb-storage/tests/crash.rs`, all 21 cases.
- **Power-loss simulation:** `power_loss_sim.rs` under the recorder.
  Example per mode: Sync, 190 crash points and 1,140 recovered states; Async,
  132 crash points and 528 states, including 29 reordered unsynced tails.
  Encrypted variants and group commit were also run. Across the five modes,
  **0 invariant violations** in 3,748 states. The recorder still observes
  every fsync and create through the engine.
- **New engine-level fault tests:**
  `crates/stratadb-storage/tests/io_write_faults.rs`
  - Short writes on WAL, segments and manifests: data intact, also after
    reopen.
  - Short reads during recovery are completed.
  - A persistent `MANIFEST` read error fails the open, naming the file.
  - WAL fsync EIO: `Indeterminate`, fail-stop, recovery keeps every
    acknowledged row, and the failed batch is all or nothing.
  - ENOSPC on a segment write: the flush fails with `resource_exhausted`;
    the engine stays healthy, the retry succeeds, and no `.tmp` is left.
  - Manifest fsync EIO: fail-stop. On reopen the old manifest plus WAL
    replay give every row and `verify` passes.
- **I/O conformance across `pread`, `sim` and `FaultyIo`:** exclusive
  create, `open_write`, `set_len`, `len`, `atomic_write`, and write faults.
- **Lint:** `clippy::disallowed_methods` now bans direct file reads *and*
  writes in the WAL, segment, storage and query crates. The only exemptions
  are test files that corrupt data on purpose and the advisory `LOCK` file.
