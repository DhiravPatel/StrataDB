# Crash-point matrix (spec §26.5)

Workload: 60 batches × 3 events (each with an idempotency key), `sync`
durability, 2 KiB WAL segments, flush every 7 batches, compaction every 14.
After recovery every batch is retried with its key: acknowledged batches must
be reported as duplicates and the final data must contain each batch once. Child process terminates at the failpoint; the parent
recovers and checks: all acknowledged batches present exactly once; batches
all-or-nothing; only the in-flight batch may additionally appear; recovery
idempotent; database writable and verifiable afterwards.

| Spec crash point | Failpoint | Expected result | Status |
|------------------|-----------|-----------------|--------|
| Before WAL append | `wal::before_write` | in-flight batch absent | ✅ tested |
| During WAL append | `wal::torn_write` (half frame written) | torn tail truncated; batch absent | ✅ tested (early + late) |
| After append before sync | `wal::before_sync` | batch may appear (in page cache) | ✅ tested |
| After sync before response | `wal::after_sync` | batch present, not acked (allowed) | ✅ tested |
| After response before memtable update¹ | `engine::after_wal_commit` | batch present via WAL replay | ✅ tested |
| WAL rotation | `wal::rotate::after_create` | empty new segment accepted | ✅ tested |
| During flush (after freeze) | `flush::after_freeze` | rows replayed from WAL | ✅ tested |
| During segment write | `segment::torn_write` | `.tmp` removed; rows from WAL | ✅ tested |
| After segment write before manifest | `flush::after_segment_write` | orphan segment deleted; rows from WAL | ✅ tested |
| During manifest update (tmp written) | `manifest::before_rename` | old manifest used; orphan deleted | ✅ tested (first & later flush) |
| After rename before dir fsync | `manifest::after_rename` | new or old manifest, both consistent | ✅ tested² |
| After manifest commit before WAL reclaim | `flush::after_manifest_commit` | flushed WAL records skipped | ✅ tested |
| During WAL reclamation | `wal::reclaim::after_delete` | remaining old segments skipped | ✅ tested |
| During compaction: output written, before manifest | `compaction::after_output_write` (1st and 2nd compaction) | output deleted as orphan; inputs authoritative | ✅ tested |
| Compaction manifest committed, before visibility swap | `compaction::after_manifest_commit` | output committed; inputs deleted on open | ✅ tested |
| During obsolete-file deletion | `compaction::after_delete_obsolete` | remaining obsolete files deleted on open | ✅ tested |
| During retention deletion | (same `rewrite` primitive and failpoints as compaction) | as above | ✅ covered via shared path |
| During backup | — | — | ⛔ not implemented |

¹ The engine acknowledges *after* memtable insert, so this point is "after
durable commit, before ack + visibility".
² With process crashes the rename is already visible; the "old manifest"
branch is covered by the power-loss simulation below (states captured before
the directory fsync that follows the manifest rename).

## Power-loss model (partial) — `crates/stratadb-storage/tests/power_loss.rs`
Model: WAL frames with `seq > durable_seq` are discarded from an image of the
data directory (unsynced page cache lost); segments and manifest are fsynced
before being referenced. Checks per mode:

| Mode | Expected | Status |
|------|----------|--------|
| `sync` | every acknowledged batch survives | ✅ tested |
| `group_commit` | every acknowledged batch survives | ✅ tested |
| `async` | flushed / synced batches survive; WAL may end below the manifest boundary and recovery must not reuse sequence numbers | ✅ tested (asserts the WAL-behind-manifest path is exercised) |

The simulation below supersedes this model; it is kept as a fast regression test.

## Power-loss simulation — `crates/stratadb-storage/tests/power_loss_sim.rs`
Recorder: `stratadb_common::simfs` (feature `simfs`, test builds only). Every
data-file fsync (`fsutil::sync_file`) and directory fsync (`fsutil::fsync_dir`)
records the durable state; a crash image is captured immediately before and
after each of them. Every image is recovered as: only-synced state; full
state (process crash); and random states in which each un-synced 512-byte
sector is new or old/zero (lost, torn or reordered writes), file sizes revert,
and un-synced creates/renames/unlinks are lost (atomically per connected
component, so renames — including replace-renames — stay atomic). Model and
assumptions: docs/storage-format/durability.md.

Workload: 30 idempotent batches × 3 events, 2 KiB WAL segments (rotation every
few batches), flush every 5 batches, compaction every 12. Invariants per
state: the engine opens without operator action; acknowledged (`sync`,
`group_commit`) or flushed (`async`) batches present exactly once; batches
all-or-nothing; nothing not yet written appears; `async` survivors form a
prefix; sampled states (1 in 7) also retry every acknowledged batch with its
key (must be a duplicate), write, flush and reopen.

| Mode | Crash points | Recovered states | Result |
|------|--------------|------------------|--------|
| `sync` | 190 | 1 140 | ✅ 0 violations |
| `group_commit` | 190 | 760 | ✅ 0 violations |
| `async` | 132 | 528 | ✅ 0 violations (28 states need the WAL v2 reordered-tail rule; asserted > 0) |

Found and fixed by this simulation: with WAL v1, an un-synced tail persisted
out of order (earlier frame lost, later frame survived) made recovery refuse
to open — 28 of 528 `async` states (docs/storage-format/wal-v2.md).

Sensitivity (each mutation run once, then reverted): removing the directory
fsync after the manifest rename → 724 violating states; removing the segment
file fsync before publishing → 1 432; acknowledging group commits without the
WAL fsync → 579 (`sync` + `group_commit`).

Not modelled: lying hardware (flushes ignored), bit rot, sub-sector tearing,
cross-file write ordering beyond fsync boundaries is modelled but not
exhaustively enumerated (random sampling, 2–4 variants per crash point).

## Format compatibility — `crates/stratadb-storage/tests/compat.rs`
Golden v1 data directory in `tests/fixtures/storage-v1/` (2 tenants, 2 segments,
6 unflushed WAL rows, all value types). Must decode exactly; never regenerate
to fix a failure.
