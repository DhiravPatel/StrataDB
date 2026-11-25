# Durability guarantees (single node, v0)

See ADR-0009 for the mode table. Additional facts operators must know:

- **Failure model covered:** process crash (tested at every crash point in
  docs/testing/crash-matrix.md), OS crash / power loss in `sync` and
  `group_commit` modes *provided the storage device honours flush requests*.
  Power loss is tested by simulation (`tests/power_loss_sim.rs`, below).
- **Power-loss model (simulated):** after power loss, bytes written but not
  fsynced may be lost, torn at 512-byte sector granularity, or persisted out of
  order; unwritten regions read as zeros; a file's size may revert; creates,
  renames and unlinks not followed by a directory fsync may be lost, but a
  rename is atomic (old or new name, never neither). An fsync of a file makes
  all of its previously written bytes durable. WAL v2 records the fsync
  watermark in every frame so recovery can prove an out-of-order tail was never
  synced (docs/storage-format/wal-v2.md).
- **Not covered:** loss or corruption of the disk/machine. Single-node
  StrataDB cannot protect against it; keep backups in a separate failure domain.
- **Platform assumptions:** 512-byte sector writes are atomic; atomic `rename(2)` within a directory; directory
  fsync makes create/rename/unlink durable; on macOS Rust's `sync_all` /
  `sync_data` issue `F_FULLFSYNC`. Supported filesystems: APFS, ext4, XFS.
- **Write errors:** any WAL write/fsync failure or manifest commit failure puts
  the engine in a *failed* state (writes rejected with `unavailable`) until
  restart; recovery then establishes the true durable state.
- **Visibility:** a write is query-visible when acknowledged. A query sees a
  snapshot: committed segments + memtable rows at the snapshot instant, with
  no row missing or duplicated across a concurrent flush.
- **Disk full:** surfaces as `resource_exhausted` (retryable) on the write
  that hit it, and poisons the WAL (restart required). Disk high-watermark
  admission control is not implemented yet (docs/project/KNOWN_LIMITATIONS.md).
