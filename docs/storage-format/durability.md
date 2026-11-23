# Durability guarantees (single node, v0)

See ADR-0009 for the mode table. Additional facts operators must know:

- **Failure model covered:** process crash (tested at 15 injection points),
  OS crash / power loss in `sync` and `group_commit` modes *provided the
  storage device honours flush requests*. Power-loss behaviour is reasoned
  from the protocol, **not yet tested** with a fault-injecting filesystem.
- **Not covered:** loss or corruption of the disk/machine. Single-node
  StrataDB cannot protect against it; keep backups in a separate failure domain.
- **Platform assumptions:** atomic `rename(2)` within a directory; directory
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
  admission control is not implemented yet (KNOWN_LIMITATIONS.md).
