# ADR-0009: Durability modes and acknowledgement semantics

- **Status:** Accepted
- **Spec references:** §2.1, §3.3, §8.3, §8.4
- **Operator documentation:** [docs/storage-format/durability.md](../storage-format/durability.md)

| Mode | Ack means | Survives | Does **not** survive |
|------|-----------|----------|----------------------|
| `async` | WAL frame written to OS page cache | process crash | OS crash, power loss (until next sync, default ≤1 s) |
| `sync` | WAL frame covered by a successful fsync (`F_FULLFSYNC` on macOS) | process crash, OS crash, power loss* | loss of the disk/machine |
| `group_commit` (default) | same as `sync`; leader waits ≤ `max_delay` (2 ms) to share one fsync | same as `sync` | same as `sync` |
| `replicated` | not implemented (distributed phase) | — | — |

\* Assuming storage that honours flush commands.

Receipts carry `commit_seq` and `level` (`wal_written` | `wal_synced`). Rows
are query-visible when the write returns. Errors are explicit: `Indeterminate`
means the outcome is unknown (retry with an idempotency key).
