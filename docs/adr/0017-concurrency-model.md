# ADR-0017: Storage engine concurrency model

- **Status:** Accepted
- **Spec references:** §5.3, §9.4

## Decision
- The storage engine is synchronous (std threads, blocking file I/O). The
  network server will call it via a bounded blocking pool. Rationale: file
  I/O is blocking on all supported platforms anyway; synchronous code makes
  durability ordering explicit and testable; no locks are held across `.await`.
- WAL group commit: leader/follower over a mutex + condvar; one fsync in
  flight; durable target captured before the sync.
- **Write gate** (`RwLock<()>`): writers hold it shared across WAL append +
  memtable insert; memtable freeze takes it exclusively. This guarantees the
  freeze boundary `wal.last_seq()` covers every assigned sequence number,
  preventing the "flushed_through skips an in-flight record" data-loss race.
- Lock order: write_gate → state → memtable. Manifest mutex serializes flushes.
- Snapshots copy `Arc` handles under the state lock; flush swaps frozen
  memtable → segments under the same lock, so no row is ever double-visible
  or missing (concurrency test checks both).
