# ADR-0002: WAL record format and recovery policy

- **Status:** Accepted
- **Date:** 2026-11-20
- **Spec references:** §8.1–§8.6, ticket C-001..C-008
- **Normative format:** [docs/storage-format/wal-v1.md](../storage-format/wal-v1.md)

## Decision
- Segment files `wal-<base_seq:020>.log` with a 32-byte checksummed header,
  created via tmp + fsync + rename + directory fsync.
- Frames: `crc32c | len u32 | type u8 | seq u64 | payload`, little-endian. The
  CRC covers payload **and** length/type/seq, so a corrupted length is detected.
- **One frame = one atomic commit.** An event batch is one frame, so batch
  atomicity needs no separate commit marker. Sequence numbers are dense (+1).
- Checksum: CRC-32C (hardware-accelerated on aarch64/x86_64; RFC 3720 vectors tested).
  It detects accidental corruption only — not tampering.
- Recovery policy `TruncateTornTail` (default): an invalid frame in the *final*
  segment is truncated **only if no checksum-valid frame follows**. If valid
  frames follow (mid-log damage), recovery fails closed. Invalid frames in
  non-final segments always fail. `ForceTruncate` is an explicit operator override.
- Continuity: overlap is always an error; a gap is allowed only if every
  missing sequence is `<=` the manifest's `flushed_through_seq`.
- If the WAL ends below `flushed_through_seq` (possible in `Async` mode after
  power loss), a new segment starts at `flushed_through_seq + 1` so sequence
  numbers never repeat.
- After any write/fsync error the WAL is **poisoned** (fsyncgate): no retries.
- Reclamation deletes closed segments oldest-first, only up to a boundary the
  caller proves durable via the manifest; never the active segment.

## Alternatives rejected
- Per-record commit markers: redundant with frame checksums for single-frame commits.
- xxHash/CRC32 (IEEE): CRC-32C has better hardware support and is the storage-industry default.
- Truncate-at-first-error unconditionally (RocksDB "tolerate corrupted tail"):
  can silently discard acknowledged data after mid-log corruption.

## Evidence
`crates/stratadb-wal`: every-bit-flip detection, truncation at every byte
offset, mid-log corruption fail-closed, gap/overlap detection, concurrent
Sync/GroupCommit, poison-on-fsync-failure; `stratadb-storage/tests/crash.rs`
process-crash matrix.

## Update 2026-11-25: format v2 (`synced_through`)
Power-loss simulation showed that out-of-order persistence of un-synced
appends (an earlier frame lost, a later one surviving) made v1 recovery fail
closed after an ordinary power cut (`async`, or `group_commit` with several
frames in flight). v2 adds `synced_through` (8 bytes) to the frame header so
recovery can prove such a tail was never fsynced and truncate it, while still
failing closed when a later frame proves the damaged record had been synced.
Alternatives rejected: unconditional truncate-at-first-error (silently loses
acknowledged records after media damage); separate sync-marker records (extra
writes, consume sequence numbers); a watermark file (an extra fsync per
group). Spec: docs/storage-format/wal-v2.md.

## Revisit conditions
Encryption at rest (ADR pending, §18.6) will require a v2 frame or segment-level envelope.
