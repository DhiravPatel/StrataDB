# WAL format v1

> Read-only since 2026-11-25: new segments are written as [v2](wal-v2.md),
> which adds `synced_through` to each frame and refines recovery rule 5.

## Segment header (32 bytes)
| Offset | Size | Field | Notes |
|--------|------|-------|-------|
| 0 | 8 | magic | `53 54 52 57 41 4C 0D 0A` (`STRWAL\r\n`) |
| 8 | 2 | format_version | `1` |
| 10 | 2 | flags | must be 0 |
| 12 | 8 | base_seq | sequence of the first frame; equals the file name |
| 20 | 8 | created_ms | i64 Unix ms (informational) |
| 28 | 4 | header_crc | CRC-32C of bytes 0..28 |

## Frame (17-byte header + payload, no padding/alignment)
| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | crc = CRC-32C(payload ‖ frame bytes 4..17) |
| 4 | 4 | payload_len (≤ 256 MiB format limit; writer default limit 32 MiB) |
| 8 | 1 | record_type (`1` = EventBatch) |
| 9 | 8 | seq (u64) |
| 17 | payload_len | payload |

Record types:
- `1` EventBatch — payload: `uvarint count` then `count` × (`uvarint len`, event-encoding-v1 bytes).
- `2` IdempotentEventBatch — payload: `u8 version = 1`, `bytes key` (1–128 visible
  ASCII), `32 bytes request SHA-256`, `ivarint committed_ms`, then an EventBatch
  payload. Defined in `crates/stratadb-storage/src/idempotency.rs`.

## Recovery rules
1. Remove `*.tmp`. List `wal-<20 digits>.log`, sort by base_seq.
2. A header must be valid (magic, CRC, version, flags 0, base_seq = name).
3. Within a segment, frame seqs are exactly `base_seq, base_seq+1, …`. A
   checksum-valid frame with the wrong seq is corruption.
4. Between segments: base < previous next ⇒ overlap error. Missing sequences
   are allowed only if all are ≤ the manifest's `flushed_through_seq`.
5. First invalid frame in the **final** segment: if any checksum-valid frame
   with `seq ≥ expected` exists later in the file ⇒ fail (`Corruption`),
   unless the operator selects `ForceTruncate`. Otherwise truncate there and fsync.
6. Invalid frame in a non-final segment ⇒ fail.
7. Unknown record type with a valid checksum ⇒ `Unsupported` (newer writer).
8. Records with `seq ≤ flushed_through_seq` are skipped (already in segments).
9. Next sequence = `max(last frame seq, flushed_through_seq) + 1`; if that is not
   contiguous with the final segment, a new segment is created.

## Rotation
Before rotation the active segment is fsynced. The new segment is created as
`.tmp`, header written and fsynced, renamed, and the directory fsynced.

## Reclamation
Closed segment *i* covers `[base_i, base_{i+1} − 1]`. It may be deleted once
`base_{i+1} − 1 ≤ flushed_through_seq` of a **durable** manifest. Deletion is
oldest-first, followed by a directory fsync. The active segment is never deleted.
