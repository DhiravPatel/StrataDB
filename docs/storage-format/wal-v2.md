# WAL format v2

> Since 2026-11-26 new segments are written as [v3](wal-v3.md), which keeps
> this layout and adds columnar record types. v2 is still written when
> `[storage] wal_format_version = 2` is set (rollback window).

Written since StrataDB 0.0.1 (2026-11-25). Readers accept v1 and v2
([wal-v1.md](wal-v1.md)); a segment's version is in its header and fixes the
frame layout for the whole segment. Golden fixture: `tests/fixtures/wal-v2/`
(`crates/stratadb-wal` test `reads_wal_v2_fixture`).

## Segment header (32 bytes)
Identical to v1 except `format_version` = `2`.

## Frame (25-byte header + payload, no padding/alignment)
| Offset | Size | Field | Notes |
|--------|------|-------|-------|
| 0 | 4 | crc | CRC-32C of `payload ‖ frame bytes 4..25` |
| 4 | 4 | payload_len | ≤ 256 MiB (format limit); ≤ configured `max_record_bytes` on write |
| 8 | 1 | record_type | 1 = EventBatch, 2 = IdempotentEventBatch |
| 9 | 8 | seq | strictly `+1` per frame within a segment |
| 17 | 8 | synced_through | every `seq ≤ synced_through` had been fsynced when this frame was written; `< seq` |
| 25 | n | payload | |

## Why `synced_through`
After power loss, bytes written but not yet fsynced may be lost, torn at
sector granularity, or persisted **out of order** (a later sector survives
while an earlier one is lost). v1 recovery could not distinguish such an
un-synced tail followed by surviving frames from damage to acknowledged
records, so it failed closed and an ordinary power cut could require operator
intervention (`ForceTruncate`). Found by the power-loss simulation
(`crates/stratadb-storage/tests/power_loss_sim.rs`).

An fsync makes *all* previously written bytes of the file durable. So if any
frame after the damage records `synced_through ≥ k` (k = the expected sequence
number at the damage), record k had been fsynced and power loss cannot explain
the damage.

## Recovery rules
As v1 ([wal-v1.md](wal-v1.md) §Recovery rules), except rule 5. For the first
invalid frame in the **final** segment, with expected sequence `k`
(`format::classify_invalid_tail`):

| What follows | v2 classification | Action |
|---|---|---|
| no checksum-valid frame with `seq ≥ k` | `Torn` | truncate at the damage, fsync |
| valid frames, all with `synced_through < k` | `UnsyncedReordered` | truncate at the damage, fsync; reported (`Truncation.kind`, `discarded_valid_frames = true`) |
| a valid frame with `synced_through ≥ k` | `PossiblyDurableDamage` | fail with `Corruption` unless `ForceTruncate` |

v1 segments keep the v1 rule (any later valid frame ⇒ `PossiblyDurableDamage`).

Nothing acknowledged under `sync` / `group_commit` is discarded by the
`UnsyncedReordered` rule: acknowledgement happens after the fsync covering the
record, and every frame written after that fsync carries `synced_through ≥` its
sequence number. Under `async`, records after the last fsync are inside the
documented loss window.

Remaining blind spot (unchanged from v1): damage to the *last* acknowledged
record(s) when no later frame was written is indistinguishable from a torn
tail and is truncated. Segment and manifest checksums are unaffected.

## Appending after upgrade
A v1 final segment is replayed but never appended to: recovery starts a new v2
segment at the next sequence number. Downgrading to a release that only reads
v1 is not supported once a v2 segment exists (it reports `FormatVersion`).
