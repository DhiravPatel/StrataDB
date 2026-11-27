# WAL format v3

v3 is written since 2026-11-26. Readers accept v1–v3. Golden fixtures:
- `tests/fixtures/wal-v3/`: frames of all four record types
  (`crates/stratadb-wal` test `reads_wal_v3_fixture`);
- `tests/fixtures/storage-wal-v3/`: a data directory with unflushed columnar
  records (`crates/stratadb-storage/tests/compat_wal_v3.rs`).

## Differences from v2
- **The same layout as [v2](wal-v2.md):** header, 25-byte frame header,
  `synced_through`, encryption and recovery rules are identical.
- **`format_version`** in the header is `3`.
- **Two new record types**, valid only in v3 segments:

| Type | Payload |
|------|---------|
| 1 `EventBatch` | row batch ([event-encoding-v1](event-encoding-v1.md), `uvarint n` + n length-prefixed events) |
| 2 `IdempotentEventBatch` | idempotency header + row batch |
| 3 `ColumnarEventBatch` | [columnar batch v1](event-batch-columnar-v1.md) |
| 4 `IdempotentColumnarEventBatch` | idempotency header + columnar batch v1 |

  Writers of v3 segments emit types 3 and 4. Types 1 and 2 remain valid in
  v3 segments and are read.
- **Why a version bump:** the new record types change what a segment may
  contain. A release that reads only v1–v2 rejects a v3 segment at its
  header with `FormatVersion`, before replay, rather than partway through a
  replay with `Unsupported`.

## Rules
- **A type 3 or 4 frame in a v1 or v2 segment** that passes its checksum was
  not written by StrataDB. Replay fails with `Corruption`.
- **The write version** is `WalOptions::format_version`, `[storage]
  wal_format_version`: 3 by default, or 2.
  - With 2, only types 1 and 2 are written, in v2 segments. Use it for a
    rollback window after upgrading, while the previous release must still
    be able to read the WAL.
  - A segment is only appended to if its version equals the write version.
    Otherwise recovery starts a new segment at the next sequence number (as
    for v1 → v2), so one segment never mixes versions.
- **Downgrade:** once a v3 segment exists, a release that reads only v1–v2
  cannot open the directory until that segment has been flushed and
  reclaimed. To keep a rollback possible, run with `wal_format_version = 2`
  until the upgrade is final.
