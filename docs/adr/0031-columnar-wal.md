# ADR-0031: WAL v3, columnar batch payloads

- **Status:** Accepted
- **Spec references:** STRATA-SYS-07-002; Appendix E (format changes need a
  version bump, docs and fixtures); ADR-0015 (compatibility fixtures)

## Context
WAL payloads were row batches: every event encoded on its own (event encoding
v1), repeating the tenant id, full timestamps and repeated strings in every
row. On the app-logs profile that is about 232 bytes of WAL per event. The
spec requires the columnar-encoded batch, LZ4-compressed when the batch is
16 KiB or larger.

## Decision
### The columnar batch (`stratadb_schema::columnar`)
- **Layout:** a version byte, flags (LZ4), the row count, then a
  column-major body:
  - the tenant once, since a batch has exactly one tenant;
  - delta-coded `event_time` and `ingest_time`;
  - severities and schema versions;
  - dictionary-coded `service` and `severity_text`;
  - length-prefixed ids;
  - then bodies and maps with the existing value codec.
- **Compression:** the body is LZ4-compressed (`lz4_flex`, safe mode, already
  a dependency) when it is 16 KiB or larger, and only kept if smaller.
- **Decoding validation:** as strict as the row codec, plus canonical
  dictionaries and allocation bounds checked before allocating: rows against
  body size, and the decompressed size against 255 × the compressed size.
- **Normative description:** `docs/storage-format/event-batch-columnar-v1.md`.

### WAL v3 (`stratadb_wal::format`)
- **Version 3:** the v2 frame layout and recovery rules, plus record types
  3 (`ColumnarEventBatch`) and 4 (`IdempotentColumnarEventBatch`).
- **Why a version bump** for what is only a new record type: so that an
  older release rejects the directory at the segment header with
  `FormatVersion`, rather than partway through replay.
- **Corruption:** a checksum-valid type 3 or 4 frame in a v1 or v2 segment
  fails replay with `Corruption`.
- **Rollback window:** `WalOptions::format_version`, `[storage]
  wal_format_version`, is 3 by default or 2.
  - With 2 the engine writes row records in v2 segments, readable by the
    previous release, as documented in `runbooks/upgrade.md`.
  - The WAL refuses to append a record type its write version cannot hold.
  - A segment is never appended to across versions: recovery starts a new
    segment, as for v1 → v2.

### Engine
- **Writes:** the engine writes type 3/4 records in v3 and type 1/2 in v2.
- **Replay:** reads all four types.
- **Idempotency:** the record wraps the columnar batch unchanged, so keys
  survive restarts as before.

### Visibility
- `WalStats.bytes_appended` / `records_appended` and
  `strata_wal_bytes_total` / `strata_wal_records_total`.
- The bench reports `wal_bytes_per_event`, and the cost ledger gains
  encode/decode ops for both formats.

### Evidence
- **Format checks:**
  - golden fixtures `tests/fixtures/wal-v3` (frames) and
    `tests/fixtures/storage-wal-v3` (a data directory with plain, LZ4 and
    idempotent columnar records);
  - upgrade tests from the v1 fixture and from v2;
  - the rollback-window test;
  - property and damage tests of the codec;
  - fuzzing through WAL recovery and the event-codec target, with a
    re-encode check.
- **Unchanged:** the crash matrix, power-loss simulation and existing
  fixtures, now all running on v3.

## Consequences
Measured in 3 interleaved foreground A/B pairs (report
`2026-11-26-columnar-wal`):
- **Bytes:** WAL bytes per event fall from 232.5 to 69.8 (3.3×).
- **CPU:** ingest CPU per event rises about 19% (855 → about 1,040 ns).
  - The cost ledger attributes it to encoding: 184 ns/row against 90 for
    rows, of which LZ4 is about 65–70 ns.
  - Replay is slightly faster: 785 against 807 ns/row.
- **Commit latency:** on this fsync-latency-bound laptop, throughput and
  commit latency are unchanged within noise. The byte saving matters where
  WAL bandwidth or fsync size does: HW-REF-1 NVMe at high ingest rates. It is
  not yet measured there.
- **Downgrades:** a directory with a v3 WAL segment cannot be opened by
  releases that read only v1–v2. Operators who need a rollback run with
  `wal_format_version = 2` until the upgrade is final.
- **Not yet:**
  - replay decodes into rows, because the memtable is row-based. Decoding
    straight into a columnar memtable is A1 (phase 11), which completes
    SYS-07-002;
  - encoding and LZ4 run on the writer's thread before the WAL lock and the
    write gate, so they add CPU but never extend the time the WAL is held.

## Alternatives rejected
- **A version byte inside the payload instead of new record types:** old
  payloads have no version byte, so the two could not be told apart without
  the record type.
- **Compressing every batch:** small batches gain little and pay a fixed
  cost. The spec's 16 KiB threshold is kept.
- **zstd:** better compression, but a new native dependency and more CPU.
  LZ4 is what the spec names, and it is already used by segments.
