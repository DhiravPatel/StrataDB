# Segment formats v1, v2, v3 (provisional — ADR-0001/0003, RFC-0001, RFC-0002)

> **v3** (current) = v2 + shredded attribute columns: keys present in ≥ 10 % of a
> segment's rows (≤ 64 per scope) are stored as separate `OptValue` chunks and
> removed from the map column; the footer lists them after the column directory
> (see RFC-0002). Readers return complete maps by merging. Readers accept v1–v3.

> **v2** (written since 2026-11-21) is identical to v1 except: header version = 2,
> and each column-directory entry is followed by `u8 compression` (0 none,
> 1 LZ4 block) and `uvarint raw_len`. The chunk CRC covers the *stored*
> bytes; decompression must yield exactly `raw_len` bytes (≤ 1 GiB). Chunks
> are stored uncompressed when LZ4 saves < 1/8. Readers accept v1 and v2.

```text
[0..16)   header: magic "STRSEG\r\n" | version u16 = 1 or 2 | flags u16 = 0 | reserved u32 = 0
[16..F)   column chunks, contiguous, each CRC-32C'd (CRC stored in the footer)
[F..T)    footer (encoding below)
[T..T+16) trailer: footer_len u32 | footer_crc u32 | magic "STRSEG\r\n"
```

## Footer
```text
u64 segment_id | bytes tenant_id | uvarint row_count
ivarint min_event_time, max_event_time | uvarint min_seq, max_seq
uvarint min_schema_version, max_schema_version | ivarint created_ms
uvarint ncols, then per column:
  u16 column_id | u8 encoding | uvarint offset | uvarint len | u32 crc | uvarint missing
  [v2 only: u8 compression | uvarint raw_len]
keyset attribute_keys | keyset resource_keys     (u8 0 + uvarint n + n strings | u8 1 = overflow)
uvarint n + n × (u8 severity, uvarint count)
uvarint n + n × (u16 tag, u8 required, bytes data)   extensions
```
Readers reject unknown column ids, encodings, and **required** extensions;
optional extensions are ignored.

### Extension tag registry
| Tag | Required | Name | Contents |
|-----|----------|------|----------|
| 1 | no | idempotency keys | `uvarint n` × (`bytes key`, `32 B request SHA-256`, `uvarint commit_seq`, `uvarint accepted`, `ivarint committed_ms`) for batches whose rows are in this segment |
| 2 | no | Bloom filters | see `crates/stratadb-segment/src/bloom.rs` (version byte, per-field filters over string values, hash v1 = FNV-1a-64 + SplitMix64 double hashing, k = 7, ≈ 9.6 bits/value) |

## Columns (id → encoding)
| id | column | encoding |
|----|--------|----------|
| 1 | event_time | 1 DeltaVarint |
| 2 | ingest_time | 1 DeltaVarint |
| 3 | seq | 1 DeltaVarint |
| 4 | idx | 1 DeltaVarint |
| 5 | severity | 2 RawU8 |
| 6 | service | 4 DictStr |
| 7 | severity_text | 4 DictStr |
| 8 | trace_id | 3 OptStr |
| 9 | span_id | 3 OptStr |
| 10 | event_id | 3 OptStr |
| 11 | body | 5 OptValue |
| 12 | attributes | 6 Map |
| 13 | resource_attributes | 6 Map |
| 14 | schema_version | 1 DeltaVarint |

DeltaVarint: per row `ivarint(v_i − v_{i−1})` (wrapping, v_{−1} = 0).
OptStr: per row `uvarint(len+1)` + bytes, or `0` = missing. DictStr: `uvarint n`,
n strings, then per row `uvarint code` (0 = missing, k = dict[k−1]).
OptValue: per row `u8` presence + value. Map: per row event-encoding map.

## Invariants
Single tenant per segment; rows strictly ordered by `(event_time, seq, idx)`;
footer statistics equal data (checked by `verify`). Written via tmp + fsync +
rename + dir fsync ("prepared"); visible only once a durable manifest
references it ("committed").
