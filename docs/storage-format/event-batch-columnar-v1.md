# Columnar event batch v1

The payload of WAL record types 3 and 4 ([wal-v3](wal-v3.md)), spec
STRATA-SYS-07-002. Code: `crates/stratadb-schema/src/columnar.rs`. Varints
are canonical LEB128, and signed varints are zig-zag (as in
[event-encoding-v1](event-encoding-v1.md)).

```text
u8      version = 1
u8      flags                 bit 0 = LZ4; other bits must be 0
uvarint n                     rows, 1..=max_items (default 2^20)
if LZ4:
  uvarint raw_len             length of the decompressed body
  bytes   lz4 block           the rest of the payload (LZ4 block format)
else:
  bytes   body                the rest of the payload
```

`body` holds the columns in this order, each for rows `0..n`:

```text
bytes   tenant_id             once: every event of a batch has this tenant
n x ivarint event_time delta  first relative to 0, then to the previous row (wrapping)
n x ivarint ingest_time delta same
n x u8      severity          0, 1, 5, 9, 13, 17, 21
n x uvarint schema_version    < 2^32
dict    service
dict    severity_text
n x ostr    trace_id
n x ostr    span_id
n x ostr    event_id
n x optval  body              u8 0 = missing | u8 1 + value
n x map     attributes
n x map     resource_attributes
```

- **`dict`:** `uvarint d` (≤ n), then d distinct strings (`bytes`), then
  n × `uvarint code`, where 0 is missing and k is entry k − 1. Every entry is
  used at least once (canonical).
- **`ostr`:** `uvarint 0` = missing, or `uvarint len + 1` followed by `len`
  UTF-8 bytes.
- **`value`, `map`:** as in event-encoding-v1. Map keys are strictly
  increasing.

## Writing
- The body is compressed when it is at least 16 KiB, and the compressed form
  is kept only if it is smaller.
- A batch holds exactly one tenant. The engine refuses mixed batches before
  encoding.

## Reading
A decoder must reject the following, each a `Corruption` error, or
`FormatVersion` for the version byte, and must never panic:
- an unknown version or flags;
- n = 0, or n above the limit;
- more rows than the body could hold: at least 10 bytes per row;
- a compressed body whose declared `raw_len` exceeds 255 × the compressed
  length (+16) or the decoder's limit;
- a decompressed length that differs from `raw_len`;
- invalid UTF-8, tenant, severity, value tag or map order;
- dictionary codes out of range, and unused or duplicate entries;
- trailing bytes in the body or in the payload.

Decoding allocates per-row vectors only after the row count has been checked
against the body length.
