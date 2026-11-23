# RFC-0002: Segment format v3 — shredded attribute columns

## Status
Accepted (implemented 2026-11-21)

## Context
Every benchmark run (baseline, Bloom A/B, compression A/B) shows attribute
predicates and group-bys (queries 02, 07, 08, 10) dominating latency: the
executor decodes the whole per-row attribute map chunk to read one key.

## Decision
Segment format **v3**. At write time, per segment and per scope (attributes,
resource attributes), keys present in ≥ 10 % of rows (max 64 per scope,
ties by key) are *shredded*: each gets its own column chunk with encoding
`OptValue` (per row: 0 = missing, 1 = present + typed value — so missing and
null stay distinct). The map column for that scope stores the remaining keys
only. The v3 footer appends, after the column directory:

```text
uvarint n | n × (u8 scope (0 attr, 1 resource) | bytes key | uvarint offset |
                 uvarint len | u32 crc | uvarint missing | u8 compression | uvarint raw_len)
```

Readers: `read_column(Attributes|ResourceAttributes)` and `read_rows` return
**complete** maps (residual map merged with shredded columns), so all
existing callers stay correct. `read_shredded(scope, key)` returns one key's
values when that key is shredded. The query executor reads only the
shredded chunks when every referenced key of a scope is shredded in that
segment and the whole map is not referenced; otherwise it reads merged maps.

## Compatibility
Readers accept v1, v2, v3. A v2 reader rejects v3 (`Unsupported`) rather than
silently returning maps without the shredded keys. Compaction upgrades older
segments. Footer bounds/CRC rules apply to shredded chunks identically.

## Testing / evidence
Round trip with and without shredding is identical; every-byte-flip
detection; missing vs null preserved; golden v1 fixture; optimizer
equivalence property tests; A/B report `2026-11-21-shredding-ab.md`.
