# ADR-0003: Segment layout v1

- **Status:** Accepted (provisional — see ADR-0001)
- **Date:** 2026-11-20
- **Spec references:** §10, §11, §12.2
- **Normative format:** [docs/storage-format/segment-v1.md](../storage-format/segment-v1.md)

## Decision
- One segment = one tenant (storage-level tenant isolation; spec §18.4) and one
  flush of that tenant's rows. Path `segments/<tenant>/seg-<id:020>.sseg`.
- Rows sorted by `(event_time, seq, idx)`.
- 14 fixed columns (event_time, ingest_time, seq, idx, severity, service,
  severity_text, trace_id, span_id, event_id, body, attributes,
  resource_attributes, schema_version). Each chunk independently CRC-32C'd so
  projections read and verify only what they need (`pread`).
- Encodings: delta-zigzag-varint (times, seq), raw u8 (severity), dictionary
  (service, severity_text), optional-string (ids), optional-value (body),
  per-row typed map (attributes).
- Footer: tenant, row count, time/seq/schema bounds, column directory with
  missing counts, distinct attribute-key set (≤1024 keys, else "overflow"),
  severity histogram, and TLV extensions (unknown *required* → reject, unknown
  optional → ignore). Trailer carries footer length + CRC + magic.
- Single row group per segment in v1 (segments are bounded by memtable size).

## Consequences / known gaps (tracked in KNOWN_LIMITATIONS.md)
- Attributes are stored per-row as typed maps, not shredded into columns:
  attribute predicates must decode the whole attributes chunk.
- No general-purpose compression (zstd/lz4) yet; only lightweight encodings.
- No bloom filters / inverted indexes yet (Phase 3, via extensions).

## Revisit conditions
Phase 3 benchmarks (row-group sizing, attribute shredding, zstd) and ADR-0001 Parquet evaluation.
