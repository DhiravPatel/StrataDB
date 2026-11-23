# Manifest format v1

```text
STRATADB-MANIFEST v1 crc32c=<8 lowercase hex> len=<decimal>\n
<len bytes of UTF-8 JSON>
```
JSON object (unknown fields rejected):
`format_version` (1), `version` (monotonic), `next_segment_id`,
`flushed_through_seq`, `segments[]` with `id, tenant, segment_format_version,
min/max_schema_version, min/max_event_time, min/max_seq, row_count, file_bytes,
footer_crc, created_ms, state ("committed"), lineage[]`.

Invariants checked on load and before commit: unique ids; every id <
`next_segment_id`; every segment's `max_seq ≤ flushed_through_seq`.
On open, each segment's footer CRC, id, tenant, row count and seq bounds must
match its entry, otherwise the engine refuses to start.

Unreferenced segment files at open: id ≥ `next_segment_id` ⇒ deleted (output
of a crashed flush; rows are still in the WAL); id < `next_segment_id` ⇒ moved
to `quarantine/` (never deleted automatically).
A missing MANIFEST with existing WAL/segment files ⇒ refuse to start.
