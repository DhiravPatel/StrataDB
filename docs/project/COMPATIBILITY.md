# Compatibility

| Artifact | Current version | Reader accepts | Notes |
|----------|-----------------|----------------|-------|
| WAL segment | 3 (2 with `wal_format_version = 2`) | 1, 2, 3 | v2 adds `synced_through` per frame (wal-v2.md); v3 adds columnar record types 3–4 (wal-v3.md); older segments are replayed, never appended to; fixtures `tests/fixtures/storage-v1` (v1), `tests/fixtures/wal-v2`, `tests/fixtures/wal-v3`, `tests/fixtures/storage-wal-v3`; others → `FormatVersion` |
| Event encoding (row) | 1 | 1 | WAL types 1–2 |
| Columnar event batch | 1 | 1 | WAL types 3–4 (event-batch-columnar-v1.md) |
| Segment | 3 (provisional) | 1, 2, 3 | v2: LZ4 chunks (RFC-0001); v3: shredded attributes (RFC-0002); compaction upgrades older segments |
| Manifest | 1 | 1 | unknown JSON fields rejected |
| Query language | StrataQL v0 | — | grammar in docs/query-language |
| HTTP API | v1 (`/api/v1`) | v1 | `docs/api/http.md`, `docs/api/openapi.json`; additive fields only within v1 |
| Encryption (KEYRING, sealed container, WAL flag) | 1 | 1 | ADR-0024, encryption-v1.md; fixture `tests/fixtures/encrypted-v1`; pre-encryption readers reject the WAL flag and the sealed magic |
| Pagination cursor | 1 | 1 | opaque, client-held (ADR-0023); a release must decode every version it issued or reject it with `invalid_input` |

Policy: ADR-0015. Pre-1.0 there is no on-disk compatibility promise across
releases; any break will ship with an offline migration tool and release note.
