# Compatibility

| Artifact | Current version | Reader accepts | Notes |
|----------|-----------------|----------------|-------|
| WAL segment | 2 | 1, 2 | v2 adds `synced_through` per frame (wal-v2.md); v1 segments are replayed, never appended to; fixtures `tests/fixtures/storage-v1` (v1), `tests/fixtures/wal-v2`; others → `FormatVersion` |
| Event encoding | 1 | 1 | |
| Segment | 3 (provisional) | 1, 2, 3 | v2: LZ4 chunks (RFC-0001); v3: shredded attributes (RFC-0002); compaction upgrades older segments |
| Manifest | 1 | 1 | unknown JSON fields rejected |
| Query language | StrataQL v0 | — | grammar in docs/query-language |
| HTTP API | v1 (`/api/v1`) | v1 | `docs/api/http.md`, `docs/api/openapi.json`; additive fields only within v1 |
| Encryption (KEYRING, sealed container, WAL flag) | 1 | 1 | ADR-0024, encryption-v1.md; fixture `tests/fixtures/encrypted-v1`; pre-encryption readers reject the WAL flag and the sealed magic |
| Pagination cursor | 1 | 1 | opaque, client-held (ADR-0023); a release must decode every version it issued or reject it with `invalid_input` |

Policy: ADR-0015. Pre-1.0 there is no on-disk compatibility promise across
releases; any break will ship with an offline migration tool and release note.
