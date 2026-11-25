# Compatibility

| Artifact | Current version | Reader accepts | Notes |
|----------|-----------------|----------------|-------|
| WAL segment | 1 | 1 | `Unsupported` for others |
| Event encoding | 1 | 1 | |
| Segment | 3 (provisional) | 1, 2, 3 | v2: LZ4 chunks (RFC-0001); v3: shredded attributes (RFC-0002); compaction upgrades older segments |
| Manifest | 1 | 1 | unknown JSON fields rejected |
| Query language | StrataQL v0 | — | grammar in docs/query-language |
| HTTP API | v1 (`/api/v1`) | v1 | `docs/api/http.md`, `docs/api/openapi.json`; additive fields only within v1 |
| Pagination cursor | 1 | 1 | opaque, client-held (ADR-0023); a release must decode every version it issued or reject it with `invalid_input` |

Policy: ADR-0015. Pre-1.0 there is no on-disk compatibility promise across
releases; any break will ship with an offline migration tool and release note.
