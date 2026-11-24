# Compatibility

| Artifact | Current version | Reader accepts | Notes |
|----------|-----------------|----------------|-------|
| WAL segment | 1 | 1 | `Unsupported` for others |
| Event encoding | 1 | 1 | |
| Segment | 3 (provisional) | 1, 2, 3 | v2: LZ4 chunks (RFC-0001); v3: shredded attributes (RFC-0002); compaction upgrades older segments |
| Manifest | 1 | 1 | unknown JSON fields rejected |
| Query language | StrataQL v0 | — | grammar in docs/query-language |
| HTTP API | — | — | not implemented |

Policy: ADR-0015. Pre-1.0 there is no on-disk compatibility promise across
releases; any break will ship with an offline migration tool and release note.
