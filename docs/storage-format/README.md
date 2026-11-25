# Persistent formats

| Format | Version | Document | Code |
|--------|---------|----------|------|
| WAL segment | 2 (reads 1–2) | [wal-v2.md](wal-v2.md), [wal-v1.md](wal-v1.md) | `crates/stratadb-wal/src/format.rs` |
| Event encoding | 1 | [event-encoding-v1.md](event-encoding-v1.md) | `crates/stratadb-schema/src/codec.rs` |
| Segment | 3 (reads 1–3; provisional) | [segment-v1.md](segment-v1.md), [RFC-0001](../rfcs/0001-segment-v2-compression.md), [RFC-0002](../rfcs/0002-attribute-shredding.md) | `crates/stratadb-segment` |
| Manifest | 1 | [manifest-v1.md](manifest-v1.md) | `crates/stratadb-storage/src/manifest.rs` |

Encryption at rest: [encryption-v1.md](encryption-v1.md) (KEYRING, sealed container, encrypted WAL segments).

Durability semantics: [durability.md](durability.md). Data directory layout:

```text
<data_dir>/
  LOCK                      exclusive flock held by the running engine
  MANIFEST                  authoritative segment list + WAL boundary
  wal/wal-<base_seq:020>.log
  segments/<tenant>/seg-<id:020>.sseg
  quarantine/               unreferenced segments moved aside (never auto-deleted)
```
Files are created `0600`, directories `0700`. Symlinks in data directories are ignored.
All integers are little-endian. All checksums are CRC-32C (Castagnoli).
