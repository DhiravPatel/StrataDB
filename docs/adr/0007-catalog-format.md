# ADR-0007: Catalog / manifest format

- **Status:** Accepted
- **Spec references:** §10.5, §15
- **Normative format:** [docs/storage-format/manifest-v1.md](../storage-format/manifest-v1.md)

## Decision
A single `MANIFEST` file per data directory: a header line with CRC-32C and
length, followed by JSON (inspectable). Replaced atomically (tmp + fsync +
rename + dir fsync). Unknown JSON fields are rejected. Segment file paths are
derived from `(tenant, id)`, never stored, so a tampered manifest cannot point
outside the data directory. SQLite was considered (spec §15.2) and deferred:
the v0 catalog (segment list + WAL boundary) is small and rewrite-on-commit is
simple and crash-safe.

## Consequences
Manifest rewrite cost is O(segments) per flush. Acceptable for 0.1; compaction
keeps segment counts bounded. Tenants/tables/retention policies will need a
richer catalog (SQLite or a journaled log) — revisit at Phase 5.
