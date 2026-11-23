# ADR-0010: Backup format and procedure

- **Status:** Accepted
- **Spec references:** §19

## Decision
Directory backup: `BACKUP` (envelope `STRATADB-BACKUP v1 sha256=<hex> len=<n>`
+ JSON listing every file with size and SHA-256), `MANIFEST`, and
byte-identical copies of committed segments. Procedure: flush → capture
manifest and pin segment handles under the manifest lock → copy + hash →
verify each copy by reopening it → write `MANIFEST` and `BACKUP` → fsync tree
→ rename `<dest>.tmp` → `<dest>`. Consistency point: the captured
manifest's `flushed_through_seq` (all writes acknowledged before the call).
Auth tokens are excluded. Paths in `BACKUP` must match the derived segment
layout (path traversal rejected).

Restore: target must be absent or empty → verify backup → copy into
`<target>.tmp` re-checking hashes → empty WAL dir → rename → open with full
segment verification. Restore is offline (CLI) only.

Incremental backups and encryption are not implemented.

## Evidence
`crates/stratadb-storage/tests/backup.rs` (backup under concurrent ingest +
compaction, restore to clean dir, corruption/missing-file/traversal detection,
overwrite refusal, interrupted backup not published), CLI drill in
`apps/stratadb-cli/tests/cli.rs`, HTTP `admin_endpoints_require_privileges_and_are_safe`.
