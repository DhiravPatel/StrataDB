# Runbook: upgrading StrataDB

1. Read the release notes and COMPATIBILITY.md for format changes.
2. Stop the server (the CLI needs the data-directory lock).
3. Install the new binaries next to the old ones (keep the old ones for rollback).
4. `strata doctor --data-dir D` — must not report `fail`.
5. `strata upgrade --data-dir D --dry-run` — shows segments per format version
   and what would be rewritten. New binaries read every older format, so this
   step is optional: compaction also upgrades segments gradually.
6. To upgrade everything now: `strata upgrade --data-dir D --backup-to /backups/pre-upgrade`
   (creates and **verifies** a backup first; checks free disk before each segment;
   each segment is rewritten with the crash-safe compaction protocol, so an
   interrupted run can simply be re-run).
7. Start the server with the new binary; check `/api/v1/ready` and `strata doctor`.

**Rollback:** older binaries cannot read segments in newer formats. Roll back by
stopping the server and `strata backup restore /backups/pre-upgrade --data-dir NEW`
with the old binary, accepting the loss of writes made after the backup.
