# Runbook: backup and restore

## Create
- CLI (server stopped): `strata backup create --data-dir D --output /backups/2026-11-21`
- Server running: start with `--backup-dir /backups`, then
  `curl -X POST -H "Authorization: Bearer $ADMIN" -d '{"name":"2026-11-21"}' http://127.0.0.1:7070/api/v1/admin/backup`
  (token needs `backup.create`).
Copy the backup to a **different failure domain** (another disk/host). A
backup on the same disk does not protect against disk loss.

## Verify (do this regularly)
`strata backup verify /backups/2026-11-21` — checks every SHA-256, segment
checksum, and manifest reference. Non-zero exit = do not rely on it.

## Restore
1. Stop the server. Keep the damaged data directory for analysis.
2. `strata backup restore /backups/2026-11-21 --data-dir /data/stratadb-restored`
   (target must be absent or empty; it is never overwritten).
3. Inspect the output: `restored_through_seq` is the recovery point; writes
   acknowledged after the backup was taken are **not** present (RPO = time since backup).
4. Re-create API tokens (`strata-server token create`) — tokens are not in backups.
5. Start the server on the restored directory.

## Drill
Run quarterly: restore the latest backup into a scratch directory, run
`strata status` and a known query, record elapsed time (RTO) and data age (RPO).
