# ADR-0022: Tamper-evident audit log

- **Status:** Accepted
- **Spec references:** §18.7

## Decision
`<data_dir>/audit/audit.log`, JSON lines `{"hash", "record"}`; `record.prev`
chains to the previous hash (SHA-256). Appends take an exclusive file lock,
re-read the tail, and fsync, so the server and the token CLI can both write.
Events: `server_start`, `server_stop`, `auth_failure` (permission + reason,
never the credential), `permission_denied` (token id), `token_create`,
`token_revoke`, `admin_compaction`, `backup_create`, `retention_set`,
`retention_clear`. Administrative actions **fail closed**: if their audit
record cannot be written they are refused (503). Security-event records on
the request path are best-effort (logged on failure) so an audit disk
problem cannot take ingestion down.

Verification: `strata-server audit-verify --data-dir D` recomputes the chain.
Limits: tamper-evident, not tamper-proof (a writer can rewrite the whole
file; tail truncation is only detectable against an externally recorded
latest seq/hash). Ship the log off-host for stronger guarantees. No rotation yet.
