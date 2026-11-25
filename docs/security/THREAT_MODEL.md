# Threat model (initial, spec §18.1)

## Assets
Tenant log data; credentials/tokens (future); data-directory integrity;
availability of ingestion and queries.

## Trust boundaries
1. Network clients → server (future): untrusted.
2. Authenticated principal → tenant scope: tenant derived from identity only.
3. Process ↔ data directory: trusted filesystem, but contents are validated
   (they may be corrupted or tampered with by anyone with disk access).
4. Operator with filesystem access = full administrator (cannot be mitigated in-process).

## Threats and current mitigations
| Threat | Mitigation (status) |
|--------|---------------------|
| Cross-tenant read | Snapshot is tenant-scoped; segments are single-tenant in per-tenant dirs; executor rejects foreign rows/segments (implemented, tested) |
| Tenant spoofing via payload | Payload `tenant_id` never sets tenant; engine rejects mismatched batches (implemented, tested) |
| Path traversal via tenant/segment names | Tenant alphabet `[A-Za-z0-9_-]`; segment paths derived, never read from manifest; symlinks ignored (implemented, tested) |
| Malicious/corrupt persisted bytes | CRC-32C everywhere; bounded decoders that never panic or over-allocate (implemented, property-tested); fuzzing pending |
| Silent data loss from corruption | Fail-closed recovery; quarantine instead of delete (implemented, tested) |
| Malicious queries (CPU/memory) | Parser length/depth/IN limits; timeouts, cancellation, row/result/memory budgets; LIKE without regex backtracking blowup (implemented, tested) |
| Oversized events/batches | Validation limits; WAL record size limit (implemented); HTTP body limits (pending server) |
| Unauthenticated access | Bearer API tokens on every data endpoint; no default credentials; health/ready/version reveal no data (implemented, tested) |
| Token theft from disk | Only SHA-256 verifiers stored, file 0600; constant-time compare (implemented, tested) |
| Network interception | TLS 1.2/1.3 via rustls; non-loopback bind requires TLS or an explicit insecure flag (implemented, tested) |
| Slow-handshake DoS | handshake timeout 10 s, ≤ 256 concurrent handshakes (implemented) |
| Repudiation / forged audit records | hash-chained audit log, fail-closed for admin actions (implemented, tested); tamper-evident only |
| Disk exhaustion | free-space reserve pauses writes with retryable 429; compaction/backup check space first (implemented, tested) |
| Credential stuffing | Global failed-auth limiter → 429 (implemented; per-client limiting pending) |
| Information leakage via errors | Corruption/I/O/internal errors replaced by a generic message; details logged server-side (implemented) |
| Request floods | Body cap 8 MiB, ingest/query semaphores → 429 (implemented, tested) |
| Credential leakage in logs | Tokens are never logged; plaintext printed only once at creation; `AppState` Debug omits token data |
| Tampering with data files | Plaintext directories: CRC detects accidents only. Encrypted directories (ADR-0024): AES-GCM authenticates every segment block, manifest and WAL frame (tampering → corruption error, never silent) |
| Supply chain | Minimal deps, all permissive licenses, lockfile committed; `cargo audit`/`cargo deny` in CI **pending installation** |
| Data at rest exposure | Files 0600, dirs 0700; optional encryption at rest (ADR-0024): segments, manifest, WAL and backups sealed with AES-256-GCM, KEK outside the data directory. Not covered: memory/swap/core dumps, file metadata, token store and audit log (no event data) |
| Key compromise / loss | KEK file must be 0600 (enforced); KEK rotation without data rewrite; lost KEK = data loss by design (runbook) |
