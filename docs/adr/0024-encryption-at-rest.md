# ADR-0024: Encryption at rest

- **Status:** Accepted
- **Spec references:** §18.6, §19.1 (backup encryption), §19.4 (restore with correct/wrong key)

## Context
Data files are owner-only (0600/0700) but readable by anyone with disk,
snapshot or backup access. The spec requires a defined scope, a key
management interface, DEK/KEK separation, rotation, backup/WAL/segment/
metadata encryption, recovery behaviour when keys are unavailable, vetted
libraries, and pluggable key management.

## Decision
* **Crypto library:** `ring` (already in the tree for TLS, ADR-0021):
  AES-256-GCM, HKDF-SHA256, the system CSPRNG. `zeroize` clears key material.
  No new third-party code; no cryptography implemented in StrataDB.
* **Key hierarchy:** a KEK from a `KeyProvider` (trait; default
  `FileKeyProvider`: 64 hex chars in a 0600 file outside the data directory,
  `strata keys generate`). Random DEKs, wrapped by the KEK (AES-256-GCM, random
  nonce, AAD binds the DEK id), in `<data_dir>/KEYRING` with a KEK check value,
  so a wrong KEK is reported as such (`permission_denied`), not as corruption.
* **Per-file keys:** every file derives its own key
  `HKDF-SHA256(DEK, random 32-byte salt, domain-separated info)`.
* **Segments and MANIFEST:** sealed container v1 (docs/storage-format/encryption-v1.md):
  64 KiB blocks, each AES-256-GCM with nonce = block index, AAD = the
  authenticated header (kind, DEK id, salt, block size, plaintext length).
  Random access is preserved; truncation, reordering and tampering are detected.
  The plaintext inside is the unchanged segment/manifest format.
* **WAL:** header flag bit 0 + a CRC'd 48-byte extension (DEK id, salt); each
  frame payload is sealed with the segment's key, nonce = sequence number,
  AAD = frame header fields. Frame CRCs cover the ciphertext, so power-loss
  classification (WAL v2) works without the key. Nonce uniqueness: an
  encrypted WAL never appends to a segment written by an earlier process
  (recovery always starts a new segment), so sequence numbers reused after a
  truncated tail fall under a different key.
* **Rotation:** KEK rotation rewraps the KEYRING atomically (no data rewrite);
  DEK rotation adds a new active DEK; `strata upgrade` rewrites segments that
  are plaintext (or, by compaction, older DEKs). Offline (CLI) operations.
* **Enabling:** configuring a key on an existing plaintext directory creates
  the KEYRING first; new files are encrypted, old ones remain readable until
  rewritten (`strata upgrade`). Disabling is not supported. An encrypted
  directory never opens without its key.
* **Backups:** sealed files are copied as-is plus the KEYRING (wrapped keys);
  verify/restore require the KEK that wrapped it.

## Alternatives rejected
* Whole-file encryption with a stream cipher: no random access, no per-block authentication.
* Random 96-bit nonces for WAL frames: birthday bound reached after ~2³² frames per key.
* XChaCha20-Poly1305 (192-bit random nonces): not in `ring`; would add a new crypto dependency.
* Filesystem/volume encryption only: still recommended, but does not protect
  backups or copies and is outside the database's control.
* Encrypting in the OS page cache layer / `unsafe` mmap tricks: forbidden (`unsafe_code`).

## Consequences
* Overhead: AES-GCM per block on reads/writes; +16 B per 64 KiB block and per WAL frame, +48 B per WAL segment header.
* One extra WAL segment per process start when encrypted.
* Losing the KEK loses the data and every backup made under it — key backup is the operator's responsibility (runbook).
* Not covered (documented in the threat model): data in memory, swap and core
  dumps; the token store and audit log (no event data; tokens are stored as
  verifiers); file names, sizes and timestamps; the BACKUP index file.

## Evidence
`stratadb-crypto` tests (tamper/truncation/block-swap rejection, random-access
proptest, wrong-KEK detection, rotation, key-file permissions, frame AAD
binding); WAL tests (round trip, new segment per process, missing/other key,
ciphertext tamper, reordered tail); engine tests `tests/encryption.rs` (no
plaintext on disk after flush/compaction/WAL, enabling on existing data +
upgrade, rotation, encrypted backup/verify/restore with right/wrong/missing
key, tamper detection, golden fixture `tests/fixtures/encrypted-v1`);
power-loss simulation with encryption (`sync` and `async`, 1 320 states, 0
violations); CLI workflow test.

## Revisit conditions
An external KMS/HSM provider (implement `KeyProvider`); online rotation;
encrypting the token store and audit log; FIPS-validated crypto requirements.
