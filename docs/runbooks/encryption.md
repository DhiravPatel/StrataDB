# Runbook: encryption at rest

See ADR-0024 for the design and docs/storage-format/encryption-v1.md for formats.

## Enable
```bash
strata keys generate --output /etc/stratadb/kek.hex        # 0600, never overwrites
# Back the key file up NOW, separately from the data and from data backups.
strata init --data-dir /var/lib/stratadb --key-file /etc/stratadb/kek.hex
```
Server: `[encryption] key_file = "/etc/stratadb/kek.hex"` or
`STRATADB_KEY_FILE` / `--key-file`. CLI commands take `--key-file` or
`STRATADB_KEY_FILE`.

**Existing plaintext database:** stop the server, then
`strata upgrade --data-dir D --key-file K --backup-to /safe/place`. The first
open with a key creates the KEYRING; new files are encrypted; `upgrade`
rewrites plaintext segments, and plaintext WAL segments are reclaimed by the
next flush. Verify with `strata doctor --data-dir D --key-file K`.
Encryption cannot be turned off again.

## Rotate
* **Key-encryption key** (e.g. yearly, or when a key file may have leaked):
  `strata keys generate --output kek-2.hex` then
  `strata keys rotate-kek --data-dir D --key-file kek-1.hex --new-key-file kek-2.hex`.
  Instant (rewraps the KEYRING). Point the server at the new file. **Keep the
  old key file until every backup taken with it has expired** — a backup
  carries the KEYRING of its time.
* **Data key:** `strata keys rotate-dek --data-dir D --key-file K`, then
  `strata upgrade` (and compaction over time) re-encrypts old segments.

Both need exclusive access (stop the server).

## Lost or wrong key
* Wrong key → `permission_denied: the configured key (...) is not the
  key-encryption key of this data directory (keyring expects key <id>)`.
  `strata keys status` shows the expected id; compare with
  `strata keys generate`'s output stored at creation time.
* **Lost key → the data and every backup made under it are unrecoverable.**
  There is no recovery path by design.

## Backups
Backups of an encrypted database stay encrypted and include the KEYRING.
`strata backup verify|restore ... --key-file K` needs the key that was current
when the backup was taken.

## Not covered
Memory, swap and core dumps (disable core dumps, use encrypted swap); file
names/sizes/timestamps; the token store and audit log (no event data). Use
full-disk encryption as well where possible.
