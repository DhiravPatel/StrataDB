# Encryption formats v1 (ADR-0024)

All integers little-endian; CRCs are CRC-32C. AEAD is AES-256-GCM (16-byte
tags) from `ring`; key derivation is HKDF-SHA256.

## KEYRING (`<data_dir>/KEYRING`)
```text
STRATADB-KEYRING v1 crc32c=<8 hex> len=<n>\n<JSON body of n bytes>
{"version": 1, "kek_id": "<16 hex>", "kek_check": "<hex>", "active": <id>,
 "deks": [{"id": <u32>, "created_ms": <i64>, "wrapped": "<hex>"}]}
```
* `kek_id` = first 8 bytes of SHA-256("stratadb-kek-id\0" ‖ KEK), hex.
* `wrapped` = nonce(12, random) ‖ AES-256-GCM(KEK, nonce, DEK(32), aad = "stratadb-dek:" ‖ id u32).
* `kek_check` = nonce(12) ‖ AES-256-GCM(KEK, nonce, "stratadb-kek-check", aad = "kek-check").
* Written with temp file + fsync + rename + directory fsync. Never contains a key in clear.

## Sealed file container v1 (segments, MANIFEST)
```text
0..8   magic         "STRENC\r\n"
8..10  version       1
10..12 kind          1 segment, 2 manifest
12..16 dek_id        u32
16..48 salt          32 random bytes
48..52 block_size    u32 (4 KiB..=16 MiB; writer uses 64 KiB)
52..60 plaintext_len u64
60..64 header_crc    CRC-32C of bytes 0..60
then ⌈plaintext_len / block_size⌉ blocks: ciphertext(block) ‖ tag(16)
key   = HKDF-SHA256(ikm = DEK, salt, info = "stratadb/sealed-file/v1")
nonce = 0u32 ‖ block index u64
aad   = bytes 0..60 of the header
file length = 64 + plaintext_len + 16 × blocks (checked before any read)
```
The plaintext is an ordinary segment (segment-v1.md) or manifest
(manifest-v1.md). Readers detect the magic and require the keyring.

## Encrypted WAL segments (extension of wal-v2.md)
Header flag bit 0 (`FLAG_ENCRYPTED`, only valid with format version ≥ 2) and a
48-byte extension right after the 32-byte header:
```text
0..4   dek_id   u32
4..36  salt     32 random bytes
36..44 reserved 0
44..48 crc      CRC-32C of extension bytes 0..44
```
Frames keep the v2 layout; `payload` = AES-256-GCM ciphertext ‖ tag of the
record payload with
```text
key   = HKDF-SHA256(ikm = DEK, salt, info = "stratadb/wal-segment/v1")
nonce = 0u32 ‖ seq u64
aad   = record_type u8 ‖ seq u64 ‖ synced_through u64
```
The frame CRC covers the ciphertext (recovery's torn/reordered-tail rules run
without decrypting). A writer never appends to an encrypted segment created by
an earlier process. Readers that predate encryption reject the flag
(`unknown WAL header flags`).

## Compatibility
Golden fixture: `tests/fixtures/encrypted-v1/` (test key `[0x5a; 32]`, in
`crates/stratadb-storage/tests/encryption.rs`; never regenerate).
