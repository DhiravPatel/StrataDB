//! Sealed file container v1 (segments, manifest). Normative description:
//! docs/storage-format/encryption-v1.md.
//!
//! ```text
//! header (64 bytes):
//!   0..8   magic          b"STRENC\r\n"
//!   8..10  version        u16 LE (= 1)
//!   10..12 kind           u16 LE (1 segment, 2 manifest)
//!   12..16 dek_id         u32 LE (keyring entry)
//!   16..48 salt           32 random bytes (per file)
//!   48..52 block_size     u32 LE (plaintext bytes per block, 4 KiB..=16 MiB)
//!   52..60 plaintext_len  u64 LE
//!   60..64 header_crc     u32 LE CRC-32C of bytes 0..60
//! body: ⌈plaintext_len / block_size⌉ blocks, each AES-256-GCM(ciphertext ‖ 16-byte tag)
//!   key   = HKDF-SHA256(ikm = DEK, salt, info = "stratadb/sealed-file/v1")
//!   nonce = 0u32 ‖ block_index u64 LE
//!   aad   = header bytes 0..60
//! file length = 64 + plaintext_len + 16 · blocks (checked)
//! ```
//!
//! The file is written once (nonces are block indices under a per-file key).
//! The header is authenticated by every block, so changing the length, salt,
//! key id or kind, truncating, or reordering blocks is detected.

use std::path::Path;

use ring::aead::{Aad, LessSafeKey};
use stratadb_common::checksum::crc32c;
use stratadb_common::io::RandomRead;
use stratadb_common::{Error, Result};

use crate::{auth_failed, counter_nonce, derive, random, Keyring, SALT_LEN, TAG_LEN};

pub const MAGIC: [u8; 8] = *b"STRENC\r\n";
pub const VERSION: u16 = 1;
pub const HEADER_LEN: usize = 64;
pub const DEFAULT_BLOCK: u32 = 64 * 1024;
const MIN_BLOCK: u32 = 4096;
const MAX_BLOCK: u32 = 16 * 1024 * 1024;
const INFO: &[u8] = b"stratadb/sealed-file/v1";

pub const KIND_SEGMENT: u16 = 1;
pub const KIND_MANIFEST: u16 = 2;

/// Whether `prefix` (the first bytes of a file) starts a sealed container.
pub fn is_sealed(prefix: &[u8]) -> bool {
    prefix.len() >= MAGIC.len() && prefix[..MAGIC.len()] == MAGIC
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub kind: u16,
    pub dek_id: u32,
    pub salt: [u8; SALT_LEN],
    pub block_size: u32,
    pub plaintext_len: u64,
}

impl Header {
    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut b = [0u8; HEADER_LEN];
        b[0..8].copy_from_slice(&MAGIC);
        b[8..10].copy_from_slice(&VERSION.to_le_bytes());
        b[10..12].copy_from_slice(&self.kind.to_le_bytes());
        b[12..16].copy_from_slice(&self.dek_id.to_le_bytes());
        b[16..48].copy_from_slice(&self.salt);
        b[48..52].copy_from_slice(&self.block_size.to_le_bytes());
        b[52..60].copy_from_slice(&self.plaintext_len.to_le_bytes());
        let crc = crc32c(&b[0..60]);
        b[60..64].copy_from_slice(&crc.to_le_bytes());
        b
    }

    /// Parse and validate a header (no key needed).
    pub fn decode(b: &[u8], loc: &str) -> Result<Self> {
        if b.len() < HEADER_LEN || !is_sealed(b) {
            return Err(Error::corruption(loc, "not a sealed StrataDB file"));
        }
        let u16_at = |o: usize| u16::from_le_bytes([b[o], b[o + 1]]);
        let u32_at = |o: usize| u32::from_le_bytes([b[o], b[o + 1], b[o + 2], b[o + 3]]);
        if crc32c(&b[0..60]) != u32_at(60) {
            return Err(Error::corruption(
                loc,
                "sealed file header checksum mismatch",
            ));
        }
        let version = u16_at(8);
        if version != VERSION {
            return Err(Error::FormatVersion {
                what: "sealed file",
                found: u32::from(version),
                supported: VERSION.to_string(),
            });
        }
        let block_size = u32_at(48);
        if !(MIN_BLOCK..=MAX_BLOCK).contains(&block_size) {
            return Err(Error::corruption(
                loc,
                "sealed file block size out of range",
            ));
        }
        let mut salt = [0u8; SALT_LEN];
        salt.copy_from_slice(&b[16..48]);
        let mut pl = [0u8; 8];
        pl.copy_from_slice(&b[52..60]);
        Ok(Self {
            kind: u16_at(10),
            dek_id: u32_at(12),
            salt,
            block_size,
            plaintext_len: u64::from_le_bytes(pl),
        })
    }

    fn blocks(&self) -> u64 {
        self.plaintext_len.div_ceil(u64::from(self.block_size))
    }

    /// Expected total file length.
    pub fn file_len(&self) -> Option<u64> {
        (HEADER_LEN as u64)
            .checked_add(self.plaintext_len)?
            .checked_add(self.blocks().checked_mul(TAG_LEN as u64)?)
    }

    /// (offset, stored length, plaintext length) of block `i`.
    fn block_span(&self, i: u64) -> (u64, usize, usize) {
        let bs = u64::from(self.block_size);
        let plain = (self.plaintext_len - i * bs).min(bs) as usize;
        (
            HEADER_LEN as u64 + i * (bs + TAG_LEN as u64),
            plain + TAG_LEN,
            plain,
        )
    }
}

/// Seal `plaintext` with the keyring's active DEK.
pub fn seal(keys: &Keyring, kind: u16, plaintext: &[u8]) -> Result<Vec<u8>> {
    seal_with_block(keys, kind, plaintext, DEFAULT_BLOCK)
}

pub fn seal_with_block(
    keys: &Keyring,
    kind: u16,
    plaintext: &[u8],
    block_size: u32,
) -> Result<Vec<u8>> {
    if !(MIN_BLOCK..=MAX_BLOCK).contains(&block_size) {
        return Err(Error::invalid_input("block size out of range"));
    }
    let (dek_id, dek) = keys.active();
    let mut salt = [0u8; SALT_LEN];
    random(&mut salt)?;
    let h = Header {
        kind,
        dek_id,
        salt,
        block_size,
        plaintext_len: plaintext.len() as u64,
    };
    let key = derive(dek, &salt, INFO)?;
    let hb = h.encode();
    let mut out = Vec::with_capacity(h.file_len().unwrap_or(0) as usize);
    out.extend_from_slice(&hb);
    for (i, chunk) in plaintext.chunks(block_size as usize).enumerate() {
        let mut blk = chunk.to_vec();
        key.seal_in_place_append_tag(counter_nonce(i as u64), Aad::from(&hb[0..60]), &mut blk)
            .map_err(|_| Error::internal("encryption failed"))?;
        out.extend_from_slice(&blk);
    }
    Ok(out)
}

/// Decrypt a whole sealed file held in memory (e.g. the manifest).
pub fn open_bytes(keys: &Keyring, data: &[u8], loc: &str) -> Result<Vec<u8>> {
    let h = Header::decode(data, loc)?;
    if h.file_len() != Some(data.len() as u64) {
        return Err(Error::corruption(
            loc,
            "sealed file length does not match its header (truncated?)",
        ));
    }
    let key = derive(keys.dek(h.dek_id, loc)?, &h.salt, INFO)?;
    let aad = &data[0..60];
    let mut out = Vec::with_capacity(h.plaintext_len as usize);
    for i in 0..h.blocks() {
        let (off, stored, _) = h.block_span(i);
        let mut blk = data[off as usize..off as usize + stored].to_vec();
        let p = key
            .open_in_place(counter_nonce(i), Aad::from(aad), &mut blk)
            .map_err(|_| auth_failed(loc))?;
        out.extend_from_slice(p);
    }
    Ok(out)
}

/// Random-access reader over a sealed file (raw bytes come from any
/// [`RandomRead`] source, normally an I/O-engine file).
pub struct SealedFile {
    file: Box<dyn RandomRead>,
    header: Header,
    hb: [u8; HEADER_LEN],
    key: LessSafeKey,
    loc: String,
}

impl std::fmt::Debug for SealedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedFile")
            .field("loc", &self.loc)
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}

impl SealedFile {
    /// Open and validate the header and total length (blocks are
    /// authenticated when read).
    pub fn open(file: Box<dyn RandomRead>, keys: &Keyring, path: &Path) -> Result<Self> {
        let loc = path.display().to_string();
        let mut hb = [0u8; HEADER_LEN];
        if file.len() < HEADER_LEN as u64 {
            return Err(Error::corruption(
                &loc,
                "sealed file shorter than its header",
            ));
        }
        file.read_exact_at(&mut hb, 0)?;
        let header = Header::decode(&hb, &loc)?;
        let len = file.len();
        if header.file_len() != Some(len) {
            return Err(Error::corruption(
                &loc,
                "sealed file length does not match its header (truncated?)",
            ));
        }
        let key = derive(keys.dek(header.dek_id, &loc)?, &header.salt, INFO)?;
        Ok(Self {
            file,
            header,
            hb,
            key,
            loc,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Plaintext length.
    pub fn len(&self) -> u64 {
        self.header.plaintext_len
    }

    pub fn is_empty(&self) -> bool {
        self.header.plaintext_len == 0
    }

    /// Fill `buf` with plaintext bytes starting at `offset`, authenticating
    /// every block touched.
    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let end = offset
            .checked_add(buf.len() as u64)
            .filter(|e| *e <= self.header.plaintext_len)
            .ok_or_else(|| Error::corruption(&self.loc, "read past the end of a sealed file"))?;
        if buf.is_empty() {
            return Ok(());
        }
        let bs = u64::from(self.header.block_size);
        let (first, last) = (offset / bs, (end - 1) / bs);
        let mut written = 0usize;
        for i in first..=last {
            let (off, stored, _) = self.header.block_span(i);
            let mut blk = vec![0u8; stored];
            self.file.read_exact_at(&mut blk, off)?;
            let plain = self
                .key
                .open_in_place(counter_nonce(i), Aad::from(&self.hb[0..60]), &mut blk)
                .map_err(|_| auth_failed(&self.loc))?;
            let block_start = i * bs;
            let from = offset.max(block_start) - block_start;
            let to = end.min(block_start + plain.len() as u64) - block_start;
            let n = (to - from) as usize;
            buf[written..written + n].copy_from_slice(&plain[from as usize..to as usize]);
            written += n;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::{SecretKey, StaticKeyProvider};
    use proptest::prelude::*;

    fn keys(dir: &Path) -> Keyring {
        Keyring::create(dir, &StaticKeyProvider(SecretKey::from_bytes([9; 32]))).unwrap()
    }

    #[test]
    fn tamper_and_truncation_are_detected() {
        let d = tempfile::tempdir().unwrap();
        let k = keys(d.path());
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let sealed = seal_with_block(&k, KIND_SEGMENT, &data, 4096).unwrap();
        assert_eq!(open_bytes(&k, &sealed, "t").unwrap(), data);
        // Every single-byte change anywhere is rejected.
        for i in (0..sealed.len()).step_by(97) {
            let mut x = sealed.clone();
            x[i] ^= 0x40;
            assert!(open_bytes(&k, &x, "t").is_err(), "byte {i}");
        }
        assert!(open_bytes(&k, &sealed[..sealed.len() - 1], "t").is_err());
        // Swapping two blocks is rejected (nonce = index).
        let mut x = sealed.clone();
        let s = 4096 + TAG_LEN;
        let (a, b) = (HEADER_LEN, HEADER_LEN + s);
        let first: Vec<u8> = x[a..a + s].to_vec();
        let second: Vec<u8> = x[b..b + s].to_vec();
        x[a..a + s].copy_from_slice(&second);
        x[b..b + s].copy_from_slice(&first);
        assert!(open_bytes(&k, &x, "t").is_err());
        // Same plaintext sealed twice: different ciphertext (random salt).
        assert_ne!(
            seal(&k, KIND_SEGMENT, &data).unwrap(),
            seal(&k, KIND_SEGMENT, &data).unwrap()
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        /// Random-access reads return exactly the plaintext range.
        #[test]
        fn random_access_reads(len in 0usize..20_000, reads in prop::collection::vec((0usize..20_000, 0usize..9000), 1..8)) {
            let d = tempfile::tempdir().unwrap();
            let k = keys(d.path());
            let data: Vec<u8> = (0..len).map(|i| (i * 31 % 256) as u8).collect();
            let p = d.path().join("f");
            std::fs::write(&p, seal_with_block(&k, KIND_SEGMENT, &data, 4096).unwrap()).unwrap();
            let f = SealedFile::open(Box::new(std::fs::File::open(&p).unwrap()), &k, &p).unwrap();
            prop_assert_eq!(f.len(), len as u64);
            for (off, n) in reads {
                let mut buf = vec![0u8; n];
                let r = f.read_exact_at(&mut buf, off as u64);
                if off + n <= len {
                    r.unwrap();
                    prop_assert_eq!(&buf[..], &data[off..off + n]);
                } else {
                    prop_assert!(r.is_err());
                }
            }
        }
    }
}
