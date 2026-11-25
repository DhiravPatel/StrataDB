//! `<data_dir>/KEYRING`: data-encryption keys wrapped by the KEK.
//!
//! ```text
//! STRATADB-KEYRING v1 crc32c=<8 hex> len=<n>\n<JSON>
//! JSON: {"version": 1, "kek_id": "<fingerprint>", "kek_check": "<hex>",
//!        "active": <dek id>, "deks": [{"id": n, "created_ms": t, "wrapped": "<hex>"}]}
//! wrapped   = nonce(12) ‖ AES-256-GCM(KEK, nonce, DEK, aad = "stratadb-dek:" ‖ id LE)
//! kek_check = nonce(12) ‖ AES-256-GCM(KEK, nonce, "stratadb-kek-check", aad = "kek-check")
//! ```
//! Written with `fsutil::atomic_write`. Wrapping uses random 96-bit nonces
//! (a handful of DEKs per KEK, far below the random-nonce bound).

use std::collections::BTreeMap;
use std::path::Path;

use ring::aead::{Aad, Nonce, NONCE_LEN};
use serde::{Deserialize, Serialize};
use stratadb_common::checksum::crc32c;
use stratadb_common::{fsutil, time, Error, Result};

use crate::{auth_failed, random, raw_key, KeyProvider, SecretKey, KEY_LEN};

pub const KEYRING_FILE: &str = "KEYRING";
const MAGIC_PREFIX: &str = "STRATADB-KEYRING v1 ";
const CHECK_PLAINTEXT: &[u8] = b"stratadb-kek-check";
const MAX_DEKS: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DekEntry {
    id: u32,
    created_ms: i64,
    wrapped: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    version: u32,
    kek_id: String,
    kek_check: String,
    active: u32,
    deks: Vec<DekEntry>,
}

/// Unwrapped data-encryption keys of one data directory. Immutable while an
/// engine uses it; rotation is an offline operation.
pub struct Keyring {
    kek: SecretKey,
    file: File,
    deks: BTreeMap<u32, SecretKey>,
}

impl std::fmt::Debug for Keyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keyring")
            .field("kek_id", &self.file.kek_id)
            .field("active", &self.file.active)
            .field("deks", &self.deks.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok())
        .collect()
}

fn seal_random(kek: &SecretKey, aad: &[u8], plain: &[u8]) -> Result<String> {
    let mut nonce = [0u8; NONCE_LEN];
    random(&mut nonce)?;
    let mut buf = plain.to_vec();
    raw_key(kek)?
        .seal_in_place_append_tag(
            Nonce::assume_unique_for_key(nonce),
            Aad::from(aad),
            &mut buf,
        )
        .map_err(|_| Error::internal("key wrapping failed"))?;
    let mut out = nonce.to_vec();
    out.extend_from_slice(&buf);
    Ok(hex(&out))
}

fn open_random(kek: &SecretKey, aad: &[u8], text: &str, loc: &str) -> Result<Vec<u8>> {
    let raw = unhex(text).ok_or_else(|| Error::corruption(loc, "malformed hex in keyring"))?;
    if raw.len() < NONCE_LEN + crate::TAG_LEN {
        return Err(Error::corruption(loc, "wrapped key too short"));
    }
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&raw[..NONCE_LEN]);
    let mut ct = raw[NONCE_LEN..].to_vec();
    let p = raw_key(kek)?
        .open_in_place(Nonce::assume_unique_for_key(nonce), Aad::from(aad), &mut ct)
        .map_err(|_| auth_failed(loc))?;
    Ok(p.to_vec())
}

fn dek_aad(id: u32) -> Vec<u8> {
    let mut a = b"stratadb-dek:".to_vec();
    a.extend_from_slice(&id.to_le_bytes());
    a
}

impl Keyring {
    /// Path of the keyring in `dir`.
    pub fn path(dir: &Path) -> std::path::PathBuf {
        dir.join(KEYRING_FILE)
    }

    pub fn exists(dir: &Path) -> bool {
        Self::path(dir).exists()
    }

    /// Create a keyring with one random DEK and persist it (fails if one exists).
    pub fn create(dir: &Path, provider: &dyn KeyProvider) -> Result<Self> {
        if Self::exists(dir) {
            return Err(Error::new(
                stratadb_common::ErrorKind::AlreadyExists,
                "a KEYRING already exists in this data directory",
            ));
        }
        let kek = provider.kek()?;
        let dek = SecretKey::generate()?;
        let file = File {
            version: 1,
            kek_id: kek.fingerprint(),
            kek_check: seal_random(&kek, b"kek-check", CHECK_PLAINTEXT)?,
            active: 1,
            deks: vec![DekEntry {
                id: 1,
                created_ms: time::now_unix_millis(),
                wrapped: seal_random(&kek, &dek_aad(1), dek.bytes())?,
            }],
        };
        let k = Self {
            kek,
            file,
            deks: BTreeMap::from([(1, dek)]),
        };
        k.persist(dir)?;
        Ok(k)
    }

    /// Load and unwrap `<dir>/KEYRING`; `Ok(None)` if there is none. A KEK
    /// that does not match the keyring is reported as such (not as corruption).
    pub fn load(dir: &Path, provider: &dyn KeyProvider) -> Result<Option<Self>> {
        let p = Self::path(dir);
        let bytes = match std::fs::read(&p) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::io(format!("reading {}", p.display()), e)),
        };
        let loc = p.display().to_string();
        let file = Self::decode(&bytes, &loc)?;
        let kek = provider.kek()?;
        if kek.fingerprint() != file.kek_id
            || open_random(&kek, b"kek-check", &file.kek_check, &loc)? != CHECK_PLAINTEXT
        {
            return Err(Error::new(
                stratadb_common::ErrorKind::PermissionDenied,
                format!(
                    "the configured key ({}) is not the key-encryption key of this data \
                     directory (keyring expects key {})",
                    provider.describe(),
                    file.kek_id
                ),
            ));
        }
        let mut deks = BTreeMap::new();
        for d in &file.deks {
            let raw = open_random(&kek, &dek_aad(d.id), &d.wrapped, &loc)?;
            let arr: [u8; KEY_LEN] = raw
                .try_into()
                .map_err(|_| Error::corruption(&loc, "wrapped DEK has the wrong length"))?;
            deks.insert(d.id, SecretKey::from_bytes(arr));
        }
        if !deks.contains_key(&file.active) {
            return Err(Error::corruption(&loc, "active DEK missing from keyring"));
        }
        Ok(Some(Self { kek, file, deks }))
    }

    /// Parse the file framing and JSON (no key needed).
    fn decode(bytes: &[u8], loc: &str) -> Result<File> {
        let nl = bytes
            .iter()
            .position(|&b| b == b'\n')
            .filter(|&p| p < 128)
            .ok_or_else(|| Error::corruption(loc, "missing keyring header"))?;
        let header = std::str::from_utf8(&bytes[..nl])
            .map_err(|_| Error::corruption(loc, "keyring header is not UTF-8"))?;
        let rest = header
            .strip_prefix(MAGIC_PREFIX)
            .ok_or_else(|| Error::corruption(loc, "bad keyring magic/version"))?;
        let (mut crc, mut len) = (None, None);
        for part in rest.split(' ') {
            if let Some(v) = part.strip_prefix("crc32c=") {
                crc = u32::from_str_radix(v, 16).ok();
            } else if let Some(v) = part.strip_prefix("len=") {
                len = v.parse::<usize>().ok();
            }
        }
        let body = &bytes[nl + 1..];
        if crc != Some(crc32c(body)) || len != Some(body.len()) {
            return Err(Error::corruption(loc, "keyring checksum/length mismatch"));
        }
        let f: File = serde_json::from_slice(body)
            .map_err(|e| Error::corruption(loc, format!("keyring JSON invalid: {e}")))?;
        if f.version != 1 {
            return Err(Error::FormatVersion {
                what: "keyring",
                found: f.version,
                supported: "1".into(),
            });
        }
        if f.deks.is_empty() || f.deks.len() > MAX_DEKS {
            return Err(Error::corruption(loc, "keyring DEK count out of range"));
        }
        Ok(f)
    }

    /// Validate framing only (diagnostics, fuzzing).
    pub fn check_format(bytes: &[u8]) -> Result<()> {
        Self::decode(bytes, KEYRING_FILE).map(|_| ())
    }

    fn persist(&self, dir: &Path) -> Result<()> {
        let body = serde_json::to_vec_pretty(&self.file)
            .map_err(|e| Error::internal(format!("serializing keyring: {e}")))?;
        let mut out = format!(
            "{MAGIC_PREFIX}crc32c={:08x} len={}\n",
            crc32c(&body),
            body.len()
        )
        .into_bytes();
        out.extend_from_slice(&body);
        fsutil::atomic_write(&Self::path(dir), &out, "keyring")
    }

    /// The DEK new files are sealed with.
    pub fn active(&self) -> (u32, &SecretKey) {
        let id = self.file.active;
        (id, &self.deks[&id])
    }

    /// The DEK with `id` (error if the keyring does not hold it).
    pub fn dek(&self, id: u32, loc: &str) -> Result<&SecretKey> {
        self.deks
            .get(&id)
            .ok_or_else(|| Error::corruption(loc, format!("data key {id} is not in the keyring")))
    }

    /// Fingerprint of the KEK this keyring is wrapped with.
    pub fn kek_id(&self) -> &str {
        &self.file.kek_id
    }

    pub fn dek_ids(&self) -> Vec<u32> {
        self.deks.keys().copied().collect()
    }

    /// Re-wrap every DEK with a new KEK (no data is rewritten). Atomic.
    pub fn rotate_kek(&mut self, dir: &Path, new: &dyn KeyProvider) -> Result<()> {
        let kek = new.kek()?;
        if kek.fingerprint() == self.file.kek_id {
            return Err(Error::invalid_input("the new key is the current key"));
        }
        let mut file = self.file.clone();
        file.kek_id = kek.fingerprint();
        file.kek_check = seal_random(&kek, b"kek-check", CHECK_PLAINTEXT)?;
        for d in &mut file.deks {
            d.wrapped = seal_random(&kek, &dek_aad(d.id), self.deks[&d.id].bytes())?;
        }
        let next = Self {
            kek,
            file,
            deks: self.deks.clone(),
        };
        next.persist(dir)?;
        *self = next;
        Ok(())
    }

    /// Add a new random DEK and make it active for new files. Older DEKs stay
    /// (files sealed with them remain readable until rewritten). Atomic.
    pub fn rotate_dek(&mut self, dir: &Path) -> Result<u32> {
        if self.file.deks.len() >= MAX_DEKS {
            return Err(Error::resource_exhausted(
                "keyring holds the maximum number of data keys",
            ));
        }
        let id = self.deks.keys().next_back().copied().unwrap_or(0) + 1;
        let dek = SecretKey::generate()?;
        let mut file = self.file.clone();
        file.deks.push(DekEntry {
            id,
            created_ms: time::now_unix_millis(),
            wrapped: seal_random(&self.kek, &dek_aad(id), dek.bytes())?,
        });
        file.active = id;
        let mut deks = self.deks.clone();
        deks.insert(id, dek);
        let next = Self {
            kek: self.kek.clone(),
            file,
            deks,
        };
        next.persist(dir)?;
        *self = next;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::StaticKeyProvider;
    use stratadb_common::ErrorKind;

    fn prov(b: u8) -> StaticKeyProvider {
        StaticKeyProvider(SecretKey::from_bytes([b; 32]))
    }

    #[test]
    fn create_load_and_wrong_key() {
        let d = tempfile::tempdir().unwrap();
        assert!(Keyring::load(d.path(), &prov(1)).unwrap().is_none());
        let k = Keyring::create(d.path(), &prov(1)).unwrap();
        let (id, dek) = k.active();
        let back = Keyring::load(d.path(), &prov(1)).unwrap().unwrap();
        assert_eq!(back.active().1.bytes(), dek.bytes());
        assert_eq!(id, 1);
        let e = Keyring::load(d.path(), &prov(2)).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::PermissionDenied, "{e}");
        assert!(
            Keyring::create(d.path(), &prov(1)).is_err(),
            "never overwritten"
        );
        // The file never contains the DEK or KEK in clear.
        let raw = std::fs::read(Keyring::path(d.path())).unwrap();
        let needle = hex(dek.bytes());
        assert!(!String::from_utf8_lossy(&raw).contains(&needle));
    }

    #[test]
    fn rotation_keeps_old_data_keys_readable() {
        let d = tempfile::tempdir().unwrap();
        let mut k = Keyring::create(d.path(), &prov(1)).unwrap();
        let first = k.active().1.clone();
        assert_eq!(k.rotate_dek(d.path()).unwrap(), 2);
        k.rotate_kek(d.path(), &prov(3)).unwrap();
        assert!(
            Keyring::load(d.path(), &prov(1)).is_err(),
            "old KEK no longer opens it"
        );
        let k2 = Keyring::load(d.path(), &prov(3)).unwrap().unwrap();
        assert_eq!(k2.active().0, 2);
        assert_eq!(k2.dek(1, "t").unwrap().bytes(), first.bytes());
        assert!(k2.dek(9, "t").is_err());
    }

    #[test]
    fn tampering_is_detected() {
        let d = tempfile::tempdir().unwrap();
        Keyring::create(d.path(), &prov(1)).unwrap();
        let p = Keyring::path(d.path());
        let raw = std::fs::read(&p).unwrap();
        for i in (0..raw.len()).step_by(13) {
            let mut x = raw.clone();
            x[i] ^= 0x01;
            std::fs::write(&p, &x).unwrap();
            assert!(Keyring::load(d.path(), &prov(1)).is_err(), "byte {i}");
        }
    }
}
