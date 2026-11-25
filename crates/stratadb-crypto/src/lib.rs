//! Encryption at rest (spec §18.6, ADR-0024).
//!
//! * **Keys.** A key-encryption key (KEK, 256-bit) comes from a pluggable
//!   [`KeyProvider`] (default: a key file). It never touches the data
//!   directory. Data-encryption keys (DEKs, 256-bit, random) are stored
//!   *wrapped* by the KEK in `<data_dir>/KEYRING` ([`Keyring`]). Rotating the KEK
//!   rewraps the keyring (no data rewrite); rotating the DEK makes a new DEK
//!   active for new files.
//! * **Files** (segments, manifest): [`container`] — a sealed container whose
//!   content is split into blocks, each sealed with AES-256-GCM under a
//!   per-file subkey `HKDF-SHA256(DEK, random 32-byte salt)`; nonce = block
//!   index; AAD = the authenticated header. Random access stays possible.
//! * **WAL frames:** [`FrameKey`] — per WAL segment subkey (random salt in the
//!   segment header), nonce = record sequence number. The WAL never appends to
//!   a segment written by an earlier process when encrypted, so a (subkey,
//!   nonce) pair is never reused even though recovery can reuse sequence
//!   numbers of discarded frames.
//!
//! Only `ring` primitives are used (AES-256-GCM, HKDF-SHA256, system RNG); no
//! cryptography is implemented here.

pub mod container;
mod keyring;

use std::path::{Path, PathBuf};

use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
use ring::hkdf;
use ring::rand::{SecureRandom, SystemRandom};
use stratadb_common::{Error, Result};
use zeroize::Zeroizing;

pub use keyring::{Keyring, KEYRING_FILE};

pub const KEY_LEN: usize = 32;
pub const TAG_LEN: usize = 16;
pub const SALT_LEN: usize = 32;

/// 256-bit secret key material, zeroed on drop; never printed.
#[derive(Clone)]
pub struct SecretKey(Zeroizing<[u8; KEY_LEN]>);

impl SecretKey {
    pub fn from_bytes(b: [u8; KEY_LEN]) -> Self {
        Self(Zeroizing::new(b))
    }

    /// A fresh random key from the system CSPRNG.
    pub fn generate() -> Result<Self> {
        let mut b = Zeroizing::new([0u8; KEY_LEN]);
        random(&mut *b)?;
        Ok(Self(b))
    }

    pub(crate) fn bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// Parse 64 hexadecimal characters (surrounding whitespace ignored).
    pub fn from_hex(text: &str) -> Result<Self> {
        let t = text.trim();
        if t.len() != KEY_LEN * 2 {
            return Err(Error::invalid_input(
                "a key must be 64 hexadecimal characters (256 bits)",
            ));
        }
        let mut b = Zeroizing::new([0u8; KEY_LEN]);
        for (i, slot) in b.iter_mut().enumerate() {
            *slot = u8::from_str_radix(&t[2 * i..2 * i + 2], 16)
                .map_err(|_| Error::invalid_input("key contains non-hexadecimal characters"))?;
        }
        Ok(Self(b))
    }

    /// Non-secret identifier: first 8 bytes of SHA-256("stratadb-kek-id\0" ‖ key).
    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(b"stratadb-kek-id\0");
        h.update(self.bytes());
        h.finalize()[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(<redacted>)")
    }
}

/// Fill `buf` from the system CSPRNG.
pub fn random(buf: &mut [u8]) -> Result<()> {
    SystemRandom::new()
        .fill(buf)
        .map_err(|_| Error::internal("system random number generator failed"))
}

/// Source of the key-encryption key. Implementations must not log the key.
pub trait KeyProvider: Send + Sync + std::fmt::Debug {
    /// The current KEK. Its [`SecretKey::fingerprint`] identifies it in the keyring.
    fn kek(&self) -> Result<SecretKey>;
    /// Human-readable description for diagnostics (never the key).
    fn describe(&self) -> String;
}

/// KEK read from a file holding 64 hex characters. The file must not be
/// readable or writable by group or others (mode `0600` or `0400`).
#[derive(Debug, Clone)]
pub struct FileKeyProvider {
    path: PathBuf,
}

impl FileKeyProvider {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl KeyProvider for FileKeyProvider {
    fn kek(&self) -> Result<SecretKey> {
        use std::os::unix::fs::PermissionsExt;
        let loc = self.path.display();
        let md = std::fs::metadata(&self.path)
            .map_err(|e| Error::io(format!("reading key file {loc}"), e))?;
        if !md.is_file() {
            return Err(Error::invalid_input(format!(
                "key file {loc} is not a regular file"
            )));
        }
        if md.permissions().mode() & 0o077 != 0 {
            return Err(Error::invalid_input(format!(
                "key file {loc} is accessible by group or others; run `chmod 600 {loc}`"
            )));
        }
        let text = Zeroizing::new(
            std::fs::read_to_string(&self.path)
                .map_err(|e| Error::io(format!("reading key file {loc}"), e))?,
        );
        SecretKey::from_hex(&text)
    }

    fn describe(&self) -> String {
        format!("key file {}", self.path.display())
    }
}

/// KEK held in memory (tests and embedding applications).
#[derive(Debug, Clone)]
pub struct StaticKeyProvider(pub SecretKey);

impl KeyProvider for StaticKeyProvider {
    fn kek(&self) -> Result<SecretKey> {
        Ok(self.0.clone())
    }
    fn describe(&self) -> String {
        "in-memory key".into()
    }
}

/// Create a new key file with a random 256-bit key (mode 0600; never overwrites).
pub fn generate_key_file(path: &Path) -> Result<SecretKey> {
    use std::io::Write;
    let key = SecretKey::generate()?;
    let hex: Zeroizing<String> =
        Zeroizing::new(key.bytes().iter().map(|b| format!("{b:02x}")).collect());
    let mut f = stratadb_common::fsutil::create_new_file(path)?;
    f.write_all(hex.as_bytes())
        .and_then(|()| f.write_all(b"\n"))
        .map_err(|e| Error::io(format!("writing {}", path.display()), e))?;
    stratadb_common::fsutil::sync_file(&f, path, false)
        .map_err(|e| Error::io(format!("fsyncing {}", path.display()), e))?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        stratadb_common::fsutil::fsync_dir(parent)?;
    }
    Ok(key)
}

/// AES-256-GCM key derived as HKDF-SHA256(ikm = `key`, salt, info).
pub(crate) fn derive(key: &SecretKey, salt: &[u8], info: &[u8]) -> Result<LessSafeKey> {
    let prk = hkdf::Salt::new(hkdf::HKDF_SHA256, salt).extract(key.bytes());
    let info = [info];
    let okm = prk
        .expand(&info, &AES_256_GCM)
        .map_err(|_| Error::internal("HKDF expand failed"))?;
    Ok(LessSafeKey::new(UnboundKey::from(okm)))
}

pub(crate) fn raw_key(key: &SecretKey) -> Result<LessSafeKey> {
    let k = UnboundKey::new(&AES_256_GCM, key.bytes())
        .map_err(|_| Error::internal("invalid AES-256 key"))?;
    Ok(LessSafeKey::new(k))
}

pub(crate) fn counter_nonce(n: u64) -> Nonce {
    let mut b = [0u8; aead::NONCE_LEN];
    b[4..].copy_from_slice(&n.to_le_bytes());
    Nonce::assume_unique_for_key(b)
}

/// Authentication failure: wrong key, corruption, or tampering.
pub(crate) fn auth_failed(loc: &str) -> Error {
    Error::corruption(
        loc,
        "authenticated decryption failed (wrong key, corruption or tampering)",
    )
}

const WAL_INFO: &[u8] = b"stratadb/wal-segment/v1";

/// Encrypts the payloads of one WAL segment. Nonce = record sequence number;
/// the caller guarantees a sequence number is sealed at most once per key
/// (one key per segment file, segments never reopened for append).
pub struct FrameKey(LessSafeKey);

impl std::fmt::Debug for FrameKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrameKey(<redacted>)")
    }
}

impl FrameKey {
    pub fn new(dek: &SecretKey, salt: &[u8; SALT_LEN]) -> Result<Self> {
        Ok(Self(derive(dek, salt, WAL_INFO)?))
    }

    /// Encrypt `payload` in place and append the tag.
    pub fn seal(&self, seq: u64, aad: &[u8], payload: &mut Vec<u8>) -> Result<()> {
        self.0
            .seal_in_place_append_tag(counter_nonce(seq), Aad::from(aad), payload)
            .map_err(|_| Error::internal("WAL frame encryption failed"))
    }

    /// Decrypt `ct ‖ tag` in place; returns the plaintext.
    pub fn open<'a>(&self, seq: u64, aad: &[u8], ct: &'a mut [u8], loc: &str) -> Result<&'a [u8]> {
        self.0
            .open_in_place(counter_nonce(seq), Aad::from(aad), ct)
            .map(|p| &*p)
            .map_err(|_| auth_failed(loc))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn hex_keys_and_fingerprints() {
        let k = SecretKey::from_hex(&"ab".repeat(32)).unwrap();
        assert_eq!(k.bytes(), &[0xab; 32]);
        assert_eq!(k.fingerprint().len(), 16);
        assert_ne!(
            k.fingerprint(),
            SecretKey::from_bytes([1; 32]).fingerprint()
        );
        assert!(SecretKey::from_hex("abc").is_err());
        assert!(SecretKey::from_hex(&"zz".repeat(32)).is_err());
        assert_eq!(format!("{k:?}"), "SecretKey(<redacted>)");
    }

    #[test]
    fn key_file_permissions_are_enforced() {
        use std::os::unix::fs::PermissionsExt;
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("kek");
        let k = generate_key_file(&p).unwrap();
        assert_eq!(
            std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let prov = FileKeyProvider::new(&p);
        assert_eq!(prov.kek().unwrap().bytes(), k.bytes());
        assert!(generate_key_file(&p).is_err(), "never overwrites");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        let e = prov.kek().unwrap_err();
        assert!(e.to_string().contains("chmod 600"), "{e}");
    }

    #[test]
    fn frame_keys_round_trip_and_bind_everything() {
        let dek = SecretKey::from_bytes([7; 32]);
        let fk = FrameKey::new(&dek, &[1; 32]).unwrap();
        let mut p = b"payload".to_vec();
        fk.seal(5, b"aad", &mut p).unwrap();
        assert_eq!(p.len(), 7 + TAG_LEN);
        let mut c = p.clone();
        assert_eq!(fk.open(5, b"aad", &mut c, "t").unwrap(), b"payload");
        // Wrong seq, aad, salt, or a flipped bit: rejected.
        assert!(fk.open(6, b"aad", &mut p.clone(), "t").is_err());
        assert!(fk.open(5, b"aaX", &mut p.clone(), "t").is_err());
        let other = FrameKey::new(&dek, &[2; 32]).unwrap();
        assert!(other.open(5, b"aad", &mut p.clone(), "t").is_err());
        let mut flipped = p.clone();
        flipped[0] ^= 1;
        assert!(fk.open(5, b"aad", &mut flipped, "t").is_err());
    }
}
