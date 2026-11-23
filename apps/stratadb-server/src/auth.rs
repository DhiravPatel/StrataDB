//! API tokens and permissions (spec §18.2, §18.3; ADR-0013, ADR-0018).
//!
//! Token format: `sdb_<id: 16 hex>_<secret: 64 hex>` (256-bit secret from the
//! OS CSPRNG). Only `SHA-256(secret)` is stored. Verification looks up the id
//! and compares digests in constant time. Plaintext tokens are shown once at
//! creation and never logged.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use stratadb_common::{fsutil, time, Error, ErrorKind, Result, TenantId};
use subtle::ConstantTimeEq;

/// RBAC permissions (spec §18.3). Persisted as strings; never renumber/rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Permission {
    #[serde(rename = "ingest")]
    Ingest,
    #[serde(rename = "query")]
    Query,
    #[serde(rename = "schema.read")]
    SchemaRead,
    #[serde(rename = "schema.write")]
    SchemaWrite,
    #[serde(rename = "index.manage")]
    IndexManage,
    #[serde(rename = "retention.manage")]
    RetentionManage,
    #[serde(rename = "backup.create")]
    BackupCreate,
    #[serde(rename = "backup.restore")]
    BackupRestore,
    #[serde(rename = "user.manage")]
    UserManage,
    #[serde(rename = "cluster.read")]
    ClusterRead,
    #[serde(rename = "cluster.admin")]
    ClusterAdmin,
    #[serde(rename = "security.audit")]
    SecurityAudit,
}

impl Permission {
    pub const ALL: [Permission; 12] = [
        Permission::Ingest,
        Permission::Query,
        Permission::SchemaRead,
        Permission::SchemaWrite,
        Permission::IndexManage,
        Permission::RetentionManage,
        Permission::BackupCreate,
        Permission::BackupRestore,
        Permission::UserManage,
        Permission::ClusterRead,
        Permission::ClusterAdmin,
        Permission::SecurityAudit,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Permission::Ingest => "ingest",
            Permission::Query => "query",
            Permission::SchemaRead => "schema.read",
            Permission::SchemaWrite => "schema.write",
            Permission::IndexManage => "index.manage",
            Permission::RetentionManage => "retention.manage",
            Permission::BackupCreate => "backup.create",
            Permission::BackupRestore => "backup.restore",
            Permission::UserManage => "user.manage",
            Permission::ClusterRead => "cluster.read",
            Permission::ClusterAdmin => "cluster.admin",
            Permission::SecurityAudit => "security.audit",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|p| p.as_str() == s.trim())
            .ok_or_else(|| Error::invalid_input(format!("unknown permission {s:?}")))
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenRecord {
    pub id: String,
    pub tenant: TenantId,
    pub permissions: BTreeSet<Permission>,
    /// Lowercase hex SHA-256 of the secret part.
    pub sha256: String,
    pub created_ms: i64,
    pub expires_ms: Option<i64>,
    pub revoked: bool,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenFile {
    format_version: u32,
    tokens: Vec<TokenRecord>,
}

/// An authenticated caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub token_id: String,
    pub tenant: TenantId,
    pub permissions: BTreeSet<Permission>,
}

impl Principal {
    pub fn require(&self, p: Permission) -> Result<()> {
        if self.permissions.contains(&p) {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::PermissionDenied,
                format!("token lacks the `{p}` permission"),
            ))
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

fn random_hex(bytes: usize) -> Result<String> {
    let mut b = vec![0u8; bytes];
    getrandom::fill(&mut b)
        .map_err(|e| Error::internal(format!("OS random number generator failed: {e}")))?;
    Ok(hex(&b))
}

/// Token store backed by `<data_dir>/auth/tokens.json` (0600, atomic replace).
#[derive(Debug, Clone)]
pub struct TokenStore {
    path: PathBuf,
    by_id: HashMap<String, TokenRecord>,
    /// Modification time of the file when loaded (for hot reload).
    mtime: Option<std::time::SystemTime>,
}

pub const TOKEN_PREFIX: &str = "sdb_";

impl TokenStore {
    pub fn path_for(data_dir: &Path) -> PathBuf {
        data_dir.join("auth").join("tokens.json")
    }

    /// Load the store; a missing file is an empty store.
    pub fn load(data_dir: &Path) -> Result<Self> {
        let path = Self::path_for(data_dir);
        let by_id = match std::fs::read(&path) {
            Ok(b) => {
                let f: TokenFile = serde_json::from_slice(&b)
                    .map_err(|e| Error::corruption(path.display().to_string(), e.to_string()))?;
                if f.format_version != 1 {
                    return Err(Error::FormatVersion {
                        what: "token file",
                        found: f.format_version,
                        supported: "1".into(),
                    });
                }
                f.tokens.into_iter().map(|t| (t.id.clone(), t)).collect()
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(Error::io(format!("reading {}", path.display()), e)),
        };
        let mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
        Ok(Self { path, by_id, mtime })
    }

    fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fsutil::create_dir_durable(parent)?;
        }
        let mut tokens: Vec<TokenRecord> = self.by_id.values().cloned().collect();
        tokens.sort_by(|a, b| a.id.cmp(&b.id));
        let bytes = serde_json::to_vec_pretty(&TokenFile {
            format_version: 1,
            tokens,
        })
        .map_err(|e| Error::internal(e.to_string()))?;
        fsutil::atomic_write(&self.path, &bytes, "tokens")
    }

    /// Create a token. Returns the record and the plaintext token (show once).
    pub fn create(
        &mut self,
        tenant: TenantId,
        permissions: BTreeSet<Permission>,
        expires_ms: Option<i64>,
        description: Option<String>,
    ) -> Result<(TokenRecord, String)> {
        if permissions.is_empty() {
            return Err(Error::invalid_input(
                "a token needs at least one permission",
            ));
        }
        let id = random_hex(8)?;
        let secret = random_hex(32)?;
        let rec = TokenRecord {
            id: id.clone(),
            tenant,
            permissions,
            sha256: hex(&Sha256::digest(secret.as_bytes())),
            created_ms: time::now_unix_millis(),
            expires_ms,
            revoked: false,
            description,
        };
        self.by_id.insert(id.clone(), rec.clone());
        self.save()?;
        Ok((rec, format!("{TOKEN_PREFIX}{id}_{secret}")))
    }

    /// True if the file on disk changed since this store was loaded.
    pub fn changed_on_disk(&self) -> bool {
        std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok()
            != self.mtime
    }

    /// Directory the store was loaded from (`<data_dir>`).
    pub fn data_dir(&self) -> Option<&Path> {
        self.path.parent().and_then(Path::parent)
    }

    pub fn revoke(&mut self, id: &str) -> Result<()> {
        let rec = self
            .by_id
            .get_mut(id)
            .ok_or_else(|| Error::not_found(format!("no token with id {id}")))?;
        rec.revoked = true;
        self.save()
    }

    pub fn list(&self) -> Vec<&TokenRecord> {
        let mut v: Vec<&TokenRecord> = self.by_id.values().collect();
        v.sort_by(|a, b| a.id.cmp(&b.id));
        v
    }

    /// Verify a presented token. All failures return the same
    /// `Unauthenticated` message so callers cannot probe which part failed.
    pub fn verify(&self, presented: &str) -> Result<Principal> {
        let fail = || Error::new(ErrorKind::Unauthenticated, "invalid or expired API token");
        let rest = presented.strip_prefix(TOKEN_PREFIX).ok_or_else(fail)?;
        let (id, secret) = rest.split_once('_').ok_or_else(fail)?;
        let rec = self.by_id.get(id).ok_or_else(fail)?;
        let want = unhex(&rec.sha256).ok_or_else(fail)?;
        let got = Sha256::digest(secret.as_bytes());
        if !bool::from(got.as_slice().ct_eq(&want)) {
            return Err(fail());
        }
        if rec.revoked || rec.expires_ms.is_some_and(|t| t <= time::now_unix_millis()) {
            return Err(fail());
        }
        Ok(Principal {
            token_id: rec.id.clone(),
            tenant: rec.tenant.clone(),
            permissions: rec.permissions.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn create_verify_revoke_expire() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = TokenStore::load(dir.path()).unwrap();
        let t = TenantId::new("acme").unwrap();
        let (rec, tok) = s
            .create(t.clone(), BTreeSet::from([Permission::Ingest]), None, None)
            .unwrap();
        assert!(tok.starts_with("sdb_") && tok.len() == 4 + 16 + 1 + 64);
        let p = s.verify(&tok).unwrap();
        assert_eq!(p.tenant, t);
        assert!(p.require(Permission::Ingest).is_ok());
        assert_eq!(
            p.require(Permission::Query).unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
        // File holds no plaintext and is owner-only.
        let path = TokenStore::path_for(dir.path());
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(!raw.contains(tok.rsplit('_').next().unwrap()));
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // Reload from disk.
        let s2 = TokenStore::load(dir.path()).unwrap();
        assert!(s2.verify(&tok).is_ok());
        // Tampered / malformed tokens.
        let mut bad = tok.clone();
        bad.pop();
        bad.push(if tok.ends_with('0') { '1' } else { '0' });
        for b in [
            bad.as_str(),
            "",
            "sdb_",
            "Bearer x",
            &format!("sdb_{}_", rec.id),
        ] {
            assert_eq!(s.verify(b).unwrap_err().kind(), ErrorKind::Unauthenticated);
        }
        s.revoke(&rec.id).unwrap();
        assert!(s.verify(&tok).is_err());
        let (_, expired) = s
            .create(t, BTreeSet::from([Permission::Query]), Some(1), None)
            .unwrap();
        assert!(s.verify(&expired).is_err());
    }

    #[test]
    fn permissions_roundtrip() {
        for p in Permission::ALL {
            assert_eq!(Permission::parse(p.as_str()).unwrap(), p);
        }
        assert!(Permission::parse("root").is_err());
        assert!(TokenStore::load(tempfile::tempdir().unwrap().path())
            .unwrap()
            .create(TenantId::new("t").unwrap(), BTreeSet::new(), None, None)
            .is_err());
    }
}
