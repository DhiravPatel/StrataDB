//! Validated identifiers.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Maximum tenant identifier length in bytes.
pub const MAX_TENANT_ID_LEN: usize = 64;

/// A tenant identifier.
///
/// Tenant IDs are used as directory names in the storage layout, so the
/// accepted alphabet is deliberately restrictive: ASCII letters, digits, `_`
/// and `-`, not starting with `-`. This makes path traversal (`..`, `/`) and
/// hidden-file names impossible by construction (spec §18.1, §18.4).
///
/// A `TenantId` must be derived from an authenticated identity, never from an
/// event payload (spec §6.1).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TenantId(String);

impl TenantId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        validate_tenant_id(&id)?;
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn validate_tenant_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > MAX_TENANT_ID_LEN {
        return Err(Error::invalid_input(format!(
            "tenant id must be 1..={MAX_TENANT_ID_LEN} bytes"
        )));
    }
    if id.starts_with('-') {
        return Err(Error::invalid_input("tenant id must not start with '-'"));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error::invalid_input(
            "tenant id may contain only ASCII letters, digits, '_' and '-'",
        ));
    }
    Ok(())
}

impl TryFrom<String> for TenantId {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        TenantId::new(s)
    }
}

impl From<TenantId> for String {
    fn from(t: TenantId) -> String {
        t.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_safe_ids() {
        for ok in ["a", "tenant-a", "T_1", "x".repeat(64).as_str()] {
            TenantId::new(ok).unwrap();
        }
    }

    #[test]
    fn rejects_traversal_and_bad_chars() {
        for bad in [
            "",
            "..",
            "../etc",
            "a/b",
            "a\\b",
            "-rf",
            ".hidden",
            "a b",
            "ü",
            "a\0",
            &"x".repeat(65),
        ] {
            assert!(TenantId::new(bad).is_err(), "{bad:?} accepted");
        }
    }
}
