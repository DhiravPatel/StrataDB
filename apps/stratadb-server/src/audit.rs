//! Tamper-evident audit log (spec §18.7, ticket H-004).
//!
//! File: `<data_dir>/audit/audit.log` (0600), one JSON object per line:
//! `{"hash": "<sha256 hex>", "record": {...}}` where `record.prev` is the
//! previous line's hash (`"genesis"` for the first) and `hash =
//! SHA-256(serialized record)`. Editing, reordering, or deleting a line
//! breaks the chain, which [`verify`] detects. This is tamper-*evident*, not
//! tamper-proof: an attacker with write access could rewrite the whole file;
//! ship the log to a separate system for stronger guarantees.
//!
//! Records never contain secrets (tokens) or log payloads. Appends from
//! several processes (server + token CLI) are serialized with an exclusive
//! file lock, and the chain tail is re-read under the lock.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use stratadb_common::{fsutil, time, Error, IoResultExt, Result};

const GENESIS: &str = "genesis";

#[derive(Debug)]
pub struct AuditLog {
    path: PathBuf,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hash_record(record: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(record).map_err(|e| Error::internal(e.to_string()))?;
    Ok(hex(&Sha256::digest(&bytes)))
}

/// Read the last line of `f` (records are small; scan back at most 1 MiB).
fn last_line(f: &mut File) -> Result<Option<String>> {
    let len = f.metadata().ctx(|| "stat audit log".to_owned())?.len();
    if len == 0 {
        return Ok(None);
    }
    let start = len.saturating_sub(1 << 20);
    f.seek(SeekFrom::Start(start))
        .ctx(|| "seek audit log".to_owned())?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)
        .ctx(|| "read audit log".to_owned())?;
    Ok(buf
        .trim_end_matches('\n')
        .rsplit('\n')
        .next()
        .map(str::to_owned))
}

impl AuditLog {
    pub fn path_for(data_dir: &Path) -> PathBuf {
        data_dir.join("audit").join("audit.log")
    }

    pub fn open(data_dir: &Path) -> Result<Self> {
        let path = Self::path_for(data_dir);
        if let Some(p) = path.parent() {
            fsutil::create_dir_durable(p)?;
        }
        Ok(Self { path })
    }

    /// Append a record and fsync it. `details` must not contain secrets.
    pub fn record(&self, event: &str, actor: Option<(&str, &str)>, details: Value) -> Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .mode(fsutil::FILE_MODE)
            .open(&self.path)
            .ctx(|| format!("opening {}", self.path.display()))?;
        f.lock().ctx(|| "locking audit log".to_owned())?;
        let (prev, seq) = match last_line(&mut f)? {
            None => (GENESIS.to_owned(), 0u64),
            Some(line) => {
                let v: Value = serde_json::from_str(&line).map_err(|e| {
                    Error::corruption(
                        self.path.display().to_string(),
                        format!("last audit line: {e}"),
                    )
                })?;
                let h = v["hash"].as_str().unwrap_or_default().to_owned();
                (h, v["record"]["seq"].as_u64().unwrap_or(0) + 1)
            }
        };
        let record = json!({
            "seq": seq,
            "ts": time::format_rfc3339(time::now_unix_nanos()),
            "event": event,
            "token_id": actor.map(|a| a.0),
            "tenant": actor.map(|a| a.1),
            "details": details,
            "prev": prev,
        });
        let line = json!({"hash": hash_record(&record)?, "record": record});
        let mut bytes = serde_json::to_vec(&line).map_err(|e| Error::internal(e.to_string()))?;
        bytes.push(b'\n');
        f.write_all(&bytes)
            .ctx(|| "appending audit record".to_owned())?;
        f.sync_data().ctx(|| "fsyncing audit log".to_owned())?;
        Ok(())
    }
}

/// Verify the hash chain; returns the number of records.
pub fn verify(data_dir: &Path) -> Result<u64> {
    let path = AuditLog::path_for(data_dir);
    let loc = path.display().to_string();
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(Error::io(format!("reading {loc}"), e)),
    };
    let mut prev = GENESIS.to_owned();
    let mut n = 0u64;
    for (i, line) in text.lines().enumerate() {
        let bad = |why: &str| Error::corruption(&loc, format!("audit line {}: {why}", i + 1));
        let v: Value = serde_json::from_str(line).map_err(|_| bad("not JSON"))?;
        let rec = &v["record"];
        if rec["prev"].as_str() != Some(prev.as_str()) {
            return Err(bad("chain broken (previous record missing or altered)"));
        }
        if rec["seq"].as_u64() != Some(n) {
            return Err(bad("sequence gap"));
        }
        let h = hash_record(rec)?;
        if v["hash"].as_str() != Some(h.as_str()) {
            return Err(bad("record hash mismatch (record altered)"));
        }
        prev = h;
        n += 1;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_detects_edits_deletions_and_reordering() {
        let dir = tempfile::tempdir().unwrap();
        let log = AuditLog::open(dir.path()).unwrap();
        for i in 0..4 {
            log.record("test", Some(("tok1", "acme")), json!({"i": i}))
                .unwrap();
        }
        assert_eq!(verify(dir.path()).unwrap(), 4);
        let path = AuditLog::path_for(dir.path());
        let orig = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = orig.lines().collect();
        let cases = [
            orig.replace("\"i\":2", "\"i\":9"),
            format!("{}\n{}\n{}\n", lines[0], lines[1], lines[3]),
            format!("{}\n{}\n{}\n{}\n", lines[0], lines[2], lines[1], lines[3]),
        ];
        for bad in cases {
            std::fs::write(&path, bad).unwrap();
            assert!(verify(dir.path()).is_err());
        }
        // Truncating the tail is detectable only by an external record of the
        // latest seq/hash; the remaining prefix still verifies.
        std::fs::write(&path, format!("{}\n", lines[0])).unwrap();
        assert_eq!(verify(dir.path()).unwrap(), 1);
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
