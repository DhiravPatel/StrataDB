//! Connection registry: the StrataDB servers the studio proxies to.
//!
//! Stored as `<data-dir>/connections.toml` (format version 1, see
//! docs/studio/README.md). The file holds API tokens, so:
//! - the data directory is created `0700` and the file is written `0600`;
//! - every change is written atomically: a new file in the same directory
//!   (created `0600`), `fsync`, `rename` over the old one, `fsync` the
//!   directory. A crash leaves either the old or the new registry;
//! - tokens are never returned by [`Registry::list`] (only `token_set`), never
//!   logged ([`Connection`]'s `Debug` masks them) and never put in URLs.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{Result, StudioError};

pub const FILE_NAME: &str = "connections.toml";
pub const FORMAT_VERSION: u32 = 1;
/// Most connections a registry holds.
pub const MAX_CONNECTIONS: usize = 256;
const MAX_TOKEN_LEN: usize = 4096;
const MAX_URL_LEN: usize = 2048;
const MAX_FILE_BYTES: u64 = 4 << 20;

/// One StrataDB server.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connection {
    pub name: String,
    /// `http(s)://host[:port]`, normalized (no path, query or trailing slash).
    pub url: String,
    /// API token (`Authorization: Bearer`); never leaves the studio process.
    pub token: String,
    #[serde(default = "yes")]
    pub tls_verify: bool,
    /// PEM file with the CA certificates to trust instead of the WebPKI roots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
}

fn yes() -> bool {
    true
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("name", &self.name)
            .field("url", &self.url)
            .field("token", &"***")
            .field("tls_verify", &self.tls_verify)
            .field("ca_file", &self.ca_file)
            .finish()
    }
}

/// What the browser sees of a connection: everything but the token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConnectionView {
    pub name: String,
    pub url: String,
    pub tls_verify: bool,
    pub ca_file: Option<PathBuf>,
    pub token_set: bool,
}

impl From<&Connection> for ConnectionView {
    fn from(c: &Connection) -> Self {
        Self {
            name: c.name.clone(),
            url: c.url.clone(),
            tls_verify: c.tls_verify,
            ca_file: c.ca_file.clone(),
            token_set: !c.token.is_empty(),
        }
    }
}

/// A change to an existing connection; `None` fields are kept. A new token
/// can be set but the old one is never read back.
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionPatch {
    pub url: Option<String>,
    pub token: Option<String>,
    pub tls_verify: Option<bool>,
    /// `Some("")` clears the CA file.
    pub ca_file: Option<String>,
}

impl std::fmt::Debug for ConnectionPatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionPatch")
            .field("url", &self.url)
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("tls_verify", &self.tls_verify)
            .field("ca_file", &self.ca_file)
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileFormat {
    version: u32,
    #[serde(default, rename = "connection")]
    connections: Vec<Connection>,
}

/// Validate a connection name: `[A-Za-z0-9_-]{1,64}`, not starting with `-`.
pub fn validate_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && !name.starts_with('-')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(StudioError::invalid(
            "connection name must be 1-64 characters of [A-Za-z0-9_-] and not start with '-'",
        ))
    }
}

/// Validate and normalize a server base URL to `scheme://authority`.
pub fn normalize_url(raw: &str) -> Result<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw.len() > MAX_URL_LEN {
        return Err(StudioError::invalid("server URL is empty or too long"));
    }
    let uri: axum::http::Uri = raw
        .parse()
        .map_err(|e| StudioError::invalid(format!("invalid server URL: {e}")))?;
    let scheme = match uri.scheme_str() {
        Some(s) if s.eq_ignore_ascii_case("http") => "http",
        Some(s) if s.eq_ignore_ascii_case("https") => "https",
        _ => {
            return Err(StudioError::invalid(
                "server URL must start with http:// or https://",
            ))
        }
    };
    let Some(auth) = uri.authority() else {
        return Err(StudioError::invalid("server URL has no host"));
    };
    if auth.as_str().contains('@') {
        return Err(StudioError::invalid(
            "server URL must not contain credentials; set the API token instead",
        ));
    }
    if auth.host().is_empty() {
        return Err(StudioError::invalid("server URL has no host"));
    }
    if !matches!(uri.path(), "" | "/") || uri.query().is_some() {
        return Err(StudioError::invalid(
            "server URL must be only scheme://host[:port] (the studio adds /api/v1)",
        ));
    }
    Ok(format!("{scheme}://{}", auth.as_str().to_ascii_lowercase()))
}

/// Validate an API token: 1..=4096 visible ASCII characters.
pub fn validate_token(token: &str) -> Result<()> {
    if token.is_empty()
        || token.len() > MAX_TOKEN_LEN
        || !token.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(StudioError::invalid(
            "API token must be 1-4096 visible ASCII characters",
        ));
    }
    Ok(())
}

fn validate_ca_file(p: &Path) -> Result<()> {
    if !p.is_absolute() {
        return Err(StudioError::invalid("CA file must be an absolute path"));
    }
    if !p.is_file() {
        return Err(StudioError::invalid(format!(
            "CA file {} does not exist or is not a file",
            p.display()
        )));
    }
    Ok(())
}

impl Connection {
    /// Build and validate a new connection.
    pub fn new(
        name: &str,
        url: &str,
        token: &str,
        tls_verify: bool,
        ca_file: Option<PathBuf>,
    ) -> Result<Self> {
        validate_name(name)?;
        validate_token(token)?;
        if let Some(p) = &ca_file {
            validate_ca_file(p)?;
        }
        Ok(Self {
            name: name.to_owned(),
            url: normalize_url(url)?,
            token: token.to_owned(),
            tls_verify,
            ca_file,
        })
    }

    fn apply(&mut self, p: ConnectionPatch) -> Result<()> {
        let mut next = self.clone();
        if let Some(u) = p.url {
            next.url = normalize_url(&u)?;
        }
        if let Some(t) = p.token.filter(|t| !t.is_empty()) {
            validate_token(&t)?;
            next.token = t;
        }
        if let Some(v) = p.tls_verify {
            next.tls_verify = v;
        }
        match p.ca_file.as_deref() {
            None => {}
            Some("") => next.ca_file = None,
            Some(s) => {
                let path = PathBuf::from(s);
                validate_ca_file(&path)?;
                next.ca_file = Some(path);
            }
        }
        *self = next;
        Ok(())
    }
}

#[derive(Debug)]
struct Inner {
    conns: BTreeMap<String, Connection>,
}

/// The persistent set of connections.
#[derive(Debug)]
pub struct Registry {
    dir: PathBuf,
    path: PathBuf,
    inner: Mutex<Inner>,
}

impl Registry {
    /// Open (creating the data directory if needed) and load the registry.
    pub fn open(data_dir: &Path) -> Result<Self> {
        create_private_dir(data_dir)?;
        let path = data_dir.join(FILE_NAME);
        let conns = match std::fs::metadata(&path) {
            Ok(m) => {
                if m.len() > MAX_FILE_BYTES {
                    return Err(StudioError::invalid(format!(
                        "{} is larger than {MAX_FILE_BYTES} bytes",
                        path.display()
                    )));
                }
                restrict_mode(&path, &m)?;
                let text = std::fs::read_to_string(&path)
                    .map_err(|e| StudioError::io("reading the connection registry", &e))?;
                parse(&text).map_err(|e| {
                    StudioError::invalid(format!("parsing {}: {}", path.display(), e.message))
                })?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(StudioError::io("reading the connection registry", &e)),
        };
        Ok(Self {
            dir: data_dir.to_owned(),
            path,
            inner: Mutex::new(Inner { conns }),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Every connection, masked, in name order.
    pub fn list(&self) -> Vec<ConnectionView> {
        self.lock()
            .conns
            .values()
            .map(ConnectionView::from)
            .collect()
    }

    /// The full connection (with its token), for the proxy only.
    pub fn get(&self, name: &str) -> Option<Connection> {
        self.lock().conns.get(name).cloned()
    }

    pub fn view(&self, name: &str) -> Option<ConnectionView> {
        self.lock().conns.get(name).map(ConnectionView::from)
    }

    /// Add a connection; its name must be new.
    pub fn add(&self, c: Connection) -> Result<ConnectionView> {
        let mut g = self.lock();
        if g.conns.contains_key(&c.name) {
            return Err(StudioError::conflict(format!(
                "a connection named {:?} already exists",
                c.name
            )));
        }
        if g.conns.len() >= MAX_CONNECTIONS {
            return Err(StudioError::exhausted(format!(
                "at most {MAX_CONNECTIONS} connections"
            )));
        }
        let view = ConnectionView::from(&c);
        let mut next = g.conns.clone();
        next.insert(c.name.clone(), c);
        self.persist(&next)?;
        g.conns = next;
        Ok(view)
    }

    /// Change an existing connection.
    pub fn update(&self, name: &str, patch: ConnectionPatch) -> Result<ConnectionView> {
        let mut g = self.lock();
        let Some(mut c) = g.conns.get(name).cloned() else {
            return Err(StudioError::not_found(format!(
                "no connection named {name:?}"
            )));
        };
        c.apply(patch)?;
        let view = ConnectionView::from(&c);
        let mut next = g.conns.clone();
        next.insert(name.to_owned(), c);
        self.persist(&next)?;
        g.conns = next;
        Ok(view)
    }

    /// Remove a connection.
    pub fn remove(&self, name: &str) -> Result<()> {
        let mut g = self.lock();
        if !g.conns.contains_key(name) {
            return Err(StudioError::not_found(format!(
                "no connection named {name:?}"
            )));
        }
        let mut next = g.conns.clone();
        next.remove(name);
        self.persist(&next)?;
        g.conns = next;
        Ok(())
    }

    /// Write the registry atomically with mode 0600 (called with the lock held,
    /// so writers are serialized; memory changes only after the file is durable).
    fn persist(&self, conns: &BTreeMap<String, Connection>) -> Result<()> {
        let file = FileFormat {
            version: FORMAT_VERSION,
            connections: conns.values().cloned().collect(),
        };
        let text = toml::to_string(&file)
            .map_err(|e| StudioError::internal(format!("encoding the registry: {e}")))?;
        let body = format!(
            "# StrataStudio connection registry (format {FORMAT_VERSION}). Contains API tokens: keep it 0600.\n{text}"
        );
        write_private_atomic(&self.dir, &self.path, body.as_bytes())
    }
}

fn parse(text: &str) -> Result<BTreeMap<String, Connection>> {
    let f: FileFormat = toml::from_str(text).map_err(|e| StudioError::invalid(format!("{e}")))?;
    if f.version != FORMAT_VERSION {
        return Err(StudioError::invalid(format!(
            "unsupported registry format version {} (this build reads {FORMAT_VERSION})",
            f.version
        )));
    }
    let mut out = BTreeMap::new();
    for c in f.connections {
        validate_name(&c.name)?;
        validate_token(&c.token)?;
        let url = normalize_url(&c.url)?;
        let name = c.name.clone();
        if out.insert(name.clone(), Connection { url, ..c }).is_some() {
            return Err(StudioError::invalid(format!(
                "duplicate connection name {name:?}"
            )));
        }
    }
    Ok(out)
}

/// Create `dir` (and parents) with mode 0700 for the leaf.
pub fn create_private_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        match std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
        {
            Ok(()) => Ok(()),
            Err(e) => Err(StudioError::io("creating the data directory", &e)),
        }
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(dir).map_err(|e| StudioError::io("creating the data directory", &e))
    }
}

#[cfg(unix)]
fn restrict_mode(path: &Path, m: &std::fs::Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if m.permissions().mode() & 0o077 != 0 {
        tracing::warn!(
            component = "registry",
            path = %path.display(),
            "connection registry was readable by group or others; restricting to 0600"
        );
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| StudioError::io("restricting registry permissions", &e))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn restrict_mode(_path: &Path, _m: &std::fs::Metadata) -> Result<()> {
    Ok(())
}

fn random_suffix() -> String {
    let mut b = [0u8; 8];
    // A collision only makes `create_new` fail; fall back to the pid.
    if getrandom::fill(&mut b).is_err() {
        return format!("{}", std::process::id());
    }
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Write `bytes` to `path` atomically: temp file (0600) → fsync → rename → fsync dir.
pub fn write_private_atomic(dir: &Path, path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = dir.join(format!(".{FILE_NAME}.tmp-{}", random_suffix()));
    let res = (|| -> std::io::Result<()> {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut f = opts.open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)?;
        #[cfg(unix)]
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    })();
    if let Err(e) = res {
        let _ = std::fs::remove_file(&tmp);
        return Err(StudioError::io("writing the connection registry", &e));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn urls_are_normalized_and_validated() {
        assert_eq!(
            normalize_url("HTTP://LocalHost:7070/").unwrap(),
            "http://localhost:7070"
        );
        assert_eq!(
            normalize_url(" https://db.example:443 ").unwrap(),
            "https://db.example:443"
        );
        for bad in [
            "",
            "localhost:7070",
            "ftp://x",
            "http://u:p@x:1",
            "http://x/api/v1",
            "http://x/?a=1",
            "http:///",
        ] {
            assert!(normalize_url(bad).is_err(), "{bad:?} accepted");
        }
    }

    #[test]
    fn names_and_tokens_are_validated() {
        assert!(validate_name("prod_eu-1").is_ok());
        for bad in ["", "-x", "a b", "a/b", &"x".repeat(65)] {
            assert!(validate_name(bad).is_err(), "{bad:?}");
        }
        assert!(validate_token("sdb_abc").is_ok());
        assert!(validate_token("has space").is_err());
        assert!(validate_token("").is_err());
    }

    #[test]
    fn debug_never_prints_tokens() {
        let c = Connection::new("a", "http://h:1", "sdb_secret_token", true, None).unwrap();
        assert!(!format!("{c:?}").contains("sdb_secret_token"));
        let p = ConnectionPatch {
            token: Some("sdb_other_secret".into()),
            ..ConnectionPatch::default()
        };
        assert!(!format!("{p:?}").contains("sdb_other_secret"));
    }

    #[test]
    fn add_update_remove_persist_with_private_mode() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("studio");
        let r = Registry::open(&data).unwrap();
        assert!(r.list().is_empty());
        let v = r
            .add(Connection::new("local", "http://127.0.0.1:7070", "sdb_tok1", true, None).unwrap())
            .unwrap();
        assert!(v.token_set);
        assert!(r
            .add(Connection::new("local", "http://x:1", "t", true, None).unwrap())
            .is_err());
        r.update(
            "local",
            ConnectionPatch {
                url: Some("http://127.0.0.1:7071".into()),
                tls_verify: Some(false),
                ..ConnectionPatch::default()
            },
        )
        .unwrap();
        // Token kept when the patch has none.
        let r2 = Registry::open(&data).unwrap();
        let c = r2.get("local").unwrap();
        assert_eq!(
            (c.url.as_str(), c.token.as_str(), c.tls_verify),
            ("http://127.0.0.1:7071", "sdb_tok1", false)
        );
        // Masked list.
        let listed = serde_json::to_string(&r2.list()).unwrap();
        assert!(!listed.contains("sdb_tok1"), "{listed}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let m = std::fs::metadata(r2.path()).unwrap().permissions().mode();
            assert_eq!(m & 0o777, 0o600);
            let d = std::fs::metadata(&data).unwrap().permissions().mode();
            assert_eq!(d & 0o777, 0o700);
        }
        r2.remove("local").unwrap();
        assert!(r2.remove("local").is_err());
        assert!(Registry::open(&data).unwrap().list().is_empty());
        // No temp files left behind.
        let names: Vec<_> = std::fs::read_dir(&data)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec![FILE_NAME.to_owned()]);
    }

    #[test]
    fn bad_files_are_refused_and_loose_modes_tightened() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(FILE_NAME);
        std::fs::write(&p, "version = 9\n").unwrap();
        assert!(Registry::open(dir.path()).is_err());
        std::fs::write(
            &p,
            "version = 1\n[[connection]]\nname = \"a\"\nurl = \"http://h:1\"\ntoken = \"t\"\n",
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        let r = Registry::open(dir.path()).unwrap();
        assert!(r.get("a").unwrap().tls_verify);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let m = std::fs::metadata(&p).unwrap().permissions().mode();
            assert_eq!(m & 0o777, 0o600);
        }
    }
}
