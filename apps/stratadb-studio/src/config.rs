//! Studio configuration: TOML file, environment and command-line overrides.
//!
//! Precedence: flag > environment > file > default. Secrets (the studio
//! password) are read from the environment or the file, never from the
//! command line, so they do not appear in `ps` output or shell history.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

pub const DEFAULT_LISTEN: &str = "127.0.0.1:7171";
/// Environment variable holding the studio password (overrides the file).
pub const PASSWORD_ENV: &str = "STRATA_STUDIO_PASSWORD";
/// Shortest accepted studio password.
pub const MIN_PASSWORD_LEN: usize = 12;
const MAX_PASSWORD_LEN: usize = 1024;

/// A secret string. `Debug` never prints it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secret(***)")
    }
}

/// Bounds on everything the studio does on behalf of a browser.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Largest upstream response body read into memory.
    pub max_response_bytes: usize,
    /// Timeout of non-query upstream calls (health, metrics, admin).
    pub request_timeout: Duration,
    /// Upper bound of a query's upstream call; the per-request `timeout_ms`
    /// (plus a small margin) is capped at this.
    pub max_query_timeout: Duration,
    /// Long-running admin calls (compaction, backup).
    pub admin_timeout: Duration,
    /// Upstream calls in flight at once (each holds a blocking thread).
    pub max_concurrent_upstream: usize,
    /// How long a request waits for an upstream slot before 503.
    pub upstream_queue_timeout: Duration,
    /// Largest request body the browser may send.
    pub max_body_bytes: usize,
    /// Idle lifetime of a studio session.
    pub session_ttl: Duration,
    /// Sessions kept at once (oldest evicted).
    pub max_sessions: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_response_bytes: 32 << 20,
            request_timeout: Duration::from_secs(15),
            max_query_timeout: Duration::from_secs(310),
            admin_timeout: Duration::from_secs(600),
            max_concurrent_upstream: 32,
            upstream_queue_timeout: Duration::from_secs(5),
            max_body_bytes: 1 << 20,
            session_ttl: Duration::from_secs(12 * 3600),
            max_sessions: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// The effective configuration.
#[derive(Debug, Clone)]
pub struct StudioConfig {
    pub listen: SocketAddr,
    pub data_dir: PathBuf,
    /// Studio password; `Some` enables login (mandatory off loopback).
    pub password: Option<Secret>,
    pub tls: Option<TlsFiles>,
    /// Allow a non-loopback listener without TLS (behind a TLS-terminating
    /// reverse proxy). Authentication is still required.
    pub allow_insecure_http: bool,
    /// Extra `Host` names accepted on a loopback listener (DNS-rebinding
    /// defence; see `auth::check_host`).
    pub allowed_hosts: Vec<String>,
    pub limits: Limits,
}

impl StudioConfig {
    /// A loopback configuration with defaults (used by tests and `connection` commands).
    pub fn local(data_dir: PathBuf) -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 7171)),
            data_dir,
            password: None,
            tls: None,
            allow_insecure_http: false,
            allowed_hosts: Vec::new(),
            limits: Limits::default(),
        }
    }

    pub fn auth_required(&self) -> bool {
        self.password.is_some()
    }

    /// Enforce the listener security policy (ADR-0041).
    pub fn validate(&self) -> Result<(), String> {
        if let Some(p) = &self.password {
            let n = p.expose().chars().count();
            if !(MIN_PASSWORD_LEN..=MAX_PASSWORD_LEN).contains(&n) {
                return Err(format!(
                    "the studio password must be {MIN_PASSWORD_LEN}..={MAX_PASSWORD_LEN} characters"
                ));
            }
        }
        if !self.listen.ip().is_loopback() {
            if self.password.is_none() {
                return Err(format!(
                    "listening on non-loopback address {} requires studio authentication: \
                     set {PASSWORD_ENV} or [auth] password_file in the config file",
                    self.listen
                ));
            }
            if self.tls.is_none() && !self.allow_insecure_http {
                return Err(format!(
                    "listening on non-loopback address {} requires TLS ([tls] cert/key), or \
                     allow_insecure_http = true behind a TLS-terminating proxy",
                    self.listen
                ));
            }
        }
        let l = &self.limits;
        if l.max_concurrent_upstream == 0 || l.max_concurrent_upstream > 1024 {
            return Err("limits.max_concurrent_upstream must be 1..=1024".into());
        }
        if l.max_response_bytes < 4096 || l.max_response_bytes > (1 << 30) {
            return Err("limits.max_response_bytes must be 4 KiB..=1 GiB".into());
        }
        if l.max_body_bytes < 1024 || l.max_body_bytes > (64 << 20) {
            return Err("limits.max_body_bytes must be 1 KiB..=64 MiB".into());
        }
        if l.max_sessions == 0 || l.max_sessions > 100_000 {
            return Err("limits.max_sessions must be 1..=100000".into());
        }
        Ok(())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen: Option<SocketAddr>,
    data_dir: Option<PathBuf>,
    allow_insecure_http: Option<bool>,
    allowed_hosts: Option<Vec<String>>,
    auth: Option<FileAuth>,
    tls: Option<FileTls>,
    limits: Option<FileLimits>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAuth {
    password: Option<String>,
    password_file: Option<PathBuf>,
}

impl std::fmt::Debug for FileAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileAuth")
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("password_file", &self.password_file)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileTls {
    cert: PathBuf,
    key: PathBuf,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileLimits {
    max_response_bytes: Option<usize>,
    request_timeout_ms: Option<u64>,
    max_query_timeout_ms: Option<u64>,
    admin_timeout_ms: Option<u64>,
    max_concurrent_upstream: Option<usize>,
    upstream_queue_timeout_ms: Option<u64>,
    max_body_bytes: Option<usize>,
    session_ttl_secs: Option<u64>,
    max_sessions: Option<usize>,
}

/// Command-line overrides.
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub config: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    pub data_dir: Option<PathBuf>,
}

/// The default data directory, `~/.strata-studio`.
pub fn default_data_dir(home: Option<&Path>) -> PathBuf {
    match home {
        Some(h) => h.join(".strata-studio"),
        None => PathBuf::from(".strata-studio"),
    }
}

fn expand_home(p: PathBuf, home: Option<&Path>) -> PathBuf {
    match (p.strip_prefix("~"), home) {
        (Ok(rest), Some(h)) => h.join(rest),
        _ => p,
    }
}

fn read_password_file(p: &Path) -> Result<Secret, String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(m) = std::fs::metadata(p) {
            if m.permissions().mode() & 0o077 != 0 {
                tracing::warn!(
                    component = "config",
                    path = %p.display(),
                    "password file is readable by group or others; chmod 600 it"
                );
            }
        }
    }
    let s = std::fs::read_to_string(p)
        .map_err(|e| format!("reading password file {}: {e}", p.display()))?;
    let line = s.lines().next().unwrap_or("").trim_end_matches('\r');
    if line.is_empty() {
        return Err(format!("password file {} is empty", p.display()));
    }
    Ok(Secret::new(line))
}

/// Resolve the effective configuration. `env_password` is the value of
/// [`PASSWORD_ENV`] and `home` the user's home directory.
pub fn resolve(
    o: &Overrides,
    env_password: Option<String>,
    home: Option<&Path>,
) -> Result<StudioConfig, String> {
    let file: FileConfig = match &o.config {
        Some(p) => {
            let text = std::fs::read_to_string(p)
                .map_err(|e| format!("reading config {}: {e}", p.display()))?;
            toml::from_str(&text).map_err(|e| format!("parsing config {}: {e}", p.display()))?
        }
        None => FileConfig::default(),
    };
    let listen = match o.listen.or(file.listen) {
        Some(a) => a,
        None => DEFAULT_LISTEN
            .parse()
            .map_err(|e| format!("default listen address: {e}"))?,
    };
    let data_dir = o
        .data_dir
        .clone()
        .or(file.data_dir)
        .map(|p| expand_home(p, home))
        .unwrap_or_else(|| default_data_dir(home));
    let auth = file.auth.unwrap_or_default();
    let password = match env_password.filter(|s| !s.is_empty()) {
        Some(p) => Some(Secret::new(p)),
        None => match (auth.password_file, auth.password) {
            (Some(_), Some(_)) => {
                return Err("[auth]: set either password or password_file, not both".into())
            }
            (Some(f), None) => Some(read_password_file(&expand_home(f, home))?),
            (None, Some(p)) => Some(Secret::new(p)),
            (None, None) => None,
        },
    };
    let d = Limits::default();
    let fl = file.limits.unwrap_or_default();
    let ms = |v: Option<u64>, def: Duration| v.map(Duration::from_millis).unwrap_or(def);
    let limits = Limits {
        max_response_bytes: fl.max_response_bytes.unwrap_or(d.max_response_bytes),
        request_timeout: ms(fl.request_timeout_ms, d.request_timeout),
        max_query_timeout: ms(fl.max_query_timeout_ms, d.max_query_timeout),
        admin_timeout: ms(fl.admin_timeout_ms, d.admin_timeout),
        max_concurrent_upstream: fl
            .max_concurrent_upstream
            .unwrap_or(d.max_concurrent_upstream),
        upstream_queue_timeout: ms(fl.upstream_queue_timeout_ms, d.upstream_queue_timeout),
        max_body_bytes: fl.max_body_bytes.unwrap_or(d.max_body_bytes),
        session_ttl: fl
            .session_ttl_secs
            .map(Duration::from_secs)
            .unwrap_or(d.session_ttl),
        max_sessions: fl.max_sessions.unwrap_or(d.max_sessions),
    };
    let cfg = StudioConfig {
        listen,
        data_dir,
        password,
        tls: file.tls.map(|t| TlsFiles {
            cert: expand_home(t.cert, home),
            key: expand_home(t.key, home),
        }),
        allow_insecure_http: file.allow_insecure_http.unwrap_or(false),
        allowed_hosts: file.allowed_hosts.unwrap_or_default(),
        limits,
    };
    cfg.validate()?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, text).unwrap();
        p
    }

    #[test]
    fn defaults_are_loopback_without_auth() {
        let c = resolve(&Overrides::default(), None, Some(Path::new("/home/u"))).unwrap();
        assert_eq!(c.listen.to_string(), DEFAULT_LISTEN);
        assert_eq!(c.data_dir, PathBuf::from("/home/u/.strata-studio"));
        assert!(!c.auth_required());
    }

    #[test]
    fn non_loopback_requires_password_and_tls() {
        let o = Overrides {
            listen: Some("0.0.0.0:7171".parse().unwrap()),
            ..Overrides::default()
        };
        let e = resolve(&o, None, None).unwrap_err();
        assert!(e.contains("requires studio authentication"), "{e}");
        let e = resolve(&o, Some("a-long-enough-password".into()), None).unwrap_err();
        assert!(e.contains("requires TLS"), "{e}");
        let dir = tempfile::tempdir().unwrap();
        let f = write(dir.path(), "s.toml", "allow_insecure_http = true\n");
        let o2 = Overrides {
            config: Some(f),
            ..o
        };
        let c = resolve(&o2, Some("a-long-enough-password".into()), None).unwrap();
        assert!(c.auth_required());
    }

    #[test]
    fn short_passwords_are_rejected() {
        let e = resolve(&Overrides::default(), Some("short".into()), None).unwrap_err();
        assert!(e.contains("characters"), "{e}");
    }

    #[test]
    fn file_settings_and_precedence() {
        let dir = tempfile::tempdir().unwrap();
        let pw = write(dir.path(), "pw", "file-password-123\n");
        let f = write(
            dir.path(),
            "s.toml",
            &format!(
                "listen = \"127.0.0.1:9000\"\ndata_dir = \"~/studio\"\n[auth]\npassword_file = \"{}\"\n[limits]\nmax_response_bytes = 65536\nrequest_timeout_ms = 2000\n",
                pw.display()
            ),
        );
        let o = Overrides {
            config: Some(f.clone()),
            ..Overrides::default()
        };
        let c = resolve(&o, None, Some(Path::new("/h"))).unwrap();
        assert_eq!(c.listen.port(), 9000);
        assert_eq!(c.data_dir, PathBuf::from("/h/studio"));
        assert_eq!(c.password.as_ref().unwrap().expose(), "file-password-123");
        assert_eq!(c.limits.max_response_bytes, 65536);
        assert_eq!(c.limits.request_timeout, Duration::from_secs(2));
        // Flags and the environment win over the file.
        let o = Overrides {
            config: Some(f),
            listen: Some("127.0.0.1:9001".parse().unwrap()),
            data_dir: Some(PathBuf::from("/d")),
        };
        let c = resolve(&o, Some("env-password-1234".into()), None).unwrap();
        assert_eq!(c.listen.port(), 9001);
        assert_eq!(c.data_dir, PathBuf::from("/d"));
        assert_eq!(c.password.unwrap().expose(), "env-password-1234");
    }

    #[test]
    fn unknown_keys_are_rejected_and_secrets_not_debug_printed() {
        let dir = tempfile::tempdir().unwrap();
        let f = write(dir.path(), "s.toml", "lisen = \"127.0.0.1:1\"\n");
        let o = Overrides {
            config: Some(f),
            ..Overrides::default()
        };
        assert!(resolve(&o, None, None)
            .unwrap_err()
            .contains("parsing config"));
        let c = resolve(
            &Overrides::default(),
            Some("super-secret-value".into()),
            None,
        )
        .unwrap();
        assert!(!format!("{c:?}").contains("super-secret-value"));
    }
}
