//! `strata` — StrataDB command-line tool.
//!
//! This tool opens a data directory **directly** (it takes the directory lock,
//! so the server must not be running). Filesystem access to the data directory
//! is equivalent to full administrative access; the `--tenant` flag is chosen
//! by the operator, not derived from an authenticated principal.

use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

mod doctor;
mod export;
mod hostcheck;
#[cfg(feature = "parquet")]
mod parquet_io;
mod shell;

use clap::{Parser, Subcommand};
use serde_json::{json, Map};
use stratadb_common::{time, Error, Result, TenantId};
use stratadb_query::{execute, Datum, QueryOptions};
use stratadb_schema::json::event_from_json;
use stratadb_schema::{Limits, Value};
use stratadb_storage::{Durability, Engine, EngineOptions, IdempotencyKey, RetentionPolicy};

#[derive(Parser)]
#[command(name = "strata", version, about = "StrataDB command-line tool")]
struct Cli {
    /// Key-encryption key file of an encrypted data directory (ADR-0024);
    /// defaults to $STRATADB_KEY_FILE. Giving it for an unencrypted directory
    /// enables encryption for new files.
    #[arg(long, global = true)]
    key_file: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

static KEY_FILE: std::sync::OnceLock<Option<PathBuf>> = std::sync::OnceLock::new();

/// The configured key provider, if any (`--key-file` or `STRATADB_KEY_FILE`).
fn key_provider() -> Option<std::sync::Arc<dyn stratadb_crypto::KeyProvider>> {
    KEY_FILE
        .get()
        .cloned()
        .flatten()
        .map(|p| std::sync::Arc::new(stratadb_crypto::FileKeyProvider::new(p)) as _)
}

#[derive(Subcommand)]
enum Cmd {
    /// Create (or recover) a data directory.
    Init {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Ingest newline-delimited JSON events (`-` reads stdin).
    Ingest {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value_t = 500)]
        batch_size: usize,
        /// ndjson | parquet (default: parquet for `*.parquet`, else ndjson).
        #[arg(long)]
        format: Option<String>,
        /// Make the import idempotent: batch i is written with idempotency key
        /// `import:<ID>:<i>`, so re-running an interrupted import of the same
        /// file skips batches that already committed (within the dedup window).
        #[arg(long)]
        import_id: Option<String>,
        /// Write a JSON validation report (counts and per-line errors) here.
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Export events (or a row query) to NDJSON or CSV, with checkpoint/resume.
    Export {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
        /// Output file (must not exist unless --resume).
        #[arg(long)]
        output: PathBuf,
        /// ndjson | csv | parquet (parquet: events only, no resume)
        #[arg(long, default_value = "ndjson")]
        format: String,
        /// Row query to export instead of the tenant's events.
        #[arg(long)]
        sql: Option<String>,
        /// Filter for an events export, e.g. "event_time >= TIMESTAMP '2026-11-01T00:00:00Z'".
        #[arg(long = "where")]
        filter: Option<String>,
        #[arg(long, default_value_t = 10_000)]
        page_size: usize,
        /// Continue an interrupted export from `<output>.checkpoint`.
        #[arg(long)]
        resume: bool,
        /// Stop after this many pages (leaves a checkpoint; finish with --resume).
        #[arg(long)]
        max_pages: Option<u64>,
    },
    /// Interactive StrataQL shell (statements end with `;`, `\help`).
    Shell {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
    },
    /// Run a query and print one JSON object per row.
    Query {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
        /// Query text (or use --file).
        sql: Option<String>,
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
        #[arg(long, default_value_t = 100_000)]
        max_rows: usize,
        /// Also print execution statistics to stderr.
        #[arg(long)]
        stats: bool,
    },
    /// Flush memtables (recovered from the WAL) into segments.
    Flush {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Inspect or verify segments.
    Segments {
        #[command(subcommand)]
        cmd: SegCmd,
    },
    /// Merge small segments (one pass per tenant) and delete obsolete files.
    Compact {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Manage per-tenant retention policies.
    Retention {
        #[command(subcommand)]
        cmd: RetCmd,
    },
    /// Create, verify, or restore backups.
    Backup {
        #[command(subcommand)]
        cmd: BackupCmd,
    },
    /// Upgrade on-disk formats to this binary's versions (spec §30.2).
    /// Irreversible for older binaries: requires --backup-to or --skip-backup.
    Upgrade {
        #[arg(long)]
        data_dir: PathBuf,
        /// Report what would change without modifying anything.
        #[arg(long)]
        dry_run: bool,
        /// Create and verify a backup here before upgrading.
        #[arg(long)]
        backup_to: Option<PathBuf>,
        /// Proceed without a backup (you accept that downgrade is impossible).
        #[arg(long)]
        skip_backup: bool,
    },
    /// Read-only health and integrity diagnostics (exit 1 on any failure).
    Doctor {
        #[arg(long)]
        data_dir: PathBuf,
        /// Server configuration file: also check TLS, backups and memory sizing.
        #[arg(long)]
        config: Option<PathBuf>,
        /// Also read every segment and verify index filters against the rows.
        #[arg(long)]
        deep: bool,
        /// Warn when the newest backup is older than this many hours.
        #[arg(long, default_value_t = 168)]
        max_backup_age_hours: u64,
    },
    /// List, verify or rebuild per-segment index filters (Bloom, trigram).
    Indexes {
        #[command(subcommand)]
        cmd: IndexCmd,
    },
    /// Manage encryption-at-rest keys (ADR-0024).
    Keys {
        #[command(subcommand)]
        cmd: KeysCmd,
    },
    /// Print engine status and recovery report as JSON.
    Status {
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum RetCmd {
    /// Set a tenant's retention (rows older than max-age become eligible for deletion).
    Set {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        max_age_days: u64,
        /// Legal hold: suspend deletion for this tenant.
        #[arg(long)]
        hold: bool,
    },
    /// Remove a tenant's policy (data is then kept indefinitely).
    Clear {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
    },
    /// Print all policies.
    Inspect {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Apply policies now.
    Enforce {
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum BackupCmd {
    Create {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Verify {
        path: PathBuf,
    },
    /// Restore into a new (absent or empty) data directory.
    Restore {
        path: PathBuf,
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum IndexCmd {
    /// Filters per segment (fields, sizes); reads footers only.
    List {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Read every segment and check its filters against the rows: no false
    /// negatives (exit 1 otherwise) and whether they match current settings.
    Verify {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Rewrite segments whose filters are stale or unsound from their rows.
    Rebuild {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum KeysCmd {
    /// Create a new random 256-bit key file (mode 0600; never overwrites).
    Generate {
        #[arg(long)]
        output: PathBuf,
    },
    /// Show whether a data directory is encrypted and with which keys.
    Status {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Re-wrap the data keys with a new key-encryption key (no data rewrite).
    /// Keep the old key until backups made with it have expired.
    RotateKek {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        new_key_file: PathBuf,
    },
    /// Start a new data key for new files; `strata upgrade` rewrites old ones.
    RotateDek {
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum SegCmd {
    List {
        #[arg(long)]
        data_dir: PathBuf,
    },
    /// Verify every checksum and footer statistic of every segment.
    Verify {
        #[arg(long)]
        data_dir: PathBuf,
    },
}

fn open(dir: PathBuf, flush_on_close: bool) -> Result<Engine> {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.flush_on_close = flush_on_close;
    o.wal.durability = Durability::GroupCommit {
        max_delay: Duration::from_millis(1),
    };
    o.encryption = key_provider();
    Engine::open(o)
}

fn tenant(s: &str) -> Result<TenantId> {
    TenantId::new(s)
}

/// JSON rendering: timestamps as RFC 3339 strings, bytes as hex; missing
/// values are omitted from the row object (null is rendered as `null`).
fn to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) => json!(f),
        Value::String(s) => json!(s),
        Value::Bytes(b) => json!(b.iter().map(|x| format!("{x:02x}")).collect::<String>()),
        Value::Timestamp(t) => json!(time::format_rfc3339(*t)),
        Value::List(l) => serde_json::Value::Array(l.iter().map(to_json).collect()),
        Value::Map(m) => {
            serde_json::Value::Object(m.iter().map(|(k, v)| (k.clone(), to_json(v))).collect())
        }
    }
}

fn row_json(cols: &[String], row: &[Datum]) -> serde_json::Value {
    let mut m = Map::new();
    for (c, d) in cols.iter().zip(row) {
        if let Some(v) = d {
            m.insert(c.clone(), to_json(v));
        }
    }
    serde_json::Value::Object(m)
}

/// Most per-line errors kept in an ingest report.
const MAX_REPORTED_ERRORS: usize = 10_000;

/// Batching, idempotency keys and error collection shared by NDJSON and
/// Parquet imports.
struct Importer<'e> {
    engine: &'e Engine,
    tenant: TenantId,
    lim: Limits,
    batch_size: usize,
    import_id: Option<String>,
    batch: Vec<stratadb_schema::Event>,
    digest: sha2::Sha256,
    batch_no: u64,
    accepted: usize,
    rejected: usize,
    duplicate_batches: usize,
    first_seq: Option<u64>,
    last_seq: u64,
    level: &'static str,
    errors: Vec<serde_json::Value>,
}

impl Importer<'_> {
    /// One input record (1-based `line`; `raw` feeds the batch digest).
    fn add(&mut self, line: usize, parsed: Result<serde_json::Value>, raw: &[u8]) -> Result<()> {
        use sha2::Digest;
        let ev = parsed
            .and_then(|v| event_from_json(&v, &self.tenant, time::now_unix_nanos(), &self.lim));
        match ev {
            Ok(ev) => {
                self.digest.update(raw);
                self.digest.update(b"\n");
                self.batch.push(ev);
                if self.batch.len() >= self.batch_size.max(1) {
                    self.flush()?;
                }
            }
            Err(e) => {
                self.rejected += 1;
                let err =
                    json!({"line": line, "error": e.kind().as_str(), "message": e.to_string()});
                eprintln!("{err}");
                if self.errors.len() < MAX_REPORTED_ERRORS {
                    self.errors.push(err);
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        use sha2::Digest;
        if self.batch.is_empty() {
            return Ok(());
        }
        let n = self.batch.len();
        let d: [u8; 32] = std::mem::take(&mut self.digest).finalize().into();
        let key = self
            .import_id
            .as_ref()
            .map(|id| IdempotencyKey::new(format!("import:{id}:{}", self.batch_no), d))
            .transpose()?;
        self.batch_no += 1;
        let r = self.engine.write_batch_idempotent(
            &self.tenant,
            std::mem::take(&mut self.batch),
            key,
        )?;
        if r.duplicate {
            self.duplicate_batches += 1;
        } else {
            self.accepted += n;
        }
        self.first_seq.get_or_insert(r.commit_seq);
        self.last_seq = r.commit_seq;
        self.level = r.level.as_str();
        Ok(())
    }
}

fn ingest(
    dir: PathBuf,
    t: &str,
    file: PathBuf,
    batch_size: usize,
    format: Option<String>,
    import_id: Option<String>,
    report: Option<PathBuf>,
) -> Result<bool> {
    let t = tenant(t)?;
    if let Some(id) = &import_id {
        // Keys are `import:<id>:<batch>`; the same rules as idempotency keys.
        IdempotencyKey::new(format!("import:{id}:0"), [0; 32])?;
    }
    if let Some(r) = &report {
        if r.exists() {
            return Err(Error::invalid_input(format!(
                "report {} exists; refusing to overwrite it",
                r.display()
            )));
        }
    }
    let parquet = match format.as_deref() {
        Some("parquet") => true,
        Some("ndjson" | "jsonl") => false,
        Some(f) => return Err(Error::invalid_input(format!("unknown --format {f:?}"))),
        None => file.extension().is_some_and(|e| e == "parquet"),
    };
    let engine = open(dir, true)?;
    let mut imp = Importer {
        engine: &engine,
        tenant: t,
        lim: Limits::default(),
        batch_size,
        import_id,
        batch: Vec::with_capacity(batch_size.max(1)),
        digest: Default::default(),
        batch_no: 0,
        accepted: 0,
        rejected: 0,
        duplicate_batches: 0,
        first_seq: None,
        last_seq: 0,
        level: "none",
        errors: Vec::new(),
    };
    if parquet {
        #[cfg(feature = "parquet")]
        parquet_io::read_rows(&file, |i, row| {
            let raw = row.as_ref().map(|v| v.to_string()).unwrap_or_default();
            imp.add(i + 1, row, raw.as_bytes())
        })?;
        #[cfg(not(feature = "parquet"))]
        return Err(Error::unsupported("this build has no Parquet support"));
    } else {
        let reader: Box<dyn BufRead> = if file.as_os_str() == "-" {
            Box::new(std::io::stdin().lock())
        } else {
            Box::new(std::io::BufReader::new(
                std::fs::File::open(&file)
                    .map_err(|e| Error::io(format!("opening {}", file.display()), e))?,
            ))
        };
        for (lineno, line) in reader.lines().enumerate() {
            let line = line.map_err(|e| Error::io("reading input", e))?;
            if line.trim().is_empty() {
                continue;
            }
            let parsed = serde_json::from_str::<serde_json::Value>(&line)
                .map_err(|e| Error::invalid_input(format!("invalid JSON: {e}")));
            imp.add(lineno + 1, parsed, line.as_bytes())?;
        }
    }
    imp.flush()?;
    let (accepted, rejected) = (imp.accepted, imp.rejected);
    let summary = json!({"accepted": accepted, "rejected": rejected,
           "duplicate_batches": imp.duplicate_batches, "batches": imp.batch_no,
           "first_commit_seq": imp.first_seq, "last_commit_seq": imp.last_seq, "durability": imp.level,
           "import_id": imp.import_id,
           "state": "committed to WAL and flushed to segments"});
    let errors = std::mem::take(&mut imp.errors);
    drop(imp);
    engine.close()?;
    if let Some(r) = &report {
        let mut full = summary.clone();
        full["errors"] = serde_json::Value::Array(errors);
        full["errors_truncated"] = json!(rejected.saturating_sub(MAX_REPORTED_ERRORS));
        stratadb_common::fsutil::atomic_write(r, full.to_string().as_bytes(), "ingest_report")?;
    }
    println!("{summary}");
    Ok(rejected == 0)
}

fn run(cli: Cli) -> Result<bool> {
    match cli.cmd {
        Cmd::Init { data_dir } => {
            let e = open(data_dir.clone(), false)?;
            let r = e.open_report();
            println!(
                "{}",
                json!({"data_dir": data_dir, "created": r.created, "segments": r.segments,
                       "wal_rows_recovered": r.rows_recovered_from_wal})
            );
            e.close()?;
        }
        Cmd::Ingest {
            data_dir,
            tenant,
            file,
            batch_size,
            format,
            import_id,
            report,
        } => {
            return ingest(
                data_dir, &tenant, file, batch_size, format, import_id, report,
            )
        }
        Cmd::Export {
            data_dir,
            tenant: t,
            output,
            format,
            sql,
            filter,
            page_size,
            resume,
            max_pages,
        } => {
            if page_size == 0 || page_size > 100_000 {
                return Err(Error::invalid_input("--page-size must be 1..=100000"));
            }
            if format == "parquet" {
                if sql.is_some() || resume || max_pages.is_some() {
                    return Err(Error::invalid_input(
                        "Parquet export writes events only (use --where to filter) and cannot \
                         be resumed or split (--sql, --resume, --max-pages)",
                    ));
                }
                #[cfg(feature = "parquet")]
                {
                    let engine = open(data_dir, false)?;
                    let summary = parquet_io::export(
                        &engine,
                        &tenant(&t)?,
                        filter.as_deref(),
                        &output,
                        page_size,
                    )?;
                    engine.close()?;
                    println!("{summary}");
                    return Ok(true);
                }
                #[cfg(not(feature = "parquet"))]
                return Err(Error::unsupported("this build has no Parquet support"));
            }
            let args = export::Args {
                tenant: tenant(&t)?,
                sql,
                filter,
                format: export::Format::parse(&format)?,
                output,
                page_size,
                resume,
                max_pages,
            };
            let engine = open(data_dir, false)?;
            let summary = export::run(&engine, &args)?;
            engine.close()?;
            println!("{summary}");
        }
        Cmd::Shell {
            data_dir,
            tenant: t,
        } => {
            let t = tenant(&t)?;
            let engine = open(data_dir, true)?;
            let stdout = std::io::stdout();
            let ok = shell::run(&engine, &t, std::io::stdin().lock(), &mut stdout.lock())?;
            engine.close()?;
            return Ok(ok);
        }
        Cmd::Query {
            data_dir,
            tenant: t,
            sql,
            file,
            timeout_ms,
            max_rows,
            stats,
        } => {
            let text = match (sql, file) {
                (Some(s), None) => s,
                (None, Some(f)) => std::fs::read_to_string(&f)
                    .map_err(|e| Error::io(format!("reading {}", f.display()), e))?,
                _ => {
                    return Err(Error::invalid_input(
                        "give the query as an argument or with --file (not both)",
                    ))
                }
            };
            let engine = open(data_dir, false)?;
            let opts = QueryOptions {
                timeout: Duration::from_millis(timeout_ms),
                max_result_rows: max_rows,
                ..QueryOptions::default()
            };
            let out = execute(&engine.snapshot(&tenant(&t)?), &text, &opts)?;
            let stdout = std::io::stdout();
            let mut w = stdout.lock();
            if let Some(plan) = &out.explain {
                let _ = write!(w, "{plan}");
            }
            for row in &out.rows {
                let _ = writeln!(w, "{}", row_json(&out.columns, row));
            }
            if stats {
                eprintln!("{:?}", out.stats);
            }
            engine.close()?;
        }
        Cmd::Flush { data_dir } => {
            let e = open(data_dir, false)?;
            let r = e.flush()?;
            println!(
                "{}",
                json!({"rows_flushed": r.rows_flushed, "segments_written": r.segments_written,
                       "flushed_through_seq": r.flushed_through_seq,
                       "wal_segments_reclaimed": r.wal_segments_reclaimed})
            );
            e.close()?;
        }
        Cmd::Segments {
            cmd: SegCmd::List { data_dir },
        } => {
            let m = stratadb_storage::Manifest::load(&data_dir, None)?
                .ok_or_else(|| Error::not_found("no MANIFEST in data directory"))?;
            for s in &m.segments {
                println!(
                    "{}",
                    json!({"id": s.id, "tenant": s.tenant.as_str(), "rows": s.row_count, "bytes": s.file_bytes,
                           "min_event_time": time::format_rfc3339(s.min_event_time),
                           "max_event_time": time::format_rfc3339(s.max_event_time),
                           "min_seq": s.min_seq, "max_seq": s.max_seq})
                );
            }
        }
        Cmd::Segments {
            cmd: SegCmd::Verify { data_dir },
        } => {
            let e = open(data_dir, false)?;
            let n = e.verify()?;
            println!("{}", json!({"segments_verified": n, "result": "ok"}));
            e.close()?;
        }
        Cmd::Compact { data_dir } => {
            let e = open(data_dir, false)?;
            let mut total = (0usize, 0usize, 0usize);
            loop {
                let r = e.compact()?;
                total = (
                    total.0 + r.groups,
                    total.1 + r.inputs,
                    total.2 + r.obsolete_deleted,
                );
                if r.groups == 0 {
                    break;
                }
            }
            println!(
                "{}",
                json!({"groups": total.0, "inputs_merged": total.1, "obsolete_deleted": total.2,
                                  "segments_now": e.stats().segments})
            );
            e.close()?;
        }
        Cmd::Retention { cmd } => match cmd {
            RetCmd::Set {
                data_dir,
                tenant: t,
                max_age_days,
                hold,
            } => {
                let e = open(data_dir, false)?;
                let ms = i64::try_from(max_age_days)
                    .ok()
                    .and_then(|d| d.checked_mul(86_400_000))
                    .ok_or_else(|| Error::invalid_input("max-age-days too large"))?;
                e.set_retention(
                    &tenant(&t)?,
                    Some(RetentionPolicy {
                        max_age_ms: ms,
                        hold,
                    }),
                )?;
                println!(
                    "{}",
                    json!({"tenant": t, "max_age_days": max_age_days, "hold": hold})
                );
                e.close()?;
            }
            RetCmd::Clear {
                data_dir,
                tenant: t,
            } => {
                let e = open(data_dir, false)?;
                e.set_retention(&tenant(&t)?, None)?;
                println!("{}", json!({"tenant": t, "policy": null}));
                e.close()?;
            }
            RetCmd::Inspect { data_dir } => {
                let m = stratadb_storage::Manifest::load(&data_dir, None)?
                    .ok_or_else(|| Error::not_found("no MANIFEST in data directory"))?;
                for (t, p) in &m.retention {
                    println!(
                        "{}",
                        json!({"tenant": t.as_str(), "max_age_ms": p.max_age_ms, "hold": p.hold})
                    );
                }
            }
            RetCmd::Enforce { data_dir } => {
                let e = open(data_dir, false)?;
                let r = e.enforce_retention(time::now_unix_millis())?;
                println!(
                    "{}",
                    json!({"segments_dropped": r.segments_dropped, "segments_rewritten": r.segments_rewritten,
                                      "rows_deleted": r.rows_deleted, "tenants_on_hold": r.tenants_on_hold,
                                      "obsolete_deleted": r.obsolete_deleted})
                );
                e.close()?;
            }
        },
        Cmd::Backup { cmd } => match cmd {
            BackupCmd::Create { data_dir, output } => {
                let e = open(data_dir, false)?;
                let r = e.backup(&output)?;
                println!(
                    "{}",
                    json!({"path": r.path, "manifest_version": r.manifest_version,
                                      "flushed_through_seq": r.flushed_through_seq, "segments": r.segments,
                                      "rows": r.rows, "bytes": r.bytes, "elapsed_ms": r.elapsed.as_millis() as u64})
                );
                e.close()?;
            }
            BackupCmd::Verify { path } => {
                let m = stratadb_storage::verify_backup(&path, key_provider().as_deref())?;
                println!(
                    "{}",
                    json!({"result": "ok", "manifest_version": m.manifest_version,
                                      "flushed_through_seq": m.flushed_through_seq, "segments": m.segments,
                                      "rows": m.rows, "files": m.files.len()})
                );
            }
            BackupCmd::Restore { path, data_dir } => {
                let r = stratadb_storage::restore_backup(&path, &data_dir, key_provider())?;
                println!(
                    "{}",
                    json!({"data_dir": r.data_dir, "restored_manifest_version": r.manifest_version,
                                      "restored_through_seq": r.flushed_through_seq, "segments": r.segments,
                                      "rows": r.rows, "elapsed_ms": r.elapsed.as_millis() as u64,
                                      "note": "writes acknowledged after the backup point are not included"})
                );
            }
        },
        Cmd::Upgrade {
            data_dir,
            dry_run,
            backup_to,
            skip_backup,
        } => {
            if !dry_run && backup_to.is_none() && !skip_backup {
                return Err(Error::invalid_input(
                    "upgrading is irreversible for older binaries: pass --backup-to <dir> (recommended) \
                     or --skip-backup, or use --dry-run",
                ));
            }
            let e = open(data_dir, false)?;
            let plan = e.upgrade_segments(true)?;
            let mut backup = None;
            if !dry_run {
                if let Some(dir) = &backup_to {
                    let r = e.backup(dir)?;
                    stratadb_storage::verify_backup(dir, None)?;
                    backup =
                        Some(json!({"path": r.path, "segments": r.segments, "verified": true}));
                }
            }
            let r = if dry_run {
                plan.clone()
            } else {
                e.upgrade_segments(false)?
            };
            let to_upgrade: usize = plan
                .versions_before
                .iter()
                .filter(|(v, _)| **v < stratadb_segment::FORMAT_VERSION)
                .map(|(_, n)| n)
                .sum();
            println!(
                "{}",
                json!({
                    "dry_run": dry_run,
                    "target_segment_format": stratadb_segment::FORMAT_VERSION,
                    "segments_by_version_before": plan.versions_before.iter().map(|(k, v)| (k.to_string(), *v)).collect::<std::collections::BTreeMap<_, _>>(),
                    "segments_to_upgrade": to_upgrade,
                    "segments_upgraded": r.segments_upgraded,
                    "bytes_before": plan.bytes_before,
                    "bytes_after": r.bytes_after,
                    "backup": backup,
                    "rollback": "older binaries cannot read upgraded segments; restore the backup to roll back",
                })
            );
            e.close()?;
        }
        Cmd::Keys { cmd } => return keys(cmd),
        Cmd::Indexes { cmd } => return indexes(cmd),
        Cmd::Doctor {
            data_dir,
            config,
            deep,
            max_backup_age_hours,
        } => {
            let opts = doctor::Options {
                config,
                deep,
                max_backup_age: Duration::from_secs(max_backup_age_hours.saturating_mul(3600)),
            };
            let r = doctor::run(&data_dir, key_provider().as_deref(), &opts);
            let worst = r.worst();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({"result": worst, "checks": r.checks}))
                    .unwrap_or_default()
            );
            return Ok(worst != "fail");
        }
        Cmd::Status { data_dir } => {
            let e = open(data_dir, false)?;
            let s = e.stats();
            let r = e.open_report();
            println!(
                "{}",
                json!({
                    "segments": s.segments, "segment_rows": s.segment_rows, "segment_bytes": s.segment_bytes,
                    "memtable_rows": s.active_rows, "manifest_version": s.manifest_version,
                    "flushed_through_seq": s.flushed_through_seq,
                    "wal": {"segments": s.wal.segments, "last_seq": s.wal.last_seq, "durable_seq": s.wal.durable_seq},
                    "recovery": {"wal_rows_replayed": r.rows_recovered_from_wal,
                                 "wal_truncated_bytes": r.wal.truncation.as_ref().map(|t| t.bytes_discarded),
                                 "orphan_segments_removed": r.orphan_segments_removed,
                                 "segments_quarantined": r.segments_quarantined,
                                 "elapsed_ms": r.elapsed.as_millis() as u64},
                    "failed": s.failed,
                })
            );
            e.close()?;
        }
    }
    Ok(true)
}

fn indexes(cmd: IndexCmd) -> Result<bool> {
    let row = |s: &stratadb_storage::SegmentIndexes| {
        let mut j = json!({"segment": s.segment_id, "tenant": s.tenant.as_str(), "rows": s.rows,
                           "bloom": s.bloom_fields, "bloom_bytes": s.bloom_bytes,
                           "ngram": s.ngram_fields, "ngram_bytes": s.ngram_bytes});
        if let Some(c) = &s.check {
            j["current"] = json!(c.current);
            j["false_negatives"] = json!(c.false_negatives);
        }
        j
    };
    match cmd {
        IndexCmd::List { data_dir } => {
            let e = open(data_dir, false)?;
            for s in e.indexes(false)? {
                println!("{}", row(&s));
            }
            e.close()?;
            Ok(true)
        }
        IndexCmd::Verify { data_dir } => {
            let e = open(data_dir, false)?;
            let all = e.indexes(true)?;
            e.close()?;
            let (mut stale, mut unsound) = (0, 0);
            for s in &all {
                println!("{}", row(s));
                if let Some(c) = &s.check {
                    stale += usize::from(!c.current);
                    unsound += usize::from(!c.false_negatives.is_empty());
                }
            }
            println!(
                "{}",
                json!({"segments": all.len(), "stale": stale, "unsound": unsound,
                       "result": if unsound > 0 { "fail" } else if stale > 0 { "stale" } else { "ok" },
                       "hint": if unsound + stale > 0 { "run `strata indexes rebuild`" } else { "" }})
            );
            if unsound > 0 {
                return Err(Error::corruption(
                    "indexes",
                    format!("{unsound} segment(s) have filters missing stored values"),
                ));
            }
            Ok(true)
        }
        IndexCmd::Rebuild { data_dir, dry_run } => {
            let e = open(data_dir, false)?;
            let r = e.rebuild_indexes(dry_run)?;
            e.close()?;
            println!(
                "{}",
                json!({"segments_checked": r.segments_checked, "stale": r.segments_stale,
                       "unsound": r.segments_unsound, "rebuilt": r.segments_rebuilt, "dry_run": r.dry_run})
            );
            Ok(true)
        }
    }
}

fn keys(cmd: KeysCmd) -> Result<bool> {
    use stratadb_crypto::{FileKeyProvider, Keyring};
    let need_key = || {
        key_provider().ok_or_else(|| {
            Error::invalid_input("this command needs the current key: pass --key-file")
        })
    };
    let require_encrypted = |dir: &std::path::Path| {
        if Keyring::exists(dir) {
            Ok(())
        } else {
            Err(Error::invalid_input(
                "the data directory is not encrypted (no KEYRING); open it once with --key-file to enable encryption",
            ))
        }
    };
    match cmd {
        KeysCmd::Generate { output } => {
            let k = stratadb_crypto::generate_key_file(&output)?;
            println!(
                "{}",
                json!({"key_file": output, "key_id": k.fingerprint(),
                       "note": "store this file outside the data directory and back it up separately; without it encrypted data and backups cannot be read"})
            );
        }
        KeysCmd::Status { data_dir } => {
            if !Keyring::exists(&data_dir) {
                println!("{}", json!({"encrypted": false}));
                return Ok(true);
            }
            let p = need_key()?;
            let k = Keyring::load(&data_dir, p.as_ref())?
                .ok_or_else(|| Error::internal("KEYRING disappeared"))?;
            println!(
                "{}",
                json!({"encrypted": true, "key_id": k.kek_id(), "data_keys": k.dek_ids(),
                       "active_data_key": k.active().0})
            );
        }
        KeysCmd::RotateKek {
            data_dir,
            new_key_file,
        } => {
            require_encrypted(&data_dir)?;
            let p = need_key()?;
            // Opening proves exclusive access and the current key.
            let e = open(data_dir.clone(), false)?;
            let mut k = Keyring::load(&data_dir, p.as_ref())?
                .ok_or_else(|| Error::internal("KEYRING disappeared"))?;
            k.rotate_kek(&data_dir, &FileKeyProvider::new(&new_key_file))?;
            e.close()?;
            println!(
                "{}",
                json!({"rotated": "key-encryption key", "key_id": k.kek_id(),
                       "note": "use the new key file from now on; keep the old one for older backups"})
            );
        }
        KeysCmd::RotateDek { data_dir } => {
            require_encrypted(&data_dir)?;
            let p = need_key()?;
            let e = open(data_dir.clone(), false)?;
            let mut k = Keyring::load(&data_dir, p.as_ref())?
                .ok_or_else(|| Error::internal("KEYRING disappeared"))?;
            let id = k.rotate_dek(&data_dir)?;
            e.close()?;
            println!(
                "{}",
                json!({"rotated": "data key", "active_data_key": id,
                       "note": "new files use the new data key; run `strata upgrade` to rewrite older segments"})
            );
        }
    }
    Ok(true)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let key = cli
        .key_file
        .clone()
        .or_else(|| std::env::var_os("STRATADB_KEY_FILE").map(PathBuf::from));
    let _ = KEY_FILE.set(key);
    match run(cli) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(3),
        Err(e) => {
            eprintln!(
                "{}",
                json!({"error": e.kind().as_str(), "message": e.to_string()})
            );
            ExitCode::FAILURE
        }
    }
}
