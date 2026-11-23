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

use clap::{Parser, Subcommand};
use serde_json::{json, Map};
use stratadb_common::{time, Error, Result, TenantId};
use stratadb_query::{execute, Datum, QueryOptions};
use stratadb_schema::json::event_from_json;
use stratadb_schema::{Limits, Value};
use stratadb_storage::{Durability, Engine, EngineOptions, RetentionPolicy};

#[derive(Parser)]
#[command(name = "strata", version, about = "StrataDB command-line tool")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
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
    /// Print engine status and recovery report as JSON.
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
    },
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

fn ingest(dir: PathBuf, t: &str, file: PathBuf, batch_size: usize) -> Result<bool> {
    let t = tenant(t)?;
    let reader: Box<dyn BufRead> = if file.as_os_str() == "-" {
        Box::new(std::io::stdin().lock())
    } else {
        Box::new(std::io::BufReader::new(
            std::fs::File::open(&file)
                .map_err(|e| Error::io(format!("opening {}", file.display()), e))?,
        ))
    };
    let engine = open(dir, true)?;
    let lim = Limits::default();
    let (mut accepted, mut rejected) = (0usize, 0usize);
    let (mut first_seq, mut last_seq) = (None, 0u64);
    let mut batch = Vec::with_capacity(batch_size.max(1));
    let mut level = "none";
    let mut flush_batch = |batch: &mut Vec<_>| -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let n = batch.len();
        let r = engine.write_batch(&t, std::mem::take(batch))?;
        accepted += n;
        first_seq.get_or_insert(r.commit_seq);
        last_seq = r.commit_seq;
        level = r.level.as_str();
        Ok(())
    };
    for (lineno, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| Error::io("reading input", e))?;
        if line.trim().is_empty() {
            continue;
        }
        let parsed = serde_json::from_str::<serde_json::Value>(&line)
            .map_err(|e| Error::invalid_input(format!("invalid JSON: {e}")))
            .and_then(|v| event_from_json(&v, &t, time::now_unix_nanos(), &lim));
        match parsed {
            Ok(ev) => {
                batch.push(ev);
                if batch.len() >= batch_size.max(1) {
                    flush_batch(&mut batch)?;
                }
            }
            Err(e) => {
                rejected += 1;
                eprintln!(
                    "{}",
                    json!({"line": lineno + 1, "error": e.kind().as_str(), "message": e.to_string()})
                );
            }
        }
    }
    flush_batch(&mut batch)?;
    engine.close()?;
    println!(
        "{}",
        json!({"accepted": accepted, "rejected": rejected, "first_commit_seq": first_seq,
               "last_commit_seq": last_seq, "durability": level,
               "state": "committed to WAL and flushed to segments"})
    );
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
        } => return ingest(data_dir, &tenant, file, batch_size),
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
            let m = stratadb_storage::Manifest::load(&data_dir)?
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
                let m = stratadb_storage::Manifest::load(&data_dir)?
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
                let m = stratadb_storage::verify_backup(&path)?;
                println!(
                    "{}",
                    json!({"result": "ok", "manifest_version": m.manifest_version,
                                      "flushed_through_seq": m.flushed_through_seq, "segments": m.segments,
                                      "rows": m.rows, "files": m.files.len()})
                );
            }
            BackupCmd::Restore { path, data_dir } => {
                let r = stratadb_storage::restore_backup(&path, &data_dir)?;
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
                    stratadb_storage::verify_backup(dir)?;
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
        Cmd::Doctor { data_dir } => {
            let r = doctor::run(&data_dir);
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

fn main() -> ExitCode {
    match run(Cli::parse()) {
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
