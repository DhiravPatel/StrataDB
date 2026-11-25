//! `strata export` — stream a tenant's events or a query result to CSV or
//! NDJSON with checkpoint/resume (spec §30.3).
//!
//! * Without `--sql` the output is the tenant's **events** in the JSON ingest
//!   shape (`stratadb_schema::json::event_to_json`), in commit order, so an
//!   NDJSON export re-imports with `strata ingest` to identical events (see that
//!   function for the documented lossy cases). `--where` filters them.
//! * With `--sql` the output is the query's rows (row queries only, since
//!   export is paginated).
//! * Pages come from keyset pagination (ADR-0023), so memory is bounded by
//!   `--page-size` and the export is one consistent snapshot: rows committed
//!   after the first page are excluded, even across `--resume`.
//! * After each page the output is fsynced, then a checkpoint
//!   (`<output>.checkpoint`, atomic write) records the cursor and the output
//!   length. `--resume` truncates anything written after the last checkpoint
//!   and continues. An existing output is never overwritten.
//!
//! CSV: RFC 4180 quoting; a header row; missing values and null are empty;
//! non-string values are rendered as JSON text. Values are written verbatim:
//! cells beginning with `=`, `+`, `-` or `@` may be interpreted as formulas by
//! spreadsheet programs (documented in docs/operations/export-import.md).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{json, Value as J};
use stratadb_common::{fsutil, Error, IoResultExt, Result, TenantId};
use stratadb_query::{execute_page, Datum, PageRequest, QueryOptions};
use stratadb_schema::json::{event_to_json, value_to_json};
use stratadb_schema::{Event, Severity, Value};
use stratadb_storage::Engine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Ndjson,
    Csv,
}

impl Format {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "ndjson" | "jsonl" => Ok(Format::Ndjson),
            "csv" => Ok(Format::Csv),
            _ => Err(Error::invalid_input("--format must be ndjson or csv")),
        }
    }
    fn as_str(self) -> &'static str {
        match self {
            Format::Ndjson => "ndjson",
            Format::Csv => "csv",
        }
    }
}

pub struct Args {
    pub tenant: TenantId,
    pub sql: Option<String>,
    pub filter: Option<String>,
    pub format: Format,
    pub output: PathBuf,
    pub page_size: usize,
    pub resume: bool,
    pub max_pages: Option<u64>,
}

const EVENT_COLUMNS: &str = "event_time, service, severity_number, severity_text, body, \
                             trace_id, span_id, event_id, attributes, resource_attributes";
/// CSV header of an events export (the keys of `event_to_json`).
const EVENT_CSV: [&str; 9] = [
    "event_time",
    "service",
    "severity",
    "body",
    "trace_id",
    "span_id",
    "event_id",
    "attributes",
    "resource_attributes",
];

fn checkpoint_path(output: &Path) -> PathBuf {
    let mut s = output.as_os_str().to_owned();
    s.push(".checkpoint");
    PathBuf::from(s)
}

/// RFC 4180 field.
pub fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_owned()
    }
}

fn csv_cell(v: Option<&J>) -> String {
    match v {
        None | Some(J::Null) => String::new(),
        Some(J::String(s)) => csv_field(s),
        Some(other) => csv_field(&other.to_string()),
    }
}

fn csv_line<'a>(cells: impl Iterator<Item = String> + 'a) -> String {
    let mut line = cells.collect::<Vec<_>>().join(",");
    line.push_str("\r\n");
    line
}

/// Rebuild an event from a row of `EVENT_COLUMNS`.
fn row_event(tenant: &TenantId, row: &[Datum]) -> Result<Event> {
    let s = |d: &Datum| match d {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    };
    let map = |d: &Datum| match d {
        Some(Value::Map(m)) => m.clone(),
        _ => Default::default(),
    };
    let Some(Value::Timestamp(t)) = row[0] else {
        return Err(Error::internal("export row without event_time"));
    };
    let mut e = Event::new(tenant.clone(), t, 0);
    e.service = s(&row[1]);
    if let Some(Value::Int(n)) = row[2] {
        e.severity = u8::try_from(n)
            .ok()
            .and_then(Severity::from_u8)
            .unwrap_or_default();
    }
    e.severity_text = s(&row[3]);
    e.body = row[4].clone();
    e.trace_id = s(&row[5]);
    e.span_id = s(&row[6]);
    e.event_id = s(&row[7]);
    e.attributes = map(&row[8]);
    e.resource_attributes = map(&row[9]);
    Ok(e)
}

fn render_page(
    a: &Args,
    events_mode: bool,
    columns: &[String],
    rows: &[Vec<Datum>],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for row in rows {
        let line = if events_mode {
            let j = event_to_json(&row_event(&a.tenant, row)?);
            match a.format {
                Format::Ndjson => format!("{j}\n"),
                Format::Csv => csv_line(EVENT_CSV.iter().map(|c| csv_cell(j.get(*c)))),
            }
        } else {
            match a.format {
                Format::Ndjson => {
                    let mut m = serde_json::Map::new();
                    for (c, d) in columns.iter().zip(row) {
                        if let Some(v) = d {
                            m.insert(c.clone(), value_to_json(v));
                        }
                    }
                    format!("{}\n", J::Object(m))
                }
                Format::Csv => csv_line(
                    row.iter()
                        .map(|d| csv_cell(d.as_ref().map(value_to_json).as_ref())),
                ),
            }
        };
        out.extend_from_slice(line.as_bytes());
    }
    Ok(out)
}

/// Run (or resume) an export. Returns the summary printed by the CLI.
pub fn run(engine: &Engine, a: &Args) -> Result<J> {
    if a.sql.is_some() && a.filter.is_some() {
        return Err(Error::invalid_input("use --sql or --where, not both"));
    }
    let events_mode = a.sql.is_none();
    let query = match (&a.sql, &a.filter) {
        (Some(q), _) => q.trim().trim_end_matches(';').to_owned(),
        (None, Some(w)) => format!("SELECT {EVENT_COLUMNS} FROM logs WHERE {w}"),
        (None, None) => format!("SELECT {EVENT_COLUMNS} FROM logs"),
    };
    let cp_path = checkpoint_path(&a.output);
    let parent = match a.output.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let (mut file, mut cursor, mut rows, mut pages, mut bytes) = if a.resume {
        let raw =
            std::fs::read(&cp_path).ctx(|| format!("reading checkpoint {}", cp_path.display()))?;
        let cp: J = serde_json::from_slice(&raw)
            .map_err(|e| Error::invalid_input(format!("bad checkpoint: {e}")))?;
        let matches = cp["version"] == 1
            && cp["tenant"] == a.tenant.as_str()
            && cp["query"] == query.as_str()
            && cp["format"] == a.format.as_str();
        if !matches {
            return Err(Error::invalid_input(
                "checkpoint was written for a different tenant, query or format",
            ));
        }
        let bytes = cp["bytes"].as_u64().unwrap_or(0);
        let f = OpenOptions::new()
            .write(true)
            .open(&a.output)
            .ctx(|| format!("opening {}", a.output.display()))?;
        let len = f.metadata().ctx(|| "stat output".to_owned())?.len();
        if len < bytes {
            return Err(Error::corruption(
                a.output.display().to_string(),
                format!("output is {len} bytes but the checkpoint covers {bytes}"),
            ));
        }
        // Discard anything written after the last checkpoint.
        f.set_len(bytes).ctx(|| "truncating output".to_owned())?;
        fsutil::sync_file(&f, &a.output, false).ctx(|| "fsyncing output".to_owned())?;
        drop(f);
        let f = OpenOptions::new()
            .append(true)
            .open(&a.output)
            .ctx(|| format!("opening {}", a.output.display()))?;
        (
            f,
            cp["cursor"].as_str().map(str::to_owned),
            cp["rows"].as_u64().unwrap_or(0),
            cp["pages"].as_u64().unwrap_or(0),
            bytes,
        )
    } else {
        if cp_path.exists() {
            return Err(Error::invalid_input(format!(
                "{} exists: an earlier export was interrupted; use --resume or remove it",
                cp_path.display()
            )));
        }
        let mut f: File = fsutil::create_new_file(&a.output).map_err(|e| {
            Error::invalid_input(format!(
                "{e} (refusing to overwrite an existing output; choose another path)"
            ))
        })?;
        let mut bytes = 0;
        if a.format == Format::Csv && events_mode {
            let h = csv_line(EVENT_CSV.iter().map(|c| (*c).to_owned()));
            f.write_all(h.as_bytes())
                .ctx(|| "writing header".to_owned())?;
            bytes = h.len() as u64;
        }
        (f, None, 0, 0, bytes)
    };
    let opts = QueryOptions {
        timeout: Duration::from_secs(600),
        max_result_rows: a.page_size,
        ..QueryOptions::default()
    };
    let snapshot_tenant = a.tenant.clone();
    let mut wrote_sql_header = bytes > 0 || pages > 0;
    let complete = loop {
        if a.max_pages.is_some_and(|m| pages >= m) {
            break false;
        }
        let page = execute_page(
            &engine.snapshot(&snapshot_tenant),
            &query,
            &opts,
            &PageRequest {
                page_size: a.page_size,
                cursor: cursor.clone(),
            },
        )?;
        let mut buf = Vec::new();
        if a.format == Format::Csv && !events_mode && !wrote_sql_header {
            let h = csv_line(page.output.columns.iter().map(|c| csv_field(c)));
            buf.extend_from_slice(h.as_bytes());
            wrote_sql_header = true;
        }
        buf.extend(render_page(
            a,
            events_mode,
            &page.output.columns,
            &page.output.rows,
        )?);
        file.write_all(&buf).ctx(|| "writing output".to_owned())?;
        fsutil::sync_file(&file, &a.output, true).ctx(|| "fsyncing output".to_owned())?;
        rows += page.output.rows.len() as u64;
        bytes += buf.len() as u64;
        pages += 1;
        match page.next_cursor {
            Some(c) => {
                let cp = json!({"version": 1, "tenant": a.tenant.as_str(), "query": query,
                                "format": a.format.as_str(), "cursor": c, "rows": rows,
                                "pages": pages, "bytes": bytes});
                fsutil::atomic_write(&cp_path, cp.to_string().as_bytes(), "export_checkpoint")?;
                cursor = Some(c);
            }
            None => break true,
        }
    };
    if complete && fsutil::remove_if_exists(&cp_path)? {
        fsutil::fsync_dir(&parent)?;
    }
    Ok(
        json!({"output": a.output, "format": a.format.as_str(), "rows": rows, "pages": pages,
              "bytes": bytes, "complete": complete,
              "checkpoint": (!complete).then(|| cp_path.display().to_string())}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_quoting() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_field("l1\nl2"), "\"l1\nl2\"");
        assert_eq!(csv_cell(Some(&json!({"k": 1}))), "\"{\"\"k\"\":1}\"");
        assert_eq!(csv_cell(Some(&J::Null)), "");
        assert_eq!(csv_cell(None), "");
        assert_eq!(csv_cell(Some(&json!(5))), "5");
    }
}
