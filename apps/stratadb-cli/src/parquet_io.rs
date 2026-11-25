//! Parquet export/import of events (spec §30.3, ADR-0025).
//!
//! Schema of an events export (one row group per export page, uncompressed):
//! ```text
//! message stratadb_event {
//!   required int64  event_time (TIMESTAMP(NANOS, true));
//!   optional binary service (STRING);
//!   optional binary severity (STRING);       -- normalized name; absent when UNSPECIFIED
//!   optional binary severity_text (STRING);  -- original spelling, when kept
//!   optional binary body (STRING);           -- body, when it is a string
//!   optional binary body_json (JSON);        -- body, when it is not a string
//!   optional binary trace_id (STRING);
//!   optional binary span_id (STRING);
//!   optional binary event_id (STRING);
//!   optional binary attributes (JSON);
//!   optional binary resource_attributes (JSON);
//! }
//! ```
//! Import maps each row to the JSON ingest shape and runs the normal ingest
//! mapping and validation (`event_from_json`), so a Parquet export re-imports
//! to identical events (same lossy cases as NDJSON). Generic files: any other
//! column becomes an attribute; `event_time` may be a timestamp (ns/µs/ms) or
//! an integer of nanoseconds or an RFC 3339 string.
//!
//! The file is written to `<output>.tmp` and renamed into place only when
//! complete (a Parquet footer cannot be appended to, so there is no resume).

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::record::Field;
use serde_json::{json, Map, Value as J};
use stratadb_common::{fsutil, Error, Result, TenantId};
use stratadb_query::{execute_page, PageRequest, QueryOptions};
use stratadb_schema::json::{severity_to_json, value_to_json};
use stratadb_schema::{Severity, Value};
use stratadb_storage::Engine;

const SCHEMA: &str = "message stratadb_event {
  required int64 event_time (TIMESTAMP(NANOS,true));
  optional binary service (STRING);
  optional binary severity (STRING);
  optional binary severity_text (STRING);
  optional binary body (STRING);
  optional binary body_json (JSON);
  optional binary trace_id (STRING);
  optional binary span_id (STRING);
  optional binary event_id (STRING);
  optional binary attributes (JSON);
  optional binary resource_attributes (JSON);
}";

const QUERY: &str = "SELECT event_time, service, severity_number, severity_text, body, \
                     trace_id, span_id, event_id, attributes, resource_attributes FROM logs";

fn pq(e: parquet::errors::ParquetError) -> Error {
    Error::internal(format!("parquet: {e}"))
}

/// One optional string column: values + definition levels.
#[derive(Default)]
struct StrCol {
    vals: Vec<ByteArray>,
    defs: Vec<i16>,
}

impl StrCol {
    fn push(&mut self, v: Option<String>) {
        match v {
            Some(s) => {
                self.vals.push(ByteArray::from(s.into_bytes()));
                self.defs.push(1);
            }
            None => self.defs.push(0),
        }
    }
}

/// Export the tenant's events (optionally filtered) to a Parquet file.
pub fn export(
    engine: &Engine,
    tenant: &TenantId,
    filter: Option<&str>,
    output: &Path,
    page_size: usize,
) -> Result<J> {
    if output.exists() {
        return Err(Error::invalid_input(format!(
            "{} exists; refusing to overwrite it",
            output.display()
        )));
    }
    let query = match filter {
        Some(w) => format!("{QUERY} WHERE {w}"),
        None => QUERY.to_owned(),
    };
    let schema = Arc::new(parquet::schema::parser::parse_message_type(SCHEMA).map_err(pq)?);
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .set_created_by(format!("stratadb {}", env!("CARGO_PKG_VERSION")))
            .build(),
    );
    let tmp = fsutil::tmp_path(output);
    fsutil::remove_if_exists(&tmp)?;
    let file = fsutil::create_new_file(&tmp)?;
    let mut w = SerializedFileWriter::new(file, schema, props).map_err(pq)?;
    let opts = QueryOptions {
        timeout: Duration::from_secs(600),
        max_result_rows: page_size,
        ..QueryOptions::default()
    };
    let (mut cursor, mut rows, mut groups) = (None, 0u64, 0u64);
    loop {
        let page = execute_page(
            &engine.snapshot(tenant),
            &query,
            &opts,
            &PageRequest {
                page_size,
                cursor: cursor.clone(),
            },
        )?;
        if !page.output.rows.is_empty() {
            let mut times = Vec::with_capacity(page.output.rows.len());
            let mut cols: Vec<StrCol> = (0..10).map(|_| StrCol::default()).collect();
            for r in &page.output.rows {
                let Some(Value::Timestamp(t)) = r[0] else {
                    return Err(Error::internal("export row without event_time"));
                };
                times.push(t);
                let s = |d: &Option<Value>| match d {
                    Some(Value::String(s)) => Some(s.clone()),
                    _ => None,
                };
                let json_of = |d: &Option<Value>| match d {
                    Some(Value::Map(m)) if !m.is_empty() => {
                        Some(value_to_json(&Value::Map(m.clone())).to_string())
                    }
                    _ => None,
                };
                let sev = match r[2] {
                    Some(Value::Int(n)) => u8::try_from(n)
                        .ok()
                        .and_then(Severity::from_u8)
                        .unwrap_or_default(),
                    _ => Severity::Unspecified,
                };
                cols[0].push(s(&r[1]));
                cols[1].push((sev != Severity::Unspecified).then(|| sev.as_str().to_owned()));
                cols[2].push(s(&r[3]));
                match &r[4] {
                    Some(Value::String(b)) => {
                        cols[3].push(Some(b.clone()));
                        cols[4].push(None);
                    }
                    Some(v) => {
                        cols[3].push(None);
                        cols[4].push(Some(value_to_json(v).to_string()));
                    }
                    None => {
                        cols[3].push(None);
                        cols[4].push(None);
                    }
                }
                cols[5].push(s(&r[5]));
                cols[6].push(s(&r[6]));
                cols[7].push(s(&r[7]));
                cols[8].push(json_of(&r[8]));
                cols[9].push(json_of(&r[9]));
            }
            let mut rg = w.next_row_group().map_err(pq)?;
            let mut idx = 0usize;
            while let Some(mut col) = rg.next_column().map_err(pq)? {
                if idx == 0 {
                    col.typed::<Int64Type>()
                        .write_batch(&times, None, None)
                        .map_err(pq)?;
                } else {
                    let c = &cols[idx - 1];
                    col.typed::<ByteArrayType>()
                        .write_batch(&c.vals, Some(&c.defs), None)
                        .map_err(pq)?;
                }
                col.close().map_err(pq)?;
                idx += 1;
            }
            rg.close().map_err(pq)?;
            rows += page.output.rows.len() as u64;
            groups += 1;
        }
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    let file = w.into_inner().map_err(pq)?;
    fsutil::sync_file(&file, &tmp, false)
        .map_err(|e| Error::io(format!("fsyncing {}", tmp.display()), e))?;
    drop(file);
    std::fs::rename(&tmp, output)
        .map_err(|e| Error::io(format!("renaming {}", tmp.display()), e))?;
    if let Some(p) = output.parent().filter(|p| !p.as_os_str().is_empty()) {
        fsutil::fsync_dir(p)?;
    }
    let bytes = std::fs::metadata(output).map(|m| m.len()).unwrap_or(0);
    Ok(
        json!({"output": output, "format": "parquet", "rows": rows, "row_groups": groups,
              "bytes": bytes, "complete": true}),
    )
}

fn field_json(f: &Field) -> J {
    match f {
        Field::Null => J::Null,
        Field::Bool(b) => J::Bool(*b),
        Field::Byte(v) => J::from(*v),
        Field::Short(v) => J::from(*v),
        Field::Int(v) => J::from(*v),
        Field::Long(v) => J::from(*v),
        Field::UByte(v) => J::from(*v),
        Field::UShort(v) => J::from(*v),
        Field::UInt(v) => J::from(*v),
        Field::ULong(v) => J::from(*v),
        Field::Float(v) => J::from(f64::from(*v)),
        Field::Double(v) => J::from(*v),
        Field::Str(s) => J::String(s.clone()),
        // Exact: nanoseconds rendered as RFC 3339 with 9 fractional digits.
        Field::TimestampMillis(v) => J::String(stratadb_common::time::format_rfc3339(
            v.saturating_mul(1_000_000),
        )),
        Field::TimestampMicros(v) => J::String(stratadb_common::time::format_rfc3339(
            v.saturating_mul(1_000),
        )),
        // Nested columns keep their structure (struct → object, list → array,
        // map → object with string keys).
        Field::Group(row) => J::Object(
            row.get_column_iter()
                .map(|(k, v)| (k.clone(), field_json(v)))
                .collect(),
        ),
        Field::ListInternal(list) => J::Array(list.elements().iter().map(field_json).collect()),
        Field::MapInternal(map) => J::Object(
            map.entries()
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        Field::Str(s) => s.clone(),
                        other => field_json(other).to_string(),
                    };
                    (key, field_json(v))
                })
                .collect(),
        ),
        // Binary as lowercase hex (as StrataDB renders bytes elsewhere).
        Field::Bytes(b) => J::String(b.data().iter().map(|x| format!("{x:02x}")).collect()),
        // Dates, times, decimals, float16: their textual form.
        other => J::String(other.to_string()),
    }
}

/// Nanoseconds of a timestamp field (exact), if it is one.
fn timestamp_nanos(f: &Field) -> Option<i64> {
    match f {
        Field::TimestampMillis(v) => Some(v.saturating_mul(1_000_000)),
        Field::TimestampMicros(v) => Some(v.saturating_mul(1_000)),
        _ => None,
    }
}

/// Map one Parquet row to the JSON ingest shape.
fn row_to_input(row: &parquet::record::Row) -> Result<J> {
    let mut o = Map::new();
    let mut severity: Option<String> = None;
    let mut severity_text: Option<String> = None;
    for (name, f) in row.get_column_iter() {
        if matches!(f, Field::Null) {
            continue;
        }
        let parse_json = |s: &str| {
            serde_json::from_str::<J>(s)
                .map_err(|e| Error::invalid_input(format!("column `{name}`: invalid JSON: {e}")))
        };
        match (name.as_str(), f) {
            // Event-time columns: integer nanoseconds (exact).
            ("event_time" | "timestamp", f) if timestamp_nanos(f).is_some() => {
                o.insert(
                    name.clone(),
                    J::from(timestamp_nanos(f).unwrap_or_default()),
                );
            }
            ("severity", Field::Str(s)) => severity = Some(s.clone()),
            ("severity_text", Field::Str(s)) => severity_text = Some(s.clone()),
            ("body_json", Field::Str(s)) => {
                o.insert("body".into(), parse_json(s)?);
            }
            ("attributes" | "resource_attributes", Field::Str(s)) => {
                o.insert(name.clone(), parse_json(s)?);
            }
            (_, f) => {
                o.insert(name.clone(), field_json(f));
            }
        }
    }
    // Reproduce the exported (severity, severity_text) pair exactly.
    let sev = severity
        .as_deref()
        .and_then(Severity::parse)
        .unwrap_or(Severity::Unspecified);
    let input = match (&severity, &severity_text) {
        (None, None) => None,
        (Some(s), None) if Severity::parse(s).is_none() => Some(J::String(s.clone())),
        _ => severity_to_json(sev, severity_text.as_deref()),
    };
    if let Some(v) = input {
        o.insert("severity".into(), v);
    }
    Ok(J::Object(o))
}

/// Read a Parquet file as ingest JSON objects (row by row, via `f`).
///
/// A row that cannot be *mapped* (bad JSON column, invalid event) is passed
/// to `f` as an error and the import continues. A failure to *read* the file
/// (unsupported codec, corrupt page) aborts the import: the reader cannot
/// make progress past it, so continuing would repeat the error forever.
pub fn read_rows(path: &Path, mut f: impl FnMut(usize, Result<J>) -> Result<()>) -> Result<usize> {
    let loc = path.display().to_string();
    let file = File::open(path).map_err(|e| Error::io(format!("opening {loc}"), e))?;
    let reader = SerializedFileReader::new(file).map_err(|e| read_error(&loc, e))?;
    let mut n = 0;
    for (i, row) in reader
        .get_row_iter(None)
        .map_err(|e| read_error(&loc, e))?
        .enumerate()
    {
        let r = row.map_err(|e| read_error(&loc, e))?;
        n += 1;
        f(i, row_to_input(&r))?;
    }
    Ok(n)
}

/// A file-level read failure, with a hint for codecs this build lacks.
fn read_error(loc: &str, e: parquet::errors::ParquetError) -> Error {
    let msg = e.to_string();
    if msg.contains("Disabled feature at compile time") {
        Error::unsupported(format!(
            "{loc}: {msg}. Supported Parquet codecs: uncompressed, snappy, gzip, lz4; \
             rewrite the file with one of them (e.g. pyarrow `compression=\"snappy\"`)"
        ))
    } else {
        Error::corruption(loc, format!("cannot read Parquet file: {msg}"))
    }
}
