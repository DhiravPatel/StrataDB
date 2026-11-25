//! `strata export` / idempotent `strata ingest` / `strata shell`, end to end
//! through the binary (spec §30.3, §22.1).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn strata(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .output()
        .unwrap()
}

fn strata_stdin(args: &[&str], input: &str) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    c.stdin.take().unwrap().write_all(input.as_bytes()).unwrap();
    c.wait_with_output().unwrap()
}

fn json_out(o: &Output) -> serde_json::Value {
    let s = String::from_utf8_lossy(&o.stdout);
    serde_json::from_str(s.lines().last().unwrap_or("")).unwrap_or_else(|e| {
        panic!(
            "{e}: stdout={s} stderr={}",
            String::from_utf8_lossy(&o.stderr)
        )
    })
}

/// 250 varied events: severity spellings/numbers/unknown text, nested bodies,
/// unknown top-level keys, ids, resources; plus a body needing CSV quoting.
fn write_events(path: &Path, n: usize) {
    let now = stratadb_common::time::now_unix_nanos();
    let mut f = std::fs::File::create(path).unwrap();
    for i in 0..n {
        let t = stratadb_common::time::format_rfc3339(now - (n - i) as i64 * 1_000_000);
        let sev = match i % 5 {
            0 => r#""ERROR""#.to_owned(),
            1 => r#""warning""#.to_owned(),
            2 => "17".to_owned(),
            3 => r#""boom""#.to_owned(),
            _ => "null".to_owned(),
        };
        let body = match i % 4 {
            0 => format!(r#""plain {i}""#),
            1 => format!(r#"{{"k": [1, 2.5, null, true], "i": {i}}}"#),
            2 => r#""quote \" comma , and\nnewline""#.to_owned(),
            _ => format!("{}", 18_446_744_073_709_551_000u64 - i as u64),
        };
        writeln!(
            f,
            r#"{{"event_time":"{t}","service":"svc{}","severity":{sev},"body":{body},"trace_id":"{:032X}","event_id":"e{i}","attributes":{{"n":{i},"f":{i}.5}},"resource":{{"host":"h{}"}},"extra_top":"x{i}"}}"#,
            i % 3,
            i,
            i % 2
        )
        .unwrap();
    }
}

#[test]
fn export_round_trips_and_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n);
    let s = |n: &str| p(n).to_str().unwrap().to_owned();
    write_events(&p("in.ndjson"), 250);
    for db in ["a", "b"] {
        assert!(strata(&["init", "--data-dir", &s(db)]).status.success());
    }
    let o = strata(&[
        "ingest",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--file",
        &s("in.ndjson"),
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(json_out(&o)["accepted"], 250);

    // One-shot export of A.
    let o = strata(&[
        "export",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--output",
        &s("a1.ndjson"),
        "--page-size",
        "40",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let sum = json_out(&o);
    assert_eq!(
        (
            sum["rows"].as_u64(),
            sum["pages"].as_u64(),
            sum["complete"].as_bool()
        ),
        (Some(250), Some(7), Some(true))
    );
    let a1 = std::fs::read(p("a1.ndjson")).unwrap();
    assert!(!p("a1.ndjson.checkpoint").exists());

    // Re-import the export into B, export B: byte-identical (lossless round trip).
    let o = strata(&[
        "ingest",
        "--data-dir",
        &s("b"),
        "--tenant",
        "t",
        "--file",
        &s("a1.ndjson"),
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(json_out(&o)["accepted"], 250);
    let o = strata(&[
        "export",
        "--data-dir",
        &s("b"),
        "--tenant",
        "t",
        "--output",
        &s("b1.ndjson"),
    ]);
    assert!(o.status.success());
    assert_eq!(
        std::fs::read(p("b1.ndjson")).unwrap(),
        a1,
        "re-imported export differs"
    );

    // Never overwrite an existing output.
    let o = strata(&[
        "export",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--output",
        &s("a1.ndjson"),
    ]);
    assert!(!o.status.success());
    assert_eq!(std::fs::read(p("a1.ndjson")).unwrap(), a1);

    // Interrupted export: two pages, then a torn partial write after the checkpoint.
    let o = strata(&[
        "export",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--output",
        &s("a2.ndjson"),
        "--page-size",
        "40",
        "--max-pages",
        "2",
    ]);
    assert!(o.status.success());
    assert_eq!(json_out(&o)["complete"], false);
    assert!(p("a2.ndjson.checkpoint").exists());
    std::fs::OpenOptions::new()
        .append(true)
        .open(p("a2.ndjson"))
        .unwrap()
        .write_all(b"{\"torn\": tru")
        .unwrap();
    // Without --resume the interrupted export is not clobbered.
    let o = strata(&[
        "export",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--output",
        &s("a3.ndjson"),
    ]);
    assert!(o.status.success()); // a different output is fine
    let o = strata(&[
        "export",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--output",
        &s("a2.ndjson"),
        "--page-size",
        "40",
    ]);
    assert!(!o.status.success());
    // A resume with a different query is refused.
    let o = strata(&[
        "export",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--output",
        &s("a2.ndjson"),
        "--page-size",
        "40",
        "--resume",
        "--where",
        "service = 'svc1'",
    ]);
    assert!(!o.status.success());
    // New writes after the first page must not leak into the resumed export.
    let o = strata(&[
        "ingest",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--file",
        &s("in.ndjson"),
    ]);
    assert!(o.status.success());
    let o = strata(&[
        "export",
        "--data-dir",
        &s("a"),
        "--tenant",
        "t",
        "--output",
        &s("a2.ndjson"),
        "--page-size",
        "40",
        "--resume",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(json_out(&o)["complete"], true);
    assert_eq!(
        std::fs::read(p("a2.ndjson")).unwrap(),
        a1,
        "resumed export differs"
    );
    assert!(!p("a2.ndjson.checkpoint").exists());
}

#[test]
fn csv_exports_and_filters() {
    let dir = tempfile::tempdir().unwrap();
    let s = |n: &str| dir.path().join(n).to_str().unwrap().to_owned();
    write_events(&dir.path().join("in.ndjson"), 20);
    assert!(strata(&["init", "--data-dir", &s("d")]).status.success());
    assert!(strata(&[
        "ingest",
        "--data-dir",
        &s("d"),
        "--tenant",
        "t",
        "--file",
        &s("in.ndjson")
    ])
    .status
    .success());

    let o = strata(&[
        "export",
        "--data-dir",
        &s("d"),
        "--tenant",
        "t",
        "--output",
        &s("q.csv"),
        "--format",
        "csv",
        "--sql",
        "SELECT event_id, body, attributes['n'] AS n FROM logs WHERE event_id IN ('e2', 'e5')",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let csv = std::fs::read_to_string(dir.path().join("q.csv")).unwrap();
    assert_eq!(
        csv,
        "event_id,body,n\r\ne2,\"quote \"\" comma , and\nnewline\",2\r\ne5,\"{\"\"i\"\":5,\"\"k\"\":[1,2.5,null,true]}\",5\r\n"
    );

    let o = strata(&[
        "export",
        "--data-dir",
        &s("d"),
        "--tenant",
        "t",
        "--output",
        &s("e.csv"),
        "--format",
        "csv",
        "--where",
        "service = 'svc1'",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let csv = std::fs::read_to_string(dir.path().join("e.csv")).unwrap();
    let mut lines = csv.split("\r\n");
    assert_eq!(
        lines.next().unwrap(),
        "event_time,service,severity,body,trace_id,span_id,event_id,attributes,resource_attributes"
    );
    assert_eq!(json_out(&o)["rows"], 7); // i % 3 == 1 for i in 0..20
                                         // Aggregates cannot be exported page by page.
    let o = strata(&[
        "export",
        "--data-dir",
        &s("d"),
        "--tenant",
        "t",
        "--output",
        &s("agg.csv"),
        "--sql",
        "SELECT count(*) FROM logs",
    ]);
    assert!(!o.status.success());
}

#[test]
fn idempotent_import_with_report() {
    let dir = tempfile::tempdir().unwrap();
    let s = |n: &str| dir.path().join(n).to_str().unwrap().to_owned();
    write_events(&dir.path().join("in.ndjson"), 230);
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("in.ndjson"))
            .unwrap();
        writeln!(f, "{{broken").unwrap();
        writeln!(f, r#"{{"ingest_time": 5}}"#).unwrap();
    }
    assert!(strata(&["init", "--data-dir", &s("d")]).status.success());
    let args = |report: &str| {
        vec![
            "ingest".to_owned(),
            "--data-dir".into(),
            s("d"),
            "--tenant".into(),
            "t".into(),
            "--file".into(),
            s("in.ndjson"),
            "--batch-size".into(),
            "50".into(),
            "--import-id".into(),
            "nov-25".into(),
            "--report".into(),
            s(report),
        ]
    };
    let run = |a: Vec<String>| strata(&a.iter().map(String::as_str).collect::<Vec<_>>());
    let o = run(args("r1.json"));
    assert_eq!(o.status.code(), Some(3)); // two rejected lines
    let sum = json_out(&o);
    assert_eq!(
        (
            sum["accepted"].as_u64(),
            sum["batches"].as_u64(),
            sum["duplicate_batches"].as_u64()
        ),
        (Some(230), Some(5), Some(0))
    );
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("r1.json")).unwrap()).unwrap();
    assert_eq!(report["rejected"], 2);
    assert_eq!(report["errors"][0]["line"], 231);
    assert_eq!(report["errors"][1]["line"], 232);

    // Re-running the same import writes nothing new.
    let o = run(args("r2.json"));
    let sum = json_out(&o);
    assert_eq!(
        (sum["accepted"].as_u64(), sum["duplicate_batches"].as_u64()),
        (Some(0), Some(5))
    );
    let o = strata(&[
        "query",
        "--data-dir",
        &s("d"),
        "--tenant",
        "t",
        "SELECT count(*) AS n FROM logs",
    ]);
    assert_eq!(json_out(&o)["n"], 230);
    // An existing report is never overwritten.
    let o = run(args("r1.json"));
    assert!(!o.status.success());

    // Same import id, different file content → refused (key conflict), not merged.
    write_events(&dir.path().join("in.ndjson"), 60);
    let o = run(args("r3.json"));
    assert!(
        !o.status.success(),
        "{}",
        String::from_utf8_lossy(&o.stdout)
    );
    assert!(
        String::from_utf8_lossy(&o.stderr).contains("already_exists"),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
}

#[test]
fn shell_runs_piped_statements() {
    let dir = tempfile::tempdir().unwrap();
    let s = |n: &str| dir.path().join(n).to_str().unwrap().to_owned();
    write_events(&dir.path().join("in.ndjson"), 12);
    assert!(strata(&["init", "--data-dir", &s("d")]).status.success());
    assert!(strata(&[
        "ingest",
        "--data-dir",
        &s("d"),
        "--tenant",
        "t",
        "--file",
        &s("in.ndjson")
    ])
    .status
    .success());
    let script = "SELECT service, count(*) AS n\n  FROM logs GROUP BY service ORDER BY service;\n\
                  \\rows 5\nSELECT event_id FROM logs;\n\\format json\nSELECT event_id FROM logs WHERE event_id = 'e3';\n\
                  SELECT nope FROM logs;\n\\bogus\n\\q\nSELECT 'never reached';\n";
    let o = strata_stdin(&["shell", "--data-dir", &s("d"), "--tenant", "t"], script);
    let out = String::from_utf8_lossy(&o.stdout);
    assert!(out.contains("| service | n |"), "{out}");
    assert!(out.contains("| svc0    | 4 |"), "{out}");
    assert!(out.contains("(3 rows)"), "{out}");
    assert!(out.contains("Display limit is 5 rows."), "{out}");
    assert!(out.contains("(5 rows; more rows exist"), "{out}");
    assert!(out.contains("{\"event_id\":\"e3\"}"), "{out}");
    assert!(out.contains("ERROR: invalid_input"), "{out}");
    assert!(out.contains("ERROR: unknown command \\bogus"), "{out}");
    assert!(!out.contains("never reached"), "{out}");
    assert_eq!(o.status.code(), Some(3), "a failed statement → exit 3");
}

#[cfg(feature = "parquet")]
mod parquet_tests {
    use super::*;
    use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::sync::Arc;

    #[test]
    fn parquet_round_trip_is_lossless() {
        let dir = tempfile::tempdir().unwrap();
        let p = |n: &str| dir.path().join(n);
        let s = |n: &str| p(n).to_str().unwrap().to_owned();
        write_events(&p("in.ndjson"), 250);
        for db in ["a", "b"] {
            assert!(strata(&["init", "--data-dir", &s(db)]).status.success());
        }
        let (da, db, input) = (s("a"), s("b"), s("in.ndjson"));
        let (pq, x) = (s("a.parquet"), s("x.parquet"));
        assert!(strata(&[
            "ingest",
            "--data-dir",
            &da,
            "--tenant",
            "t",
            "--file",
            &input
        ])
        .status
        .success());
        let o = strata(&[
            "export",
            "--data-dir",
            &da,
            "--tenant",
            "t",
            "--output",
            &pq,
            "--format",
            "parquet",
            "--page-size",
            "100",
        ]);
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        let sum = json_out(&o);
        assert_eq!(sum["rows"].as_u64(), Some(250));
        assert_eq!(sum["row_groups"].as_u64(), Some(3));
        assert!(!p("a.parquet.tmp").exists());

        // A valid Parquet file with the documented schema.
        let r = SerializedFileReader::new(std::fs::File::open(p("a.parquet")).unwrap()).unwrap();
        let md = r.metadata();
        assert_eq!(md.file_metadata().num_rows(), 250);
        assert_eq!(md.num_row_groups(), 3);
        let schema = md.file_metadata().schema_descr();
        let names: Vec<String> = (0..schema.num_columns())
            .map(|i| schema.column(i).name().to_owned())
            .collect();
        assert_eq!(
            names,
            [
                "event_time",
                "service",
                "severity",
                "severity_text",
                "body",
                "body_json",
                "trace_id",
                "span_id",
                "event_id",
                "attributes",
                "resource_attributes"
            ]
        );
        let ts = format!("{:?}", schema.column(0).logical_type());
        assert!(ts.contains("Timestamp") && ts.contains("NANOS"), "{ts}");
        assert!(format!("{:?}", schema.column(9).logical_type()).contains("Json"));

        // Import into B; NDJSON exports of A and B are identical.
        let o = strata(&["ingest", "--data-dir", &db, "--tenant", "t", "--file", &pq]);
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        assert_eq!(json_out(&o)["accepted"], 250);
        let (na, nb) = (s("a.ndjson"), s("b.ndjson"));
        for (d, out) in [(&da, &na), (&db, &nb)] {
            assert!(
                strata(&["export", "--data-dir", d, "--tenant", "t", "--output", out])
                    .status
                    .success()
            );
        }
        assert_eq!(
            std::fs::read(&na).unwrap(),
            std::fs::read(&nb).unwrap(),
            "parquet round trip changed events"
        );

        // Idempotent re-import of the same file.
        for want in [0u64, 3] {
            let o = strata(&[
                "ingest",
                "--data-dir",
                &db,
                "--tenant",
                "u",
                "--file",
                &pq,
                "--batch-size",
                "100",
                "--import-id",
                "pq-1",
            ]);
            assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
            assert_eq!(json_out(&o)["duplicate_batches"].as_u64(), Some(want));
        }

        // Refusals: overwrite, query mode, resume.
        let base = [
            "export",
            "--data-dir",
            &da,
            "--tenant",
            "t",
            "--format",
            "parquet",
            "--output",
        ];
        let cases: [Vec<&str>; 3] = [
            vec![&pq],
            vec![&x, "--sql", "SELECT event_id FROM logs"],
            vec![&x, "--resume"],
        ];
        for extra in cases {
            let mut a: Vec<&str> = base.to_vec();
            a.extend(extra);
            assert!(!strata(&a).status.success(), "{a:?}");
        }
        assert!(!p("x.parquet").exists());
    }

    /// A Parquet file not written by StrataDB: µs timestamps, `timestamp`,
    /// `message` and `level` aliases, an extra column that becomes an attribute.
    #[test]
    fn imports_generic_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let s = |n: &str| dir.path().join(n).to_str().unwrap().to_owned();
        let schema = Arc::new(
            parquet::schema::parser::parse_message_type(
                "message m { required int64 timestamp (TIMESTAMP(MICROS,true));
                  required binary message (STRING); optional binary level (STRING);
                  optional binary region (STRING); }",
            )
            .unwrap(),
        );
        let f = std::fs::File::create(dir.path().join("g.parquet")).unwrap();
        let mut w = parquet::file::writer::SerializedFileWriter::new(f, schema, Default::default())
            .unwrap();
        let now_us = stratadb_common::time::now_unix_nanos() / 1000;
        let mut rg = w.next_row_group().unwrap();
        let mut i = 0;
        while let Some(mut c) = rg.next_column().unwrap() {
            let strs = |v: &[&str]| v.iter().map(|x| ByteArray::from(*x)).collect::<Vec<_>>();
            match i {
                0 => c.typed::<Int64Type>().write_batch(
                    &[now_us, now_us + 1, now_us + 2],
                    None,
                    None,
                ),
                1 => c.typed::<ByteArrayType>().write_batch(
                    &strs(&["disk full", "ok", "retry"]),
                    None,
                    None,
                ),
                2 => c.typed::<ByteArrayType>().write_batch(
                    &strs(&["error", "INFO"]),
                    Some(&[1, 0, 1]),
                    None,
                ),
                _ => {
                    c.typed::<ByteArrayType>()
                        .write_batch(&strs(&["eu-1"]), Some(&[0, 1, 0]), None)
                }
            }
            .unwrap();
            c.close().unwrap();
            i += 1;
        }
        rg.close().unwrap();
        w.close().unwrap();
        let (d, g) = (s("d"), s("g.parquet"));
        assert!(strata(&["init", "--data-dir", &d]).status.success());
        let o = strata(&["ingest", "--data-dir", &d, "--tenant", "t", "--file", &g]);
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        assert_eq!(json_out(&o)["accepted"], 3);
        let o = strata(&[
            "query", "--data-dir", &d, "--tenant", "t",
            "SELECT body, severity, severity_text, attributes['region'] AS region, event_time FROM logs ORDER BY event_time",
        ]);
        let rows: Vec<serde_json::Value> = String::from_utf8_lossy(&o.stdout)
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["body"], "disk full");
        assert_eq!(rows[0]["severity"], "ERROR");
        assert_eq!(rows[0]["severity_text"], "error");
        assert_eq!(rows[1]["region"], "eu-1");
        assert_eq!(rows[1]["severity"], "UNSPECIFIED");
        assert_eq!(rows[2]["severity"], "INFO");
        let t0 = stratadb_common::time::format_rfc3339(now_us * 1000);
        assert_eq!(
            rows[0]["event_time"],
            t0.as_str(),
            "µs timestamps converted to ns"
        );
    }
}

/// Import of Parquet files written by pyarrow and DuckDB
/// (`tests/fixtures/parquet-interop`, see its README).
#[cfg(feature = "parquet")]
#[test]
fn imports_third_party_parquet_files() {
    let fx = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/parquet-interop");
    let dir = tempfile::tempdir().unwrap();
    let query = |d: &str, sql: &str| -> Vec<serde_json::Value> {
        let o = strata(&["query", "--data-dir", d, "--tenant", "t", sql]);
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        String::from_utf8_lossy(&o.stdout)
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    };
    for codec in ["snappy", "gzip", "lz4"] {
        let d = dir.path().join(codec).to_str().unwrap().to_owned();
        let f = fx.join(format!("pyarrow_{codec}.parquet"));
        assert!(strata(&["init", "--data-dir", &d]).status.success());
        let o = strata(&[
            "ingest",
            "--data-dir",
            &d,
            "--tenant",
            "t",
            "--file",
            f.to_str().unwrap(),
        ]);
        assert!(
            o.status.success(),
            "{codec}: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        assert_eq!(json_out(&o)["accepted"], 3, "{codec}");
        let rows = query(&d, "SELECT event_time, body, severity, severity_text, attributes FROM logs ORDER BY event_time");
        assert_eq!(
            rows[0]["event_time"], "2026-11-20T10:30:00.123456789Z",
            "{codec}: ns exact"
        );
        assert_eq!(rows[0]["body"], "from pyarrow");
        assert_eq!(
            (
                rows[0]["severity"].as_str(),
                rows[0]["severity_text"].as_str()
            ),
            (Some("WARN"), Some("warning"))
        );
        assert_eq!(
            rows[0]["attributes"],
            serde_json::json!({"ctx": {"tries": 1, "user": "u1"}, "labels": {"k": "v"}, "ok": true,
                               "ratio": 0.5, "status": 200, "tags": ["a", "b"]}),
            "{codec}: nested columns keep their structure"
        );
        assert_eq!(
            rows[1]["attributes"],
            serde_json::json!({"ok": false, "status": 504, "tags": []})
        );
        assert_eq!(rows[2]["severity"], "ERROR");
    }
    // DuckDB (µs TIMESTAMPTZ, BIGINT).
    let d = dir.path().join("duck").to_str().unwrap().to_owned();
    assert!(strata(&["init", "--data-dir", &d]).status.success());
    let f = fx.join("duckdb_snappy.parquet");
    let o = strata(&[
        "ingest",
        "--data-dir",
        &d,
        "--tenant",
        "t",
        "--file",
        f.to_str().unwrap(),
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let rows = query(
        &d,
        "SELECT event_time, body, severity, attributes['n'] AS n FROM logs ORDER BY event_time",
    );
    assert_eq!(rows[0]["event_time"], "2026-11-25T10:00:00.123456000Z");
    assert_eq!(
        (
            rows[0]["body"].as_str(),
            rows[0]["severity"].as_str(),
            rows[0]["n"].as_i64()
        ),
        (Some("from duckdb"), Some("ERROR"), Some(42))
    );
    assert_eq!(rows[1]["severity"], "INFO");
    // An unsupported codec fails fast with a clear error (it used to loop forever).
    let d = dir.path().join("zstd").to_str().unwrap().to_owned();
    assert!(strata(&["init", "--data-dir", &d]).status.success());
    let started = std::time::Instant::now();
    let f = fx.join("pyarrow_zstd.parquet");
    let o = strata(&[
        "ingest",
        "--data-dir",
        &d,
        "--tenant",
        "t",
        "--file",
        f.to_str().unwrap(),
    ]);
    assert!(!o.status.success());
    let err = String::from_utf8_lossy(&o.stderr);
    assert!(
        err.contains("unsupported") && err.contains("snappy"),
        "{err}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(30));
}
