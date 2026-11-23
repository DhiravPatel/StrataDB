//! End-to-end test of the `strata` binary: init → ingest (with a rejected
//! line) → query → status → segments verify.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::io::Write;
use std::process::{Command, Output};

fn strata(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .output()
        .unwrap()
}

fn stdout(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

#[test]
fn end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let d = data.to_str().unwrap();
    assert!(strata(&["init", "--data-dir", d]).status.success());

    let input = dir.path().join("events.ndjson");
    let mut f = std::fs::File::create(&input).unwrap();
    let now = stratadb_common::time::format_rfc3339(stratadb_common::time::now_unix_nanos());
    for i in 0..10 {
        let sev = if i % 2 == 0 { "ERROR" } else { "info" };
        writeln!(f, r#"{{"event_time":"{now}","service":"api","severity":"{sev}","body":"m{i}","attributes":{{"n":{i}}}}}"#).unwrap();
    }
    writeln!(f, "{{not json").unwrap();
    writeln!(f, r#"{{"ingest_time": 1}}"#).unwrap();
    drop(f);

    let o = strata(&[
        "ingest",
        "--data-dir",
        d,
        "--tenant",
        "acme",
        "--file",
        input.to_str().unwrap(),
    ]);
    assert_eq!(o.status.code(), Some(3), "partial rejection exit code");
    let summary: serde_json::Value = serde_json::from_str(stdout(&o).trim()).unwrap();
    assert_eq!(summary["accepted"], 10);
    assert_eq!(summary["rejected"], 2);
    let errs = String::from_utf8_lossy(&o.stderr);
    assert!(
        errs.contains(r#""line":11"#) && errs.contains(r#""line":12"#),
        "{errs}"
    );

    let o = strata(&[
        "query",
        "--data-dir",
        d,
        "--tenant",
        "acme",
        "SELECT severity, count(*) AS n FROM logs GROUP BY severity ORDER BY severity",
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert_eq!(
        stdout(&o).trim(),
        "{\"n\":5,\"severity\":\"ERROR\"}\n{\"n\":5,\"severity\":\"INFO\"}"
    );

    // Another tenant sees nothing.
    let o = strata(&[
        "query",
        "--data-dir",
        d,
        "--tenant",
        "other",
        "SELECT count(*) AS n FROM logs",
    ]);
    assert_eq!(stdout(&o).trim(), "{\"n\":0}");

    // Bad SQL → error JSON and failure exit code.
    let o = strata(&[
        "query",
        "--data-dir",
        d,
        "--tenant",
        "acme",
        "SELECT * FROM nowhere",
    ]);
    assert_eq!(o.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&o.stderr).contains("invalid_input"));

    let o = strata(&["segments", "verify", "--data-dir", d]);
    assert!(stdout(&o).contains("\"result\":\"ok\""), "{}", stdout(&o));
    let o = strata(&["status", "--data-dir", d]);
    let s: serde_json::Value = serde_json::from_str(stdout(&o).trim()).unwrap();
    assert_eq!(s["segment_rows"], 10);
    assert_eq!(s["failed"], serde_json::Value::Null);

    // Backup → verify → restore drill through the binary.
    let bk = dir.path().join("bk");
    let o = strata(&[
        "backup",
        "create",
        "--data-dir",
        d,
        "--output",
        bk.to_str().unwrap(),
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let o = strata(&["backup", "verify", bk.to_str().unwrap()]);
    assert!(stdout(&o).contains("\"result\":\"ok\""));
    let restored = dir.path().join("restored");
    let o = strata(&[
        "backup",
        "restore",
        bk.to_str().unwrap(),
        "--data-dir",
        restored.to_str().unwrap(),
    ]);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let o = strata(&[
        "query",
        "--data-dir",
        restored.to_str().unwrap(),
        "--tenant",
        "acme",
        "SELECT count(*) AS n FROM logs",
    ]);
    assert_eq!(stdout(&o).trim(), "{\"n\":10}");
    // Retention + compaction commands.
    let o = strata(&[
        "retention",
        "set",
        "--data-dir",
        d,
        "--tenant",
        "acme",
        "--max-age-days",
        "30",
        "--hold",
    ]);
    assert!(o.status.success());
    assert!(stdout(&strata(&["retention", "inspect", "--data-dir", d])).contains("\"hold\":true"));
    assert!(strata(&["retention", "enforce", "--data-dir", d])
        .status
        .success());
    assert!(strata(&["compact", "--data-dir", d]).status.success());
}
