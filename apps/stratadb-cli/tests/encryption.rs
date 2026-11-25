//! Encryption-at-rest operator workflow through the `strata` binary (ADR-0024).

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output};

const SECRET: &str = "SSN-078-05-1120";

fn strata(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .env_remove("STRATADB_KEY_FILE")
        .output()
        .unwrap()
}

fn ok(o: &Output) -> serde_json::Value {
    assert!(
        o.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&o.stdout),
        String::from_utf8_lossy(&o.stderr)
    );
    let s = String::from_utf8_lossy(&o.stdout);
    serde_json::from_str(s.trim().lines().last().unwrap_or("null"))
        .unwrap_or(serde_json::Value::Null)
}

fn err_kind(o: &Output) -> String {
    assert!(
        !o.status.success(),
        "{}",
        String::from_utf8_lossy(&o.stdout)
    );
    let e: serde_json::Value = serde_json::from_str(
        String::from_utf8_lossy(&o.stderr)
            .trim()
            .lines()
            .last()
            .unwrap(),
    )
    .unwrap();
    e["error"].as_str().unwrap().to_owned()
}

fn contains_secret(dir: &Path) -> bool {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if std::fs::read(&p)
                .unwrap()
                .windows(SECRET.len())
                .any(|w| w == SECRET.as_bytes())
            {
                return true;
            }
        }
    }
    false
}

#[test]
fn encrypted_workflow() {
    let t = tempfile::tempdir().unwrap();
    let p = |n: &str| t.path().join(n).to_str().unwrap().to_owned();
    // Keys: created 0600, never overwritten.
    ok(&strata(&["keys", "generate", "--output", &p("k1")]));
    ok(&strata(&["keys", "generate", "--output", &p("k2")]));
    assert!(!strata(&["keys", "generate", "--output", &p("k1")])
        .status
        .success());
    {
        use std::os::unix::fs::PermissionsExt;
        let m = std::fs::metadata(p("k1")).unwrap().permissions().mode();
        assert_eq!(m & 0o777, 0o600);
    }
    let mut f = std::fs::File::create(p("in.ndjson")).unwrap();
    for i in 0..30 {
        writeln!(
            f,
            r#"{{"service":"hr","body":"{SECRET} record {i}","event_id":"e{i}"}}"#
        )
        .unwrap();
    }
    drop(f);

    let d = p("data");
    ok(&strata(&["init", "--data-dir", &d, "--key-file", &p("k1")]));
    ok(&strata(&[
        "ingest",
        "--data-dir",
        &d,
        "--tenant",
        "t",
        "--file",
        &p("in.ndjson"),
        "--key-file",
        &p("k1"),
    ]));
    assert!(
        !contains_secret(Path::new(&d)),
        "plaintext found in data dir"
    );
    let q = |key: Option<&str>| {
        let mut a = vec![
            "query",
            "--data-dir",
            &d,
            "--tenant",
            "t",
            "SELECT count(*) AS n FROM logs",
        ];
        if let Some(k) = key {
            a.extend(["--key-file", k]);
        }
        strata(&a)
    };
    assert_eq!(ok(&q(Some(&p("k1"))))["n"], 30);
    assert_eq!(err_kind(&q(None)), "permission_denied");
    assert_eq!(err_kind(&q(Some(&p("k2")))), "permission_denied");

    // doctor: warns without the key, verifies everything with it.
    let doc = |key: Option<&str>| {
        let mut a = vec!["doctor", "--data-dir", &d];
        if let Some(k) = key {
            a.extend(["--key-file", k]);
        }
        let o = strata(&a);
        serde_json::from_slice::<serde_json::Value>(&o.stdout).unwrap()
    };
    let without = doc(None);
    assert!(without.to_string().contains("pass --key-file"), "{without}");
    let with = doc(Some(&p("k1")));
    let checks = with["checks"].as_array().unwrap();
    let get = |n: &str| checks.iter().find(|c| c["check"] == n).cloned().unwrap();
    assert_eq!(get("encryption")["status"], "ok", "{with}");
    assert_eq!(get("manifest")["status"], "ok", "{with}");
    assert_eq!(get("segments")["status"], "ok", "{with}");

    // Rotation: new data key, then a new key-encryption key.
    let s = ok(&strata(&[
        "keys",
        "status",
        "--data-dir",
        &d,
        "--key-file",
        &p("k1"),
    ]));
    assert_eq!(s["active_data_key"], 1);
    ok(&strata(&[
        "keys",
        "rotate-dek",
        "--data-dir",
        &d,
        "--key-file",
        &p("k1"),
    ]));
    ok(&strata(&[
        "keys",
        "rotate-kek",
        "--data-dir",
        &d,
        "--key-file",
        &p("k1"),
        "--new-key-file",
        &p("k2"),
    ]));
    assert_eq!(err_kind(&q(Some(&p("k1")))), "permission_denied");
    assert_eq!(ok(&q(Some(&p("k2"))))["n"], 30);
    let s = ok(&strata(&[
        "keys",
        "status",
        "--data-dir",
        &d,
        "--key-file",
        &p("k2"),
    ]));
    assert_eq!(s["active_data_key"], 2);
    assert_eq!(s["data_keys"], serde_json::json!([1, 2]));

    // Backups stay encrypted; verify/restore need the key.
    ok(&strata(&[
        "backup",
        "create",
        "--data-dir",
        &d,
        "--output",
        &p("bk"),
        "--key-file",
        &p("k2"),
    ]));
    assert!(!contains_secret(Path::new(&p("bk"))));
    assert_eq!(
        err_kind(&strata(&["backup", "verify", &p("bk")])),
        "permission_denied"
    );
    ok(&strata(&[
        "backup",
        "verify",
        &p("bk"),
        "--key-file",
        &p("k2"),
    ]));
    ok(&strata(&[
        "backup",
        "restore",
        &p("bk"),
        "--data-dir",
        &p("restored"),
        "--key-file",
        &p("k2"),
    ]));
    let o = strata(&[
        "query",
        "--data-dir",
        &p("restored"),
        "--tenant",
        "t",
        "--key-file",
        &p("k2"),
        "SELECT count(*) AS n FROM logs",
    ]);
    assert_eq!(ok(&o)["n"], 30);
    // Rotating on an unencrypted directory is refused.
    ok(&strata(&["init", "--data-dir", &p("plain")]));
    assert!(!strata(&[
        "keys",
        "rotate-dek",
        "--data-dir",
        &p("plain"),
        "--key-file",
        &p("k1")
    ])
    .status
    .success());
}
