//! `strata doctor` through the real binary: detects problems, modifies nothing.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

fn strata(args: &[&str]) -> (i32, serde_json::Value) {
    let o = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .output()
        .unwrap();
    let v = serde_json::from_slice(&o.stdout).unwrap_or(serde_json::Value::Null);
    (o.status.code().unwrap(), v)
}

fn status_of(v: &serde_json::Value, check: &str) -> String {
    v["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["check"] == check)
        .map(|c| c["status"].as_str().unwrap().to_owned())
        .unwrap_or_default()
}

fn snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap().flatten() {
            if e.file_type().unwrap().is_dir() {
                stack.push(e.path());
            } else {
                out.insert(
                    e.path().display().to_string(),
                    std::fs::read(e.path()).unwrap(),
                );
            }
        }
    }
    out
}

#[test]
fn doctor_detects_problems_without_modifying_anything() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("d");
    let d = data.to_str().unwrap();
    let input = tmp.path().join("e.ndjson");
    std::fs::write(
        &input,
        (0..50)
            .map(|i| format!("{{\"body\":\"m{i}\"}}\n"))
            .collect::<String>(),
    )
    .unwrap();
    // First ingest is flushed into a segment (ingest closes with a flush); then
    // add WAL-only data via a second engine that we kill before flushing.
    assert_eq!(
        Command::new(env!("CARGO_BIN_EXE_strata"))
            .args([
                "ingest",
                "--data-dir",
                d,
                "--tenant",
                "t",
                "--file",
                input.to_str().unwrap()
            ])
            .status()
            .unwrap()
            .code(),
        Some(0)
    );
    let (code, v) = strata(&["doctor", "--data-dir", d]);
    assert_eq!(code, 0, "{v:#}");
    // Every check except host disk space (environment-dependent) must be ok.
    for c in v["checks"].as_array().unwrap() {
        if c["check"] != "disk" {
            assert_eq!(c["status"], "ok", "{c}");
        }
    }

    // Torn WAL tail: append garbage to the active WAL segment → warn, untouched.
    let wal = std::fs::read_dir(data.join("wal"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut w = std::fs::read(&wal).unwrap();
    w.extend_from_slice(&[0x55; 9]);
    std::fs::write(&wal, &w).unwrap();
    let before = snapshot(&data);
    let (code, v) = strata(&["doctor", "--data-dir", d]);
    assert_eq!((code, status_of(&v, "wal").as_str()), (0, "warn"), "{v:#}");
    assert_eq!(snapshot(&data), before, "doctor must be read-only");

    // Corrupt a segment byte → fail.
    let seg = std::fs::read_dir(data.join("segments/t"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut s = std::fs::read(&seg).unwrap();
    s[40] ^= 0xff;
    std::fs::write(&seg, &s).unwrap();
    let (code, v) = strata(&["doctor", "--data-dir", d]);
    assert_ne!(code, 0);
    assert_eq!(status_of(&v, "segments"), "fail", "{v:#}");
    assert_eq!(v["result"], "fail");

    // Missing directory → fail.
    let (code, v) = strata(&[
        "doctor",
        "--data-dir",
        tmp.path().join("nope").to_str().unwrap(),
    ]);
    assert_ne!(code, 0);
    assert_eq!(status_of(&v, "data_dir"), "fail");
}
