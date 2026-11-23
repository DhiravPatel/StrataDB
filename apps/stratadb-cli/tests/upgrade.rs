//! `strata upgrade` on the golden v1 fixture: dry run, backup gate, upgrade, data identical.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::process::Command;

fn strata(args: &[&str]) -> (i32, serde_json::Value, String) {
    let o = Command::new(env!("CARGO_BIN_EXE_strata"))
        .args(args)
        .output()
        .unwrap();
    (
        o.status.code().unwrap(),
        serde_json::from_slice(&o.stdout).unwrap_or(serde_json::Value::Null),
        String::from_utf8_lossy(&o.stderr).into_owned(),
    )
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let t = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &t);
        } else {
            std::fs::copy(e.path(), t).unwrap();
        }
    }
}

#[test]
fn upgrade_v1_fixture_with_backup() {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("d");
    copy_dir(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/storage-v1"),
        &data,
    );
    let d = data.to_str().unwrap();
    let count = |t: &str| {
        let o = Command::new(env!("CARGO_BIN_EXE_strata"))
            .args([
                "query",
                "--data-dir",
                d,
                "--tenant",
                t,
                "SELECT count(*) AS n FROM logs",
            ])
            .output()
            .unwrap();
        String::from_utf8_lossy(&o.stdout).trim().to_owned()
    };
    // Flush the fixture's WAL rows first so the counts include them.
    assert_eq!(
        Command::new(env!("CARGO_BIN_EXE_strata"))
            .args(["flush", "--data-dir", d])
            .status()
            .unwrap()
            .code(),
        Some(0)
    );
    let (alpha, beta) = (count("alpha"), count("beta"));
    let (code, v, _) = strata(&["upgrade", "--data-dir", d, "--dry-run"]);
    assert_eq!(code, 0, "{v}");
    assert_eq!(v["segments_by_version_before"]["1"], 2, "{v}");
    assert_eq!(v["segments_upgraded"], 0);
    // Refuses to run without a backup decision.
    let (code, _, err) = strata(&["upgrade", "--data-dir", d]);
    assert_ne!(code, 0);
    assert!(err.contains("irreversible"), "{err}");
    let bk = tmp.path().join("pre-upgrade");
    let (code, v, err) = strata(&[
        "upgrade",
        "--data-dir",
        d,
        "--backup-to",
        bk.to_str().unwrap(),
    ]);
    assert_eq!(code, 0, "{err}");
    assert_eq!(v["segments_upgraded"], 2, "{v}");
    assert_eq!(v["backup"]["verified"], true);
    // Everything is now current format; data unchanged; second run is a no-op.
    let (_, v, _) = strata(&["upgrade", "--data-dir", d, "--dry-run"]);
    assert_eq!(v["segments_to_upgrade"], 0, "{v}");
    assert_eq!((count("alpha"), count("beta")), (alpha, beta));
    let (code, v, _) = strata(&["doctor", "--data-dir", d]);
    assert_eq!(code, 0, "{v}");
    // The backup still holds the v1 segments for rollback.
    let (code, _, _) = strata(&["backup", "verify", bk.to_str().unwrap()]);
    assert_eq!(code, 0);
}
