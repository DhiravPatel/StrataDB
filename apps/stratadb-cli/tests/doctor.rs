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

/// Write a self-signed certificate expiring `days` from now (negative =
/// expired) and its key (mode 0600); returns their paths.
fn cert(dir: &Path, name: &str, days: i64) -> (std::path::PathBuf, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let ymd = |ns: i64| {
        let s = stratadb_common::time::format_rfc3339(ns);
        (
            s[0..4].parse::<i32>().unwrap(),
            s[5..7].parse::<u8>().unwrap(),
            s[8..10].parse::<u8>().unwrap(),
        )
    };
    let now = stratadb_common::time::now_unix_nanos();
    let day = 86_400_000_000_000i64;
    let (y0, m0, d0) = ymd(now - 400 * day);
    let (y1, m1, d1) = ymd(now + days * day);
    let mut p = rcgen::CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    p.not_before = rcgen::date_time_ymd(y0, m0, d0);
    p.not_after = rcgen::date_time_ymd(y1, m1, d1);
    let key = rcgen::KeyPair::generate().unwrap();
    let c = p.self_signed(&key).unwrap();
    let (cp, kp) = (
        dir.join(format!("{name}.pem")),
        dir.join(format!("{name}.key")),
    );
    std::fs::write(&cp, c.pem()).unwrap();
    std::fs::write(&kp, key.serialize_pem()).unwrap();
    std::fs::set_permissions(&kp, std::fs::Permissions::from_mode(0o600)).unwrap();
    (cp, kp)
}

fn config(dir: &Path, cert: &Path, key: &Path, backups: Option<&Path>) -> std::path::PathBuf {
    let mut s = format!(
        "config_version = 1\nlisten = \"127.0.0.1:0\"\n[tls]\ncert = \"{}\"\nkey = \"{}\"\n",
        cert.display(),
        key.display()
    );
    if let Some(b) = backups {
        s.push_str(&format!("[backup]\ndir = \"{}\"\n", b.display()));
    }
    let p = dir.join(format!(
        "strata-{}.toml",
        cert.file_stem().unwrap().to_string_lossy()
    ));
    std::fs::write(&p, s).unwrap();
    p
}

/// Host and deployment checks of spec §22.3: TLS expiry, backups, memory,
/// open-file limit, config validity and deep index verification.
#[test]
fn doctor_checks_host_tls_backups_and_indexes() {
    let t = tempfile::tempdir().unwrap();
    let data = t.path().join("data");
    let d = data.to_str().unwrap();
    let backups = t.path().join("backups");
    std::fs::create_dir(&backups).unwrap();
    assert_eq!(strata(&["init", "--data-dir", d]).0, 0);
    let input = t.path().join("in.ndjson");
    std::fs::write(
        &input,
        "{\"body\":\"hello doctor\",\"trace_id\":\"0123456789abcdef0123456789abcdef\"}\n",
    )
    .unwrap();
    assert_eq!(
        strata(&[
            "ingest",
            "--data-dir",
            d,
            "--tenant",
            "t",
            "--file",
            input.to_str().unwrap()
        ])
        .0,
        0
    );

    // Certificate expiring soon, no backups yet.
    let (c, k) = cert(t.path(), "soon", 10);
    let cfg = config(t.path(), &c, &k, Some(&backups));
    let (code, v) = strata(&[
        "doctor",
        "--data-dir",
        d,
        "--config",
        cfg.to_str().unwrap(),
        "--deep",
    ]);
    assert_eq!(code, 0, "{v:#}");
    assert_eq!(status_of(&v, "config"), "ok");
    assert_eq!(status_of(&v, "tls"), "warn", "{v:#}");
    assert!(v.to_string().contains("expires in"), "{v}");
    assert_eq!(status_of(&v, "backups"), "warn");
    assert_eq!(status_of(&v, "indexes"), "ok");
    assert!(["ok", "warn"].contains(&status_of(&v, "fd_limit").as_str()));
    assert!(["ok", "warn"].contains(&status_of(&v, "memory").as_str()));

    // A fresh backup satisfies the backup check.
    assert_eq!(
        strata(&[
            "backup",
            "create",
            "--data-dir",
            d,
            "--output",
            backups.join("b1").to_str().unwrap()
        ])
        .0,
        0
    );
    let (c2, k2) = cert(t.path(), "long", 200);
    let cfg2 = config(t.path(), &c2, &k2, Some(&backups));
    let (_, v) = strata(&[
        "doctor",
        "--data-dir",
        d,
        "--config",
        cfg2.to_str().unwrap(),
    ]);
    assert_eq!(status_of(&v, "backups"), "ok", "{v:#}");
    assert_eq!(status_of(&v, "tls"), "ok", "{v:#}");
    // ...unless it is older than the allowed age.
    let (_, v) = strata(&[
        "doctor",
        "--data-dir",
        d,
        "--config",
        cfg2.to_str().unwrap(),
        "--max-backup-age-hours",
        "0",
    ]);
    assert_eq!(status_of(&v, "backups"), "warn");

    // An expired certificate fails the run.
    let (c3, k3) = cert(t.path(), "old", -3);
    let cfg3 = config(t.path(), &c3, &k3, Some(&backups));
    let (code, v) = strata(&[
        "doctor",
        "--data-dir",
        d,
        "--config",
        cfg3.to_str().unwrap(),
    ]);
    assert_eq!(status_of(&v, "tls"), "fail", "{v:#}");
    assert!(v.to_string().contains("EXPIRED"));
    assert_ne!(code, 0);

    // An invalid config is a failure, not a crash.
    let bad = t.path().join("bad.toml");
    std::fs::write(&bad, "config_version = 1\nlimits = 3\n").unwrap();
    let (_, v) = strata(&["doctor", "--data-dir", d, "--config", bad.to_str().unwrap()]);
    assert_eq!(status_of(&v, "config"), "fail");

    // A too-low open-file limit fails.
    let o = Command::new("/bin/sh")
        .args([
            "-c",
            &format!(
                "ulimit -n 40 && exec '{}' doctor --data-dir '{d}'",
                env!("CARGO_BIN_EXE_strata")
            ),
        ])
        .output()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(status_of(&v, "fd_limit"), "fail", "{v:#}");
}
