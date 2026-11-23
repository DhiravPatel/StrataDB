//! Benchmark smoke test (spec §32.1): tiny run, output must be valid and complete.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

#[test]
fn bench_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_strata-bench"))
        .args([
            "--events",
            "3000",
            "--batch-size",
            "500",
            "--reps",
            "1",
            "--durability",
            "async",
            "--profile",
            "high-cardinality",
            "--data-dir",
        ])
        .arg(dir.path().join("d"))
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let j: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(j["queries"].as_array().unwrap().len(), 11);
    assert!(j["ingest"]["events_per_s_wall"].as_f64().unwrap() > 0.0);
    // The sampled trace id must be found by the exact-lookup query.
    assert!(
        j["queries"][0]["result_rows"].as_u64().unwrap() >= 1,
        "{}",
        j["queries"][0]
    );
}
