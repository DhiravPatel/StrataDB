# Development

Requirements: Rust 1.95.0 (pinned in `rust-toolchain.toml`), macOS or Linux.
No paid services, no network access needed after dependencies are fetched.

```bash
scripts/check.sh            # fmt --check, clippy -D warnings, full test suite
cargo test -p stratadb-wal  # one crate
cargo test -p stratadb-storage --test crash   # process-crash matrix (~3 s)
```

The default suite is sized for a 24 GB laptop (< 1 minute). Long stress
tests must be `#[ignore]`d and run explicitly with `-- --ignored`.

Failpoints: compiled only with the `failpoints` feature (enabled in test builds).
`STRATADB_FAILPOINTS="wal::after_sync=exit@3" cargo test …` — see docs/testing.

Disk: the dev profile uses `debug = "line-tables-only"`. `cargo clean` reclaims space.
