#!/usr/bin/env bash
# Standard validation gate (spec §15). Exits non-zero on any failure.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "==> cargo fmt --check";   cargo fmt --all -- --check
echo "==> cargo clippy";        cargo clippy --workspace --all-targets --all-features -- -D warnings
echo "==> cargo test";          cargo test --workspace
if command -v cargo-audit >/dev/null; then echo "==> cargo audit"; cargo audit; else echo "(cargo-audit not installed; skipped)"; fi
echo "All checks passed."
