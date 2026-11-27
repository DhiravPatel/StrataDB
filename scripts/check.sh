#!/usr/bin/env bash
# Standard validation gate (spec §15). Exits non-zero on any failure.
set -euo pipefail
cd "$(dirname "$0")/.."
echo "==> cargo fmt --check";   cargo fmt --all -- --check
echo "==> layer check";        python3 scripts/check_layers.py --self-test && python3 scripts/check_layers.py
echo "==> cargo clippy";        cargo clippy --workspace --all-targets --all-features -- -D warnings
echo "==> cargo test";          cargo test --workspace
echo "==> requirement inventory"; python3 scripts/spec_inventory.py --check
if command -v cargo-audit >/dev/null; then echo "==> cargo audit"; cargo audit; else echo "(cargo-audit not installed; skipped)"; fi
echo "All checks passed."
