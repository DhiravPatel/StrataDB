# Baseline report (session start, 2026-11-20)

- Working directory `StrataDB/` contained only `StrataDB_Master_Engineering_Specification.md` (3 556 lines); not a git repository; no existing code to preserve.
- Toolchain: rustc/cargo 1.95.0 stable (aarch64-apple-darwin); git, docker present; `cargo-audit`, `cargo-deny`, `cargo-fuzz` not installed.
- Hardware: Apple M4, 10 cores, 24 GiB RAM; macOS 27.0.1; APFS.
- **Disk: 27 GiB free of 460 GiB (94 % used)** — drove the decision to avoid heavy dependencies (ADR-0001, ADR-0006) and to reduce debug info.
- crates.io reachable.
- Actions: `git init -b main`; workspace created. No commits were made (left for the owner).
