# Dependency review

All runtime dependencies are open source, run locally, need no paid or hosted
service, and transmit no data. Licenses verified with `cargo metadata` on 2026-11-20.

| Crate | Version | License | Why | Notes |
|-------|---------|---------|-----|-------|
| thiserror | 2 | MIT OR Apache-2.0 | error derive | ubiquitous, no runtime deps |
| crc32c | 0.6 | MIT OR Apache-2.0 | hardware CRC-32C | verified against RFC 3720 vectors |
| serde / serde_json | 1 | MIT OR Apache-2.0 | manifest JSON, JSON ingestion | de-facto standard |
| parking_lot | 0.12 | MIT OR Apache-2.0 | Mutex/Condvar/RwLock with `unlocked`, timed waits, no poisoning | widely used |
| tracing | 0.1 | MIT | structured diagnostics | subscriber not yet chosen |
| clap | 4 (CLI only) | MIT OR Apache-2.0 | argument parsing (spec §4 suggestion) | default features off |
| axum | 0.8 (server only) | MIT | HTTP routing, body limits (ADR-0018) | Tokio project; default features off |
| tokio | 1 (server only) | MIT | async runtime, signals | io-util only in tests |
| sha2 | 0.10 (server only) | MIT OR Apache-2.0 | token verifier digests | RustCrypto |
| subtle | 2.6 (server only) | BSD-3-Clause | constant-time comparison | dalek-cryptography |
| getrandom | 0.3 (server only) | MIT OR Apache-2.0 | OS CSPRNG for token secrets | rust-random |
| fs4 | 1.1 (storage) | MIT OR Apache-2.0 | free-space query without our own `unsafe` (rustix) | disk reserve |
| rustls | 0.23 (server) | Apache-2.0 OR ISC OR MIT | TLS (ADR-0021); stable line, not the 0.24 dev prerelease | `ring` provider (Apache-2.0 AND ISC) |
| tokio-rustls | 0.26 (server) | MIT OR Apache-2.0 | async TLS streams | |
| ring | 0.17 | Apache-2.0 AND ISC | encryption at rest (ADR-0024): AES-256-GCM, HKDF-SHA256, CSPRNG | already present via rustls; now a direct dependency of `stratadb-crypto` |
| zeroize | 1 | MIT OR Apache-2.0 | clears key material on drop | already in the lockfile; no transitive deps |
| parquet | 54 (CLI, feature `parquet`, default-features off) | Apache-2.0 | Parquet import/export (ADR-0025) | no Arrow, no codecs; 36 crates on macOS; transitive licenses all permissive (incl. CC0-1.0 tiny-keccak, Unicode-3.0 unicode-ident) |
| rcgen (dev) | 0.14 | MIT OR Apache-2.0 | test certificates | dev-only |
| flate2 | 1 (server, SDK) | MIT OR Apache-2.0 | gzip request bodies; `rust_backend` (miniz_oxide, pure Rust) | |
| toml | 0.9 (server) | MIT OR Apache-2.0 | config file | parse + serde only |
| ureq | 3.5 (SDK) | MIT OR Apache-2.0 | blocking HTTP client with pooling; rustls + ring | SDK only |
| tempfile (dev) | 3 | MIT OR Apache-2.0 | test directories | dev-only |
| proptest (dev) | 1 | MIT OR Apache-2.0 | property tests | dev-only |

Transitive runtime licenses: MIT, Apache-2.0, ISC (ring, rustls-webpki, untrusted), BSD-3-Clause (subtle), Unlicense OR MIT (memchr),
(MIT OR Apache-2.0) AND Unicode-3.0 (unicode-ident). All compatible with the
proposed Apache-2.0 project license (ADR-0016).

Deliberately **not** adopted yet (ADR-0001, ADR-0006): arrow, parquet,
datafusion (size/build cost; adopt only with benchmark evidence).

Pending tooling: `cargo audit` (RustSec) and `cargo deny` are not installed
on the dev machine; CI workflow installs them.
