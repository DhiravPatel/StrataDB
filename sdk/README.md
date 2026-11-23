# StrataDB SDKs

| SDK | Path | Dependencies | Conformance run |
|-----|------|--------------|-----------------|
| Rust | `sdk/rust` (`stratadb-client`) | ureq (rustls), flate2 | `cargo test -p stratadb-client` |
| Python ≥ 3.9 | `sdk/python/stratadb_client` | standard library only | `cargo test -p stratadb-server --test python_sdk` |
| Go ≥ 1.22 | `sdk/go` (package `stratadb`) | standard library only | `cargo test -p stratadb-server --test go_sdk` |

Common contract (spec §23.1): bearer-token auth; atomic batch ingest; an
idempotency key per ingest call **reused across all of its retries**;
bounded exponential backoff with full jitter honouring `Retry-After`; only
transport errors and retryable server errors are retried; gzip for bodies
≥ 1 KiB; per-attempt timeouts; cancellation (Rust/Python flag, Go `context`);
custom CA for TLS; structured errors (`kind`, `retryable`); a per-attempt
metrics hook; tokens never appear in `Debug`/`repr`/`String`.

Each conformance suite checks the same scenarios against a real server:
partial-batch receipts, duplicate detection on retry with the same key, the
gzip path, query results, a non-retryable `invalid_input`, `401` for a bad
token, and bounded retries.
