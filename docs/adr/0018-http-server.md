# ADR-0018: HTTP server framework and request execution model

- **Status:** Accepted
- **Date:** 2026-11-20
- **Spec references:** §2.1, §7.4, §18.2, §18.5, §18.9, §21

## Options
1. `axum` (on hyper + tokio + tower): maintained by the Tokio project,
   MIT, routing/extractors/body limits built in.
2. Bare `hyper` 1.x + `hyper-util`: fewer abstractions, more hand-written
   routing and body handling (more code to get wrong).
3. A non-tokio server (e.g. `tiny_http`): blocking, fewer features, weaker
   backpressure control.

## Decision
`axum` with `default-features = false` (http1, json, tokio only). The storage
engine stays synchronous (ADR-0017); handlers call it through
a blocking pool, and **semaphores bound concurrent ingest and query
requests** (excess requests get `429` + `Retry-After` immediately instead of
queueing without bound).

*Update 2026-11-26 (spec STRATA-SYS-03-014/015, ADR-0026):*
- The blocking pool is StrataDB's own `sx-blk-NN` pool (`[limits]
  blocking_threads`): threads start on demand up to a fixed maximum, and its
  queue holds every admitted request.
- `tokio::task::spawn_blocking` is no longer used; tokio's blocking pool is
  capped at 2 threads.
- Runtime threads are named `sx-net-NN` (`[limits] net_threads`).
- net + scan + blocking + 4 fixed threads must fit the budget
  `2 × logical_cpus + 32`, and the server refuses configurations that do not.

## Security defaults
- Every data endpoint requires a bearer API token; there is **no
  unauthenticated mode and no default credential**. Tokens are created
  explicitly (`strata-server token create`) and printed once; only a SHA-256
  verifier is stored (`<data_dir>/auth/tokens.json`, mode 0600), compared in
  constant time (`subtle`). Token secrets are 256 bits from the OS CSPRNG
  (`getrandom`).
- Tenant comes from the token, never from the request.
- Listens on `127.0.0.1` by default. Because TLS is not implemented yet,
  binding a non-loopback address requires the explicit
  `--insecure-allow-remote-without-tls` flag.
- Request bodies are capped (default 8 MiB); per-event limits from
  `stratadb_schema::Limits` apply.
- Liveness/readiness/version are unauthenticated and reveal no data.

## Consequences
Adds tokio, hyper, axum, tower, sha2, subtle, getrandom (all MIT or
MIT/Apache-2.0) to the server binary only; library crates stay runtime-free.
