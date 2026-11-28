# ADR-0041: StrataStudio, a web administration and query tool

- **Status:** Proposed (implemented in `apps/stratadb-studio`; awaiting owner review)
- **Spec references:** IX.1.1 (layers, STRATA-ARCH-01-001); the HTTP API v1
  (docs/api/http.md, ADR-0018); TLS (ADR-0021); audit (ADR-0022);
  pagination (ADR-0023); query log (ADR-0029); workload classes (ADR-0036);
  snapshot catalog (ADR-0039)

## Context
Operators and developers had `curl` and the `strata` CLI, but nothing like
pgAdmin: a place to browse servers, run StrataQL with paging and timings,
read the dashboard series and do routine admin (compaction, backups,
retention).

The constraints:
- **No Node toolchain:** disk on the dev machine is tight, and the project
  avoids heavy dependencies (CLAUDE.md).
- **Tokens:** StrataDB API tokens grant tenant data access and must not
  leak to browsers, logs or URLs.
- **No server changes in this step:** the server is being changed in
  parallel, so the studio may only use the documented API.

## Decision
### Architecture: a proxy, not a browser client
`strata-studio` is one Rust binary (crate `stratadb-studio`, layer L7). It
serves:
- an embedded single-page UI;
- a JSON API under `/api/...`.

The JSON API's `/api/servers/{name}/...` routes call the named StrataDB
server's HTTP API v1 with that connection's token. The browser talks only to
the studio.

Why a proxy rather than calling StrataDB from the browser:
- **Tokens stay server-side.** The browser never holds a StrataDB token, so
  an XSS bug in the UI, a malicious extension, or browser storage cannot
  exfiltrate one. The registry returns `token_set: true`, never the token.
- **No CORS changes to StrataDB.** The server's API stays same-origin-free
  and needs no `Access-Control-*` policy.
- **One place for bounds.** Timeouts, response-size caps, concurrency
  limits and structured errors are enforced once, in Rust.
- **Server-side helpers.** The studio parses Prometheus text into dashboard
  JSON and computes rates between scrapes. It probes token permissions
  without side effects, and cancels a query when its browser goes away.

### UI without a build step
The UI is vanilla HTML, CSS and JavaScript ES modules (about 2,000 lines),
compiled into the binary with `include_str!`. There is no bundler, npm,
framework or CDN.
- The CSP allows only `'self'`: no inline script or style, and no `eval`.
- The DOM is built with `createElement` and `textContent`, never from HTML
  strings, so server values cannot inject markup.
- The SQL editor is a transparent `<textarea>` over a highlighted `<pre>`.
  This gives keyword highlighting, Tab/Shift+Tab indenting and
  Ctrl/Cmd+Enter without an editor library.
- Modals use the native `<dialog>` element. Charts are small SVG
  sparklines, and memory bars are `<meter>`.

The trade-off is fewer editor features (no autocompletion or multi-cursor)
than a CodeMirror- or Monaco-based UI. That is acceptable for an admin tool,
and it can change later without touching the backend.

### Dependencies
No new crates were added to `Cargo.lock`:
- **Already used by the server:** `axum` 0.8, `tokio`, `serde`/`serde_json`,
  `toml` (plus its `display` feature, for writing the registry), `clap`,
  `tracing`/`tracing-subscriber`, `rustls`/`tokio-rustls` (the studio's
  optional TLS listener), `getrandom` (session ids), `subtle` (constant-time
  compare) and `sha2`.
- **From the Rust SDK:** `ureq` 3 (rustls, WebPKI roots) is the upstream
  HTTP client. It is synchronous, so calls run on tokio's blocking pool
  behind a semaphore.

No StrataDB crate is a normal dependency: the studio uses only HTTP API v1.
- **Tests:** they start a real in-process server, so `stratadb-server`,
  `stratadb-storage` and `stratadb-common` are dev-dependencies.
- **Layer map:** `docs/architecture/layers.toml` puts the studio at L7.
- **Forbidden edges:** a `[[forbid]]` rule bars `stratadb-segment` and
  `stratadb-wal`, even in tests.

### Security model
- **Listener.** It binds `127.0.0.1:7171` by default. A non-loopback
  address fails to start unless both of these hold:
  - a studio password is configured (from `STRATA_STUDIO_PASSWORD` or the
    config file, never a flag);
  - TLS is configured (`[tls]`), or `allow_insecure_http = true` behind a
    TLS-terminating proxy.
- **Sessions.** The login compares SHA-256 digests in constant time. A
  session is a 256-bit random id in a cookie (`HttpOnly`,
  `SameSite=Strict`, `Path=/`, and `Secure` under TLS). The server keeps
  only each id's digest.
  - Sessions expire after 12 h idle, and at most 1,024 exist.
  - At most 20 failed logins are accepted per minute, globally (then 429).
    A global limit can lock out the real user during an attack; that is
    the accepted cost of keeping it simple.
- **CSRF.** Every `POST`/`PUT`/`DELETE` needs both of these, and is refused
  otherwise:
  - the `X-Strata-Studio: 1` header (a cross-origin page cannot send it
    without a preflight, which is never granted);
  - an `Origin` equal to the request's `Host`, when present, and a
    `Sec-Fetch-Site` of `same-origin`/`none`, when present.
- **DNS rebinding.** On a loopback listener, the `Host` header must be a
  loopback name (or an `allowed_hosts` entry). Otherwise an
  unauthenticated local studio could be driven by a rebinding page.
- **Headers.** Every response carries:
  - `Content-Security-Policy: default-src 'none'; script-src 'self';
    style-src 'self'; ... frame-ancestors 'none'`;
  - `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`,
    `Referrer-Policy: no-referrer`, COOP/CORP `same-origin` and a
    restrictive `Permissions-Policy`;
  - HSTS under TLS;
  - `Cache-Control: no-store` on the API.
- **Bounds.**
  - Request bodies: 1 MiB.
  - Upstream: 32 concurrent calls; a request waits at most 5 s for a slot,
    then gets 503.
  - Response bodies: 32 MiB.
  - Timeouts: 15 s per call. Queries get their `timeout_ms` + 5 s, capped
    at 310 s; compaction and backup get 600 s.
  - Tabs, history and sessions are capped too, so no queue or map grows
    without bound.
- **Upstream calls:**
  - no redirects are followed;
  - environment proxies are ignored;
  - the token travels only in `Authorization`;
  - TLS verification is on unless the operator turns it off per
    connection, and a CA file can be pinned.
- **Registry.** It is `<data-dir>/connections.toml`:
  - written atomically (temp file `0600`, fsync, rename, fsync of the
    directory);
  - the directory is `0700`, and a looser file mode is tightened on load;
  - `Debug` of a connection masks its token.
- **Audit.** Every state change is a `tracing` event with target `audit`.
  It carries the action, the actor (a non-secret label derived from the
  session id's digest, or `local`), the connection and the outcome status.
  - The actions: connection add/edit/remove/test, login/logout, compaction,
    backup, retention set/clear, query cancel and CSRF refusals.
  - It never carries tokens, passwords, cookies or query text.

## Consequences
- **Limited by the API.** The studio can do only what HTTP API v1 offers.
  The gaps (a schema endpoint, a whoami/permissions endpoint, backup
  configuration discovery, and a few others) are listed under "Server
  changes requested" in docs/studio/STATUS.md.
- **One tenant per connection.** Each connection is one token, so one
  tenant. To see several tenants, add one connection per token.
- **Blocking threads.** ureq is blocking: each in-flight upstream call holds
  a blocking thread. The semaphore bounds this to
  `max_concurrent_upstream`, 32 by default.
- **UI tests.** The UI has no JS unit tests: there is no JS toolchain in the
  repository. The Rust integration tests check that:
  - the UI's assets are served, with the right headers and no external
    references;
  - every module import resolves;
  - the query-log SQL that the UI builds runs against a real server.
