# StrataStudio

`strata-studio` is a web administration and query tool for StrataDB, in the
spirit of pgAdmin. One binary serves:
- a single-page UI (embedded; no external resources);
- a JSON API that proxies to one or more StrataDB servers.

The browser never talks to StrataDB directly, and API tokens never leave the
studio process. The design is described in
[ADR-0041](../adr/0041-strata-studio.md); what is done and what is missing
is in [STATUS.md](STATUS.md).

## Install and run
```sh
cargo build --release -p stratadb-studio      # → target/release/strata-studio

# A server and a token to use (example; see docs/api/http.md).
strata-server token create --data-dir ./data --tenant demo \
  --permissions ingest,query,cluster.read,cluster.admin,retention.manage,backup.create
strata-server serve --data-dir ./data --backup-dir ./backups &

# Register it with the studio. The token is read from the environment or
# stdin, never from a flag.
STRATA_STUDIO_TOKEN=sdb_... strata-studio connection add --name local --url http://127.0.0.1:7070
strata-studio connection test --name local     # exit 0 when reachable and the token works

strata-studio serve                             # http://127.0.0.1:7171/
```

You can also add servers from the UI: **+ Server** in the object browser.

### Commands
| Command | |
|---|---|
| `strata-studio serve [--listen ADDR] [--data-dir DIR] [--config FILE]` | serve the UI and API (default `127.0.0.1:7171`, data dir `~/.strata-studio`) |
| `strata-studio connection add --name N --url URL [--no-tls-verify] [--ca-file PEM]` | add a server; token from `STRATA_STUDIO_TOKEN` or the first line of stdin |
| `strata-studio connection list [--json]` | list servers (tokens are never printed) |
| `strata-studio connection remove --name N` | remove a server |
| `strata-studio connection test --name N` | probe reachability, readiness, version and token permissions (JSON; exit 1 on failure) |

All commands take `--data-dir` / `--config`.

Environment variables:
- `STRATA_STUDIO_DATA_DIR`, `STRATA_STUDIO_CONFIG`, `STRATA_STUDIO_LISTEN`;
- `STRATA_STUDIO_PASSWORD` (the studio password);
- `STRATA_STUDIO_LOG`: a tracing filter, default `info`. Logs are JSON on
  stderr.

### Configuration file
Everything is optional. Command-line flags win over the environment, which
wins over the file.

```toml
listen = "127.0.0.1:7171"
data_dir = "~/.strata-studio"
# Accept these extra Host names on a loopback listener (e.g. behind a local proxy).
allowed_hosts = []
# Allow a non-loopback listener without TLS, behind a TLS-terminating proxy.
allow_insecure_http = false

[auth]
# Either; STRATA_STUDIO_PASSWORD overrides both. At least 12 characters.
password_file = "/etc/strata-studio/password"   # first line; keep it 0600
# password = "..."

[tls]                       # serve HTTPS (rustls)
cert = "/etc/strata-studio/cert.pem"
key = "/etc/strata-studio/key.pem"

[limits]
max_response_bytes = 33554432    # upstream response cap (32 MiB)
request_timeout_ms = 15000       # health/metrics/admin calls
max_query_timeout_ms = 310000    # a query's call: its timeout_ms + 5 s, capped here
admin_timeout_ms = 600000        # compaction and backup
max_concurrent_upstream = 32     # upstream calls in flight
upstream_queue_timeout_ms = 5000 # wait for a slot, then 503
max_body_bytes = 1048576         # browser request bodies
session_ttl_secs = 43200         # idle session lifetime
max_sessions = 1024
```

## Using the UI
- **Object browser:** Servers → each server (status dot: green ready, amber
  reachable but not ready or degraded, red unreachable) → Dashboard, Query
  Tool, Tables, Query Log and Admin.
  - **Tables** lists `logs` and `system.query_log` with their columns, and
    the query log's attributes. Clicking a table opens a query tool with a
    sample query.
- **Query tool:**
  - **Running:** Ctrl/Cmd+Enter runs the query, or the selection if there
    is one. Explain and Explain Analyze prefix the statement.
  - **Cancel:** the studio gives every run a `query_id`, so Cancel sends
    `DELETE /api/v1/query/{id}`. While a query runs, its state (queued with
    its position, or running) is polled.
  - **Paging:** results come page by page (`page_size`, `next_cursor`).
    Fetched pages are kept for Prev/Next. Aggregates and EXPLAIN cannot be
    paginated, so they are rerun without `page_size`.
  - **Stats:** elapsed and round trip, the queue/parse/plan/exec/cpu
    timings, the work class (and any demotion), rows scanned and matched,
    segments, bytes read, fingerprint and query id.
  - **Errors and refusals:** errors show StrataDB's `error`, `code` and
    message. A refusal (`429 TOO_MANY_QUERIES`, `503 SERVER_OVERLOADED`)
    counts down `retry_after_ms` before enabling Retry.
  - **Export:** CSV (formula-injection-safe) or JSON of the fetched rows,
    made in the browser.
  - **History:** the last 100 queries per server, kept in this browser's
    localStorage.
- **Dashboard:** refreshes every 2, 5, 10 or 30 s (or paused); it pauses
  while the tab is hidden. It shows:
  - health, readiness and the lifecycle state (`READY`, `DEGRADED`,
    `DRAINING` and so on), and the version;
  - ingest and query rates with sparklines;
  - query, ingest, WAL fsync and flush latency (p50/p95/p99/mean over the
    refresh interval, from histogram buckets);
  - storage gauges;
  - workload classes (slots, running, queued, refusals, utilization,
    p99 wait);
  - memory regions;
  - snapshots and the bytes they retain;
  - threads and pools.

  Sections the token cannot read (they need `cluster.read`) say so.
- **Admin:** compaction (`cluster.admin`), backup (`backup.create`; needs
  the server's `--backup-dir`), and retention view/set/clear
  (`retention.manage`).
  - Every action asks for confirmation in an in-page dialog.
  - Setting a retention that deletes data asks you to type the number of
    days.
- **Query log:** recent queries from `system.query_log`, filtered by time
  window, status, fingerprint, text and minimum duration. A second view
  lists the top fingerprints by CPU. Click an entry for details or to open
  its normalized text in a query tool.
- **Themes:** light and dark (by default, the OS setting; the toggle is
  remembered).
- **Responsive:** on narrow screens the browser becomes an overlay.

## Security model
- **Tokens:**
  - stored only in `<data-dir>/connections.toml`: mode `0600`, in a `0700`
    directory, written atomically;
  - sent only as `Authorization: Bearer` from the studio to StrataDB;
  - never returned to the browser (`token_set: true` instead), never
    logged, never put in a URL.
  - Edit a connection without a token to keep the saved one.
- **Listener:** loopback by default. A non-loopback `--listen` needs:
  - a studio password (`STRATA_STUDIO_PASSWORD` or `[auth]`);
  - TLS (`[tls]`), or `allow_insecure_http = true` behind a TLS proxy.
- **Login:** an `HttpOnly`, `SameSite=Strict` session cookie (`Secure`
  under TLS), 12 h idle expiry, and at most 20 failed logins per minute.
  With no password configured (loopback only), there is no login.
- **CSRF:** state-changing requests need `X-Strata-Studio: 1`. A present
  `Origin` must match `Host`, and a present `Sec-Fetch-Site` must be
  same-origin.
- **DNS rebinding:** a loopback studio rejects `Host` names that are not
  loopback, unless they are listed in `allowed_hosts`.
- **Headers:** a strict CSP (`'self'` only, no inline script or style,
  `frame-ancestors 'none'`), `nosniff`, `X-Frame-Options: DENY`,
  `no-referrer`, COOP/CORP, HSTS under TLS, and `no-store` on the API.
- **Bounds:** body, response size, timeouts and upstream concurrency (see
  `[limits]`). Upstream redirects are not followed, and proxy environment
  variables are ignored.
- **Audit:** state changes are logged with target `audit`: the action, the
  actor (`local`, `cli` or a session label), the connection and the outcome.
  The log never contains secrets or query text. StrataDB also audits the
  admin calls themselves (ADR-0022).
- **Tenancy:** each connection's token belongs to one tenant, and StrataDB
  enforces that. The studio adds no access of its own beyond "can log in to
  the studio": every studio user can use every saved connection.

## Studio API (used by the UI)
Errors are `{"error", "message", "retryable", "source": "studio"|"upstream"}`.
Upstream errors add `upstream_status` and, when present, `code`,
`retry_after_ms`, `query_id`, `fingerprint` and `request_id`. They keep
StrataDB's status code (for example 429), and a `Retry-After` header when
StrataDB sent one.

| Method | Path | |
|---|---|---|
| GET | `/api/session` | `auth_required`, `authenticated`, version, limits |
| POST | `/api/auth/login`, `/api/auth/logout` | `{"password"}` |
| GET, POST | `/api/connections` | list (masked) / add `{name, url, token, tls_verify?, ca_file?}` |
| GET, PUT, DELETE | `/api/connections/{name}` | view / patch `{url?, token?, tls_verify?, ca_file?}` / remove |
| POST | `/api/connections/{name}/test`, `/api/connection-test` | probe a saved / unsaved connection |
| GET | `/api/servers/{name}/status` | health + ready + version in one call |
| GET | `/api/servers/{name}/health`, `/ready`, `/version` | proxied |
| POST | `/api/servers/{name}/query` | `{query, query_id?, page_size?, cursor?, workload_class?, timeout_ms?, max_rows?}` |
| GET, DELETE | `/api/servers/{name}/query/{id}` | status / cancel |
| GET | `/api/servers/{name}/metrics` | dashboard summary (rates and interval latencies since the previous call) |
| GET | `/api/servers/{name}/metrics/families` | every parsed Prometheus family |
| GET | `/api/servers/{name}/admin/{workload,memory,threads,snapshots}` | proxied |
| POST | `/api/servers/{name}/admin/compaction`, `/admin/backup` | proxied (`{"name"}` for backup) |
| GET, PUT, DELETE | `/api/servers/{name}/retention` | proxied (`{max_age_days, hold}`) |

The registry file (`connections.toml`, format 1):
```toml
version = 1
[[connection]]
name = "local"
url = "http://127.0.0.1:7070"
token = "sdb_..."
tls_verify = true
# ca_file = "/abs/path/ca.pem"
```

## Development
```sh
cargo build -p stratadb-studio
cargo test -p stratadb-studio          # unit tests + tests/studio.rs (real in-process server)
cargo clippy -p stratadb-studio --all-targets -- -D warnings
```

The UI lives in `apps/stratadb-studio/ui/` and is embedded at compile time,
so rebuild after editing it. Each new asset must also be listed in
`src/web.rs` (`ASSETS`). A test fails if a module imports a file that is
not embedded.
