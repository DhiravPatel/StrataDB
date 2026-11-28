# StrataStudio status

Crate `apps/stratadb-studio`, binary `strata-studio`. Design:
[ADR-0041](../adr/0041-strata-studio.md). Usage: [README.md](README.md).

## Done
- **Backend (Rust, axum 0.8):**
  - **Connection registry:** add, edit, test and remove servers, through
    the API and the CLI.
    - The file is TOML, format 1, written atomically with mode `0600` in a
      `0700` directory; a looser mode is tightened on load.
    - Tokens are masked (`token_set`) in every response and in `Debug`
      output, never logged, and never put in URLs.
    - URLs are normalized. Credentials, paths and queries in URLs are
      refused. A CA file can be set per connection, and TLS verification can
      be turned off per connection.
  - **Proxy:** health, ready, version and status (all three combined),
    query, query status and cancel, metrics, and the admin
    workload/memory/threads/snapshots views, compaction, backup and
    retention.
    - **Queries** carry `query_id` (the studio generates one if needed),
      `page_size`/`cursor`, `workload_class`, `timeout_ms` and `max_rows`.
      Unknown fields are refused.
    - **Bounds:** a semaphore limits concurrent upstream calls, with a
      bounded wait for a slot. Every call has a timeout, and response sizes
      are capped. Redirects are not followed and environment proxies are
      ignored.
    - **Errors** are structured JSON: StrataDB's `error`, `code`,
      `message`, `retryable` and `retry_after_ms`, plus `upstream_status`,
      `source`, `query_id`, `fingerprint` and `request_id`. They keep
      StrataDB's status and `Retry-After`.
    - **Abandoned queries:** a query whose browser request goes away is
      cancelled upstream.
  - **Metrics:** a Prometheus text parser (bounded; unit-tested).
    - It builds a dashboard summary: gauges, per-second rates between
      scrapes, p50/p95/p99/mean per interval and since start (Prometheus
      `histogram_quantile` interpolation), memory regions, workload gauges
      and threads.
    - The previous scrape is kept per connection, bounded.
  - **Security:**
    - loopback by default; a non-loopback listener requires a password, and
      TLS or an explicit `allow_insecure_http`;
    - password login with a constant-time compare, throttling, and
      `HttpOnly`, `SameSite=Strict` session cookies (`Secure` under TLS);
    - the `X-Strata-Studio` header plus `Origin`/`Sec-Fetch-Site` checks;
    - a `Host` check against DNS rebinding;
    - CSP and security headers, and body limits;
    - `audit` tracing events without secrets;
    - optional TLS listener (rustls).
  - **CLI:** `serve`; `connection add|list|remove|test`. Secrets are never
    taken from flags.
- **UI:** vanilla ES modules, embedded, with no build step and no external
  resources.
  - **Object browser:** servers with a status dot; Dashboard, Query Tool,
    Tables (`logs` and `system.query_log` with columns and query-log
    attributes), Query Log and Admin.
  - **Query tool:**
    - a highlighted editor: Tab/Shift+Tab, auto-indent, and Ctrl/Cmd+Enter
      to run the query or the selection;
    - Run, Explain, Explain Analyze, and Cancel by `query_id`, with live
      queued/running status;
    - a paged grid (Prev/Next over fetched pages); aggregates are rerun
      without `page_size`;
    - timing stats and errors, including refusals with a retry countdown;
    - CSV/JSON export;
    - bounded per-connection history in localStorage.
  - **Dashboard:** health, readiness, lifecycle state and version; ingest
    and query rates with sparklines; interval latencies; storage gauges;
    workload classes; memory regions; snapshots and retained bytes;
    threads and pools. Refresh intervals are selectable, and refreshing
    pauses while the tab is hidden.
  - **Admin:** compaction, backup (explains when backups are not
    configured) and retention view/set/clear. Each action is confirmed in a
    `<dialog>`; a destructive retention needs the number of days typed.
  - **Query log browser:** filters on window, status, fingerprint, text and
    minimum duration; a top-fingerprints-by-CPU view; entry details; "open
    in Query Tool".
  - **Quality:** light and dark themes; a responsive layout; keyboard
    access (tabs, tree, grid cells, dialogs); empty, loading and error
    states.
- **Tests:** `cargo test -p stratadb-studio`:
  - **Unit tests:** config, registry, auth/CSRF/host, metrics parser and
    summary, error mapping.
  - **Integration tests** (`tests/studio.rs`): a real in-process
    StrataDB server (with the query log enabled) and the studio, both on
    ephemeral ports. They cover:
    - the registry: masking, persistence across restarts, 0600/0700 modes,
      edits keeping the token;
    - connection probes;
    - queries: pagination, EXPLAIN, errors, studio-side validation, a bad
      token, an unreachable server;
    - the query-log browser's generated SQL;
    - queued status, cancel, `429 TOO_MANY_QUERIES` with retry hints, and
      cancel-on-disconnect;
    - metrics JSON and the 403 without `cluster.read`;
    - admin views and actions (compaction, backup and its conflict,
      backups disabled, retention);
    - auth: login, cookies, logout and throttling;
    - CSRF and `Host` checks, body limits;
    - UI assets: headers, CSP, no inline script, module imports resolving,
      no external origins.

## Known limitations
- **No JS unit tests:** the repository has no JS toolchain. The UI is
  checked through Rust tests (assets, headers, imports, and the SQL it
  generates) and by hand against a local `strata-server`.
- **Fixed schema:** columns are hard-coded from the StrataQL grammar. Only
  the query log's attributes are known; `logs` attribute keys are not
  discoverable (see the schema request below).
- **Token permissions:** they are probed without side effects for `query`,
  `cluster.read` and `retention.manage` only. `cluster.admin` and
  `backup.create` show up only when used (see the whoami request below).
- **One tenant per connection:** a connection is one token, so one tenant.
  Studio users are not separated: anyone who can log in to the studio can
  use every saved connection. There is no per-user registry and no roles.
- **Shared login throttle:** the limit is global (20 failures per minute),
  so an attacker can briefly lock out the real user.
- **Sessions** live in memory: restarting the studio logs everyone out.
- **Environment proxies** (`HTTPS_PROXY`) are ignored for upstream calls.
- **Blocking threads:** the upstream client (ureq) is blocking. Each
  in-flight call holds a blocking thread, at most `max_concurrent_upstream`
  (32).
- **Aggregate pagination:** servers since ADR-0042 ignore `page_size` on
  aggregates and answer `"paginated": false`, so those queries run once.
  Older servers reject it with 400; the studio then retries without it, and
  the failed first attempt appears in `system.query_log`.
- **Export** covers the rows fetched so far, not the whole result. Page
  through, or raise the page size, to export more.
- **Lifetime-only metrics:** the dashboard rates come from the studio's own
  previous scrape, so a new dashboard shows rates only after the second
  refresh, and several studios each keep their own baseline.
- **Memory and query log off:** on a server without memory accounting or
  the query log, those sections show StrataDB's `unsupported` message.

## Server changes requested
**Status: all five are implemented** in `strata-server` (ADR-0042, tests in
`apps/stratadb-server/tests/studio_api.rs`, documented in
`docs/api/http.md`). The schema endpoint lists keys from flushed segments
only. Adopting `whoami` and `schema` in the UI, instead of probing and the
hard-coded column list, is the next studio task.

The original requests are kept below for reference.

1. **Caller identity and permissions.** New endpoint
   `GET /api/v1/whoami`; permission: any valid token.
   ```json
   {"tenant": "acme", "token_id": "b4bf84c58e77b76e",
    "permissions": ["query", "cluster.read", "cluster.admin", "retention.manage", "backup.create"],
    "expires_ms": null}
   ```
   Lets the studio show exact permissions, including `cluster.admin` and
   `backup.create`, and grey out actions the token cannot perform, instead
   of probing. It returns no secret: `token_id` is the public id that
   `token list` already prints.
2. **Schema discovery.** New endpoint `GET /api/v1/schema`; permission:
   `query`; the caller's tenant only.
   ```json
   {"tables": [
      {"name": "logs", "columns": [{"name": "event_time", "type": "timestamp"}, ...],
       "attribute_keys": [{"key": "region", "scope": "attributes", "rows": 1200}, ...],
       "attribute_keys_truncated": false},
      {"name": "system.query_log", "enabled": true, "columns": [...],
       "attribute_keys": [{"key": "elapsed_us", "scope": "attributes"}, ...]}]}
   ```
   `attribute_keys` should be bounded (for example the top 500 by row
   count, from segment metadata or the shredded-key directory, never a
   scan). Lets the object browser list real attribute keys and drop its
   hard-coded column list.
3. **Backup configuration and listing.** New endpoint
   `GET /api/v1/admin/backup`; permission: `backup.create`.
   ```json
   {"enabled": true, "backups": [{"name": "b1", "created_unix_ms": 1764262000000, "bytes": 12345, "rows": 10}],
    "truncated": false}
   ```
   It answers `{"enabled": false, "backups": []}` without `--backup-dir`,
   and the list is bounded (for example the newest 100). Lets the Admin
   panel say up front whether backups are possible, and show existing
   names, instead of learning it from a failed `POST`.
4. **Accept `page_size` on non-paginable queries.** `POST /api/v1/query`
   with `page_size` on an aggregate or `EXPLAIN` should run the query as if
   `page_size` were absent (still bounded by `max_rows`). The response
   would add `"paginated": false` and `next_cursor: null`, instead of
   `400 "aggregate queries cannot be paginated"`. This removes the studio's
   retry and the spurious failed entry in `system.query_log`. (Alternative:
   a `"paginate": "auto"` request field with that behaviour, keeping the
   current 400 for explicit callers.)
5. **List the caller's running and queued queries** (optional, for a
   pgAdmin-like "activity" view). New endpoint `GET /api/v1/queries`;
   permission: `query`; the caller's tenant only. The list is bounded by
   the concurrency limits plus the queue, so it is naturally small.
   ```json
   {"queries": [{"query_id": "ss12…", "state": "running|queued", "class": "normal",
                 "queue_position": null, "elapsed_ms": 1200, "fingerprint": "2e17…",
                 "cancel_requested": false}]}
   ```
   It would carry no query text (or the normalized text only), like
   `system.query_log`.
