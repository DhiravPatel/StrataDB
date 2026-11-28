# HTTP API v1 (`strata-server`)

Base path `/api/v1`. JSON bodies. TLS: start with `--tls-cert cert.pem
--tls-key key.pem` (required for non-loopback addresses unless the explicit
insecure flag is given; ADR-0021). Security-relevant calls are recorded in
the audit log (ADR-0022). All data endpoints require
`Authorization: Bearer <token>`; the tenant is taken from the token.
OpenAPI 3.1 document: [openapi.json](openapi.json) (a test asserts every documented route exists).

| Method | Path | Permission | Notes |
|--------|------|------------|-------|
| GET | `/health` | none | liveness: process is serving |
| GET | `/ready` | none | 200 when the engine is healthy and the server is `READY` or `DEGRADED`; 503 when the engine has failed (`"status":"failed"`) or the server is starting or draining (`"status":"unavailable"`). `"state"` is the server lifecycle state (spec IX.22.7) |
| GET | `/version` | none | server, API and format versions |
| POST | `/ingest/batch`, `/ingest` | `ingest` | NDJSON (default) or `application/json` object/array |
| POST | `/query` | `query` | `{"query", "timeout_ms"?, "max_rows"?, "query_id"?, "database"?: "default", "page_size"?, "cursor"?}`; unknown fields → 400; response includes `query_id` |
| GET / DELETE | `/query/{id}` | `query` | status / cancel a **running** query of the caller's tenant (others' ids → 404); a cancelled query fails with `cancelled` |
| GET | `/metrics` | `cluster.read` | Prometheus text format (series: docs/operations/metrics.md) |
| POST | `/admin/compaction` | `cluster.admin` | runs compaction passes until nothing is left to merge |
| POST | `/admin/backup` | `backup.create` | `{"name": "<[A-Za-z0-9_-]{1,64}>"}`; requires `--backup-dir`; 409 if the name exists |
| GET / PUT / DELETE | `/retention` | `retention.manage` | caller's tenant only; PUT `{"max_age_days": n, "hold": bool}` |

Restore is offline only (`strata backup restore`).

## Ingest response
```json
{"accepted": 2, "rejected": 1, "errors": [{"index": 1, "error": "invalid_input", "message": "…"}],
 "commit_seq": 17, "durability": "wal_synced", "visible_to_queries": true,
 "flushed_to_segments": false, "replicated": false}
```
All valid records of one request commit **atomically** as one WAL batch;
invalid records are reported by 0-based index (first 100) and never stored.
If no record is valid: `422`. Durability fields state exactly which boundary
was reached (spec §2.1); `wal_written` in async mode.

## Idempotent retries
Send `Idempotency-Key: <1–128 visible ASCII>`; scope is per tenant, window
24 h (≤ 100 000 keys; oldest evicted first). A byte-identical retry returns
the original `commit_seq`/`accepted` with `"duplicate": true` and stores
nothing; the same key with a different body → `409 already_exists`; a retry
while the first attempt is still committing → `503` (retry). Survives restarts
and flushes. Beyond the window/capacity, delivery is at-least-once (ADR-0019).

## Query response
`{"columns": [...], "rows": [{col: value}], "explain": null|text, "partial": false, "next_cursor": null|string, "stats": {...}}`.
`stats` includes `sources_from_metadata` (segments and memtable windows
answered from metadata without reading columns, e.g. a plain `count(*)`;
ADR-0037), `scan_workers` (workers that scanned segments) and
`est_cpu_ms` (the estimate that set the degree of parallelism), and the phase
decomposition `queue_us`, `parse_us`, `plan_us` and `exec_us` (spec VIII.1.4; `queue + parse + plan + exec ≤ elapsed_ms`,
and the remainder is serialization), plus `cpu_us`: the CPU time of the
query, counting the executing thread and the scan-pool workers that helped.
The body also carries `fingerprint`, the query's normalized 64-bit hash (16 hex
digits; literals replaced, whitespace and keyword case ignored).

**Encoding** (ADR-0044):
- **Values:** timestamps are RFC 3339 strings with 9 fractional digits,
  bytes are lowercase hex, and floats use the shortest round-trip form
  (non-finite floats are `null`).
- **Rows:** missing values are omitted from a row, keys are in sorted
  order, and the body has an exact `Content-Length`.
- **Where it happens:** rows are encoded on the blocking pool, column by
  column, and the time is counted in the `serialize` stage.

**Work classes and admission** (spec VIII.11, ADR-0036):
- **Classes.** Each query has a class: `interactive` (estimated < 100 ms of
  CPU), `normal`, or `batch` (≥ 10 s). The estimate comes from metadata at
  plan time. An operator may assign a tenant's class
  (`[workload.tenant_classes]`). A request may lower its own class with
  `"workload_class": "normal" | "batch"`, but can never raise it.
- **Reported class.** `stats.class` is the class at admission;
  `stats.final_class` is the class at the end. A query running past twice
  its class budget is demoted one class.
- **Queueing.** A query without a free slot waits in a bounded fair queue:
  class weight, then tenant, then arrival order. While it waits:
  - `GET /api/v1/query/{id}` reports `"state": "queued"`, the class and an
    approximate `queue_position` (0 = next);
  - `DELETE` cancels it.
- **Refusals.**
  - A full queue, or a wait beyond `queue_timeout_ms` (5 s by default, at
    most the query timeout), gives `429` with
    `{"code": "TOO_MANY_QUERIES", "retry_after_ms": n}` and `Retry-After`.
  - Severe memory pressure (Linux PSI) gives `503 SERVER_OVERLOADED` to
    non-interactive queries.

Every query response also carries these headers, error responses included
(STRATA-SYS-11-006):
- `x-stratadb-query-id` and `x-stratadb-fingerprint`;
- `x-stratadb-queue-us`, the wait for a worker;
- on success, `x-stratadb-parse-us`, `x-stratadb-plan-us`,
  `x-stratadb-exec-us` and `x-stratadb-cpu-us`, all in whole microseconds.

Each successful query is also logged as a structured `query` event (target
`query`) with these timings, the fingerprint, the rows scanned and returned,
and the bytes read. It contains no SQL text or values.

### `system.query_log`
Every query is recorded once, whether it succeeded or failed (including one
rejected for concurrency). Unauthenticated requests are not recorded. Read the
log with an ordinary query: `SELECT ... FROM system.query_log`.
- **Scope:** each tenant sees only its own entries.
- **Schema:** the event schema, so pruning, aggregation, `ORDER BY` and
  pagination work as for `logs`. Each entry is one row:
  - `event_time`: when the request arrived;
  - `event_id`: the query id;
  - `service`: `strata`;
  - `severity`: `INFO` if the query succeeded, `ERROR` if it failed;
  - `body`: the normalized query text, with literals replaced by `?`, so
    values a query names are not copied into the log;
  - `attributes`: the measurements, listed below.
- **Always present:** `query_id`, `fingerprint`, `status` (`ok` or the error
  kind), `queue_us`, `elapsed_us`, `serialize_us` and `result_rows`.
- **Present when the query ran:**
  - the table read: `table`;
  - phase timings and CPU: `parse_us`, `plan_us`, `exec_us`, `cpu_us`;
  - work done: `rows_scanned`, `rows_matched`, `bytes_read`,
    `segments_total`, `segments_scanned` and `memory_rows_scanned`;
  - resources: `memory_peak` and `scan_workers`.
- **Write timing:** entries are written every `[observability]
  query_log_flush_ms` (default 7.5 s), so a crash loses at most that
  interval's entries. Recording never blocks or fails a query: when the
  queue is full, entries are dropped and counted
  (`strata_query_log_dropped_total`).

Top fingerprints by CPU:
```sql
SELECT attributes['fingerprint'] AS fp, count(*) AS n, sum(attributes['cpu_us']) AS cpu_us
FROM system.query_log WHERE event_time >= TIMESTAMP '2026-11-26T00:00:00Z'
GROUP BY attributes['fingerprint'] ORDER BY cpu_us DESC LIMIT 10
```
Missing values are **omitted** from row objects; null is JSON `null`.
Duplicate column names get `#2`, `#3` suffixes. Timestamps are RFC 3339 strings.

## Pagination
Row queries can be read page by page (ADR-0023). Send `page_size` (capped at
`max_rows`) on the first request; while the response carries a non-null
`next_cursor`, send the **same `query` text** again with `"cursor": next_cursor`.

- Rows are ordered by the query's `ORDER BY`, then by commit order; without
  `ORDER BY` the order is commit (ingest) order.
- Pages never overlap. Every row acknowledged before the first page (and not
  since removed by retention) is returned exactly once; rows committed after
  the first page never appear. Flushes and compaction between pages are safe.
- `LIMIT` bounds the total across pages.
- Cursors are opaque, bound to the tenant and query text, and do not expire;
  a cursor for another query or tenant, or an edited cursor, → `400`.
- Aggregate and `EXPLAIN` queries cannot be paginated (`400`).

## Errors
`{"error": "<kind>", "message": "...", "retryable": bool}`; retryable errors
carry `Retry-After: 1`. Status mapping: invalid_input/unsupported 400,
unauthenticated 401, permission_denied 403, payload too large 413,
no valid records 422, resource_exhausted 429, unavailable/indeterminate 503,
timeout 504, corruption/io/internal 500 (details logged server-side only).

## Limits (defaults)
Body 8 MiB · 32 concurrent ingest requests · 16 concurrent queries (interactive
16, normal 12, batch 4; up to 64 more wait in the queue, then 429) ·
query timeout ≤ 60 s · ≤ 100 000 result rows · 50 failed auth attempts/s globally (→ 429).

## Tokens
`strata-server token create --data-dir D --tenant T --permissions ingest,query [--ttl-days N]`
prints the token once. `token list`, `token revoke --id`. The running server
**hot-reloads** the token file (checked at most every 2 s), so creation,
revocation and rotation need no restart; a corrupt file is not applied (the
previous tokens stay active) and the failure is logged.

## Quotas (per tenant)
Configured in `[quotas]` (defaults) and `[quotas.tenants.<id>]` (overrides):
`ingest_events_per_sec` + `ingest_burst_events` (token bucket; exceeding it →
`429`, retryable; a single batch larger than the burst → `400`) and
`storage_bytes` (segments + memtable; exceeding it → `429` until retention or a
higher quota frees space). Counted in `stratadb_quota_rejections_total`.

## Correlation and logs
Every response carries `x-request-id` (a client-supplied id matching
`[A-Za-z0-9_-]{1,64}` is kept, anything else is replaced). The server writes
JSON logs to stderr (`STRATADB_LOG`, default `info`): engine events plus one
`access` line per request (method, path, status, latency, request id; never
headers, bodies or credentials).

### `GET /api/v1/admin/memory` (cluster.read)
The memory budget and its regions, plus the live accounting tree (ADR-0030):
- `plan`: the byte limits: `budget`, `source` (`cgroup`, `physical` or
  `max_server_memory_bytes`), `control`, `ingest`, `cache`, `background`,
  `query`, `per_query`;
- `tracked_bytes`: the total held;
- `tree`: `{name, used, peak, limit, children}` from `process` down to
  `query/tenant:<id>/query:<id>/exec`.

Tenants appear by name here, which is why it needs an operator token.

A query refused for memory gets `429 resource_exhausted` (retryable). Its
message starts with `MEMORY_LIMIT_EXCEEDED` and names:
- the tracker path that refused it, with its limit and usage;
- the requested bytes;
- the top 5 consumers. Other tenants appear only as `<other>`.

### `GET /api/v1/whoami` (any valid token)
`{"tenant", "token_id", "permissions": [...], "expires_ms"}` for the
calling token. The secret is never returned.

### `GET /api/v1/schema` (query)
`{"tables": [{"name", "enabled", "columns": [{"name", "type"}], "attribute_keys": [{"key", "scope", "rows"}], "attribute_keys_truncated"}]}`
for `logs` and `system.query_log`.
- **Attribute keys:** the caller's attribute (`scope: "attributes"`) and
  resource (`"resource"`) keys, with the rows of the flushed segments that
  hold them.
- **Bounded:** at most 500 per scope, most common first, read from segment
  metadata (no scan).

### `GET /api/v1/queries` (query)
The caller's queued and running queries:
`{"queries": [{"query_id", "state", "class", "queue_position"?, "elapsed_ms", "fingerprint", "cancel_requested"}]}`.

### `GET /api/v1/admin/backup` (backup.create)
`{"enabled", "backups": [{"name", "created_unix_ms", "bytes", "rows", "segments"}], "truncated"}`:
- backups under `--backup-dir`, newest first, at most 200;
- `enabled: false` without `--backup-dir`.

### `GET /api/v1/admin/caches` (cluster.read)
The plan cache:
- `hits`, `misses`, `insertions`, `evictions`, `entries`, `bytes`, the
  capacities, and `hit_p99_us`;
- `top_fingerprints`: the 100 most looked-up query shapes, with
  `normalized` text, hits, misses and `hit_ratio`.

Query responses report `stats.plan_cache` (`hit`/`miss`), `est_rows`,
`q_error`, `sources_skipped_by_order`, `column_cache_hits`,
`sort_runs_merged` (sorted segment runs merged) and `topk_rows_skipped`
(rows of an `ORDER BY ... LIMIT` beaten by the shared top-k bound before
being built). The
`column_cache` object (present when `[storage] column_cache_bytes` > 0)
reports the decoded column cache's hits, misses, bytes and protected
entries.

### `GET /api/v1/admin/snapshots` (cluster.read)
Open snapshots of the main (`logs`) and query log engines (spec
STRATA-SYS-17-005):
- each snapshot: `id`, `tenant`, `created_unix_ms`, `age_ms`, `holder`
  (`query:<id>` for queries), `segments`, and the
  `obsolete_segments_pinned` / `obsolete_bytes_pinned` it keeps alive;
- totals: `obsolete_segments`, `obsolete_bytes`, `disk_total_bytes`,
  `obsolete_ratio`, and `warn`, true above 10% of the disk.

Tenants appear by name, which is why it needs an operator token.

### `GET /api/v1/admin/workload` (cluster.read)
Workload classes and admission (spec VIII.11.4, STRATA-SYS-11-010):
- totals: `max_concurrent_queries`, `running`, `queued`, `queue_max`,
  `cpu_utilization`, `batch_adjustments_total` (AIMD steps), and the PSI
  readings `pressure_cpu_some_avg10` and `pressure_memory_full_avg10`
  (`null` without PSI);
- per class (`interactive`, `normal`, `batch`): `slots` (batch adapts),
  `configured_slots`, `running`, `queued`, admission outcome counters,
  `demoted_total`, `arrival_rate`, `mean_service_ms`, `utilization`
  (arrival rate × mean service / slots, as in Little's law),
  `queue_wait_p99_ms` and `cpu_seconds_total`.

If a class's `utilization` approaches 1, its queue grows without bound.
Raise its slots or reduce service time.

### `GET /api/v1/admin/threads` (cluster.read)
The thread inventory, StrataDB's equivalent of the spec's `system.threads`
(STRATA-SYS-03-014). Response fields:
- `logical_cpus` and `budget` (`2 × logical_cpus + 32`);
- `registered`;
- `os_threads` and `untracked` (Linux only, otherwise `null`);
- `within_budget` and `by_role`;
- `pools.scan` and `pools.blocking`;
- `threads`: a list of `{name, role, cpu_seconds, cpu_age_ms, affinity}`.

Each thread records its own CPU time, so `cpu_age_ms` gives the age of the
figure.
