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
| GET | `/ready` | none | 200 when the engine is healthy, 503 in failed state |
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
`stats` includes `scan_workers` (workers that scanned segments) and
`est_cpu_ms` (the estimate that set the degree of parallelism), and the phase
decomposition `queue_us`, `parse_us`, `plan_us` and `exec_us` (spec VIII.1.4; `queue + parse + plan + exec ≤ elapsed_ms`,
and the remainder is serialization), plus `cpu_us`: the CPU time of the
query, counting the executing thread and the scan-pool workers that helped.
The body also carries `fingerprint`, the query's normalized 64-bit hash (16 hex
digits; literals replaced, whitespace and keyword case ignored).

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
Body 8 MiB · 32 concurrent ingest requests · 16 concurrent queries (excess → 429) ·
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
