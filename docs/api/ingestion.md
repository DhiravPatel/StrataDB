# Ingestion (current: `strata ingest`; HTTP API pending)

Input: NDJSON, one JSON object per line. Mapping (see `stratadb-schema/src/json.rs`):

| JSON field | Canonical field | Rules |
|------------|-----------------|-------|
| `event_time` \| `timestamp` | event_time | RFC 3339 string or integer **nanoseconds**; absent → ingest time; both aliases → error |
| `service` | service | string, 1–256 bytes |
| `severity` \| `level` | severity (+ severity_text) | text (case-insensitive; non-canonical spelling preserved) or OTLP number |
| `body` \| `message` | body | any JSON value |
| `trace_id` / `span_id` | trace_id / span_id | 32 / 16 hex chars; lower-cased |
| `event_id` | event_id | string, 1–256 bytes |
| `attributes` | attributes | object; types preserved (int/uint/float/bool/string/list/map/null) |
| `resource` \| `resource_attributes` | resource_attributes | object |
| `ingest_time`, `schema_version` | — | **rejected** (server-assigned) |
| `tenant_id` | attributes.tenant_id | never sets the tenant |
| any other key | attributes.<key> | conflict with `attributes.<key>` → error |

Default limits: event ≤ 1 MiB, ≤ 1024 attribute fields, depth ≤ 8, keys ≤ 256 B,
strings ≤ 256 KiB, event_time within [ingest − 365 d, ingest + 1 h].
Invalid lines are reported individually (line number, error kind, message) and
never silently dropped; valid lines are committed in batches.
