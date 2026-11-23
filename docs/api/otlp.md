# OTLP logs ingestion

Endpoint: `POST /v1/logs` (standard OTLP/HTTP path), bearer token with the
`ingest` permission; the tenant comes from the token.

- **Encoding:** OTLP/HTTP **protobuf** (`application/x-protobuf`, the OTLP
  default) and **JSON** (`application/json`); other types → `415`. The protobuf
  decoder is hand-written (no codegen), skips unknown fields, bounds nesting, and
  is verified against `protoc --encode` output (`tests/fixtures/otlp/`).
  Responses and error bodies use the request's encoding.
- **Compression:** `Content-Encoding: gzip` supported (≤ 32 MiB decompressed,
  else `400`, not retryable). Applies to `/api/v1/ingest*` too.
- **Response:** `{}` on full success; `{"partialSuccess": {"rejectedLogRecords":
  "<n>", "errorMessage": "<first error>"}}` when some records are invalid.
  Commit receipt in headers: `x-stratadb-commit-seq`, `x-stratadb-durability`,
  `x-stratadb-duplicate`, `x-stratadb-accepted`. Errors use
  `{"code": <gRPC code>, "message": ...}` with HTTP 400/401/403/429/503/500;
  retryable errors carry `Retry-After`.
- **Idempotency:** `Idempotency-Key` header works as on `/api/v1/ingest`.

## Field mapping
See the table in `crates/stratadb-schema/src/otlp.rs` (normative). Summary:
resource attributes → `resource_attributes`; `service.name` → `service`;
`timeUnixNano` → `event_time` (fallback `observedTimeUnixNano`, then ingest time);
`severityNumber` → severity bucket (exact number kept in
`attributes['otel.severity_number']` when it is not a bucket base);
`severityText` → `severity_text`; `body`/attributes keep their types
(int64 strings → int, bytes base64 → bytes); hex `traceId`/`spanId` →
lowercase (empty/all-zero → missing; invalid → record rejected);
`eventName`, `flags`, `droppedAttributesCount`, scope name/version/attributes
→ `attributes['otel.*']`. Collisions with `otel.*` keys and duplicate
attribute keys reject the record (never silently overwritten).

## OpenTelemetry Collector example
```yaml
exporters:
  otlphttp/stratadb:
    logs_endpoint: https://stratadb.example:7070/v1/logs
    # encoding: proto (default) and json are both supported
    compression: gzip
    headers:
      Authorization: "Bearer ${env:STRATADB_TOKEN}"
service:
  pipelines:
    logs:
      receivers: [otlp]
      exporters: [otlphttp/stratadb]
```
Status: tested against protoc-generated binaries, the OTLP JSON example shape,
and the **official OpenTelemetry Go SDK OTLP/HTTP log exporter** (protobuf +
gzip + batch processor; `tools/otlp-interop`, test `otlp_interop`). The
Collector binary itself has not been run (same exporter library family; needs
Docker or an otelcol build). gRPC transport is not supported.
