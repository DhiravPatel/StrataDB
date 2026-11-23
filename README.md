# StrataDB

A self-hostable, log-native analytical database for structured logs, written in Rust.

> **Status: pre-alpha (0.0.1).** The single-node storage core (WAL, flush,
> segments, manifest, crash recovery), a first query engine, and an
> authenticated HTTP API (TLS, audit log, OTLP/JSON, idempotent ingest),
> compaction, retention, and verified backup/restore are implemented and tested.
> It has **not** had an external security review, fuzzing, or production load
> testing. Do not use for production data yet. See
> [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) and
> [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md).

## What works today
- **Durable ingestion** through a checksummed WAL (`async` / `sync` / `group_commit`)
  with explicit commit receipts and **bounded idempotent retries** (durable dedup keys).
- **Crash recovery** verified by killing a real process at 19 transitions (WAL,
  flush, segment write, manifest, reclamation, compaction) plus a power-loss model.
- **Storage**: per-tenant columnar segments (format v2, LZ4 chunks, every byte
  checksummed), compaction, per-tenant retention with legal hold, verified
  backup/restore with restore drills, disk free-space reserve.
- **StrataQL**: `SELECT … WHERE … GROUP BY … ORDER BY … LIMIT`, aggregates,
  `time_bucket`, `LIKE/ILIKE`, `IN`, `IS [NOT] NULL|MISSING`, `EXPLAIN [ANALYZE]`,
  time/key/severity pruning, **Bloom-filter indexes** for ids, enforced budgets, cancellation.
- **Server**: HTTP API v1 + OTLP/HTTP JSON, TLS (rustls), API tokens with RBAC,
  tenant isolation end to end, hash-chained audit log, Prometheus metrics,
  TOML config, OpenAPI document.
- **Clients**: Rust, Python and Go SDKs (conformance-tested against a real server).
- **Quality gates**: ~190 tests, property tests, mutation fuzzing in every run,
  cargo-fuzz targets, reproducible benchmark harness with raw results.

## Quick start (local CLI)
```bash
cargo build --release -p stratadb-cli
B=target/release/strata
$B init --data-dir ./data
echo '{"service":"payment","severity":"ERROR","body":"timed out","attributes":{"http.status_code":504}}' > ev.ndjson
$B ingest --data-dir ./data --tenant acme --file ev.ndjson
$B query  --data-dir ./data --tenant acme \
  "SELECT service, count(*) AS n FROM logs WHERE severity = 'ERROR' GROUP BY service"
$B segments verify --data-dir ./data
```

## HTTP server
```bash
cargo build --release -p stratadb-server
S=target/release/strata-server
$S token create --data-dir ./data --tenant acme --permissions ingest,query   # prints token once
$S serve --data-dir ./data            # 127.0.0.1:7070
curl -H "Authorization: Bearer $TOKEN" --data-binary @ev.ndjson http://127.0.0.1:7070/api/v1/ingest/batch
curl -H "Authorization: Bearer $TOKEN" -d '{"query":"SELECT count(*) FROM logs"}' http://127.0.0.1:7070/api/v1/query
```
API reference: [docs/api/http.md](docs/api/http.md) · OTLP: [docs/api/otlp.md](docs/api/otlp.md) ·
configuration: [docs/operations/configuration.md](docs/operations/configuration.md) ·
deployment: [docs/operations/deployment.md](docs/operations/deployment.md) · Rust SDK: `sdk/rust`.

## Documentation
- Architecture: [ARCHITECTURE.md](ARCHITECTURE.md) · decisions: [docs/adr/](docs/adr/)
- Formats & durability: [docs/storage-format/](docs/storage-format/)
- Query language: [docs/query-language/](docs/query-language/)
- Development: [DEVELOPMENT.md](DEVELOPMENT.md) · roadmap: [ROADMAP.md](ROADMAP.md)
- Security: [SECURITY.md](SECURITY.md), [THREAT_MODEL.md](THREAT_MODEL.md)

## License
Not yet selected — see [docs/adr/0016-license.md](docs/adr/0016-license.md).
No license is granted until the project owner confirms one.
