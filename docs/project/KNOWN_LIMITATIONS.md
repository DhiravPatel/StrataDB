# Known limitations (current state, 0.0.1)

## Not implemented
- HTTP server (`strata-server`) has API-token auth, TLS (rustls), and an audit
  log. No mTLS, no certificate hot-reload, no audit-log rotation, no gRPC/OTLP,
  SDKs are synchronous/blocking (Go uses contexts). Docker/compose/systemd files exist but the
  image build is unverified.
- OTLP: HTTP with protobuf or JSON (no gRPC transport); interop-tested with the official
  OpenTelemetry Go exporter, not with a Collector binary.
- The auth-failure rate limit is a single global window (not per client/IP).
  Per-tenant rate buckets are in memory (they reset on restart).
- Logs are JSON on stderr; no OpenTelemetry trace export yet.
- Compaction is single-threaded with a simple size-tiered policy.
- Retention is applied periodically (default 60 s) at segment granularity; expired
  rows stay queryable until the next run. No per-row delete API.
- Backups are full (no incremental), unencrypted, and must be copied to another
  failure domain by the operator. Restore is offline only.
- Indexes: footer statistics + per-segment Bloom filters for id fields and
  selected string attributes. No exact-match postings or inverted (token) index;
  `LIKE` always scans. Changing the Bloom key list affects only new segments.
- Idempotency is bounded (24 h / 100 000 keys by default); retries outside
  that bound, or retries without a key, are at-least-once.
- Disk reserve (512 MiB / 2 %) pauses writes before the volume fills; if
  the disk fills anyway (e.g. another process), a WAL write error still puts
  the engine into the failed state (restart required).
- Format upgrades (`strata upgrade`) cannot be rolled back except by restoring a backup.

## Durability / testing gaps
- Crash tests simulate **process** crashes. Power loss is covered only by a
  partial model (unsynced WAL frames dropped); write reordering within files
  and lost un-fsynced directory operations are not simulated.
- Fsync failures poison the WAL by design; recovery requires restart.

## Performance
- Only one internal developer-laptop baseline exists (`benchmarks/reports/`). **No performance claims are made**; no competitor comparisons.
- Segments use LZ4 chunk compression (v2); no zstd cold tier yet. Hot attribute keys (≥ 10 % of
  rows) are shredded into columns (v3); rarer keys stay in per-row maps.
- Query execution is row-at-a-time over columnar batches; memtable
  snapshots copy `Arc` handles of the tenant's rows (O(memtable rows) per query).
- Every committed segment keeps an open file descriptor.
- Manifest is rewritten in full on every flush.

## Semantics
- Without `ORDER BY`, result order is unspecified.
- Query memory budget is an estimate, not exact accounting.
- `strata` CLI requires exclusive access to the data directory (no server
  must be running) and trusts the operator-supplied `--tenant`.
