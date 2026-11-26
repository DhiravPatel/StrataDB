# Metrics (spec §2.5, §3.2, §25)

`GET /api/v1/metrics` (permission `cluster.read`) returns the Prometheus text
format (version 0.0.4). Series are prefixed `stratadb_`. The exceptions are
the pipeline-stage families below, which use the spec's `strata_` names
(ADR-0026).

**No tenant labels.** Per-tenant series would make cardinality unbounded and
would reveal tenant names to anyone with `cluster.read`. Label sets are fixed
and small (`endpoint`, `outcome`).

## Latency and size histograms
Buckets: latencies 0.25 ms … 30 s (`le` in seconds); sizes 1 … 16 384.
Each histogram has cumulative `_bucket{le=…}`, `_sum` and `_count` series.

| Series | Meaning |
|--------|---------|
| `ingest_ack_seconds{endpoint="ingest"\|"otlp"}` | request received → acknowledged (committed at the configured durability); only acknowledged requests |
| `query_seconds` | query request latency, all outcomes |
| `wal_sync_seconds` | WAL fsync latency (group commits and segment rotations) |
| `wal_group_commit_records` | records made durable by one WAL fsync (group-commit size, spec §8.4) |
| `flush_seconds` | memtable flushes that wrote segments |

## Pipeline stages (spec VIII.1, µs resolution)
Buckets: 1 µs … 10 s. The sum is kept in nanoseconds, so sub-microsecond
stages are not lost.

| Series | Stages |
|--------|--------|
| `strata_insert_stage_seconds{stage}` | in pipeline order: `auth`, `decode` (body decompress, UTF-8, JSON framing), `validate` (event mapping and limits), `admission` (flush-backlog backpressure wait), `wal_encode` (batch encoding), `wal_append` (frame + CRC + write, including WAL lock wait), `group_commit_wait` (written → durable; sync modes), `memtable_insert`, `ack` (response construction). Server stages cover `/api/v1/ingest`; engine stages cover every write path |
| `strata_query_stage_seconds{stage}` | `queue` (waiting for an execution thread), `parse`, `plan`, `exec`, `serialize`; successful queries only |

## Scan worker pool
| Series | Meaning |
|--------|---------|
| `strata_scan_pool_workers` | threads of the shared scan pool (`sx-work-NN`; `[limits] scan_threads`) |
| `strata_scan_pool_active_tasks` | scan tasks queued or running, at most 2 × workers |
| `strata_scan_pool_tasks_total{result="accepted"\|"rejected"}` | tasks offered to the pool; `rejected` means the pool was busy and the query ran that share on its own thread |

A rising `rejected` rate under load means the pool is the bottleneck. Queries
still complete (with lower parallelism). Per-query parallelism is reported in
query responses as `stats.scan_workers` and `stats.est_cpu_ms`.

Compare stage quantiles with the VIII.1.2 budgets
(docs/benchmarks/harness.md); the budgets are defined for HW-REF-1.

```promql
histogram_quantile(0.99, sum by (le, stage) (rate(strata_insert_stage_seconds_bucket[5m])))
```

Useful queries:
```promql
histogram_quantile(0.99, rate(stratadb_ingest_ack_seconds_bucket{endpoint="ingest"}[5m]))
histogram_quantile(0.95, rate(stratadb_query_seconds_bucket[5m]))
rate(stratadb_wal_group_commit_records_sum[5m]) / rate(stratadb_wal_group_commit_records_count[5m])
```

## Query counters
| Series | Meaning |
|--------|---------|
| `queries_total{outcome}` | `ok`, `invalid` (bad query / unsupported), `timeout`, `cancelled`, `rejected` (budget or concurrency limit), `error` (internal / corruption); unauthenticated requests are not counted |
| `query_rows_scanned_total`, `query_rows_matched_total` | rows scanned / matching the filter, successful queries |
| `query_segment_bytes_read_total` | stored segment bytes read |
| `query_segments_scanned_total`, `query_segments_pruned_total` | segments scanned / skipped by any pruning (time, keys, severity, Bloom, trigram, cursor) |

## Engine gauges and counters
`events_written_total`, `batches_written_total`, `flushes_total`,
`flush_failures_total`, `write_stalls_total`, `write_rejections_total`,
`duplicates_suppressed_total`, `idempotency_evictions_total`,
`quota_rejections_total`, `disk_free_bytes`, `disk_write_rejections_total`,
`compactions_total`, `segments_deleted_total`, `retention_rows_deleted_total`,
`memtable_bytes`, `segment_bytes`, `wal_durable_seq`, `engine_failed` — see the
`# HELP` text of each series.

## Suggested alerts
* `engine_failed == 1` — engine in fail-stop state (restart; see runbooks/recovery.md).
* `rate(write_rejections_total[5m]) > 0` or `disk_write_rejections_total` increasing — backpressure / disk reserve.
* p99 of `wal_sync_seconds` rising — storage latency problem (affects every acknowledgement in `sync`/`group_commit`).
* `rate(queries_total{outcome=~"timeout|rejected"}[5m])` — query budgets too tight or overload.
