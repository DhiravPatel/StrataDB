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
| `strata_query_stage_seconds{stage}` | `queue` (waiting for an execution thread), `parse`, `plan`, `exec`, `serialize` (row encoding on the blocking pool plus the envelope); successful queries only |

## Group commit
`strata_wal_group_delay_seconds` (µs buckets) is the time the WAL sync leader
waited to gather a group. With adaptive group commit it is about 0 at low
load and at most min(fsync latency, max delay) under load. Read it together
with `stratadb_wal_group_commit_records` (group size) and
`stratadb_wal_sync_seconds` (fsync latency). `strata doctor storage --dir
<data>/wal` measures the device's fdatasync latency directly.

## Storage I/O (spec STRATA-SYS-04-001, 02-037; ADR-0027)
| Series | Meaning |
|--------|---------|
| `strata_io_max_open_files` / `strata_io_files_open` | segment file descriptor budget and files currently open |
| `strata_io_reads_total` / `strata_io_read_bytes_total` | storage reads and bytes |
| `strata_io_short_reads_total` | short reads whose remainder was resubmitted |
| `strata_io_read_retries_total` / `strata_io_read_errors_total` | failed reads retried once / failed after the retry (the query failed, naming the file) |
| `strata_io_file_opens_total` / `strata_io_file_evictions_total` | opens (including reopens) and files closed to stay within the budget |
| `strata_io_emfile_recoveries_total` | `EMFILE` on open recovered by closing cold files |

A steadily rising eviction rate means the budget is small for the working set
(raise `[storage] max_open_files` or the service's `LimitNOFILE`). Any
`read_errors` means storage returned errors twice in a row: check the device
and `strata doctor`.

## Threads (spec STRATA-SYS-03-014/015)
| Series | Meaning |
|--------|---------|
| `strata_thread_budget` | `2 × logical_cpus + 32` |
| `strata_threads{role}` | registered engine threads: `main`, `net` (`sx-net-NN`), `work` (`sx-work-NN`), `blk` (`sx-blk-NN`), `maint` (`sx-maint`) |
| `strata_thread_cpu_seconds{role}` | CPU time of live threads, as last recorded by each thread (after a task or when parking) |
| `strata_blocking_pool_workers` / `_max_workers` / `_busy` / `_queued` | blocking request pool: started threads (grow on demand), maximum, running and waiting jobs |
| `strata_blocking_pool_rejected_total` | blocking jobs refused because the queue was full (requests got `resource_exhausted`) |

`GET /api/v1/admin/threads` (cluster.read) lists every thread with its role,
name, CPU time and its age, and affinity (`unpinned`). On Linux it also
reports the OS thread count and the number of untracked threads.

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

## In-flight I/O (spec STRATA-SYS-04-008)
| Series | Meaning |
|--------|---------|
| `strata_io_max_inflight_per_device` | the per-device limit |
| `strata_io_inflight` | I/O calls in flight now |
| `strata_io_inflight_peak` | highest in flight on one device since start (at the limit = the bound binds) |
| `strata_io_inflight_waits_total` | calls that waited for a permit |

`strata_wal_commit_wake_seconds` (histogram): the time from a group commit's
durability being published to a covered writer resuming. It grows with the
number of concurrent writers (ADR-0032).

## WAL volume
| Series | Meaning |
|--------|---------|
| `strata_wal_bytes_total` | WAL frame bytes appended (headers + payloads); divide by events for bytes per event (about 70 B with WAL v3 on the app-logs profile, about 230 B with v2) |
| `strata_wal_records_total` | WAL records appended (one per committed batch) |

## Memory (spec VIII.5.2, ADR-0030)
| Series | Meaning |
|--------|---------|
| `strata_memory_tracked_bytes{region}` | bytes reserved now, by region: `control`, `ingest`, `cache`, `background`, `query` |
| `strata_memory_limit_bytes{region}` | each region's limit (`cache` is a target share, not enforced) |
| `strata_memory_peak_bytes{region}` | peak reserved since start |
| `strata_memory_budget_bytes` | process budget (`max_server_memory`) |
| `strata_memory_resident_bytes` | RSS at the last 10 s sample |
| `strata_memory_untracked_bytes` | RSS not covered by tracked memory. Alert when it grows: untracked memory is what gets a process OOM-killed |

## Query log (`system.query_log`)
| Series | Meaning |
|--------|---------|
| `strata_query_log_entries_total` | queries recorded (ok and failed) |
| `strata_query_log_written_total` | entries written to storage |
| `strata_query_log_dropped_total` | entries dropped because the queue was full (alert if it grows) |
| `strata_query_log_write_errors_total` | entries lost to storage write errors, e.g. a full disk |

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

## Plan cache and estimates (ADR-0040)
| Series | Meaning |
|--------|---------|
| `strata_plan_cache_{hits,misses,insertions,evictions}_total` | plan cache lookups and changes |
| `strata_plan_cache_entries`, `strata_plan_cache_bytes` | cached plans and their approximate size |
| `strata_plan_cache_hit_seconds` | histogram of hit lookup latency |
| `strata_plan_cache_fingerprint_hit_ratio{fingerprint}` | hit ratio of the 100 most looked-up query shapes |
| `strata_column_cache_{hits,misses,insertions,evictions}_total`, `strata_column_cache_bytes`, `strata_column_cache_protected_entries`, `strata_column_cache_hit_seconds` | decoded column cache (only when `[storage] column_cache_bytes` > 0): bytes are charged to the cache memory region; protected = used at least twice (SLRU) |
| `strata_query_estimate_q_error` | histogram of max(estimated/actual, actual/estimated) rows per query; high values also come from early stops and metadata answers (work avoided) |

## Snapshots and retained segments (spec VIII.17, ADR-0039)
| Series | Meaning |
|--------|---------|
| `strata_snapshots_open` | engine snapshots currently held (each query holds one while it runs) |
| `strata_snapshot_oldest_age_seconds` | age of the oldest open snapshot: a long-running query keeps its segments alive |
| `strata_obsolete_segments`, `strata_obsolete_retained_bytes` | replaced segments (compaction, retention, upgrade) whose files still exist: pinned by a snapshot or within `old_segments_lifetime` |
| `strata_obsolete_retained_ratio` | retained bytes / disk size; the server warns above 0.10 and `GET /api/v1/admin/snapshots` names the holders |

## Workload classes and admission (spec VIII.11, ADR-0036)
| Series | Meaning |
|--------|---------|
| `strata_workload_slots{class}` | concurrent query slots of the class (batch is adapted by the AIMD controller) |
| `strata_workload_running{class}`, `strata_workload_queued{class}` | queries running / waiting for a slot |
| `strata_workload_queries_total{class,outcome}` | `admitted`, `queue_full`, `overloaded`, `queue_timeout`, `cancelled_queued` |
| `strata_workload_queue_wait_seconds{class}` | histogram of time spent waiting for a slot |
| `strata_workload_demotions_total{class}` | running queries demoted out of the class (ran past 2x its time budget) |
| `strata_workload_cpu_seconds_total{class}` | CPU time of finished queries, by the class they ended in |
| `strata_scan_pool_class_cpu_seconds_total{class}` | scan-pool worker CPU by class (the fair queue's view) |

Per-class arrival rate, mean service time, utilization and queue-wait p99 are
in `GET /api/v1/admin/workload`.

## State machines (spec IX.22, ADR-0035)
| Series | Meaning |
|--------|---------|
| `strata_state_seconds{machine,state}` | histogram of time spent in a state, recorded when the state is left (terminal states are never left) |
| `strata_state_entities{machine,state}` | entities in each state now (queries running, WAL segments sealed, ...) |
| `strata_state_illegal_transitions_total{machine}` | transitions refused because the spec does not allow them; anything non-zero is a bug |

Machines are `query_lifecycle`, `insert_lifecycle`, `wal_segment`,
`merge_task`, `connection` (exported but not yet driven), `table`, `server`
and `device`. The current server state is also in `GET /api/v1/ready`
(`"state"`), which returns 503 while the server is starting or draining.

Examples:
* stuck queries: `strata_state_entities{machine="query_lifecycle",state="RUNNING"}`
  keeps rising, or the p99 of `strata_state_seconds{machine="query_lifecycle",state="QUEUED"}` grows (admission);
* WAL not reclaimed: `strata_state_entities{machine="wal_segment",state="SEALED"}` grows (flushes are behind);
* device: `strata_state_entities{machine="device",state="DEGRADED"} == 1` means
  the fsync p99 has been above 4× its baseline for 30 s.

## Suggested alerts
* `engine_failed == 1` — engine in fail-stop state (restart; see runbooks/recovery.md).
* `rate(write_rejections_total[5m]) > 0` or `disk_write_rejections_total` increasing — backpressure / disk reserve.
* p99 of `wal_sync_seconds` rising — storage latency problem (affects every acknowledgement in `sync`/`group_commit`).
* `rate(queries_total{outcome=~"timeout|rejected"}[5m])` — query budgets too tight or overload.
* `strata_state_entities{machine="server",state="DEGRADED"} == 1` — device degraded or table read-only (disk reserve, engine failure).
* `strata_state_illegal_transitions_total > 0` — a lifecycle bug; report it.
* `rate(strata_workload_queries_total{outcome=~"queue_full|queue_timeout"}[5m]) > 0` — queries refused for lack of slots: raise `[workload]` slots or `max_concurrent_queries`, or find the expensive queries.
* `strata_workload_slots{class="batch"}` below its configured value for a long time — interactive latency is over target under CPU saturation (the controller is shedding batch work).
