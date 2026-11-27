# Configuration

`strata-server serve --config strata.toml` (or `STRATADB_CONFIG`). Precedence:
**flag > environment variable > file > default**. Validate without starting:
`strata-server config --config strata.toml` prints the effective settings
(no secrets) or exits non-zero with the first error.

| Key | Env | Default | Notes |
|-----|-----|---------|-------|
| `config_version` | — | required (`1`) | unknown versions rejected |
| `data_dir` | `STRATADB_DATA_DIR` | required | owner-only permissions |
| `listen` | `STRATADB_LISTEN` | `127.0.0.1:7070` | non-loopback requires `[tls]` |
| `durability` | `STRATADB_DURABILITY` | `group_commit` | `async` \| `sync` \| `group_commit` (ADR-0009) |
| `[tls] cert`, `key` | `STRATADB_TLS_CERT`, `STRATADB_TLS_KEY` | none | PEM; both or neither |
| `[limits] max_body_bytes` | flag only | 8 MiB | |
| `[limits] max_decompressed_bytes` | — | 32 MiB | gzip bomb protection |
| `[limits] max_concurrent_ingest` / `max_concurrent_queries` | — | 32 / 16 | excess → 429 |
| `[limits] scan_threads` | — | logical CPUs | threads of the shared scan worker pool (1–256), fixed at startup; queries scan on their own thread plus pool tasks, never on threads of their own |
| `[storage] group_commit_max_delay_ms` | — | 2 | upper bound (0–100 ms) of the group-commit wait (durability `group_commit`) |
| `[storage] group_commit_adaptive` | — | true | wait only when more writers are expected: no wait at low arrival rate; otherwise up to min(fsync latency, max delay), stopping when the group is complete (spec STRATA-SYS-07-004). `false` always waits the full delay |
| `[storage] wal_format_version` | — | 3 | WAL format written: 3 (columnar, LZ4-compressed batch payloads; spec STRATA-SYS-07-002) or 2 (row payloads, readable by the previous release; use it for a rollback window, runbooks/upgrade.md) |
| `[storage] io_max_inflight_per_device` | — | 128 | I/O calls (reads, writes, fsyncs) in flight per device (1–4096; spec STRATA-SYS-04-008). Callers wait for a permit beyond it |
| `[storage] max_open_files` | — | 50% of the soft `RLIMIT_NOFILE` | segment files kept open (LRU; at least 16); cold files are closed and reopened on demand. The server first raises the soft limit to the hard limit |
| `[limits] net_threads` | — | logical CPUs | async network runtime threads (`sx-net-NN`) |
| `[limits] blocking_threads` | — | rest of the budget, at most ingest + query permits | maximum `sx-blk-NN` threads running commits, queries and admin operations (started on demand); at least 2 |

**Thread budget (spec STRATA-SYS-03-014).** `net + scan + blocking + 6` (main,
`sx-maint-00`, the query log's `sx-maint-01` and writer `sx-maint-02`, 2 reserved
tokio blocking threads) must not exceed
`2 × logical_cpus + 32`. The server refuses to start otherwise; the effective
plan is printed at startup and by `strata-server config`, and live
threads are listed at `GET /api/v1/admin/threads`.
| `[limits] max_query_timeout_ms` / `max_query_rows` | — | 60 000 / 100 000 | per-request caps |
| `[storage] memtable_max_bytes` | — | 64 MiB | flush threshold |
| `[storage] wal_segment_max_bytes` | — | 64 MiB | |
| `[storage] disk_reserve_bytes` / `disk_reserve_ratio` | — | 512 MiB / 0.02 | writes pause below the reserve |
| `[storage] default_retention_days` | flag | keep forever | per-tenant policies override |
| `[storage] bloom_attribute_keys` | — | request_id, user_id, session_id, correlation_id | new segments; `strata indexes rebuild` applies it to existing ones |
| `[storage] ngram_index` | — | true | trigram filters on `body` prune `LIKE`/`ILIKE` scans; new segments only |
| `[storage] ngram_attribute_keys` | — | none | string attributes indexed the same way; new segments only |
| `[encryption] key_file` | `STRATADB_KEY_FILE`, `--key-file` | none | enables encryption at rest (ADR-0024, runbooks/encryption.md); restart required |
| `[backup] dir` | `STRATADB_BACKUP_DIR` | none | enables HTTP backups |
| `[security] insecure_allow_remote_without_tls` | flag | false | dev only |
| `[memory] max_server_memory_bytes` / `max_server_memory_ratio` | — | 0.80 of the cgroup limit, or of physical memory | process memory budget; the bytes setting (at least 256 MiB) overrides the ratio (0.1–0.95) |
| `[memory] ingest_memory_ratio` | — | 0.10 | memtables. A full region stalls writes until a flush frees memory, then refuses them, retryably and before the WAL append. `memtable_max_bytes` is capped at a quarter of it |
| `[memory] cache_memory_ratio` | — | 0.30 | decoded index filters (tracked; not evictable yet) |
| `[memory] merge_memory_ratio` | — | 0.10 | compaction and retention rewrites; compaction is postponed when it is full |
| `[memory] max_query_memory_bytes` | — | min(10 GiB, query pool) | per-query limit (at least 64 MiB). The query pool is what remains after 2% control and the three shares above, and must be at least 10% |
| `[memory] memory_wait_timeout_ms` | — | 0 | how long a query waits for memory before failing (0 = fail fast; at most 600 000) |
| `[memory] untracked_warn_ratio` | — | 0.10 | warn when resident memory exceeds tracked memory by this share of RSS (64 MiB floor) or by 4 GiB |
| `[quotas] default_query_memory_bytes`, `[quotas.tenants.<id>] query_memory_bytes` | — | unlimited within the pool | memory all of a tenant's running queries may hold |
| `[observability] query_log` | — | true | record every query in `system.query_log` (stored under `<data_dir>/system/query_log`, tenant-scoped; spec `obs.query_log`) |
| `[observability] query_log_flush_ms` | — | 7500 | buffering before entries are written (10–600 000); a crash loses at most this interval's entries |
| `[observability] query_log_retention_days` | — | 7 | entries older than this are deleted (1–3650) |
| `[observability] query_log_queue` | — | 10 000 | entries buffered at most (1–1 000 000); more are dropped and counted |

| `[quotas] default_ingest_events_per_sec`, `default_ingest_burst_events`, `default_storage_bytes` | — | unlimited | per-tenant limits |
| `[quotas.tenants.<id>] ingest_events_per_sec`, `ingest_burst_events`, `storage_bytes` | — | inherit defaults | field-wise override |
| log level | `STRATADB_LOG` | `info` | tracing filter syntax, e.g. `info,access=warn` |

Unknown keys are errors. All settings require a restart. The file holds no
secrets (tokens live in `<data_dir>/auth/tokens.json`, TLS keys are referenced by path).
