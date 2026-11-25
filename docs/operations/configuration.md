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

| `[quotas] default_ingest_events_per_sec`, `default_ingest_burst_events`, `default_storage_bytes` | — | unlimited | per-tenant limits |
| `[quotas.tenants.<id>] ingest_events_per_sec`, `ingest_burst_events`, `storage_bytes` | — | inherit defaults | field-wise override |
| log level | `STRATADB_LOG` | `info` | tracing filter syntax, e.g. `info,access=warn` |

Unknown keys are errors. All settings require a restart. The file holds no
secrets (tokens live in `<data_dir>/auth/tokens.json`, TLS keys are referenced by path).
