# ADR-0042: Decoded column cache (SLRU) and the server endpoints StrataStudio uses

- **Status:** Accepted
- **Spec references:** VIII.16 (STRATA-SYS-16-001, 16-002, 16-004);
  ADR-0040 (sharded cache), ADR-0041 (StrataStudio), ADR-0030 (memory
  regions)

## Context
- **Repeated decode cost:** dashboards repeat the same queries over the
  same segments, and every run reads, decompresses, verifies and decodes
  the same columns. Segments are immutable, so a decoded column never goes
  stale.
- **Studio needs:** StrataStudio (ADR-0041) requested five server changes
  (`docs/studio/STATUS.md`).

## Decision
### Shared column buffers
- `ColumnData`'s `Ints`, `U8s`, `Values` and `Maps` are now `Arc`-backed,
  like `Strs` (ADR-0034), so cloning any decoded column is O(1). This is an
  in-memory change only; no persistent format changes.

### SLRU in `ShardedLru` (STRATA-SYS-16-002)
- **Segments:** entries start *probationary*. A hit promotes an entry to
  *protected* (at most 80% of a shard), and protected overflow is demoted
  back.
- **Eviction:** the least recent probationary entry goes first.
- **Effect:** a large scan of one-time reads only displaces other one-time
  reads. Instead of the spec's "batch queries over 20% of the cache insert
  probationary only" rule, *every* first insert is probationary, which
  gives the same protection without needing the query class at the
  storage layer.
- **Metrics:** `insert` reports the bytes it freed, for exact accounting.

### Decoded column cache (`stratadb_storage::colcache`)
- **Key:** `(segment id, column)` → decoded `ColumnData`. Shredded keys and
  projected map reads are not cached, because they depend on the request.
- **Read path:** on a hit, the segment read path skips read and decode
  (`ColumnBatch.cached_columns`, `QueryStats.column_cache_hits`; no bytes
  read); on a miss it caches what it decoded.
- **Memory:** bytes are charged to the cache memory region before insertion,
  and skipped if the region is full. Evicted and replaced bytes are
  released, and everything is released when the engine drops. The region is
  charged exactly what the cache holds (tested).
- **Opt-in** (spec default 0): `EngineOptions::column_cache_bytes`,
  `[storage] column_cache_bytes`. The server caps it at the memory plan's
  cache region.
- **Exposure:** `strata_column_cache_*` and `GET /api/v1/admin/caches`.

### StrataStudio endpoints
1. **`GET /api/v1/whoami`** (any valid token): tenant, token id,
   permissions and expiry. Never the secret.
2. **`GET /api/v1/schema`** (query): tables, columns, and the caller's
   attribute and resource keys with the rows of segments holding them. They
   come from segment footers (`Snapshot::attribute_key_rows`), never a scan,
   with at most 500 per scope and a truncation flag.
3. **`GET /api/v1/admin/backup`** (backup.create): backups under
   `--backup-dir` from their manifests (`read_backup_manifest`), newest
   first, at most 200.
4. **`page_size` on aggregate and EXPLAIN queries** is ignored and the
   response says `"paginated": false`; a cursor on them is still an error.
5. **`GET /api/v1/queries`** (query): the caller's queued and running
   queries, with state, class, queue position, elapsed time and
   fingerprint.

## Consequences
- Repeated scans of cached columns read 0 bytes. The query-level effect is
  to be measured once the machine is idle (A/B pending).
- **Not done:**
  - TinyLFU admission;
  - per-cache hit latency for the decoded Bloom/trigram filter cache;
  - a result cache (16-003);
  - memtable keys in `/schema`, which counts segments only.
