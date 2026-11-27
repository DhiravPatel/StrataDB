# ADR-0027: Storage I/O engine and segment file cache

- **Status:** Accepted (read and write paths)
- **Spec references:** VIII.4 (STRATA-SYS-04-001/002/009/025), VIII.2
  (STRATA-SYS-02-036/037), VIII.24 (fault catalog); ADR-0026 (phase plan)

## Context
Every committed segment kept one open file descriptor for the life of the
process, so a large data set needed one descriptor per segment and failed
with `EMFILE` beyond the limit. Reads called `FileExt::read_exact_at`
directly in several crates, and fault tests could only inject failures at
named failpoints, not short reads or `EIO` under real queries. The spec
requires:
- one I/O interface with submit/complete semantics;
- a fault-injecting simulation backend;
- a descriptor budget with an LRU of open files.

## Decision
- **`stratadb_common::io::IoEngine`:** `open` (captures path, device, inode
  and length), `submit` and `complete` for batches of reads, and `create`
  for writes (`WriteFile::write_at` and `sync`).
- **Rules every backend shares:** checked by one conformance suite.
  - Short reads and writes are resubmitted; a short read is end of file
    only at the known length.
  - A failed read is retried once, then fails with the file, offset and
    length named. Partial data is never returned.
  - A reopened path whose inode changed is an error, never a silent read of
    another file.
- **Backends:**
  - `PreadEngine`, the portable `pread`/`pwrite` backend (the spec's
    `threadpool` row). It completes batches on the calling thread, since the
    engine is synchronous (ADR-0017). Open files live in an LRU of
    `max_open_files` entries (default 50% of the soft `RLIMIT_NOFILE`). On
    `EMFILE` or `ENFILE` it closes half the cache and retries the open.
    Evicted files are reopened on demand with an identity check.
  - `SimIo`, an in-memory backend with scripted faults: short reads and
    writes, read and fsync `EIO`, `ENOSPC`, latency spikes. A crash model
    keeps durable data plus a random, reordered, possibly sector-torn subset
    of unsynced writes. After an fsync `EIO` the dirty data is gone: it is
    not made durable by a later successful fsync, matching Linux since 4.13.
  - `FaultyIo`, a decorator injecting read faults into any backend, so upper
    layers can be tested while their writes still go to disk.
- **Wiring:**
  - Segment readers (plain and sealed), the manifest load, backup
    copy/verify and `BACKUP` reads go through the engine (`EngineOptions::io`,
    default the process-wide `PreadEngine`).
  - Sealed containers read their raw bytes through `RandomRead`.
- **Enforcement:** a per-crate `clippy.toml` in the segment, storage and
  query crates bans `std::fs::read`, `read_to_string`, `File::open`,
  `FileExt::read_at` and `read_exact_at`. Only test files that corrupt data
  on purpose are exempt, each with an explicit `allow`.
- **Startup:** `strata-server` and `strata` raise the soft `RLIMIT_NOFILE`
  to the hard limit, capped at 1,048,576 (STRATA-SYS-02-036). `[storage]
  max_open_files` overrides the cache size.
- **Visibility:** `strata_io_*` metrics: files open, budget, reads, bytes,
  short reads, retries, errors, opens, evictions, `EMFILE` recoveries.
  `strata doctor` judges the descriptor limit against the cache: a low limit
  causes reopen churn, not failure.

## Consequences
- The number of segments is no longer bounded by the descriptor limit.
- Read faults are testable under real queries, and a persistent read error
  fails the query naming the segment file.
- In 3 foreground A/B pairs the overhead was not measurable (report
  2026-11-26-io-engine).
- **Not yet:** an io_uring backend, and a bound on in-flight I/O per worker
  or device (STRATA-SYS-04-008). Writes were routed in the 2026-11-26
  update.

## Update 2026-11-26: write path
- **What moved onto the engine:**
  - WAL appends, fsyncs, segment creation, rotation, torn-tail truncation
    and replay reads;
  - segment writes (flush, compaction, retention, upgrade);
  - manifest commits (`io::atomic_write`);
  - backup copies and verification reads.
- **Write interface:** `WriteFile` (`write_at`, `sync(data_only)`,
  `set_len`, `len`; `&self`, so the sync leader fsyncs while appenders
  write), `IoEngine::create_new` (exclusive, mode 0600) and `open_write`.
- **The WAL:** it writes at its tracked length under its lock, which is
  equivalent to `O_APPEND` with a single writer.
- **Instrumentation preserved:** the real backend creates through
  `fsutil::create_new_file` and syncs through `fsutil::sync_file`, so the
  `simfs` power-loss recorder sees every create and fsync. The failpoints
  stay where they were.
- **Namespace operations:** rename, directory fsync and unlink stay in
  `fsutil`. They are metadata, modelled by the recorder, and the durable
  publish sequence lives there.
- **One-shot reads:** whole-file reads (manifests, WAL replay, backup hashes)
  `forget` their cached descriptor, so a later unlink frees the space at
  once.
- **`FaultyIo`:** now injects `ShortWrite`, `Enospc` and `FsyncEio`, matched
  by a string suffix of the path.
- **Lint:** extended to writes (`fs::write`, `File::create`,
  `OpenOptions::open`, `FileExt::write_at` and `write_all_at`) and to the WAL
  crate.
- **Evidence:** crash matrix and power-loss simulation unchanged (0
  violations in 3,748 states), new `tests/io_write_faults.rs`, no measurable
  overhead (report `2026-11-26-io-write-path`).

## Alternatives rejected
- **Keep one descriptor per segment, raise the limit:** still fails at
  scale and is not portable to restricted containers.
- **io_uring first:** Linux-only and needs `unsafe` or a dependency with
  internal `unsafe` (ADR-0026). It will be a further backend behind the same
  trait.
