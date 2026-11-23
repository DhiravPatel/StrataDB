# ADR-0019: Bounded durable idempotency

- **Status:** Accepted
- **Spec references:** §7.3, §2.1

## Decision
- Key scope `(tenant, key)`; key 1–128 visible ASCII; HTTP header `Idempotency-Key`.
- The digest is SHA-256 of the **raw request body** (computed by the server),
  not of encoded events (those contain server ingest times).
- Same key + digest within the window → original receipt, `duplicate: true`,
  nothing written. Same key + other digest → `already_exists` (409, not
  retryable). Key still committing → `unavailable` (503, retryable).
- Durability of dedup state: WAL record type 2 carries key/digest/commit time;
  each flushed segment carries the keys of its batches in optional footer
  extension tag 1 (ignored by v1 readers — forward compatible). On open, the
  index is rebuilt from segment footers + WAL replay, skipping expired keys.
- Bounds: window 24 h, 100 000 keys (defaults). Capacity pressure evicts the
  oldest keys early (`idempotency_evictions_total`). Outside these bounds
  delivery is at-least-once. This is **not** end-to-end exactly-once.
- Compaction (future) must carry extension tag 1 forward for unexpired keys.

## Evidence
Unit tests (index, codecs), `tests/idempotency.rs` (restart, flush, restart
after flush, conflict, expiry, eviction, 8-way concurrent same key), crash
matrix now retries every batch after each of 15 crash points and requires
exactly one copy, HTTP test `idempotent_ingest_over_http`.
