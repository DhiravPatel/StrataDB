# ADR-0020: Compaction and retention

- **Status:** Accepted
- **Spec references:** §10.4, §16

## Decision
One primitive, `rewrite(inputs, keep)`, serialized with flushes by the
manifest lock: build ≤ 1 output segment (verified by read-back) → one manifest
commit removes inputs, adds output (with `lineage`), records inputs in
`obsolete_segments` → atomic visible-set swap → delete obsolete files once no
snapshot holds them (`Arc` refcount); recovery deletes recorded obsolete
files. Idempotency footer extensions (tag 1) are carried forward for
unexpired keys.

- **Compaction policy:** per tenant, segments < 16 MiB are candidates; when ≥ 4
  exist, merge the oldest (by min_seq) up to 32 inputs / 64 MiB. One group per
  tenant per pass; the background thread runs a pass after each flush check,
  so flushes keep priority and a pass is bounded.
- **Retention:** per-tenant policy `{max_age_ms, hold}` in the manifest (plus an
  optional server-wide default). Every `retention_interval` (60 s): segments
  entirely older than the cutoff are dropped; straddling segments are
  rewritten without expired rows. `hold` suspends deletion (legal hold).
  Semantics: rows older than the cutoff become *eligible*; they disappear from
  queries and active files at the next enforcement run; rows still in the
  memtable are handled after their flush; backups made earlier still contain
  them (until those backups are deleted by the operator). Deleting rows may
  also forget the idempotency keys of the batches they belonged to.

## Evidence
`tests/maintenance.rs` (merge equivalence property test, snapshot pinning,
recovery of obsolete deletions, idempotency carried through compaction,
retention drop/rewrite/hold/durability); crash matrix cases
`compaction::after_output_write`, `after_manifest_commit`, `after_delete_obsolete`.
