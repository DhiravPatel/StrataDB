# ADR-0005: Index architecture

- **Status:** Accepted for Bloom filters (F-003); exact-match postings and inverted indexes still Proposed
- **Spec references:** §12

## Proposal
- All indexes are **segment-local, derived, rebuildable** structures stored as
  optional footer extensions or sidecar files referenced by the manifest.
- Planned order: (1) footer statistics + key presence (done); (2) bloom
  filters for selected high-cardinality fields (trace_id, request_id);
  (3) sorted postings / roaring bitmaps for exact-match fields; (4) token
  inverted index for `body` with an explicit tokenizer version.
- No global per-value metadata. Index creation is manual in 0.x; the engine
  records predicate statistics and *recommends* indexes (spec §12.7).
- Every index path has a scan fallback and an equivalence test (index result == scan result).

## Revisit
After the Phase 2 query engine exists and the benchmark dataset generator is in place.

## Implemented (2026-11-21): Bloom filters
Segment footer extension tag 2; string values only; trace_id, span_id,
event_id + configurable attribute keys (default request_id, user_id,
session_id, correlation_id). Lazily decoded per segment handle; undecodable
filters are ignored (no pruning) rather than failing queries. Planner uses
top-level string `=` / `IN` conjuncts only. Evidence: no-false-negative test,
optimizer-equivalence property test with id predicates, A/B report
`benchmarks/reports/2026-11-21-bloom-ab.md` (exact-id lookups 2.6–2.8× faster,
+1 % storage).

## Implemented (2026-11-25): trigram filters for substring search (F-005)
A token (word) inverted index cannot prune `LIKE '%x%'` soundly (`'%TIMEOUT%'`
must match inside `TIMEOUTS`), and StrataQL exposes substring semantics, not
token search. Instead each segment stores, per indexed field (`body` by
default; `storage.ngram_attribute_keys` optional), a Bloom filter of every
3-character window of its string values, characters folded with the `ILIKE`
rule so one index serves `LIKE` and `ILIKE`. A top-level `LIKE`/`ILIKE`
conjunct contributes every folded trigram of every literal run of ≥ 3
characters; a segment lacking any of them is pruned (`ngram(body)` in
`indexes_used`, `segments_pruned_ngram`). Footer extension tag 3 (optional).
Evidence: no-false-negative property test against an independent LIKE
reference (segment crate), engine equivalence property test with `LIKE`/`ILIKE`
predicates, pruning test, mutation checks (2 injected bugs caught), A/B report
`benchmarks/reports/2026-11-25-ngram-ab.md`: absent substring 3.0 ms → 3 µs
(all segments pruned); common substrings unchanged; +0.17 % segment bytes;
final flush +11 %. Limits: no pruning for literal runs < 3 characters, for
`NOT LIKE`, or under `OR`; no phrase/token/prefix operators (spec §14.1 modes
other than substring and equality are not exposed).

## Decision (2026-11-25): exact-match postings (F-004) deferred
Segments are read as whole column chunks (no row groups), so a postings list
inside a segment would only skip per-row filter evaluation (vectorized, µs per
segment), not I/O or decode. Exact-id lookups already read one of three
segments thanks to Bloom pruning (queries 01/02: ~1.5 ms). Revisit when
segments gain row groups / page-level reads, or when profiles show filter
evaluation dominating id lookups.

## Implemented (2026-11-25): index lifecycle (F-006..F-008)
Filters are segment footer extensions, so their "manifest" is the segment
footer and they are published atomically with the segment. `Engine::indexes`
lists them; with verification it reads each segment's rows and checks
(1) **no false negatives** — every indexed value / trigram is in its filter
(a miss would make queries skip matching rows), and (2) whether the stored
filters are byte-identical to what the current settings build (filter
construction is deterministic), i.e. current vs stale. `rebuild_indexes`
rewrites stale or unsound segments through the compaction `rewrite` path
(crash-safe, covered by the crash matrix). CLI: `strata indexes
list|verify|rebuild [--dry-run]`; `verify` exits non-zero on false negatives.
Changing `bloom_attribute_keys` or n-gram settings applies to new segments;
run `strata indexes rebuild` to apply it to existing ones.
