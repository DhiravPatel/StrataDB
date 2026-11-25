# ADR-0023: Stable keyset pagination for row queries

- **Status:** Accepted
- **Spec references:** §2.3 ("Pagination with stable semantics"), §9.4, §13

## Context
Log queries often return more rows than one response should carry. OFFSET
pagination is unstable under concurrent ingest (rows shift between pages, so
they repeat or are skipped) and costs O(offset) per page. Server-side cursors
holding a snapshot would pin segment files and memtables for an unbounded time
per client.

## Decision
Stateless **keyset** pagination with an opaque cursor.

- **Row identity.** Every row has a per-tenant unique, immutable identity
  `(seq, idx)`: the WAL sequence number of its batch and its index within the
  batch. Flush, compaction, retention rewrites, `strata upgrade` and
  backup/restore preserve it (segments store `_seq`/`_idx` columns).
- **Total order.** Rows are ordered by the query's `ORDER BY` keys (with the
  existing `sort_cmp` total order over values, nulls and missing), then by
  `(seq, idx)` ascending. Without `ORDER BY` this is commit order.
- **Cursor.** The last returned row's order-key values and identity, plus
  `as_of_seq`, `returned`, and a fingerprint = CRC-32C(tenant, trimmed query
  text). Page *n+1* returns the first `page_size` rows strictly after the
  cursor. The executor keeps a top-k of `page_size + 1` rows (bounded memory);
  the extra row decides whether `next_cursor` is issued, so there is never a
  trailing empty page.
- **Snapshot bound.** `as_of_seq` = `Snapshot::max_visible_seq()` of the first
  page. Later pages drop rows with `seq > as_of_seq`. Because a write is
  inserted into the memtable before it is acknowledged, every write
  acknowledged before the first page has `seq <= as_of_seq`.
- **LIMIT** bounds the total across pages (`returned` in the cursor).
- **Pruning.** Segments with `min_seq > as_of_seq` are skipped; without
  `ORDER BY`, so are segments with `max_seq` below the cursor's `seq`; when the
  first key is `event_time`, segments wholly on the already-returned side of
  the cursor's timestamp are skipped (`segments_pruned_cursor` statistic).
- **Not paginated:** aggregate queries (their result is bounded by
  `max_rows` and recomputed per request, so cross-page consistency could not
  be guaranteed) and EXPLAIN. Both → `invalid_input`.

## Guarantees (tested in `crates/stratadb-query/tests/pagination.rs`)
1. No row is returned twice across the pages of one walk.
2. Every row acknowledged before the first page and not removed by retention
   before it would be read is returned exactly once.
3. No row committed after the first page completes appears.
4. Rows whose commit was *concurrent* with the first page may or may not
   appear (at most once).
5. Concatenated pages are in the query's order.

## Cursor format
v1, base64url (no padding) of: `version u8 | fingerprint u32 | as_of_seq
uvarint | returned uvarint | nkeys uvarint | (present u8 [value])* | seq
uvarint | idx u32 | crc32c u32` (values use the event codec encoding). Cursors
are bounded (64 KiB), checksummed and strictly decoded; any defect →
`invalid_input`. They are positional only and grant no access (the tenant is
still derived from the credential), so they are not signed. They do not
expire.

Compatibility: a release must decode every cursor version it ever issued, or
reject it cleanly with `invalid_input` (clients restart from the first page).
The format is documented here and in `docs/project/COMPATIBILITY.md`.

## Consequences
- Each page re-scans the candidate segments (minus pruning); deep pages of an
  unselective `ORDER BY service` query cost about as much as the first. Pages
  ordered by `event_time` or commit order get cheaper as the cursor advances.
- Changing the query text invalidates its cursors (by design).
- Retention may remove rows between pages; they are then simply absent.
