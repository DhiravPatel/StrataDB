# ADR-0039: Snapshot catalog: published read view, per-tenant segment lists, snapshot registry

- **Status:** Accepted
- **Spec references:** spec VIII.17 (STRATA-SYS-17-001..005); ADR-0033
  (columnar memtable), the compaction/retention ADRs (obsolete segments)

## Context
- **Snapshot cost:** `Engine::snapshot` took the engine-wide state mutex,
  walked *every* committed segment to keep the tenant's, and cloned an
  `Arc` per segment. That is O(total segments) under the lock writers also
  take. Measured at 5.8 µs with 1,500 segments, linear (about 0.4 ms at
  100k); the spec asks for 20 µs at 100k.
- **Publish cost:** every flush and compaction rebuilt the global segment
  vector.
- **Deletion timing:** obsolete files were deleted as soon as no snapshot
  referenced them; the spec asks for an `old_parts_lifetime` delay.
- **Visibility:** nothing showed which queries held old segments alive.

## Decision
### Published read view (STRATA-SYS-17-001)
- **The view:** `ReadView { active, frozen, by_tenant }` is one immutable
  object in an `arc_swap::ArcSwap`.
  - It is republished, under the state lock so publishes are ordered,
    after every change to segments or memtables: freeze, flush commit, and
    compaction/retention/upgrade rewrites.
  - Segments and memtables come from the same view, so a row is never in
    both.
- **A snapshot** does one atomic load, clones its tenant's segment list
  `Arc` (O(1)), and builds memtable views (O(chunks)). It never takes the
  state lock.
- **The dependency:** `arc-swap` (1.x, no dependencies) is the one new
  crate, and is needed for a lock-free pointer swap without `unsafe`.

### Per-tenant segment lists (STRATA-SYS-17-002)
- **Source of truth:** `State.by_tenant` maps each tenant to its segment
  list, sorted by id.
- **Copy-on-write:** a publish changes only the tenants it touches, with
  `Arc::make_mut`, which copies a list only while a snapshot still holds
  it.
- **Global list:** derived on demand (`all_segments`) for admin and
  maintenance paths, which are O(segments) anyway.
- **Quotas:** `tenant_usage` now reads one tenant's list instead of every
  segment.
- **Pinning is unchanged:** a snapshot holds its list, and so its handles.
  Reclamation still deletes a file only when the obsolete list is the last
  holder of its handle.

### Old segment lifetime (STRATA-SYS-17-004)
- **Timestamps:** obsolete entries record when they left the visible set.
- **Deletion rule:** a file is deleted only when it is unreferenced *and*
  `old_segments_lifetime` has passed.
- **Configuration:** the default is 8 minutes (spec), set with
  `[storage] old_segments_lifetime_s`. Tests of reclamation use 0.
- **Recovery:** at open, the manifest's obsolete list is still deleted at
  once (no reader can exist).

### Snapshot registry (STRATA-SYS-17-005)
- **Registration:** every snapshot registers a `Pin` (id, tenant, creation
  time, holder label, its segment list) in a 16-shard registry of weak
  references. The pin removes itself when the snapshot and all its clones
  drop.
- **Holders:** the server labels query snapshots `query:<id>`.
- **`Engine::snapshots()`:** reports open snapshots and the obsolete
  segments and bytes each pins, plus totals and the share of the disk.
- **Exposure:**
  - `GET /api/v1/admin/snapshots` (cluster.read), with the main and query
    log engines;
  - metrics `strata_snapshots_open`, `strata_snapshot_oldest_age_seconds`,
    `strata_obsolete_segments`, `strata_obsolete_retained_bytes` and
    `strata_obsolete_retained_ratio`.
- **Warning:** the 10 s health tick logs at most one warning per 5 minutes,
  naming the oldest pinning holders, when retained bytes exceed 10% of the
  disk.

## Consequences
- **Snapshot cost:** 0.2–0.3 µs at 10, 300 and 1,500 segments, against
  0.2 / 1.75 / 5.8 µs before (`tests/snapshots.rs snapshot_cost`, release;
  measured on battery, so absolute values are high but the scaling is
  clear).
- **Publish cost:** O(segments of the changed tenant) plus O(tenants) for
  the view's map; O(1) for a memtable freeze.
- **A deliberate cost on every snapshot:** one shard lock to register and
  one to unregister. This is not the catalog lock, and it is what makes
  holders visible.
- **Not done:**
  - **Persistent maps for very large single-tenant lists:** at 100k
    segments in *one* tenant a flush copies that list while a snapshot
    holds it.
  - **A measured publish rate (≥ 2,000/s):** not measured in isolation.
  - **`system.snapshots` as a SQL table:** it is an endpoint and metrics.
