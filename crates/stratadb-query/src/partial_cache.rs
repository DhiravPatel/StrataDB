//! Per-segment partial aggregate cache (spec STRATA-ARCH-09-002/003,
//! ADR-0045).
//!
//! An aggregate query computes one partial (groups and aggregate states)
//! per scanned segment, then merges them in segment order. Segments are
//! immutable, so a partial computed once can be reused by any later run of
//! the same query shape over the same segment. A repeated dashboard then
//! recomputes only the memtable, new segments and segments on the edge of
//! its time window: O(new data), not O(data).
//!
//! **Key.** `(query part, time part, segment identity)`.
//! - **Query part:** the tenant, table, `GROUP BY`, aggregates, and the
//!   filter conjuncts other than `event_time` bounds. A canonical text,
//!   kept in the entry and compared on every hit, so a hash collision can
//!   never return another query's state.
//! - **Time part:** empty when the segment's time range lies inside the
//!   filter's `event_time` bounds (every time conjunct is then true for
//!   every row, so a moving "last 24 h" window keeps hitting), otherwise
//!   the exact bounds.
//! - **Segment identity:** [`stratadb_storage::scan::ScanSource::identity`].
//!   It changes whenever a segment is rewritten. Entries are invalidated by
//!   identity only, never by time (09-003); entries of merged-away segments
//!   are never hit again and age out of the LRU.
//!
//! **Exactness.** A hit merges the same states, in the same segment order,
//! as the scan would have produced, so results (floating-point sums and
//! group order included) are identical. This is tested with the cache on
//! and off.
//!
//! **Bounds.**
//! - Byte-bounded SLRU ([`ShardedLru`]).
//! - Entries over 1/16 of the capacity are not cached.
//! - Bytes are charged to an optional memory region before insertion, and
//!   skipped when the region is full.

use std::sync::Arc;

use stratadb_common::cache::{CacheStats, ShardedLru};
use stratadb_common::memory::MemoryTracker;

use crate::exec::AggState;
use crate::groups::FrozenGroups;

/// Average entry size assumed for the entry-count bound (bytes are the real
/// bound).
const ASSUMED_ENTRY_BYTES: usize = 8 * 1024;

/// Bytes accounted per cached aggregate state (inline size plus a min/max
/// value's typical heap).
fn state_bytes() -> usize {
    std::mem::size_of::<AggState>() + 16
}

/// Time part of a key: `None` when the segment is covered by the time
/// bounds, else the exact inclusive bounds.
pub(crate) type TimePart = Option<(Option<i64>, Option<i64>)>;

/// One segment's partial result for one query shape.
#[derive(Debug)]
pub(crate) struct CachedPartial {
    key: Arc<str>,
    time: TimePart,
    pub(crate) groups: FrozenGroups<AggState>,
    /// Rows the scan read, reported as avoided on a hit.
    pub(crate) rows_scanned: u64,
    pub(crate) bytes: usize,
}

/// Shared by every query of a process (or a server); see the module docs.
#[derive(Debug)]
pub struct PartialCache {
    lru: ShardedLru<(u64, u64), Arc<CachedPartial>>,
    tracker: Option<Arc<MemoryTracker>>,
    max_entry_bytes: usize,
}

impl PartialCache {
    /// A cache of at most `bytes`, charging `tracker` (when given) for what
    /// it holds.
    pub fn new(bytes: usize, tracker: Option<Arc<MemoryTracker>>) -> Self {
        Self {
            lru: ShardedLru::new((bytes / ASSUMED_ENTRY_BYTES).max(64), bytes),
            tracker,
            max_entry_bytes: bytes / 16,
        }
    }

    pub fn stats(&self) -> CacheStats {
        self.lru.stats()
    }

    fn slot(key: &str, time: TimePart) -> u64 {
        use std::hash::{Hash, Hasher};
        // Fixed keys: the same query shape hashes alike across queries.
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        time.hash(&mut h);
        h.finish()
    }

    /// The cached partial of `(key, time)` for segment `uid`, verified
    /// against the full key.
    pub(crate) fn get(&self, key: &str, time: TimePart, uid: u64) -> Option<Arc<CachedPartial>> {
        let hit = self.lru.get(&(Self::slot(key, time), uid))?;
        (*hit.key == *key && hit.time == time).then_some(hit)
    }

    /// Cache a freshly computed partial (when it fits the per-entry bound
    /// and the memory region).
    pub(crate) fn put(
        &self,
        key: &Arc<str>,
        time: TimePart,
        uid: u64,
        groups: FrozenGroups<AggState>,
        rows_scanned: u64,
    ) {
        let bytes = groups.heap_bytes(state_bytes()) + key.len() + 64;
        if bytes > self.max_entry_bytes {
            return;
        }
        if let Some(t) = &self.tracker {
            if t.try_reserve(bytes as u64).is_err() {
                return; // the region is full: do not cache
            }
        }
        let entry = Arc::new(CachedPartial {
            key: Arc::clone(key),
            time,
            groups,
            rows_scanned,
            bytes,
        });
        let freed = self
            .lru
            .insert((Self::slot(key, time), uid), entry, bytes)
            .unwrap_or(bytes);
        if let (Some(t), true) = (&self.tracker, freed > 0) {
            t.release(freed as u64);
        }
    }
}

impl Drop for PartialCache {
    fn drop(&mut self) {
        if let Some(t) = &self.tracker {
            t.release(self.lru.stats().bytes as u64);
        }
    }
}
