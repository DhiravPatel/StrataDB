//! Decoded column cache (spec VIII.16 "uncompressed block cache",
//! STRATA-SYS-16-001/002/004): whole decoded segment columns, keyed by
//! `(segment id, column)`.
//!
//! - **Opt-in.** The default size is 0, as the spec's default for this
//!   cache. Set `EngineOptions::column_cache_bytes`
//!   (`[storage] column_cache_bytes`) to enable it.
//! - **Cheap hits.** Segments are immutable, so entries never go stale, and
//!   columns are shared buffers: a hit is an O(1) clone instead of a read,
//!   decompression, checksum and decode.
//! - **Scan resistance.** A [`ShardedLru`] in SLRU mode: a large scan's
//!   one-time reads stay probationary and cannot evict columns that
//!   queries keep using.
//! - **Accounting.** Bytes held are charged to the engine's cache memory
//!   region before an entry is added (skipped when the region is full) and
//!   released when evicted or when the cache drops.
//! - **Exclusions.** Shredded attribute keys and projected map reads are not
//!   cached; they depend on the request.

use std::sync::Arc;

use stratadb_common::cache::{CacheStats, ShardedLru};
use stratadb_common::memory::MemoryTracker;
use stratadb_schema::column::{ColumnData, ColumnId};

/// Average entry size assumed for the entry-count bound (bytes are the real
/// bound).
const ASSUMED_ENTRY_BYTES: usize = 16 * 1024;

#[derive(Debug)]
pub struct ColumnCache {
    lru: ShardedLru<(u64, ColumnId), ColumnData>,
    tracker: Option<Arc<MemoryTracker>>,
}

impl ColumnCache {
    pub fn new(bytes: usize, tracker: Option<Arc<MemoryTracker>>) -> Self {
        Self {
            lru: ShardedLru::new((bytes / ASSUMED_ENTRY_BYTES).max(64), bytes),
            tracker,
        }
    }

    pub fn get(&self, segment: u64, column: ColumnId) -> Option<ColumnData> {
        self.lru.get(&(segment, column))
    }

    /// Cache a freshly decoded column (when it fits the budget and the
    /// memory region). The region is charged exactly the bytes cached:
    /// reserve first, then release what the insert evicted or replaced (or
    /// everything, if it was not cached).
    pub fn put(&self, segment: u64, column: ColumnId, data: &ColumnData) {
        let bytes = data.approx_bytes();
        if let Some(t) = &self.tracker {
            if t.try_reserve(bytes as u64).is_err() {
                return; // the cache region is full: do not cache
            }
        }
        let freed = self
            .lru
            .insert((segment, column), data.clone(), bytes)
            .unwrap_or(bytes);
        if let (Some(t), true) = (&self.tracker, freed > 0) {
            t.release(freed as u64);
        }
    }

    pub fn stats(&self) -> CacheStats {
        self.lru.stats()
    }
}

impl Drop for ColumnCache {
    fn drop(&mut self) {
        if let Some(t) = &self.tracker {
            t.release(self.lru.stats().bytes as u64);
        }
    }
}
