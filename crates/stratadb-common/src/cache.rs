//! A sharded, bounded LRU cache (spec VIII.16, STRATA-SYS-16-001/004).
//!
//! - **Shards.** At least 64 shards, chosen by key hash, each behind its own
//!   lock. A lookup touches one shard: no global lock, and its counters live
//!   in that shard.
//! - **Bounds.** Both an entry count and a byte budget (the caller states
//!   each entry's size), split evenly across shards. Inserting evicts
//!   least-recently-used entries of the shard until both fit. An entry
//!   larger than a shard's byte budget is not cached.
//! - **Metrics** ([`CacheStats`]): hits, misses, insertions, evictions,
//!   bytes, entries, and a histogram of hit latency.
//!
//! - **Scan resistance (SLRU, STRATA-SYS-16-002).** New entries are
//!   *probationary*; a hit promotes an entry to the *protected* segment (at
//!   most 80% of a shard's entries; overflow is demoted back). Eviction
//!   takes the least recent probationary entry first, so one large scan of
//!   one-time reads only evicts other one-time reads, never the working set.
//!
//! Recency is a per-shard logical clock: a hit stamps the entry, and
//! eviction or demotion takes the oldest stamp of its segment (O(shard
//! entries), which bounded shards keep small).

use std::collections::HashMap;
use std::hash::{BuildHasher, Hash};
use std::sync::Mutex;
use std::time::Instant;

use crate::metrics::{Histogram, HistogramSnapshot};

/// Default and minimum number of shards.
pub const SHARDS: usize = 64;

#[derive(Debug)]
struct Entry<V> {
    value: V,
    bytes: usize,
    stamp: u64,
    protected: bool,
}

#[derive(Debug)]
struct Shard<K, V> {
    map: HashMap<K, Entry<V>>,
    clock: u64,
    bytes: usize,
    protected: usize,
    hits: u64,
    misses: u64,
    insertions: u64,
    evictions: u64,
}

impl<K, V> Default for Shard<K, V> {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            clock: 0,
            bytes: 0,
            protected: 0,
            hits: 0,
            misses: 0,
            insertions: 0,
            evictions: 0,
        }
    }
}

/// Counters of a cache (sum over shards).
#[derive(Debug, Clone, PartialEq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub entries: usize,
    pub bytes: usize,
    pub capacity_entries: usize,
    pub capacity_bytes: usize,
    /// Entries in the protected segment (hit at least twice).
    pub protected_entries: usize,
    /// Lookup latency of hits.
    pub hit_seconds: HistogramSnapshot,
}

/// A sharded LRU cache of `K -> V` (values are cloned out; use `Arc`s).
#[derive(Debug)]
pub struct ShardedLru<K, V> {
    shards: Vec<Mutex<Shard<K, V>>>,
    hasher: std::collections::hash_map::RandomState,
    per_shard_entries: usize,
    per_shard_bytes: usize,
    /// One histogram per shard keeps hit timing off shared atomics.
    hit_seconds: Vec<Histogram>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl<K: Hash + Eq + Clone, V: Clone> ShardedLru<K, V> {
    /// A cache of at most `entries` entries and `bytes` bytes. Zero in
    /// either disables caching (every lookup misses).
    pub fn new(entries: usize, bytes: usize) -> Self {
        let shards = SHARDS;
        Self {
            shards: (0..shards).map(|_| Mutex::new(Shard::default())).collect(),
            hasher: std::collections::hash_map::RandomState::new(),
            per_shard_entries: entries.div_ceil(shards),
            per_shard_bytes: bytes / shards,
            hit_seconds: (0..shards).map(|_| Histogram::stage()).collect(),
        }
    }

    fn shard_of<Q: Hash + ?Sized>(&self, key: &Q) -> usize {
        (self.hasher.hash_one(key) as usize) % self.shards.len()
    }

    pub fn enabled(&self) -> bool {
        self.per_shard_entries > 0 && self.per_shard_bytes > 0
    }

    /// The cached value for `key`, refreshing its recency.
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let started = Instant::now();
        let i = self.shard_of(key);
        let mut s = lock(&self.shards[i]);
        s.clock += 1;
        let clock = s.clock;
        match s.map.get_mut(key) {
            Some(e) => {
                e.stamp = clock;
                let v = e.value.clone();
                let promote = !e.protected;
                e.protected = true;
                s.hits += 1;
                if promote {
                    s.protected += 1;
                    // Keep the protected segment within its share: demote
                    // its least recent entry (never the one just hit).
                    let limit = (self.per_shard_entries * 4 / 5).max(1);
                    while s.protected > limit {
                        let Some(e) = s
                            .map
                            .values_mut()
                            .filter(|e| e.protected && e.stamp != clock)
                            .min_by_key(|e| e.stamp)
                        else {
                            break;
                        };
                        e.protected = false;
                        s.protected -= 1;
                    }
                }
                drop(s);
                self.hit_seconds[i].observe_duration(started.elapsed());
                Some(v)
            }
            None => {
                s.misses += 1;
                None
            }
        }
    }

    /// Cache `value` under `key`, stating its size; evicts least-recently
    /// used entries of the shard as needed. Returns the bytes that left the
    /// cache (evicted or replaced), or `None` if the value was not cached
    /// (disabled, or larger than a shard's byte budget).
    pub fn insert(&self, key: K, value: V, bytes: usize) -> Option<usize> {
        if !self.enabled() || bytes > self.per_shard_bytes {
            return None;
        }
        let mut freed = 0usize;
        let i = self.shard_of(&key);
        let mut s = lock(&self.shards[i]);
        s.clock += 1;
        let stamp = s.clock;
        if let Some(old) = s.map.insert(
            key,
            Entry {
                value,
                bytes,
                stamp,
                protected: false,
            },
        ) {
            s.bytes -= old.bytes;
            freed += old.bytes;
            if old.protected {
                s.protected -= 1;
            }
        }
        s.bytes += bytes;
        s.insertions += 1;
        while s.map.len() > self.per_shard_entries || s.bytes > self.per_shard_bytes {
            // Least recent probationary entry first, then protected.
            let Some(victim) = s
                .map
                .iter()
                .min_by_key(|(_, e)| (e.protected, e.stamp))
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(e) = s.map.remove(&victim) {
                s.bytes -= e.bytes;
                freed += e.bytes;
                s.evictions += 1;
                if e.protected {
                    s.protected -= 1;
                }
            }
        }
        Some(freed)
    }

    /// Drop every entry (counters are kept).
    pub fn clear(&self) {
        for sh in &self.shards {
            let mut s = lock(sh);
            s.map.clear();
            s.bytes = 0;
            s.protected = 0;
        }
    }

    pub fn stats(&self) -> CacheStats {
        let mut st = CacheStats {
            hits: 0,
            misses: 0,
            insertions: 0,
            evictions: 0,
            entries: 0,
            bytes: 0,
            capacity_entries: self.per_shard_entries * self.shards.len(),
            capacity_bytes: self.per_shard_bytes * self.shards.len(),
            protected_entries: 0,
            hit_seconds: Histogram::stage().snapshot(),
        };
        for sh in &self.shards {
            let s = lock(sh);
            st.hits += s.hits;
            st.misses += s.misses;
            st.insertions += s.insertions;
            st.evictions += s.evictions;
            st.entries += s.map.len();
            st.bytes += s.bytes;
            st.protected_entries += s.protected;
        }
        for h in &self.hit_seconds {
            let x = h.snapshot();
            for (a, b) in st.hit_seconds.buckets.iter_mut().zip(&x.buckets) {
                *a += b;
            }
            st.hit_seconds.count += x.count;
            st.hit_seconds.sum += x.sum;
        }
        st
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hits_misses_and_lru_eviction_within_bounds() {
        // 64 entries total -> 1 per shard: a second key in a shard evicts.
        let c: ShardedLru<String, u32> = ShardedLru::new(64 * 2, 1 << 20);
        for i in 0..1000u32 {
            c.insert(format!("k{i}"), i, 10);
        }
        let s = c.stats();
        assert!(s.entries <= 128, "{s:?}");
        assert_eq!(s.insertions, 1000);
        assert_eq!(s.evictions as usize, 1000 - s.entries);
        assert_eq!(s.bytes, s.entries * 10);
        // The most recent keys survive; lookups count.
        assert_eq!(c.get("k999"), Some(999));
        assert_eq!(c.get("never"), None);
        let s = c.stats();
        assert_eq!((s.hits, s.misses), (1, 1));
        assert_eq!(s.hit_seconds.count, 1);
    }

    #[test]
    fn recency_protects_used_entries() {
        let c: ShardedLru<u32, u32> = ShardedLru::new(SHARDS * 2, 1 << 20);
        // Find three keys in the same shard.
        let shard = c.shard_of(&0u32);
        let same: Vec<u32> = (0..10_000u32)
            .filter(|k| c.shard_of(k) == shard)
            .take(3)
            .collect();
        c.insert(same[0], 0, 1);
        c.insert(same[1], 1, 1);
        assert_eq!(c.get(&same[0]), Some(0)); // same[0] is now newer
        c.insert(same[2], 2, 1); // evicts the least recent: same[1]
        assert_eq!(c.get(&same[1]), None);
        assert_eq!((c.get(&same[0]), c.get(&same[2])), (Some(0), Some(2)));
    }

    #[test]
    fn a_scan_of_one_time_keys_does_not_evict_the_working_set() {
        // 10 entries per shard; keys of one shard only.
        let c: ShardedLru<u32, u32> = ShardedLru::new(SHARDS * 10, 1 << 20);
        let shard = c.shard_of(&0u32);
        let keys: Vec<u32> = (0..1_000_000u32)
            .filter(|k| c.shard_of(k) == shard)
            .take(200)
            .collect();
        let (hot, scan) = keys.split_at(5);
        for k in hot {
            c.insert(*k, *k, 1);
            assert_eq!(c.get(k), Some(*k)); // second reference: protected
        }
        // A large scan of keys read once.
        for k in scan {
            c.insert(*k, *k, 1);
        }
        for k in hot {
            assert_eq!(c.get(k), Some(*k), "hot key {k} evicted by a scan");
        }
        let s = c.stats();
        assert!(
            s.protected_entries >= 5 && s.protected_entries <= SHARDS * 8,
            "{s:?}"
        );
    }

    #[test]
    fn byte_budget_and_disabled_cache() {
        let c: ShardedLru<u32, u32> = ShardedLru::new(10_000, SHARDS * 100);
        assert!(
            c.insert(1, 1, 101).is_none(),
            "larger than a shard's budget"
        );
        assert_eq!(c.insert(2, 2, 60), Some(0));
        let s = c.stats();
        assert!(s.bytes <= s.capacity_bytes);
        let off: ShardedLru<u32, u32> = ShardedLru::new(0, 1 << 20);
        assert!(!off.enabled() && off.insert(1, 1, 1).is_none());
        assert_eq!(off.get(&1), None);
        c.clear();
        assert_eq!(c.stats().entries, 0);
    }

    #[test]
    fn concurrent_use_is_consistent() {
        let c = std::sync::Arc::new(ShardedLru::<u32, u32>::new(4096, 1 << 20));
        std::thread::scope(|s| {
            for t in 0..8u32 {
                let c = std::sync::Arc::clone(&c);
                s.spawn(move || {
                    for i in 0..5000u32 {
                        let k = (i * 7 + t) % 3000;
                        if c.get(&k).is_none() {
                            c.insert(k, k, 1);
                        }
                    }
                });
            }
        });
        let st = c.stats();
        assert_eq!(st.hits + st.misses, 40_000);
        for k in 0..3000u32 {
            if let Some(v) = c.get(&k) {
                assert_eq!(v, k);
            }
        }
    }
}
