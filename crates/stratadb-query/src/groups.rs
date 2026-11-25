//! Aggregation groups in flat storage.
//!
//! One hash table of group indices plus flat arrays: an arena holding every
//! group's key bytes (`eval::group_key_bytes` per key, length-prefixed), and
//! `n_keys` key values / `n_aggs` aggregate states per group. Creating a group
//! appends to the arrays — no per-group `Vec` or key copy — and lookups compare
//! the probe bytes against the arena slice. Groups keep insertion (first-seen)
//! order, which defines the output order of `GROUP BY` without `ORDER BY`.
//!
//! Hashing uses aHash with a per-table random seed from the OS (`runtime-rng`):
//! fast, and keys crafted without knowledge of the seed cannot force collisions.

use crate::eval::Datum;

pub(crate) struct GroupTable<S> {
    hasher: ahash::RandomState,
    table: hashbrown::HashTable<u32>,
    arena: Vec<u8>,
    /// `(start, end)` of each group's key bytes in `arena`.
    spans: Vec<(usize, usize)>,
    hashes: Vec<u64>,
    keys: Vec<Datum>,
    states: Vec<S>,
    n_keys: usize,
    n_aggs: usize,
}

impl<S> GroupTable<S> {
    pub(crate) fn new(n_keys: usize, n_aggs: usize) -> Self {
        Self {
            hasher: Default::default(),
            table: hashbrown::HashTable::new(),
            arena: Vec::new(),
            spans: Vec::new(),
            hashes: Vec::new(),
            keys: Vec::new(),
            states: Vec::new(),
            n_keys,
            n_aggs,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.spans.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.spans.is_empty()
    }

    pub(crate) fn hash(&self, kb: &[u8]) -> u64 {
        self.hasher.hash_one(kb)
    }

    /// The group whose key bytes equal `kb` (`hash` from [`Self::hash`]).
    pub(crate) fn find(&self, hash: u64, kb: &[u8]) -> Option<usize> {
        let (arena, spans) = (&self.arena, &self.spans);
        self.table
            .find(hash, |&g| {
                let (s, e) = spans[g as usize];
                &arena[s..e] == kb
            })
            .map(|&g| g as usize)
    }

    /// Append a new group (caller checked it is absent). Returns its index.
    pub(crate) fn insert(
        &mut self,
        hash: u64,
        kb: &[u8],
        keys: impl IntoIterator<Item = Datum>,
        states: impl IntoIterator<Item = S>,
    ) -> usize {
        let g = self.spans.len();
        let start = self.arena.len();
        self.arena.extend_from_slice(kb);
        self.spans.push((start, self.arena.len()));
        self.hashes.push(hash);
        self.keys.extend(keys);
        self.states.extend(states);
        debug_assert_eq!(self.keys.len(), (g + 1) * self.n_keys);
        debug_assert_eq!(self.states.len(), (g + 1) * self.n_aggs);
        let hashes = &self.hashes;
        self.table
            .insert_unique(hash, g as u32, |&x| hashes[x as usize]);
        g
    }

    pub(crate) fn states_mut(&mut self, g: usize) -> &mut [S] {
        &mut self.states[g * self.n_aggs..(g + 1) * self.n_aggs]
    }

    /// Key bytes, key values and states of group `g`.
    pub(crate) fn group(&self, g: usize) -> (&[u8], &[Datum], &[S]) {
        let (s, e) = self.spans[g];
        (
            &self.arena[s..e],
            &self.keys[g * self.n_keys..(g + 1) * self.n_keys],
            &self.states[g * self.n_aggs..(g + 1) * self.n_aggs],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stratadb_schema::Value;

    #[test]
    fn insertion_order_lookup_and_growth() {
        let mut t: GroupTable<u64> = GroupTable::new(1, 1);
        for i in 0..10_000u64 {
            let kb = (i % 2_500).to_le_bytes();
            let h = t.hash(&kb);
            match t.find(h, &kb) {
                Some(g) => t.states_mut(g)[0] += 1,
                None => {
                    t.insert(h, &kb, [Some(Value::UInt(i % 2_500))], [1u64]);
                }
            }
        }
        assert_eq!(t.len(), 2_500);
        for g in 0..t.len() {
            let (kb, keys, states) = t.group(g);
            assert_eq!(kb, &(g as u64).to_le_bytes(), "first-seen order kept");
            assert_eq!(keys, &[Some(Value::UInt(g as u64))]);
            assert_eq!(states, &[4]);
        }
        assert!(t.find(t.hash(b"nope"), b"nope").is_none());
        // Empty keys (global aggregate) are a valid group.
        let mut e: GroupTable<u8> = GroupTable::new(0, 2);
        let h = e.hash(&[]);
        let g = e.insert(h, &[], [], [1, 2]);
        assert_eq!(e.find(h, &[]), Some(g));
        assert_eq!(e.group(g).2, &[1, 2]);
    }
}
