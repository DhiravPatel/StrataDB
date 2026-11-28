//! Aggregation groups in flat storage.
//!
//! One hash table of group indices plus flat arrays: an arena holding every
//! group's key bytes (per key a `u32` length then its `eval::group_key_bytes`)
//! and `n_aggs` aggregate states per group. Creating a group appends to the
//! arrays — no per-group `Vec`, and no key *values*: the key encoding is
//! bit-exact (`encode_value`), so [`GroupTable::keys`] decodes the first-seen
//! values from the arena once per output group instead of materializing them
//! for every group of every per-segment partial. Groups keep insertion
//! (first-seen) order, which defines the output order of `GROUP BY` without
//! `ORDER BY`.
//!
//! Hashing uses aHash with a per-query random seed from the OS
//! (`runtime-rng`): fast, and keys crafted without knowledge of the seed cannot
//! force collisions. Partial tables of one query share the seed
//! ([`GroupTable::with_hasher`]), so merging reuses their stored hashes.

use stratadb_common::codec::Decoder;
use stratadb_common::{Error, Result};
use stratadb_schema::codec::{decode_value, DecodeLimits};

use crate::eval::{Datum, MISSING_KEY_TAG};

pub(crate) struct GroupTable<S> {
    hasher: ahash::RandomState,
    table: hashbrown::HashTable<u32>,
    arena: Vec<u8>,
    /// `(start, end)` of each group's key bytes in `arena`.
    spans: Vec<(usize, usize)>,
    hashes: Vec<u64>,
    states: Vec<S>,
    n_aggs: usize,
    /// Times the hash table's capacity grew.
    resizes: u64,
}

/// Statistics of one table (EXPLAIN ANALYZE, STRATA-SYS-09-011).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableStats {
    pub groups: usize,
    /// Groups the hash table holds before it grows (load factor 7/8).
    pub capacity: usize,
    pub resizes: u64,
    /// Heap bytes held (capacity, not length).
    pub bytes: usize,
}

/// Bytes per hash-table bucket: a `u32` group index plus one control byte.
const BUCKET_BYTES: usize = std::mem::size_of::<u32>() + 1;

impl<S> GroupTable<S> {
    pub(crate) fn new(n_aggs: usize) -> Self {
        Self::with_hasher(n_aggs, ahash::RandomState::new())
    }

    /// A table hashing exactly like the table `hasher` came from.
    pub(crate) fn with_hasher(n_aggs: usize, hasher: ahash::RandomState) -> Self {
        Self {
            hasher,
            table: hashbrown::HashTable::new(),
            arena: Vec::new(),
            spans: Vec::new(),
            hashes: Vec::new(),
            states: Vec::new(),
            n_aggs,
            resizes: 0,
        }
    }

    /// Heap bytes held by the table and its arrays (by capacity).
    pub(crate) fn heap_bytes(&self) -> usize {
        // hashbrown keeps capacity at 7/8 of the buckets.
        let buckets = self.table.capacity() * 8 / 7 + 1;
        buckets * BUCKET_BYTES
            + self.arena.capacity()
            + self.spans.capacity() * std::mem::size_of::<(usize, usize)>()
            + self.hashes.capacity() * std::mem::size_of::<u64>()
            + self.states.capacity() * std::mem::size_of::<S>()
    }

    /// Upper bound of the bytes the next insert of a `key_len`-byte key
    /// allocates: every array at capacity doubles (STRATA-SYS-09-002 asks
    /// for this to be reserved before the allocation).
    pub(crate) fn growth_bytes(&self, key_len: usize) -> usize {
        let doubled = |cap: usize, len: usize, add: usize, elem: usize| {
            if len + add > cap {
                (cap.max(4) + add) * elem
            } else {
                0
            }
        };
        let table = if self.table.len() >= self.table.capacity() {
            (self.table.capacity().max(3) * 2 * 8 / 7 + 1) * BUCKET_BYTES
        } else {
            0
        };
        table
            + doubled(self.arena.capacity(), self.arena.len(), key_len, 1)
            + doubled(
                self.spans.capacity(),
                self.spans.len(),
                1,
                std::mem::size_of::<(usize, usize)>(),
            )
            + doubled(
                self.hashes.capacity(),
                self.hashes.len(),
                1,
                std::mem::size_of::<u64>(),
            )
            + doubled(
                self.states.capacity(),
                self.states.len(),
                self.n_aggs,
                std::mem::size_of::<S>(),
            )
    }

    pub(crate) fn stats(&self) -> TableStats {
        TableStats {
            groups: self.len(),
            capacity: self.table.capacity(),
            resizes: self.resizes,
            bytes: self.heap_bytes(),
        }
    }

    pub(crate) fn hasher(&self) -> ahash::RandomState {
        self.hasher.clone()
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
        states: impl IntoIterator<Item = S>,
    ) -> usize {
        let g = self.spans.len();
        let start = self.arena.len();
        self.arena.extend_from_slice(kb);
        self.spans.push((start, self.arena.len()));
        self.hashes.push(hash);
        self.states.extend(states);
        debug_assert_eq!(self.states.len(), (g + 1) * self.n_aggs);
        let hashes = &self.hashes;
        let cap = self.table.capacity();
        self.table
            .insert_unique(hash, g as u32, |&x| hashes[x as usize]);
        if self.table.capacity() > cap {
            self.resizes += 1;
        }
        g
    }

    pub(crate) fn states_mut(&mut self, g: usize) -> &mut [S] {
        &mut self.states[g * self.n_aggs..(g + 1) * self.n_aggs]
    }

    /// Key bytes and states of group `g`.
    pub(crate) fn group(&self, g: usize) -> (&[u8], &[S]) {
        let (s, e) = self.spans[g];
        (
            &self.arena[s..e],
            &self.states[g * self.n_aggs..(g + 1) * self.n_aggs],
        )
    }

    /// Fold this table into `dst` (same hasher seed) in this table's group
    /// order: existing groups are combined with `merge`, new groups are
    /// appended (moving their states). Hashes are reused, not recomputed.
    pub(crate) fn merge_into(
        self,
        dst: &mut GroupTable<S>,
        mut merge: impl FnMut(&mut S, S) -> Result<()>,
    ) -> Result<()> {
        debug_assert_eq!(self.n_aggs, dst.n_aggs);
        let mut states = self.states.into_iter();
        for (g, &(s, e)) in self.spans.iter().enumerate() {
            let (h, kb) = (self.hashes[g], &self.arena[s..e]);
            let mine = states.by_ref().take(self.n_aggs);
            match dst.find(h, kb) {
                Some(idx) => {
                    for (a, b) in dst.states_mut(idx).iter_mut().zip(mine) {
                        merge(a, b)?;
                    }
                }
                None => {
                    dst.insert(h, kb, mine);
                }
            }
        }
        Ok(())
    }
}

/// A group table's groups without its hash index or seed: key bytes and
/// states in first-seen order, merged into any table by rehashing (cached
/// per-segment partials outlive the query whose seed built them; spec
/// STRATA-ARCH-09-002).
#[derive(Debug, Clone)]
pub(crate) struct FrozenGroups<S> {
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
    states: Vec<S>,
    n_aggs: usize,
}

impl<S: Clone> GroupTable<S> {
    /// A copy of the groups (exactly sized), or `None` when the key arena
    /// exceeds `u32` offsets.
    pub(crate) fn freeze(&self) -> Option<FrozenGroups<S>> {
        let spans = self
            .spans
            .iter()
            .map(|&(s, e)| Some((u32::try_from(s).ok()?, u32::try_from(e).ok()?)))
            .collect::<Option<Vec<_>>>()?;
        Some(FrozenGroups {
            arena: self.arena.clone(),
            spans,
            states: self.states.clone(),
            n_aggs: self.n_aggs,
        })
    }
}

impl<S: Clone> FrozenGroups<S> {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.spans.len()
    }

    /// Heap bytes, with `state_bytes` per aggregate state.
    pub(crate) fn heap_bytes(&self, state_bytes: usize) -> usize {
        self.arena.len()
            + self.spans.len() * std::mem::size_of::<(u32, u32)>()
            + self.states.len() * state_bytes
    }

    /// Fold these groups into `dst` in first-seen order, exactly as merging
    /// the original table would: existing groups are combined with
    /// `merge`, new ones appended (states cloned). Keys are rehashed with
    /// `dst`'s seed.
    pub(crate) fn merge_into(
        &self,
        dst: &mut GroupTable<S>,
        mut merge: impl FnMut(&mut S, &S) -> Result<()>,
    ) -> Result<()> {
        if self.n_aggs != dst.n_aggs {
            return Err(Error::internal(
                "cached groups have another aggregate count",
            ));
        }
        for (g, &(s, e)) in self.spans.iter().enumerate() {
            let kb = &self.arena[s as usize..e as usize];
            let mine = &self.states[g * self.n_aggs..(g + 1) * self.n_aggs];
            let h = dst.hash(kb);
            match dst.find(h, kb) {
                Some(idx) => {
                    for (a, b) in dst.states_mut(idx).iter_mut().zip(mine) {
                        merge(a, b)?;
                    }
                }
                None => {
                    dst.insert(h, kb, mine.iter().cloned());
                }
            }
        }
        Ok(())
    }
}

/// Decode `n_keys` key values from group key bytes (the inverse of the
/// encoding built by `Ctx::process`: per key a `u32` length, then
/// `MISSING_KEY_TAG` or an `encode_value` encoding).
pub(crate) fn decode_keys(kb: &[u8], n_keys: usize) -> Result<Vec<Datum>> {
    // The bytes were produced in memory by this process: bound decoding by
    // their own size, not by on-disk limits.
    let lim = DecodeLimits {
        max_depth: usize::from(u16::MAX),
        max_string_bytes: kb.len(),
        max_items: kb.len(),
    };
    let mut d = Decoder::new(kb, "group key");
    let mut out = Vec::with_capacity(n_keys);
    for _ in 0..n_keys {
        let n = d.get_u32()? as usize;
        let bytes = d.get_raw(n)?;
        if bytes == [MISSING_KEY_TAG] {
            out.push(None);
            continue;
        }
        let mut vd = Decoder::new(bytes, "group key");
        out.push(Some(decode_value(&mut vd, &lim)?));
        if !vd.is_empty() {
            return Err(Error::internal("trailing bytes in group key"));
        }
    }
    if !d.is_empty() {
        return Err(Error::internal("trailing bytes after group keys"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::group_key_bytes;
    use stratadb_schema::Value;

    fn key_bytes(keys: &[Datum]) -> Vec<u8> {
        let mut kb = Vec::new();
        for k in keys {
            let b = group_key_bytes(k);
            kb.extend_from_slice(&(b.len() as u32).to_le_bytes());
            kb.extend_from_slice(&b);
        }
        kb
    }

    #[test]
    fn insertion_order_lookup_and_growth() {
        let mut t: GroupTable<u64> = GroupTable::new(1);
        for i in 0..10_000u64 {
            let kb = key_bytes(&[Some(Value::UInt(i % 2_500))]);
            let h = t.hash(&kb);
            match t.find(h, &kb) {
                Some(g) => t.states_mut(g)[0] += 1,
                None => {
                    t.insert(h, &kb, [1u64]);
                }
            }
        }
        assert_eq!(t.len(), 2_500);
        for g in 0..t.len() {
            let (kb, states) = t.group(g);
            assert_eq!(
                decode_keys(kb, 1).unwrap(),
                vec![Some(Value::UInt(g as u64))],
                "first-seen order kept"
            );
            assert_eq!(states, &[4]);
        }
        assert!(t.find(t.hash(b"nope"), b"nope").is_none());
        // Empty keys (global aggregate) are a valid group.
        let mut e: GroupTable<u8> = GroupTable::new(2);
        let h = e.hash(&[]);
        let g = e.insert(h, &[], [1, 2]);
        assert_eq!(e.find(h, &[]), Some(g));
        assert_eq!(e.group(g).1, &[1, 2]);
        assert_eq!(decode_keys(&[], 0).unwrap(), Vec::<Datum>::new());
    }

    #[test]
    fn growth_is_bounded_before_it_happens() {
        let mut t: GroupTable<u64> = GroupTable::new(1);
        let mut last = t.heap_bytes();
        for i in 0..50_000u64 {
            let kb = key_bytes(&[Some(Value::UInt(i))]);
            let bound = t.growth_bytes(kb.len());
            let h = t.hash(&kb);
            t.insert(h, &kb, [i]);
            let now = t.heap_bytes();
            // What the insert allocated never exceeds the reserved bound.
            assert!(
                now.saturating_sub(last) <= bound,
                "{i}: grew {} > bound {bound}",
                now - last
            );
            last = now;
        }
        let s = t.stats();
        assert_eq!(s.groups, 50_000);
        assert!(s.capacity >= 50_000 && s.resizes >= 10, "{s:?}");
        assert_eq!(s.bytes, t.heap_bytes());
    }

    #[test]
    fn merge_keeps_order_and_combines_states() {
        let mut a: GroupTable<u64> = GroupTable::new(1);
        let mut b: GroupTable<u64> = GroupTable::with_hasher(1, a.hasher());
        for (t, keys) in [(&mut a, [1u64, 2, 3]), (&mut b, [3, 4, 1])] {
            for k in keys {
                let kb = key_bytes(&[Some(Value::UInt(k))]);
                let h = t.hash(&kb);
                t.insert(h, &kb, [k * 10]);
            }
        }
        b.merge_into(&mut a, |x, y| {
            *x += y;
            Ok(())
        })
        .unwrap();
        let got: Vec<(Datum, u64)> = (0..a.len())
            .map(|g| {
                let (kb, s) = a.group(g);
                (decode_keys(kb, 1).unwrap().remove(0), s[0])
            })
            .collect();
        let u = |x| Some(Value::UInt(x));
        assert_eq!(got, vec![(u(1), 20), (u(2), 20), (u(3), 60), (u(4), 40)]);
        // Stored hashes stayed valid: every group is still found.
        for g in 0..a.len() {
            let kb = a.group(g).0.to_vec();
            assert_eq!(a.find(a.hash(&kb), &kb), Some(g));
        }
    }

    /// Frozen groups merge into a table with another seed exactly like the
    /// original table merges into one with its own seed.
    #[test]
    fn frozen_groups_merge_like_the_table_across_seeds() {
        let build = |seed_of: Option<&GroupTable<u64>>, keys: &[u64]| {
            let mut t = match seed_of {
                Some(o) => GroupTable::with_hasher(1, o.hasher()),
                None => GroupTable::new(1),
            };
            for &k in keys {
                let kb = key_bytes(&[Some(Value::UInt(k % 7))]);
                let h = t.hash(&kb);
                match t.find(h, &kb) {
                    Some(g) => t.states_mut(g)[0] += k,
                    None => {
                        t.insert(h, &kb, [k]);
                    }
                }
            }
            t
        };
        let dump = |t: &GroupTable<u64>| -> Vec<(Vec<u8>, u64)> {
            (0..t.len())
                .map(|g| (t.group(g).0.to_vec(), t.group(g).1[0]))
                .collect()
        };
        let keys_a = [5u64, 9, 12, 3, 40];
        let keys_b = [8u64, 5, 31, 2, 16, 23];
        // Reference: same-seed merge of the original table.
        let mut want = build(None, &keys_a);
        build(Some(&want), &keys_b)
            .merge_into(&mut want, |x, y| {
                *x += y;
                Ok(())
            })
            .unwrap();
        // Frozen under one seed, merged into a table with a fresh seed.
        let frozen = build(None, &keys_b).freeze().unwrap();
        let mut got = build(None, &keys_a);
        frozen
            .merge_into(&mut got, |x, y| {
                *x += *y;
                Ok(())
            })
            .unwrap();
        assert_eq!(dump(&got), dump(&want));
        for g in 0..got.len() {
            let kb = got.group(g).0.to_vec();
            assert_eq!(got.find(got.hash(&kb), &kb), Some(g));
        }
        assert_eq!(frozen.len(), 4, "8, 5, 31, 2, 16, 23 mod 7");
        let mut other = GroupTable::<u64>::new(2);
        assert!(frozen.merge_into(&mut other, |_, _| Ok(())).is_err());
    }

    fn arb_value() -> impl proptest::strategy::Strategy<Value = Value> {
        use proptest::prelude::*;
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            any::<u64>().prop_map(Value::UInt),
            // Raw bits: -0.0, NaN payloads, infinities.
            any::<u64>().prop_map(|b| Value::Float(f64::from_bits(b))),
            ".{0,16}".prop_map(Value::String),
            proptest::collection::vec(any::<u8>(), 0..16).prop_map(Value::Bytes),
            any::<i64>().prop_map(Value::Timestamp),
        ];
        leaf.prop_recursive(3, 32, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::List),
                proptest::collection::btree_map(".{0,8}", inner, 0..4).prop_map(Value::Map),
            ]
        })
    }

    proptest::proptest! {
        /// Decoding group key bytes returns exactly (bit for bit) the values
        /// that were encoded, including floats (-0.0, NaN payloads), nested
        /// lists/maps, null and missing.
        #[test]
        fn decode_keys_inverts_encoding(
            keys in proptest::collection::vec(
                proptest::option::of(arb_value()), 0..4)
        ) {
            let kb = key_bytes(&keys);
            let back = decode_keys(&kb, keys.len()).unwrap();
            proptest::prop_assert_eq!(key_bytes(&back), kb);
        }
    }

    #[test]
    fn malformed_key_bytes_are_errors_not_panics() {
        for bad in [
            &[1u8][..],
            &[5, 0, 0, 0, 1],
            &[1, 0, 0, 0, 0x7e],
            &[0, 0, 0, 0, 9],
        ] {
            assert!(decode_keys(bad, 1).is_err(), "{bad:?}");
        }
    }
}
