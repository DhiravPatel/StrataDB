//! Normalized-key sorting (spec STRATA-SYS-14-001/002/006; ADR-0043).
//!
//! - **Encoding:** every `ORDER BY` key becomes a fixed-width,
//!   memcmp-comparable prefix ([`COLUMN_BYTES`] per column), so most
//!   comparisons are one `memcmp` instead of a walk over typed values.
//! - **Sorting:** indices, not rows, are sorted (a permutation, 14-006):
//!   radix for short keys and many rows, pdqsort otherwise (14-002).
//! - **Merging:** sorted runs (one per scanned segment, sorted on the scan
//!   workers) are combined by a loser tree (14-001).
//!
//! **Correctness rule.** A prefix only ever *agrees* with the full order
//! ([`crate::eval::sort_cmp`]): if `a` sorts before `b` then
//! `prefix(a) <= prefix(b)`. Equal prefixes do not prove equal values (a
//! long string is truncated), so ties go to the caller's full comparator.
//! The result is always exactly the stable sort by that comparator.

use std::cmp::Ordering;

use stratadb_schema::Value;

use crate::eval::Datum;

/// Bytes per sort column: a tag byte (type, null, missing) and an 8-byte
/// payload.
pub(crate) const COLUMN_BYTES: usize = 9;

/// Radix sort from this many rows (spec STRATA-SYS-14-002) ...
pub(crate) const RADIX_MIN_ROWS: usize = 4096;
/// ... for keys up to this many bytes.
pub(crate) const RADIX_MAX_KEY_BYTES: usize = 16;

/// Tags in ascending order. Numbers share one tag and are compared by value.
const TAG_BOOL: u8 = 0;
const TAG_NUMBER: u8 = 1;
const TAG_TIMESTAMP: u8 = 2;
const TAG_STRING: u8 = 3;
const TAG_BYTES: u8 = 4;
const TAG_LIST: u8 = 5;
const TAG_MAP: u8 = 6;
const TAG_NULL: u8 = 7;
const TAG_MISSING: u8 = 8;

/// String and byte prefixes keep this many bytes plus a length byte; values
/// this long or shorter are encoded exactly.
const STR_PREFIX: usize = 7;

/// Integers up to this magnitude convert to `f64` exactly.
const EXACT_F64_INT: u64 = 1 << 53;

/// Normalized keys for a set of rows: `stride` bytes per row, of which the
/// first `width` are compared.
#[derive(Debug)]
pub(crate) struct NormKeys {
    bytes: Vec<u8>,
    stride: usize,
    /// Columns up to and including the first one holding an inexact value:
    /// bytes of later columns can contradict the order of an undecided
    /// earlier one, so they are left to the full comparator.
    width: usize,
    /// Every value's prefix is exact: equal prefixes mean equal keys.
    exact: bool,
    rows: usize,
}

impl NormKeys {
    /// Encode `keys(row)` (one datum per entry of `desc`) for every row.
    pub(crate) fn build<R>(rows: &[R], desc: &[bool], keys: impl Fn(&R) -> &[Datum]) -> NormKeys {
        let stride = desc.len() * COLUMN_BYTES;
        let mut bytes = vec![0u8; rows.len() * stride];
        let mut first_inexact = desc.len();
        if stride > 0 {
            let skip = common_prefixes(rows, desc.len(), &keys);
            for (r, out) in rows.iter().zip(bytes.chunks_exact_mut(stride)) {
                for (j, (d, desc)) in keys(r).iter().zip(desc).enumerate() {
                    let col = &mut out[j * COLUMN_BYTES..(j + 1) * COLUMN_BYTES];
                    if !encode(d, *desc, skip[j], col) && j < first_inexact {
                        first_inexact = j;
                    }
                }
            }
        }
        let exact = first_inexact == desc.len();
        NormKeys {
            bytes,
            stride,
            width: if exact {
                stride
            } else {
                (first_inexact + 1) * COLUMN_BYTES
            },
            exact,
            rows: rows.len(),
        }
    }

    /// Whether equal prefixes mean equal keys.
    pub(crate) fn exact(&self) -> bool {
        self.exact
    }

    /// The compared bytes of row `i`.
    fn key(&self, i: usize) -> &[u8] {
        let at = i * self.stride;
        &self.bytes[at..at + self.width]
    }
}

/// Per column, the byte prefix shared by every string value and by every
/// bytes value. Dropping a prefix all values share keeps their byte order,
/// and moves the distinguishing bytes into the key: log values such as
/// URLs or `service-NN` share long prefixes.
fn common_prefixes<R>(rows: &[R], cols: usize, keys: &impl Fn(&R) -> &[Datum]) -> Vec<Skip> {
    /// The first value seen of one kind and the prefix length shared so far.
    #[derive(Clone, Copy, Default)]
    struct Shared<'a> {
        first: Option<&'a [u8]>,
        len: usize,
    }
    impl<'a> Shared<'a> {
        fn add(&mut self, v: &'a [u8]) {
            match self.first {
                None => {
                    self.first = Some(v);
                    self.len = v.len();
                }
                Some(f) => {
                    let n = self.len.min(v.len());
                    self.len = f[..n]
                        .iter()
                        .zip(&v[..n])
                        .take_while(|(a, b)| a == b)
                        .count();
                }
            }
        }
    }
    let mut shared: Vec<[Shared<'_>; 2]> = vec![[Shared::default(); 2]; cols];
    for r in rows {
        for (j, d) in keys(r).iter().enumerate().take(cols) {
            match d {
                Some(Value::String(v)) => shared[j][0].add(v.as_bytes()),
                Some(Value::Bytes(v)) => shared[j][1].add(v),
                _ => {}
            }
        }
    }
    shared
        .iter()
        .map(|[s, b]| Skip {
            string: s.len,
            bytes: b.len,
        })
        .collect()
}

/// Leading bytes left out of a column's string and bytes prefixes.
#[derive(Debug, Clone, Copy, Default)]
struct Skip {
    string: usize,
    bytes: usize,
}

/// Write `d`'s prefix into `out` (`COLUMN_BYTES` long), leaving out the
/// column's common leading bytes; returns whether it is exact (two exact
/// values with equal prefixes compare equal).
fn encode(d: &Datum, desc: bool, skip: Skip, out: &mut [u8]) -> bool {
    let (tag, payload, exact) = match d {
        None => (TAG_MISSING, [0u8; 8], true),
        Some(v) => match v {
            Value::Bool(b) => (TAG_BOOL, u64::from(*b).to_be_bytes(), true),
            Value::Int(x) => (
                TAG_NUMBER,
                float_key(*x as f64),
                x.unsigned_abs() <= EXACT_F64_INT,
            ),
            Value::UInt(x) => (TAG_NUMBER, float_key(*x as f64), *x <= EXACT_F64_INT),
            Value::Float(f) => (TAG_NUMBER, float_key(*f), true),
            Value::Timestamp(t) => (TAG_TIMESTAMP, int_key(*t), true),
            Value::String(s) => {
                let (p, exact) = bytes_prefix(&s.as_bytes()[skip.string.min(s.len())..]);
                (TAG_STRING, p, exact)
            }
            Value::Bytes(b) => {
                let (p, exact) = bytes_prefix(&b[skip.bytes.min(b.len())..]);
                (TAG_BYTES, p, exact)
            }
            Value::List(_) => (TAG_LIST, [0u8; 8], false),
            Value::Map(_) => (TAG_MAP, [0u8; 8], false),
            Value::Null => (TAG_NULL, [0u8; 8], true),
        },
    };
    out[0] = tag;
    out[1..COLUMN_BYTES].copy_from_slice(&payload);
    if desc {
        for b in out.iter_mut() {
            *b = !*b;
        }
    }
    exact
}

/// Signed integer, big-endian with the sign bit flipped.
fn int_key(x: i64) -> [u8; 8] {
    ((x as u64) ^ (1 << 63)).to_be_bytes()
}

/// IEEE total-order transform, with `-0.0` folded into `0.0` and every NaN
/// after `+inf` (as [`crate::eval::sort_cmp`] orders them).
fn float_key(f: f64) -> [u8; 8] {
    if f.is_nan() {
        return [0xff; 8];
    }
    let f = if f == 0.0 { 0.0 } else { f };
    let bits = f.to_bits();
    let key = if bits >> 63 == 1 {
        !bits
    } else {
        bits | (1 << 63)
    };
    key.to_be_bytes()
}

/// The first [`STR_PREFIX`] bytes, zero padded, then `min(len, 8)`. This is
/// monotone in byte order: a shorter prefix pads with the smallest byte,
/// and the length byte separates a value from its zero-extended neighbours.
fn bytes_prefix(b: &[u8]) -> ([u8; 8], bool) {
    let mut p = [0u8; 8];
    let n = b.len().min(STR_PREFIX);
    p[..n].copy_from_slice(&b[..n]);
    p[STR_PREFIX] = b.len().min(STR_PREFIX + 1) as u8;
    (p, b.len() <= STR_PREFIX)
}

/// The indices `0..n` in sorted order, keeping the first `keep`: by
/// normalized key, then by `tie` (the full comparator, needed only where
/// keys are equal; `None` when equal keys mean equal rows), then by index.
/// This is exactly a stable sort by the full comparator, truncated.
pub(crate) fn order(
    keys: &NormKeys,
    tie: Option<&dyn Fn(usize, usize) -> Ordering>,
    keep: Option<usize>,
) -> Vec<u32> {
    let n = keys.rows;
    let total = |a: &u32, b: &u32| {
        let (a, b) = (*a as usize, *b as usize);
        keys.key(a)
            .cmp(keys.key(b))
            .then_with(|| tie.map_or(Ordering::Equal, |t| t(a, b)))
            .then(a.cmp(&b))
    };
    let mut perm: Vec<u32> = (0..n as u32).collect();
    match keep {
        Some(0) => return Vec::new(),
        Some(k) if k < n => {
            perm.select_nth_unstable_by(k - 1, total);
            perm.truncate(k);
            // Radix needs index order among equal keys (stability).
            if k >= RADIX_MIN_ROWS && keys.width <= RADIX_MAX_KEY_BYTES {
                perm.sort_unstable();
            }
        }
        _ => {}
    }
    if perm.len() >= RADIX_MIN_ROWS && keys.width <= RADIX_MAX_KEY_BYTES {
        radix_sort(keys, &mut perm);
        if let Some(t) = tie {
            // Stable: equal-key runs are in index order; sort each by the
            // full comparator, keeping index order among its ties.
            let mut start = 0;
            while start < perm.len() {
                let k = keys.key(perm[start] as usize);
                let mut end = start + 1;
                while end < perm.len() && keys.key(perm[end] as usize) == k {
                    end += 1;
                }
                if end - start > 1 {
                    perm[start..end].sort_by(|a, b| t(*a as usize, *b as usize));
                }
                start = end;
            }
        }
    } else {
        perm.sort_unstable_by(total);
    }
    perm
}

/// Stable LSD radix sort of `perm` by the compared key bytes, skipping
/// digits that are the same in every key (tag bytes, high timestamp bytes).
fn radix_sort(keys: &NormKeys, perm: &mut Vec<u32>) {
    let (w, n) = (keys.width, perm.len());
    let mut hist = vec![[0u32; 256]; w];
    for &i in perm.iter() {
        for (h, b) in hist.iter_mut().zip(keys.key(i as usize)) {
            h[usize::from(*b)] += 1;
        }
    }
    let mut tmp = vec![0u32; n];
    for d in (0..w).rev() {
        let h = &hist[d];
        if h.iter().any(|c| *c as usize == n) {
            continue; // one value: this digit does not reorder anything
        }
        let mut offs = [0u32; 256];
        let mut sum = 0u32;
        for (o, c) in offs.iter_mut().zip(h) {
            *o = sum;
            sum += c;
        }
        for &i in perm.iter() {
            let b = usize::from(keys.bytes[i as usize * keys.stride + d]);
            tmp[offs[b] as usize] = i;
            offs[b] += 1;
        }
        std::mem::swap(perm, &mut tmp);
    }
}

/// Merge sorted `runs` into one sorted sequence, keeping the first `keep`.
/// A loser tree (spec STRATA-SYS-14-001) over the runs' heads; ties go to
/// the earlier run, so the result equals a stable sort of the runs'
/// concatenation by `cmp`.
pub(crate) fn merge_runs<T>(
    runs: Vec<Vec<T>>,
    keep: Option<usize>,
    cmp: impl Fn(&T, &T) -> Ordering,
) -> Vec<T> {
    let total: usize = runs.iter().map(Vec::len).sum();
    let want = keep.map_or(total, |k| k.min(total));
    let mut runs: Vec<Vec<T>> = runs.into_iter().filter(|r| !r.is_empty()).collect();
    match runs.len() {
        0 => return Vec::new(),
        1 => {
            let mut only = runs.swap_remove(0);
            only.truncate(want);
            return only;
        }
        _ => {}
    }
    let mut tree = LoserTree::new(runs, cmp);
    let mut out = Vec::with_capacity(want);
    while out.len() < want {
        match tree.pop() {
            Some(x) => out.push(x),
            None => break,
        }
    }
    out
}

/// A tree of losers over `k` sources: nodes `1..k` hold the loser of their
/// subtree's match, `tree[0]` the overall winner; leaf `i` is node `k + i`.
/// One pop replays a single leaf-to-root path: `log2 k` comparisons.
struct LoserTree<T, F> {
    heads: Vec<Option<T>>,
    rest: Vec<std::vec::IntoIter<T>>,
    tree: Vec<usize>,
    cmp: F,
}

impl<T, F: Fn(&T, &T) -> Ordering> LoserTree<T, F> {
    fn new(runs: Vec<Vec<T>>, cmp: F) -> Self {
        let k = runs.len();
        let mut rest: Vec<std::vec::IntoIter<T>> = runs.into_iter().map(Vec::into_iter).collect();
        let heads = rest.iter_mut().map(Iterator::next).collect();
        let mut t = LoserTree {
            heads,
            rest,
            tree: vec![0; k],
            cmp,
        };
        let w = t.build(1);
        t.tree[0] = w;
        t
    }

    /// Whether source `a`'s head goes before source `b`'s: an exhausted
    /// source always loses; ties go to the earlier source.
    fn beats(&self, a: usize, b: usize) -> bool {
        match (&self.heads[a], &self.heads[b]) {
            (Some(x), Some(y)) => match (self.cmp)(x, y) {
                Ordering::Less => true,
                Ordering::Greater => false,
                Ordering::Equal => a < b,
            },
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => a < b,
        }
    }

    /// Play the matches below node `x`; returns its winner.
    fn build(&mut self, x: usize) -> usize {
        let k = self.heads.len();
        if x >= k {
            return x - k;
        }
        let (a, b) = (self.build(2 * x), self.build(2 * x + 1));
        if self.beats(a, b) {
            self.tree[x] = b;
            a
        } else {
            self.tree[x] = a;
            b
        }
    }

    fn pop(&mut self) -> Option<T> {
        let k = self.heads.len();
        let w = self.tree[0];
        let out = self.heads[w].take()?;
        self.heads[w] = self.rest[w].next();
        let mut winner = w;
        let mut x = (w + k) / 2;
        while x >= 1 {
            if self.beats(self.tree[x], winner) {
                std::mem::swap(&mut self.tree[x], &mut winner);
            }
            x /= 2;
        }
        self.tree[0] = winner;
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

    use super::*;
    use crate::eval::sort_cmp;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn value() -> impl Strategy<Value = Datum> {
        let leaf = prop_oneof![
            Just(None),
            Just(Some(Value::Null)),
            any::<bool>().prop_map(|b| Some(Value::Bool(b))),
            (-5i64..5).prop_map(|x| Some(Value::Int(x))),
            any::<i64>().prop_map(|x| Some(Value::Int(x))),
            // Around 2^53 and 2^63, where `as f64` rounds.
            (0i64..4).prop_map(|d| Some(Value::Int((1 << 53) + d))),
            (0u64..4).prop_map(|d| Some(Value::UInt(u64::MAX - d))),
            any::<u64>().prop_map(|x| Some(Value::UInt(x))),
            prop_oneof![
                Just(0.0),
                Just(-0.0),
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(f64::NEG_INFINITY),
                Just(9_007_199_254_740_992.0),
                Just(9.223_372_036_854_776e18),
                Just(1.5),
                Just(-2.0),
                any::<f64>(),
            ]
            .prop_map(|f| Some(Value::Float(f))),
            (-3i64..3).prop_map(|t| Some(Value::Timestamp(t))),
            any::<i64>().prop_map(|t| Some(Value::Timestamp(t))),
            "[a\u{0}b]{0,10}".prop_map(|s| Some(Value::String(s))),
            proptest::collection::vec(0u8..3, 0..10).prop_map(|b| Some(Value::Bytes(b))),
            (0i64..3).prop_map(|x| Some(Value::List(vec![Value::Int(x)]))),
            (0i64..3).prop_map(|x| {
                let mut m = BTreeMap::new();
                m.insert("k".to_owned(), Value::Int(x));
                Some(Value::Map(m))
            }),
        ];
        leaf
    }

    fn full(desc: &[bool], a: &[Datum], b: &[Datum]) -> Ordering {
        for (j, d) in desc.iter().enumerate() {
            let o = sort_cmp(&a[j], &b[j]);
            let o = if *d { o.reverse() } else { o };
            if o.is_ne() {
                return o;
            }
        }
        Ordering::Equal
    }

    fn check(rows: &[Vec<Datum>], desc: &[bool], keep: Option<usize>) {
        let keys = NormKeys::build(rows, desc, |r| r.as_slice());
        let tie = |a: usize, b: usize| full(desc, &rows[a], &rows[b]);
        let got = order(&keys, Some(&tie), keep);
        let mut want: Vec<u32> = (0..rows.len() as u32).collect();
        want.sort_by(|a, b| full(desc, &rows[*a as usize], &rows[*b as usize]));
        if let Some(k) = keep {
            want.truncate(k);
        }
        assert_eq!(got, want);
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 300, ..ProptestConfig::default() })]

        /// Prefixes never contradict the full order.
        #[test]
        fn prefixes_agree_with_the_full_order(a in value(), b in value(), desc in any::<bool>()) {
            let (mut pa, mut pb) = ([0u8; COLUMN_BYTES], [0u8; COLUMN_BYTES]);
            let ea = encode(&a, desc, Skip::default(), &mut pa);
            let eb = encode(&b, desc, Skip::default(), &mut pb);
            let o = sort_cmp(&a, &b);
            let o = if desc { o.reverse() } else { o };
            match pa.cmp(&pb) {
                Ordering::Less => prop_assert_eq!(o, Ordering::Less, "{:?} {:?}", a, b),
                Ordering::Greater => prop_assert_eq!(o, Ordering::Greater, "{:?} {:?}", a, b),
                Ordering::Equal => if ea && eb {
                    prop_assert_eq!(o, Ordering::Equal, "{:?} {:?}", a, b);
                },
            }
        }

        /// The sort equals a stable sort by the full comparator, whole or
        /// truncated, for any mix of types and directions.
        #[test]
        fn order_equals_a_stable_sort(
            rows in proptest::collection::vec(proptest::collection::vec(value(), 2), 0..60),
            desc in proptest::collection::vec(any::<bool>(), 2),
            keep in proptest::option::of(0usize..70),
        ) {
            check(&rows, &desc, keep);
        }

        /// The loser tree merge equals a stable sort of the concatenation.
        #[test]
        fn merging_runs_equals_a_stable_sort(
            runs in proptest::collection::vec(proptest::collection::vec((0u8..6, any::<u16>()), 0..20), 0..9),
            keep in proptest::option::of(0usize..120),
        ) {
            let sorted: Vec<Vec<(u8, u16)>> = runs.iter().map(|r| {
                let mut r = r.clone();
                r.sort_by_key(|x| x.0);
                r
            }).collect();
            let mut want: Vec<(u8, u16)> = sorted.iter().flatten().copied().collect();
            want.sort_by_key(|x| x.0);
            if let Some(k) = keep {
                want.truncate(k);
            }
            let got = merge_runs(sorted, keep, |a, b| a.0.cmp(&b.0));
            prop_assert_eq!(got, want);
        }
    }

    /// The radix path (n >= RADIX_MIN_ROWS, one short column) with ties,
    /// truncated strings and both directions.
    #[test]
    fn radix_sorts_large_inputs_exactly() {
        let mut s = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        for kind in 0..4 {
            let rows: Vec<Vec<Datum>> = (0..RADIX_MIN_ROWS * 3)
                .map(|_| {
                    let r = next();
                    vec![match kind {
                        0 => Some(Value::Timestamp((r % 5000) as i64 - 2500)),
                        1 => Some(Value::String(format!("svc-{:03}-{}", r % 300, r % 7))),
                        2 => Some(Value::Int((r % 2000) as i64 - 1000)),
                        _ => match r % 5 {
                            0 => None,
                            1 => Some(Value::Null),
                            2 => Some(Value::Float((r % 100) as f64 / 3.0)),
                            _ => Some(Value::String(format!("{}", r % 50))),
                        },
                    }]
                })
                .collect();
            for desc in [false, true] {
                let keys = NormKeys::build(&rows, &[desc], |r| r.as_slice());
                assert!(keys.width <= RADIX_MAX_KEY_BYTES);
                check(&rows, &[desc], None);
                check(&rows, &[desc], Some(100));
            }
        }
    }

    /// Boundary values: rounding of large integers, signed zeros, NaN,
    /// infinities, and strings around the prefix length.
    fn tricky() -> Vec<Datum> {
        let mut v: Vec<Datum> = vec![None, Some(Value::Null)];
        let big = 1i64 << 53;
        for x in [
            0,
            1,
            -1,
            big - 1,
            big,
            big + 1,
            big + 2,
            -big - 1,
            i64::MAX,
            i64::MAX - 1,
            i64::MIN,
        ] {
            v.push(Some(Value::Int(x)));
        }
        for x in [0, (1u64 << 53) + 1, u64::MAX, u64::MAX - 1, 1u64 << 63] {
            v.push(Some(Value::UInt(x)));
        }
        for f in [
            0.0,
            -0.0,
            0.5,
            -0.5,
            big as f64,
            9.223_372_036_854_776e18,
            1.844_674_407_370_955_2e19,
            f64::MAX,
            f64::MIN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::NAN,
        ] {
            v.push(Some(Value::Float(f)));
        }
        for s in [
            "",
            "a",
            "a\u{0}",
            "abcdefg",
            "abcdefg\u{0}",
            "abcdefga",
            "abcdefgb",
            "abcdefgab",
            "abcdefgb\u{0}",
            "abcdefh",
        ] {
            v.push(Some(Value::String(s.to_owned())));
            v.push(Some(Value::Bytes(s.as_bytes().to_vec())));
        }
        v.push(Some(Value::Bool(false)));
        v.push(Some(Value::Bool(true)));
        v.push(Some(Value::Timestamp(i64::MIN)));
        v.push(Some(Value::Timestamp(-1)));
        v.push(Some(Value::Timestamp(0)));
        v.push(Some(Value::Timestamp(i64::MAX)));
        v
    }

    /// Every pair of boundary values: prefixes agree with the full order,
    /// and exact prefixes decide it.
    #[test]
    fn boundary_values_pairwise() {
        let v = tricky();
        for desc in [false, true] {
            for a in &v {
                for b in &v {
                    let (mut pa, mut pb) = ([0u8; COLUMN_BYTES], [0u8; COLUMN_BYTES]);
                    let (ea, eb) = (
                        encode(a, desc, Skip::default(), &mut pa),
                        encode(b, desc, Skip::default(), &mut pb),
                    );
                    let exact = ea && eb;
                    let o = sort_cmp(a, b);
                    let o = if desc { o.reverse() } else { o };
                    let p = pa.cmp(&pb);
                    assert!(p == o || p.is_eq(), "{a:?} {b:?} desc={desc}");
                    assert!(!(exact && p.is_eq()) || o.is_eq(), "{a:?} {b:?} exact");
                }
            }
            // And sorting all of them (with duplicates) matches.
            let rows: Vec<Vec<Datum>> = v
                .iter()
                .chain(v.iter().rev())
                .map(|d| vec![d.clone()])
                .collect();
            check(&rows, &[desc], None);
        }
    }

    /// Values sharing a long prefix get exact keys from their distinct tail,
    /// and the order is still the full one (including values that are a
    /// prefix of the shared part's extension, and non-string values).
    #[test]
    fn common_prefixes_are_left_out() {
        let mut rows: Vec<Vec<Datum>> = (0..300)
            .map(|i| {
                vec![Some(Value::String(format!(
                    "GET /api/v1/items/{}",
                    (i * 7919) % 1000
                )))]
            })
            .collect();
        let k = NormKeys::build(&rows, &[false], |r| r.as_slice());
        assert!(k.exact(), "tails of at most 3 digits are exact");
        check(&rows, &[false], None);
        check(&rows, &[true], Some(50));
        rows.push(vec![Some(Value::String("GET /api/v1/items/".into()))]);
        rows.push(vec![Some(Value::String("GET /api".into()))]);
        rows.push(vec![Some(Value::Int(3))]);
        rows.push(vec![None]);
        rows.push(vec![Some(Value::Bytes(b"GET /api/v1/items/9".to_vec()))]);
        check(&rows, &[false], None);
        check(&rows, &[true], None);
    }

    #[test]
    fn exactness_and_width() {
        let rows = vec![
            vec![Some(Value::Int(1)), Some(Value::String("short".into()))],
            vec![
                Some(Value::Int(2)),
                Some(Value::String("a much longer value".into())),
            ],
        ];
        let k = NormKeys::build(&rows, &[false, false], |r| r.as_slice());
        assert!(!k.exact());
        assert_eq!(k.width, 2 * COLUMN_BYTES);
        let rows = vec![
            vec![
                Some(Value::String("a much longer value".into())),
                Some(Value::Int(1)),
            ],
            vec![
                Some(Value::String("b much longer value".into())),
                Some(Value::Int(2)),
            ],
        ];
        let k = NormKeys::build(&rows, &[false, false], |r| r.as_slice());
        assert_eq!(
            k.width, COLUMN_BYTES,
            "columns after an inexact one are not compared"
        );
        let none: Vec<Vec<Datum>> = vec![vec![], vec![]];
        let k = NormKeys::build(&none, &[], |r| r.as_slice());
        assert_eq!(order(&k, None, None), vec![0, 1]);
    }
}
