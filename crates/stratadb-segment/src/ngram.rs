//! Per-segment trigram filters for substring search (spec §12.4, §14.1;
//! ticket F-005). Footer extension tag 3 (optional: readers that do not know
//! it ignore it).
//!
//! Purpose: prune segments for `LIKE` / `ILIKE` predicates. A token (word)
//! index cannot do this soundly — `'%TIMEOUT%'` must match inside `TIMEOUTS` —
//! but a trigram index can: if a string contains the literal run `r`, it
//! contains every 3-character window of `r`. A segment whose filter lacks any
//! window of any literal run of the pattern holds no matching row.
//!
//! * Characters are Unicode scalar values folded with the `ILIKE` rule
//!   (`c.to_lowercase().next()`), applied per character, so one index serves
//!   case-sensitive and case-insensitive patterns (folding only adds false
//!   positives, never false negatives). No Unicode normalization (matching is
//!   on code points, as in the `LIKE` evaluator).
//! * Only string values are indexed (`LIKE` on any other type is unknown).
//! * Windows are stored in a [`Bloom`] (hash v1, k = 7, ≈1 % false positives
//!   per probe). A field with more than [`MAX_GRAMS`] distinct trigrams gets no
//!   filter (no pruning, never wrong results).
//!
//! ```text
//! u8 version (=1) | uvarint nfilters | per filter:
//!   u8 kind (1 body, 2 attribute, 3 resource attribute) [bytes key — kinds 2, 3]
//!   u8 k | uvarint nbits | bytes bitset (⌈nbits/8⌉)
//! ```

use std::collections::{BTreeMap, HashSet};

use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{Error, Result};
use stratadb_schema::Value;

use crate::bloom::Bloom;
use crate::{Extension, RowRef};

pub const EXT_NGRAM: u16 = 3;
const VERSION: u8 = 1;
/// Window length.
pub const N: usize = 3;
pub const MAX_GRAMS: usize = 2_000_000;

/// A field a trigram filter can cover.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NgramField {
    Body,
    Attr(String),
    Resource(String),
}

impl NgramField {
    pub fn describe(&self) -> String {
        match self {
            NgramField::Body => "ngram(body)".into(),
            NgramField::Attr(k) => format!("ngram(attributes['{k}'])"),
            NgramField::Resource(k) => format!("ngram(resource_attributes['{k}'])"),
        }
    }
}

/// The `ILIKE` case fold, per character.
pub fn fold(c: char) -> char {
    c.to_lowercase().next().unwrap_or(c)
}

/// Every folded trigram of `s`, as a 3-character string.
pub fn grams(s: &str) -> impl Iterator<Item = String> + '_ {
    windows(s).map(|w| w.iter().collect())
}

/// Every folded trigram of `s` as a character window (no allocation per
/// window).
fn windows(s: &str) -> impl Iterator<Item = [char; N]> + '_ {
    let mut it = s.chars().map(fold);
    let mut w = ['\0'; N];
    let mut filled = 0;
    std::iter::from_fn(move || loop {
        let c = it.next()?;
        w.rotate_left(1);
        w[N - 1] = c;
        filled += 1;
        if filled >= N {
            return Some(w);
        }
    })
}

/// Insert a window into a filter, hashing its UTF-8 bytes exactly as
/// `Bloom::insert(&String)` would (stack buffer, no allocation).
fn insert_window(b: &mut Bloom, w: &[char; N]) {
    let mut buf = [0u8; 4 * N];
    let mut len = 0;
    for c in w {
        len += c.encode_utf8(&mut buf[len..]).len();
    }
    // The bytes are valid UTF-8 by construction.
    if let Ok(s) = std::str::from_utf8(&buf[..len]) {
        b.insert(s);
    }
}

/// Literal runs of a `LIKE` pattern (text between `%` / `_` wildcards, with
/// `\` escapes resolved) that are at least [`N`] characters long.
pub fn literal_runs(pattern: &str) -> Vec<String> {
    let mut runs = Vec::new();
    let mut cur = String::new();
    let mut it = pattern.chars();
    while let Some(c) = it.next() {
        match c {
            '%' | '_' => {
                if cur.chars().count() >= N {
                    runs.push(std::mem::take(&mut cur));
                }
                cur.clear();
            }
            '\\' => cur.push(it.next().unwrap_or('\\')),
            c => cur.push(c),
        }
    }
    if cur.chars().count() >= N {
        runs.push(cur);
    }
    runs
}

/// The probes a `LIKE` pattern implies: every folded trigram of every literal
/// run. Empty when the pattern has no run of [`N`]+ characters (no pruning).
pub fn probes(pattern: &str) -> Vec<String> {
    let mut out: Vec<String> = literal_runs(pattern)
        .iter()
        .flat_map(|r| grams(r).collect::<Vec<_>>())
        .collect();
    out.sort();
    out.dedup();
    out
}

/// All trigram filters of one segment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NgramSet {
    filters: BTreeMap<NgramField, Bloom>,
}

impl NgramSet {
    pub fn get(&self, f: &NgramField) -> Option<&Bloom> {
        self.filters.get(f)
    }

    pub fn size_bytes(&self) -> usize {
        self.filters.values().map(Bloom::size_bytes).sum()
    }

    pub fn fields(&self) -> impl Iterator<Item = &NgramField> {
        self.filters.keys()
    }

    /// Whether the field may contain a string with all of `probes`
    /// (`true` when the field has no filter).
    pub fn may_match(&self, f: &NgramField, probes: &[String]) -> bool {
        match self.filters.get(f) {
            Some(b) => probes.iter().all(|g| b.may_contain(g)),
            None => true,
        }
    }

    /// Build filters for `body` (if `body`) and the given attribute keys.
    pub fn build(
        rows: &[RowRef<'_>],
        body: bool,
        attr_keys: &[String],
        resource_keys: &[String],
    ) -> Self {
        type Sets = BTreeMap<NgramField, HashSet<[char; N]>>;
        let mut sets: Sets = BTreeMap::new();
        let add = |f: NgramField, s: &str, sets: &mut Sets| {
            let set = sets.entry(f).or_default();
            if set.len() <= MAX_GRAMS {
                set.extend(windows(s));
            }
        };
        if body {
            sets.insert(NgramField::Body, HashSet::new());
        }
        for k in attr_keys {
            sets.insert(NgramField::Attr(k.clone()), HashSet::new());
        }
        for k in resource_keys {
            sets.insert(NgramField::Resource(k.clone()), HashSet::new());
        }
        for r in rows {
            let e = r.event;
            if body {
                if let Some(Value::String(s)) = &e.body {
                    add(NgramField::Body, s, &mut sets);
                }
            }
            for k in attr_keys {
                if let Some(Value::String(s)) = e.attributes.get(k) {
                    add(NgramField::Attr(k.clone()), s, &mut sets);
                }
            }
            for k in resource_keys {
                if let Some(Value::String(s)) = e.resource_attributes.get(k) {
                    add(NgramField::Resource(k.clone()), s, &mut sets);
                }
            }
        }
        let mut filters = BTreeMap::new();
        for (f, set) in sets {
            if set.len() > MAX_GRAMS {
                continue;
            }
            let mut b = Bloom::with_capacity(set.len());
            for w in &set {
                insert_window(&mut b, w);
            }
            filters.insert(f, b);
        }
        Self { filters }
    }

    /// Fields whose filter lacks a trigram of a string in `rows` (a false
    /// negative would make `LIKE` skip matching data). Empty = sound.
    pub fn false_negatives(&self, rows: &[RowRef<'_>]) -> Vec<String> {
        let mut bad = std::collections::BTreeSet::new();
        for r in rows {
            let e = r.event;
            for (f, b) in &self.filters {
                let v = match f {
                    NgramField::Body => match &e.body {
                        Some(Value::String(s)) => Some(s.as_str()),
                        _ => None,
                    },
                    NgramField::Attr(k) => e.attributes.get(k).and_then(Value::as_str),
                    NgramField::Resource(k) => e.resource_attributes.get(k).and_then(Value::as_str),
                };
                if let Some(s) = v {
                    if grams(s).any(|g| !b.may_contain(&g)) {
                        bad.insert(f.describe());
                    }
                }
            }
        }
        bad.into_iter().collect()
    }

    pub fn to_extension(&self) -> Extension {
        let mut e = Encoder::new();
        e.put_u8(VERSION);
        e.put_uvarint(self.filters.len() as u64);
        for (f, b) in &self.filters {
            match f {
                NgramField::Body => e.put_u8(1),
                NgramField::Attr(k) => {
                    e.put_u8(2);
                    e.put_str(k);
                }
                NgramField::Resource(k) => {
                    e.put_u8(3);
                    e.put_str(k);
                }
            }
            b.encode_into(&mut e);
        }
        Extension {
            tag: EXT_NGRAM,
            required: false,
            data: e.into_inner(),
        }
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        let mut d = Decoder::new(data, "ngram extension");
        if d.get_u8()? != VERSION {
            return Err(Error::unsupported("unknown ngram filter version"));
        }
        let n = d.get_len(1024, 3)?;
        let mut filters = BTreeMap::new();
        for _ in 0..n {
            let f = match d.get_u8()? {
                1 => NgramField::Body,
                2 => NgramField::Attr(d.get_string(1024)?),
                3 => NgramField::Resource(d.get_string(1024)?),
                _ => return Err(Error::corruption("ngram extension", "unknown field kind")),
            };
            filters.insert(f, Bloom::decode_from(&mut d, data.len())?);
        }
        d.expect_end()?;
        Ok(Self { filters })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;
    use stratadb_common::TenantId;
    use stratadb_schema::Event;

    fn set_for(bodies: &[&str]) -> NgramSet {
        let t = TenantId::new("t").unwrap();
        let evs: Vec<Event> = bodies
            .iter()
            .map(|b| {
                let mut e = Event::new(t.clone(), 1, 1);
                e.body = Some(Value::from(*b));
                e
            })
            .collect();
        let rows: Vec<RowRef<'_>> = evs
            .iter()
            .enumerate()
            .map(|(i, e)| RowRef {
                seq: 1,
                idx: i as u32,
                event: e,
            })
            .collect();
        NgramSet::build(&rows, true, &[], &[])
    }

    #[test]
    fn windows_match_string_grams() {
        for s in ["", "ab", "abc", "AbcDé", "x\u{130}yz"] {
            let a: Vec<String> = windows(s).map(|w| w.iter().collect()).collect();
            let b: Vec<String> = {
                let c: Vec<char> = s.chars().map(fold).collect();
                c.windows(N).map(|w| w.iter().collect()).collect()
            };
            assert_eq!(a, b, "{s}");
        }
        let mut x = Bloom::with_capacity(4);
        insert_window(&mut x, &['a', 'é', 'c']);
        assert!(x.may_contain("aéc"));
    }

    #[test]
    fn runs_and_probes() {
        assert_eq!(literal_runs("%TIMEOUT%"), vec!["TIMEOUT"]);
        assert_eq!(literal_runs("ab%cd_efg\\%h%"), vec!["efg%h"]);
        assert!(literal_runs("%a_b%").is_empty());
        assert_eq!(probes("%Abcd%"), vec!["abc", "bcd"]);
        assert!(probes("%%").is_empty());
    }

    #[test]
    fn prunes_absent_substrings_and_round_trips() {
        let s = set_for(&["Payment authorization TIMEOUTS", "ok 42"]);
        let back = NgramSet::decode(&s.to_extension().data).unwrap();
        assert_eq!(back, s);
        let f = NgramField::Body;
        assert!(s.may_match(&f, &probes("%TIMEOUT%")));
        assert!(s.may_match(&f, &probes("%timeout%"))); // folded index
        assert!(!s.may_match(&f, &probes("%deadlock%")));
        assert!(s.may_match(&NgramField::Attr("x".into()), &probes("%zzz%")));
    }

    proptest! {
        /// No false negatives: whenever the evaluator says a body matches,
        /// the segment's filter must not exclude the pattern.
        #[test]
        fn never_excludes_a_matching_segment(
            bodies in proptest::collection::vec("[aAbB\u{e9}\u{c9}%_ ]{0,12}", 1..6),
            pattern in "[aAbB\u{e9}\u{c9}%_\\\\]{0,8}",
            ci in any::<bool>(),
        ) {
            let refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
            let s = set_for(&refs);
            let any_match = bodies.iter().any(|b| {
                stratadb_schema_like(b, &pattern, ci)
            });
            if any_match {
                prop_assert!(s.may_match(&NgramField::Body, &probes(&pattern)));
            }
        }
    }

    /// Reference LIKE (same rules as the query crate's evaluator: `%`, `_`,
    /// `\` escapes, per-character fold for ILIKE), via regex-free DP.
    fn stratadb_schema_like(text: &str, pattern: &str, ci: bool) -> bool {
        #[derive(Clone, Copy, PartialEq)]
        enum P {
            Any,
            One,
            Lit(char),
        }
        let f = |c: char| if ci { fold(c) } else { c };
        let mut pat = Vec::new();
        let mut it = pattern.chars();
        while let Some(c) = it.next() {
            pat.push(match c {
                '%' => P::Any,
                '_' => P::One,
                '\\' => P::Lit(f(it.next().unwrap_or('\\'))),
                c => P::Lit(f(c)),
            });
        }
        let t: Vec<char> = text.chars().map(f).collect();
        let mut dp = vec![vec![false; t.len() + 1]; pat.len() + 1];
        dp[0][0] = true;
        for i in 1..=pat.len() {
            for j in 0..=t.len() {
                dp[i][j] = match pat[i - 1] {
                    P::Any => dp[i - 1][j] || (j > 0 && dp[i][j - 1]),
                    P::One => j > 0 && dp[i - 1][j - 1],
                    P::Lit(c) => j > 0 && t[j - 1] == c && dp[i - 1][j - 1],
                };
            }
        }
        dp[pat.len()][t.len()]
    }
}
