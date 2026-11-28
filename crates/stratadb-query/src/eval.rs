//! Value semantics (normative: docs/query-language/semantics.md).
//!
//! * A [`Datum`] is `None` for *missing* or `Some(Value)`; `Value::Null` is null.
//! * Predicates use three-valued logic: comparisons involving missing/null,
//!   or between incomparable types, are *unknown*; `WHERE` keeps only *true*.
//! * Numbers compare numerically across int/uint/float. Strings, bools,
//!   timestamps and bytes compare only with their own type. Lists and maps are
//!   incomparable (unknown) in predicates.
//! * No implicit coercion between strings and timestamps or numbers.

use std::cmp::Ordering;

use stratadb_common::codec::Encoder;
use stratadb_schema::Value;

pub type Datum = Option<Value>;
/// Three-valued truth: `None` = unknown.
pub type Truth = Option<bool>;

fn num_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    use Value::*;
    match (a, b) {
        (Int(x), Int(y)) => Some(x.cmp(y)),
        (UInt(x), UInt(y)) => Some(x.cmp(y)),
        (Int(x), UInt(y)) => Some(i128::from(*x).cmp(&i128::from(*y))),
        (UInt(x), Int(y)) => Some(i128::from(*x).cmp(&i128::from(*y))),
        (Float(x), Float(y)) => x.partial_cmp(y),
        (Float(x), Int(y)) => x.partial_cmp(&(*y as f64)),
        (Int(x), Float(y)) => (*x as f64).partial_cmp(y),
        (Float(x), UInt(y)) => x.partial_cmp(&(*y as f64)),
        (UInt(x), Float(y)) => (*x as f64).partial_cmp(y),
        _ => None,
    }
}

fn is_num(v: &Value) -> bool {
    matches!(v, Value::Int(_) | Value::UInt(_) | Value::Float(_))
}

/// Predicate comparison. `None` = unknown.
pub fn compare(a: &Datum, b: &Datum) -> Option<Ordering> {
    compare_ref(a.as_ref(), b.as_ref())
}

/// [`compare`] over borrowed values (`None` = missing); avoids cloning.
pub fn compare_ref(a: Option<&Value>, b: Option<&Value>) -> Option<Ordering> {
    let (Some(a), Some(b)) = (a, b) else {
        return None;
    };
    use Value::*;
    match (a, b) {
        (Null, _) | (_, Null) => None,
        _ if is_num(a) && is_num(b) => num_cmp(a, b),
        (String(x), String(y)) => Some(x.cmp(y)),
        (Bool(x), Bool(y)) => Some(x.cmp(y)),
        (Timestamp(x), Timestamp(y)) => Some(x.cmp(y)),
        (Bytes(x), Bytes(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

pub fn and(a: Truth, b: Truth) -> Truth {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

pub fn or(a: Truth, b: Truth) -> Truth {
    match (a, b) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

pub fn not(a: Truth) -> Truth {
    a.map(|v| !v)
}

/// Interpret a datum as a truth value (for boolean columns/attributes).
pub fn truth_of(d: &Datum) -> Truth {
    match d {
        Some(Value::Bool(b)) => Some(*b),
        _ => None,
    }
}

/// SQL LIKE with `%` (any run) and `_` (one character); `\` escapes the next
/// character. See [`LikePattern`].
pub fn like(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    LikePattern::new(pattern, case_insensitive).matches(text, &mut Vec::new())
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum LikeTok {
    Any,
    One,
    Lit(char),
}

/// A compiled LIKE pattern. Matching is a linear-memory greedy matcher with
/// a single backtrack point (O(n·m) worst case; inputs are bounded by
/// validation limits). Case folding maps each character to the first char of
/// its Unicode lowercase mapping.
///
/// Fast paths (identical results, checked against the general matcher by a
/// property test; spec STRATA-SYS-08-013/014/015):
/// - patterns made only of literal characters and `%` at the ends compare
///   lengths before bytes for exact/prefix/suffix and search `%x%` with a
///   prebuilt SIMD substring finder (`memchr::memmem`);
/// - case-insensitive such patterns whose literal is ASCII use ASCII case
///   folding when the text is ASCII (a SIMD scan for the first byte in
///   either case, then a case-insensitive compare). Text with non-ASCII
///   bytes takes the general matcher, which folds Unicode.
#[derive(Debug, Clone)]
pub struct LikePattern {
    toks: Vec<LikeTok>,
    case_insensitive: bool,
    fast: Option<Fast>,
}

/// Where a literal must occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Anchor {
    Exact,
    Prefix,
    Suffix,
    Contains,
}

#[derive(Debug, Clone)]
enum Fast {
    /// Case-sensitive literal (`Contains` uses the prebuilt SIMD finder;
    /// measured at or slightly above std's substring search on short and
    /// long text).
    Sensitive(Anchor, String, memchr::memmem::Finder<'static>),
    /// Case-insensitive ASCII literal, lowercased (the finder searches a
    /// lowercased copy of the text for `Contains`).
    AsciiFold(Anchor, Vec<u8>, memchr::memmem::Finder<'static>),
}

thread_local! {
    /// Scratch for lowercasing ASCII text before a case-insensitive search.
    static FOLDED: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Text up to this length is searched without a lowercased copy (measured
/// crossover on log bodies: the copy costs more than it saves below it).
const FOLD_COPY_MIN: usize = 256;

/// Whether the lowercase ASCII `finder` needle occurs in ASCII `hay`
/// ignoring case: short text is scanned in place for the needle's first
/// byte in either case; longer text is lowercased into scratch memory
/// (vectorized) and searched with the SIMD finder.
fn contains_folded(hay: &[u8], finder: &memchr::memmem::Finder<'_>) -> bool {
    if hay.len() <= FOLD_COPY_MIN {
        return contains_ascii_fold(hay, finder.needle());
    }
    FOLDED.with(|b| match b.try_borrow_mut() {
        Ok(mut b) => {
            b.clear();
            b.extend_from_slice(hay);
            b.make_ascii_lowercase();
            finder.find(&b).is_some()
        }
        // Re-entered on this thread (not expected): search without scratch.
        Err(_) => contains_ascii_fold(hay, finder.needle()),
    })
}

/// Whether `needle` (lowercase ASCII) occurs in `hay` ignoring ASCII case,
/// without scratch memory (a scan for the needle's first byte in either
/// case, then a case-insensitive compare).
fn contains_ascii_fold(hay: &[u8], needle: &[u8]) -> bool {
    let Some((&first, rest)) = needle.split_first() else {
        return true;
    };
    if hay.len() < needle.len() {
        return false;
    }
    let last_start = hay.len() - needle.len();
    let mut from = 0;
    while from <= last_start {
        let window = &hay[from..=last_start];
        let Some(off) = memchr::memchr2(first, first.to_ascii_uppercase(), window) else {
            return false;
        };
        let at = from + off;
        if hay[at + 1..at + needle.len()].eq_ignore_ascii_case(rest) {
            return true;
        }
        from = at + 1;
    }
    false
}

impl LikePattern {
    pub fn new(pattern: &str, case_insensitive: bool) -> Self {
        let fold = |c: char| {
            if case_insensitive {
                c.to_lowercase().next().unwrap_or(c)
            } else {
                c
            }
        };
        let mut toks = Vec::new();
        let mut it = pattern.chars();
        while let Some(c) = it.next() {
            toks.push(match c {
                '%' => LikeTok::Any,
                '_' => LikeTok::One,
                '\\' => LikeTok::Lit(fold(it.next().unwrap_or('\\'))),
                c => LikeTok::Lit(fold(c)),
            });
        }
        // [%] literal* [%]
        let lead = toks.first() == Some(&LikeTok::Any);
        let trail = toks.len() > usize::from(lead) && toks.last() == Some(&LikeTok::Any);
        let inner = &toks[usize::from(lead)..toks.len() - usize::from(trail)];
        let lit: Option<String> = inner
            .iter()
            .map(|t| match t {
                LikeTok::Lit(c) => Some(*c),
                _ => None,
            })
            .collect();
        let anchor = match (lead, trail) {
            (false, false) => Anchor::Exact,
            (false, true) => Anchor::Prefix,
            (true, false) => Anchor::Suffix,
            (true, true) => Anchor::Contains,
        };
        let fast = lit.and_then(|l| {
            if !case_insensitive {
                let finder = memchr::memmem::Finder::new(l.as_bytes()).into_owned();
                Some(Fast::Sensitive(anchor, l, finder))
            } else if l.is_ascii() {
                // Already folded: lowercase ASCII.
                let finder = memchr::memmem::Finder::new(l.as_bytes()).into_owned();
                Some(Fast::AsciiFold(anchor, l.into_bytes(), finder))
            } else {
                None
            }
        });
        Self {
            toks,
            case_insensitive,
            fast,
        }
    }

    /// Whether `text` matches. `buf` is scratch space reused across rows.
    pub fn matches(&self, text: &str, buf: &mut Vec<char>) -> bool {
        match &self.fast {
            Some(Fast::Sensitive(anchor, lit, finder)) => {
                let (t, l) = (text.as_bytes(), lit.as_bytes());
                return match anchor {
                    Anchor::Exact => t.len() == l.len() && t == l,
                    Anchor::Prefix => t.len() >= l.len() && &t[..l.len()] == l,
                    Anchor::Suffix => t.len() >= l.len() && &t[t.len() - l.len()..] == l,
                    Anchor::Contains => finder.find(t).is_some(),
                };
            }
            Some(Fast::AsciiFold(anchor, l, finder)) if text.is_ascii() => {
                let t = text.as_bytes();
                return match anchor {
                    Anchor::Exact => t.eq_ignore_ascii_case(l),
                    Anchor::Prefix => t.len() >= l.len() && t[..l.len()].eq_ignore_ascii_case(l),
                    Anchor::Suffix => {
                        t.len() >= l.len() && t[t.len() - l.len()..].eq_ignore_ascii_case(l)
                    }
                    Anchor::Contains => contains_folded(t, finder),
                };
            }
            _ => {}
        }
        buf.clear();
        if self.case_insensitive {
            buf.extend(text.chars().map(|c| c.to_lowercase().next().unwrap_or(c)));
        } else {
            buf.extend(text.chars());
        }
        let (t, pat) = (&buf[..], &self.toks[..]);
        let (mut ti, mut pi) = (0usize, 0usize);
        let mut star: Option<(usize, usize)> = None;
        while ti < t.len() {
            match pat.get(pi) {
                Some(LikeTok::Any) => {
                    star = Some((pi, ti));
                    pi += 1;
                }
                Some(LikeTok::One) => {
                    ti += 1;
                    pi += 1;
                }
                Some(LikeTok::Lit(c)) if *c == t[ti] => {
                    ti += 1;
                    pi += 1;
                }
                _ => match star {
                    Some((sp, st)) => {
                        pi = sp + 1;
                        ti = st + 1;
                        star = Some((sp, st + 1));
                    }
                    None => return false,
                },
            }
        }
        pat[pi..].iter().all(|p| *p == LikeTok::Any)
    }
}

fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Bool(_) => 0,
        Value::Int(_) | Value::UInt(_) | Value::Float(_) => 1,
        Value::Timestamp(_) => 2,
        Value::String(_) => 3,
        Value::Bytes(_) => 4,
        Value::List(_) => 5,
        Value::Map(_) => 6,
        Value::Null => 7,
    }
}

/// Total order for ORDER BY: values by type rank then value; NaN after
/// numbers; null, then missing, sort last ascending (first descending).
pub fn sort_cmp(a: &Datum, b: &Datum) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Greater,
        (_, None) => Ordering::Less,
        (Some(x), Some(y)) => {
            let (rx, ry) = (type_rank(x), type_rank(y));
            if rx != ry {
                return rx.cmp(&ry);
            }
            match (x, y) {
                (Value::List(_), Value::List(_)) | (Value::Map(_), Value::Map(_)) => {
                    group_key_bytes(a).cmp(&group_key_bytes(b))
                }
                _ => if is_num(x) && is_num(y) {
                    exact_num_cmp(x, y)
                } else {
                    compare(a, b)
                }
                .unwrap_or_else(|| {
                    // NaN or Null vs Null: order deterministically.
                    let nan = |v: &Value| matches!(v, Value::Float(f) if f.is_nan());
                    nan(x).cmp(&nan(y))
                }),
            }
        }
    }
}

/// Exact numeric order for sorting: integers and floats compare as the
/// numbers they denote, with no rounding, so the order is total and
/// transitive (`Int(2^53 + 1)` sorts after `Float(2^53)`, unlike the
/// predicate comparison's `as f64`). `None` only for NaN.
fn exact_num_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    let int = |v: &Value| match v {
        Value::Int(x) => Some(i128::from(*x)),
        Value::UInt(x) => Some(i128::from(*x)),
        _ => None,
    };
    match (a, b) {
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Float(f), o) => int_float_cmp(int(o)?, *f).map(Ordering::reverse),
        (o, Value::Float(f)) => int_float_cmp(int(o)?, *f),
        _ => Some(int(a)?.cmp(&int(b)?)),
    }
}

/// `i` against `f` exactly. `f.trunc()` is an integer that `as i128`
/// converts exactly, saturating only far outside any `i64`/`u64`.
fn int_float_cmp(i: i128, f: f64) -> Option<Ordering> {
    if f.is_nan() {
        return None;
    }
    let t = f.trunc();
    match i.cmp(&(t as i128)) {
        // Same integer part: `i < f` exactly when `f` has a positive fraction.
        Ordering::Equal => 0.0f64.partial_cmp(&(f - t)),
        o => Some(o),
    }
}

/// Group-key byte for a missing value (no value encoding starts with it).
pub const MISSING_KEY_TAG: u8 = 0xff;

/// Canonical bytes for grouping: equal datums ⇔ equal bytes. Missing and
/// null are distinct groups.
pub fn group_key_bytes(d: &Datum) -> Vec<u8> {
    let mut e = Encoder::new();
    match d {
        None => e.put_u8(MISSING_KEY_TAG),
        Some(v) => stratadb_schema::codec::encode_value(&mut e, v),
    }
    e.into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_valued_logic() {
        assert_eq!(and(Some(true), None), None);
        assert_eq!(and(Some(false), None), Some(false));
        assert_eq!(or(None, Some(true)), Some(true));
        assert_eq!(not(None), None);
    }

    #[test]
    fn comparisons() {
        let i = |x| Some(Value::Int(x));
        assert_eq!(
            compare(&i(1), &Some(Value::Float(1.5))),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare(&i(-1), &Some(Value::UInt(u64::MAX))),
            Some(Ordering::Less)
        );
        assert_eq!(compare(&i(1), &Some(Value::from("1"))), None);
        assert_eq!(compare(&i(1), &None), None);
        assert_eq!(compare(&i(1), &Some(Value::Null)), None);
        assert_eq!(compare(&Some(Value::Timestamp(5)), &i(5)), None);
    }

    proptest::proptest! {
        /// The `str`-search fast path agrees with the general matcher.
        #[test]
        fn like_fast_path_equals_general(
            text in "[abAB%_\\\\éÉKk\u{212A}]{0,12}",
            pattern in "[abAB%_\\\\éÉkK]{0,6}",
            ci in proptest::bool::ANY,
        ) {
            let p = LikePattern::new(&pattern, ci);
            let mut general = p.clone();
            general.fast = None;
            let mut buf = Vec::new();
            proptest::prop_assert_eq!(p.matches(&text, &mut buf), general.matches(&text, &mut buf));
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(2000))]
        /// The SIMD substring and ASCII-folding searches agree with the
        /// general matcher on longer texts with many partial matches.
        #[test]
        fn substring_search_equals_general(
            text in "[abcABC ]{0,48}",
            lit in "[abcABC]{1,5}",
            lead in proptest::bool::ANY,
            trail in proptest::bool::ANY,
            ci in proptest::bool::ANY,
        ) {
            let pattern = format!("{}{lit}{}", if lead { "%" } else { "" }, if trail { "%" } else { "" });
            let p = LikePattern::new(&pattern, ci);
            proptest::prop_assert!(p.fast.is_some());
            let mut general = p.clone();
            general.fast = None;
            let mut buf = Vec::new();
            proptest::prop_assert_eq!(p.matches(&text, &mut buf), general.matches(&text, &mut buf), "{} ~ {}", text, pattern);
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(300))]
        /// Long texts take the lowercased-copy path (> FOLD_COPY_MIN).
        #[test]
        fn long_text_folded_search_equals_general(
            text in "[abcABC ]{250,320}",
            lit in "[abcABC]{2,6}",
        ) {
            let p = LikePattern::new(&format!("%{lit}%"), true);
            let mut general = p.clone();
            general.fast = None;
            let mut buf = Vec::new();
            proptest::prop_assert_eq!(p.matches(&text, &mut buf), general.matches(&text, &mut buf));
        }
    }

    #[test]
    fn ascii_folding_edges() {
        assert!(contains_ascii_fold(b"xxAbC", b"abc"));
        assert!(contains_ascii_fold(b"ABC", b"abc"));
        assert!(!contains_ascii_fold(b"AB", b"abc"));
        assert!(contains_ascii_fold(b"", b""));
        assert!(
            contains_ascii_fold(b"aaab", b"aab"),
            "overlapping candidates"
        );
        // Non-ASCII text goes to the general (Unicode) matcher: the Kelvin
        // sign folds to 'k'.
        assert!(like("\u{212A}elvin", "%kelvin%", true));
        assert!(like("STRASSE", "%strasse", true));
        assert!(!like("Straße", "%strasse", true));
    }

    /// Substring-search throughput on 1 KiB ASCII strings (spec
    /// STRATA-SYS-08-014: >= 4 GB/s/core). Run with `cargo test --release -p
    /// stratadb-query eval::tests::substring_throughput -- --ignored
    /// --nocapture`.
    #[test]
    #[ignore = "timing measurement"]
    fn substring_throughput() {
        let rows: Vec<String> = (0..4096u32)
            .map(|i| {
                let mut s = String::with_capacity(1024);
                while s.len() < 1000 {
                    s.push_str(&format!(
                        "request {i} served by node-{} in {} ms; ",
                        i % 97,
                        i % 13
                    ));
                }
                if i % 50 == 0 {
                    s.push_str("TIMEOUT");
                }
                s
            })
            .collect();
        let bytes: usize = rows.iter().map(String::len).sum();
        let run = |name: &str, f: &dyn Fn(&str) -> bool| {
            let reps = 50;
            let t = std::time::Instant::now();
            let mut hits = 0usize;
            for _ in 0..reps {
                hits += rows.iter().filter(|r| f(std::hint::black_box(r))).count();
            }
            let gbs = (bytes * reps) as f64 / t.elapsed().as_secs_f64() / 1e9;
            println!("{name:34} {gbs:6.2} GB/s ({hits} hits)");
        };
        let mut buf = Vec::new();
        let like = LikePattern::new("%TIMEOUT%", false);
        let ilike = LikePattern::new("%timeout%", true);
        let mut general = ilike.clone();
        general.fast = None;
        run("str::contains (previous LIKE)", &|t| t.contains("TIMEOUT"));
        run("LIKE '%TIMEOUT%' matches()", &|t| {
            like.matches(t, &mut Vec::new())
        });
        run("ILIKE '%timeout%' (ASCII fold)", &|t| {
            ilike.matches(t, &mut Vec::new())
        });
        let gen = std::cell::RefCell::new(&mut buf);
        run("ILIKE general matcher (previous)", &|t| {
            general.matches(t, &mut gen.borrow_mut())
        });

        // Short log bodies (~45 bytes): per-call overhead dominates.
        let short: Vec<String> = (0..65536u32)
            .map(|i| {
                let mut s = format!("request {i} served by node-{} in {} ms", i % 97, i % 13);
                if i % 50 == 0 {
                    s.push_str(" TIMEOUT");
                }
                s
            })
            .collect();
        let srun = |name: &str, f: &dyn Fn(&str) -> bool| {
            let reps = 50;
            let t = std::time::Instant::now();
            let mut hits = 0usize;
            for _ in 0..reps {
                hits += short.iter().filter(|r| f(std::hint::black_box(r))).count();
            }
            let ns = t.elapsed().as_nanos() as f64 / (reps * short.len()) as f64;
            println!("short rows: {name:30} {ns:5.1} ns/row ({hits} hits)");
        };
        srun("str::contains (direct)", &|t| t.contains("TIMEOUT"));
        let finder = memchr::memmem::Finder::new(b"TIMEOUT");
        srun("memmem Finder::find (direct)", &|t| {
            finder.find(t.as_bytes()).is_some()
        });
        srun("LIKE matches()", &|t| like.matches(t, &mut Vec::new()));
        srun("ILIKE (ASCII fold)", &|t| ilike.matches(t, &mut Vec::new()));
        srun("ILIKE general (previous)", &|t| {
            general.matches(t, &mut Vec::new())
        });
        srun("ILIKE first-byte scan (no copy)", &|t| {
            t.is_ascii() && contains_ascii_fold(t.as_bytes(), b"timeout")
        });
    }

    #[test]
    fn like_patterns() {
        assert!(like("Payment authorization timed out", "%timed%out", false));
        assert!(like("abc", "a_c", false));
        assert!(!like("abc", "a_", false));
        assert!(like("ABC", "abc", true));
        assert!(!like("ABC", "abc", false));
        assert!(like("100%", "100\\%", false));
        assert!(!like("1000", "100\\%", false));
        assert!(like("", "%", false));
        assert!(like("aaa", "%a%a%a%", false));
        assert!(!like("aa", "%a%a%a%", false));
    }

    #[test]
    fn sort_order_of_mixed_numbers_is_exact_and_transitive() {
        let big = 1i64 << 53;
        let (a, f, b) = (
            Some(Value::Int(big)),
            Some(Value::Float(big as f64)),
            Some(Value::Int(big + 1)),
        );
        assert_eq!(sort_cmp(&a, &f), Ordering::Equal);
        assert_eq!(
            sort_cmp(&f, &b),
            Ordering::Less,
            "no rounding of the integer"
        );
        assert_eq!(sort_cmp(&a, &b), Ordering::Less);
        let max = Some(Value::UInt(u64::MAX));
        assert_eq!(
            sort_cmp(&max, &Some(Value::Float(1.8446744073709552e19))),
            Ordering::Less
        );
        assert_eq!(
            sort_cmp(&Some(Value::Int(-1)), &Some(Value::Float(-1.5))),
            Ordering::Greater
        );
        assert_eq!(
            sort_cmp(&Some(Value::Int(0)), &Some(Value::Float(-0.0))),
            Ordering::Equal
        );
        assert_eq!(
            sort_cmp(&Some(Value::Int(7)), &Some(Value::Float(f64::NAN))),
            Ordering::Less
        );
        assert_eq!(
            sort_cmp(
                &Some(Value::Int(i64::MIN)),
                &Some(Value::Float(f64::NEG_INFINITY))
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn sort_order_nulls_last() {
        let mut v = vec![
            None,
            Some(Value::Null),
            Some(Value::Int(2)),
            Some(Value::Int(1)),
        ];
        v.sort_by(sort_cmp);
        assert_eq!(
            v,
            vec![
                Some(Value::Int(1)),
                Some(Value::Int(2)),
                Some(Value::Null),
                None
            ]
        );
    }
}
