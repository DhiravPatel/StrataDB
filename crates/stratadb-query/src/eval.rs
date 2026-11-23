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
/// character. Linear-memory greedy matcher with single backtrack point
/// (O(n·m) worst case; inputs are bounded by validation limits).
pub fn like(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    #[derive(Clone, Copy, PartialEq)]
    enum P {
        Any,
        One,
        Lit(char),
    }
    let mut pat = Vec::new();
    let mut it = pattern.chars();
    while let Some(c) = it.next() {
        pat.push(match c {
            '%' => P::Any,
            '_' => P::One,
            '\\' => P::Lit(it.next().unwrap_or('\\')),
            c => P::Lit(c),
        });
    }
    let fold = |c: char| {
        if case_insensitive {
            c.to_lowercase().next().unwrap_or(c)
        } else {
            c
        }
    };
    let t: Vec<char> = text.chars().map(fold).collect();
    let pat: Vec<P> = pat
        .into_iter()
        .map(|p| match p {
            P::Lit(c) => P::Lit(fold(c)),
            p => p,
        })
        .collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        match pat.get(pi) {
            Some(P::Any) => {
                star = Some((pi, ti));
                pi += 1;
            }
            Some(P::One) => {
                ti += 1;
                pi += 1;
            }
            Some(P::Lit(c)) if *c == t[ti] => {
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
    pat[pi..].iter().all(|p| *p == P::Any)
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
                _ => compare(a, b).unwrap_or_else(|| {
                    // NaN or Null vs Null: order deterministically.
                    let nan = |v: &Value| matches!(v, Value::Float(f) if f.is_nan());
                    nan(x).cmp(&nan(y))
                }),
            }
        }
    }
}

/// Canonical bytes for grouping: equal datums ⇔ equal bytes. Missing and
/// null are distinct groups.
pub fn group_key_bytes(d: &Datum) -> Vec<u8> {
    let mut e = Encoder::new();
    match d {
        None => e.put_u8(0xff),
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
