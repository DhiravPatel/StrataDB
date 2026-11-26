//! Vectorized `WHERE` evaluation (spec §13.5): selection vectors and typed
//! column kernels.
//!
//! A filter is compiled once per query into a tree of [`Node`]s. Leaves of the
//! form `column OP literal` (and `IN`, `IS [NOT] NULL`, `IS [NOT] MISSING`,
//! `[NOT] [I]LIKE`, bare boolean columns) read the batch's typed columns by
//! reference — no per-row `Value` construction for strings, no literal clones,
//! `LIKE` patterns compiled once, severity predicates answered from a 256-entry
//! table. `AND`/`OR` evaluate their right side only on rows the left side left
//! undecided. Every other shape falls back to the scalar evaluator per row.
//!
//! Semantics are *identical* to the scalar `exec::eval_truth` by construction
//! (same `compare` rules, same three-valued logic); the unit property test
//! below checks row-for-row equality over random expressions and batches, and
//! `QueryOptions::enable_vectorized = false` disables this path.

use std::cmp::Ordering;

use stratadb_common::{Error, Result};
use stratadb_schema::{Severity, Value};
use stratadb_segment::{ColumnData, ColumnId, Scope};

use crate::ast::{CmpOp, ColumnRef, Expr};
use crate::eval::{self, LikePattern, Truth};
use crate::exec::{eval_truth, Batch};

/// Rows evaluated per call; matches the executor's budget-check interval.
pub(crate) const CHUNK: usize = 1024;

/// A borrowed row value: missing, a string column cell, or any value.
#[derive(Clone, Copy)]
enum VRef<'a> {
    Missing,
    Str(&'a str),
    V(&'a Value),
}

/// Predicate applied to one column value.
#[derive(Debug, Clone)]
enum Pred {
    /// `column OP literal`; `flip` = the literal was on the left.
    Cmp {
        op: CmpOp,
        lit: Value,
        flip: bool,
    },
    In {
        list: Vec<Value>,
        negated: bool,
    },
    IsNull(bool),
    IsMissing(bool),
    Like(LikePattern, bool),
    /// Bare column used as a condition (`truth_of`).
    Truthy,
}

fn cmp_ref(v: VRef<'_>, lit: &Value) -> Option<Ordering> {
    match v {
        VRef::Missing => None,
        // compare(String, x) is defined only for x = String.
        VRef::Str(s) => match lit {
            Value::String(y) => Some(s.cmp(y.as_str())),
            _ => None,
        },
        VRef::V(x) => eval::compare_ref(Some(x), Some(lit)),
    }
}

fn apply(op: CmpOp, o: Ordering) -> bool {
    match op {
        CmpOp::Eq => o.is_eq(),
        CmpOp::Ne => o.is_ne(),
        CmpOp::Lt => o.is_lt(),
        CmpOp::Le => o.is_le(),
        CmpOp::Gt => o.is_gt(),
        CmpOp::Ge => o.is_ge(),
    }
}

impl Pred {
    fn eval(&self, v: VRef<'_>, buf: &mut Vec<char>) -> Truth {
        match self {
            Pred::Cmp { op, lit, flip } => cmp_ref(v, lit).map(|o| {
                // compare(lit, col) == compare(col, lit).reverse() (symmetric rules).
                apply(*op, if *flip { o.reverse() } else { o })
            }),
            Pred::In { list, negated } => {
                let mut unknown = false;
                let mut found = false;
                for lit in list {
                    match cmp_ref(v, lit) {
                        Some(o) if o.is_eq() => {
                            found = true;
                            break;
                        }
                        Some(_) => {}
                        None => unknown = true,
                    }
                }
                let t = if found {
                    Some(true)
                } else if unknown {
                    None
                } else {
                    Some(false)
                };
                if *negated {
                    eval::not(t)
                } else {
                    t
                }
            }
            Pred::IsNull(negated) => Some(matches!(v, VRef::V(Value::Null)) != *negated),
            Pred::IsMissing(negated) => Some(matches!(v, VRef::Missing) != *negated),
            Pred::Like(p, negated) => match v {
                VRef::Str(s) => Some(p.matches(s, buf) != *negated),
                VRef::V(Value::String(s)) => Some(p.matches(s, buf) != *negated),
                _ => None,
            },
            Pred::Truthy => match v {
                VRef::V(Value::Bool(b)) => Some(*b),
                _ => None,
            },
        }
    }
}

/// Where a leaf reads its value from.
#[derive(Debug, Clone)]
enum Access {
    /// Int column exposed as a timestamp (`true`) or an int (`false`).
    Ints(ColumnId, bool),
    Strs(ColumnId),
    Body,
    Attr(Scope, String),
    Tenant,
}

#[derive(Debug, Clone)]
enum Node {
    Leaf(Access, Pred),
    /// Severity predicate precomputed for every stored code.
    SevTable(Box<[Truth; 256]>),
    And(Box<Node>, Box<Node>),
    Or(Box<Node>, Box<Node>),
    Not(Box<Node>),
    /// Scalar fallback.
    Row(Expr),
}

enum Source {
    Ints(ColumnId, bool),
    Sev(bool),
    Other(Access),
}

fn source(e: &Expr) -> Option<Source> {
    let Expr::Column(c) = e else { return None };
    Some(match c {
        ColumnRef::EventTime => Source::Ints(ColumnId::EventTime, true),
        ColumnRef::IngestTime => Source::Ints(ColumnId::IngestTime, true),
        ColumnRef::SchemaVersion => Source::Ints(ColumnId::SchemaVersion, false),
        ColumnRef::Severity => Source::Sev(false),
        ColumnRef::SeverityNumber => Source::Sev(true),
        ColumnRef::Service => Source::Other(Access::Strs(ColumnId::Service)),
        ColumnRef::SeverityText => Source::Other(Access::Strs(ColumnId::SeverityText)),
        ColumnRef::TraceId => Source::Other(Access::Strs(ColumnId::TraceId)),
        ColumnRef::SpanId => Source::Other(Access::Strs(ColumnId::SpanId)),
        ColumnRef::EventId => Source::Other(Access::Strs(ColumnId::EventId)),
        ColumnRef::Body => Source::Other(Access::Body),
        ColumnRef::TenantId => Source::Other(Access::Tenant),
        ColumnRef::Attr(k) => Source::Other(Access::Attr(Scope::Attributes, k.clone())),
        ColumnRef::Resource(k) => Source::Other(Access::Attr(Scope::Resource, k.clone())),
        // Whole maps would need a cloned `Value::Map` per row: scalar path.
        ColumnRef::Attributes | ColumnRef::ResourceAttributes => return None,
    })
}

fn leaf(src: Source, pred: Pred) -> Node {
    match src {
        Source::Ints(id, ts) => Node::Leaf(Access::Ints(id, ts), pred),
        Source::Other(a) => Node::Leaf(a, pred),
        Source::Sev(number) => {
            let mut buf = Vec::new();
            let mut table = Box::new([None; 256]);
            for (code, slot) in table.iter_mut().enumerate() {
                let code = code as u8;
                *slot = if number {
                    pred.eval(VRef::V(&Value::Int(i64::from(code))), &mut buf)
                } else {
                    let s = Severity::from_u8(code).unwrap_or_default();
                    pred.eval(VRef::Str(s.as_str()), &mut buf)
                };
            }
            Node::SevTable(table)
        }
    }
}

/// Compile a filter expression.
fn compile(e: &Expr) -> Node {
    let fallback = || Node::Row(e.clone());
    match e {
        Expr::And(l, r) => Node::And(Box::new(compile(l)), Box::new(compile(r))),
        Expr::Or(l, r) => Node::Or(Box::new(compile(l)), Box::new(compile(r))),
        Expr::Not(x) => Node::Not(Box::new(compile(x))),
        Expr::Cmp(l, op, r) => match (source(l), &**l, source(r), &**r) {
            (Some(s), _, None, Expr::Literal(v)) => leaf(
                s,
                Pred::Cmp {
                    op: *op,
                    lit: v.clone(),
                    flip: false,
                },
            ),
            (None, Expr::Literal(v), Some(s), _) => leaf(
                s,
                Pred::Cmp {
                    op: *op,
                    lit: v.clone(),
                    flip: true,
                },
            ),
            _ => fallback(),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => match source(expr) {
            Some(s) => leaf(
                s,
                Pred::In {
                    list: list.clone(),
                    negated: *negated,
                },
            ),
            None => fallback(),
        },
        Expr::IsNull { expr, negated } => match source(expr) {
            Some(s) => leaf(s, Pred::IsNull(*negated)),
            None => fallback(),
        },
        Expr::IsMissing { expr, negated } => match source(expr) {
            Some(s) => leaf(s, Pred::IsMissing(*negated)),
            None => fallback(),
        },
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => match source(expr) {
            Some(s) => leaf(
                s,
                Pred::Like(LikePattern::new(pattern, *case_insensitive), *negated),
            ),
            None => fallback(),
        },
        Expr::Column(_) => match source(e) {
            Some(s) => leaf(s, Pred::Truthy),
            None => fallback(),
        },
        _ => fallback(),
    }
}

fn not_loaded(id: ColumnId) -> Error {
    Error::internal(format!("column {id:?} not loaded or of unexpected type"))
}

impl Node {
    /// Evaluate for the absolute row indices `rows`, writing `out[row - base]`.
    fn eval(
        &self,
        b: &Batch<'_>,
        rows: &[usize],
        base: usize,
        out: &mut [Truth],
        buf: &mut Vec<char>,
    ) -> Result<()> {
        match self {
            Node::Leaf(access, pred) => leaf_eval(access, pred, b, rows, base, out, buf)?,
            Node::SevTable(t) => {
                let Ok(ColumnData::U8s(v)) = b.col(ColumnId::Severity) else {
                    return Err(not_loaded(ColumnId::Severity));
                };
                for &i in rows {
                    out[i - base] = t[usize::from(v[i])];
                }
            }
            Node::And(l, r) | Node::Or(l, r) => {
                let is_and = matches!(self, Node::And(..));
                l.eval(b, rows, base, out, buf)?;
                // The right side matters only where the left did not decide.
                let decided = Some(!is_and);
                let rest: Vec<usize> = rows
                    .iter()
                    .copied()
                    .filter(|&i| out[i - base] != decided)
                    .collect();
                if !rest.is_empty() {
                    let mut tmp = vec![None; out.len()];
                    r.eval(b, &rest, base, &mut tmp, buf)?;
                    for i in rest {
                        let (a, c) = (out[i - base], tmp[i - base]);
                        out[i - base] = if is_and {
                            eval::and(a, c)
                        } else {
                            eval::or(a, c)
                        };
                    }
                }
            }
            Node::Not(x) => {
                x.eval(b, rows, base, out, buf)?;
                for &i in rows {
                    out[i - base] = eval::not(out[i - base]);
                }
            }
            Node::Row(e) => {
                for &i in rows {
                    out[i - base] = eval_truth(e, b, i)?;
                }
            }
        }
        Ok(())
    }
}

fn leaf_eval(
    access: &Access,
    pred: &Pred,
    b: &Batch<'_>,
    rows: &[usize],
    base: usize,
    out: &mut [Truth],
    buf: &mut Vec<char>,
) -> Result<()> {
    match access {
        Access::Ints(id, ts) => {
            let Ok(ColumnData::Ints(v)) = b.col(*id) else {
                return Err(not_loaded(*id));
            };
            for &i in rows {
                let val = if *ts {
                    Value::Timestamp(v[i])
                } else {
                    Value::Int(v[i])
                };
                out[i - base] = pred.eval(VRef::V(&val), buf);
            }
        }
        Access::Strs(id) => {
            let Ok(ColumnData::Strs(v)) = b.col(*id) else {
                return Err(not_loaded(*id));
            };
            for &i in rows {
                let r = v[i].as_deref().map_or(VRef::Missing, VRef::Str);
                out[i - base] = pred.eval(r, buf);
            }
        }
        Access::Body => {
            let Ok(ColumnData::Values(v)) = b.col(ColumnId::Body) else {
                return Err(not_loaded(ColumnId::Body));
            };
            for &i in rows {
                let r = v[i].as_ref().map_or(VRef::Missing, VRef::V);
                out[i - base] = pred.eval(r, buf);
            }
        }
        Access::Tenant => {
            let t = pred.eval(VRef::Str(b.tenant.as_str()), buf);
            for &i in rows {
                out[i - base] = t;
            }
        }
        Access::Attr(scope, key) => {
            if let Some(col) = b.shredded.get(&(*scope, key.clone())) {
                for &i in rows {
                    let r = col[i].as_ref().map_or(VRef::Missing, VRef::V);
                    out[i - base] = pred.eval(r, buf);
                }
            } else {
                let id = match scope {
                    Scope::Attributes => ColumnId::Attributes,
                    Scope::Resource => ColumnId::ResourceAttributes,
                };
                let Ok(ColumnData::Maps(v)) = b.col(id) else {
                    return Err(not_loaded(id));
                };
                for &i in rows {
                    let r = v[i].get(key).map_or(VRef::Missing, VRef::V);
                    out[i - base] = pred.eval(r, buf);
                }
            }
        }
    }
    Ok(())
}

/// A compiled filter.
#[derive(Debug, Clone)]
pub(crate) struct Filter {
    root: Node,
}

impl Filter {
    pub(crate) fn new(e: &Expr) -> Self {
        Self { root: compile(e) }
    }

    /// Truth of the filter for rows `start..end` of `b`.
    pub(crate) fn eval(&self, b: &Batch<'_>, start: usize, end: usize) -> Result<Vec<Truth>> {
        let rows: Vec<usize> = (start..end).collect();
        let mut out = vec![None; end - start];
        self.root.eval(b, &rows, start, &mut out, &mut Vec::new())?;
        Ok(out)
    }

    /// Number of scalar-fallback nodes.
    #[cfg(test)]
    fn fallbacks(&self) -> usize {
        fn count(n: &Node) -> usize {
            match n {
                Node::Row(_) => 1,
                Node::And(l, r) | Node::Or(l, r) => count(l) + count(r),
                Node::Not(x) => count(x),
                Node::Leaf(..) | Node::SevTable(_) => 0,
            }
        }
        count(&self.root)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use std::collections::{BTreeMap, HashMap};

    use proptest::prelude::*;
    use stratadb_common::TenantId;

    use super::*;
    use crate::eval::Datum;

    fn arb_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            (-3i64..3).prop_map(Value::Int),
            (0u64..3).prop_map(Value::UInt),
            prop::sample::select(vec![-1.5f64, 0.0, 2.0, f64::NAN]).prop_map(Value::Float),
            prop::sample::select(vec!["", "a", "ab", "ERROR", "%x", "Ab", "b"])
                .prop_map(|s| Value::String(s.to_owned())),
            (-2i64..2).prop_map(Value::Timestamp),
            prop::collection::vec(0u8..2, 0..2).prop_map(Value::Bytes),
            prop::collection::vec((-1i64..2).prop_map(Value::Int), 0..2).prop_map(Value::List),
        ]
    }

    fn arb_datum() -> impl Strategy<Value = Datum> {
        prop_oneof![1 => Just(None), 5 => arb_value().prop_map(Some)]
    }

    fn arb_opt_str() -> impl Strategy<Value = Option<String>> {
        prop::option::of(
            prop::sample::select(vec!["", "a", "ab", "api", "ERROR", "%x"]).prop_map(str::to_owned),
        )
    }

    fn arb_map() -> impl Strategy<Value = BTreeMap<String, Value>> {
        prop::collection::btree_map(
            prop::sample::select(vec!["a", "b", "r"]).prop_map(str::to_owned),
            arb_value(),
            0..3,
        )
    }

    #[derive(Debug, Clone)]
    struct Rows {
        ints: Vec<(i64, i64, i64)>,
        sev: Vec<u8>,
        strs: Vec<[Option<String>; 5]>,
        body: Vec<Datum>,
        attrs: Vec<BTreeMap<String, Value>>,
        res: Vec<BTreeMap<String, Value>>,
        shredded: Option<Vec<Datum>>,
    }

    fn arb_rows() -> impl Strategy<Value = Rows> {
        (1usize..40).prop_flat_map(|n| {
            let five = (
                arb_opt_str(),
                arb_opt_str(),
                arb_opt_str(),
                arb_opt_str(),
                arb_opt_str(),
            )
                .prop_map(|(a, b, c, d, e)| [a, b, c, d, e]);
            (
                prop::collection::vec((-2i64..2, -2i64..2, 0i64..3), n),
                prop::collection::vec(
                    prop_oneof![3 => prop::sample::select(vec![0u8, 1, 9, 13, 17, 21]), 1 => any::<u8>()],
                    n,
                ),
                prop::collection::vec(five, n),
                prop::collection::vec(arb_datum(), n),
                prop::collection::vec(arb_map(), n),
                prop::collection::vec(arb_map(), n),
                prop::option::of(prop::collection::vec(arb_datum(), n)),
            )
                .prop_map(|(ints, sev, strs, body, attrs, res, shredded)| Rows {
                    ints,
                    sev,
                    strs,
                    body,
                    attrs,
                    res,
                    shredded,
                })
        })
    }

    fn batch<'a>(r: &Rows, t: &'a TenantId) -> Batch<'a> {
        let mut cols = HashMap::new();
        cols.insert(
            ColumnId::EventTime,
            ColumnData::Ints(r.ints.iter().map(|x| x.0).collect()),
        );
        cols.insert(
            ColumnId::IngestTime,
            ColumnData::Ints(r.ints.iter().map(|x| x.1).collect()),
        );
        cols.insert(
            ColumnId::SchemaVersion,
            ColumnData::Ints(r.ints.iter().map(|x| x.2).collect()),
        );
        cols.insert(ColumnId::Severity, ColumnData::U8s(r.sev.clone()));
        for (j, id) in [
            ColumnId::Service,
            ColumnId::SeverityText,
            ColumnId::TraceId,
            ColumnId::SpanId,
            ColumnId::EventId,
        ]
        .into_iter()
        .enumerate()
        {
            cols.insert(
                id,
                ColumnData::Strs(r.strs.iter().map(|s| s[j].clone()).collect()),
            );
        }
        cols.insert(ColumnId::Body, ColumnData::Values(r.body.clone()));
        cols.insert(ColumnId::Attributes, ColumnData::Maps(r.attrs.clone()));
        cols.insert(
            ColumnId::ResourceAttributes,
            ColumnData::Maps(r.res.clone()),
        );
        let mut shredded = HashMap::new();
        if let Some(s) = &r.shredded {
            shredded.insert((Scope::Attributes, "s".to_owned()), s.clone());
        }
        Batch {
            len: r.ints.len(),
            tenant: t,
            cols,
            shredded,
            raw: HashMap::new(),
        }
    }

    fn arb_col() -> impl Strategy<Value = Expr> {
        prop::sample::select(vec![
            ColumnRef::EventTime,
            ColumnRef::IngestTime,
            ColumnRef::SchemaVersion,
            ColumnRef::Severity,
            ColumnRef::SeverityNumber,
            ColumnRef::Service,
            ColumnRef::SeverityText,
            ColumnRef::TraceId,
            ColumnRef::SpanId,
            ColumnRef::EventId,
            ColumnRef::Body,
            ColumnRef::TenantId,
            ColumnRef::Attr("a".into()),
            ColumnRef::Attr("s".into()),
            ColumnRef::Attr("zz".into()),
            ColumnRef::Resource("r".into()),
            ColumnRef::Attributes,
            ColumnRef::ResourceAttributes,
        ])
        .prop_map(Expr::Column)
    }

    fn arb_op() -> impl Strategy<Value = CmpOp> {
        prop::sample::select(vec![
            CmpOp::Eq,
            CmpOp::Ne,
            CmpOp::Lt,
            CmpOp::Le,
            CmpOp::Gt,
            CmpOp::Ge,
        ])
    }

    fn lit(v: Value) -> Box<Expr> {
        Box::new(Expr::Literal(v))
    }

    fn arb_leaf() -> impl Strategy<Value = Expr> {
        // Severity names/numbers as literals exercise the 256-entry table.
        let sev_lit = prop_oneof![
            prop::sample::select(vec!["ERROR", "INFO", "UNSPECIFIED", "error"])
                .prop_map(|s| Value::String(s.to_owned())),
            (0i64..30).prop_map(Value::Int),
        ];
        let any_lit = prop_oneof![3 => arb_value(), 1 => sev_lit];
        prop_oneof![
            (arb_col(), arb_op(), any_lit.clone()).prop_map(|(c, op, v)| Expr::Cmp(
                Box::new(c),
                op,
                lit(v)
            )),
            (any_lit.clone(), arb_op(), arb_col()).prop_map(|(v, op, c)| Expr::Cmp(
                lit(v),
                op,
                Box::new(c)
            )),
            (arb_col(), arb_op(), arb_col()).prop_map(|(a, op, b)| Expr::Cmp(
                Box::new(a),
                op,
                Box::new(b)
            )),
            (
                arb_col(),
                prop::collection::vec(any_lit, 0..4),
                any::<bool>()
            )
                .prop_map(|(c, list, negated)| Expr::InList {
                    expr: Box::new(c),
                    list,
                    negated
                }),
            (arb_col(), any::<bool>()).prop_map(|(c, negated)| Expr::IsNull {
                expr: Box::new(c),
                negated
            }),
            (arb_col(), any::<bool>()).prop_map(|(c, negated)| Expr::IsMissing {
                expr: Box::new(c),
                negated
            }),
            (
                arb_col(),
                prop::sample::select(vec![
                    "%", "a%", "%b", "%a%", "a_", "", "_%", "\\%x", "%A%", "E%R"
                ]),
                any::<bool>(),
                any::<bool>()
            )
                .prop_map(|(c, p, negated, ci)| Expr::Like {
                    expr: Box::new(c),
                    pattern: p.to_owned(),
                    negated,
                    case_insensitive: ci
                }),
            arb_col(),
            any::<bool>().prop_map(|b| Expr::Literal(Value::Bool(b))),
            (arb_col(), arb_op(), (-2i64..2).prop_map(Value::Timestamp)).prop_map(|(c, op, v)| {
                Expr::Cmp(
                    Box::new(Expr::TimeBucket {
                        width_ns: 2,
                        expr: Box::new(c),
                    }),
                    op,
                    lit(v),
                )
            }),
        ]
    }

    fn arb_expr() -> impl Strategy<Value = Expr> {
        arb_leaf().prop_recursive(4, 24, 2, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone())
                    .prop_map(|(a, b)| Expr::And(Box::new(a), Box::new(b))),
                (inner.clone(), inner.clone())
                    .prop_map(|(a, b)| Expr::Or(Box::new(a), Box::new(b))),
                inner.prop_map(|a| Expr::Not(Box::new(a))),
            ]
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]

        /// The vectorized evaluator agrees with the scalar one on every row,
        /// for whole batches and for chunks starting mid-batch.
        #[test]
        fn vectorized_equals_scalar(rows in arb_rows(), e in arb_expr(), start in 0usize..40) {
            let t = TenantId::new("ab").unwrap();
            let b = batch(&rows, &t);
            let f = Filter::new(&e);
            let start = start.min(b.len);
            let got = f.eval(&b, start, b.len).unwrap();
            for i in start..b.len {
                let want = eval_truth(&e, &b, i).unwrap();
                prop_assert_eq!(got[i - start], want, "row {} of {:?}", i, e);
            }
        }
    }

    fn arb_group_expr() -> impl Strategy<Value = Expr> {
        prop_oneof![
            4 => arb_col(),
            1 => arb_col().prop_map(|c| Expr::TimeBucket { width_ns: 2, expr: Box::new(c) }),
            1 => arb_leaf(),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        /// The borrowed group-key encoder is byte-identical to encoding the
        /// evaluated (owned) datum, so grouping semantics are unchanged.
        #[test]
        fn group_key_into_equals_group_key_bytes(rows in arb_rows(), e in arb_group_expr()) {
            let t = TenantId::new("ab").unwrap();
            let b = batch(&rows, &t);
            for i in 0..b.len {
                let mut enc = stratadb_common::codec::Encoder::new();
                b.group_key_into(&e, i, &mut enc).unwrap();
                let want = eval::group_key_bytes(&crate::exec::eval_expr(&e, &b, i).unwrap());
                prop_assert_eq!(enc.into_inner(), want, "row {} of {:?}", i, e);
            }
        }
    }

    #[test]
    fn common_shapes_compile_without_fallback() {
        let col = |c| Box::new(Expr::Column(c));
        let e = Expr::And(
            Box::new(Expr::Cmp(
                col(ColumnRef::Service),
                CmpOp::Eq,
                lit(Value::from("api")),
            )),
            Box::new(Expr::Or(
                Box::new(Expr::Cmp(
                    lit(Value::from("ERROR")),
                    CmpOp::Eq,
                    col(ColumnRef::Severity),
                )),
                Box::new(Expr::Like {
                    expr: col(ColumnRef::Body),
                    pattern: "%TIMEOUT%".into(),
                    negated: false,
                    case_insensitive: true,
                }),
            )),
        );
        assert_eq!(Filter::new(&e).fallbacks(), 0);
        let whole_map = Expr::IsNull {
            expr: col(ColumnRef::Attributes),
            negated: false,
        };
        assert_eq!(Filter::new(&whole_map).fallbacks(), 1);
    }
}
