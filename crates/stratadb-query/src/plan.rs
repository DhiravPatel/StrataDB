//! Semantic analysis and logical planning (spec §13.3, §13.4).
//!
//! Responsibilities: expand `SELECT *`, validate aggregate/grouping rules,
//! statically reject comparisons between canonical columns and literals of the
//! wrong type (no silent coercion), resolve ORDER BY keys, compute the set of
//! segment columns to read (projection pruning), and extract *sound* segment
//! pruning predicates from top-level conjuncts of `WHERE`.

use std::collections::BTreeSet;

use stratadb_common::{Error, Result};
use stratadb_schema::column::ColumnId;
use stratadb_schema::{Severity, Value};
use stratadb_storage::scan::IndexField;
/// Per-source pruning predicates, defined by storage (spec STRATA-ARCH-01-003):
/// the planner states them, storage evaluates them against its statistics
/// and indexes.
pub use stratadb_storage::scan::Pruning;

use crate::ast::*;

#[derive(Debug, Clone, PartialEq)]
pub enum SortSource {
    /// Index into the output columns.
    Output(usize),
    /// Expression evaluated per source row (non-aggregate queries only).
    Hidden(Expr),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutputSource {
    /// Evaluate per row (non-aggregate query).
    Row(Expr),
    /// Index into `Plan::group_by`.
    Group(usize),
    /// Index into `Plan::aggregates`.
    Agg(usize),
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub explain: ExplainMode,
    /// The table to read.
    pub table: Table,
    pub output_names: Vec<String>,
    pub outputs: Vec<OutputSource>,
    pub filter: Option<Expr>,
    pub aggregate: bool,
    pub group_by: Vec<Expr>,
    pub aggregates: Vec<(AggFunc, Option<Expr>)>,
    pub order: Vec<(SortSource, bool)>,
    pub limit: Option<u64>,
    pub columns: BTreeSet<ColumnId>,
    pub pruning: Pruning,
    /// Attribute keys referenced individually (`attributes['k']`), and whether
    /// the whole map is referenced (`attributes`, `SELECT *`). Same for resource.
    pub attr_keys: BTreeSet<String>,
    pub whole_attributes: bool,
    pub resource_keys: BTreeSet<String>,
    pub whole_resource: bool,
    /// Attribute (resp. resource) keys referenced only as whole `GROUP BY`
    /// keys (`GROUP BY attributes['k']` and nowhere else, except as that
    /// group's output column): scans need only their encoded bytes, so they
    /// may be returned undecoded (`ColumnRequest::raw_attr_keys`).
    pub raw_group_attr_keys: BTreeSet<String>,
    pub raw_group_resource_keys: BTreeSet<String>,
}

fn column_id(c: &ColumnRef) -> Option<ColumnId> {
    Some(match c {
        ColumnRef::EventTime => ColumnId::EventTime,
        ColumnRef::IngestTime => ColumnId::IngestTime,
        ColumnRef::Service => ColumnId::Service,
        ColumnRef::Severity | ColumnRef::SeverityNumber => ColumnId::Severity,
        ColumnRef::SeverityText => ColumnId::SeverityText,
        ColumnRef::Body => ColumnId::Body,
        ColumnRef::TraceId => ColumnId::TraceId,
        ColumnRef::SpanId => ColumnId::SpanId,
        ColumnRef::EventId => ColumnId::EventId,
        ColumnRef::SchemaVersion => ColumnId::SchemaVersion,
        ColumnRef::Attributes | ColumnRef::Attr(_) => ColumnId::Attributes,
        ColumnRef::ResourceAttributes | ColumnRef::Resource(_) => ColumnId::ResourceAttributes,
        ColumnRef::TenantId => return None,
    })
}

/// Expected literal class for a canonical column, used for static type checks.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Class {
    Timestamp,
    Text,
    Number,
    Dynamic,
}

fn class_of(c: &ColumnRef) -> Class {
    match c {
        ColumnRef::EventTime | ColumnRef::IngestTime => Class::Timestamp,
        ColumnRef::Service
        | ColumnRef::Severity
        | ColumnRef::SeverityText
        | ColumnRef::TraceId
        | ColumnRef::SpanId
        | ColumnRef::EventId
        | ColumnRef::TenantId => Class::Text,
        ColumnRef::SeverityNumber | ColumnRef::SchemaVersion => Class::Number,
        _ => Class::Dynamic,
    }
}

fn check_literal(col: &ColumnRef, lit: &Value) -> Result<()> {
    let ok = matches!(
        (class_of(col), lit),
        (_, Value::Null)
            | (Class::Dynamic, _)
            | (Class::Timestamp, Value::Timestamp(_))
            | (Class::Text, Value::String(_))
            | (
                Class::Number,
                Value::Int(_) | Value::UInt(_) | Value::Float(_)
            )
    );
    if ok {
        return Ok(());
    }
    let hint = match class_of(col) {
        Class::Timestamp => " (use TIMESTAMP '2026-01-01T00:00:00Z')",
        _ => "",
    };
    Err(Error::invalid_input(format!(
        "type mismatch: `{col}` cannot be compared with {}{hint}",
        lit.type_name()
    )))
}

fn type_check(e: &Expr) -> Result<()> {
    let mut err = None;
    e.walk(&mut |x| {
        if err.is_some() {
            return;
        }
        let r = match x {
            Expr::Cmp(a, _, b) => match (a.as_ref(), b.as_ref()) {
                (Expr::Column(c), Expr::Literal(v)) | (Expr::Literal(v), Expr::Column(c)) => {
                    check_literal(c, v)
                }
                _ => Ok(()),
            },
            Expr::InList { expr, list, .. } => match expr.as_ref() {
                Expr::Column(c) => list.iter().try_for_each(|v| check_literal(c, v)),
                _ => Ok(()),
            },
            Expr::Like { expr, .. } => match expr.as_ref() {
                Expr::Column(c) if !matches!(class_of(c), Class::Text | Class::Dynamic) => Err(
                    Error::invalid_input(format!("LIKE requires a text column, `{c}` is not")),
                ),
                _ => Ok(()),
            },
            _ => Ok(()),
        };
        if let Err(e) = r {
            err = Some(e);
        }
    });
    err.map_or(Ok(()), Err)
}

fn conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::And(a, b) => {
            conjuncts(a, out);
            conjuncts(b, out);
        }
        other => out.push(other),
    }
}

fn require_key(c: &ColumnRef, p: &mut Pruning) {
    match c {
        ColumnRef::Attr(k) => {
            p.required_attrs.insert(k.clone());
        }
        ColumnRef::Resource(k) => {
            p.required_resource.insert(k.clone());
        }
        _ => {}
    }
}

/// The storage field an equality predicate on `c` names (string-valued
/// identifier and attribute columns).
fn equals_field(c: &ColumnRef) -> Option<IndexField> {
    Some(match c {
        ColumnRef::TraceId => IndexField::TraceId,
        ColumnRef::SpanId => IndexField::SpanId,
        ColumnRef::EventId => IndexField::EventId,
        ColumnRef::Attr(k) => IndexField::Attr(k.clone()),
        ColumnRef::Resource(k) => IndexField::Resource(k.clone()),
        _ => return None,
    })
}

fn severity_code(v: &Value) -> Option<u8> {
    // Unknown severity text can never match; u8::MAX is not a valid code.
    v.as_str()
        .map(|s| Severity::parse(s).map_or(u8::MAX, Severity::as_u8))
}

fn intersect_sev(p: &mut Pruning, set: BTreeSet<u8>) {
    p.severities = Some(match p.severities.take() {
        Some(prev) => prev.intersection(&set).copied().collect(),
        None => set,
    });
}

/// Derive pruning predicates. Only top-level conjuncts are used, and only
/// forms whose truth implies the pruning condition (soundness).
/// A filter that metadata can evaluate exactly: only `event_time`
/// comparisons with timestamp literals and `severity = 'NAME'` /
/// `severity IN (...)` with canonical names, joined by `AND`. A source whose
/// time range lies inside `[time_lo, time_hi]` then matches exactly the rows
/// of the listed severities (STRATA-SYS-09-006/007).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetaFilter {
    /// Inclusive bounds.
    pub time_lo: Option<i64>,
    pub time_hi: Option<i64>,
    /// `None`: every severity.
    pub severities: Option<BTreeSet<u8>>,
}

impl MetaFilter {
    /// `None` when any part of `filter` is not of the supported forms.
    pub fn of(filter: Option<&Expr>) -> Option<Self> {
        let mut m = MetaFilter::default();
        let Some(f) = filter else {
            return Some(m);
        };
        let canonical = |v: &Value| -> Option<u8> {
            let s = v.as_str()?;
            let sev = Severity::parse(s)?;
            (sev.as_str() == s).then(|| sev.as_u8())
        };
        let mut cs = Vec::new();
        conjuncts(f, &mut cs);
        for c in cs {
            match c {
                Expr::Cmp(a, op, b) => {
                    let (col, op, lit) = match (a.as_ref(), b.as_ref()) {
                        (Expr::Column(c), Expr::Literal(v)) => (c, *op, v),
                        (Expr::Literal(v), Expr::Column(c)) => (c, op.flip(), v),
                        _ => return None,
                    };
                    match (col, lit) {
                        (ColumnRef::EventTime, Value::Timestamp(t)) => {
                            let t = *t;
                            let (lo, hi) = match op {
                                CmpOp::Eq => (Some(t), Some(t)),
                                CmpOp::Ge => (Some(t), None),
                                CmpOp::Gt => (Some(t.checked_add(1)?), None),
                                CmpOp::Le => (None, Some(t)),
                                CmpOp::Lt => (None, Some(t.checked_sub(1)?)),
                                CmpOp::Ne => return None,
                            };
                            if let Some(lo) = lo {
                                m.time_lo = Some(m.time_lo.map_or(lo, |x| x.max(lo)));
                            }
                            if let Some(hi) = hi {
                                m.time_hi = Some(m.time_hi.map_or(hi, |x| x.min(hi)));
                            }
                        }
                        (ColumnRef::Severity, v) if op == CmpOp::Eq => {
                            m.restrict(BTreeSet::from([canonical(v)?]));
                        }
                        _ => return None,
                    }
                }
                Expr::InList {
                    expr,
                    list,
                    negated: false,
                } if matches!(expr.as_ref(), Expr::Column(ColumnRef::Severity)) => {
                    let set = list
                        .iter()
                        .map(canonical)
                        .collect::<Option<BTreeSet<u8>>>()?;
                    m.restrict(set);
                }
                _ => return None,
            }
        }
        Some(m)
    }

    fn restrict(&mut self, set: BTreeSet<u8>) {
        self.severities = Some(match self.severities.take() {
            Some(prev) => prev.intersection(&set).copied().collect(),
            None => set,
        });
    }

    /// Whether every row of a source with this inclusive time range passes
    /// the time bounds.
    pub fn covers_time(&self, (min, max): (i64, i64)) -> bool {
        self.time_lo.is_none_or(|lo| min >= lo) && self.time_hi.is_none_or(|hi| max <= hi)
    }
}

pub fn extract_pruning(filter: &Expr) -> Pruning {
    let mut p = Pruning::default();
    let mut cs = Vec::new();
    conjuncts(filter, &mut cs);
    for c in cs {
        match c {
            Expr::Cmp(a, op, b) => {
                let (col, op, lit) = match (a.as_ref(), b.as_ref()) {
                    (Expr::Column(c), Expr::Literal(v)) => (c, *op, v),
                    (Expr::Literal(v), Expr::Column(c)) => (c, op.flip(), v),
                    _ => continue,
                };
                if matches!(lit, Value::Null) {
                    continue; // comparison with NULL is never true; no pruning needed
                }
                require_key(col, &mut p);
                if let (ColumnRef::EventTime, Value::Timestamp(t)) = (col, lit) {
                    let t = *t;
                    let (lo, hi) = match op {
                        CmpOp::Eq => (Some(t), Some(t)),
                        CmpOp::Ge => (Some(t), None),
                        CmpOp::Gt => (t.checked_add(1), None),
                        CmpOp::Le => (None, Some(t)),
                        CmpOp::Lt => (None, t.checked_sub(1)),
                        CmpOp::Ne => (None, None),
                    };
                    if let Some(lo) = lo {
                        p.time_lo = Some(p.time_lo.map_or(lo, |x| x.max(lo)));
                    }
                    if let Some(hi) = hi {
                        p.time_hi = Some(p.time_hi.map_or(hi, |x| x.min(hi)));
                    }
                }
                if let (CmpOp::Eq, Value::String(v), Some(f)) = (op, lit, equals_field(col)) {
                    p.add_equals(&f, vec![v.clone()]);
                }
                if let (ColumnRef::Severity, CmpOp::Eq) = (col, op) {
                    if let Some(code) = severity_code(lit) {
                        intersect_sev(&mut p, BTreeSet::from([code]));
                    }
                }
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                if let Expr::Column(col) = expr.as_ref() {
                    require_key(col, &mut p);
                    if *col == ColumnRef::Severity {
                        intersect_sev(&mut p, list.iter().filter_map(severity_code).collect());
                    }
                    let strings: Option<Vec<String>> =
                        list.iter().map(|v| v.as_str().map(str::to_owned)).collect();
                    if let (Some(f), Some(vals)) = (equals_field(col), strings) {
                        p.add_equals(&f, vals);
                    }
                }
            }
            Expr::Like {
                expr,
                pattern,
                negated: false,
                ..
            } => {
                if let Expr::Column(col) = expr.as_ref() {
                    require_key(col, &mut p);
                    let field = match col {
                        ColumnRef::Body => Some(IndexField::Body),
                        ColumnRef::Attr(k) => Some(IndexField::Attr(k.clone())),
                        ColumnRef::Resource(k) => Some(IndexField::Resource(k.clone())),
                        _ => None,
                    };
                    if let Some(f) = field {
                        p.add_like(&f, pattern);
                    }
                }
            }
            Expr::IsNull {
                expr,
                negated: false,
            }
            | Expr::IsMissing {
                expr,
                negated: true,
            } => {
                if let Expr::Column(col) = expr.as_ref() {
                    require_key(col, &mut p);
                }
            }
            _ => {}
        }
    }
    p
}

/// A literal's truth value in a filter (`truth_of`): `None` when `e` is not
/// a literal.
fn literal_truth(e: &Expr) -> Option<crate::eval::Truth> {
    match e {
        Expr::Literal(v) => Some(crate::eval::truth_of(&Some(v.clone()))),
        _ => None,
    }
}

fn truth_literal(t: crate::eval::Truth) -> Expr {
    Expr::Literal(t.map_or(Value::Null, Value::Bool))
}

fn flatten<'e>(e: &'e Expr, and: bool, out: &mut Vec<&'e Expr>) {
    match e {
        Expr::And(l, r) if and => {
            flatten(l, and, out);
            flatten(r, and, out);
        }
        Expr::Or(l, r) if !and => {
            flatten(l, and, out);
            flatten(r, and, out);
        }
        other => out.push(other),
    }
}

/// Constant folding and duplicate elimination for a filter (spec
/// STRATA-SYS-08-018), exact under three-valued logic:
/// - comparisons, `IN`, `LIKE`, `IS [NOT] NULL` and `time_bucket` over
///   literals are evaluated once;
/// - `false AND x` = `false`, `true AND x` = `x`, `true OR x` = `true`,
///   `false OR x` = `x`, `NOT literal` folds;
/// - repeated operands of an `AND` (or `OR`) chain are kept once.
pub fn fold(e: &Expr) -> Expr {
    match e {
        Expr::And(..) | Expr::Or(..) => {
            let and = matches!(e, Expr::And(..));
            let mut parts = Vec::new();
            flatten(e, and, &mut parts);
            let mut kept: Vec<Expr> = Vec::new();
            for p in parts {
                let f = fold(p);
                match literal_truth(&f) {
                    // Absorbing element: the whole chain is decided.
                    Some(Some(b)) if b != and => return truth_literal(Some(b)),
                    // Identity element: drop it.
                    Some(Some(_)) => continue,
                    _ => {}
                }
                if !kept.contains(&f) {
                    kept.push(f);
                }
            }
            let mut it = kept.into_iter();
            match it.next() {
                // Every operand was the identity.
                None => truth_literal(Some(and)),
                Some(first) => it.fold(first, |acc, x| {
                    if and {
                        Expr::And(Box::new(acc), Box::new(x))
                    } else {
                        Expr::Or(Box::new(acc), Box::new(x))
                    }
                }),
            }
        }
        Expr::Not(x) => {
            let f = fold(x);
            match literal_truth(&f) {
                Some(t) => truth_literal(crate::eval::not(t)),
                None => Expr::Not(Box::new(f)),
            }
        }
        Expr::Cmp(l, op, r) => {
            let (l, r) = (fold(l), fold(r));
            if let (Expr::Literal(a), Expr::Literal(b)) = (&l, &r) {
                let o = crate::eval::compare(&Some(a.clone()), &Some(b.clone()));
                return truth_literal(o.map(|o| match op {
                    CmpOp::Eq => o.is_eq(),
                    CmpOp::Ne => o.is_ne(),
                    CmpOp::Lt => o.is_lt(),
                    CmpOp::Le => o.is_le(),
                    CmpOp::Gt => o.is_gt(),
                    CmpOp::Ge => o.is_ge(),
                }));
            }
            Expr::Cmp(Box::new(l), *op, Box::new(r))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let x = fold(expr);
            if let Expr::Literal(v) = &x {
                let d = Some(v.clone());
                let mut unknown = false;
                let mut found = false;
                for item in list {
                    match crate::eval::compare(&d, &Some(item.clone())) {
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
                return truth_literal(if *negated { crate::eval::not(t) } else { t });
            }
            Expr::InList {
                expr: Box::new(x),
                list: list.clone(),
                negated: *negated,
            }
        }
        Expr::IsNull { expr, negated } => {
            let x = fold(expr);
            if let Expr::Literal(v) = &x {
                return truth_literal(Some(matches!(v, Value::Null) != *negated));
            }
            Expr::IsNull {
                expr: Box::new(x),
                negated: *negated,
            }
        }
        Expr::IsMissing { expr, negated } => {
            let x = fold(expr);
            if matches!(x, Expr::Literal(_)) {
                // A literal is never missing.
                return truth_literal(Some(*negated));
            }
            Expr::IsMissing {
                expr: Box::new(x),
                negated: *negated,
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            let x = fold(expr);
            if let Expr::Literal(v) = &x {
                let t = match v {
                    Value::String(s) => {
                        Some(crate::eval::like(s, pattern, *case_insensitive) != *negated)
                    }
                    _ => None,
                };
                return truth_literal(t);
            }
            Expr::Like {
                expr: Box::new(x),
                pattern: pattern.clone(),
                negated: *negated,
                case_insensitive: *case_insensitive,
            }
        }
        Expr::TimeBucket { width_ns, expr } => {
            let x = fold(expr);
            if let Expr::Literal(v) = &x {
                return Expr::Literal(match v {
                    Value::Timestamp(t) => Value::Timestamp(t.div_euclid(*width_ns) * width_ns),
                    _ => Value::Null,
                });
            }
            Expr::TimeBucket {
                width_ns: *width_ns,
                expr: Box::new(x),
            }
        }
        other => other.clone(),
    }
}

impl Plan {
    pub fn new(stmt: Statement) -> Result<Self> {
        let mut q = stmt.query;
        if let Some(f) = &q.filter {
            if f.contains_agg() {
                return Err(Error::invalid_input(
                    "aggregate functions are not allowed in WHERE",
                ));
            }
            type_check(f)?;
        }
        // Fold constants (a filter that folds to true keeps every row).
        q.filter = q
            .filter
            .as_ref()
            .map(fold)
            .filter(|f| literal_truth(f) != Some(Some(true)));
        for g in &q.group_by {
            if g.contains_agg() {
                return Err(Error::invalid_input(
                    "aggregate functions are not allowed in GROUP BY",
                ));
            }
        }
        let items: Vec<SelectItem> = match q.select {
            Some(items) => items,
            None => ColumnRef::star()
                .into_iter()
                .map(|c| SelectItem {
                    expr: Expr::Column(c),
                    alias: None,
                })
                .collect(),
        };
        for it in &items {
            type_check(&it.expr)?;
        }
        let aggregate = !q.group_by.is_empty() || items.iter().any(|i| i.expr.contains_agg());
        let mut aggregates: Vec<(AggFunc, Option<Expr>)> = Vec::new();
        let mut outputs = Vec::with_capacity(items.len());
        for it in &items {
            if !aggregate {
                outputs.push(OutputSource::Row(it.expr.clone()));
                continue;
            }
            match &it.expr {
                Expr::Agg { func, arg } => {
                    let key = (*func, arg.as_deref().cloned());
                    let idx = match aggregates.iter().position(|a| a == &key) {
                        Some(i) => i,
                        None => {
                            aggregates.push(key);
                            aggregates.len() - 1
                        }
                    };
                    outputs.push(OutputSource::Agg(idx));
                }
                e if e.contains_agg() => {
                    return Err(Error::invalid_input(format!(
                        "`{e}`: expressions over aggregates are not supported"
                    )))
                }
                e => match q.group_by.iter().position(|g| g == e) {
                    Some(i) => outputs.push(OutputSource::Group(i)),
                    None => {
                        return Err(Error::invalid_input(format!(
                            "`{e}` must appear in GROUP BY or be inside an aggregate"
                        )))
                    }
                },
            }
        }
        let output_names: Vec<String> = items.iter().map(SelectItem::name).collect();
        let mut order = Vec::new();
        for ob in &q.order_by {
            let src = match &ob.key {
                OrderKey::Position(n) => {
                    if *n == 0 || *n > items.len() {
                        return Err(Error::invalid_input(format!(
                            "ORDER BY position {n} is out of range (1..={})",
                            items.len()
                        )));
                    }
                    SortSource::Output(n - 1)
                }
                OrderKey::Alias(a) => match items
                    .iter()
                    .position(|i| i.alias.as_deref() == Some(a.as_str()))
                {
                    Some(i) => SortSource::Output(i),
                    None => {
                        return Err(Error::invalid_input(format!(
                            "ORDER BY `{a}` is neither a column nor an output alias"
                        )))
                    }
                },
                OrderKey::Expr(e) => match items.iter().position(|i| &i.expr == e) {
                    Some(i) => SortSource::Output(i),
                    None if !aggregate && !e.contains_agg() => {
                        type_check(e)?;
                        SortSource::Hidden(e.clone())
                    }
                    None => {
                        return Err(Error::invalid_input(format!(
                            "ORDER BY `{e}` must reference an output column in aggregate queries"
                        )))
                    }
                },
            };
            order.push((src, ob.desc));
        }
        let mut columns = BTreeSet::new();
        let (mut attr_keys, mut resource_keys) = (BTreeSet::new(), BTreeSet::new());
        let (mut whole_attributes, mut whole_resource) = (false, false);
        let mut add = |e: &Expr| {
            e.walk(&mut |x| {
                if let Expr::Column(c) = x {
                    if let Some(id) = column_id(c) {
                        columns.insert(id);
                    }
                    match c {
                        ColumnRef::Attr(k) => {
                            attr_keys.insert(k.clone());
                        }
                        ColumnRef::Resource(k) => {
                            resource_keys.insert(k.clone());
                        }
                        ColumnRef::Attributes => whole_attributes = true,
                        ColumnRef::ResourceAttributes => whole_resource = true,
                        _ => {}
                    }
                }
            })
        };
        items.iter().for_each(|i| add(&i.expr));
        q.filter.iter().for_each(&mut add);
        q.group_by.iter().for_each(&mut add);
        for (s, _) in &order {
            if let SortSource::Hidden(e) = s {
                add(e);
            }
        }
        let (raw_group_attr_keys, raw_group_resource_keys) =
            raw_group_keys(aggregate, &items, &outputs, q.filter.as_ref(), &q.group_by);
        let pruning = q.filter.as_ref().map(extract_pruning).unwrap_or_default();
        Ok(Plan {
            explain: stmt.explain,
            table: q.table,
            output_names,
            outputs,
            filter: q.filter,
            aggregate,
            group_by: q.group_by,
            aggregates,
            order,
            limit: q.limit,
            columns,
            pruning,
            attr_keys,
            whole_attributes,
            resource_keys,
            whole_resource,
            raw_group_attr_keys,
            raw_group_resource_keys,
        })
    }
}

/// Keys of `GROUP BY attributes['k']` / `resource['k']` entries that no other
/// part of the query evaluates per row (see `Plan::raw_group_attr_keys`).
fn raw_group_keys(
    aggregate: bool,
    items: &[SelectItem],
    outputs: &[OutputSource],
    filter: Option<&Expr>,
    group_by: &[Expr],
) -> (BTreeSet<String>, BTreeSet<String>) {
    let (mut attr, mut res) = (BTreeSet::new(), BTreeSet::new());
    if !aggregate {
        return (attr, res);
    }
    // Every other per-row use: filters, aggregate arguments and non-group
    // outputs, and group keys that are expressions over the attribute.
    let mut other: BTreeSet<(bool, String)> = BTreeSet::new();
    let mut note = |e: &Expr| {
        e.walk(&mut |x| match x {
            Expr::Column(ColumnRef::Attr(k)) => {
                other.insert((false, k.clone()));
            }
            Expr::Column(ColumnRef::Resource(k)) => {
                other.insert((true, k.clone()));
            }
            _ => {}
        })
    };
    for (it, src) in items.iter().zip(outputs) {
        if !matches!(src, OutputSource::Group(_)) {
            note(&it.expr);
        }
    }
    filter.into_iter().for_each(&mut note);
    for g in group_by {
        match g {
            Expr::Column(ColumnRef::Attr(k)) => {
                attr.insert(k.clone());
            }
            Expr::Column(ColumnRef::Resource(k)) => {
                res.insert(k.clone());
            }
            e => note(e),
        }
    }
    attr.retain(|k| !other.contains(&(false, k.clone())));
    res.retain(|k| !other.contains(&(true, k.clone())));
    (attr, res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{parse, ParseLimits};

    fn plan(s: &str) -> Result<Plan> {
        Plan::new(parse(s, &ParseLimits::default())?)
    }

    #[test]
    fn filters_are_folded_and_deduplicated() {
        let p =
            plan("SELECT body FROM logs WHERE 1 = 1 AND severity = 'ERROR' AND severity = 'ERROR'")
                .unwrap();
        assert_eq!(
            p.filter.as_ref().unwrap().to_string(),
            plan("SELECT body FROM logs WHERE severity = 'ERROR'")
                .unwrap()
                .filter
                .unwrap()
                .to_string()
        );
        // A filter that folds to true keeps every row: no filter at all.
        assert!(plan("SELECT body FROM logs WHERE 2 > 1 OR service = 'x'")
            .unwrap()
            .filter
            .is_none());
        // ... and one that folds to false stays as a constant.
        let p = plan("SELECT body FROM logs WHERE 1 = 2 AND service = 'x'").unwrap();
        assert_eq!(p.filter, Some(Expr::Literal(Value::Bool(false))));
        // Unknown (NULL) is not an identity element of AND.
        let p = plan("SELECT body FROM logs WHERE NULL = 1 AND service = 'x'").unwrap();
        assert!(matches!(p.filter, Some(Expr::And(..))), "{:?}", p.filter);
        // Folding feeds pruning: the time bound survives the constant.
        let p = plan(
            "SELECT body FROM logs WHERE TRUE AND event_time >= TIMESTAMP '2026-01-01T00:00:00Z'",
        )
        .unwrap();
        assert!(p.pruning.time_lo.is_some());
    }

    #[test]
    fn raw_group_keys_only_when_used_solely_as_group_keys() {
        let raw = |q: &str| {
            let p = plan(q).unwrap();
            (p.raw_group_attr_keys, p.raw_group_resource_keys)
        };
        let set = |ks: &[&str]| ks.iter().map(|k| (*k).to_owned()).collect::<BTreeSet<_>>();
        assert_eq!(
            raw("SELECT attributes['u'] AS u, count(*) FROM logs GROUP BY attributes['u']"),
            (set(&["u"]), set(&[]))
        );
        assert_eq!(
            raw("SELECT count(*) FROM logs GROUP BY attributes['u'], resource['h']"),
            (set(&["u"]), set(&["h"]))
        );
        // Also filtered on, aggregated, or grouped by an expression: decoded.
        for q in [
            "SELECT count(*) FROM logs WHERE attributes['u'] = 'a' GROUP BY attributes['u']",
            "SELECT count(attributes['u']) FROM logs GROUP BY attributes['u']",
            "SELECT count(*) FROM logs GROUP BY attributes['u'], attributes['u'] = 'x'",
            "SELECT attributes['u'] FROM logs",
        ] {
            assert_eq!(raw(q), (set(&[]), set(&[])), "{q}");
        }
    }

    #[test]
    fn projection_and_pruning() {
        let p = plan("SELECT body FROM logs WHERE event_time >= TIMESTAMP '1970-01-01T00:00:01Z' \
                      AND event_time < TIMESTAMP '1970-01-01T00:00:02Z' AND severity IN ('ERROR','FATAL') \
                      AND attributes['region'] = 'eu' AND (service = 'a' OR attributes['x'] = 1)")
        .unwrap();
        assert_eq!(
            p.columns,
            BTreeSet::from([
                ColumnId::Body,
                ColumnId::EventTime,
                ColumnId::Severity,
                ColumnId::Attributes,
                ColumnId::Service
            ])
        );
        assert_eq!(p.pruning.time_lo, Some(1_000_000_000));
        assert_eq!(p.pruning.time_hi, Some(1_999_999_999));
        assert_eq!(p.pruning.severities, Some(BTreeSet::from([17, 21])));
        // `x` is inside an OR, so it must not become a required key.
        assert_eq!(
            p.pruning.required_attrs,
            BTreeSet::from(["region".to_owned()])
        );
    }

    #[test]
    fn negations_do_not_prune() {
        let p = plan(
            "SELECT * FROM logs WHERE NOT attributes['a'] = 1 AND attributes['b'] IS MISSING \
                      AND attributes['c'] NOT IN (1)",
        )
        .unwrap();
        assert!(p.pruning.required_attrs.is_empty());
    }

    #[test]
    fn validation_errors() {
        for bad in [
            "SELECT service, count(*) FROM logs",
            "SELECT * FROM logs GROUP BY service",
            "SELECT count(*) FROM logs WHERE count(*) > 1",
            "SELECT * FROM logs WHERE event_time > '2026-01-01'",
            "SELECT * FROM logs WHERE service = 5",
            "SELECT * FROM logs WHERE event_time LIKE 'x'",
            "SELECT service FROM logs ORDER BY 2",
            "SELECT service, count(*) FROM logs GROUP BY service ORDER BY body",
            "SELECT service FROM logs ORDER BY nope",
        ] {
            assert!(plan(bad).is_err(), "{bad} accepted");
        }
    }

    #[test]
    fn aggregate_outputs() {
        let p = plan(
            "SELECT service, count(*) AS n, count(*) FROM logs GROUP BY service ORDER BY n DESC",
        )
        .unwrap();
        assert_eq!(p.aggregates.len(), 1, "duplicate aggregates are shared");
        assert_eq!(
            p.outputs,
            vec![
                OutputSource::Group(0),
                OutputSource::Agg(0),
                OutputSource::Agg(0)
            ]
        );
        assert_eq!(p.order, vec![(SortSource::Output(1), true)]);
    }
}
