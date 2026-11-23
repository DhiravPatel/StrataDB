//! Semantic analysis and logical planning (spec §13.3, §13.4).
//!
//! Responsibilities: expand `SELECT *`, validate aggregate/grouping rules,
//! statically reject comparisons between canonical columns and literals of the
//! wrong type (no silent coercion), resolve ORDER BY keys, compute the set of
//! segment columns to read (projection pruning), and extract *sound* segment
//! pruning predicates from top-level conjuncts of `WHERE`.

use std::collections::BTreeSet;

use stratadb_common::{Error, Result};
use stratadb_schema::{Severity, Value};
use stratadb_segment::bloom::BloomField;
use stratadb_segment::ColumnId;

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

/// Segment pruning predicates. Each is implied by the filter, so a segment
/// failing any of them cannot contain a matching row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Pruning {
    /// Inclusive event_time bounds.
    pub time_lo: Option<i64>,
    pub time_hi: Option<i64>,
    /// Attribute keys that must be present for any row to match.
    pub required_attrs: BTreeSet<String>,
    pub required_resource: BTreeSet<String>,
    /// Severity numbers one of which a matching row must have.
    pub severities: Option<BTreeSet<u8>>,
    /// Bloom-filter probes: a matching row has one of these string values in
    /// the field. Only string literals are recorded (see segment::bloom).
    pub bloom: Vec<(BloomField, Vec<String>)>,
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub explain: ExplainMode,
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

fn bloom_field(c: &ColumnRef) -> Option<BloomField> {
    Some(match c {
        ColumnRef::TraceId => BloomField::TraceId,
        ColumnRef::SpanId => BloomField::SpanId,
        ColumnRef::EventId => BloomField::EventId,
        ColumnRef::Attr(k) => BloomField::Attr(k.clone()),
        ColumnRef::Resource(k) => BloomField::Resource(k.clone()),
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
                if let (CmpOp::Eq, Value::String(v), Some(f)) = (op, lit, bloom_field(col)) {
                    p.bloom.push((f, vec![v.clone()]));
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
                    if let (Some(f), Some(vals)) = (bloom_field(col), strings) {
                        p.bloom.push((f, vals));
                    }
                }
            }
            Expr::Like {
                expr,
                negated: false,
                ..
            }
            | Expr::IsNull {
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

impl Plan {
    pub fn new(stmt: Statement) -> Result<Self> {
        let q = stmt.query;
        if let Some(f) = &q.filter {
            if f.contains_agg() {
                return Err(Error::invalid_input(
                    "aggregate functions are not allowed in WHERE",
                ));
            }
            type_check(f)?;
        }
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
        let pruning = q.filter.as_ref().map(extract_pruning).unwrap_or_default();
        Ok(Plan {
            explain: stmt.explain,
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{parse, ParseLimits};

    fn plan(s: &str) -> Result<Plan> {
        Plan::new(parse(s, &ParseLimits::default())?)
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
