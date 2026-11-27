//! Query AST. Column references are resolved during parsing because v0 has a
//! single implicit table (`logs`) with a fixed set of canonical columns plus
//! typed attribute maps.

use std::fmt;

use stratadb_schema::Value;

/// A resolved column reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ColumnRef {
    EventTime,
    IngestTime,
    Service,
    /// Normalized severity as upper-case text (`'ERROR'`).
    Severity,
    /// Normalized severity number (OTLP bucket base: 1, 5, 9, 13, 17, 21; 0 = unspecified).
    SeverityNumber,
    SeverityText,
    Body,
    TraceId,
    SpanId,
    EventId,
    /// The snapshot's tenant (constant per query; derived from authentication).
    TenantId,
    SchemaVersion,
    /// The whole attributes map.
    Attributes,
    ResourceAttributes,
    /// `attributes['key']`
    Attr(String),
    /// `resource_attributes['key']`
    Resource(String),
}

impl ColumnRef {
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "event_time" => ColumnRef::EventTime,
            "ingest_time" => ColumnRef::IngestTime,
            "service" => ColumnRef::Service,
            "severity" => ColumnRef::Severity,
            "severity_number" => ColumnRef::SeverityNumber,
            "severity_text" => ColumnRef::SeverityText,
            "body" => ColumnRef::Body,
            "trace_id" => ColumnRef::TraceId,
            "span_id" => ColumnRef::SpanId,
            "event_id" => ColumnRef::EventId,
            "tenant_id" => ColumnRef::TenantId,
            "schema_version" => ColumnRef::SchemaVersion,
            "attributes" => ColumnRef::Attributes,
            "resource_attributes" => ColumnRef::ResourceAttributes,
            _ => return None,
        })
    }

    /// Columns returned by `SELECT *`, in order.
    pub fn star() -> Vec<ColumnRef> {
        vec![
            ColumnRef::EventTime,
            ColumnRef::IngestTime,
            ColumnRef::Service,
            ColumnRef::Severity,
            ColumnRef::SeverityText,
            ColumnRef::Body,
            ColumnRef::TraceId,
            ColumnRef::SpanId,
            ColumnRef::EventId,
            ColumnRef::Attributes,
            ColumnRef::ResourceAttributes,
        ]
    }
}

fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnRef::EventTime => f.write_str("event_time"),
            ColumnRef::IngestTime => f.write_str("ingest_time"),
            ColumnRef::Service => f.write_str("service"),
            ColumnRef::Severity => f.write_str("severity"),
            ColumnRef::SeverityNumber => f.write_str("severity_number"),
            ColumnRef::SeverityText => f.write_str("severity_text"),
            ColumnRef::Body => f.write_str("body"),
            ColumnRef::TraceId => f.write_str("trace_id"),
            ColumnRef::SpanId => f.write_str("span_id"),
            ColumnRef::EventId => f.write_str("event_id"),
            ColumnRef::TenantId => f.write_str("tenant_id"),
            ColumnRef::SchemaVersion => f.write_str("schema_version"),
            ColumnRef::Attributes => f.write_str("attributes"),
            ColumnRef::ResourceAttributes => f.write_str("resource_attributes"),
            ColumnRef::Attr(k) => write!(f, "attributes[{}]", quote(k)),
            ColumnRef::Resource(k) => write!(f, "resource_attributes[{}]", quote(k)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    pub fn flip(self) -> Self {
        match self {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::Le => CmpOp::Ge,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::Ge => CmpOp::Le,
            o => o,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Min,
    Max,
    Sum,
    Avg,
}

impl AggFunc {
    fn as_str(self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(ColumnRef),
    Literal(Value),
    Cmp(Box<Expr>, CmpOp, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    IsMissing {
        expr: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Value>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: String,
        negated: bool,
        case_insensitive: bool,
    },
    /// `time_bucket(INTERVAL '...', expr)`; width in nanoseconds.
    TimeBucket {
        width_ns: i64,
        expr: Box<Expr>,
    },
    /// `None` argument = `COUNT(*)`.
    Agg {
        func: AggFunc,
        arg: Option<Box<Expr>>,
    },
}

impl Expr {
    pub fn contains_agg(&self) -> bool {
        let mut found = false;
        self.walk(&mut |e| found |= matches!(e, Expr::Agg { .. }));
        found
    }

    /// Pre-order traversal.
    pub fn walk(&self, f: &mut dyn FnMut(&Expr)) {
        f(self);
        match self {
            Expr::Column(_) | Expr::Literal(_) => {}
            Expr::Cmp(a, _, b) | Expr::And(a, b) | Expr::Or(a, b) => {
                a.walk(f);
                b.walk(f);
            }
            Expr::Not(e)
            | Expr::IsNull { expr: e, .. }
            | Expr::IsMissing { expr: e, .. }
            | Expr::InList { expr: e, .. }
            | Expr::Like { expr: e, .. }
            | Expr::TimeBucket { expr: e, .. } => e.walk(f),
            Expr::Agg { arg, .. } => {
                if let Some(a) = arg {
                    a.walk(f);
                }
            }
        }
    }
}

fn fmt_lit(v: &Value, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match v {
        Value::Null => f.write_str("NULL"),
        Value::Bool(b) => f.write_str(if *b { "TRUE" } else { "FALSE" }),
        Value::Int(i) => write!(f, "{i}"),
        Value::UInt(u) => write!(f, "{u}"),
        Value::Float(x) => write!(f, "{x:?}"),
        Value::String(s) => f.write_str(&quote(s)),
        Value::Timestamp(t) => write!(
            f,
            "TIMESTAMP '{}'",
            stratadb_common::time::format_rfc3339(*t)
        ),
        other => write!(f, "<{}>", other.type_name()),
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Column(c) => write!(f, "{c}"),
            Expr::Literal(v) => fmt_lit(v, f),
            Expr::Cmp(a, op, b) => write!(f, "{a} {} {b}", op.as_str()),
            Expr::And(a, b) => write!(f, "({a} AND {b})"),
            Expr::Or(a, b) => write!(f, "({a} OR {b})"),
            Expr::Not(e) => write!(f, "NOT {e}"),
            Expr::IsNull { expr, negated } => {
                write!(f, "{expr} IS {}NULL", if *negated { "NOT " } else { "" })
            }
            Expr::IsMissing { expr, negated } => {
                write!(f, "{expr} IS {}MISSING", if *negated { "NOT " } else { "" })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                write!(f, "{expr} {}IN (", if *negated { "NOT " } else { "" })?;
                for (i, v) in list.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    fmt_lit(v, f)?;
                }
                f.write_str(")")
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => write!(
                f,
                "{expr} {}{} {}",
                if *negated { "NOT " } else { "" },
                if *case_insensitive { "ILIKE" } else { "LIKE" },
                quote(pattern)
            ),
            Expr::TimeBucket { width_ns, expr } => {
                write!(f, "time_bucket(INTERVAL '{width_ns} ns', {expr})")
            }
            Expr::Agg { func, arg } => match arg {
                None => write!(f, "{}(*)", func.as_str()),
                Some(a) => write!(f, "{}({a})", func.as_str()),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

impl SelectItem {
    pub fn name(&self) -> String {
        self.alias.clone().unwrap_or_else(|| self.expr.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderKey {
    Expr(Expr),
    /// 1-based output column position.
    Position(usize),
    /// Output column alias.
    Alias(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub key: OrderKey,
    pub desc: bool,
}

/// A table a query reads. Every table has the event schema; system tables
/// are tenant-scoped like `logs` (spec §17).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
    /// The tenant's events.
    Logs,
    /// `system.query_log`: one row per query the tenant ran (STRATA-SYS-13-009).
    QueryLog,
}

impl Table {
    pub fn name(self) -> &'static str {
        match self {
            Table::Logs => "logs",
            Table::QueryLog => "system.query_log",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// The table in `FROM`.
    pub table: Table,
    /// `None` = `SELECT *`.
    pub select: Option<Vec<SelectItem>>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExplainMode {
    None,
    Plan,
    Analyze,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    pub explain: ExplainMode,
    pub query: Query,
}
