//! Recursive-descent parser for the StrataDB query subset. The grammar is
//! normative in `docs/query-language/grammar.md`; anything not listed there is
//! rejected with a positioned error (spec §2.3: never expose unimplemented SQL).

use stratadb_common::time::{parse_rfc3339, NANOS_PER_SEC};
use stratadb_common::{Error, Result};
use stratadb_schema::Value;

use crate::ast::*;
use crate::lexer::{syntax_error, tokenize, Tok, Token};

/// Parser limits (spec §13.2, §18.8).
#[derive(Debug, Clone, Copy)]
pub struct ParseLimits {
    pub max_query_bytes: usize,
    pub max_depth: usize,
    pub max_in_list: usize,
}

impl Default for ParseLimits {
    fn default() -> Self {
        Self {
            max_query_bytes: 64 * 1024,
            max_depth: 64,
            max_in_list: 10_000,
        }
    }
}

const RESERVED: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "ORDER",
    "LIMIT",
    "AND",
    "OR",
    "NOT",
    "IS",
    "NULL",
    "MISSING",
    "IN",
    "LIKE",
    "ILIKE",
    "AS",
    "ASC",
    "DESC",
    "TRUE",
    "FALSE",
    "TIMESTAMP",
    "INTERVAL",
    "EXPLAIN",
    "ANALYZE",
];

pub fn parse(src: &str, lim: &ParseLimits) -> Result<Statement> {
    if src.len() > lim.max_query_bytes {
        return Err(Error::invalid_input(format!(
            "query is {} bytes; limit is {}",
            src.len(),
            lim.max_query_bytes
        )));
    }
    let mut p = Parser {
        toks: tokenize(src)?,
        i: 0,
        depth: 0,
        lim: *lim,
    };
    let stmt = p.statement()?;
    if p.peek() == &Tok::Semi {
        p.i += 1;
    }
    if p.peek() != &Tok::Eof {
        return Err(p.err("unexpected input after end of query"));
    }
    Ok(stmt)
}

struct Parser {
    toks: Vec<Token>,
    i: usize,
    depth: usize,
    lim: ParseLimits,
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.toks[self.i.min(self.toks.len() - 1)].tok
    }

    fn peek_at(&self, n: usize) -> &Tok {
        &self.toks[(self.i + n).min(self.toks.len() - 1)].tok
    }

    fn pos(&self) -> usize {
        self.toks[self.i.min(self.toks.len() - 1)].pos
    }

    fn err(&self, msg: impl std::fmt::Display) -> Error {
        let near = match self.peek() {
            Tok::Eof => "end of input".to_owned(),
            t => format!("{t:?}"),
        };
        syntax_error(self.pos(), format!("{msg} (near {near})"))
    }

    fn next(&mut self) -> Tok {
        let t = self.peek().clone();
        if self.i < self.toks.len() {
            self.i += 1;
        }
        t
    }

    fn is_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Tok::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn is_kw_at(&self, n: usize, kw: &str) -> bool {
        matches!(self.peek_at(n), Tok::Ident(s) if s.eq_ignore_ascii_case(kw))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(self.err(format!("expected {kw}")))
        }
    }

    fn expect(&mut self, t: Tok, what: &str) -> Result<()> {
        if self.peek() == &t {
            self.i += 1;
            Ok(())
        } else {
            Err(self.err(format!("expected {what}")))
        }
    }

    fn enter(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > self.lim.max_depth {
            return Err(self.err(format!("expression nesting exceeds {}", self.lim.max_depth)));
        }
        Ok(())
    }

    fn statement(&mut self) -> Result<Statement> {
        let explain = if self.eat_kw("EXPLAIN") {
            if self.eat_kw("ANALYZE") {
                ExplainMode::Analyze
            } else {
                ExplainMode::Plan
            }
        } else {
            ExplainMode::None
        };
        Ok(Statement {
            explain,
            query: self.query()?,
        })
    }

    fn query(&mut self) -> Result<Query> {
        self.expect_kw("SELECT")?;
        let select = if self.peek() == &Tok::Star {
            self.i += 1;
            None
        } else {
            let mut items = vec![self.select_item()?];
            while self.peek() == &Tok::Comma {
                self.i += 1;
                items.push(self.select_item()?);
            }
            Some(items)
        };
        self.expect_kw("FROM")?;
        let table = self.table()?;
        let filter = if self.eat_kw("WHERE") {
            Some(self.expr()?)
        } else {
            None
        };
        let mut group_by = Vec::new();
        if self.eat_kw("GROUP") {
            self.expect_kw("BY")?;
            group_by.push(self.expr()?);
            while self.peek() == &Tok::Comma {
                self.i += 1;
                group_by.push(self.expr()?);
            }
        }
        let mut order_by = Vec::new();
        if self.eat_kw("ORDER") {
            self.expect_kw("BY")?;
            loop {
                let alias = match self.peek() {
                    Tok::Ident(name) | Tok::QuotedIdent(name)
                        if ColumnRef::from_name(name).is_none()
                            && !RESERVED.contains(&name.to_ascii_uppercase().as_str())
                            && !name.eq_ignore_ascii_case("resource")
                            && !matches!(self.peek_at(1), Tok::LParen | Tok::LBracket) =>
                    {
                        Some(name.clone())
                    }
                    _ => None,
                };
                let key = match self.peek() {
                    _ if alias.is_some() => {
                        self.i += 1;
                        OrderKey::Alias(alias.unwrap_or_default())
                    }
                    Tok::Int(n) => {
                        let n = *n;
                        self.i += 1;
                        if n < 1 {
                            return Err(self.err("ORDER BY position must be >= 1"));
                        }
                        OrderKey::Position(n as usize)
                    }
                    _ => OrderKey::Expr(self.expr()?),
                };
                let desc = if self.eat_kw("DESC") {
                    true
                } else {
                    self.eat_kw("ASC");
                    false
                };
                order_by.push(OrderBy { key, desc });
                if self.peek() != &Tok::Comma {
                    break;
                }
                self.i += 1;
            }
        }
        let limit = if self.eat_kw("LIMIT") {
            match self.next() {
                Tok::Int(n) if n >= 0 => Some(n as u64),
                _ => {
                    self.i -= 1;
                    return Err(self.err("LIMIT requires a non-negative integer"));
                }
            }
        } else {
            None
        };
        Ok(Query {
            table,
            select,
            filter,
            group_by,
            order_by,
            limit,
        })
    }

    /// `logs` or `system.query_log`.
    fn table(&mut self) -> Result<Table> {
        let name = |t: &Tok| match t {
            Tok::Ident(t) | Tok::QuotedIdent(t) => Some(t.to_ascii_lowercase()),
            _ => None,
        };
        let first = name(self.peek());
        if first.as_deref() == Some("logs") {
            self.i += 1;
            return Ok(Table::Logs);
        }
        if first.as_deref() == Some("system")
            && self.peek_at(1) == &Tok::Dot
            && name(self.peek_at(2)).as_deref() == Some("query_log")
        {
            self.i += 3;
            return Ok(Table::QueryLog);
        }
        Err(self.err("unknown table; the tables are `logs` and `system.query_log`"))
    }

    fn select_item(&mut self) -> Result<SelectItem> {
        let expr = self.expr()?;
        let alias = if self.eat_kw("AS") {
            match self.next() {
                Tok::Ident(s) | Tok::QuotedIdent(s) => Some(s),
                _ => {
                    self.i -= 1;
                    return Err(self.err("expected alias after AS"));
                }
            }
        } else {
            None
        };
        Ok(SelectItem { expr, alias })
    }

    fn expr(&mut self) -> Result<Expr> {
        self.enter()?;
        let mut e = self.and_expr()?;
        while self.eat_kw("OR") {
            e = Expr::Or(Box::new(e), Box::new(self.and_expr()?));
        }
        self.depth -= 1;
        Ok(e)
    }

    fn and_expr(&mut self) -> Result<Expr> {
        let mut e = self.not_expr()?;
        while self.eat_kw("AND") {
            e = Expr::And(Box::new(e), Box::new(self.not_expr()?));
        }
        Ok(e)
    }

    fn not_expr(&mut self) -> Result<Expr> {
        if self.eat_kw("NOT") {
            self.enter()?;
            let e = Expr::Not(Box::new(self.not_expr()?));
            self.depth -= 1;
            return Ok(e);
        }
        self.predicate()
    }

    fn predicate(&mut self) -> Result<Expr> {
        let left = self.primary()?;
        let op = match self.peek() {
            Tok::Eq => Some(CmpOp::Eq),
            Tok::Ne => Some(CmpOp::Ne),
            Tok::Lt => Some(CmpOp::Lt),
            Tok::Le => Some(CmpOp::Le),
            Tok::Gt => Some(CmpOp::Gt),
            Tok::Ge => Some(CmpOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.i += 1;
            let right = self.primary()?;
            return Ok(Expr::Cmp(Box::new(left), op, Box::new(right)));
        }
        if self.eat_kw("IS") {
            let negated = self.eat_kw("NOT");
            if self.eat_kw("NULL") {
                return Ok(Expr::IsNull {
                    expr: Box::new(left),
                    negated,
                });
            }
            if self.eat_kw("MISSING") {
                return Ok(Expr::IsMissing {
                    expr: Box::new(left),
                    negated,
                });
            }
            return Err(self.err("expected NULL or MISSING after IS"));
        }
        let negated = if self.is_kw("NOT")
            && (self.is_kw_at(1, "IN") || self.is_kw_at(1, "LIKE") || self.is_kw_at(1, "ILIKE"))
        {
            self.i += 1;
            true
        } else {
            false
        };
        if self.eat_kw("IN") {
            self.expect(Tok::LParen, "( after IN")?;
            let mut list = vec![self.literal()?];
            while self.peek() == &Tok::Comma {
                self.i += 1;
                list.push(self.literal()?);
                if list.len() > self.lim.max_in_list {
                    return Err(self.err(format!("IN list exceeds {} items", self.lim.max_in_list)));
                }
            }
            self.expect(Tok::RParen, ") to close IN list")?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated,
            });
        }
        for (kw, ci) in [("LIKE", false), ("ILIKE", true)] {
            if self.eat_kw(kw) {
                return match self.next() {
                    Tok::Str(pattern) => Ok(Expr::Like {
                        expr: Box::new(left),
                        pattern,
                        negated,
                        case_insensitive: ci,
                    }),
                    _ => {
                        self.i -= 1;
                        Err(self.err(format!("{kw} requires a string pattern")))
                    }
                };
            }
        }
        if negated {
            return Err(self.err("expected IN, LIKE or ILIKE after NOT"));
        }
        Ok(left)
    }

    /// A literal value (used in IN lists).
    fn literal(&mut self) -> Result<Value> {
        match self.primary()? {
            Expr::Literal(v) => Ok(v),
            _ => Err(self.err("expected a literal")),
        }
    }

    fn primary(&mut self) -> Result<Expr> {
        let tok = self.next();
        match tok {
            Tok::Str(s) => Ok(Expr::Literal(Value::String(s))),
            Tok::Int(n) => Ok(Expr::Literal(Value::Int(n))),
            Tok::Float(f) => Ok(Expr::Literal(Value::Float(f))),
            Tok::Minus => match self.next() {
                Tok::Int(n) => Ok(Expr::Literal(Value::Int(-n))),
                Tok::Float(f) => Ok(Expr::Literal(Value::Float(-f))),
                _ => {
                    self.i -= 1;
                    Err(self.err("expected a number after '-' (arithmetic is not supported)"))
                }
            },
            Tok::LParen => {
                let e = self.expr()?;
                self.expect(Tok::RParen, ")")?;
                Ok(e)
            }
            Tok::QuotedIdent(name) => self.column(&name),
            Tok::Ident(name) => {
                let upper = name.to_ascii_uppercase();
                match upper.as_str() {
                    "TRUE" => return Ok(Expr::Literal(Value::Bool(true))),
                    "FALSE" => return Ok(Expr::Literal(Value::Bool(false))),
                    "NULL" => return Ok(Expr::Literal(Value::Null)),
                    "TIMESTAMP" => return self.timestamp_literal(),
                    _ => {}
                }
                if self.peek() == &Tok::LParen {
                    return self.function(&upper);
                }
                if RESERVED.contains(&upper.as_str()) {
                    self.i -= 1;
                    return Err(self.err(format!("unexpected keyword {upper}")));
                }
                self.column(&name)
            }
            _ => {
                self.i -= 1;
                Err(self.err("expected an expression"))
            }
        }
    }

    fn timestamp_literal(&mut self) -> Result<Expr> {
        match self.next() {
            Tok::Str(s) => {
                // A literal without a zone designator is interpreted as UTC.
                let t = parse_rfc3339(&s).or_else(|_| parse_rfc3339(&format!("{s}Z")))?;
                Ok(Expr::Literal(Value::Timestamp(t)))
            }
            _ => {
                self.i -= 1;
                Err(self.err("TIMESTAMP requires a quoted RFC 3339 string"))
            }
        }
    }

    fn column(&mut self, name: &str) -> Result<Expr> {
        let lower = name.to_ascii_lowercase();
        let map_kind = match lower.as_str() {
            "attributes" => Some(true),
            "resource_attributes" | "resource" => Some(false),
            _ => None,
        };
        if let Some(is_attr) = map_kind {
            if self.peek() == &Tok::LBracket {
                self.i += 1;
                let key = match self.next() {
                    Tok::Str(k) => k,
                    _ => {
                        self.i -= 1;
                        return Err(self.err("expected a quoted key, e.g. attributes['region']"));
                    }
                };
                self.expect(Tok::RBracket, "]")?;
                return Ok(Expr::Column(if is_attr {
                    ColumnRef::Attr(key)
                } else {
                    ColumnRef::Resource(key)
                }));
            }
            if lower == "resource" {
                return Err(self.err("use resource_attributes['key']"));
            }
        }
        if self.peek() == &Tok::Dot {
            return Err(self.err("dotted names are not supported; use attributes['a.b']"));
        }
        ColumnRef::from_name(name).map(Expr::Column).ok_or_else(|| {
            self.i -= 1;
            let e = self.err(format!(
                "unknown column `{name}`; attribute fields are accessed as attributes['{name}']"
            ));
            self.i += 1;
            e
        })
    }

    fn function(&mut self, upper: &str) -> Result<Expr> {
        self.expect(Tok::LParen, "(")?;
        let agg = match upper {
            "COUNT" => Some(AggFunc::Count),
            "MIN" => Some(AggFunc::Min),
            "MAX" => Some(AggFunc::Max),
            "SUM" => Some(AggFunc::Sum),
            "AVG" => Some(AggFunc::Avg),
            _ => None,
        };
        if let Some(func) = agg {
            let arg = if func == AggFunc::Count && self.peek() == &Tok::Star {
                self.i += 1;
                None
            } else {
                let a = self.expr()?;
                if a.contains_agg() {
                    return Err(self.err("aggregate functions cannot be nested"));
                }
                Some(Box::new(a))
            };
            self.expect(Tok::RParen, ")")?;
            return Ok(Expr::Agg { func, arg });
        }
        if upper == "TIME_BUCKET" {
            self.expect_kw("INTERVAL")?;
            let width_ns = match self.next() {
                Tok::Str(s) => parse_interval(&s).map_err(|m| self.err(m))?,
                _ => {
                    self.i -= 1;
                    return Err(self.err("INTERVAL requires a quoted string like '5 minutes'"));
                }
            };
            self.expect(Tok::Comma, ",")?;
            let expr = Box::new(self.expr()?);
            self.expect(Tok::RParen, ")")?;
            return Ok(Expr::TimeBucket { width_ns, expr });
        }
        Err(self.err(format!("unknown function {upper}")))
    }
}

/// Parse `'<n> <unit>'` into nanoseconds.
pub fn parse_interval(s: &str) -> std::result::Result<i64, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("interval {s:?} needs a unit"))?;
    let n: i64 = s[..split]
        .parse()
        .map_err(|_| format!("interval {s:?} needs a positive integer"))?;
    let unit = s[split..].trim().to_ascii_lowercase();
    let mult = match unit.as_str() {
        "ns" | "nanosecond" | "nanoseconds" => 1,
        "us" | "microsecond" | "microseconds" => 1_000,
        "ms" | "millisecond" | "milliseconds" => 1_000_000,
        "s" | "sec" | "second" | "seconds" => NANOS_PER_SEC,
        "m" | "min" | "minute" | "minutes" => 60 * NANOS_PER_SEC,
        "h" | "hour" | "hours" => 3_600 * NANOS_PER_SEC,
        "d" | "day" | "days" => 86_400 * NANOS_PER_SEC,
        _ => return Err(format!("unknown interval unit {unit:?}")),
    };
    match n.checked_mul(mult) {
        Some(v) if v > 0 => Ok(v),
        _ => Err(format!(
            "interval {s:?} must be positive and fit in 292 years"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Result<Statement> {
        parse(s, &ParseLimits::default())
    }

    #[test]
    fn parses_spec_example() {
        let s = p(
            "SELECT event_time, service, severity, body, trace_id FROM logs \
                   WHERE event_time >= TIMESTAMP '2026-11-20 10:00:00' \
                   AND event_time < TIMESTAMP '2026-11-20 11:00:00' AND severity = 'ERROR' \
                   ORDER BY event_time DESC LIMIT 100;",
        )
        .unwrap();
        assert_eq!(s.query.select.as_ref().unwrap().len(), 5);
        assert_eq!(s.query.limit, Some(100));
        assert!(s.query.order_by[0].desc);
    }

    #[test]
    fn parses_aggregates_and_buckets() {
        let s = p("SELECT service, time_bucket(INTERVAL '5 minutes', event_time) AS b, count(*) \
                   FROM logs GROUP BY service, time_bucket(INTERVAL '5 minutes', event_time) ORDER BY 3 DESC")
        .unwrap();
        assert_eq!(s.query.group_by.len(), 2);
        assert_eq!(s.query.order_by[0].key, OrderKey::Position(3));
    }

    #[test]
    fn tables() {
        assert_eq!(p("SELECT * FROM logs").unwrap().query.table, Table::Logs);
        assert_eq!(p("SELECT * FROM LOGS").unwrap().query.table, Table::Logs);
        for q in [
            "SELECT * FROM system.query_log",
            "SELECT * FROM SYSTEM.Query_Log",
        ] {
            assert_eq!(p(q).unwrap().query.table, Table::QueryLog);
        }
        for bad in [
            "SELECT * FROM system",
            "SELECT * FROM system.",
            "SELECT * FROM system.parts",
            "SELECT * FROM other",
        ] {
            let e = p(bad).unwrap_err().to_string();
            assert!(e.contains("unknown table"), "{bad}: {e}");
        }
    }

    #[test]
    fn parses_predicates() {
        let s = p(
            "SELECT * FROM logs WHERE attributes['http.status_code'] >= 500 \
                   AND resource_attributes['host'] NOT IN ('a', 'b') AND body ILIKE '%time%out%' \
                   AND trace_id IS NOT MISSING AND NOT (service = 'x' OR service IS NULL)",
        )
        .unwrap();
        assert!(s.query.filter.is_some());
        assert!(s.query.select.is_none());
    }

    #[test]
    fn rejects_unsupported_syntax_with_positions() {
        for bad in [
            "SELECT * FROM other",
            "SELECT * FROM logs JOIN x",
            "SELECT region FROM logs",
            "SELECT a.b FROM logs",
            "SELECT count(count(*)) FROM logs",
            "SELECT * FROM logs WHERE 1 + 2 = 3",
            "SELECT * FROM logs LIMIT -1",
            "SELECT now() FROM logs",
            "UPDATE logs SET x = 1",
            "SELECT * FROM logs WHERE x NOT = 1",
            "SELECT time_bucket(INTERVAL '5 parsecs', event_time) FROM logs",
        ] {
            let e = p(bad).unwrap_err();
            assert_eq!(e.kind(), stratadb_common::ErrorKind::InvalidInput, "{bad}");
        }
        let e = p("SELECT * FROM logs WHERE zzz = 1").unwrap_err();
        assert!(e.to_string().contains("offset 25"), "{e}");
    }

    #[test]
    fn enforces_limits() {
        let deep = format!(
            "SELECT * FROM logs WHERE {}service = 'a'{}",
            "(".repeat(100),
            ")".repeat(100)
        );
        assert!(p(&deep).is_err());
        let nots = format!(
            "SELECT * FROM logs WHERE {}service = 'a'",
            "NOT ".repeat(100)
        );
        assert!(p(&nots).is_err());
        let long = format!("SELECT * FROM logs WHERE body = '{}'", "x".repeat(70_000));
        assert!(p(&long).is_err());
    }

    #[test]
    fn intervals() {
        assert_eq!(parse_interval("5 minutes").unwrap(), 300 * NANOS_PER_SEC);
        assert_eq!(parse_interval("1h").unwrap(), 3600 * NANOS_PER_SEC);
        assert!(parse_interval("0 s").is_err());
        assert!(parse_interval("99999999999 days").is_err());
    }
}
