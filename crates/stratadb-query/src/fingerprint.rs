//! Normalized query fingerprints (spec STRATA-SYS-13-009).
//!
//! Two queries that differ only in literal values, whitespace, comments or
//! keyword case get the same fingerprint, so a workload can be ranked by
//! total CPU per query shape. Normalization works on tokens:
//! - string, integer and float literals become `?`, and a negative number
//!   (`- 5`) becomes one `?`;
//! - a run of `?` separated by commas collapses to one `?`, so `IN (1, 2)`
//!   and `IN (1, 2, 3)` share a fingerprint;
//! - attribute subscripts keep their key (`attributes['region']`): a
//!   different key reads a different column, so it is a different query;
//! - unquoted identifiers and keywords are lowercased; quoted identifiers
//!   keep their case.
//!
//! The fingerprint is FNV-1a 64 of the normalized text. It is stable across
//! processes, versions and platforms; changing it needs a changelog entry,
//! since stored query logs would no longer group with new entries.

use crate::lexer::{tokenize, Tok};

/// Longest normalized text kept (longer text is still hashed in full).
pub const MAX_NORMALIZED_BYTES: usize = 4096;

/// A query's normalized text and its 64-bit fingerprint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint {
    pub hash: u64,
    /// Normalized text, truncated to [`MAX_NORMALIZED_BYTES`] on a character
    /// boundary.
    pub normalized: String,
}

impl Fingerprint {
    /// The hash as 16 lowercase hex digits.
    pub fn hex(&self) -> String {
        format!("{:016x}", self.hash)
    }
}

/// FNV-1a, 64-bit.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Debug, Clone, PartialEq)]
enum Part {
    Placeholder,
    Text(String),
    Comma,
    Minus,
}

/// Normalized text of `sql`. Text the lexer rejects is normalized by
/// collapsing whitespace and lowercasing, so failed queries still group.
pub fn normalize(sql: &str) -> String {
    let Ok(toks) = tokenize(sql) else {
        return sql
            .split_whitespace()
            .map(str::to_lowercase)
            .collect::<Vec<_>>()
            .join(" ");
    };
    let mut parts: Vec<Part> = Vec::with_capacity(toks.len());
    for (i, t) in toks.iter().enumerate() {
        let prev = i.checked_sub(1).map(|j| &toks[j].tok);
        let next = toks.get(i + 1).map(|t| &t.tok);
        let part = match &t.tok {
            Tok::Eof => break,
            // A subscript key names a column: keep it.
            Tok::Str(s) if prev == Some(&Tok::LBracket) && next == Some(&Tok::RBracket) => {
                Part::Text(format!("'{}'", s.replace('\'', "''")))
            }
            Tok::Str(_) | Tok::Int(_) | Tok::Float(_) => {
                // `- 5` (unary minus) is one literal.
                if parts.last() == Some(&Part::Minus) && !value_before_minus(&parts) {
                    parts.pop();
                }
                // `?, ?` collapses to one `?`.
                if parts.last() == Some(&Part::Comma)
                    && parts.len() >= 2
                    && parts[parts.len() - 2] == Part::Placeholder
                {
                    parts.pop();
                    continue;
                }
                Part::Placeholder
            }
            Tok::Ident(s) => Part::Text(s.to_ascii_lowercase()),
            Tok::QuotedIdent(s) => Part::Text(format!("\"{}\"", s.replace('"', "\"\""))),
            Tok::Comma => Part::Comma,
            Tok::Minus => Part::Minus,
            Tok::LParen => Part::Text("(".into()),
            Tok::RParen => Part::Text(")".into()),
            Tok::LBracket => Part::Text("[".into()),
            Tok::RBracket => Part::Text("]".into()),
            Tok::Dot => Part::Text(".".into()),
            Tok::Star => Part::Text("*".into()),
            Tok::Eq => Part::Text("=".into()),
            Tok::Ne => Part::Text("!=".into()),
            Tok::Lt => Part::Text("<".into()),
            Tok::Le => Part::Text("<=".into()),
            Tok::Gt => Part::Text(">".into()),
            Tok::Ge => Part::Text(">=".into()),
            Tok::Semi => Part::Text(";".into()),
        };
        parts.push(part);
    }
    // A trailing `;` does not change the query.
    while parts.last() == Some(&Part::Text(";".into())) {
        parts.pop();
    }
    let mut out = String::with_capacity(sql.len());
    for (i, p) in parts.iter().enumerate() {
        let s = text(p);
        if i > 0 {
            let prev = text(&parts[i - 1]);
            // `count(` and `attributes[` attach to their name; `in (` does not.
            let call = matches!(s, "(" | "[") && is_name(prev) && !is_keyword(prev);
            let tight =
                matches!(s, "," | ")" | "]" | ".") || matches!(prev, "(" | "[" | ".") || call;
            if !tight {
                out.push(' ');
            }
        }
        out.push_str(s);
    }
    out
}

fn text(p: &Part) -> &str {
    match p {
        Part::Placeholder => "?",
        Part::Text(t) => t.as_str(),
        Part::Comma => ",",
        Part::Minus => "-",
    }
}

fn is_name(t: &str) -> bool {
    !t.is_empty()
        && t.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '"')
}

/// Whether the token before a `-` ends a value (then the `-` is binary).
fn value_before_minus(parts: &[Part]) -> bool {
    match parts.len().checked_sub(2).map(|i| &parts[i]) {
        Some(Part::Placeholder) => true,
        Some(Part::Text(t)) => t == ")" || t == "]" || (is_name(t) && !is_keyword(t)),
        _ => false,
    }
}

/// Keywords after which a `-` starts a negative literal.
fn is_keyword(t: &str) -> bool {
    matches!(
        t,
        "select"
            | "where"
            | "and"
            | "or"
            | "not"
            | "in"
            | "limit"
            | "by"
            | "interval"
            | "between"
            | "like"
            | "ilike"
            | "is"
            | "timestamp"
            | "then"
            | "else"
            | "when"
            | "from"
            | "on"
            | "as"
    )
}

/// Fingerprint of `sql`.
pub fn fingerprint(sql: &str) -> Fingerprint {
    let mut normalized = normalize(sql);
    let hash = fnv1a(normalized.as_bytes());
    if normalized.len() > MAX_NORMALIZED_BYTES {
        let mut end = MAX_NORMALIZED_BYTES;
        while !normalized.is_char_boundary(end) {
            end -= 1;
        }
        normalized.truncate(end);
    }
    Fingerprint { hash, normalized }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(s: &str) -> u64 {
        fingerprint(s).hash
    }

    #[test]
    fn literals_whitespace_case_and_comments_do_not_matter() {
        let a = "SELECT count(*) FROM logs WHERE service = 'auth' AND event_time >= 5 LIMIT 10";
        let b = "select   COUNT(*)\nfrom LOGS -- dashboard\n where service='billing' and event_time >= 99 limit 1;";
        assert_eq!(fp(a), fp(b));
        assert_eq!(
            normalize(a),
            "select count(*) from logs where service = ? and event_time >= ? limit ?"
        );
    }

    #[test]
    fn lists_and_negative_numbers_collapse() {
        assert_eq!(
            fp("SELECT * FROM logs WHERE attributes['code'] IN (1, 2, 3)"),
            fp("SELECT * FROM logs WHERE attributes['code'] IN (7)")
        );
        assert_eq!(
            normalize("SELECT * FROM logs WHERE x IN (1, -2, 3)"),
            "select * from logs where x in (?)"
        );
        assert_eq!(
            fp("SELECT * FROM logs WHERE attributes['t'] > -5"),
            fp("SELECT * FROM logs WHERE attributes['t'] > 5")
        );
        // Binary minus is kept.
        assert_eq!(
            normalize("SELECT a - 1 FROM logs"),
            "select a - ? from logs"
        );
    }

    #[test]
    fn structure_changes_the_fingerprint() {
        let base = fp("SELECT * FROM logs WHERE attributes['region'] = 'eu'");
        // Different column (subscript key).
        assert_ne!(
            base,
            fp("SELECT * FROM logs WHERE attributes['user'] = 'eu'")
        );
        // Different operator, table, projection.
        assert_ne!(
            base,
            fp("SELECT * FROM logs WHERE attributes['region'] != 'eu'")
        );
        assert_ne!(
            base,
            fp("SELECT * FROM system.query_log WHERE attributes['region'] = 'eu'")
        );
        assert_ne!(
            base,
            fp("SELECT body FROM logs WHERE attributes['region'] = 'eu'")
        );
        // Quoted identifiers keep their case.
        assert_ne!(fp("SELECT \"A\" FROM logs"), fp("SELECT \"a\" FROM logs"));
    }

    #[test]
    fn stable_values_and_unlexable_text() {
        // Pinned: a change breaks grouping with stored query logs.
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(
            fingerprint("SELECT 1 FROM logs").hex(),
            format!("{:016x}", fnv1a(b"select ? from logs"))
        );
        // The lexer rejects `#`; the text is still normalized and hashed.
        assert_eq!(normalize("SELECT  #  FROM\tlogs"), "select # from logs");
        assert_eq!(fp("SELECT # FROM logs"), fp("select   #   from logs"));
    }

    #[test]
    fn long_text_is_truncated_but_hashed_in_full() {
        let a = format!("SELECT {} FROM logs", "body, ".repeat(2000) + "service");
        let b = format!("SELECT {} FROM logs", "body, ".repeat(2000) + "severity");
        let (fa, fb) = (fingerprint(&a), fingerprint(&b));
        assert!(fa.normalized.len() <= MAX_NORMALIZED_BYTES);
        assert_eq!(fa.normalized, fb.normalized);
        assert_ne!(fa.hash, fb.hash);
    }
}
