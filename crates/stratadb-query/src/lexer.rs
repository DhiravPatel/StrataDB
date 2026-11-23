//! Tokenizer. Positions are byte offsets into the query text (spec §13.2:
//! preserve source spans for diagnostics).

use stratadb_common::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Ident(String),
    QuotedIdent(String),
    Str(String),
    Int(i64),
    Float(f64),
    Comma,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Dot,
    Star,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Semi,
    Minus,
    Eof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub tok: Tok,
    pub pos: usize,
}

pub fn syntax_error(pos: usize, msg: impl std::fmt::Display) -> Error {
    Error::invalid_input(format!("syntax error at offset {pos}: {msg}"))
}

pub fn tokenize(src: &str) -> Result<Vec<Token>> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let start = i;
        let tok =
            match c {
                b' ' | b'\t' | b'\n' | b'\r' => {
                    i += 1;
                    continue;
                }
                b'-' if b.get(i + 1) == Some(&b'-') => {
                    while i < b.len() && b[i] != b'\n' {
                        i += 1;
                    }
                    continue;
                }
                b',' => (Tok::Comma, i += 1).0,
                b'(' => (Tok::LParen, i += 1).0,
                b')' => (Tok::RParen, i += 1).0,
                b'[' => (Tok::LBracket, i += 1).0,
                b']' => (Tok::RBracket, i += 1).0,
                b'.' if !b.get(i + 1).is_some_and(u8::is_ascii_digit) => (Tok::Dot, i += 1).0,
                b'*' => (Tok::Star, i += 1).0,
                b';' => (Tok::Semi, i += 1).0,
                b'-' => (Tok::Minus, i += 1).0,
                b'=' => {
                    i += if b.get(i + 1) == Some(&b'=') { 2 } else { 1 };
                    Tok::Eq
                }
                b'!' if b.get(i + 1) == Some(&b'=') => {
                    i += 2;
                    Tok::Ne
                }
                b'<' => match b.get(i + 1) {
                    Some(b'=') => {
                        i += 2;
                        Tok::Le
                    }
                    Some(b'>') => {
                        i += 2;
                        Tok::Ne
                    }
                    _ => {
                        i += 1;
                        Tok::Lt
                    }
                },
                b'>' => {
                    if b.get(i + 1) == Some(&b'=') {
                        i += 2;
                        Tok::Ge
                    } else {
                        i += 1;
                        Tok::Gt
                    }
                }
                b'\'' | b'"' => {
                    let quote = c;
                    i += 1;
                    let mut s = String::new();
                    loop {
                        let Some(rest) = src.get(i..) else {
                            return Err(syntax_error(start, "unterminated quoted text"));
                        };
                        let Some(off) = rest.find(quote as char) else {
                            return Err(syntax_error(start, "unterminated quoted text"));
                        };
                        s.push_str(&rest[..off]);
                        i += off + 1;
                        // Doubled quote is an escaped quote.
                        if b.get(i) == Some(&quote) {
                            s.push(quote as char);
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    if quote == b'\'' {
                        Tok::Str(s)
                    } else {
                        Tok::QuotedIdent(s)
                    }
                }
                b'0'..=b'9' | b'.' => {
                    while i < b.len() && b[i].is_ascii_digit() {
                        i += 1;
                    }
                    let mut is_float = false;
                    if i < b.len() && b[i] == b'.' {
                        is_float = true;
                        i += 1;
                        while i < b.len() && b[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
                        is_float = true;
                        i += 1;
                        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
                            i += 1;
                        }
                        while i < b.len() && b[i].is_ascii_digit() {
                            i += 1;
                        }
                    }
                    let text = &src[start..i];
                    if is_float {
                        let f: f64 = text
                            .parse()
                            .map_err(|_| syntax_error(start, format!("invalid number {text}")))?;
                        if !f.is_finite() {
                            return Err(syntax_error(start, "number out of range"));
                        }
                        Tok::Float(f)
                    } else {
                        Tok::Int(text.parse().map_err(|_| {
                            syntax_error(start, format!("integer {text} out of range"))
                        })?)
                    }
                }
                c if c.is_ascii_alphabetic() || c == b'_' => {
                    while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                        i += 1;
                    }
                    Tok::Ident(src[start..i].to_owned())
                }
                _ => {
                    let ch = src[i..].chars().next().unwrap_or('?');
                    return Err(syntax_error(i, format!("unexpected character {ch:?}")));
                }
            };
        out.push(Token { tok, pos: start });
    }
    out.push(Token {
        tok: Tok::Eof,
        pos: b.len(),
    });
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens() {
        let t: Vec<Tok> = tokenize("SELECT a,'it''s' <> 1.5e3 -- c\n<= [x]")
            .unwrap()
            .into_iter()
            .map(|t| t.tok)
            .collect();
        assert_eq!(
            t,
            vec![
                Tok::Ident("SELECT".into()),
                Tok::Ident("a".into()),
                Tok::Comma,
                Tok::Str("it's".into()),
                Tok::Ne,
                Tok::Float(1500.0),
                Tok::Le,
                Tok::LBracket,
                Tok::Ident("x".into()),
                Tok::RBracket,
                Tok::Eof
            ]
        );
        assert!(tokenize("'open").is_err());
        assert!(tokenize("99999999999999999999").is_err());
        assert!(tokenize("a § b").is_err());
    }
}
