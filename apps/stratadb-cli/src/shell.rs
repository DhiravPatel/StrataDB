//! `strata shell` — interactive StrataQL prompt over a local data directory
//! (spec §22.1).
//!
//! Statements end with `;` and may span lines. Meta commands (on their own
//! line): `\q` quit, `\help`, `\format table|json|csv`, `\timing` (toggle
//! execution statistics), `\rows N` (display limit). Row queries show at most
//! the display limit (default 1 000) — a paginated read, so large results never
//! load fully — and say when more rows exist; aggregate and EXPLAIN queries run
//! normally. Errors are printed and the shell continues. The prompt is shown
//! only when stdin is a terminal, so scripts can pipe statements in.

use std::io::{BufRead, IsTerminal, Write};
use std::time::{Duration, Instant};

use stratadb_common::{Result, TenantId};
use stratadb_query::ast::ExplainMode;
use stratadb_query::plan::Plan;
use stratadb_query::{execute, execute_page, Datum, PageRequest, QueryOptions, QueryOutput};
use stratadb_schema::json::value_to_json;
use stratadb_schema::Value;
use stratadb_storage::Engine;

use crate::export::csv_field;

const MAX_CELL: usize = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fmt {
    Table,
    Json,
    Csv,
}

struct State {
    fmt: Fmt,
    timing: bool,
    rows: usize,
}

fn cell(d: &Datum) -> String {
    match d {
        None => String::new(),
        Some(Value::Null) => "NULL".into(),
        Some(Value::String(s)) => s.clone(),
        Some(v) => match value_to_json(v) {
            serde_json::Value::String(s) => s,
            j => j.to_string(),
        },
    }
}

fn truncate(s: &str) -> String {
    let flat = s.replace(['\n', '\r', '\t'], " ");
    if flat.chars().count() <= MAX_CELL {
        flat
    } else {
        let mut t: String = flat.chars().take(MAX_CELL - 1).collect();
        t.push('…');
        t
    }
}

fn table(out: &QueryOutput) -> String {
    let cells: Vec<Vec<String>> = out
        .rows
        .iter()
        .map(|r| r.iter().map(|d| truncate(&cell(d))).collect())
        .collect();
    let mut w: Vec<usize> = out.columns.iter().map(|c| c.chars().count()).collect();
    for r in &cells {
        for (i, c) in r.iter().enumerate() {
            w[i] = w[i].max(c.chars().count());
        }
    }
    let line = |vals: &[String]| {
        let parts: Vec<String> = vals
            .iter()
            .enumerate()
            .map(|(i, v)| format!(" {v}{} ", " ".repeat(w[i] - v.chars().count())))
            .collect();
        format!("|{}|\n", parts.join("|"))
    };
    let sep = format!(
        "+{}+\n",
        w.iter()
            .map(|n| "-".repeat(n + 2))
            .collect::<Vec<_>>()
            .join("+")
    );
    let mut s = sep.clone();
    s.push_str(&line(&out.columns));
    s.push_str(&sep);
    for r in &cells {
        s.push_str(&line(r));
    }
    s.push_str(&sep);
    s
}

fn render(st: &State, out: &QueryOutput) -> String {
    if let Some(plan) = &out.explain {
        return plan.clone();
    }
    match st.fmt {
        Fmt::Table => table(out),
        Fmt::Json => out
            .rows
            .iter()
            .map(|r| {
                let m: serde_json::Map<String, serde_json::Value> = out
                    .columns
                    .iter()
                    .zip(r)
                    .filter_map(|(c, d)| d.as_ref().map(|v| (c.clone(), value_to_json(v))))
                    .collect();
                format!("{}\n", serde_json::Value::Object(m))
            })
            .collect(),
        Fmt::Csv => {
            let mut s = out
                .columns
                .iter()
                .map(|c| csv_field(c))
                .collect::<Vec<_>>()
                .join(",");
            s.push('\n');
            for r in &out.rows {
                let line: Vec<String> = r
                    .iter()
                    .map(|d| match d {
                        None | Some(Value::Null) => String::new(),
                        _ => csv_field(&cell(d)),
                    })
                    .collect();
                s.push_str(&line.join(","));
                s.push('\n');
            }
            s
        }
    }
}

fn run_statement(engine: &Engine, tenant: &TenantId, st: &State, sql: &str) -> Result<String> {
    let started = Instant::now();
    let opts = QueryOptions {
        timeout: Duration::from_secs(60),
        max_result_rows: st.rows,
        ..QueryOptions::default()
    };
    let stmt = stratadb_query::parser::parse(sql, &opts.parse_limits)?;
    let plan = Plan::new(stmt)?;
    let snap = engine.snapshot(tenant);
    let (out, more) = if plan.aggregate || plan.explain != ExplainMode::None {
        (execute(&snap, sql, &opts)?, false)
    } else {
        let p = execute_page(
            &snap,
            sql,
            &opts,
            &PageRequest {
                page_size: st.rows,
                cursor: None,
            },
        )?;
        (p.output, p.next_cursor.is_some())
    };
    let mut s = render(st, &out);
    if out.explain.is_none() && st.fmt == Fmt::Table {
        let n = out.rows.len();
        s.push_str(&format!(
            "({n} row{}{})\n",
            if n == 1 { "" } else { "s" },
            if more {
                format!(
                    "; more rows exist — showing the first {}, add LIMIT or use `strata export`",
                    st.rows
                )
            } else {
                String::new()
            }
        ));
    }
    if st.timing {
        let x = &out.stats;
        s.push_str(&format!(
            "Time: {:.2} ms (rows scanned {}, matched {}, segments scanned {}/{}, indexes {:?})\n",
            started.elapsed().as_secs_f64() * 1000.0,
            x.rows_scanned,
            x.rows_matched,
            x.segments_scanned,
            x.segments_total,
            x.indexes_used
        ));
    }
    Ok(s)
}

const HELP: &str = "Statements end with ';'. Meta commands:\n  \\q              quit\n  \\format F       output format: table | json | csv\n  \\timing         toggle execution statistics\n  \\rows N         display at most N rows of a row query (default 1000)\n  \\help           this text\nThe language is StrataQL (docs/query-language/grammar.md); FROM logs only.\n";

fn meta(st: &mut State, cmd: &str) -> std::result::Result<Option<String>, ()> {
    let mut it = cmd.split_whitespace();
    let name = it.next().unwrap_or("");
    let arg = it.next();
    Ok(Some(match (name, arg) {
        ("\\q" | "\\quit", _) => return Err(()),
        ("\\help" | "\\?", _) => HELP.to_owned(),
        ("\\timing", _) => {
            st.timing = !st.timing;
            format!("Timing is {}.\n", if st.timing { "on" } else { "off" })
        }
        ("\\format", Some(f)) => {
            st.fmt = match f {
                "table" => Fmt::Table,
                "json" => Fmt::Json,
                "csv" => Fmt::Csv,
                _ => return Ok(Some("ERROR: format must be table, json or csv\n".into())),
            };
            format!("Output format is {f}.\n")
        }
        ("\\rows", Some(n)) => match n.parse::<usize>() {
            Ok(n) if (1..=100_000).contains(&n) => {
                st.rows = n;
                format!("Display limit is {n} rows.\n")
            }
            _ => "ERROR: \\rows takes 1..=100000\n".into(),
        },
        _ => format!("ERROR: unknown command {cmd} (\\help lists commands)\n"),
    }))
}

/// Run the shell until `\q` or end of input. Returns false if any statement failed.
pub fn run(
    engine: &Engine,
    tenant: &TenantId,
    input: impl BufRead,
    out: &mut impl Write,
) -> Result<bool> {
    let interactive = std::io::stdin().is_terminal();
    let mut st = State {
        fmt: Fmt::Table,
        timing: false,
        rows: 1000,
    };
    let mut ok = true;
    let mut buf = String::new();
    let prompt = |out: &mut dyn Write, cont: bool| {
        if interactive {
            let _ = write!(out, "{}", if cont { "   ...> " } else { "strata> " });
            let _ = out.flush();
        }
    };
    if interactive {
        let _ = writeln!(
            out,
            "StrataDB shell — tenant `{tenant}`. \\help for help, \\q to quit."
        );
    }
    prompt(out, false);
    for line in input.lines() {
        let line = line.map_err(|e| stratadb_common::Error::io("reading input", e))?;
        let trimmed = line.trim();
        if buf.trim().is_empty() && trimmed.starts_with('\\') {
            match meta(&mut st, trimmed) {
                Err(()) => return Ok(ok),
                Ok(Some(msg)) => {
                    let _ = write!(out, "{msg}");
                }
                Ok(None) => {}
            }
            prompt(out, false);
            continue;
        }
        buf.push_str(&line);
        buf.push('\n');
        if trimmed.ends_with(';') {
            let sql = buf.trim().trim_end_matches(';').trim().to_owned();
            buf.clear();
            if !sql.is_empty() {
                match run_statement(engine, tenant, &st, &sql) {
                    Ok(s) => {
                        let _ = write!(out, "{s}");
                    }
                    Err(e) => {
                        ok = false;
                        let _ = writeln!(out, "ERROR: {}: {e}", e.kind().as_str());
                    }
                }
            }
            prompt(out, false);
        } else {
            prompt(out, true);
        }
    }
    if !buf.trim().is_empty() {
        let _ = writeln!(
            out,
            "ERROR: incomplete statement at end of input (missing ';')"
        );
        ok = false;
    }
    Ok(ok)
}
