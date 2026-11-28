//! Columnar JSON result serializer (spec STRATA-SYS-06-003, ADR-0044).
//!
//! - **Old path:** query results were turned into a `serde_json::Value`
//!   tree, one map per row with every column name cloned into it, then
//!   serialized.
//! - **This path:** it writes the same bytes directly. Rows are taken in
//!   blocks; each column of a block is encoded into its own scratch buffer
//!   (a tight per-column loop), then the block's columns are interleaved
//!   into row objects with pre-encoded keys.
//! - **Formatters:** numbers use `itoa`/`zmij` (shortest round-trip floats,
//!   the formatters `serde_json` itself uses), timestamps a non-allocating
//!   RFC 3339 writer.
//!
//! **Compatibility:** the output is byte-identical to `serde_json` (tested
//! against it): keys in sorted order, missing values omitted, non-finite
//! floats as `null`, bytes as lowercase hex, timestamps as RFC 3339.

use serde_json::{Map, Value as J};
use stratadb_query::Datum;
use stratadb_schema::Value;

/// Rows per block: each column's scratch buffer holds one block of values.
pub const BLOCK_ROWS: usize = 1024;

/// `serde_json`'s escape classes for each byte: 0 = none, `u` = `\u00XX`,
/// otherwise the escape character.
const ESCAPE: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 0x20 {
        t[i] = b'u';
        i += 1;
    }
    t[0x08] = b'b';
    t[0x09] = b't';
    t[0x0a] = b'n';
    t[0x0c] = b'f';
    t[0x0d] = b'r';
    t[b'"' as usize] = b'"';
    t[b'\\' as usize] = b'\\';
    t
};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// Append `s` as a JSON string, escaped exactly as `serde_json` does.
pub fn write_str(s: &str, out: &mut Vec<u8>) {
    let b = s.as_bytes();
    out.reserve(b.len() + 2);
    out.push(b'"');
    let mut run = 0;
    for (i, &c) in b.iter().enumerate() {
        let e = ESCAPE[usize::from(c)];
        if e == 0 {
            continue;
        }
        out.extend_from_slice(&b[run..i]);
        run = i + 1;
        if e == b'u' {
            out.extend_from_slice(b"\\u00");
            out.push(HEX[usize::from(c >> 4)]);
            out.push(HEX[usize::from(c & 0xf)]);
        } else {
            out.push(b'\\');
            out.push(e);
        }
    }
    out.extend_from_slice(&b[run..]);
    out.push(b'"');
}

/// Append one value in the result encoding.
pub fn write_value(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(true) => out.extend_from_slice(b"true"),
        Value::Bool(false) => out.extend_from_slice(b"false"),
        Value::Int(i) => out.extend_from_slice(itoa::Buffer::new().format(*i).as_bytes()),
        Value::UInt(u) => out.extend_from_slice(itoa::Buffer::new().format(*u).as_bytes()),
        Value::Float(f) if f.is_finite() => {
            out.extend_from_slice(zmij::Buffer::new().format_finite(*f).as_bytes());
        }
        Value::Float(_) => out.extend_from_slice(b"null"),
        Value::String(s) => write_str(s, out),
        Value::Bytes(b) => {
            out.reserve(b.len() * 2 + 2);
            out.push(b'"');
            for x in b {
                out.push(HEX[usize::from(x >> 4)]);
                out.push(HEX[usize::from(x & 0xf)]);
            }
            out.push(b'"');
        }
        Value::Timestamp(t) => {
            out.push(b'"');
            stratadb_common::time::write_rfc3339(*t, out);
            out.push(b'"');
        }
        Value::List(l) => {
            out.push(b'[');
            for (i, x) in l.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_value(x, out);
            }
            out.push(b']');
        }
        Value::Map(m) => {
            // A BTreeMap: keys already in serde_json's (sorted) order.
            out.push(b'{');
            for (i, (k, x)) in m.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_str(k, out);
                out.push(b':');
                write_value(x, out);
            }
            out.push(b'}');
        }
    }
}

/// One column's encoded values for a block: value `i` is
/// `buf[ends[i-1]..ends[i]]`, empty when missing (no present value encodes
/// to nothing).
#[derive(Default)]
struct Column {
    buf: Vec<u8>,
    ends: Vec<usize>,
}

/// Append `rows` as a JSON array of objects keyed by `names` (unique), in
/// sorted key order, missing values omitted.
pub fn write_rows(names: &[String], rows: &[Vec<Datum>], out: &mut Vec<u8>) {
    let mut order: Vec<usize> = (0..names.len()).collect();
    order.sort_by(|a, b| names[*a].cmp(&names[*b]));
    let keys: Vec<Vec<u8>> = order
        .iter()
        .map(|j| {
            let mut k = Vec::with_capacity(names[*j].len() + 3);
            write_str(&names[*j], &mut k);
            k.push(b':');
            k
        })
        .collect();
    let mut cols: Vec<Column> = order.iter().map(|_| Column::default()).collect();
    out.push(b'[');
    for (bi, block) in rows.chunks(BLOCK_ROWS).enumerate() {
        // Column at a time: one tight loop per column over the block.
        for (col, j) in cols.iter_mut().zip(&order) {
            col.buf.clear();
            col.ends.clear();
            for r in block {
                if let Some(Some(v)) = r.get(*j) {
                    write_value(v, &mut col.buf);
                }
                col.ends.push(col.buf.len());
            }
        }
        // Interleave into row objects.
        let bytes: usize = cols.iter().map(|c| c.buf.len()).sum();
        out.reserve(bytes + block.len() * (2 + keys.iter().map(Vec::len).sum::<usize>()));
        for i in 0..block.len() {
            if bi > 0 || i > 0 {
                out.push(b',');
            }
            out.push(b'{');
            let mut first = true;
            for (col, key) in cols.iter().zip(&keys) {
                let start = if i == 0 { 0 } else { col.ends[i - 1] };
                let end = col.ends[i];
                if end > start {
                    if !first {
                        out.push(b',');
                    }
                    first = false;
                    out.extend_from_slice(key);
                    out.extend_from_slice(&col.buf[start..end]);
                }
            }
            out.push(b'}');
        }
    }
    out.push(b']');
}

/// The bytes of `envelope` serialized as a JSON object with `raw_key` added,
/// split around that key's value: `prefix` ends with `"raw_key":`, `suffix`
/// is the rest. The key goes where `serde_json` would put it (keys sorted),
/// so `prefix + raw + suffix` equals `serde_json`'s output with the value
/// in place. `raw_key` must not be in `envelope`.
pub fn envelope_parts(
    envelope: &Map<String, J>,
    raw_key: &str,
) -> serde_json::Result<(Vec<u8>, Vec<u8>)> {
    let (mut prefix, mut suffix) = (Vec::with_capacity(256), Vec::with_capacity(2048));
    prefix.push(b'{');
    let mut raw_done = false;
    for (k, v) in envelope {
        if !raw_done && raw_key < k.as_str() {
            write_str(raw_key, &mut prefix);
            prefix.push(b':');
            raw_done = true;
        }
        let out = if raw_done { &mut suffix } else { &mut prefix };
        if raw_done {
            out.push(b',');
        }
        write_str(k, out);
        out.push(b':');
        serde_json::to_writer(&mut *out, v)?;
        if !raw_done {
            out.push(b',');
        }
    }
    if !raw_done {
        write_str(raw_key, &mut prefix);
        prefix.push(b':');
    }
    suffix.push(b'}');
    Ok((prefix, suffix))
}

/// [`envelope_parts`] joined around `raw` (one buffer; for small bodies and
/// tests).
pub fn write_envelope(
    envelope: &Map<String, J>,
    raw_key: &str,
    raw: &[u8],
    out: &mut Vec<u8>,
) -> serde_json::Result<()> {
    let (prefix, suffix) = envelope_parts(envelope, raw_key)?;
    out.extend_from_slice(&prefix);
    out.extend_from_slice(raw);
    out.extend_from_slice(&suffix);
    Ok(())
}

/// A response body sent as a few pre-built chunks with an exact length (so
/// the response keeps `Content-Length`), without joining them into one
/// buffer: a large encoded result is never copied.
#[derive(Debug)]
pub struct ChunkedBody {
    chunks: std::collections::VecDeque<bytes::Bytes>,
    remaining: u64,
}

impl ChunkedBody {
    pub fn new(chunks: Vec<Vec<u8>>) -> Self {
        let remaining = chunks.iter().map(|c| c.len() as u64).sum();
        ChunkedBody {
            chunks: chunks
                .into_iter()
                .filter(|c| !c.is_empty())
                .map(bytes::Bytes::from)
                .collect(),
            remaining,
        }
    }
}

impl http_body::Body for ChunkedBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<bytes::Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let next = this.chunks.pop_front().map(|c| {
            this.remaining -= c.len() as u64;
            Ok(http_body::Frame::data(c))
        });
        std::task::Poll::Ready(next)
    }

    fn is_end_stream(&self) -> bool {
        self.chunks.is_empty()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

/// The previous `serde_json::Value` encoding of one value: the reference
/// the serializer is tested and benchmarked against.
pub fn value_tree(v: &Value) -> J {
    match v {
        Value::Null => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Int(i) => J::from(*i),
        Value::UInt(u) => J::from(*u),
        Value::Float(f) => serde_json::Number::from_f64(*f).map_or(J::Null, J::Number),
        Value::String(s) => J::String(s.clone()),
        Value::Bytes(b) => J::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        Value::Timestamp(t) => J::String(stratadb_common::time::format_rfc3339(*t)),
        Value::List(l) => J::Array(l.iter().map(value_tree).collect()),
        Value::Map(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), value_tree(v))).collect()),
    }
}

/// The previous row encoding (one `serde_json` map per row).
pub fn row_tree(names: &[String], row: &[Datum]) -> J {
    let mut m = Map::new();
    for (k, d) in names.iter().zip(row) {
        if let Some(v) = d {
            m.insert(k.clone(), value_tree(v));
        }
    }
    J::Object(m)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn leaf() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            any::<u64>().prop_map(Value::UInt),
            any::<f64>().prop_map(Value::Float),
            prop_oneof![
                Just(f64::NAN),
                Just(f64::INFINITY),
                Just(-0.0),
                Just(1e-300),
                Just(1e21)
            ]
            .prop_map(Value::Float),
            any::<String>().prop_map(Value::String),
            "[\\x00-\\x1f\"\\\\/a\u{7f}\u{e9}\u{1f600}]{0,12}".prop_map(Value::String),
            proptest::collection::vec(any::<u8>(), 0..8).prop_map(Value::Bytes),
            any::<i64>().prop_map(Value::Timestamp),
        ]
    }

    fn value() -> impl Strategy<Value = Value> {
        leaf().prop_recursive(2, 12, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::List),
                proptest::collection::btree_map("[a-z\"]{0,4}", inner, 0..4)
                    .prop_map(|m: BTreeMap<String, Value>| Value::Map(m)),
            ]
        })
    }

    proptest! {
        /// Byte-identical to serde_json for any values, names (escaping,
        /// key order) and missing cells, across block boundaries.
        #[test]
        fn rows_equal_serde_json(
            names in proptest::collection::btree_set("[a-z_\"\\\\\u{e9}]{0,6}", 0..5),
            cells in proptest::collection::vec(proptest::option::of(value()), 0..40),
            short in 0usize..3,
        ) {
            let names: Vec<String> = names.into_iter().rev().collect();
            let w = names.len().max(1);
            let rows: Vec<Vec<Datum>> = cells
                .chunks(w)
                .enumerate()
                // Some rows shorter than the column list.
                .map(|(i, c)| c[..c.len().saturating_sub(if i % 3 == 0 { short } else { 0 })].to_vec())
                .collect();
            let mut got = Vec::new();
            write_rows(&names, &rows, &mut got);
            let want = serde_json::to_vec(&J::Array(rows.iter().map(|r| row_tree(&names, r)).collect())).unwrap();
            prop_assert_eq!(String::from_utf8(got).unwrap(), String::from_utf8(want).unwrap());
        }

        #[test]
        fn strings_equal_serde_json(s in any::<String>()) {
            let mut got = Vec::new();
            write_str(&s, &mut got);
            prop_assert_eq!(got, serde_json::to_vec(&s).unwrap());
        }
    }

    #[test]
    fn many_blocks() {
        let names = vec!["b".to_owned(), "a".to_owned()];
        let rows: Vec<Vec<Datum>> = (0..BLOCK_ROWS as i64 * 3 + 7)
            .map(|i| {
                vec![
                    Some(Value::Int(i)),
                    (i % 5 != 0).then(|| Value::String(format!("s{i}"))),
                ]
            })
            .collect();
        let mut got = Vec::new();
        write_rows(&names, &rows, &mut got);
        let want = serde_json::to_vec(&J::Array(
            rows.iter().map(|r| row_tree(&names, r)).collect(),
        ))
        .unwrap();
        assert_eq!(got, want);
        let mut empty = Vec::new();
        write_rows(&names, &[], &mut empty);
        assert_eq!(empty, b"[]");
    }

    #[test]
    fn chunked_body_yields_every_chunk_with_an_exact_length() {
        use http_body::Body as _;
        let mut b = ChunkedBody::new(vec![
            b"{\"a\":".to_vec(),
            Vec::new(),
            b"[1]".to_vec(),
            b"}".to_vec(),
        ]);
        assert_eq!(b.size_hint().exact(), Some(9));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let mut got = Vec::new();
        while let std::task::Poll::Ready(Some(f)) = std::pin::Pin::new(&mut b).poll_frame(&mut cx) {
            got.extend_from_slice(&f.unwrap().into_data().unwrap());
            assert_eq!(b.size_hint().exact(), Some(9 - got.len() as u64));
        }
        assert_eq!(got, br#"{"a":[1]}"#);
        assert!(b.is_end_stream());
    }

    #[test]
    fn envelope_places_the_raw_key_in_sorted_order() {
        for key in ["a", "m", "rows", "zz"] {
            let mut env = Map::new();
            env.insert("columns".into(), serde_json::json!(["x"]));
            env.insert("query_id".into(), serde_json::json!("q"));
            env.insert("stats".into(), serde_json::json!({"n": 1}));
            let mut got = Vec::new();
            write_envelope(&env, key, b"[1,2]", &mut got).unwrap();
            let mut full = env.clone();
            full.insert(key.into(), serde_json::json!([1, 2]));
            assert_eq!(got, serde_json::to_vec(&J::Object(full)).unwrap(), "{key}");
        }
        let mut got = Vec::new();
        write_envelope(&Map::new(), "rows", b"[]", &mut got).unwrap();
        assert_eq!(got, br#"{"rows":[]}"#);
    }
}

/// Output throughput on events-table rows, old tree path vs this one (spec
/// STRATA-SYS-06-003: >= 400 MB/s per core). Run with `cargo test
/// --release -p stratadb-server --lib json_throughput -- --ignored
/// --nocapture`.
#[cfg(test)]
mod bench {
    #![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

    use super::*;
    use std::collections::BTreeMap;

    fn events(n: usize) -> (Vec<String>, Vec<Vec<Datum>>) {
        let names = [
            "event_time",
            "service",
            "severity",
            "body",
            "trace_id",
            "user_id",
            "latency_ms",
            "ratio",
            "attributes",
        ]
        .map(String::from)
        .to_vec();
        let mut s = 0x9e37_79b9_7f4a_7c15u64;
        let rows = (0..n)
            .map(|i| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                let mut attrs = BTreeMap::new();
                attrs.insert("http.method".to_owned(), Value::String("GET".into()));
                attrs.insert("http.status".to_owned(), Value::Int(200 + (s % 5) as i64));
                vec![
                    Some(Value::Timestamp(
                        1_795_170_600_000_000_000 + i as i64 * 1_000_003,
                    )),
                    Some(Value::String(format!("service-{:02}", s % 40))),
                    Some(Value::String(
                        ["INFO", "WARN", "ERROR"][(s % 3) as usize].into(),
                    )),
                    Some(Value::String(format!(
                        "GET /api/v1/items/{} completed in {} ms \"ok\"\tuser={}",
                        s % 100_000,
                        s % 997,
                        s % 5000
                    ))),
                    Some(Value::String(format!(
                        "{:032x}",
                        u128::from(s) * 0x1_0000_0001
                    ))),
                    (i % 7 != 0).then(|| Value::String(format!("user-{}", s % 50_000))),
                    Some(Value::Int((s % 10_000) as i64)),
                    Some(Value::Float((s % 100_000) as f64 / 997.0)),
                    Some(Value::Map(attrs)),
                ]
            })
            .collect();
        (names, rows)
    }

    #[test]
    #[ignore = "timing measurement"]
    fn json_throughput() {
        let (names, rows) = events(100_000);
        let mut t = [Vec::new(), Vec::new()];
        let mut bytes = 0;
        for round in 0..9 {
            for k in 0..2 {
                let new = (round + k) % 2 == 0;
                let started = std::time::Instant::now();
                let out = if new {
                    let mut out = Vec::new();
                    write_rows(&names, &rows, &mut out);
                    out
                } else {
                    let tree = J::Array(rows.iter().map(|r| row_tree(&names, r)).collect());
                    serde_json::to_vec(&tree).unwrap()
                };
                t[usize::from(new)].push(started.elapsed().as_secs_f64());
                bytes = out.len();
                std::hint::black_box(out);
            }
        }
        for v in &mut t {
            v.sort_by(f64::total_cmp);
        }
        let mbs = |secs: f64| bytes as f64 / secs / 1e6;
        println!(
            "{} rows, {} bytes: value tree {:.0} MB/s, columnar {:.0} MB/s ({:.2}x)",
            rows.len(),
            bytes,
            mbs(t[0][4]),
            mbs(t[1][4]),
            t[0][4] / t[1][4]
        );
    }
}
