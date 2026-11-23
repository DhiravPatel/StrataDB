//! OTLP/HTTP **protobuf** support for logs, without code generation.
//!
//! `decode_export_request` parses an `ExportLogsServiceRequest` (official
//! opentelemetry-proto field numbers) into the OTLP/JSON shape consumed by
//! [`crate::otlp::map_export_request`], so both encodings share one mapping.
//! Unknown fields are skipped (forward compatibility); group wire types are
//! rejected; nesting depth and every length are bounded by the input.
//! Verified against binaries produced by `protoc --encode`
//! (tests/fixtures/otlp).

use serde_json::{json, Map, Value as J};
use stratadb_common::{Error, Result};

const MAX_DEPTH: usize = 32;

fn bad(m: impl Into<String>) -> Error {
    Error::invalid_input(format!("invalid OTLP protobuf: {}", m.into()))
}

struct R<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> R<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, p: 0 }
    }
    fn done(&self) -> bool {
        self.p >= self.b.len()
    }
    fn varint(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        for i in 0..10 {
            let byte = *self.b.get(self.p).ok_or_else(|| bad("truncated varint"))?;
            self.p += 1;
            if i == 9 && byte > 1 {
                return Err(bad("varint overflow"));
            }
            v |= u64::from(byte & 0x7f) << (7 * i);
            if byte & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(bad("varint too long"))
    }
    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        let s = self
            .b
            .get(self.p..self.p + N)
            .ok_or_else(|| bad("truncated fixed field"))?;
        self.p += N;
        let mut a = [0u8; N];
        a.copy_from_slice(s);
        Ok(a)
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = usize::try_from(self.varint()?).map_err(|_| bad("length overflow"))?;
        let end = self
            .p
            .checked_add(n)
            .filter(|e| *e <= self.b.len())
            .ok_or_else(|| bad("length exceeds message"))?;
        let s = &self.b[self.p..end];
        self.p = end;
        Ok(s)
    }
    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| bad("string field is not UTF-8"))
    }
    /// Next (field number, wire type).
    fn key(&mut self) -> Result<(u64, u8)> {
        let k = self.varint()?;
        let wt = (k & 7) as u8;
        let field = k >> 3;
        if field == 0 {
            return Err(bad("field number 0"));
        }
        Ok((field, wt))
    }
    fn skip(&mut self, wt: u8) -> Result<()> {
        match wt {
            0 => self.varint().map(|_| ()),
            1 => self.fixed::<8>().map(|_| ()),
            2 => self.bytes().map(|_| ()),
            5 => self.fixed::<4>().map(|_| ()),
            _ => Err(bad(format!("unsupported wire type {wt}"))),
        }
    }
    /// Expect wire type `want` for a known field.
    fn expect(wt: u8, want: u8, what: &str) -> Result<()> {
        if wt == want {
            Ok(())
        } else {
            Err(bad(format!("{what}: wire type {wt}, expected {want}")))
        }
    }
}

fn b64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                out.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn any_value(b: &[u8], depth: usize) -> Result<J> {
    if depth > MAX_DEPTH {
        return Err(bad("AnyValue nested too deeply"));
    }
    let mut r = R::new(b);
    let mut out = J::Object(Map::new());
    while !r.done() {
        let (f, wt) = r.key()?;
        // oneof: the last value present wins (protobuf semantics).
        out = match f {
            1 => {
                R::expect(wt, 2, "string_value")?;
                json!({"stringValue": r.string()?})
            }
            2 => {
                R::expect(wt, 0, "bool_value")?;
                json!({"boolValue": r.varint()? != 0})
            }
            3 => {
                R::expect(wt, 0, "int_value")?;
                json!({"intValue": (r.varint()? as i64).to_string()})
            }
            4 => {
                R::expect(wt, 1, "double_value")?;
                let d = f64::from_bits(u64::from_le_bytes(r.fixed::<8>()?));
                match serde_json::Number::from_f64(d) {
                    Some(n) => json!({"doubleValue": n}),
                    None => json!({"doubleValue": d.to_string()}), // NaN/Inf: rejected by validation
                }
            }
            5 => {
                R::expect(wt, 2, "array_value")?;
                json!({"arrayValue": {"values": repeated_any(r.bytes()?, depth + 1)?}})
            }
            6 => {
                R::expect(wt, 2, "kvlist_value")?;
                json!({"kvlistValue": {"values": key_values_list(r.bytes()?, depth + 1)?}})
            }
            7 => {
                R::expect(wt, 2, "bytes_value")?;
                json!({"bytesValue": b64(r.bytes()?)})
            }
            _ => {
                r.skip(wt)?;
                out
            }
        };
    }
    Ok(out)
}

/// `ArrayValue { repeated AnyValue values = 1; }`
fn repeated_any(b: &[u8], depth: usize) -> Result<Vec<J>> {
    let mut r = R::new(b);
    let mut v = Vec::new();
    while !r.done() {
        match r.key()? {
            (1, 2) => v.push(any_value(r.bytes()?, depth)?),
            (_, wt) => r.skip(wt)?,
        }
    }
    Ok(v)
}

/// `KeyValue { string key = 1; AnyValue value = 2; }`
fn key_value(b: &[u8], depth: usize) -> Result<J> {
    let mut r = R::new(b);
    let (mut key, mut value) = (String::new(), J::Object(Map::new()));
    while !r.done() {
        match r.key()? {
            (1, 2) => key = r.string()?,
            (2, 2) => value = any_value(r.bytes()?, depth + 1)?,
            (_, wt) => r.skip(wt)?,
        }
    }
    Ok(json!({"key": key, "value": value}))
}

/// `KeyValueList { repeated KeyValue values = 1; }`
fn key_values_list(b: &[u8], depth: usize) -> Result<Vec<J>> {
    let mut r = R::new(b);
    let mut v = Vec::new();
    while !r.done() {
        match r.key()? {
            (1, 2) => v.push(key_value(r.bytes()?, depth)?),
            (_, wt) => r.skip(wt)?,
        }
    }
    Ok(v)
}

fn log_record(b: &[u8]) -> Result<J> {
    let mut r = R::new(b);
    let mut o = Map::new();
    let mut attrs = Vec::new();
    while !r.done() {
        let (f, wt) = r.key()?;
        match f {
            1 | 11 => {
                R::expect(wt, 1, "timestamp")?;
                let t = u64::from_le_bytes(r.fixed::<8>()?);
                o.insert(
                    (if f == 1 {
                        "timeUnixNano"
                    } else {
                        "observedTimeUnixNano"
                    })
                    .into(),
                    json!(t.to_string()),
                );
            }
            2 => {
                R::expect(wt, 0, "severity_number")?;
                o.insert("severityNumber".into(), json!(r.varint()? as i64 as i32));
            }
            3 => {
                R::expect(wt, 2, "severity_text")?;
                o.insert("severityText".into(), json!(r.string()?));
            }
            5 => {
                R::expect(wt, 2, "body")?;
                o.insert("body".into(), any_value(r.bytes()?, 1)?);
            }
            6 => {
                R::expect(wt, 2, "attributes")?;
                attrs.push(key_value(r.bytes()?, 1)?);
            }
            7 => {
                R::expect(wt, 0, "dropped_attributes_count")?;
                o.insert("droppedAttributesCount".into(), json!(r.varint()?));
            }
            8 => {
                R::expect(wt, 5, "flags")?;
                o.insert("flags".into(), json!(u32::from_le_bytes(r.fixed::<4>()?)));
            }
            9 | 10 => {
                R::expect(wt, 2, "trace/span id")?;
                o.insert(
                    (if f == 9 { "traceId" } else { "spanId" }).into(),
                    json!(hex(r.bytes()?)),
                );
            }
            12 => {
                R::expect(wt, 2, "event_name")?;
                o.insert("eventName".into(), json!(r.string()?));
            }
            _ => r.skip(wt)?,
        }
    }
    o.insert("attributes".into(), J::Array(attrs));
    Ok(J::Object(o))
}

fn scope_logs(b: &[u8]) -> Result<J> {
    let mut r = R::new(b);
    let mut scope = Map::new();
    let mut records = Vec::new();
    while !r.done() {
        match r.key()? {
            (1, 2) => {
                let mut s = R::new(r.bytes()?);
                let mut attrs = Vec::new();
                while !s.done() {
                    match s.key()? {
                        (1, 2) => {
                            scope.insert("name".into(), json!(s.string()?));
                        }
                        (2, 2) => {
                            scope.insert("version".into(), json!(s.string()?));
                        }
                        (3, 2) => attrs.push(key_value(s.bytes()?, 1)?),
                        (_, wt) => s.skip(wt)?,
                    }
                }
                scope.insert("attributes".into(), J::Array(attrs));
            }
            (2, 2) => records.push(log_record(r.bytes()?)?),
            (_, wt) => r.skip(wt)?,
        }
    }
    Ok(json!({"scope": scope, "logRecords": records}))
}

fn resource_logs(b: &[u8]) -> Result<J> {
    let mut r = R::new(b);
    let mut attrs = Vec::new();
    let mut scopes = Vec::new();
    while !r.done() {
        match r.key()? {
            (1, 2) => {
                let mut s = R::new(r.bytes()?);
                while !s.done() {
                    match s.key()? {
                        (1, 2) => attrs.push(key_value(s.bytes()?, 1)?),
                        (_, wt) => s.skip(wt)?,
                    }
                }
            }
            (2, 2) => scopes.push(scope_logs(r.bytes()?)?),
            (_, wt) => r.skip(wt)?,
        }
    }
    Ok(json!({"resource": {"attributes": attrs}, "scopeLogs": scopes}))
}

/// Decode `ExportLogsServiceRequest` into the OTLP/JSON representation.
pub fn decode_export_request(b: &[u8]) -> Result<J> {
    let mut r = R::new(b);
    let mut rls = Vec::new();
    while !r.done() {
        match r.key()? {
            (1, 2) => rls.push(resource_logs(r.bytes()?)?),
            (_, wt) => r.skip(wt)?,
        }
    }
    Ok(json!({"resourceLogs": rls}))
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes_field(out: &mut Vec<u8>, field: u64, b: &[u8]) {
    put_varint(out, (field << 3) | 2);
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// `ExportLogsServiceResponse` (empty when nothing was rejected).
pub fn encode_export_response(rejected: u64, error_message: &str) -> Vec<u8> {
    if rejected == 0 && error_message.is_empty() {
        return Vec::new();
    }
    let mut ps = Vec::new();
    if rejected != 0 {
        put_varint(&mut ps, 1 << 3);
        put_varint(&mut ps, rejected);
    }
    if !error_message.is_empty() {
        put_bytes_field(&mut ps, 2, error_message.as_bytes());
    }
    let mut out = Vec::new();
    put_bytes_field(&mut out, 1, &ps);
    out
}

/// `google.rpc.Status { int32 code = 1; string message = 2; }`
pub fn encode_status(code: i32, message: &str) -> Vec<u8> {
    let mut out = Vec::new();
    if code != 0 {
        put_varint(&mut out, 1 << 3);
        put_varint(&mut out, i64::from(code) as u64);
    }
    if !message.is_empty() {
        put_bytes_field(&mut out, 2, message.as_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::map_export_request;
    use crate::{Limits, Severity, Value};
    use stratadb_common::TenantId;

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(format!(
            "{}/../../tests/fixtures/otlp/{name}",
            env!("CARGO_MANIFEST_DIR")
        ))
        .unwrap()
    }

    /// `request.binpb` was produced by `protoc --encode` from `request.textproto`.
    #[test]
    fn decodes_protoc_generated_request() {
        let j = decode_export_request(&fixture("request.binpb")).unwrap();
        let t = TenantId::new("acme").unwrap();
        let b = map_export_request(&j, &t, 1_795_170_602_000_000_000, &Limits::default()).unwrap();
        assert_eq!(b.records.len(), 2);
        let e = b.records[0].as_ref().unwrap();
        assert_eq!(e.event_time, 1_795_170_600_000_000_000);
        assert_eq!(e.service.as_deref(), Some("checkout"));
        assert_eq!(e.severity, Severity::Error);
        assert_eq!(e.severity_text.as_deref(), Some("ERROR"));
        assert_eq!(e.body, Some(Value::from("card declined")));
        assert_eq!(
            e.trace_id.as_deref(),
            Some("5b8efff798038103d269b633813fc60c")
        );
        assert_eq!(e.span_id.as_deref(), Some("eee19b7ec3c1b174"));
        assert_eq!(e.attributes["http.status_code"], Value::Int(402));
        assert_eq!(e.attributes["ratio"], Value::Float(0.25));
        assert_eq!(e.attributes["ok"], Value::Bool(false));
        assert_eq!(e.attributes["raw"], Value::Bytes(b"hello".to_vec()));
        assert_eq!(
            e.attributes["list"],
            Value::List(vec![Value::from("a"), Value::Int(-3)])
        );
        assert!(matches!(e.attributes["map"], Value::Map(_)));
        assert_eq!(e.attributes["otel.flags"], Value::Int(1));
        assert_eq!(
            e.attributes["otel.event_name"],
            Value::from("payment.failed")
        );
        assert_eq!(e.attributes["otel.scope.name"], Value::from("my.library"));
        assert_eq!(e.resource_attributes["host.cpu"], Value::Int(8));
        let e2 = b.records[1].as_ref().unwrap();
        assert_eq!(
            e2.event_time, 1_795_170_601_000_000_000,
            "observed time fallback"
        );
        assert_eq!(
            e2.severity,
            Severity::Warn,
            "severity from text when number is 0"
        );
    }

    /// `response_partial.binpb` was produced by `protoc --encode`.
    #[test]
    fn response_matches_protoc_encoding() {
        assert_eq!(
            encode_export_response(2, "bad traceId"),
            fixture("response_partial.binpb")
        );
        assert!(encode_export_response(0, "").is_empty());
    }

    #[test]
    fn rejects_malformed_input_without_panicking() {
        let good = fixture("request.binpb");
        for cut in 0..good.len() {
            let _ = decode_export_request(&good[..cut]);
        }
        assert!(decode_export_request(&[0x0b]).is_err(), "group wire type");
        assert!(
            decode_export_request(&[0x0a, 0x05, 0x01]).is_err(),
            "length beyond input"
        );
        // Deeply nested arrays are bounded.
        let mut v: Vec<u8> = vec![];
        for _ in 0..100 {
            let mut outer = vec![0x2a]; // field 5 (array_value), len
            let mut arr = vec![0x0a]; // ArrayValue.values
            put_varint(&mut arr, v.len() as u64);
            arr.extend_from_slice(&v);
            put_varint(&mut outer, arr.len() as u64);
            outer.extend_from_slice(&arr);
            v = outer;
        }
        assert!(any_value(&v, 0).is_err());
        assert_eq!(b64(b"hello"), "aGVsbG8=");
        assert_eq!(b64(b"hi"), "aGk=");
        // Unknown fields are skipped.
        let mut with_unknown = good.clone();
        with_unknown.extend_from_slice(&[0x98, 0x06, 0x07]); // field 99 varint
        assert!(decode_export_request(&with_unknown).is_ok());
    }
}
