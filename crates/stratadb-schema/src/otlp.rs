//! OTLP logs (OTLP/HTTP, JSON encoding) → canonical events (spec §7.6, §23.2).
//!
//! Mapping (normative: docs/api/otlp.md):
//! | OTLP | StrataDB |
//! |------|----------|
//! | `resource.attributes` | `resource_attributes` |
//! | resource attribute `service.name` (string) | `service` |
//! | `timeUnixNano` (or `observedTimeUnixNano` if 0/absent) | `event_time` (else ingest time) |
//! | `severityNumber` | `severity` bucket; number kept in `attributes["otel.severity_number"]` when not a bucket base |
//! | `severityText` | `severity_text` |
//! | `body` (AnyValue) | `body` (typed) |
//! | `attributes` | `attributes` |
//! | `traceId` / `spanId` (hex) | `trace_id` / `span_id` (lowercase; all-zero or empty → missing) |
//! | `eventName` | `attributes["otel.event_name"]` |
//! | `flags` (≠ 0) | `attributes["otel.flags"]` |
//! | `droppedAttributesCount` (≠ 0) | `attributes["otel.dropped_attributes_count"]` |
//! | `scope.name` / `scope.version` / `scope.attributes` | `attributes["otel.scope.name"]`, `["otel.scope.version"]`, `["otel.scope.attributes"]` (map) |
//!
//! A log-record attribute that collides with one of the `otel.*` keys is
//! rejected rather than overwritten. Unknown JSON fields are ignored (OTLP
//! forward compatibility). int64 values are accepted as JSON strings or numbers.

use std::collections::BTreeMap;

use serde_json::Value as J;
use stratadb_common::{Error, Result, TenantId};

use crate::event::{Event, Severity};
use crate::validate::{validate_event, Limits};
use crate::value::Value;

fn bad(msg: impl Into<String>) -> Error {
    Error::invalid_input(msg.into())
}

/// proto3 JSON int64/uint64: string or number.
fn int64(v: &J) -> Result<Option<i64>> {
    match v {
        J::Null => Ok(None),
        J::Number(n) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| bad("expected an integer")),
        J::String(s) => s
            .parse::<i64>()
            .map(Some)
            .map_err(|_| bad(format!("invalid int64 {s:?}"))),
        _ => Err(bad("expected an integer")),
    }
}

fn uint64_as_i64(v: &J, field: &str) -> Result<Option<i64>> {
    let u = match v {
        J::Null => return Ok(None),
        J::Number(n) => n
            .as_u64()
            .ok_or_else(|| bad(format!("{field}: expected unsigned integer")))?,
        J::String(s) => s
            .parse::<u64>()
            .map_err(|_| bad(format!("{field}: invalid uint64 {s:?}")))?,
        _ => return Err(bad(format!("{field}: expected unsigned integer"))),
    };
    if u == 0 {
        return Ok(None);
    }
    i64::try_from(u)
        .map(Some)
        .map_err(|_| bad(format!("{field}: timestamp beyond year 2262")))
}

fn b64_decode(s: &str) -> Result<Vec<u8>> {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0u32);
    for c in s.bytes().filter(|c| *c != b'=') {
        let v = T
            .iter()
            .position(|t| *t == c)
            .ok_or_else(|| bad("bytesValue is not base64"))? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Ok(out)
}

/// OTLP AnyValue → typed value. `depth` guards nesting.
pub fn any_value(v: &J, depth: usize, lim: &Limits) -> Result<Value> {
    if depth > lim.max_depth {
        return Err(bad("OTLP value nested too deeply"));
    }
    let o = match v {
        J::Object(o) => o,
        J::Null => return Ok(Value::Null),
        _ => return Err(bad("AnyValue must be an object")),
    };
    if o.is_empty() {
        return Ok(Value::Null);
    }
    if let Some(s) = o.get("stringValue") {
        return s
            .as_str()
            .map(Value::from)
            .ok_or_else(|| bad("stringValue must be a string"));
    }
    if let Some(b) = o.get("boolValue") {
        return b
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| bad("boolValue must be a bool"));
    }
    if let Some(i) = o.get("intValue") {
        return int64(i)?
            .map(Value::Int)
            .ok_or_else(|| bad("intValue missing"));
    }
    if let Some(d) = o.get("doubleValue") {
        let f = match d {
            J::Number(n) => n.as_f64(),
            J::String(s) => s.parse::<f64>().ok(), // "NaN"/"Infinity" parse but fail validation
            _ => None,
        }
        .ok_or_else(|| bad("doubleValue must be a number"))?;
        return Ok(Value::Float(f));
    }
    if let Some(b) = o.get("bytesValue") {
        return b
            .as_str()
            .ok_or_else(|| bad("bytesValue must be base64"))
            .and_then(b64_decode)
            .map(Value::Bytes);
    }
    if let Some(a) = o.get("arrayValue") {
        let vals = a
            .get("values")
            .and_then(J::as_array)
            .map_or(&[][..], Vec::as_slice);
        return vals
            .iter()
            .map(|x| any_value(x, depth + 1, lim))
            .collect::<Result<_>>()
            .map(Value::List);
    }
    if let Some(kv) = o.get("kvlistValue") {
        return key_values(kv.get("values").unwrap_or(&J::Null), depth + 1, lim).map(Value::Map);
    }
    Err(bad("unknown AnyValue variant"))
}

/// `[KeyValue]` → map. Duplicate keys are rejected (not silently resolved).
pub fn key_values(v: &J, depth: usize, lim: &Limits) -> Result<BTreeMap<String, Value>> {
    let mut m = BTreeMap::new();
    let Some(items) = v.as_array() else {
        return match v {
            J::Null => Ok(m),
            _ => Err(bad("attributes must be an array of {key, value}")),
        };
    };
    for kv in items {
        let key = kv
            .get("key")
            .and_then(J::as_str)
            .ok_or_else(|| bad("attribute without key"))?;
        let val = any_value(kv.get("value").unwrap_or(&J::Null), depth, lim)?;
        if m.insert(key.to_owned(), val).is_some() {
            return Err(bad(format!("duplicate attribute key {key:?}")));
        }
    }
    Ok(m)
}

fn hex_id(v: Option<&J>, len: usize, field: &str) -> Result<Option<String>> {
    let Some(s) = v.and_then(J::as_str) else {
        return Ok(None);
    };
    if s.is_empty() || s.bytes().all(|b| b == b'0') {
        return Ok(None); // OTLP: empty / all-zero means "not set"
    }
    if s.len() != len || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(bad(format!("{field} must be {len} hex characters")));
    }
    Ok(Some(s.to_ascii_lowercase()))
}

fn put_reserved(attrs: &mut BTreeMap<String, Value>, k: &str, v: Value) -> Result<()> {
    if attrs.contains_key(k) {
        return Err(bad(format!(
            "log attribute {k:?} collides with an OTLP mapping key"
        )));
    }
    attrs.insert(k.to_owned(), v);
    Ok(())
}

/// Result of mapping one export request: per-record outcomes in order.
#[derive(Debug)]
pub struct OtlpBatch {
    pub records: Vec<Result<Event>>,
}

/// Map an `ExportLogsServiceRequest` (JSON). Structural errors in the
/// envelope fail the whole request; errors in individual log records are
/// reported per record.
pub fn map_export_request(
    req: &J,
    tenant: &TenantId,
    ingest_time: i64,
    lim: &Limits,
) -> Result<OtlpBatch> {
    let rls = req
        .get("resourceLogs")
        .map(|v| {
            v.as_array()
                .ok_or_else(|| bad("resourceLogs must be an array"))
        })
        .transpose()?
        .map_or(&[][..], Vec::as_slice);
    let mut records = Vec::new();
    for rl in rls {
        let resource = key_values(
            rl.pointer("/resource/attributes").unwrap_or(&J::Null),
            1,
            lim,
        )?;
        let service = resource
            .get("service.name")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let sls = rl
            .get("scopeLogs")
            .and_then(J::as_array)
            .map_or(&[][..], Vec::as_slice);
        for sl in sls {
            let scope = sl.get("scope");
            let scope_name = scope
                .and_then(|s| s.get("name"))
                .and_then(J::as_str)
                .filter(|s| !s.is_empty());
            let scope_version = scope
                .and_then(|s| s.get("version"))
                .and_then(J::as_str)
                .filter(|s| !s.is_empty());
            let scope_attrs = key_values(
                scope.and_then(|s| s.get("attributes")).unwrap_or(&J::Null),
                1,
                lim,
            )?;
            let lrs = sl
                .get("logRecords")
                .and_then(J::as_array)
                .map_or(&[][..], Vec::as_slice);
            for lr in lrs {
                records.push((|| -> Result<Event> {
                    let mut e = Event::new(tenant.clone(), ingest_time, ingest_time);
                    let t =
                        uint64_as_i64(lr.get("timeUnixNano").unwrap_or(&J::Null), "timeUnixNano")?;
                    let ot = uint64_as_i64(
                        lr.get("observedTimeUnixNano").unwrap_or(&J::Null),
                        "observedTimeUnixNano",
                    )?;
                    if let Some(ts) = t.or(ot) {
                        e.event_time = ts;
                    }
                    e.service = service.clone();
                    e.resource_attributes = resource.clone();
                    e.attributes = key_values(lr.get("attributes").unwrap_or(&J::Null), 1, lim)?;
                    let num = lr
                        .get("severityNumber")
                        .map(int64)
                        .transpose()?
                        .flatten()
                        .unwrap_or(0);
                    let text = lr
                        .get("severityText")
                        .and_then(J::as_str)
                        .filter(|s| !s.is_empty());
                    e.severity = if num != 0 {
                        Severity::from_otlp_number(num)
                    } else {
                        text.and_then(Severity::parse)
                            .unwrap_or(Severity::Unspecified)
                    };
                    if num != 0 && i64::from(e.severity.as_u8()) != num {
                        put_reserved(&mut e.attributes, "otel.severity_number", Value::Int(num))?;
                    }
                    e.severity_text = text.map(str::to_owned);
                    if let Some(b) = lr.get("body") {
                        let v = any_value(b, 1, lim)?;
                        if v != Value::Null {
                            e.body = Some(v);
                        }
                    }
                    e.trace_id = hex_id(lr.get("traceId"), 32, "traceId")?;
                    e.span_id = hex_id(lr.get("spanId"), 16, "spanId")?;
                    if let Some(n) = lr
                        .get("eventName")
                        .and_then(J::as_str)
                        .filter(|s| !s.is_empty())
                    {
                        put_reserved(&mut e.attributes, "otel.event_name", Value::from(n))?;
                    }
                    if let Some(f) = lr.get("flags").and_then(J::as_u64).filter(|f| *f != 0) {
                        put_reserved(&mut e.attributes, "otel.flags", Value::Int(f as i64))?;
                    }
                    if let Some(d) = lr
                        .get("droppedAttributesCount")
                        .and_then(J::as_u64)
                        .filter(|d| *d != 0)
                    {
                        put_reserved(
                            &mut e.attributes,
                            "otel.dropped_attributes_count",
                            Value::Int(d as i64),
                        )?;
                    }
                    if let Some(n) = scope_name {
                        put_reserved(&mut e.attributes, "otel.scope.name", Value::from(n))?;
                    }
                    if let Some(v) = scope_version {
                        put_reserved(&mut e.attributes, "otel.scope.version", Value::from(v))?;
                    }
                    if !scope_attrs.is_empty() {
                        put_reserved(
                            &mut e.attributes,
                            "otel.scope.attributes",
                            Value::Map(scope_attrs.clone()),
                        )?;
                    }
                    validate_event(&e, lim)?;
                    Ok(e)
                })());
            }
        }
    }
    Ok(OtlpBatch { records })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: i64 = 1_795_170_600_000_000_000;

    /// Shape of the example in the OTLP specification repository
    /// (examples/logs.json), adapted to recent timestamps.
    fn example() -> J {
        json!({"resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "my.service"}},
                {"key": "host.cpu", "value": {"intValue": "8"}}]},
            "scopeLogs": [{
                "scope": {"name": "my.library", "version": "1.0.0",
                          "attributes": [{"key": "my.scope.attribute", "value": {"stringValue": "some scope attribute"}}]},
                "logRecords": [{
                    "timeUnixNano": "1795170600000000000",
                    "observedTimeUnixNano": "1795170600100000000",
                    "severityNumber": 10,
                    "severityText": "Information",
                    "traceId": "5B8EFFF798038103D269B633813FC60C",
                    "spanId": "EEE19B7EC3C1B174",
                    "body": {"stringValue": "Example log record"},
                    "attributes": [
                        {"key": "string.attribute", "value": {"stringValue": "some string"}},
                        {"key": "boolean.attribute", "value": {"boolValue": true}},
                        {"key": "int.attribute", "value": {"intValue": "10"}},
                        {"key": "double.attribute", "value": {"doubleValue": 637.704}},
                        {"key": "array.attribute", "value": {"arrayValue": {"values": [
                            {"stringValue": "many"}, {"stringValue": "values"}]}}},
                        {"key": "map.attribute", "value": {"kvlistValue": {"values": [
                            {"key": "some.map.key", "value": {"stringValue": "some value"}}]}}},
                        {"key": "bytes.attribute", "value": {"bytesValue": "aGVsbG8="}}],
                    "flags": 1,
                    "eventName": "my.event"
                }]
            }]
        }]})
    }

    #[test]
    fn maps_specification_example() {
        let t = TenantId::new("acme").unwrap();
        let b = map_export_request(&example(), &t, NOW, &Limits::default()).unwrap();
        let e = b.records.into_iter().next().unwrap().unwrap();
        assert_eq!(e.tenant_id, t);
        assert_eq!(e.event_time, NOW);
        assert_eq!(e.service.as_deref(), Some("my.service"));
        assert_eq!(e.severity, Severity::Info);
        assert_eq!(e.severity_text.as_deref(), Some("Information"));
        assert_eq!(e.attributes["otel.severity_number"], Value::Int(10));
        assert_eq!(
            e.trace_id.as_deref(),
            Some("5b8efff798038103d269b633813fc60c")
        );
        assert_eq!(e.span_id.as_deref(), Some("eee19b7ec3c1b174"));
        assert_eq!(e.body, Some(Value::from("Example log record")));
        assert_eq!(e.attributes["int.attribute"], Value::Int(10));
        assert_eq!(e.attributes["double.attribute"], Value::Float(637.704));
        assert_eq!(
            e.attributes["bytes.attribute"],
            Value::Bytes(b"hello".to_vec())
        );
        assert_eq!(
            e.attributes["array.attribute"],
            Value::List(vec!["many".into(), "values".into()])
        );
        assert!(matches!(e.attributes["map.attribute"], Value::Map(_)));
        assert_eq!(e.attributes["otel.event_name"], Value::from("my.event"));
        assert_eq!(e.attributes["otel.flags"], Value::Int(1));
        assert_eq!(e.attributes["otel.scope.name"], Value::from("my.library"));
        assert_eq!(e.attributes["otel.scope.version"], Value::from("1.0.0"));
        assert_eq!(e.resource_attributes["host.cpu"], Value::Int(8));
    }

    #[test]
    fn per_record_errors_and_defaults() {
        let t = TenantId::new("acme").unwrap();
        let req = json!({"resourceLogs": [{"scopeLogs": [{"logRecords": [
            {"body": {"stringValue": "no time"}, "traceId": "", "spanId": "0000000000000000"},
            {"traceId": "xyz"},
            {"observedTimeUnixNano": 1795170600000000000u64, "severityText": "WARN"},
            {"attributes": [{"key": "otel.flags", "value": {"intValue": 1}}], "flags": 1},
            {"attributes": [{"key": "a", "value": {"intValue": 1}}, {"key": "a", "value": {"intValue": 2}}]}
        ]}]}]});
        let b = map_export_request(&req, &t, NOW, &Limits::default()).unwrap();
        let r: Vec<_> = b.records;
        let first = r[0].as_ref().unwrap();
        assert_eq!(
            first.event_time, NOW,
            "absent timestamps fall back to ingest time"
        );
        assert_eq!(
            (first.trace_id.clone(), first.span_id.clone()),
            (None, None)
        );
        assert!(r[1].is_err(), "invalid traceId");
        let third = r[2].as_ref().unwrap();
        assert_eq!(third.event_time, NOW);
        assert_eq!(third.severity, Severity::Warn);
        assert!(r[3].is_err(), "collision with mapping key is rejected");
        assert!(r[4].is_err(), "duplicate attribute keys are rejected");
        // Envelope errors fail the request.
        assert!(
            map_export_request(&json!({"resourceLogs": 5}), &t, NOW, &Limits::default()).is_err()
        );
        // Empty request is valid and yields nothing.
        assert!(map_export_request(&json!({}), &t, NOW, &Limits::default())
            .unwrap()
            .records
            .is_empty());
    }

    #[test]
    fn base64() {
        assert_eq!(b64_decode("aGVsbG8gd29ybGQ=").unwrap(), b"hello world");
        assert_eq!(b64_decode("").unwrap(), b"");
        assert!(b64_decode("*").is_err());
    }
}
