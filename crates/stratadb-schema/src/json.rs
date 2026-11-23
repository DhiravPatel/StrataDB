//! JSON / NDJSON payload mapping to canonical events (spec §6.4 unknown-field policy).
//!
//! Mapping (documented in docs/api/ingestion.md):
//! * `event_time` | `timestamp`: RFC 3339 string or integer Unix nanoseconds.
//!   Absent → ingest time.
//! * `service`, `trace_id`, `span_id`, `event_id`: strings. Hex IDs are lowercased.
//! * `severity` | `level`: string (normalized; original kept in `severity_text`
//!   if it is not the canonical spelling) or OTLP severity number.
//! * `body` | `message`: any JSON value.
//! * `attributes`: object; `resource` | `resource_attributes`: object.
//! * `ingest_time`, `schema_version`: reserved — rejected.
//! * `tenant_id` and every other top-level key: preserved in `attributes`
//!   (the payload can never set the canonical tenant).
//! * Supplying two aliases of the same field, or a top-level key that also
//!   appears inside `attributes`, is rejected rather than silently resolved.

use std::collections::BTreeMap;

use stratadb_common::time::parse_rfc3339;
use stratadb_common::{Error, Result, TenantId};

use crate::event::{Event, Severity};
use crate::validate::{validate_event, Limits};
use crate::value::Value;

fn invalid(field: &str, why: impl std::fmt::Display) -> Error {
    Error::invalid_input(format!("field `{field}`: {why}"))
}

/// Convert a JSON value to a typed [`Value`]. Integers that fit in i64 become
/// `Int`; larger non-negative integers become `UInt`; other numbers `Float`.
pub fn json_to_value(v: &serde_json::Value, depth: usize, lim: &Limits) -> Result<Value> {
    if depth > lim.max_depth {
        return Err(invalid(
            "attributes",
            format!("nesting exceeds {}", lim.max_depth),
        ));
    }
    Ok(match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt(u)
            } else {
                match n.as_f64() {
                    Some(f) if f.is_finite() => Value::Float(f),
                    _ => return Err(invalid("attributes", "number not representable")),
                }
            }
        }
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(a) => Value::List(
            a.iter()
                .map(|x| json_to_value(x, depth + 1, lim))
                .collect::<Result<_>>()?,
        ),
        serde_json::Value::Object(o) => Value::Map(json_object_to_map(o, depth + 1, lim)?),
    })
}

fn json_object_to_map(
    o: &serde_json::Map<String, serde_json::Value>,
    depth: usize,
    lim: &Limits,
) -> Result<BTreeMap<String, Value>> {
    o.iter()
        .map(|(k, v)| Ok((k.clone(), json_to_value(v, depth, lim)?)))
        .collect()
}

fn take_alias<'a>(
    o: &'a serde_json::Map<String, serde_json::Value>,
    names: &[&str],
) -> Result<Option<&'a serde_json::Value>> {
    let mut found: Option<(&str, &serde_json::Value)> = None;
    for n in names {
        if let Some(v) = o.get(*n) {
            if let Some((prev, _)) = found {
                return Err(invalid(
                    n,
                    format!("conflicts with `{prev}`; supply only one"),
                ));
            }
            found = Some((n, v));
        }
    }
    Ok(found.map(|(_, v)| v))
}

fn opt_string(field: &str, v: Option<&serde_json::Value>) -> Result<Option<String>> {
    match v {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(invalid(field, "must be a string")),
    }
}

const KNOWN: &[&str] = &[
    "event_time",
    "timestamp",
    "service",
    "severity",
    "level",
    "body",
    "message",
    "trace_id",
    "span_id",
    "event_id",
    "attributes",
    "resource",
    "resource_attributes",
];
const RESERVED: &[&str] = &["ingest_time", "schema_version"];

/// Build a canonical event from one JSON object and validate it.
pub fn event_from_json(
    v: &serde_json::Value,
    tenant: &TenantId,
    ingest_time: i64,
    lim: &Limits,
) -> Result<Event> {
    let o = v
        .as_object()
        .ok_or_else(|| Error::invalid_input("event must be a JSON object"))?;
    let mut ev = Event::new(tenant.clone(), ingest_time, ingest_time);

    if let Some(t) = take_alias(o, &["event_time", "timestamp"])? {
        ev.event_time = match t {
            serde_json::Value::String(s) => parse_rfc3339(s)?,
            serde_json::Value::Number(n) => n
                .as_i64()
                .ok_or_else(|| invalid("event_time", "integer nanoseconds expected"))?,
            _ => {
                return Err(invalid(
                    "event_time",
                    "RFC 3339 string or integer nanoseconds",
                ))
            }
        };
    }
    ev.service = opt_string("service", o.get("service"))?;
    match take_alias(o, &["severity", "level"])? {
        None | Some(serde_json::Value::Null) => {}
        Some(serde_json::Value::String(s)) => match Severity::parse(s) {
            Some(sev) => {
                ev.severity = sev;
                if s != sev.as_str() {
                    ev.severity_text = Some(s.clone());
                }
            }
            None => ev.severity_text = Some(s.clone()),
        },
        Some(serde_json::Value::Number(n)) => {
            let n = n
                .as_i64()
                .ok_or_else(|| invalid("severity", "integer severity number expected"))?;
            ev.severity = Severity::from_otlp_number(n);
            ev.severity_text = Some(n.to_string());
        }
        Some(_) => return Err(invalid("severity", "must be a string or number")),
    }
    if let Some(b) = take_alias(o, &["body", "message"])? {
        ev.body = Some(json_to_value(b, 1, lim)?);
    }
    ev.trace_id = opt_string("trace_id", o.get("trace_id"))?.map(|s| s.to_ascii_lowercase());
    ev.span_id = opt_string("span_id", o.get("span_id"))?.map(|s| s.to_ascii_lowercase());
    ev.event_id = opt_string("event_id", o.get("event_id"))?;
    match o.get("attributes") {
        None | Some(serde_json::Value::Null) => {}
        Some(serde_json::Value::Object(a)) => ev.attributes = json_object_to_map(a, 1, lim)?,
        Some(_) => return Err(invalid("attributes", "must be an object")),
    }
    match take_alias(o, &["resource", "resource_attributes"])? {
        None | Some(serde_json::Value::Null) => {}
        Some(serde_json::Value::Object(a)) => {
            ev.resource_attributes = json_object_to_map(a, 1, lim)?
        }
        Some(_) => return Err(invalid("resource_attributes", "must be an object")),
    }
    for (k, v) in o {
        if KNOWN.contains(&k.as_str()) {
            continue;
        }
        if RESERVED.contains(&k.as_str()) {
            return Err(invalid(k, "reserved field is assigned by the server"));
        }
        if ev.attributes.contains_key(k) {
            return Err(invalid(k, "present both at top level and in `attributes`"));
        }
        ev.attributes.insert(k.clone(), json_to_value(v, 1, lim)?);
    }
    validate_event(&ev, lim)?;
    Ok(ev)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn t() -> TenantId {
        TenantId::new("tenant-a").unwrap()
    }
    const NOW: i64 = 1_795_170_600_000_000_000;

    #[test]
    fn maps_spec_example() {
        let v = json!({
            "event_time": "2026-11-20T10:30:00.123Z",
            "service": "payment-service",
            "severity": "ERROR",
            "body": "Payment authorization timed out",
            "trace_id": "0123456789ABCDEF0123456789abcdef",
            "span_id": "0123456789abcdef",
            "attributes": {"request_id": "req_123", "http.status_code": 504, "region": "ap-south-1"}
        });
        let ev = event_from_json(&v, &t(), NOW, &Limits::default()).unwrap();
        assert_eq!(ev.event_time, NOW + 123_000_000);
        assert_eq!(ev.severity, Severity::Error);
        assert_eq!(ev.severity_text, None);
        assert_eq!(
            ev.trace_id.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(ev.attributes["http.status_code"], Value::Int(504));
        assert_eq!(ev.tenant_id, t());
    }

    #[test]
    fn payload_cannot_override_tenant() {
        let v = json!({"tenant_id": "tenant-b", "body": "x"});
        let ev = event_from_json(&v, &t(), NOW, &Limits::default()).unwrap();
        assert_eq!(ev.tenant_id, t());
        assert_eq!(ev.attributes["tenant_id"], Value::from("tenant-b"));
    }

    #[test]
    fn rejects_conflicts_and_reserved() {
        let lim = Limits::default();
        assert!(event_from_json(&json!({"body": 1, "message": 2}), &t(), NOW, &lim).is_err());
        assert!(event_from_json(&json!({"ingest_time": 1}), &t(), NOW, &lim).is_err());
        assert!(event_from_json(
            &json!({"region": "a", "attributes": {"region": "b"}}),
            &t(),
            NOW,
            &lim
        )
        .is_err());
        assert!(event_from_json(&json!([1, 2]), &t(), NOW, &lim).is_err());
        assert!(event_from_json(&json!({"event_time": 1.5}), &t(), NOW, &lim).is_err());
    }

    #[test]
    fn severity_variants() {
        let lim = Limits::default();
        let ev = event_from_json(&json!({"level": "warning"}), &t(), NOW, &lim).unwrap();
        assert_eq!(ev.severity, Severity::Warn);
        assert_eq!(ev.severity_text.as_deref(), Some("warning"));
        let ev = event_from_json(&json!({"severity": 18}), &t(), NOW, &lim).unwrap();
        assert_eq!(ev.severity, Severity::Error);
        let ev = event_from_json(&json!({"severity": "custom"}), &t(), NOW, &lim).unwrap();
        assert_eq!(ev.severity, Severity::Unspecified);
        assert_eq!(ev.severity_text.as_deref(), Some("custom"));
    }

    #[test]
    fn missing_null_empty_distinct() {
        let lim = Limits::default();
        let ev = event_from_json(&json!({"a": null, "b": ""}), &t(), NOW, &lim).unwrap();
        assert_eq!(ev.attributes["a"], Value::Null);
        assert_eq!(ev.attributes["b"], Value::from(""));
        assert!(!ev.attributes.contains_key("c"));
    }

    #[test]
    fn big_unsigned_preserved() {
        let lim = Limits::default();
        let ev = event_from_json(&json!({"n": u64::MAX}), &t(), NOW, &lim).unwrap();
        assert_eq!(ev.attributes["n"], Value::UInt(u64::MAX));
    }
}
