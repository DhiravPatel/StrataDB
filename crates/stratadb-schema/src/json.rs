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

/// JSON form of a value (export/query output). JSON-native types map 1:1;
/// `Timestamp` becomes an RFC 3339 string and `Bytes` a lowercase hex string,
/// so those two do not round-trip through [`json_to_value`] (they come back as
/// strings).
pub fn value_to_json(v: &Value) -> serde_json::Value {
    use serde_json::Value as J;
    match v {
        Value::Null => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Int(i) => J::from(*i),
        Value::UInt(u) => J::from(*u),
        Value::Float(f) => serde_json::Number::from_f64(*f).map_or(J::Null, J::Number),
        Value::String(s) => J::String(s.clone()),
        Value::Bytes(b) => J::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        Value::Timestamp(t) => J::String(stratadb_common::time::format_rfc3339(*t)),
        Value::List(l) => J::Array(l.iter().map(value_to_json).collect()),
        Value::Map(m) => J::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        ),
    }
}

/// The JSON input that [`event_from_json`] maps back to `ev` (except
/// `ingest_time`, `schema_version` and `tenant_id`, which the server assigns).
///
/// Exact inverse for every event produced by [`event_from_json`]:
/// `event_from_json(&event_to_json(&e), ..) == e` up to `ingest_time`
/// (property-tested). Lossy for events from other sources only where JSON
/// cannot express them: `Timestamp`/`Bytes` values nested in body or
/// attributes (exported as strings), non-finite floats (null), and an OTLP
/// severity number paired with unrelated severity text (the normalized
/// severity is kept, the text dropped).
pub fn event_to_json(ev: &Event) -> serde_json::Value {
    let mut o = serde_json::Map::new();
    o.insert(
        "event_time".into(),
        serde_json::Value::String(stratadb_common::time::format_rfc3339(ev.event_time)),
    );
    let s = |x: &str| serde_json::Value::String(x.to_owned());
    if let Some(v) = &ev.service {
        o.insert("service".into(), s(v));
    }
    if let Some(sev) = severity_to_json(ev.severity, ev.severity_text.as_deref()) {
        o.insert("severity".into(), sev);
    }
    if let Some(b) = &ev.body {
        o.insert("body".into(), value_to_json(b));
    }
    for (k, v) in [
        ("trace_id", &ev.trace_id),
        ("span_id", &ev.span_id),
        ("event_id", &ev.event_id),
    ] {
        if let Some(v) = v {
            o.insert(k.into(), s(v));
        }
    }
    let map = |m: &BTreeMap<String, Value>| {
        serde_json::Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect(),
        )
    };
    if !ev.attributes.is_empty() {
        o.insert("attributes".into(), map(&ev.attributes));
    }
    if !ev.resource_attributes.is_empty() {
        o.insert("resource_attributes".into(), map(&ev.resource_attributes));
    }
    serde_json::Value::Object(o)
}

/// Inverse of the `severity`/`level` input rules of [`event_from_json`]: the
/// JSON `severity` input that reproduces `(sev, text)` (`None`: omit it).
pub fn severity_to_json(sev: Severity, text: Option<&str>) -> Option<serde_json::Value> {
    let Some(t) = text else {
        // Input was absent (Unspecified) or the canonical spelling.
        return (sev != Severity::Unspecified).then(|| serde_json::Value::from(sev.as_str()));
    };
    // Input was a number n: severity = from_otlp_number(n), text = n.to_string().
    if let Ok(n) = t.parse::<i64>() {
        if n.to_string() == t && Severity::from_otlp_number(n) == sev {
            return Some(serde_json::Value::from(n));
        }
    }
    // Input was a known non-canonical spelling, or unknown text (Unspecified).
    let parsed = Severity::parse(t);
    if parsed == Some(sev) || (parsed.is_none() && sev == Severity::Unspecified) {
        return Some(serde_json::Value::from(t));
    }
    // Not producible by JSON ingest (e.g. OTLP number + unrelated text).
    (sev != Severity::Unspecified).then(|| serde_json::Value::from(sev.as_str()))
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

#[cfg(test)]
mod roundtrip {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;

    pub(super) const NOW: i64 = 1_795_170_600_123_456_789;
    const DAY: i64 = 86_400_000_000_000;

    fn arb_json(depth: u32) -> BoxedStrategy<serde_json::Value> {
        let leaf = prop_oneof![
            Just(serde_json::Value::Null),
            any::<bool>().prop_map(serde_json::Value::from),
            any::<i64>().prop_map(serde_json::Value::from),
            (i64::MAX as u64..=u64::MAX).prop_map(serde_json::Value::from),
            (-1e12f64..1e12).prop_map(serde_json::Value::from),
            "[a-zA-Z0-9 _.%\u{e9}-]{0,12}".prop_map(serde_json::Value::from),
        ];
        if depth == 0 {
            return leaf.boxed();
        }
        prop_oneof![
            3 => leaf,
            1 => prop::collection::vec(arb_json(depth - 1), 0..3).prop_map(serde_json::Value::Array),
            1 => prop::collection::btree_map("[a-z.]{1,6}", arb_json(depth - 1), 0..3)
                .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
        ]
        .boxed()
    }

    pub(super) fn arb_input_pub() -> impl Strategy<Value = serde_json::Value> {
        arb_input()
    }

    fn arb_input() -> impl Strategy<Value = serde_json::Value> {
        let severity = prop_oneof![
            Just(None),
            prop::sample::select(vec![
                "ERROR",
                "error",
                "Warning",
                "INFO",
                "notice",
                "UNSPECIFIED",
                "",
                "boom",
                "17",
                "007",
            ])
            .prop_map(|s| Some(json!(s))),
            (-3i64..30).prop_map(|n| Some(json!(n))),
        ];
        (
            // Within the accepted window around NOW (max age / future skew).
            prop::option::of(NOW - 30 * DAY..NOW + 3_000_000_000_000),
            prop::option::of("[a-z-]{1,8}"),
            severity,
            prop::option::of(arb_json(2)),
            prop::option::of("[0-9a-fA-F]{32}"),
            prop::option::of("[a-z0-9]{1,8}"),
            prop::collection::btree_map("[a-z.]{1,6}", arb_json(1), 0..4),
            prop::collection::btree_map("[a-z.]{1,6}", arb_json(1), 0..3),
            prop::collection::btree_map("x_[a-z]{1,4}", arb_json(1), 0..2),
        )
            .prop_map(|(t, svc, sev, body, trace, eid, attrs, res, extra)| {
                let mut o = serde_json::Map::new();
                if let Some(t) = t {
                    o.insert("event_time".into(), json!(t));
                }
                if let Some(s) = svc {
                    o.insert("service".into(), json!(s));
                }
                if let Some(s) = sev {
                    o.insert("severity".into(), s);
                }
                if let Some(b) = body {
                    o.insert("body".into(), b);
                }
                if let Some(t) = trace {
                    o.insert("trace_id".into(), json!(t));
                }
                if let Some(e) = eid {
                    o.insert("event_id".into(), json!(e));
                }
                if !attrs.is_empty() {
                    o.insert("attributes".into(), json!(attrs));
                }
                if !res.is_empty() {
                    o.insert("resource".into(), json!(res));
                }
                // Unknown top-level keys land in attributes on ingest.
                for (k, v) in extra {
                    o.insert(k, v);
                }
                serde_json::Value::Object(o)
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2000))]
        /// export(ingest(j)) re-ingests to the identical event.
        #[test]
        fn event_json_round_trips(j in arb_input()) {
            let t = TenantId::new("rt").unwrap();
            let lim = Limits::default();
            if let Ok(e) = event_from_json(&j, &t, NOW, &lim) {
                let out = event_to_json(&e);
                let back = event_from_json(&out, &t, NOW, &lim)
                    .map_err(|err| TestCaseError::fail(format!("{err}: {out}")))?;
                prop_assert_eq!(back, e, "exported as {}", out);
            }
        }
    }

    #[test]
    fn documented_lossy_cases() {
        let t = TenantId::new("rt").unwrap();
        let mut e = Event::new(t.clone(), 5, 5);
        e.attributes.insert("ts".into(), Value::Timestamp(0));
        e.attributes
            .insert("raw".into(), Value::Bytes(vec![1, 255]));
        e.severity = Severity::Error;
        e.severity_text = Some("boom".into()); // OTLP-style pair
        let j = event_to_json(&e);
        assert_eq!(j["attributes"]["ts"], "1970-01-01T00:00:00.000000000Z");
        assert_eq!(j["attributes"]["raw"], "01ff");
        assert_eq!(j["severity"], "ERROR");
    }
}

#[cfg(test)]
mod roundtrip_coverage {
    use super::*;
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::TestRunner;

    /// Guard against a vacuous round-trip test: most generated inputs must be
    /// valid events, and every severity input form must be exercised.
    #[test]
    fn generator_produces_mostly_valid_events() {
        let mut runner = TestRunner::deterministic();
        let t = TenantId::new("rt").unwrap();
        let (mut ok, mut with_text, mut numeric) = (0, 0, 0);
        for _ in 0..1000 {
            let j = super::roundtrip::arb_input_pub()
                .new_tree(&mut runner)
                .unwrap()
                .current();
            if let Ok(e) = event_from_json(&j, &t, super::roundtrip::NOW, &Limits::default()) {
                ok += 1;
                with_text += usize::from(e.severity_text.is_some());
                numeric += usize::from(j.get("severity").is_some_and(serde_json::Value::is_number));
            }
        }
        assert!(ok > 800, "only {ok}/1000 generated inputs were valid");
        assert!(with_text > 100 && numeric > 50, "{with_text} {numeric}");
    }
}
