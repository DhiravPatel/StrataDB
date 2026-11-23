//! Event validation (spec §6.5, ticket B-005).
//!
//! Invalid events are rejected with an `InvalidInput` error naming the field;
//! they are never silently dropped or coerced.

use std::collections::BTreeMap;

use stratadb_common::{Error, Result};

use crate::event::Event;
use crate::value::Value;

/// Ingestion limits. Defaults are conservative and documented in
/// `docs/api/ingestion.md`; operators may tune them.
#[derive(Debug, Clone)]
pub struct Limits {
    /// Approximate serialized size of one event.
    pub max_event_bytes: usize,
    /// Total number of attribute entries (recursively, including list items)
    /// across `attributes` and `resource_attributes`.
    pub max_fields: usize,
    /// Maximum nesting depth of attribute values (a scalar at top level is depth 1).
    pub max_depth: usize,
    pub max_key_bytes: usize,
    /// Maximum length of any single string or byte value, including the body.
    pub max_string_bytes: usize,
    /// Events may be at most this far in the future relative to ingest time.
    pub max_future_skew_ns: i64,
    /// Events may be at most this far in the past relative to ingest time.
    pub max_past_age_ns: i64,
}

const HOUR_NS: i64 = 3_600 * 1_000_000_000;

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_event_bytes: 1024 * 1024,
            max_fields: 1024,
            max_depth: 8,
            max_key_bytes: 256,
            max_string_bytes: 256 * 1024,
            max_future_skew_ns: HOUR_NS,
            max_past_age_ns: 365 * 24 * HOUR_NS,
        }
    }
}

fn invalid(field: &str, why: impl std::fmt::Display) -> Error {
    Error::invalid_input(format!("field `{field}`: {why}"))
}

struct Walk<'a> {
    lim: &'a Limits,
    fields: usize,
    bytes: usize,
}

impl Walk<'_> {
    fn value(&mut self, path: &str, v: &Value, depth: usize) -> Result<()> {
        if depth > self.lim.max_depth {
            return Err(invalid(
                path,
                format!("nesting exceeds {}", self.lim.max_depth),
            ));
        }
        self.bytes += 9;
        match v {
            Value::Float(f) if !f.is_finite() => {
                return Err(invalid(path, "non-finite float (NaN/Inf) not allowed"))
            }
            Value::String(s) => self.string(path, s.len())?,
            Value::Bytes(b) => self.string(path, b.len())?,
            Value::List(items) => {
                for item in items {
                    self.count(path)?;
                    self.value(path, item, depth + 1)?;
                }
            }
            Value::Map(m) => self.map(path, m, depth + 1)?,
            _ => {}
        }
        Ok(())
    }

    fn string(&mut self, path: &str, len: usize) -> Result<()> {
        if len > self.lim.max_string_bytes {
            return Err(invalid(
                path,
                format!("length {len} exceeds {}", self.lim.max_string_bytes),
            ));
        }
        self.bytes += len;
        Ok(())
    }

    fn count(&mut self, path: &str) -> Result<()> {
        self.fields += 1;
        if self.fields > self.lim.max_fields {
            return Err(invalid(
                path,
                format!(
                    "event has more than {} attribute fields",
                    self.lim.max_fields
                ),
            ));
        }
        Ok(())
    }

    fn map(&mut self, path: &str, m: &BTreeMap<String, Value>, depth: usize) -> Result<()> {
        for (k, v) in m {
            validate_key(path, k, self.lim)?;
            self.count(path)?;
            self.bytes += k.len();
            self.value(k, v, depth)?;
        }
        Ok(())
    }
}

fn validate_key(path: &str, k: &str, lim: &Limits) -> Result<()> {
    if k.is_empty() {
        return Err(invalid(path, "empty attribute key"));
    }
    if k.len() > lim.max_key_bytes {
        return Err(invalid(
            path,
            format!("key longer than {} bytes", lim.max_key_bytes),
        ));
    }
    if k.chars().any(char::is_control) {
        return Err(invalid(path, "attribute key contains control characters"));
    }
    Ok(())
}

fn hex_id(field: &str, v: &Option<String>, len: usize) -> Result<()> {
    if let Some(s) = v {
        if s.len() != len || !s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(invalid(
                field,
                format!("must be {len} lowercase hex characters"),
            ));
        }
    }
    Ok(())
}

fn short_string(field: &str, v: &Option<String>, max: usize) -> Result<()> {
    if let Some(s) = v {
        if s.is_empty() || s.len() > max {
            return Err(invalid(field, format!("must be 1..={max} bytes")));
        }
        if s.chars().any(char::is_control) {
            return Err(invalid(field, "contains control characters"));
        }
    }
    Ok(())
}

/// Validate an event against `lim`. The tenant identity is assumed to have
/// been set from the authenticated principal by the caller.
pub fn validate_event(ev: &Event, lim: &Limits) -> Result<()> {
    let min = ev.ingest_time.saturating_sub(lim.max_past_age_ns);
    let max = ev.ingest_time.saturating_add(lim.max_future_skew_ns);
    if ev.event_time < min {
        return Err(invalid("event_time", "older than the maximum accepted age"));
    }
    if ev.event_time > max {
        return Err(invalid("event_time", "too far in the future"));
    }
    hex_id("trace_id", &ev.trace_id, 32)?;
    hex_id("span_id", &ev.span_id, 16)?;
    short_string("service", &ev.service, 256)?;
    short_string("event_id", &ev.event_id, 256)?;
    short_string("severity_text", &ev.severity_text, 64)?;
    let mut w = Walk {
        lim,
        fields: 0,
        bytes: 64,
    };
    if let Some(body) = &ev.body {
        w.value("body", body, 1)?;
    }
    w.map("attributes", &ev.attributes, 1)?;
    w.map("resource_attributes", &ev.resource_attributes, 1)?;
    if w.bytes > lim.max_event_bytes {
        return Err(invalid(
            "event",
            format!("serialized size exceeds {} bytes", lim.max_event_bytes),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use stratadb_common::TenantId;

    fn ev() -> Event {
        Event::new(
            TenantId::new("t").unwrap(),
            1_000 * HOUR_NS,
            1_000 * HOUR_NS,
        )
    }

    #[test]
    fn accepts_valid() {
        let mut e = ev();
        e.trace_id = Some("0123456789abcdef0123456789abcdef".into());
        e.span_id = Some("0123456789abcdef".into());
        e.attributes
            .insert("http.status_code".into(), Value::Int(504));
        validate_event(&e, &Limits::default()).unwrap();
    }

    #[test]
    fn rejects_bad_fields() {
        let lim = Limits::default();
        let mut e = ev();
        e.trace_id = Some("XYZ".into());
        assert!(validate_event(&e, &lim).is_err());

        let mut e = ev();
        e.event_time += 2 * HOUR_NS;
        assert!(validate_event(&e, &lim).is_err());

        let mut e = ev();
        e.attributes.insert("f".into(), Value::Float(f64::NAN));
        assert!(validate_event(&e, &lim).is_err());

        let mut e = ev();
        e.attributes.insert(String::new(), Value::Null);
        assert!(validate_event(&e, &lim).is_err());

        let mut e = ev();
        let mut v = Value::Int(1);
        for _ in 0..10 {
            v = Value::List(vec![v]);
        }
        e.attributes.insert("deep".into(), v);
        assert!(validate_event(&e, &lim).is_err());

        let mut e = ev();
        e.body = Some(Value::String("x".repeat(lim.max_string_bytes + 1)));
        assert!(validate_event(&e, &lim).is_err());

        let mut e = ev();
        for i in 0..=lim.max_fields {
            e.attributes.insert(format!("k{i}"), Value::Null);
        }
        assert!(validate_event(&e, &lim).is_err());
    }
}
