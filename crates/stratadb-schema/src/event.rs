//! The canonical event (spec §2.2, §6.1).

use std::collections::BTreeMap;

use stratadb_common::TenantId;

use crate::value::Value;

/// Version of the canonical event schema written by this build.
pub const EVENT_SCHEMA_VERSION: u32 = 1;

/// Normalized severity. Numeric values follow the OpenTelemetry severity
/// number ranges (base value of each range), so OTLP mapping is lossless at
/// the bucket level; the original text/number is kept in `severity_text`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
pub enum Severity {
    #[default]
    Unspecified = 0,
    Trace = 1,
    Debug = 5,
    Info = 9,
    Warn = 13,
    Error = 17,
    Fatal = 21,
}

impl Severity {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => Severity::Unspecified,
            1 => Severity::Trace,
            5 => Severity::Debug,
            9 => Severity::Info,
            13 => Severity::Warn,
            17 => Severity::Error,
            21 => Severity::Fatal,
            _ => return None,
        })
    }

    /// Map an OTLP SeverityNumber (1..=24) to its bucket.
    pub fn from_otlp_number(n: i64) -> Self {
        match n {
            1..=4 => Severity::Trace,
            5..=8 => Severity::Debug,
            9..=12 => Severity::Info,
            13..=16 => Severity::Warn,
            17..=20 => Severity::Error,
            21..=24 => Severity::Fatal,
            _ => Severity::Unspecified,
        }
    }

    /// Case-insensitive parse of common severity spellings. Unknown text maps
    /// to `None`; callers keep the original text in `severity_text`.
    pub fn parse(text: &str) -> Option<Self> {
        let t = text.trim().to_ascii_lowercase();
        Some(match t.as_str() {
            "trace" | "verbose" => Severity::Trace,
            "debug" | "dbg" => Severity::Debug,
            "info" | "information" | "informational" | "notice" => Severity::Info,
            "warn" | "warning" => Severity::Warn,
            "error" | "err" => Severity::Error,
            "fatal" | "critical" | "crit" | "alert" | "emerg" | "emergency" | "panic" => {
                Severity::Fatal
            }
            "unspecified" | "" => Severity::Unspecified,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Unspecified => "UNSPECIFIED",
            Severity::Trace => "TRACE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Warn => "WARN",
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
        }
    }
}

/// A canonical log event.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// Producer-supplied event time, Unix nanoseconds UTC. Not trusted for
    /// ordering or security (spec §7.5).
    pub event_time: i64,
    /// Time StrataDB accepted the event, Unix nanoseconds UTC (server clock).
    pub ingest_time: i64,
    /// Derived from the authenticated principal, never from the payload.
    pub tenant_id: TenantId,
    pub service: Option<String>,
    pub severity: Severity,
    /// Original severity representation when it differs from the normalized form.
    pub severity_text: Option<String>,
    pub body: Option<Value>,
    /// Lowercase hex, 32 chars.
    pub trace_id: Option<String>,
    /// Lowercase hex, 16 chars.
    pub span_id: Option<String>,
    pub event_id: Option<String>,
    pub attributes: BTreeMap<String, Value>,
    pub resource_attributes: BTreeMap<String, Value>,
    pub schema_version: u32,
}

impl Event {
    /// Minimal event with the required fields set; useful for SDKs and tests.
    pub fn new(tenant_id: TenantId, event_time: i64, ingest_time: i64) -> Self {
        Self {
            event_time,
            ingest_time,
            tenant_id,
            service: None,
            severity: Severity::Unspecified,
            severity_text: None,
            body: None,
            trace_id: None,
            span_id: None,
            event_id: None,
            attributes: BTreeMap::new(),
            resource_attributes: BTreeMap::new(),
            schema_version: EVENT_SCHEMA_VERSION,
        }
    }

    /// Approximate in-memory footprint used for memtable budgets.
    pub fn approx_size(&self) -> usize {
        let opt = |s: &Option<String>| s.as_ref().map_or(0, String::len);
        std::mem::size_of::<Event>()
            + self.tenant_id.as_str().len()
            + opt(&self.service)
            + opt(&self.severity_text)
            + opt(&self.trace_id)
            + opt(&self.span_id)
            + opt(&self.event_id)
            + self.body.as_ref().map_or(0, Value::approx_size)
            + self
                .attributes
                .iter()
                .chain(self.resource_attributes.iter())
                .map(|(k, v)| k.len() + 32 + v.approx_size())
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_roundtrip_and_parse() {
        for s in [
            Severity::Unspecified,
            Severity::Trace,
            Severity::Debug,
            Severity::Info,
            Severity::Warn,
            Severity::Error,
            Severity::Fatal,
        ] {
            assert_eq!(Severity::from_u8(s.as_u8()), Some(s));
            assert_eq!(Severity::parse(s.as_str()), Some(s));
        }
        assert_eq!(Severity::parse("Warning"), Some(Severity::Warn));
        assert_eq!(Severity::parse("bogus"), None);
        assert_eq!(Severity::from_otlp_number(18), Severity::Error);
        assert_eq!(Severity::from_otlp_number(99), Severity::Unspecified);
        assert_eq!(Severity::from_u8(2), None);
    }
}
