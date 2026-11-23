//! Versioned binary encoding of events and values (event encoding v1).
//!
//! Byte layout is specified in `docs/storage-format/event-encoding-v1.md`.
//! Integers are little-endian / LEB128 varints (signed values zig-zag).
//!
//! Decoding is defensive: it never panics, bounds nesting depth, and never
//! allocates beyond the input size. Map keys must be strictly increasing,
//! which makes the encoding canonical and rejects duplicate keys.

use std::collections::BTreeMap;

use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{Error, Result, TenantId};

use crate::event::{Event, Severity};
use crate::value::Value;

/// Event encoding version written by this build.
pub const EVENT_ENCODING_V1: u8 = 1;

/// Bounds applied when decoding persisted bytes.
#[derive(Debug, Clone, Copy)]
pub struct DecodeLimits {
    pub max_depth: usize,
    pub max_string_bytes: usize,
    pub max_items: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_depth: 64,
            max_string_bytes: 64 * 1024 * 1024,
            max_items: 1 << 20,
        }
    }
}

mod tag {
    pub const NULL: u8 = 0;
    pub const FALSE: u8 = 1;
    pub const TRUE: u8 = 2;
    pub const INT: u8 = 3;
    pub const UINT: u8 = 4;
    pub const FLOAT: u8 = 5;
    pub const STRING: u8 = 6;
    pub const BYTES: u8 = 7;
    pub const TIMESTAMP: u8 = 8;
    pub const LIST: u8 = 9;
    pub const MAP: u8 = 10;
}

pub fn encode_value(e: &mut Encoder, v: &Value) {
    match v {
        Value::Null => e.put_u8(tag::NULL),
        Value::Bool(false) => e.put_u8(tag::FALSE),
        Value::Bool(true) => e.put_u8(tag::TRUE),
        Value::Int(i) => {
            e.put_u8(tag::INT);
            e.put_ivarint(*i);
        }
        Value::UInt(u) => {
            e.put_u8(tag::UINT);
            e.put_uvarint(*u);
        }
        Value::Float(f) => {
            e.put_u8(tag::FLOAT);
            e.put_f64(*f);
        }
        Value::String(s) => {
            e.put_u8(tag::STRING);
            e.put_str(s);
        }
        Value::Bytes(b) => {
            e.put_u8(tag::BYTES);
            e.put_bytes(b);
        }
        Value::Timestamp(t) => {
            e.put_u8(tag::TIMESTAMP);
            e.put_ivarint(*t);
        }
        Value::List(items) => {
            e.put_u8(tag::LIST);
            e.put_uvarint(items.len() as u64);
            for item in items {
                encode_value(e, item);
            }
        }
        Value::Map(m) => {
            e.put_u8(tag::MAP);
            encode_map(e, m);
        }
    }
}

pub fn encode_map(e: &mut Encoder, m: &BTreeMap<String, Value>) {
    e.put_uvarint(m.len() as u64);
    for (k, v) in m {
        e.put_str(k);
        encode_value(e, v);
    }
}

pub fn decode_value(d: &mut Decoder<'_>, lim: &DecodeLimits) -> Result<Value> {
    decode_value_depth(d, lim, 0)
}

fn decode_value_depth(d: &mut Decoder<'_>, lim: &DecodeLimits, depth: usize) -> Result<Value> {
    if depth > lim.max_depth {
        return Err(Error::corruption(
            "value",
            format!("nesting deeper than {}", lim.max_depth),
        ));
    }
    let t = d.get_u8()?;
    Ok(match t {
        tag::NULL => Value::Null,
        tag::FALSE => Value::Bool(false),
        tag::TRUE => Value::Bool(true),
        tag::INT => Value::Int(d.get_ivarint()?),
        tag::UINT => Value::UInt(d.get_uvarint()?),
        tag::FLOAT => Value::Float(d.get_f64()?),
        tag::STRING => Value::String(d.get_string(lim.max_string_bytes)?),
        tag::BYTES => Value::Bytes(d.get_bytes(lim.max_string_bytes)?.to_vec()),
        tag::TIMESTAMP => Value::Timestamp(d.get_ivarint()?),
        tag::LIST => {
            let n = d.get_len(lim.max_items, 1)?;
            let mut items = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                items.push(decode_value_depth(d, lim, depth + 1)?);
            }
            Value::List(items)
        }
        tag::MAP => Value::Map(decode_map_depth(d, lim, depth + 1)?),
        other => {
            return Err(Error::corruption(
                "value",
                format!("unknown value tag {other}"),
            ))
        }
    })
}

pub fn decode_map(d: &mut Decoder<'_>, lim: &DecodeLimits) -> Result<BTreeMap<String, Value>> {
    decode_map_depth(d, lim, 0)
}

fn decode_map_depth(
    d: &mut Decoder<'_>,
    lim: &DecodeLimits,
    depth: usize,
) -> Result<BTreeMap<String, Value>> {
    // Each entry needs at least 2 bytes (empty key length + value tag).
    let n = d.get_len(lim.max_items, 2)?;
    let mut m = BTreeMap::new();
    let mut prev: Option<String> = None;
    for _ in 0..n {
        let k = d.get_string(lim.max_string_bytes)?;
        if let Some(p) = &prev {
            if k.as_str() <= p.as_str() {
                return Err(Error::corruption(
                    "map",
                    "keys not strictly increasing (duplicate or unsorted key)",
                ));
            }
        }
        let v = decode_value_depth(d, lim, depth)?;
        prev = Some(k.clone());
        m.insert(k, v);
    }
    Ok(m)
}

fn put_opt_str(e: &mut Encoder, s: &Option<String>) {
    match s {
        None => e.put_u8(0),
        Some(s) => {
            e.put_u8(1);
            e.put_str(s);
        }
    }
}

fn get_opt_str(d: &mut Decoder<'_>, lim: &DecodeLimits) -> Result<Option<String>> {
    match d.get_u8()? {
        0 => Ok(None),
        1 => Ok(Some(d.get_string(lim.max_string_bytes)?)),
        other => Err(Error::corruption(
            "event",
            format!("invalid option marker {other}"),
        )),
    }
}

/// Encode an event (encoding v1) and append to `e`.
pub fn encode_event(e: &mut Encoder, ev: &Event) {
    e.put_u8(EVENT_ENCODING_V1);
    e.put_ivarint(ev.event_time);
    e.put_ivarint(ev.ingest_time);
    e.put_str(ev.tenant_id.as_str());
    e.put_u8(ev.severity.as_u8());
    put_opt_str(e, &ev.service);
    put_opt_str(e, &ev.severity_text);
    put_opt_str(e, &ev.trace_id);
    put_opt_str(e, &ev.span_id);
    put_opt_str(e, &ev.event_id);
    match &ev.body {
        None => e.put_u8(0),
        Some(v) => {
            e.put_u8(1);
            encode_value(e, v);
        }
    }
    encode_map(e, &ev.attributes);
    encode_map(e, &ev.resource_attributes);
    e.put_uvarint(u64::from(ev.schema_version));
}

/// Decode one event from `d`.
pub fn decode_event(d: &mut Decoder<'_>, lim: &DecodeLimits) -> Result<Event> {
    let version = d.get_u8()?;
    if version != EVENT_ENCODING_V1 {
        return Err(Error::FormatVersion {
            what: "event encoding",
            found: u32::from(version),
            supported: EVENT_ENCODING_V1.to_string(),
        });
    }
    let event_time = d.get_ivarint()?;
    let ingest_time = d.get_ivarint()?;
    let tenant = d.get_str(stratadb_common::ids::MAX_TENANT_ID_LEN)?;
    let tenant_id = TenantId::new(tenant)
        .map_err(|e| Error::corruption("event", format!("invalid tenant id: {e}")))?;
    let sev = d.get_u8()?;
    let severity = Severity::from_u8(sev)
        .ok_or_else(|| Error::corruption("event", format!("invalid severity {sev}")))?;
    let service = get_opt_str(d, lim)?;
    let severity_text = get_opt_str(d, lim)?;
    let trace_id = get_opt_str(d, lim)?;
    let span_id = get_opt_str(d, lim)?;
    let event_id = get_opt_str(d, lim)?;
    let body = match d.get_u8()? {
        0 => None,
        1 => Some(decode_value(d, lim)?),
        other => {
            return Err(Error::corruption(
                "event",
                format!("invalid body marker {other}"),
            ))
        }
    };
    let attributes = decode_map(d, lim)?;
    let resource_attributes = decode_map(d, lim)?;
    let schema_version = u32::try_from(d.get_uvarint()?)
        .map_err(|_| Error::corruption("event", "schema_version overflows u32"))?;
    Ok(Event {
        event_time,
        ingest_time,
        tenant_id,
        service,
        severity,
        severity_text,
        body,
        trace_id,
        span_id,
        event_id,
        attributes,
        resource_attributes,
        schema_version,
    })
}

/// Encode a batch of events as a length-prefixed sequence (WAL payload body).
pub fn encode_batch(events: &[Event]) -> Vec<u8> {
    let mut e = Encoder::with_capacity(events.len() * 256);
    e.put_uvarint(events.len() as u64);
    let mut scratch = Vec::new();
    for ev in events {
        scratch.clear();
        let mut s = Encoder::from_vec(std::mem::take(&mut scratch));
        encode_event(&mut s, ev);
        e.put_bytes(s.as_slice());
        scratch = s.into_inner();
    }
    e.into_inner()
}

/// Decode a batch encoded by [`encode_batch`]; the input must be fully consumed.
pub fn decode_batch(bytes: &[u8], lim: &DecodeLimits) -> Result<Vec<Event>> {
    let mut d = Decoder::new(bytes, "event batch");
    let n = d.get_len(lim.max_items, 1)?;
    let mut out = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        let rec = d.get_bytes(bytes.len())?;
        let mut rd = Decoder::new(rec, "event");
        out.push(decode_event(&mut rd, lim)?);
        rd.expect_end()?;
    }
    d.expect_end()?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            any::<u64>().prop_map(Value::UInt),
            (-1e300f64..1e300).prop_map(Value::Float),
            ".{0,16}".prop_map(Value::String),
            proptest::collection::vec(any::<u8>(), 0..16).prop_map(Value::Bytes),
            any::<i64>().prop_map(Value::Timestamp),
        ];
        leaf.prop_recursive(3, 32, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::List),
                proptest::collection::btree_map(".{0,8}", inner, 0..4).prop_map(Value::Map),
            ]
        })
    }

    prop_compose! {
        fn arb_event()(
            event_time in any::<i64>(),
            ingest_time in any::<i64>(),
            sev in prop::sample::select(vec![0u8, 1, 5, 9, 13, 17, 21]),
            service in proptest::option::of("[a-z-]{0,12}"),
            severity_text in proptest::option::of(".{0,8}"),
            body in proptest::option::of(arb_value()),
            trace_id in proptest::option::of("[0-9a-f]{32}"),
            event_id in proptest::option::of(".{0,10}"),
            attributes in proptest::collection::btree_map(".{0,10}", arb_value(), 0..6),
            resource_attributes in proptest::collection::btree_map(".{0,10}", arb_value(), 0..3),
        ) -> Event {
            Event {
                event_time, ingest_time,
                tenant_id: TenantId::new("t1").unwrap(),
                service, severity: Severity::from_u8(sev).unwrap(), severity_text, body,
                trace_id, span_id: None, event_id, attributes, resource_attributes,
                schema_version: 1,
            }
        }
    }

    proptest! {
        #[test]
        fn event_roundtrip(ev in arb_event()) {
            let mut e = Encoder::new();
            encode_event(&mut e, &ev);
            let mut d = Decoder::new(e.as_slice(), "event");
            let back = decode_event(&mut d, &DecodeLimits::default()).unwrap();
            d.expect_end().unwrap();
            prop_assert_eq!(back, ev);
        }

        #[test]
        fn batch_roundtrip(evs in proptest::collection::vec(arb_event(), 0..8)) {
            let bytes = encode_batch(&evs);
            prop_assert_eq!(decode_batch(&bytes, &DecodeLimits::default()).unwrap(), evs);
        }

        #[test]
        fn decode_garbage_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = decode_batch(&bytes, &DecodeLimits::default());
            let mut d = Decoder::new(&bytes, "fuzz");
            let _ = decode_event(&mut d, &DecodeLimits::default());
        }

        #[test]
        fn truncation_always_detected(ev in arb_event(), cut in 0usize..1000) {
            let bytes = encode_batch(std::slice::from_ref(&ev));
            let cut = cut % bytes.len();
            prop_assert!(decode_batch(&bytes[..cut], &DecodeLimits::default()).is_err());
        }
    }

    #[test]
    fn deep_nesting_rejected() {
        let mut v = Value::Null;
        for _ in 0..100 {
            v = Value::List(vec![v]);
        }
        let mut e = Encoder::new();
        encode_value(&mut e, &v);
        let mut d = Decoder::new(e.as_slice(), "v");
        assert!(decode_value(&mut d, &DecodeLimits::default()).is_err());
    }

    #[test]
    fn duplicate_map_keys_rejected() {
        let mut e = Encoder::new();
        e.put_uvarint(2);
        e.put_str("a");
        e.put_u8(0);
        e.put_str("a");
        e.put_u8(0);
        let mut d = Decoder::new(e.as_slice(), "m");
        assert!(decode_map(&mut d, &DecodeLimits::default()).is_err());
    }

    #[test]
    fn unknown_version_rejected() {
        let mut d = Decoder::new(&[99u8], "event");
        let err = decode_event(&mut d, &DecodeLimits::default()).unwrap_err();
        assert_eq!(err.kind(), stratadb_common::ErrorKind::Unsupported);
    }
}
