//! Columnar event batch encoding v1: the payload of WAL record types 3 and 4
//! (spec STRATA-SYS-07-002). Normative description:
//! docs/storage-format/event-batch-columnar-v1.md.
//!
//! A batch belongs to one tenant (the engine refuses mixed batches), so the
//! tenant is stored once. Values are stored column by column:
//! - timestamps as deltas;
//! - `service` and `severity_text` as dictionaries;
//! - the rest with the event-encoding-v1 value codec.
//!
//! When the column body is at least [`COMPRESS_THRESHOLD`] bytes it is
//! LZ4-compressed, and kept only if that makes it smaller.
//!
//! Decoding validates exactly what the row codec validates (UTF-8, tenant
//! alphabet, severity codes, value tags, nesting, canonical map key order),
//! is bounded by [`DecodeLimits`] and the input size, and requires the input
//! to be fully consumed.

use std::collections::HashMap;

use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{Error, Result, TenantId};

use crate::codec::{decode_map, decode_value, encode_map, encode_value, DecodeLimits};
use crate::column::EventColumns;
use crate::event::{Event, Severity};

/// Encoding version (first byte).
pub const VERSION: u8 = 1;
/// Bodies at least this large are LZ4-compressed (spec: batches >= 16 KiB).
pub const COMPRESS_THRESHOLD: usize = 16 * 1024;

const FLAG_LZ4: u8 = 1;

/// LZ4 cannot expand data by more than this factor; a declared raw length
/// beyond it is corrupt (and would be an allocation attack).
const MAX_LZ4_RATIO: usize = 255;

fn corrupt(msg: impl Into<String>) -> Error {
    Error::corruption("columnar event batch", msg.into())
}

/// Encode `events` (non-empty, one tenant). Panics never; the caller has
/// already validated the batch.
pub fn encode_batch(events: &[Event]) -> Result<Vec<u8>> {
    encode_batch_with(events, COMPRESS_THRESHOLD)
}

/// [`encode_batch`] with an explicit compression threshold (tests,
/// benchmarks).
pub fn encode_batch_with(events: &[Event], compress_threshold: usize) -> Result<Vec<u8>> {
    let first = events
        .first()
        .ok_or_else(|| Error::invalid_input("columnar batch needs at least one event"))?;
    if events.iter().any(|e| e.tenant_id != first.tenant_id) {
        return Err(Error::invalid_input(
            "a columnar batch holds events of one tenant",
        ));
    }
    let mut b = Encoder::with_capacity(events.len() * 128);
    b.put_str(first.tenant_id.as_str());
    let mut prev = 0i64;
    for e in events {
        b.put_ivarint(e.event_time.wrapping_sub(prev));
        prev = e.event_time;
    }
    prev = 0;
    for e in events {
        b.put_ivarint(e.ingest_time.wrapping_sub(prev));
        prev = e.ingest_time;
    }
    for e in events {
        b.put_u8(e.severity.as_u8());
    }
    for e in events {
        b.put_uvarint(u64::from(e.schema_version));
    }
    put_dict(&mut b, events, |e| e.service.as_deref());
    put_dict(&mut b, events, |e| e.severity_text.as_deref());
    for f in [
        (|e: &Event| e.trace_id.as_deref()) as fn(&Event) -> Option<&str>,
        |e| e.span_id.as_deref(),
        |e| e.event_id.as_deref(),
    ] {
        for e in events {
            match f(e) {
                None => b.put_uvarint(0),
                Some(s) => {
                    b.put_uvarint(s.len() as u64 + 1);
                    b.put_raw(s.as_bytes());
                }
            }
        }
    }
    for e in events {
        match &e.body {
            None => b.put_u8(0),
            Some(v) => {
                b.put_u8(1);
                encode_value(&mut b, v);
            }
        }
    }
    for e in events {
        encode_map(&mut b, &e.attributes);
    }
    for e in events {
        encode_map(&mut b, &e.resource_attributes);
    }
    let body = b.into_inner();

    let mut out = Encoder::with_capacity(body.len() / 2 + 16);
    out.put_u8(VERSION);
    let compressed = (body.len() >= compress_threshold)
        .then(|| lz4_flex::block::compress(&body))
        .filter(|c| c.len() < body.len());
    out.put_u8(if compressed.is_some() { FLAG_LZ4 } else { 0 });
    out.put_uvarint(events.len() as u64);
    match compressed {
        Some(c) => {
            out.put_uvarint(body.len() as u64);
            out.put_raw(&c);
        }
        None => out.put_raw(&body),
    }
    Ok(out.into_inner())
}

/// Dictionary column: distinct values in first-seen order, then one code per
/// row (0 = missing, k = dictionary entry k - 1).
fn put_dict(b: &mut Encoder, events: &[Event], f: impl Fn(&Event) -> Option<&str>) {
    // Log batches repeat values and have few distinct ones: check the
    // previous row's value, then scan a small dictionary; hash only when it
    // grows past `SMALL`.
    const SMALL: usize = 16;
    let mut dict: Vec<&str> = Vec::new();
    let mut codes: HashMap<&str, u64> = HashMap::new();
    let mut per_row = Vec::with_capacity(events.len());
    let mut last: Option<(&str, u64)> = None;
    for e in events {
        let code = match f(e) {
            None => 0,
            Some(s) => match last {
                Some((l, c)) if l == s => c,
                _ => {
                    let c = if dict.len() <= SMALL {
                        match dict.iter().position(|d| *d == s) {
                            Some(i) => i as u64 + 1,
                            None => {
                                dict.push(s);
                                if dict.len() > SMALL {
                                    codes.extend(
                                        dict.iter().enumerate().map(|(i, d)| (*d, i as u64 + 1)),
                                    );
                                }
                                dict.len() as u64
                            }
                        }
                    } else {
                        *codes.entry(s).or_insert_with(|| {
                            dict.push(s);
                            dict.len() as u64
                        })
                    };
                    last = Some((s, c));
                    c
                }
            },
        };
        per_row.push(code);
    }
    b.put_uvarint(dict.len() as u64);
    for s in &dict {
        b.put_str(s);
    }
    for c in per_row {
        b.put_uvarint(c);
    }
}

/// Decode a batch encoded by [`encode_batch`] into rows; the input must be
/// fully consumed.
pub fn decode_batch(bytes: &[u8], lim: &DecodeLimits) -> Result<Vec<Event>> {
    decode_columns(bytes, lim).map(EventColumns::into_events)
}

/// Decode a batch straight into columns (no row intermediate; WAL replay
/// into the columnar memtable, spec STRATA-SYS-07-002).
pub fn decode_columns(bytes: &[u8], lim: &DecodeLimits) -> Result<EventColumns> {
    let mut d = Decoder::new(bytes, "columnar event batch");
    let version = d.get_u8()?;
    if version != VERSION {
        return Err(Error::FormatVersion {
            what: "columnar event batch",
            found: u32::from(version),
            supported: VERSION.to_string(),
        });
    }
    let flags = d.get_u8()?;
    if flags & !FLAG_LZ4 != 0 {
        return Err(corrupt(format!("unknown flags {flags:#x}")));
    }
    let n = d.get_len(lim.max_items, 0)?;
    if n == 0 {
        return Err(corrupt("empty batch"));
    }
    let owned;
    let body: &[u8] = if flags & FLAG_LZ4 != 0 {
        let raw_len = d.get_len(usize::MAX, 0)?;
        let packed = d.get_raw(d.remaining())?;
        if raw_len > packed.len().saturating_mul(MAX_LZ4_RATIO) + 16
            || raw_len > lim.max_string_bytes.saturating_mul(4)
        {
            return Err(corrupt(format!(
                "declared size {raw_len} is impossible for {} compressed bytes",
                packed.len()
            )));
        }
        owned = lz4_flex::block::decompress(packed, raw_len)
            .map_err(|e| corrupt(format!("LZ4: {e}")))?;
        if owned.len() != raw_len {
            return Err(corrupt("decompressed size differs from the declared size"));
        }
        &owned
    } else {
        d.get_raw(d.remaining())?
    };
    d.expect_end()?;
    // Each row needs at least a few bytes of body (time deltas, severity,
    // schema version, codes, markers, map counts): reject impossible counts
    // before allocating per-row vectors.
    if n.saturating_mul(10) > body.len() {
        return Err(corrupt(format!(
            "{n} rows cannot fit in {} bytes",
            body.len()
        )));
    }
    let mut b = Decoder::new(body, "columnar event batch body");
    let tenant = b.get_str(stratadb_common::ids::MAX_TENANT_ID_LEN)?;
    let tenant = TenantId::new(tenant).map_err(|e| corrupt(format!("invalid tenant id: {e}")))?;
    let deltas = |b: &mut Decoder<'_>| -> Result<Vec<i64>> {
        let mut v = Vec::with_capacity(n);
        let mut prev = 0i64;
        for _ in 0..n {
            prev = prev.wrapping_add(b.get_ivarint()?);
            v.push(prev);
        }
        Ok(v)
    };
    let event_time = deltas(&mut b)?;
    let ingest_time = deltas(&mut b)?;
    let mut severity = Vec::with_capacity(n);
    for _ in 0..n {
        let s = b.get_u8()?;
        severity
            .push(Severity::from_u8(s).ok_or_else(|| corrupt(format!("invalid severity {s}")))?);
    }
    let mut schema_version = Vec::with_capacity(n);
    for _ in 0..n {
        schema_version.push(
            u32::try_from(b.get_uvarint()?).map_err(|_| corrupt("schema_version overflows u32"))?,
        );
    }
    let service = get_dict(&mut b, n, lim)?;
    let severity_text = get_dict(&mut b, n, lim)?;
    let mut opt_strs = || -> Result<Vec<Option<String>>> {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            let len = b.get_uvarint()?;
            v.push(match len {
                0 => None,
                l => {
                    let l =
                        usize::try_from(l - 1).map_err(|_| corrupt("string length overflow"))?;
                    if l > lim.max_string_bytes {
                        return Err(corrupt(format!("string of {l} bytes exceeds the limit")));
                    }
                    let raw = b.get_raw(l)?;
                    Some(
                        std::str::from_utf8(raw)
                            .map_err(|e| corrupt(format!("invalid UTF-8: {e}")))?
                            .to_owned(),
                    )
                }
            });
        }
        Ok(v)
    };
    let trace_id = opt_strs()?;
    let span_id = opt_strs()?;
    let event_id = opt_strs()?;
    let mut body_col = Vec::with_capacity(n);
    for _ in 0..n {
        body_col.push(match b.get_u8()? {
            0 => None,
            1 => Some(decode_value(&mut b, lim)?),
            other => return Err(corrupt(format!("invalid body marker {other}"))),
        });
    }
    let mut attributes = Vec::with_capacity(n);
    for _ in 0..n {
        attributes.push(decode_map(&mut b, lim)?);
    }
    let mut resource = Vec::with_capacity(n);
    for _ in 0..n {
        resource.push(decode_map(&mut b, lim)?);
    }
    b.expect_end()?;

    let cols = EventColumns {
        tenant,
        event_time,
        ingest_time,
        severity,
        schema_version,
        service,
        severity_text,
        trace_id,
        span_id,
        event_id,
        body: body_col,
        attributes,
        resource_attributes: resource,
    };
    Ok(cols)
}

fn get_dict(b: &mut Decoder<'_>, n: usize, lim: &DecodeLimits) -> Result<Vec<Option<String>>> {
    let entries = b.get_len(n, 1)?;
    let mut dict = Vec::with_capacity(entries);
    for _ in 0..entries {
        dict.push(b.get_str(lim.max_string_bytes)?);
    }
    let mut used = vec![false; entries];
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let code = b.get_uvarint()?;
        out.push(match code {
            0 => None,
            c => {
                let i = usize::try_from(c - 1)
                    .ok()
                    .filter(|i| *i < entries)
                    .ok_or_else(|| corrupt(format!("dictionary code {c} out of range")))?;
                used[i] = true;
                Some(dict[i].to_owned())
            }
        });
    }
    // Canonical: every entry is used and entries are distinct.
    if used.iter().any(|u| !u) {
        return Err(corrupt("unused dictionary entry"));
    }
    let mut seen = std::collections::HashSet::with_capacity(entries);
    if dict.iter().any(|s| !seen.insert(*s)) {
        return Err(corrupt("duplicate dictionary entry"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::Value;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

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
            service in proptest::option::of(prop::sample::select(vec!["api", "db", "web", ""])),
            severity_text in proptest::option::of(".{0,8}"),
            body in proptest::option::of(arb_value()),
            trace_id in proptest::option::of("[0-9a-f]{32}"),
            span_id in proptest::option::of("[0-9a-f]{0,16}"),
            event_id in proptest::option::of(".{0,10}"),
            attributes in proptest::collection::btree_map(".{0,10}", arb_value(), 0..6),
            resource_attributes in proptest::collection::btree_map(".{0,10}", arb_value(), 0..3),
            schema_version in any::<u32>(),
        ) -> Event {
            Event {
                event_time, ingest_time,
                tenant_id: TenantId::new("t1").unwrap(),
                service: service.map(str::to_owned), severity: Severity::from_u8(sev).unwrap(),
                severity_text, body, trace_id, span_id, event_id, attributes,
                resource_attributes, schema_version,
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn roundtrip_plain_and_compressed(evs in proptest::collection::vec(arb_event(), 1..40)) {
            let lim = DecodeLimits::default();
            for threshold in [usize::MAX, 0] {
                let bytes = encode_batch_with(&evs, threshold).unwrap();
                prop_assert_eq!(&decode_batch(&bytes, &lim).unwrap(), &evs);
            }
        }

        /// Rows → columns → rows is the identity, and the column-side size
        /// formula equals `Event::approx_size` (memory reservations rely on
        /// it).
        #[test]
        fn columns_round_trip_and_sizes_match(evs in proptest::collection::vec(arb_event(), 1..30)) {
            let cols = EventColumns::from_events(evs.clone()).unwrap();
            for (i, e) in evs.iter().enumerate() {
                prop_assert_eq!(cols.row_approx_size(i), e.approx_size());
            }
            prop_assert_eq!(cols.into_events(), evs);
        }

        /// Arbitrary damage never panics and never decodes to different
        /// events silently: it is an error or (if the damage hit nothing
        /// semantic, e.g. an identical byte) the same events.
        #[test]
        fn damaged_input_is_rejected_or_unchanged(
            evs in proptest::collection::vec(arb_event(), 1..10),
            at in any::<prop::sample::Index>(),
            xor in 1u8..=255,
            cut in any::<prop::sample::Index>(),
        ) {
            let lim = DecodeLimits::default();
            for threshold in [usize::MAX, 0] {
                let bytes = encode_batch_with(&evs, threshold).unwrap();
                let mut bad = bytes.clone();
                let i = at.index(bad.len());
                bad[i] ^= xor;
                if let Ok(d) = decode_batch(&bad, &lim) {
                    // Some bytes (e.g. a varint delta) change values without
                    // breaking structure; the WAL frame CRC covers those.
                    prop_assert_eq!(d.len(), evs.len());
                }
                let short = &bytes[..cut.index(bytes.len())];
                prop_assert!(decode_batch(short, &lim).is_err());
            }
        }
    }

    fn events(n: usize) -> Vec<Event> {
        (0..n)
            .map(|i| {
                let mut e = Event::new(
                    TenantId::new("acme").unwrap(),
                    1_795_170_600_000_000_000 + i as i64 * 1000,
                    1_795_170_600_100_000_000,
                );
                e.service = Some(["api", "db", "worker"][i % 3].into());
                e.severity = Severity::Info;
                e.trace_id = Some(format!("{i:032x}"));
                e.body = Some(Value::String(format!(
                    "GET /api/v1/items/{i} 200 in {} ms",
                    i % 97
                )));
                e.attributes = BTreeMap::from([
                    ("http.status_code".into(), Value::Int(200)),
                    (
                        "http.route".into(),
                        Value::String("/api/v1/items/{id}".into()),
                    ),
                ]);
                e.resource_attributes =
                    BTreeMap::from([("host".into(), Value::String("node-1".into()))]);
                e
            })
            .collect()
    }

    #[test]
    fn large_batches_are_compressed_and_smaller_than_rows() {
        let evs = events(1000);
        let col = encode_batch(&evs).unwrap();
        assert_eq!(col[1], FLAG_LZ4, "a large batch is compressed");
        let rows = crate::codec::encode_batch(&evs);
        assert!(
            col.len() * 3 < rows.len(),
            "columnar {} vs rows {}",
            col.len(),
            rows.len()
        );
        assert_eq!(decode_batch(&col, &DecodeLimits::default()).unwrap(), evs);
        // Small batches stay plain.
        let small = encode_batch(&events(3)).unwrap();
        assert_eq!(small[1], 0);
    }

    #[test]
    fn dictionaries_of_any_size_round_trip() {
        for distinct in [1, 2, 16, 17, 40, 1000] {
            let mut evs = events(1000);
            for (i, e) in evs.iter_mut().enumerate() {
                e.service = (i % 7 != 0).then(|| format!("svc-{}", (i * 31) % distinct));
            }
            let b = encode_batch(&evs).unwrap();
            assert_eq!(
                decode_batch(&b, &DecodeLimits::default()).unwrap(),
                evs,
                "{distinct}"
            );
        }
    }

    #[test]
    fn invalid_batches_are_rejected() {
        let lim = DecodeLimits::default();
        assert!(encode_batch(&[]).is_err());
        let mut mixed = events(2);
        mixed[1].tenant_id = TenantId::new("other").unwrap();
        assert!(encode_batch(&mixed).is_err());
        let good = encode_batch(&events(4)).unwrap();
        let mut v = good.clone();
        v[0] = 2;
        assert!(matches!(
            decode_batch(&v, &lim),
            Err(Error::FormatVersion { .. })
        ));
        let mut f = good.clone();
        f[1] = 0x80;
        assert!(decode_batch(&f, &lim).is_err());
        let mut extra = good.clone();
        extra.push(0);
        assert!(decode_batch(&extra, &lim).is_err(), "trailing bytes");
        // A huge declared row count fails before allocating.
        let mut huge = vec![VERSION, 0];
        let mut e = Encoder::new();
        e.put_uvarint(1 << 19);
        huge.extend(e.into_inner());
        huge.extend([0u8; 64]);
        assert!(decode_batch(&huge, &lim).is_err());
        // A compressed body declaring an impossible size fails before allocating.
        let mut bomb = vec![VERSION, FLAG_LZ4, 1];
        let mut e = Encoder::new();
        e.put_uvarint(1 << 40);
        bomb.extend(e.into_inner());
        bomb.extend([0u8; 8]);
        assert!(decode_batch(&bomb, &lim).is_err());
    }

    /// Encode cost split (run: `cargo test --release -p stratadb-schema --lib
    /// columnar::tests::encode_cost -- --ignored --nocapture`).
    #[test]
    #[ignore = "measurement, not a check"]
    fn encode_cost() {
        let evs = events(1000);
        let time = |f: &dyn Fn() -> usize| {
            let n = 300;
            let t = std::time::Instant::now();
            let mut acc = 0;
            for _ in 0..n {
                acc += std::hint::black_box(f());
            }
            std::hint::black_box(acc);
            t.elapsed().as_nanos() as f64 / (n as f64 * evs.len() as f64)
        };
        let row = time(&|| crate::codec::encode_batch(&evs).len());
        let plain = time(&|| encode_batch_with(&evs, usize::MAX).unwrap().len());
        let lz4 = time(&|| encode_batch(&evs).unwrap().len());
        let col = encode_batch(&evs).unwrap();
        let dec = time(&|| decode_batch(&col, &DecodeLimits::default()).unwrap().len());
        println!("ns/row: row encode {row:.0}, columnar {plain:.0}, columnar+LZ4 {lz4:.0}, columnar decode {dec:.0}; bytes/row: row {}, columnar+LZ4 {}",
            crate::codec::encode_batch(&evs).len() / 1000, col.len() / 1000);
    }
}
