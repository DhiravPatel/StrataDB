use super::*;
use proptest::prelude::*;
use stratadb_common::ErrorKind;
use stratadb_schema::Value;

fn tenant() -> TenantId {
    TenantId::new("tenant-a").unwrap()
}

fn event(i: i64) -> Event {
    let mut e = Event::new(tenant(), 1_000_000 + (i * 7919) % 1000, 2_000_000 + i);
    e.service = Some(["api", "db", "web"][(i % 3) as usize].to_owned());
    e.severity = if i % 5 == 0 {
        Severity::Error
    } else {
        Severity::Info
    };
    e.body = (i % 4 != 0).then(|| Value::String(format!("message {i}")));
    e.trace_id = (i % 2 == 0).then(|| format!("{i:032x}"));
    e.attributes.insert("n".into(), Value::Int(i));
    if i % 3 == 0 {
        e.attributes.insert("maybe".into(), Value::Null);
    }
    e.resource_attributes
        .insert("host".into(), Value::from("h1"));
    e
}

fn write(dir: &Path, events: &[Event]) -> WrittenSegment {
    let mut rows: Vec<RowRef<'_>> = events
        .iter()
        .enumerate()
        .map(|(i, e)| RowRef {
            seq: 10 + i as u64 / 3,
            idx: (i % 3) as u32,
            event: e,
        })
        .collect();
    write_segment(
        &dir.join(segment_file_name(1)),
        1,
        &tenant(),
        &mut rows,
        123,
    )
    .unwrap()
}

#[test]
fn roundtrip_sorted_with_stats() {
    let dir = tempfile::tempdir().unwrap();
    let events: Vec<Event> = (0..100).map(event).collect();
    let w = write(dir.path(), &events);
    let r = SegmentReader::open(&w.path).unwrap();
    r.verify().unwrap();
    let f = r.footer();
    assert_eq!(f.row_count, 100);
    assert_eq!(f.tenant_id, tenant());
    assert_eq!(f.min_seq, 10);
    assert_eq!(f.max_seq, 10 + 99 / 3);
    assert!(f.attribute_keys.may_contain("n"));
    assert!(!f.attribute_keys.may_contain("absent"));
    assert_eq!(f.severity_counts[&Severity::Error.as_u8()], 20);
    assert_eq!(f.column(ColumnId::Body).unwrap().missing, 25);
    let rows = r.read_rows().unwrap();
    assert!(rows
        .windows(2)
        .all(|w| w[0].event.event_time <= w[1].event.event_time));
    let mut got: Vec<Event> = rows.into_iter().map(|r| r.event).collect();
    let mut want = events.clone();
    let key = |e: &Event| (e.event_time, e.ingest_time);
    got.sort_by_key(key);
    want.sort_by_key(key);
    assert_eq!(got, want);
    assert!(!fsutil::tmp_path(&w.path).exists());
}

#[test]
fn column_projection_reads_only_one_chunk() {
    let dir = tempfile::tempdir().unwrap();
    let events: Vec<Event> = (0..10).map(event).collect();
    let w = write(dir.path(), &events);
    let r = SegmentReader::open(&w.path).unwrap();
    let svc = r
        .read_column(ColumnId::Service)
        .unwrap()
        .into_strs()
        .unwrap();
    assert_eq!(svc.len(), 10);
    assert!(svc.iter().all(Option::is_some));
}

#[test]
fn every_byte_flip_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let events: Vec<Event> = (0..6).map(event).collect();
    let w = write(dir.path(), &events);
    let orig = std::fs::read(&w.path).unwrap();
    let p = dir.path().join("flipped.sseg");
    for i in 0..orig.len() {
        let mut b = orig.clone();
        b[i] ^= 0x01;
        std::fs::write(&p, &b).unwrap();
        let detected = match SegmentReader::open(&p) {
            Err(_) => true,
            Ok(r) => r.verify().is_err(),
        };
        assert!(detected, "flip at byte {i} of {} not detected", orig.len());
    }
}

#[test]
fn truncation_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let events: Vec<Event> = (0..6).map(event).collect();
    let w = write(dir.path(), &events);
    let orig = std::fs::read(&w.path).unwrap();
    let p = dir.path().join("cut.sseg");
    for cut in 0..orig.len() {
        std::fs::write(&p, &orig[..cut]).unwrap();
        let err = SegmentReader::open(&p)
            .and_then(|r| r.verify())
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Corruption, "cut {cut}: {err}");
    }
}

#[test]
fn rejects_cross_tenant_rows_and_empty_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut other = event(1);
    other.tenant_id = TenantId::new("tenant-b").unwrap();
    let mine = event(2);
    let mut rows = vec![
        RowRef {
            seq: 1,
            idx: 0,
            event: &mine,
        },
        RowRef {
            seq: 1,
            idx: 1,
            event: &other,
        },
    ];
    let p = dir.path().join(segment_file_name(9));
    assert!(write_segment(&p, 9, &tenant(), &mut rows, 0).is_err());
    assert!(write_segment(&p, 9, &tenant(), &mut [], 0).is_err());
    assert!(!p.exists());
}

#[test]
fn extensions_roundtrip_and_unknown_required_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let ev = event(1);
    let mut rows = vec![RowRef {
        seq: 1,
        idx: 0,
        event: &ev,
    }];
    let p = dir.path().join("x.sseg");
    let ext = Extension {
        tag: 7,
        required: false,
        data: vec![1, 2, 3],
    };
    write_segment_with_extensions(&p, 1, &tenant(), &mut rows, 0, vec![ext.clone()]).unwrap();
    let r = SegmentReader::open(&p).unwrap();
    assert_eq!(r.footer().extension(7), Some(&ext));
    r.verify().unwrap();
    let req = Extension {
        tag: 999,
        required: true,
        data: vec![],
    };
    write_segment_with_extensions(&p, 1, &tenant(), &mut rows, 0, vec![req]).unwrap();
    assert_eq!(
        SegmentReader::open(&p).unwrap_err().kind(),
        ErrorKind::Unsupported
    );
}

#[test]
fn file_names() {
    assert_eq!(parse_segment_file_name(&segment_file_name(42)), Some(42));
    assert_eq!(parse_segment_file_name("seg-42.sseg"), None);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn arbitrary_events_roundtrip(
        specs in proptest::collection::vec(
            (any::<i64>(), proptest::option::of("[a-z]{0,6}"), proptest::option::of(".{0,12}"),
             proptest::collection::btree_map("[a-z.]{1,6}", any::<i64>().prop_map(Value::Int), 0..4)),
            1..40)
    ) {
        let dir = tempfile::tempdir().unwrap();
        let events: Vec<Event> = specs.into_iter().map(|(t, svc, body, attrs)| {
            let mut e = Event::new(tenant(), t, 0);
            e.service = svc;
            e.body = body.map(Value::String);
            e.attributes = attrs;
            e
        }).collect();
        let mut rows: Vec<RowRef<'_>> = events.iter().enumerate()
            .map(|(i, e)| RowRef { seq: i as u64 + 1, idx: 0, event: e }).collect();
        let w = write_segment(&dir.path().join("s.sseg"), 1, &tenant(), &mut rows, 0).unwrap();
        let r = SegmentReader::open(&w.path).unwrap();
        r.verify().unwrap();
        let mut back: Vec<(u64, Event)> = r.read_rows().unwrap().into_iter().map(|r| (r.seq, r.event)).collect();
        back.sort_by_key(|(s, _)| *s);
        let want: Vec<(u64, Event)> = events.into_iter().enumerate().map(|(i, e)| (i as u64 + 1, e)).collect();
        prop_assert_eq!(back, want);
    }
}

#[test]
fn v2_compression_roundtrip_and_fallback() {
    let dir = tempfile::tempdir().unwrap();
    let events: Vec<Event> = (0..500).map(event).collect();
    let mut rows: Vec<RowRef<'_>> = events
        .iter()
        .enumerate()
        .map(|(i, e)| RowRef {
            seq: i as u64 + 1,
            idx: 0,
            event: e,
        })
        .collect();
    let on = write_segment_opts(
        &dir.path().join("on.sseg"),
        1,
        &tenant(),
        &mut rows,
        0,
        vec![],
        &WriteOptions {
            compress: true,
            shred: true,
        },
    )
    .unwrap();
    let off = write_segment_opts(
        &dir.path().join("off.sseg"),
        2,
        &tenant(),
        &mut rows,
        0,
        vec![],
        &WriteOptions {
            compress: false,
            shred: true,
        },
    )
    .unwrap();
    assert!(
        on.file_bytes < off.file_bytes,
        "{} vs {}",
        on.file_bytes,
        off.file_bytes
    );
    assert!(on
        .footer
        .columns
        .iter()
        .any(|c| c.compression == compression::LZ4));
    // Tiny / incompressible chunks fall back to uncompressed.
    let one = [event(1)];
    let mut r1 = vec![RowRef {
        seq: 1,
        idx: 0,
        event: &one[0],
    }];
    let tiny = write_segment_opts(
        &dir.path().join("tiny.sseg"),
        3,
        &tenant(),
        &mut r1,
        0,
        vec![],
        &WriteOptions {
            compress: true,
            shred: true,
        },
    )
    .unwrap();
    assert!(
        tiny.footer
            .columns
            .iter()
            .all(|c| c.compression == compression::NONE),
        "{:?}",
        tiny.footer.columns
    );
    SegmentReader::open(&tiny.path).unwrap().verify().unwrap();
    assert!(off
        .footer
        .columns
        .iter()
        .all(|c| c.compression == compression::NONE && c.raw_len == c.len));
    let a = SegmentReader::open(&on.path).unwrap();
    let b = SegmentReader::open(&off.path).unwrap();
    a.verify().unwrap();
    assert_eq!(a.read_rows().unwrap(), b.read_rows().unwrap());
    // Every byte flip in a compressed segment is still detected.
    let orig = std::fs::read(&on.path).unwrap();
    let p = dir.path().join("flip.sseg");
    for i in (0..orig.len()).step_by(7) {
        let mut x = orig.clone();
        x[i] ^= 0x10;
        std::fs::write(&p, &x).unwrap();
        let detected = match SegmentReader::open(&p) {
            Err(_) => true,
            Ok(r) => r.verify().is_err(),
        };
        assert!(detected, "flip at {i} undetected");
    }
}

#[test]
fn v3_shredding_preserves_rows_null_vs_missing_and_detects_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let mut events: Vec<Event> = (0..200).map(event).collect();
    events[7].attributes.insert("rare".into(), Value::Int(1)); // 0.5 % of rows: not shredded
    let mut rows: Vec<RowRef<'_>> = events
        .iter()
        .enumerate()
        .map(|(i, e)| RowRef {
            seq: i as u64 + 1,
            idx: 0,
            event: e,
        })
        .collect();
    let on = write_segment_opts(
        &dir.path().join("on.sseg"),
        1,
        &tenant(),
        &mut rows,
        0,
        vec![],
        &WriteOptions::default(),
    )
    .unwrap();
    let off = write_segment_opts(
        &dir.path().join("off.sseg"),
        2,
        &tenant(),
        &mut rows,
        0,
        vec![],
        &WriteOptions {
            shred: false,
            compress: true,
        },
    )
    .unwrap();
    let keys: Vec<(Scope, &str)> = on
        .footer
        .shredded
        .iter()
        .map(|c| (c.scope, c.key.as_str()))
        .collect();
    assert!(keys.contains(&(Scope::Attributes, "n")), "{keys:?}");
    assert!(keys.contains(&(Scope::Attributes, "maybe")), "{keys:?}"); // 1/3 of rows
    assert!(keys.contains(&(Scope::Resource, "host")), "{keys:?}");
    assert!(!keys.iter().any(|(_, k)| *k == "rare"));
    assert!(off.footer.shredded.is_empty());
    let a = SegmentReader::open(&on.path).unwrap();
    let b = SegmentReader::open(&off.path).unwrap();
    a.verify().unwrap();
    assert_eq!(a.read_rows().unwrap(), b.read_rows().unwrap());
    assert_eq!(
        a.read_column(ColumnId::Attributes).unwrap(),
        b.read_column(ColumnId::Attributes).unwrap()
    );
    // Shredded values keep null (present) distinct from missing.
    let maybe = a
        .read_shredded(Scope::Attributes, "maybe")
        .unwrap()
        .unwrap();
    let rows_back = a.read_rows().unwrap();
    for (r, v) in rows_back.iter().zip(&maybe) {
        assert_eq!(r.event.attributes.get("maybe").cloned(), v.clone());
    }
    assert!(maybe.contains(&Some(Value::Null)) && maybe.iter().any(Option::is_none));
    assert!(a
        .read_shredded(Scope::Attributes, "rare")
        .unwrap()
        .is_none());
    // The residual map really excludes shredded keys (checked via the raw encoder).
    assert!(
        on.footer.column(ColumnId::Attributes).unwrap().raw_len
            < off.footer.column(ColumnId::Attributes).unwrap().raw_len
    );
    // Corrupting a shredded chunk is detected.
    let c = &on.footer.shredded[0];
    let mut bytes = std::fs::read(&on.path).unwrap();
    bytes[c.offset as usize] ^= 0x01;
    let p = dir.path().join("bad.sseg");
    std::fs::write(&p, &bytes).unwrap();
    assert!(SegmentReader::open(&p).unwrap().verify().is_err());
}

fn arb_attr_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(Value::UInt),
        (-1e6f64..1e6).prop_map(Value::Float),
        "[a-z\u{e9}]{0,6}".prop_map(Value::String),
        prop::collection::vec(any::<u8>(), 0..4).prop_map(Value::Bytes),
        any::<i64>().prop_map(Value::Timestamp),
    ];
    leaf.prop_recursive(2, 8, 3, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..3).prop_map(Value::List),
            prop::collection::btree_map("[a-c]{1,2}", inner, 0..3).prop_map(Value::Map),
        ]
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Key projection equals looking keys up in fully decoded maps, for
    /// shredded keys (frequent), residual keys (rare) and absent keys, in both
    /// scopes, with and without shredding.
    #[test]
    fn read_map_keys_equals_full_maps(
        rows in prop::collection::vec(
            (prop::collection::btree_map(
                prop::sample::select(vec!["hot", "warm", "rare", "x.y", ""]).prop_map(str::to_owned),
                arb_attr_value(),
                0..5,
            ),
             prop::collection::btree_map(
                prop::sample::select(vec!["host", "zone"]).prop_map(str::to_owned),
                arb_attr_value(),
                0..2,
            )),
            1..60,
        ),
        hot_every in 1usize..3,
        keys in prop::collection::btree_set(
            prop::sample::select(vec!["hot", "warm", "rare", "x.y", "", "absent", "host", "zone"]).prop_map(str::to_owned),
            1..5,
        ),
        shred in any::<bool>(),
    ) {
        let events: Vec<Event> = rows
            .into_iter()
            .enumerate()
            .map(|(i, (mut a, r))| {
                let mut e = Event::new(tenant(), 1_000 + i as i64, 2_000);
                if i % hot_every == 0 {
                    a.insert("hot".into(), Value::Int(i as i64)); // frequent → shredded
                }
                e.attributes = a;
                e.resource_attributes = r;
                e
            })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let mut refs: Vec<RowRef<'_>> = events
            .iter()
            .enumerate()
            .map(|(i, e)| RowRef { seq: 1 + i as u64, idx: 0, event: e })
            .collect();
        let path = dir.path().join(segment_file_name(1));
        let opts = WriteOptions { compress: true, shred };
        write_segment_opts(&path, 1, &tenant(), &mut refs, 0, Vec::new(), &opts).unwrap();
        let r = SegmentReader::open(&path).unwrap();
        let keys: Vec<String> = keys.into_iter().collect();
        for (scope, id) in [(Scope::Attributes, ColumnId::Attributes), (Scope::Resource, ColumnId::ResourceAttributes)] {
            let full = r.read_column(id).unwrap().into_maps().unwrap();
            let (cols, bytes) = r.read_map_keys(scope, &keys).unwrap();
            // Bytes are read whenever some key is shredded or the map chunk is projected.
            prop_assert!(bytes > 0);
            for (k, col) in keys.iter().zip(&cols) {
                prop_assert_eq!(col.len(), full.len());
                for (row, m) in full.iter().enumerate() {
                    prop_assert_eq!(&col[row], &m.get(k).cloned(), "key {:?} row {}", k, row);
                }
            }
        }
    }
}

/// `skip_value` rejects exactly what `decode_value` rejects on corrupt input.
#[test]
fn skip_value_validates_like_decode() {
    use stratadb_common::codec::{Decoder, Encoder};
    use stratadb_schema::codec::{decode_value, encode_value, skip_value, DecodeLimits};
    let mut m = std::collections::BTreeMap::new();
    m.insert(
        "a".to_owned(),
        Value::List(vec![Value::from("é"), Value::Int(-5)]),
    );
    m.insert(
        "b".to_owned(),
        Value::Map(std::collections::BTreeMap::from([(
            "z".to_owned(),
            Value::Null,
        )])),
    );
    let mut e = Encoder::new();
    encode_value(&mut e, &Value::Map(m));
    let good = e.into_inner();
    let lim = DecodeLimits::default();
    for i in 0..good.len() {
        for flip in [0x01u8, 0x80, 0xff] {
            let mut b = good.clone();
            b[i] ^= flip;
            let mut d1 = Decoder::new(&b, "t");
            let mut d2 = Decoder::new(&b, "t");
            let dec = decode_value(&mut d1, &lim).map(|_| d1.position());
            let skp = skip_value(&mut d2, &lim).map(|()| d2.position());
            assert_eq!(dec.ok(), skp.ok(), "byte {i} flip {flip:#x}");
        }
    }
}

/// A filter built from other rows than the segment's misses values; the
/// verification must report it (a false negative would hide matching rows).
#[test]
fn false_negatives_are_detected() {
    let mk = |ids: std::ops::Range<i64>| -> Vec<Event> {
        ids.map(|i| {
            let mut e = Event::new(tenant(), 1, 1);
            e.trace_id = Some(format!("{i:032x}"));
            e.attributes
                .insert("request_id".into(), Value::String(format!("r{i}")));
            e.body = Some(Value::String(format!("unique body token{i}x")));
            e
        })
        .collect()
    };
    fn refs(evs: &[Event]) -> Vec<RowRef<'_>> {
        evs.iter()
            .map(|e| RowRef {
                seq: 1,
                idx: 0,
                event: e,
            })
            .collect()
    }
    let (a, b) = (mk(0..50), mk(1000..1050));
    let keys = vec!["request_id".to_owned()];
    let bloom = bloom::BloomSet::build(&refs(&a), &keys, &[]);
    let ngram = ngram::NgramSet::build(&refs(&a), true, &[], &[]);
    assert!(bloom.false_negatives(&refs(&a)).is_empty());
    assert!(ngram.false_negatives(&refs(&a)).is_empty());
    let fb = bloom.false_negatives(&refs(&b));
    assert!(fb.contains(&"bloom(trace_id)".to_owned()), "{fb:?}");
    assert!(
        fb.contains(&"bloom(attributes['request_id'])".to_owned()),
        "{fb:?}"
    );
    assert_eq!(
        ngram.false_negatives(&refs(&b)),
        vec!["ngram(body)".to_owned()]
    );
}
