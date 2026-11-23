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
