//! The storage scan seam (spec STRATA-ARCH-01-003): segments and memtable
//! chunks serve the same columns through `ScanSource`, and a source re-checks
//! tenant ownership on every read.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;

use stratadb_common::{ErrorKind, TenantId};
use stratadb_schema::column::{ColumnData, ColumnId, Scope};
use stratadb_schema::{Event, Value};
use stratadb_storage::{ColumnRequest, Durability, Engine, EngineOptions, ScanSource};

fn tenant(name: &str) -> TenantId {
    TenantId::new(name).unwrap()
}

fn events(t: &TenantId, n: i64) -> Vec<Event> {
    (0..n)
        .map(|j| {
            let mut ev = Event::new(t.clone(), 1_795_170_600_000_000_000 + j, 1);
            ev.service = Some(format!("svc{}", j % 3));
            ev.attributes
                .insert("region".into(), Value::String(format!("r{}", j % 2)));
            ev
        })
        .collect()
}

fn engine(dir: &std::path::Path) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    Engine::open(o).unwrap()
}

#[test]
fn segments_and_memtable_serve_identical_columns() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let a = tenant("a");
    e.write_batch(&a, events(&a, 50)).unwrap();
    // Same rows, held in the memtable only.
    let mem_dir = tempfile::tempdir().unwrap();
    let mem = engine(mem_dir.path());
    mem.write_batch(&a, events(&a, 50)).unwrap();
    e.flush().unwrap();

    let none = BTreeSet::new();
    let keys = BTreeSet::from(["region".to_owned()]);
    let cols = [ColumnId::EventTime, ColumnId::Service, ColumnId::Attributes];
    let req = |projection| ColumnRequest {
        columns: &cols,
        attr_keys: &keys,
        whole_attributes: false,
        resource_keys: &none,
        whole_resource: false,
        raw_attr_keys: &none,
        raw_resource_keys: &none,
        projection,
    };

    let snap = e.snapshot(&a);
    let srcs = snap.sources();
    assert_eq!(srcs.len(), 1);
    assert!(srcs[0].owned_by(&a));
    assert_eq!(srcs[0].rows(), 50);
    let seg = srcs[0].read(&a, &req(true)).unwrap();
    assert_eq!(seg.rows, 50);
    assert!(seg.bytes_read > 0);
    // A key-only scope is served per key, not as whole maps.
    assert!(!seg.columns.contains_key(&ColumnId::Attributes));
    let region = &seg.keys[&(Scope::Attributes, "region".to_owned())];

    let msnap = mem.snapshot(&a);
    let chunks: Vec<_> = msnap.memory_chunks(4096).collect();
    assert_eq!(chunks.len(), 1);
    let m = chunks[0].read(&a, &req(true)).unwrap();
    assert_eq!(m.rows, 50);
    assert_eq!(
        m.columns[&ColumnId::EventTime],
        seg.columns[&ColumnId::EventTime]
    );
    assert_eq!(
        m.columns[&ColumnId::Service],
        seg.columns[&ColumnId::Service]
    );
    // The memtable serves whole maps; their projection equals the per-key column.
    let ColumnData::Maps(maps) = &m.columns[&ColumnId::Attributes] else {
        panic!("memtable attributes are not maps");
    };
    let projected: Vec<Option<Value>> = maps.iter().map(|m| m.get("region").cloned()).collect();
    assert_eq!(&projected, region);
    assert_eq!(chunks[0].event_time_range(), srcs[0].event_time_range());
}

#[test]
fn a_source_refuses_reads_for_another_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let (a, b) = (tenant("a"), tenant("b"));
    e.write_batch(&a, events(&a, 10)).unwrap();
    e.flush().unwrap();
    e.write_batch(&a, events(&a, 5)).unwrap();
    let snap = e.snapshot(&a);
    let none = BTreeSet::new();
    let req = ColumnRequest {
        columns: &[ColumnId::EventTime],
        attr_keys: &none,
        whole_attributes: false,
        resource_keys: &none,
        whole_resource: false,
        raw_attr_keys: &none,
        raw_resource_keys: &none,
        projection: true,
    };
    let seg = &snap.sources()[0];
    assert!(!seg.owned_by(&b));
    assert_eq!(seg.read(&b, &req).unwrap_err().kind(), ErrorKind::Internal);
    let chunk = snap.memory_chunks(16).next().unwrap();
    assert!(!chunk.owned_by(&b));
    assert_eq!(
        chunk.read(&b, &req).unwrap_err().kind(),
        ErrorKind::Internal
    );
    assert_eq!(chunk.read(&a, &req).unwrap().rows, 5);
}

#[test]
fn read_estimates_scale_with_columns_and_agree_across_sources() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine(dir.path());
    let a = tenant("a");
    e.write_batch(&a, events(&a, 1000)).unwrap();
    let mem_snap = e.snapshot(&a);
    let chunks: Vec<_> = mem_snap.memory_chunks(4096).collect();
    let none = BTreeSet::new();
    let req = |cols: &'static [ColumnId]| ColumnRequest {
        columns: cols,
        attr_keys: &none,
        whole_attributes: true,
        resource_keys: &none,
        whole_resource: false,
        raw_attr_keys: &none,
        raw_resource_keys: &none,
        projection: true,
    };
    let times: &[ColumnId] = &[ColumnId::EventTime];
    let wide: &[ColumnId] = &[ColumnId::EventTime, ColumnId::Service, ColumnId::Attributes];
    let est = |cols| {
        chunks
            .iter()
            .map(|c| c.estimated_read_bytes(&req(cols)))
            .sum::<u64>()
    };
    let m_times = est(times);
    let m_wide = est(wide);
    assert_eq!(m_times, 1000 * 8, "fixed-width: exactly the slots");
    assert!(m_wide > 4 * m_times, "{m_wide} vs {m_times}");
    drop(mem_snap);
    e.flush().unwrap();
    let snap = e.snapshot(&a);
    let seg = &snap.sources()[0];
    let s_times = seg.estimated_read_bytes(&req(times));
    let s_wide = seg.estimated_read_bytes(&req(wide));
    assert!((1000 * 8..=1000 * 8 * 3).contains(&s_times), "{s_times}");
    // Same rows: the estimates agree within a small factor.
    let ratio = s_wide as f64 / m_wide as f64;
    assert!(
        (0.5..=4.0).contains(&ratio),
        "segment {s_wide} vs memtable {m_wide}"
    );
}
