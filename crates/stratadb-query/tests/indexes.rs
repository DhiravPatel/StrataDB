//! Index lifecycle (spec §3.1, §12.8): list, verify against the rows, detect
//! stale settings, rebuild from authoritative data.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;

use stratadb_common::TenantId;
use stratadb_query::{execute, QueryOptions};
use stratadb_schema::{Event, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("ix").unwrap()
}

fn opts(dir: &Path, bloom_keys: &[&str]) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    o.bloom_attribute_keys = bloom_keys.iter().map(|s| (*s).to_owned()).collect();
    o
}

fn batch(from: i64) -> Vec<Event> {
    (from..from + 40)
        .map(|i| {
            let mut e = Event::new(
                tenant(),
                1_795_170_600_000_000_000 + i,
                1_795_170_600_000_000_000,
            );
            e.event_id = Some(format!("e{i}"));
            e.attributes
                .insert("request_id".into(), Value::String(format!("req-{i}")));
            e.body = Some(Value::String(format!("message {i}")));
            e
        })
        .collect()
}

#[test]
fn verify_detects_stale_settings_and_rebuild_fixes_them() {
    let d = tempfile::tempdir().unwrap();
    {
        let e = Engine::open(opts(d.path(), &[])).unwrap();
        for s in 0..3 {
            e.write_batch(&tenant(), batch(s * 40)).unwrap();
            e.flush().unwrap();
        }
        let list = e.indexes(false).unwrap();
        assert_eq!(list.len(), 3);
        assert!(list
            .iter()
            .all(|s| s.check.is_none() && s.bloom_bytes > 0 && s.ngram_bytes > 0));
        assert!(list[0].bloom_fields.contains(&"bloom(trace_id)".to_owned()));
        assert!(!list[0]
            .bloom_fields
            .iter()
            .any(|f| f.contains("request_id")));
        for s in e.indexes(true).unwrap() {
            let c = s.check.unwrap();
            assert!(c.false_negatives.is_empty() && c.current, "{c:?}");
        }
    }
    // New setting: request_id gets a Bloom filter — existing segments are stale.
    let e = Engine::open(opts(d.path(), &["request_id"])).unwrap();
    let sql = "SELECT event_id FROM logs WHERE attributes['request_id'] = 'req-45'";
    let before = execute(&e.snapshot(&tenant()), sql, &QueryOptions::default()).unwrap();
    assert_eq!(
        before.stats.segments_pruned_bloom, 0,
        "old segments have no request_id filter"
    );
    for s in e.indexes(true).unwrap() {
        let c = s.check.unwrap();
        assert!(c.false_negatives.is_empty());
        assert!(!c.current, "segment {} should be stale", s.segment_id);
    }
    let dry = e.rebuild_indexes(true).unwrap();
    assert_eq!(
        (
            dry.segments_stale,
            dry.segments_rebuilt,
            dry.segments_unsound
        ),
        (3, 0, 0)
    );
    let r = e.rebuild_indexes(false).unwrap();
    assert_eq!(r.segments_rebuilt, 3);
    for s in e.indexes(true).unwrap() {
        let c = s.check.unwrap();
        assert!(c.current && c.false_negatives.is_empty(), "{c:?}");
        assert!(s
            .bloom_fields
            .contains(&"bloom(attributes['request_id'])".to_owned()));
    }
    let after = execute(&e.snapshot(&tenant()), sql, &QueryOptions::default()).unwrap();
    assert_eq!(after.rows, before.rows, "rebuild must not change results");
    assert_eq!(
        after.stats.segments_pruned_bloom, 2,
        "rebuilt filters now prune"
    );
    // Idempotent: nothing left to rebuild.
    assert_eq!(e.rebuild_indexes(false).unwrap().segments_rebuilt, 0);
    drop(e);
    // Durable across restart.
    let e = Engine::open(opts(d.path(), &["request_id"])).unwrap();
    assert!(e
        .indexes(true)
        .unwrap()
        .iter()
        .all(|s| s.check.as_ref().unwrap().current));
    assert_eq!(
        execute(
            &e.snapshot(&tenant()),
            "SELECT count(*) FROM logs",
            &QueryOptions::default()
        )
        .unwrap()
        .rows[0][0],
        Some(Value::Int(120))
    );
}
