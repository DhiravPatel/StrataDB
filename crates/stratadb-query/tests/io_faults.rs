//! Storage read faults under real queries (spec STRATA-SYS-04-001/009/025,
//! STRATA-SYS-02-037): short reads are resubmitted, a failed read is retried
//! once, a persistent failure fails the query naming the segment file (never
//! partial results), and a small descriptor budget does not limit how many
//! segments a query can read.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::sync::Arc;

use stratadb_common::io::{Fault, FaultyIo, IoEngine, PreadEngine};
use stratadb_common::{ErrorKind, TenantId};
use stratadb_query::{execute, QueryOptions};
use stratadb_schema::{Event, Value};
use stratadb_storage::{Durability, Engine, EngineOptions};

fn tenant() -> TenantId {
    TenantId::new("io").unwrap()
}

fn engine(dir: &Path, io: Arc<dyn IoEngine>, segments: i64) -> Engine {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = Durability::Async;
    o.io = Some(io);
    let e = Engine::open(o).unwrap();
    for s in 0..segments {
        let evs: Vec<Event> = (0..200)
            .map(|j| {
                let mut ev = Event::new(tenant(), 1_795_170_600_000_000_000 + s * 1000 + j, 1);
                ev.service = Some(format!("svc{}", j % 5));
                ev.body = Some(Value::String(format!("event {s}/{j}")));
                ev
            })
            .collect();
        e.write_batch(&tenant(), evs).unwrap();
        e.flush().unwrap();
    }
    e
}

const SQL: &str = "SELECT service, count(*) AS n FROM logs GROUP BY service ORDER BY service";

fn run(e: &Engine) -> stratadb_common::Result<Vec<Vec<Option<Value>>>> {
    execute(&e.snapshot(&tenant()), SQL, &QueryOptions::default()).map(|o| o.rows)
}

#[test]
fn read_faults_are_retried_resubmitted_or_reported() {
    let dir = tempfile::tempdir().unwrap();
    let faulty = Arc::new(FaultyIo::new(Arc::new(PreadEngine::new(64))));
    let e = engine(dir.path(), faulty.clone(), 4);
    let want = run(&e).unwrap();
    assert_eq!(want.len(), 5);

    // Short reads everywhere: identical results.
    faulty.inject(None, Fault::ShortRead, 50);
    assert_eq!(run(&e).unwrap(), want);
    assert!(faulty.stats().short_reads_resubmitted >= 1);

    // One EIO: retried once, identical results.
    faulty.inject(None, Fault::ReadEio, 1);
    assert_eq!(run(&e).unwrap(), want);
    assert!(faulty.stats().read_retries >= 1);

    // Persistent EIO on one segment: the query fails, naming the file; no
    // partial result is returned.
    let seg = e.snapshot(&tenant()).segments()[2]
        .reader
        .path()
        .to_path_buf();
    let name = seg.file_name().unwrap().to_string_lossy().into_owned();
    faulty.inject(Some(&name), Fault::ReadEio, 1_000);
    let err = run(&e).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Io, "{err}");
    assert!(err.to_string().contains(&name), "{err}");
    // The engine itself is unaffected: once the device recovers, queries work.
    let fresh = Arc::new(FaultyIo::new(Arc::new(PreadEngine::new(64))));
    drop(e);
    let e = {
        let mut o = EngineOptions::new(dir.path());
        o.background = false;
        o.io = Some(fresh);
        Engine::open(o).unwrap()
    };
    assert_eq!(run(&e).unwrap(), want);
}

#[test]
fn many_segments_with_a_small_descriptor_budget() {
    let dir = tempfile::tempdir().unwrap();
    let io = Arc::new(PreadEngine::new(8));
    let e = engine(dir.path(), io.clone(), 30);
    let snap = e.snapshot(&tenant());
    assert_eq!(snap.segments().len(), 30);
    // Every segment's column data is read (parallel and sequential),
    // descriptors stay
    // within the budget: cold files are closed and reopened on demand.
    for par in [1, 8] {
        let out = execute(
            &snap,
            "SELECT count(*) FROM logs WHERE service = 'svc1'",
            &QueryOptions {
                parallelism: par,
                target_ms_per_worker: 0.0,
                ..QueryOptions::default()
            },
        )
        .unwrap();
        assert_eq!(out.rows[0][0], Some(Value::Int(1200)));
    }
    let s = io.stats();
    assert!(s.files_open <= 8, "{s:?}");
    assert!(s.evictions >= 22 && s.opens > 30, "{s:?}");
    // Restart: same budget, same answers.
    drop(snap);
    drop(e);
    let mut o = EngineOptions::new(dir.path());
    o.background = false;
    o.io = Some(Arc::new(PreadEngine::new(8)));
    let e = Engine::open(o).unwrap();
    assert_eq!(
        execute(
            &e.snapshot(&tenant()),
            "SELECT count(*) FROM logs",
            &QueryOptions::default()
        )
        .unwrap()
        .rows[0][0],
        Some(Value::Int(6000))
    );
}
