//! Power-loss *model* tests (partial; see docs/testing/crash-matrix.md).
//!
//! Model: after power loss, every WAL frame with `seq > durable_seq` (written
//! but not covered by a completed fsync) is lost. Segment files and the
//! manifest are always fsynced (file and directory) before they are
//! referenced, so their committed state survives; stale `.tmp` files may or
//! may not survive and recovery must cope with both.
//!
//! Superseded by the full simulation in `power_loss_sim.rs` (reordering,
//! torn sectors, lost un-fsynced renames); kept as a fast regression test.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use stratadb_common::TenantId;
use stratadb_schema::Event;
use stratadb_storage::{Durability, Engine, EngineOptions};
use stratadb_wal::format::{decode_header, parse_frame, parse_segment_file_name, HEADER_LEN};

fn tenant() -> TenantId {
    TenantId::new("pl").unwrap()
}

fn opts(dir: &Path, d: Durability) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.wal.durability = d;
    // Async uses one large WAL segment so nothing is ever synced by rotation:
    // after the power cut the WAL ends far below the manifest boundary.
    o.wal.segment_max_bytes = if d == Durability::Async {
        1 << 20
    } else {
        4096
    };
    o.flush_on_close = false;
    o
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let target = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &target);
        } else {
            fs::copy(e.path(), target).unwrap();
        }
    }
}

/// Drop every WAL frame with seq > durable_seq.
fn lose_unsynced_wal(data: &Path, durable_seq: u64) {
    let wal = data.join("wal");
    for e in fs::read_dir(&wal).unwrap() {
        let p = e.unwrap().path();
        let name = p.file_name().unwrap().to_str().unwrap().to_owned();
        let Some(_) = parse_segment_file_name(&name) else {
            continue;
        };
        let bytes = fs::read(&p).unwrap();
        let version = decode_header(&bytes, "test").unwrap().version;
        let mut pos = HEADER_LEN;
        while pos < bytes.len() {
            let f = parse_frame(&bytes[pos..], version).unwrap();
            if f.seq > durable_seq {
                break;
            }
            pos += f.encoded_len();
        }
        fs::OpenOptions::new()
            .write(true)
            .open(&p)
            .unwrap()
            .set_len(pos as u64)
            .unwrap();
    }
}

fn batch(b: usize) -> Vec<Event> {
    (0..2)
        .map(|j| {
            let mut e = Event::new(tenant(), (b * 10 + j) as i64, 0);
            e.event_id = Some(format!("b{b}-{j}"));
            e
        })
        .collect()
}

fn ids(e: &Engine) -> Vec<String> {
    e.snapshot(&tenant())
        .scan_all()
        .unwrap()
        .into_iter()
        .map(|(_, _, ev)| ev.event_id.unwrap())
        .collect()
}

/// Returns (acked (batch, seq) list, durable_seq, flushed_through) and leaves a
/// power-loss image of the data directory in `image`.
fn workload(d: Durability, image: &Path) -> (Vec<(usize, u64)>, u64, u64) {
    let live = tempfile::tempdir().unwrap();
    let e = Engine::open(opts(live.path(), d)).unwrap();
    let mut acked = Vec::new();
    for b in 0..60 {
        let r = e.write_batch(&tenant(), batch(b)).unwrap();
        acked.push((b, r.commit_seq));
        if b == 19 || b == 39 {
            e.flush().unwrap();
        }
    }
    let st = e.stats();
    // Image the directory while the engine is quiescent, then cut power.
    copy_dir(live.path(), image);
    lose_unsynced_wal(image, st.wal.durable_seq);
    std::mem::forget(e); // no shutdown work may run on the live dir after the "power cut"
    (acked, st.wal.durable_seq, st.flushed_through_seq)
}

fn check(d: Durability) {
    let tmp = tempfile::tempdir().unwrap();
    let image = tmp.path().join("image");
    let (acked, durable, flushed) = workload(d, &image);
    if d == Durability::Async {
        assert!(
            durable < flushed,
            "model must exercise WAL-behind-manifest ({durable} vs {flushed})"
        );
    }
    let e = Engine::open(opts(&image, d)).unwrap();
    let got = ids(&e);
    let set: BTreeSet<&String> = got.iter().collect();
    assert_eq!(set.len(), got.len(), "duplicates after power loss");
    let guaranteed = durable.max(flushed);
    for (b, seq) in &acked {
        let present = (0..2)
            .filter(|j| set.contains(&format!("b{b}-{j}")))
            .count();
        assert!(present == 0 || present == 2, "batch {b} partial");
        if *seq <= guaranteed {
            assert_eq!(
                present, 2,
                "{d:?}: batch {b} (seq {seq}) lost although durable (<= {guaranteed})"
            );
        }
    }
    // Sequence numbers never repeat after recovery: new writes are visible and unique.
    let r = e.write_batch(&tenant(), batch(1000)).unwrap();
    assert!(r.commit_seq > guaranteed);
    e.flush().unwrap();
    e.verify().unwrap();
    drop(e);
    let e = Engine::open(opts(&image, d)).unwrap();
    assert_eq!(
        ids(&e).len(),
        got.len() + 2,
        "second recovery after power loss"
    );
}

#[test]
fn sync_mode_survives_loss_of_unsynced_wal() {
    // In sync mode every acknowledged batch is <= durable_seq.
    check(Durability::Sync);
}

#[test]
fn group_commit_mode_survives_loss_of_unsynced_wal() {
    check(Durability::GroupCommit {
        max_delay: std::time::Duration::from_micros(100),
    });
}

#[test]
fn async_mode_keeps_flushed_data_and_restarts_sequence_safely() {
    // Async: acks beyond the last sync/flush may be lost (documented contract),
    // but flushed data survives and the WAL may end *below* the manifest
    // boundary, which recovery must handle without reusing sequence numbers.
    check(Durability::Async);
}
