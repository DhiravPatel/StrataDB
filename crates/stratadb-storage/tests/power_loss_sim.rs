//! Power-loss simulation over the whole engine (T-P1-PL, spec §26.5).
//!
//! A workload runs with the `simfs` recorder on (`stratadb_common::simfs`):
//! crash images are captured immediately before and after every file and
//! directory fsync. Each image is materialized into several post-power-loss
//! disk states — only-synced, everything (process crash), and random mixes in
//! which un-synced sectors are lost, torn or persisted out of order, file sizes
//! revert, and un-synced creates/renames/unlinks are lost (atomically per
//! inode) — and recovered. Invariants per state:
//!
//! 1. the engine opens (power loss must never need operator intervention);
//! 2. `sync`/`group_commit`: every acknowledged batch is present exactly once;
//!    `async`: every batch covered by a completed flush is present;
//! 3. batches are all-or-nothing; only batches written before the crash point
//!    can appear, and (async) the surviving batches form a prefix;
//! 4. the recovered database accepts writes, flushes, and reopens;
//! 5. retrying an acknowledged batch with its idempotency key is a duplicate.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;

use stratadb_common::simfs::{self, CrashImage, Variant};
use stratadb_common::TenantId;
use stratadb_schema::{Event, Value};
use stratadb_storage::{CompactionOptions, Durability, Engine, EngineOptions, IdempotencyKey};

// The recorder is process-global: one simulation at a time.
static SERIAL: Mutex<()> = Mutex::new(());

const BATCHES: u64 = 30;
const PER_BATCH: u64 = 3;

fn tenant() -> TenantId {
    TenantId::new("pl").unwrap()
}

fn opts(dir: &Path, d: Durability) -> EngineOptions {
    let mut o = EngineOptions::new(dir);
    o.background = false;
    o.flush_on_close = false;
    o.wal.durability = d;
    o.wal.segment_max_bytes = 2048;
    o.compaction = CompactionOptions {
        min_inputs: 2,
        ..CompactionOptions::default()
    };
    // Tiny test volumes must not trip the disk reserve.
    o.disk_reserve_bytes = 0;
    o.disk_reserve_ratio = 0.0;
    o
}

fn batch(b: u64) -> Vec<Event> {
    (0..PER_BATCH)
        .map(|k| {
            let t = 1_795_170_600_000_000_000 + (b * PER_BATCH + k) as i64;
            let mut e = Event::new(tenant(), t, t);
            e.event_id = Some(format!("b{b:03}-{k}"));
            e.body = Some(Value::from(format!(
                "batch {b} event {k} {}",
                "x".repeat(40)
            )));
            e
        })
        .collect()
}

fn key(b: u64) -> IdempotencyKey {
    IdempotencyKey::new(format!("k{b}"), [b as u8; 32]).unwrap()
}

/// Run the workload under the recorder; returns the crash images.
fn record(root: &Path, d: Durability) -> Vec<CrashImage> {
    std::fs::create_dir_all(root).unwrap();
    simfs::enable(root, 5000);
    {
        let e = Engine::open(opts(root, d)).unwrap();
        for b in 0..BATCHES {
            e.write_batch_idempotent(&tenant(), batch(b), Some(key(b)))
                .unwrap();
            if d != Durability::Async {
                simfs::set_tag(b + 1); // batches 0..=b acknowledged
            }
            if b % 5 == 4 {
                e.flush().unwrap();
                if d == Durability::Async {
                    simfs::set_tag(b + 1); // batches 0..=b durable in segments
                }
            }
            if b % 12 == 11 {
                e.compact().unwrap();
            }
        }
    }
    simfs::disable()
}

/// Count events per batch in a recovered database.
fn batches_present(e: &Engine) -> BTreeMap<u64, u64> {
    let mut out = BTreeMap::new();
    for (_, _, ev) in e.snapshot(&tenant()).scan_all().unwrap() {
        let id = ev.event_id.unwrap();
        let b: u64 = id[1..4].parse().unwrap();
        *out.entry(b).or_insert(0) += 1;
    }
    out
}

type TailKind = Option<stratadb_wal::format::InvalidTail>;

/// Check one recovered state; returns a description of the first violation,
/// or the WAL tail classification recovery applied (if it truncated).
fn check(dir: &Path, d: Durability, tag: u64, deep: bool) -> Result<TailKind, String> {
    let e = Engine::open(opts(dir, d)).map_err(|err| format!("open failed: {err}"))?;
    let truncation = e.open_report().wal.truncation.as_ref().map(|t| t.kind);
    let present = batches_present(&e);
    let mut max_present = None;
    for (&b, &n) in &present {
        if n != PER_BATCH {
            return Err(format!(
                "batch {b} partially present ({n}/{PER_BATCH} events)"
            ));
        }
        if b >= BATCHES {
            return Err(format!("unknown batch {b}"));
        }
        max_present = Some(b);
    }
    // Writes are sequential, so nothing beyond the in-flight batch exists.
    let written_bound = tag + if d == Durability::Async { 5 } else { 1 };
    if let Some(m) = max_present {
        if m >= written_bound {
            return Err(format!(
                "batch {m} present but not yet written at the crash point"
            ));
        }
    }
    for b in 0..tag {
        if !present.contains_key(&b) {
            return Err(format!(
                "{} batch {b} lost (present: {:?})",
                if d == Durability::Async {
                    "flushed"
                } else {
                    "acknowledged"
                },
                present.keys().collect::<Vec<_>>()
            ));
        }
    }
    if d == Durability::Async {
        let n = present.len() as u64;
        if present.keys().copied().ne(0..n) {
            return Err(format!(
                "surviving batches are not a prefix: {:?}",
                present.keys()
            ));
        }
    }
    if deep {
        if d != Durability::Async {
            for b in 0..tag {
                let r = e
                    .write_batch_idempotent(&tenant(), batch(b), Some(key(b)))
                    .map_err(|err| format!("retry of batch {b} failed: {err}"))?;
                if !r.duplicate {
                    return Err(format!(
                        "retry of acknowledged batch {b} was not a duplicate"
                    ));
                }
            }
        }
        e.write_batch(&tenant(), batch(BATCHES))
            .map_err(|err| format!("write after recovery failed: {err}"))?;
        e.flush()
            .map_err(|err| format!("flush after recovery failed: {err}"))?;
        drop(e);
        let e = Engine::open(opts(dir, d)).map_err(|err| format!("reopen failed: {err}"))?;
        let again = batches_present(&e);
        if again.get(&BATCHES) != Some(&PER_BATCH) {
            return Err("post-recovery write lost after reopen".into());
        }
    }
    Ok(truncation)
}

/// Returns how many states needed a reordered-tail truncation.
fn simulate(d: Durability, random_variants: u64) -> usize {
    let _g = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let work = tempfile::tempdir().unwrap();
    let images = record(&work.path().join("live"), d);
    assert!(images.len() > 100, "too few crash points: {}", images.len());
    let mut failures = Vec::new();
    let mut states = 0;
    let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
    for (i, img) in images.iter().enumerate() {
        let mut variants = vec![Variant::Durable, Variant::Current];
        variants.extend((0..random_variants).map(|s| Variant::Random(s * 7919 + i as u64)));
        for (j, v) in variants.into_iter().enumerate() {
            let dir = work.path().join(format!("img-{i}-{j}"));
            img.materialize(v, &dir).unwrap();
            states += 1;
            // Deep checks (retries, writes, reopen) on a deterministic sample.
            let deep = (i + j) % 7 == 0;
            match check(&dir, d, img.tag, deep) {
                Ok(t) => {
                    let k = t.map_or("clean".to_owned(), |k| format!("{k:?}"));
                    *kinds.entry(k).or_default() += 1;
                }
                Err(why) => failures.push(format!(
                    "[{i}] {} / {v:?} (tag {}): {why}",
                    img.label, img.tag
                )),
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
    eprintln!(
        "{d:?}: {} crash points, {states} recovered states, {} failures; WAL tails: {kinds:?}",
        images.len(),
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "{} of {states} power-loss states violated invariants; first 100:\n{}",
        failures.len(),
        failures
            .iter()
            .take(100)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    kinds.get("UnsyncedReordered").copied().unwrap_or(0)
}

#[test]
fn power_loss_sync() {
    simulate(Durability::Sync, 4);
}

#[test]
fn power_loss_group_commit() {
    simulate(
        Durability::GroupCommit {
            max_delay: std::time::Duration::ZERO,
        },
        2,
    );
}

#[test]
fn power_loss_async() {
    // The async workload leaves several un-synced frames in flight, so random
    // states must exercise the reordered-tail path (WAL v2).
    assert!(simulate(Durability::Async, 2) > 0);
}
