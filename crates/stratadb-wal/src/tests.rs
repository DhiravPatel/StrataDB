use super::*;
use proptest::prelude::*;
use stratadb_common::ErrorKind;

fn opts(d: Durability) -> WalOptions {
    WalOptions {
        durability: d,
        segment_max_bytes: 4096,
        max_record_bytes: 1 << 20,
        recovery: RecoveryPolicy::TruncateTornTail,
    }
}

type Collected = (Wal, Vec<(u64, Vec<u8>)>, RecoveryReport);

fn collect(dir: &Path, o: WalOptions, after: u64) -> Result<Collected> {
    let mut out = Vec::new();
    let (wal, rep) = Wal::open(dir, o, after, |seq, t, p| {
        assert_eq!(t, RecordType::EventBatch);
        out.push((seq, p.to_vec()));
        Ok(())
    })?;
    Ok((wal, out, rep))
}

fn payload(i: u64) -> Vec<u8> {
    format!("record-{i}-{}", "x".repeat((i % 50) as usize)).into_bytes()
}

fn segment_files(dir: &Path) -> Vec<PathBuf> {
    fsutil::list_files(dir)
        .unwrap()
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "log"))
        .collect()
}

#[test]
fn append_and_replay_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, recs, _) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    assert!(recs.is_empty());
    for i in 1..=20 {
        assert_eq!(wal.append(RecordType::EventBatch, &payload(i)).unwrap(), i);
    }
    assert_eq!(wal.durable_seq(), 20);
    drop(wal);
    let (wal, recs, rep) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    assert_eq!(recs.len(), 20);
    for (i, (seq, p)) in recs.iter().enumerate() {
        assert_eq!(*seq, i as u64 + 1);
        assert_eq!(p, &payload(*seq));
    }
    assert_eq!(rep.last_seq, 20);
    assert_eq!(wal.append(RecordType::EventBatch, b"next").unwrap(), 21);
}

#[test]
fn replay_after_skips_flushed_records() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _, _) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    for i in 1..=10 {
        wal.append(RecordType::EventBatch, &payload(i)).unwrap();
    }
    drop(wal);
    let (_, recs, rep) = collect(dir.path(), opts(Durability::Sync), 6).unwrap();
    assert_eq!(
        recs.iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![7, 8, 9, 10]
    );
    assert_eq!(rep.records_skipped, 6);
}

#[test]
fn rotation_and_reclamation() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _, _) = collect(dir.path(), opts(Durability::Async), 0).unwrap();
    for i in 1..=200 {
        wal.append(RecordType::EventBatch, &payload(i)).unwrap();
    }
    let segs = segment_files(dir.path()).len();
    assert!(segs >= 3, "expected rotation, got {segs} segments");
    // Nothing reclaimable below the first segment's end.
    assert_eq!(wal.reclaim_through(0).unwrap(), 0);
    let removed = wal.reclaim_through(100).unwrap();
    assert!(removed > 0);
    assert_eq!(segment_files(dir.path()).len(), segs - removed);
    // Reclaiming everything never removes the active segment.
    wal.reclaim_through(u64::MAX).unwrap();
    assert_eq!(segment_files(dir.path()).len(), 1);
    wal.sync().unwrap();
    drop(wal);
    let (_, recs, _) = collect(dir.path(), opts(Durability::Async), 200).unwrap();
    assert!(recs.is_empty());
}

#[test]
fn reopen_after_partial_reclaim_replays_unflushed() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _, _) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    for i in 1..=150 {
        wal.append(RecordType::EventBatch, &payload(i)).unwrap();
    }
    wal.reclaim_through(90).unwrap();
    drop(wal);
    let (_, recs, _) = collect(dir.path(), opts(Durability::Sync), 90).unwrap();
    assert_eq!(recs.first().unwrap().0, 91);
    assert_eq!(recs.last().unwrap().0, 150);
    // Opening with an older manifest boundary must detect the missing range.
    let err = collect(dir.path(), opts(Durability::Sync), 10).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Corruption, "{err}");
}

fn write_n(dir: &Path, n: u64) {
    let (wal, _, _) = collect(dir, opts(Durability::Sync), 0).unwrap();
    for i in 1..=n {
        wal.append(RecordType::EventBatch, &payload(i)).unwrap();
    }
}

fn last_segment(dir: &Path) -> PathBuf {
    segment_files(dir).pop().unwrap()
}

#[test]
fn torn_tail_at_every_offset_recovers_exact_prefix() {
    let src = tempfile::tempdir().unwrap();
    write_n(src.path(), 5);
    let seg = last_segment(src.path());
    let full = fs::read(&seg).unwrap();
    // Frame boundaries.
    let mut bounds = vec![HEADER_LEN];
    let mut pos = HEADER_LEN;
    while pos < full.len() {
        pos += format::parse_frame(&full[pos..]).unwrap().encoded_len();
        bounds.push(pos);
    }
    for cut in HEADER_LEN..full.len() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(seg.file_name().unwrap());
        fs::write(&p, &full[..cut]).unwrap();
        let (wal, recs, rep) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
        let complete = bounds.iter().filter(|&&b| b <= cut).count() as u64 - 1;
        assert_eq!(recs.len() as u64, complete, "cut at {cut}");
        assert_eq!(
            rep.truncation.is_some(),
            !bounds.contains(&cut),
            "cut at {cut}"
        );
        // Appends continue with the next dense sequence number.
        assert_eq!(
            wal.append(RecordType::EventBatch, b"after").unwrap(),
            complete + 1
        );
        drop(wal);
        let (_, recs2, rep2) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
        assert_eq!(recs2.len() as u64, complete + 1);
        assert!(rep2.truncation.is_none());
    }
}

#[test]
fn mid_log_corruption_fails_closed_unless_forced() {
    let dir = tempfile::tempdir().unwrap();
    write_n(dir.path(), 5);
    let seg = last_segment(dir.path());
    let mut data = fs::read(&seg).unwrap();
    data[HEADER_LEN + FRAME_HEADER_LEN + 2] ^= 0xff; // damage record 1's payload
    fs::write(&seg, &data).unwrap();
    let err = collect(dir.path(), opts(Durability::Sync), 0).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Corruption);
    assert!(
        err.to_string().contains("followed by valid frames"),
        "{err}"
    );
    // The file must be untouched by the failed recovery.
    assert_eq!(fs::read(&seg).unwrap(), data);

    let mut o = opts(Durability::Sync);
    o.recovery = RecoveryPolicy::ForceTruncate;
    let (_, recs, rep) = collect(dir.path(), o, 0).unwrap();
    assert!(recs.is_empty());
    assert!(rep.truncation.unwrap().discarded_valid_frames);
}

#[test]
fn corruption_in_non_final_segment_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _, _) = collect(dir.path(), opts(Durability::Async), 0).unwrap();
    for i in 1..=200 {
        wal.append(RecordType::EventBatch, &payload(i)).unwrap();
    }
    drop(wal);
    let first = segment_files(dir.path()).remove(0);
    let mut data = fs::read(&first).unwrap();
    let n = data.len();
    data[n - 1] ^= 1;
    fs::write(&first, &data).unwrap();
    let err = collect(dir.path(), opts(Durability::Async), 0).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Corruption);
}

#[test]
fn missing_middle_segment_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _, _) = collect(dir.path(), opts(Durability::Async), 0).unwrap();
    for i in 1..=200 {
        wal.append(RecordType::EventBatch, &payload(i)).unwrap();
    }
    drop(wal);
    let segs = segment_files(dir.path());
    fs::remove_file(&segs[1]).unwrap();
    let err = collect(dir.path(), opts(Durability::Async), 0).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Corruption);
    assert!(err.to_string().contains("gap"), "{err}");
}

#[test]
fn header_corruption_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    write_n(dir.path(), 3);
    let seg = last_segment(dir.path());
    let mut data = fs::read(&seg).unwrap();
    data[3] ^= 1;
    fs::write(&seg, &data).unwrap();
    assert_eq!(
        collect(dir.path(), opts(Durability::Sync), 0)
            .unwrap_err()
            .kind(),
        ErrorKind::Corruption
    );
}

#[test]
fn wal_behind_manifest_starts_new_segment() {
    // Models an Async-mode power loss where the WAL tail vanished after its
    // records were already flushed to segments (manifest covers through 50).
    let dir = tempfile::tempdir().unwrap();
    write_n(dir.path(), 10);
    let (wal, recs, _) = collect(dir.path(), opts(Durability::Sync), 50).unwrap();
    assert!(recs.is_empty());
    assert_eq!(wal.append(RecordType::EventBatch, b"x").unwrap(), 51);
    drop(wal);
    let (_, recs, _) = collect(dir.path(), opts(Durability::Sync), 50).unwrap();
    assert_eq!(recs.iter().map(|r| r.0).collect::<Vec<_>>(), vec![51]);
    // Later, with a newer manifest boundary, the layout still recovers.
    let (_, recs, _) = collect(dir.path(), opts(Durability::Sync), 51).unwrap();
    assert!(recs.is_empty());
}

#[test]
fn stale_tmp_files_are_removed() {
    let dir = tempfile::tempdir().unwrap();
    write_n(dir.path(), 2);
    fs::write(dir.path().join("wal-00000000000000000099.log.tmp"), b"junk").unwrap();
    let (_, recs, rep) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(rep.removed_tmp_files, 1);
}

#[test]
fn oversized_record_rejected_without_side_effects() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts(Durability::Sync);
    o.max_record_bytes = 16;
    let (wal, _, _) = collect(dir.path(), o, 0).unwrap();
    let err = wal.append(RecordType::EventBatch, &[0u8; 17]).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert_eq!(wal.append(RecordType::EventBatch, b"ok").unwrap(), 1);
}

fn concurrent(d: Durability) {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _, _) = collect(dir.path(), opts(d), 0).unwrap();
    let wal = Arc::new(wal);
    let threads = 8;
    let per = 50u64;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let wal = Arc::clone(&wal);
            std::thread::spawn(move || {
                (0..per)
                    .map(|i| {
                        let p = format!("t{t}-{i}");
                        let seq = wal.append(RecordType::EventBatch, p.as_bytes()).unwrap();
                        // Acknowledged => durable under Sync/GroupCommit.
                        assert!(wal.durable_seq() >= seq);
                        (seq, p)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut acked: Vec<(u64, String)> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    acked.sort();
    let total = threads * per;
    assert_eq!(acked.len() as u64, total);
    assert!(acked
        .iter()
        .enumerate()
        .all(|(i, (s, _))| *s == i as u64 + 1));
    drop(wal);
    let (_, recs, _) = collect(dir.path(), opts(d), 0).unwrap();
    let recs: Vec<(u64, String)> = recs
        .into_iter()
        .map(|(s, p)| (s, String::from_utf8(p).unwrap()))
        .collect();
    assert_eq!(recs, acked);
}

#[test]
fn concurrent_sync_appends() {
    concurrent(Durability::Sync);
}

#[test]
fn concurrent_group_commit_appends() {
    concurrent(Durability::GroupCommit {
        max_delay: Duration::from_millis(1),
    });
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn random_truncation_recovers_prefix(
        payloads in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..300), 1..12),
        cut_frac in 0.0f64..1.0,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut o = opts(Durability::Async);
        o.segment_max_bytes = 1 << 20;
        let (wal, _, _) = collect(dir.path(), o.clone(), 0).unwrap();
        for p in &payloads {
            wal.append(RecordType::EventBatch, p).unwrap();
        }
        wal.sync().unwrap();
        drop(wal);
        let seg = last_segment(dir.path());
        let data = fs::read(&seg).unwrap();
        let cut = HEADER_LEN + ((data.len() - HEADER_LEN) as f64 * cut_frac) as usize;
        fs::write(&seg, &data[..cut]).unwrap();
        let (_, recs, _) = collect(dir.path(), o, 0).unwrap();
        prop_assert!(recs.len() <= payloads.len());
        for (i, (seq, p)) in recs.iter().enumerate() {
            prop_assert_eq!(*seq, i as u64 + 1);
            prop_assert_eq!(p, &payloads[i]);
        }
    }
}
