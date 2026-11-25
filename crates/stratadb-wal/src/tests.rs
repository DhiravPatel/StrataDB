use super::*;
use proptest::prelude::*;
use stratadb_common::ErrorKind;

fn opts(d: Durability) -> WalOptions {
    WalOptions {
        durability: d,
        segment_max_bytes: 4096,
        max_record_bytes: 1 << 20,
        recovery: RecoveryPolicy::TruncateTornTail,
        keys: None,
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
        pos += format::parse_frame(&full[pos..], format::FORMAT_VERSION)
            .unwrap()
            .encoded_len();
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

/// Power loss with out-of-order persistence (WAL v2): an un-synced frame is
/// lost while a later un-synced frame survives. Recovery must truncate (no
/// acknowledged record can be affected), not fail closed.
#[test]
fn reordered_unsynced_tail_is_truncated() {
    let dir = tempfile::tempdir().unwrap();
    let (wal, _, _) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    for i in 1..=3 {
        wal.append(RecordType::EventBatch, &payload(i)).unwrap(); // synced
    }
    drop(wal);
    // Simulate two more async appends that were never fsynced.
    let mut o = opts(Durability::Async);
    o.segment_max_bytes = 1 << 20;
    let (wal, _, _) = collect(dir.path(), o.clone(), 0).unwrap();
    wal.append(RecordType::EventBatch, &payload(4)).unwrap();
    wal.append(RecordType::EventBatch, &payload(5)).unwrap();
    std::mem::forget(wal); // no close-time sync
    let seg = last_segment(dir.path());
    let mut data = fs::read(&seg).unwrap();
    // Locate frame 4 and zero it (lost sector); frame 5 survives.
    let mut pos = HEADER_LEN;
    for _ in 1..=3 {
        pos += format::parse_frame(&data[pos..], 2).unwrap().encoded_len();
    }
    let f4 = format::parse_frame(&data[pos..], 2).unwrap();
    assert_eq!((f4.seq, f4.synced_through), (4, Some(3)));
    let len4 = f4.encoded_len();
    data[pos..pos + len4].fill(0);
    fs::write(&seg, &data).unwrap();

    let (_, recs, rep) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    assert_eq!(recs.iter().map(|r| r.0).collect::<Vec<_>>(), vec![1, 2, 3]);
    let t = rep.truncation.unwrap();
    assert_eq!(t.kind, format::InvalidTail::UnsyncedReordered);
    assert!(t.discarded_valid_frames);
}

/// A v1 segment written by an older release is replayed, never appended to:
/// new records go to a fresh v2 segment; damage followed by valid frames in a
/// v1 segment still fails closed (v1 cannot prove the frames were un-synced).
#[test]
fn v1_segments_are_read_and_not_appended_to() {
    let dir = tempfile::tempdir().unwrap();
    let v1 = |frames: &[(u64, &[u8])]| {
        let mut b = format::encode_header(&format::SegmentHeader {
            base_seq: 1,
            created_ms: 0,
            version: 1,
            encryption: None,
        });
        for (seq, p) in frames {
            b.extend(format::encode_frame_v1(RecordType::EventBatch, *seq, p));
        }
        b
    };
    let seg = dir.path().join(format::segment_file_name(1));
    fs::write(&seg, v1(&[(1, b"a"), (2, b"b")])).unwrap();
    let (wal, recs, _) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    assert_eq!(recs, vec![(1, b"a".to_vec()), (2, b"b".to_vec())]);
    assert_eq!(wal.append(RecordType::EventBatch, b"c").unwrap(), 3);
    drop(wal);
    let files = segment_files(dir.path());
    assert_eq!(files.len(), 2, "{files:?}");
    let new = fs::read(&files[1]).unwrap();
    assert_eq!(format::decode_header(&new, "t").unwrap().version, 2);
    let (_, recs, _) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    assert_eq!(recs.len(), 3);

    let dir = tempfile::tempdir().unwrap();
    let mut data = v1(&[(1, b"a"), (2, b"b"), (3, b"c")]);
    data[HEADER_LEN + 17] ^= 0xff; // damage record 1's payload
    fs::write(dir.path().join(format::segment_file_name(1)), &data).unwrap();
    let err = collect(dir.path(), opts(Durability::Sync), 0).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Corruption);
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/wal-v2")
}

fn fixture_payload(i: u64) -> Vec<u8> {
    format!("wal-v2 fixture record {i}").into_bytes()
}

/// Golden WAL v2 directory: one segment, records 1..=4 with `synced_through`
/// 0, 1, 2, 3 (written under `sync`). Never regenerate to fix a failure.
#[test]
fn reads_wal_v2_fixture() {
    let src = fixture_dir();
    let dir = tempfile::tempdir().unwrap();
    for f in fs::read_dir(&src).unwrap() {
        let f = f.unwrap();
        fs::copy(f.path(), dir.path().join(f.file_name())).unwrap();
    }
    let seg = dir.path().join(format::segment_file_name(1));
    let data = fs::read(&seg).unwrap();
    let h = format::decode_header(&data, "fixture").unwrap();
    assert_eq!((h.base_seq, h.version), (1, 2));
    let mut pos = HEADER_LEN;
    for i in 1..=4u64 {
        let f = format::parse_frame(&data[pos..], 2).unwrap();
        assert_eq!((f.seq, f.synced_through), (i, Some(i - 1)));
        assert_eq!(f.payload, fixture_payload(i).as_slice());
        pos += f.encoded_len();
    }
    assert_eq!(pos, data.len());
    let (_, recs, rep) = collect(dir.path(), opts(Durability::Sync), 0).unwrap();
    assert_eq!(
        recs,
        (1..=4).map(|i| (i, fixture_payload(i))).collect::<Vec<_>>()
    );
    assert!(rep.truncation.is_none());
}

/// One-off generator for `tests/fixtures/wal-v2` (refuses to overwrite).
#[test]
#[ignore = "writes tests/fixtures/wal-v2; run once when introducing the format"]
fn generate_wal_v2_fixture() {
    let out = fixture_dir();
    assert!(!out.exists(), "fixture exists; never regenerate it");
    let tmp = tempfile::tempdir().unwrap();
    let (wal, _, _) = collect(tmp.path(), opts(Durability::Sync), 0).unwrap();
    for i in 1..=4 {
        wal.append(RecordType::EventBatch, &fixture_payload(i))
            .unwrap();
    }
    drop(wal);
    fs::create_dir_all(&out).unwrap();
    fs::copy(
        tmp.path().join(format::segment_file_name(1)),
        out.join(format::segment_file_name(1)),
    )
    .unwrap();
}

fn keyring(dir: &Path) -> Arc<stratadb_crypto::Keyring> {
    let p = stratadb_crypto::StaticKeyProvider(stratadb_crypto::SecretKey::from_bytes([4; 32]));
    Arc::new(stratadb_crypto::Keyring::create(dir, &p).unwrap())
}

fn enc_opts(keys: &Arc<stratadb_crypto::Keyring>) -> WalOptions {
    WalOptions {
        keys: Some(Arc::clone(keys)),
        ..opts(Durability::Sync)
    }
}

/// Encrypted WAL: payloads never appear in clear, replay decrypts them, every
/// process starts a fresh segment (fresh frame key), and a missing key or a
/// tampered ciphertext fails closed.
#[test]
fn encrypted_wal_round_trip() {
    let kdir = tempfile::tempdir().unwrap();
    let keys = keyring(kdir.path());
    let dir = tempfile::tempdir().unwrap();
    for round in 0..3u64 {
        let (wal, recs, _) = collect(dir.path(), enc_opts(&keys), 0).unwrap();
        assert_eq!(recs.len() as u64, round * 5);
        for (i, (seq, p)) in recs.iter().enumerate() {
            assert_eq!((*seq, p.clone()), (i as u64 + 1, payload(i as u64 + 1)));
        }
        for i in 1..=5 {
            wal.append(RecordType::EventBatch, &payload(round * 5 + i))
                .unwrap();
        }
    }
    let files = segment_files(dir.path());
    assert_eq!(files.len(), 3, "one new segment per process: {files:?}");
    for f in &files {
        let data = fs::read(f).unwrap();
        let h = format::decode_header(&data, "t").unwrap();
        assert!(h.encryption.is_some());
        assert!(
            !data.windows(9).any(|w| w == b"record-1-"),
            "plaintext payload on disk"
        );
    }
    // No key: refuse to open (never treat ciphertext as data).
    let err = collect(dir.path(), opts(Durability::Sync), 0).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");
    // Another keyring (other DEKs): the DEK id may exist but authentication fails.
    let other_dir = tempfile::tempdir().unwrap();
    let other = keyring(other_dir.path());
    let err = collect(dir.path(), enc_opts(&other), 0).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Corruption, "{err}");
    // Tampering with a ciphertext byte (and fixing up the CRC) is detected.
    let seg = &files[0];
    let mut data = fs::read(seg).unwrap();
    let hl = format::decode_header(&data, "t").unwrap().len();
    let f = format::parse_frame(&data[hl..], 2).unwrap();
    let (plen, st) = (f.payload.len(), f.synced_through.unwrap());
    data[hl + FRAME_HEADER_LEN] ^= 1;
    let crc = crc32c(&data[hl + FRAME_HEADER_LEN..hl + FRAME_HEADER_LEN + plen]);
    let mut frame = data[hl..hl + FRAME_HEADER_LEN + plen].to_vec();
    format::encode_frame_into(&mut frame, RecordType::EventBatch, 1, st, plen as u32, crc);
    data[hl..hl + FRAME_HEADER_LEN].copy_from_slice(&frame[..FRAME_HEADER_LEN]);
    fs::write(seg, &data).unwrap();
    let err = collect(dir.path(), enc_opts(&keys), 0).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::Corruption, "{err}");
}

/// The reordered-un-synced-tail rule (WAL v2) works on encrypted segments.
#[test]
fn encrypted_reordered_tail_is_truncated() {
    let kdir = tempfile::tempdir().unwrap();
    let keys = keyring(kdir.path());
    let dir = tempfile::tempdir().unwrap();
    let mut o = enc_opts(&keys);
    o.durability = Durability::Async;
    o.segment_max_bytes = 1 << 20;
    let (wal, _, _) = collect(dir.path(), o, 0).unwrap();
    for i in 1..=4 {
        wal.append(RecordType::EventBatch, &payload(i)).unwrap();
    }
    wal.sync().unwrap();
    wal.append(RecordType::EventBatch, &payload(5)).unwrap();
    wal.append(RecordType::EventBatch, &payload(6)).unwrap();
    std::mem::forget(wal);
    let seg = last_segment(dir.path());
    let mut data = fs::read(&seg).unwrap();
    let mut pos = format::decode_header(&data, "t").unwrap().len();
    for _ in 1..=4 {
        pos += format::parse_frame(&data[pos..], 2).unwrap().encoded_len();
    }
    let n5 = format::parse_frame(&data[pos..], 2).unwrap().encoded_len();
    data[pos..pos + n5].fill(0); // frame 5 lost, frame 6 survived
    fs::write(&seg, &data).unwrap();
    let (_, recs, rep) = collect(dir.path(), enc_opts(&keys), 0).unwrap();
    assert_eq!(
        recs.iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        rep.truncation.unwrap().kind,
        format::InvalidTail::UnsyncedReordered
    );
}
