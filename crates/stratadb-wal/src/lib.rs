//! StrataDB write-ahead log (spec §8, epic C).
//!
//! # Guarantees
//! * A record is *acknowledged* only when [`Wal::append`] returns `Ok`. What
//!   that means depends on [`Durability`]:
//!   * `Sync` / `GroupCommit`: the frame has been written **and** covered by a
//!     successful `sync_data` (F_FULLFSYNC on Apple platforms). Survives process
//!     crash, OS crash and power loss (assuming honest storage hardware).
//!   * `Async`: the frame has been written to the OS page cache. Survives a
//!     process crash; may be lost on OS crash / power loss until the next
//!     [`Wal::sync`].
//! * Sequence numbers are dense and strictly increasing; each frame is
//!   atomic (checksum covers length, type, sequence and payload).
//! * After any write or sync failure the WAL is *poisoned*: every later
//!   operation fails. The page-cache state after an fsync error is unknown, so
//!   retrying could falsely report durability ("fsyncgate"). Restart and
//!   recovery are required.
//! * Recovery is deterministic and idempotent: it never modifies records
//!   except truncating an invalid tail of the *last* segment, and only when no
//!   checksum-valid frame follows the damage (otherwise it fails closed).
//! * Segments are deleted only through [`Wal::reclaim_through`], oldest first,
//!   never the active segment.

pub mod format;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU64, Ordering};
use stratadb_common::checksum::crc32c;
use stratadb_common::io::{read_file, write_all_at, IoEngine, WriteFile};
use stratadb_common::{failpoint, fsutil, time, Error, IoResultExt, Result};

pub use format::RecordType;
use format::{InvalidTail, FORMAT_VERSION, FRAME_HEADER_LEN, HEADER_LEN};

/// When an append is acknowledged relative to fsync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Acknowledge after write(2); fsync only on rotation, explicit `sync`, and close.
    Async,
    /// Acknowledge after the record is fsynced. Concurrent appenders share syncs.
    Sync,
    /// Like `Sync`, but the sync leader may wait to gather more records into
    /// one fsync. The wait is adaptive (spec STRATA-SYS-07-004; see
    /// [`WalOptions::adaptive_group_delay`]) and never longer than
    /// `max_delay`. The added latency is bounded and throughput is higher.
    GroupCommit { max_delay: Duration },
}

/// How recovery treats an invalid frame in the final segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPolicy {
    /// Truncate the invalid tail only if no checksum-valid frame follows it;
    /// otherwise fail with `Corruption` (possible mid-log damage).
    TruncateTornTail,
    /// Operator override: truncate at the first invalid frame even if valid
    /// frames follow. May discard acknowledged records; reported explicitly.
    ForceTruncate,
}

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub durability: Durability,
    /// Rotate to a new segment once the active one would exceed this size.
    pub segment_max_bytes: u64,
    /// Largest payload accepted by `append`.
    pub max_record_bytes: u32,
    pub recovery: RecoveryPolicy,
    /// Encrypt frame payloads of new segments (ADR-0024). Required to read
    /// encrypted segments. When set, a segment is never appended to by a
    /// later process (each segment's frame key sees every nonce at most once).
    pub keys: Option<Arc<stratadb_crypto::Keyring>>,
    /// Group commit (spec STRATA-SYS-07-004).
    /// * `true`: the leader does not wait when fewer than one more record is
    ///   expected during an fsync (the arrival-rate estimate times the fsync
    ///   latency estimate is below 1). Otherwise it waits at most
    ///   `min(fsync latency, max_delay)`, stopping early once as many records
    ///   are pending as the previous group had or `group_bytes` are pending.
    /// * `false`: always wait the full `max_delay` (the old behaviour; for
    ///   A/B comparisons).
    pub adaptive_group_delay: bool,
    /// Flush a group early once this many bytes are pending (1 MiB).
    pub group_bytes: u64,
    /// File I/O (spec STRATA-SYS-04-001): appends, fsyncs, truncation and
    /// replay reads. `None`: the process-wide default engine.
    pub io: Option<Arc<dyn IoEngine>>,
    /// Segment format written: 3 (default; columnar record types) or 2 (no
    /// record types newer than 2, readable by the previous release; for a
    /// rollback window after upgrading). Reading accepts 1..=3 regardless.
    pub format_version: u16,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            durability: Durability::GroupCommit {
                max_delay: Duration::from_millis(2),
            },
            segment_max_bytes: 64 * 1024 * 1024,
            max_record_bytes: 32 * 1024 * 1024,
            recovery: RecoveryPolicy::TruncateTornTail,
            keys: None,
            adaptive_group_delay: true,
            group_bytes: 1 << 20,
            io: None,
            format_version: FORMAT_VERSION,
        }
    }
}

/// Exponentially weighted moving average weight of a new sample.
const EWMA_ALPHA: f64 = 0.2;

/// Group-commit estimators (all in nanoseconds), updated under the WAL lock.
#[derive(Debug, Default)]
struct GroupStats {
    /// EWMA of the time between consecutive appends.
    interarrival_ns: f64,
    last_append: Option<Instant>,
    /// EWMA of fsync latency.
    sync_ns: f64,
    /// Records made durable by the previous group.
    last_group: u64,
    /// Bytes written since the current group's fsync started.
    pending_bytes: u64,
    /// The leader is waiting to gather a group.
    gathering: bool,
}

fn ewma(old: f64, sample: f64) -> f64 {
    if old == 0.0 {
        sample
    } else {
        old + EWMA_ALPHA * (sample - old)
    }
}

/// Details of a tail truncation performed during recovery.
#[derive(Debug, Clone)]
pub struct Truncation {
    pub segment: PathBuf,
    pub offset: u64,
    pub bytes_discarded: u64,
    pub reason: String,
    /// Valid frames existed after the damage and were discarded: either
    /// provably un-synced (power loss reordered the writes; see
    /// `format::InvalidTail::UnsyncedReordered`) or under `ForceTruncate`.
    pub discarded_valid_frames: bool,
    /// How the damaged tail was classified.
    pub kind: format::InvalidTail,
}

/// Summary of a recovery scan.
#[derive(Debug, Clone, Default)]
pub struct RecoveryReport {
    pub segments_scanned: usize,
    pub records_replayed: u64,
    pub records_skipped: u64,
    /// Highest sequence number present in the WAL after recovery (0 if none).
    pub last_seq: u64,
    pub truncation: Option<Truncation>,
    pub removed_tmp_files: usize,
}

/// Point-in-time WAL statistics.
#[derive(Debug, Clone)]
pub struct WalStats {
    pub segments: usize,
    pub active_segment_bytes: u64,
    pub last_seq: u64,
    pub durable_seq: u64,
    pub poisoned: Option<String>,
    /// Frame bytes (headers + payloads) appended since open.
    pub bytes_appended: u64,
    /// Records appended since open.
    pub records_appended: u64,
}

struct Inner {
    file: Arc<dyn WriteFile>,
    path: PathBuf,
    base_seq: u64,
    len: u64,
    /// Bytes before the first frame of the active segment.
    header_len: u64,
    /// Frame key of the active segment (encrypted WAL).
    frame_key: Option<stratadb_crypto::FrameKey>,
    /// Closed (non-active) segments, oldest first: (base_seq, path).
    closed: Vec<(u64, PathBuf)>,
    next_seq: u64,
    written_seq: u64,
    durable_seq: u64,
    syncing: bool,
    poisoned: Option<String>,
    group: GroupStats,
    /// Frame bytes and records appended by this process.
    bytes_appended: u64,
    records_appended: u64,
}

/// The write-ahead log. Thread-safe; share via `Arc`.
pub struct Wal {
    dir: PathBuf,
    io: Arc<dyn IoEngine>,
    opts: WalOptions,
    inner: Mutex<Inner>,
    /// Signals a completed sync (spec STRATA-SYS-07-006). parking_lot's
    /// `notify_all` costs the sync leader one wake-up per group: the other
    /// waiters are requeued onto the WAL mutex and handed off in user space
    /// as each one releases it (ADR-0032 measures the alternatives).
    cv: Condvar,
    /// When the last durable sequence was published (ns since `born`), for
    /// `commit_wake_seconds`.
    published_ns: AtomicU64,
    born: Instant,
    /// Wakes a gathering sync leader when a record is appended (so it can
    /// stop waiting once its group is complete).
    gather_cv: Condvar,
    /// fsync latency of the active segment (group commits and rotations).
    sync_seconds: stratadb_common::metrics::Histogram,
    /// Records made durable by one fsync (group-commit size, spec §8.4).
    group_size: stratadb_common::metrics::Histogram,
    /// INSERT stage `wal_append` (spec VIII.1.2): frame encode + checksum
    /// (+ seal) + write, including waiting for the WAL lock.
    append_seconds: stratadb_common::metrics::Histogram,
    /// INSERT stage `group_commit_wait`: from written to durable (sync modes).
    commit_wait_seconds: stratadb_common::metrics::Histogram,
    /// Time the sync leader waited to gather a group (adaptive delay).
    group_delay_seconds: stratadb_common::metrics::Histogram,
    /// From a group's durability being published to a covered writer
    /// returning (the wake-up cost, STRATA-SYS-07-006).
    commit_wake_seconds: stratadb_common::metrics::Histogram,
}

/// WAL latency/size distributions.
#[derive(Debug, Clone)]
pub struct WalMetrics {
    pub sync_seconds: stratadb_common::metrics::HistogramSnapshot,
    pub group_size: stratadb_common::metrics::HistogramSnapshot,
    pub append_seconds: stratadb_common::metrics::HistogramSnapshot,
    pub commit_wait_seconds: stratadb_common::metrics::HistogramSnapshot,
    pub group_delay_seconds: stratadb_common::metrics::HistogramSnapshot,
    /// Group durability published → a covered writer returning.
    pub commit_wake_seconds: stratadb_common::metrics::HistogramSnapshot,
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

/// A newly created active segment.
struct NewSegment {
    file: Arc<dyn WriteFile>,
    path: PathBuf,
    header_len: u64,
    frame_key: Option<stratadb_crypto::FrameKey>,
}

/// Frame key of a segment from its header (keys required if encrypted).
fn segment_frame_key(
    h: &format::SegmentHeader,
    keys: Option<&stratadb_crypto::Keyring>,
    loc: &str,
) -> Result<Option<stratadb_crypto::FrameKey>> {
    let Some(e) = &h.encryption else {
        return Ok(None);
    };
    let keys = keys.ok_or_else(|| {
        Error::new(
            stratadb_common::ErrorKind::PermissionDenied,
            format!("{loc} is encrypted; configure the data directory's key"),
        )
    })?;
    Ok(Some(stratadb_crypto::FrameKey::new(
        keys.dek(e.dek_id, loc)?,
        &e.salt,
    )?))
}

fn create_segment(
    io: &dyn IoEngine,
    dir: &Path,
    base_seq: u64,
    keys: Option<&stratadb_crypto::Keyring>,
    version: u16,
) -> Result<NewSegment> {
    let path = dir.join(format::segment_file_name(base_seq));
    let tmp = fsutil::tmp_path(&path);
    fsutil::remove_if_exists(&tmp)?;
    let f = io.create_new(&tmp)?;
    let encryption = match keys {
        Some(k) => {
            let mut salt = [0u8; 32];
            stratadb_crypto::random(&mut salt)?;
            Some(format::EncryptionInfo {
                dek_id: k.active().0,
                salt,
            })
        }
        None => None,
    };
    let h = format::SegmentHeader {
        base_seq,
        created_ms: time::now_unix_millis(),
        version,
        encryption,
    };
    let frame_key = segment_frame_key(&h, keys, &path.display().to_string())?;
    let header = format::encode_header(&h);
    write_all_at(&*f, &header, 0)?;
    f.sync(false)?;
    drop(f);
    fs::rename(&tmp, &path).ctx(|| format!("renaming {}", tmp.display()))?;
    fsutil::fsync_dir(dir)?;
    failpoint::hit("wal::rotate::after_create")?;
    let file = io.open_write(&path)?;
    Ok(NewSegment {
        file,
        path,
        header_len: header.len() as u64,
        frame_key,
    })
}

impl Wal {
    /// Open (creating if needed) the WAL in `dir` and replay every record with
    /// `seq > replay_after` through `on_record`, in sequence order.
    ///
    /// `replay_after` is the highest sequence number already durable in
    /// committed segments (from the manifest). Records at or below it are
    /// skipped; a missing WAL range is an error unless it lies entirely at or
    /// below `replay_after`.
    pub fn open<F>(
        dir: &Path,
        opts: WalOptions,
        replay_after: u64,
        mut on_record: F,
    ) -> Result<(Self, RecoveryReport)>
    where
        F: FnMut(u64, RecordType, &[u8]) -> Result<()>,
    {
        if !(format::MIN_WRITE_VERSION..=FORMAT_VERSION).contains(&opts.format_version) {
            return Err(Error::invalid_input(format!(
                "WAL format_version must be within {}..={FORMAT_VERSION}",
                format::MIN_WRITE_VERSION
            )));
        }
        let io: Arc<dyn IoEngine> = opts
            .io
            .clone()
            .unwrap_or_else(stratadb_common::io::default_io);
        if opts.max_record_bytes > format::MAX_RECORD_BYTES {
            return Err(Error::invalid_input(
                "max_record_bytes exceeds WAL format limit",
            ));
        }
        fsutil::create_dir_durable(dir)?;
        let mut report = RecoveryReport::default();
        let mut segs: Vec<(u64, PathBuf)> = Vec::new();
        for path in fsutil::list_files(dir)? {
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.ends_with(".tmp") {
                fsutil::remove_if_exists(&path)?;
                report.removed_tmp_files += 1;
            } else if let Some(base) = format::parse_segment_file_name(name) {
                segs.push((base, path));
            } else {
                tracing::warn!(file = %path.display(), "ignoring unexpected file in WAL directory");
            }
        }
        if report.removed_tmp_files > 0 {
            fsutil::fsync_dir(dir)?;
        }
        segs.sort();

        // Continuity rules between segments (docs/storage-format/wal-v1.md §Recovery):
        // * a segment may not start before the previous segment's next sequence (overlap);
        // * sequences missing before a segment's base are allowed only if they are
        //   all <= replay_after (already durable in committed segments).
        let mut last_seq: u64 = replay_after;
        let mut last_version = FORMAT_VERSION;
        let mut last_encrypted = false;
        // Next sequence after the previous segment's final valid frame.
        let mut prev_next: Option<u64> = None;
        let n = segs.len();
        for (i, (base, path)) in segs.iter().enumerate() {
            let is_last = i + 1 == n;
            let loc = path.display().to_string();
            if *base == 0 {
                return Err(Error::corruption(&loc, "WAL segment base_seq 0 is invalid"));
            }
            if let Some(pn) = prev_next {
                if *base < pn {
                    return Err(Error::corruption(
                        &loc,
                        format!(
                            "WAL overlap: segment starts at {base}, previous ends at {}",
                            pn - 1
                        ),
                    ));
                }
            }
            let gap = prev_next.is_none_or(|pn| *base > pn);
            if gap && base - 1 > replay_after {
                return Err(Error::corruption(
                    &loc,
                    format!(
                        "WAL gap: records before {base} are missing but the manifest \
                         only covers through {replay_after}"
                    ),
                ));
            }
            let data = read_file(&*io, path)?;
            let header = format::decode_header(&data, &loc)?;
            if header.base_seq != *base {
                return Err(Error::corruption(
                    &loc,
                    format!("header base_seq {} != file name {base}", header.base_seq),
                ));
            }
            report.segments_scanned += 1;
            last_version = header.version;
            last_encrypted = header.encryption.is_some();
            let frame_key = segment_frame_key(&header, opts.keys.as_deref(), &loc)?;
            let mut pos = header.len();
            let mut next = *base;
            while pos < data.len() {
                match format::parse_frame(&data[pos..], header.version) {
                    Ok(frame) => {
                        if frame.seq != next {
                            return Err(Error::corruption(
                                &loc,
                                format!(
                                    "sequence discontinuity at offset {pos}: found {}, expected {next}",
                                    frame.seq
                                ),
                            ));
                        }
                        let rtype = RecordType::from_u8(frame.record_type).ok_or_else(|| {
                            Error::unsupported(format!(
                                "{loc}: unknown WAL record type {} at seq {}",
                                frame.record_type, frame.seq
                            ))
                        })?;
                        // A checksum-valid frame whose type its segment's
                        // version cannot hold was not written by StrataDB.
                        if rtype.min_version() > header.version {
                            return Err(Error::corruption(
                                &loc,
                                format!(
                                    "record type {} at seq {} in a v{} segment",
                                    frame.record_type, frame.seq, header.version
                                ),
                            ));
                        }
                        if frame.seq > replay_after {
                            match &frame_key {
                                Some(k) => {
                                    let aad = format::frame_aad(
                                        frame.record_type,
                                        frame.seq,
                                        frame.synced_through.unwrap_or(0),
                                    );
                                    let mut ct = frame.payload.to_vec();
                                    let plain = k.open(frame.seq, &aad, &mut ct, &loc)?;
                                    on_record(frame.seq, rtype, plain)?;
                                }
                                None => on_record(frame.seq, rtype, frame.payload)?,
                            }
                            report.records_replayed += 1;
                        } else {
                            report.records_skipped += 1;
                        }
                        last_seq = last_seq.max(frame.seq);
                        pos += frame.encoded_len();
                        next += 1;
                    }
                    Err(reason) => {
                        if !is_last {
                            return Err(Error::corruption(
                                &loc,
                                format!("{reason} at offset {pos} in a non-final WAL segment"),
                            ));
                        }
                        let kind =
                            format::classify_invalid_tail(&data[pos + 1..], next, header.version);
                        if kind == InvalidTail::PossiblyDurableDamage
                            && opts.recovery != RecoveryPolicy::ForceTruncate
                        {
                            return Err(Error::corruption(
                                &loc,
                                format!(
                                    "{reason} at offset {pos} (expected seq {next}) is followed by \
                                     valid frames written after it was synced; refusing to \
                                     truncate (possible damage to acknowledged records). \
                                     Inspect the WAL; recovery policy ForceTruncate discards them."
                                ),
                            ));
                        }
                        let f = io.open_write(path)?;
                        f.set_len(pos as u64)?;
                        f.sync(false)?;
                        tracing::warn!(segment = %loc, offset = pos, bytes = data.len() - pos,
                            %reason, "truncated invalid WAL tail");
                        report.truncation = Some(Truncation {
                            segment: path.clone(),
                            offset: pos as u64,
                            bytes_discarded: (data.len() - pos) as u64,
                            reason: reason.to_string(),
                            discarded_valid_frames: kind != InvalidTail::Torn,
                            kind,
                        });
                        break;
                    }
                }
            }
            prev_next = Some(next);
        }
        report.last_seq = last_seq;

        let next_seq = last_seq + 1;
        let mut frame_key_new = None;
        let (file, path, base_seq, len, closed, header_len) = match segs.last() {
            // Reuse the last segment only if appending `next_seq` keeps it
            // contiguous and it has the current format (frames of one segment
            // share one layout; older segments are never appended to). With
            // encryption a new segment (new frame key) is always started, so
            // sequence numbers reused after a truncated tail never reuse a nonce.
            Some((base, path))
                if prev_next == Some(next_seq)
                    && last_version == opts.format_version
                    && !last_encrypted
                    && opts.keys.is_none() =>
            {
                let f = io.open_write(path)?;
                // Make recovered (possibly unsynced) bytes durable before new appends.
                f.sync(false)?;
                let len = f.len()?;
                let closed = segs[..segs.len() - 1].to_vec();
                (f, path.clone(), *base, len, closed, HEADER_LEN as u64)
            }
            _ => {
                // No WAL yet, the WAL ends before the manifest (e.g. Async
                // tail lost on power failure after a flush), or encryption:
                // start a new segment so sequence numbers never repeat in it.
                let n = create_segment(
                    &*io,
                    dir,
                    next_seq,
                    opts.keys.as_deref(),
                    opts.format_version,
                )?;
                frame_key_new = n.frame_key;
                (
                    n.file,
                    n.path,
                    next_seq,
                    n.header_len,
                    segs.clone(),
                    n.header_len,
                )
            }
        };

        let wal = Wal {
            dir: dir.to_path_buf(),
            io,
            opts,
            inner: Mutex::new(Inner {
                file,
                path,
                base_seq,
                len,
                header_len,
                frame_key: frame_key_new,
                closed,
                next_seq,
                written_seq: last_seq,
                durable_seq: last_seq,
                syncing: false,
                poisoned: None,
                group: GroupStats::default(),
                bytes_appended: 0,
                records_appended: 0,
            }),
            cv: Condvar::new(),
            published_ns: AtomicU64::new(0),
            born: Instant::now(),
            gather_cv: Condvar::new(),
            sync_seconds: stratadb_common::metrics::Histogram::latency(),
            group_size: stratadb_common::metrics::Histogram::sizes(),
            append_seconds: stratadb_common::metrics::Histogram::stage(),
            commit_wait_seconds: stratadb_common::metrics::Histogram::stage(),
            group_delay_seconds: stratadb_common::metrics::Histogram::stage(),
            commit_wake_seconds: stratadb_common::metrics::Histogram::stage(),
        };
        Ok((wal, report))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// fsync latency and group-commit size distributions since open.
    pub fn metrics(&self) -> WalMetrics {
        WalMetrics {
            sync_seconds: self.sync_seconds.snapshot(),
            group_size: self.group_size.snapshot(),
            append_seconds: self.append_seconds.snapshot(),
            commit_wait_seconds: self.commit_wait_seconds.snapshot(),
            group_delay_seconds: self.group_delay_seconds.snapshot(),
            commit_wake_seconds: self.commit_wake_seconds.snapshot(),
        }
    }

    pub fn options(&self) -> &WalOptions {
        &self.opts
    }

    fn check_poisoned(g: &Inner) -> Result<()> {
        match &g.poisoned {
            Some(why) => Err(Error::unavailable(format!(
                "WAL is failed and requires restart/recovery: {why}"
            ))),
            None => Ok(()),
        }
    }

    fn poison(g: &mut Inner, why: String) {
        tracing::error!(reason = %why, "WAL poisoned");
        if g.poisoned.is_none() {
            g.poisoned = Some(why);
        }
    }

    /// Append one record and wait for the configured durability boundary.
    /// Returns its sequence number.
    ///
    /// Errors:
    /// * `InvalidInput` — payload too large (nothing written).
    /// * `Unavailable` — WAL already poisoned (nothing written).
    /// * `Indeterminate` — a write or sync failed after the frame may have
    ///   reached the file; the record may or may not survive recovery.
    pub fn append(&self, record_type: RecordType, payload: &[u8]) -> Result<u64> {
        let started = Instant::now();
        if record_type.min_version() > self.opts.format_version {
            return Err(Error::invalid_input(format!(
                "WAL record type {record_type:?} needs format v{}; this WAL writes v{}",
                record_type.min_version(),
                self.opts.format_version
            )));
        }
        if payload.len() > self.opts.max_record_bytes as usize {
            return Err(Error::invalid_input(format!(
                "WAL record of {} bytes exceeds limit {}",
                payload.len(),
                self.opts.max_record_bytes
            )));
        }
        let payload_crc = crc32c(payload);
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
        frame.resize(FRAME_HEADER_LEN, 0);
        frame.extend_from_slice(payload);

        let mut g = self.inner.lock();
        Self::check_poisoned(&g)?;
        if g.len > g.header_len && g.len + frame.len() as u64 > self.opts.segment_max_bytes {
            // One fsync in flight per WAL (spec STRATA-SYS-07-005): rotation
            // syncs the old segment, so wait for a leader's sync to finish.
            // No new leader can start while this holds the mutex.
            while g.syncing {
                self.cv.wait(&mut g);
            }
            Self::check_poisoned(&g)?;
            self.rotate_locked(&mut g)?;
        }
        let seq = g.next_seq;
        // Everything <= durable_seq was fsynced before this frame is written.
        let synced = g.durable_seq;
        match &g.frame_key {
            Some(k) => {
                // Nonce = seq under this segment's key; sealed once.
                let aad = format::frame_aad(record_type as u8, seq, synced);
                frame.truncate(FRAME_HEADER_LEN);
                let mut ct = payload.to_vec();
                k.seal(seq, &aad, &mut ct)?;
                frame.extend_from_slice(&ct);
                let crc = crc32c(&ct);
                format::encode_frame_into(
                    &mut frame,
                    record_type,
                    seq,
                    synced,
                    ct.len() as u32,
                    crc,
                );
            }
            None => format::encode_frame_into(
                &mut frame,
                record_type,
                seq,
                synced,
                payload.len() as u32,
                payload_crc,
            ),
        }

        failpoint::hit("wal::before_write")?;
        if let Some(action) = failpoint::eval("wal::torn_write") {
            // Simulate a crash in the middle of write(2).
            let _ = write_all_at(&*g.file, &frame[..frame.len() / 2], g.len);
            let _ = g.file.sync(true);
            failpoint::fire("wal::torn_write", action)?;
        }
        if let Err(e) = write_all_at(&*g.file, &frame, g.len) {
            let why = format!("write to {} failed: {e}", g.path.display());
            Self::poison(&mut g, why.clone());
            return Err(Error::indeterminate(why));
        }
        g.len += frame.len() as u64;
        g.bytes_appended += frame.len() as u64;
        g.records_appended += 1;
        g.next_seq += 1;
        g.written_seq = seq;
        {
            let gs = &mut g.group;
            if let Some(prev) = gs.last_append {
                gs.interarrival_ns = ewma(
                    gs.interarrival_ns,
                    started.saturating_duration_since(prev).as_nanos() as f64,
                );
            }
            gs.last_append = Some(started);
            gs.pending_bytes += frame.len() as u64;
            // Let a gathering leader re-check whether its group is complete.
            if gs.gathering {
                self.gather_cv.notify_one();
            }
        }
        if let Err(e) = failpoint::hit("wal::after_write") {
            Self::poison(&mut g, e.to_string());
            return Err(Error::indeterminate(e.to_string()));
        }
        self.append_seconds.observe_duration(started.elapsed());
        match self.opts.durability {
            Durability::Async => Ok(seq),
            Durability::Sync | Durability::GroupCommit { .. } => {
                let waiting = Instant::now();
                let r = self.wait_durable(g, seq).map(|()| seq);
                self.commit_wait_seconds.observe_duration(waiting.elapsed());
                r
            }
        }
    }

    /// How long the sync leader waits to gather a group (it holds `syncing`;
    /// appenders keep writing meanwhile). Returns the time waited.
    fn gather(&self, g: &mut MutexGuard<'_, Inner>, max_delay: Duration) -> Duration {
        if max_delay.is_zero() {
            return Duration::ZERO;
        }
        let start = Instant::now();
        let limit = if self.opts.adaptive_group_delay {
            let gs = &g.group;
            // Expected appends during one fsync; with too few, waiting only
            // adds latency (low arrival rate: flush immediately).
            let expected = if gs.interarrival_ns > 0.0 {
                gs.sync_ns / gs.interarrival_ns
            } else {
                0.0
            };
            if gs.sync_ns == 0.0 || expected < 1.0 {
                return Duration::ZERO;
            }
            max_delay.min(Duration::from_nanos(gs.sync_ns as u64))
        } else {
            max_delay
        };
        let deadline = start + limit;
        // Stop early once the closed-loop writers of the previous group are
        // back, or enough bytes are pending.
        let want = g.durable_seq + g.group.last_group.max(1);
        g.group.gathering = true;
        while Instant::now() < deadline {
            if self.opts.adaptive_group_delay
                && (g.written_seq >= want || g.group.pending_bytes >= self.opts.group_bytes)
            {
                break;
            }
            if self.gather_cv.wait_until(g, deadline).timed_out() {
                break;
            }
        }
        g.group.gathering = false;
        start.elapsed()
    }

    /// Leader/follower group commit: the first waiter that finds no sync in
    /// progress becomes the leader, captures `written_seq` as the durable
    /// target *before* syncing, syncs without holding the lock, then publishes.
    fn wait_durable(&self, mut g: MutexGuard<'_, Inner>, seq: u64) -> Result<()> {
        loop {
            if g.durable_seq >= seq {
                return Ok(());
            }
            if let Some(why) = &g.poisoned {
                return Err(Error::indeterminate(format!(
                    "WAL failed before record {seq} was confirmed durable: {why}"
                )));
            }
            if g.syncing {
                self.cv.wait(&mut g);
                if g.durable_seq >= seq {
                    self.commit_wake_seconds
                        .observe_duration(self.since_published());
                }
                continue;
            }
            g.syncing = true;
            if let Durability::GroupCommit { max_delay } = self.opts.durability {
                let waited = self.gather(&mut g, max_delay);
                self.group_delay_seconds.observe_duration(waited);
            }
            g.group.pending_bytes = 0;
            let target = g.written_seq;
            let records = target.saturating_sub(g.durable_seq);
            let file = Arc::clone(&g.file);
            let started = Instant::now();
            let res = MutexGuard::unlocked(&mut g, || -> Result<()> {
                failpoint::hit("wal::before_sync")?;
                file.sync(true)?;
                failpoint::hit("wal::after_sync")?;
                Ok(())
            });
            g.syncing = false;
            match res {
                Ok(()) => {
                    let took = started.elapsed();
                    self.sync_seconds.observe_duration(took);
                    self.group_size.observe(records as f64);
                    g.group.sync_ns = ewma(g.group.sync_ns, took.as_nanos() as f64);
                    g.group.last_group = records;
                    g.durable_seq = g.durable_seq.max(target);
                }
                Err(e) => Self::poison(&mut g, format!("sync failed: {e}")),
            }
            self.publish();
        }
    }

    /// Wake every writer waiting for a sync (call with the WAL mutex held,
    /// after updating `durable_seq` or poisoning).
    fn publish(&self) {
        self.published_ns.store(
            u64::try_from(self.born.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Release,
        );
        self.cv.notify_all();
    }

    fn since_published(&self) -> Duration {
        let now = u64::try_from(self.born.elapsed().as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(now.saturating_sub(self.published_ns.load(Ordering::Acquire)))
    }

    fn rotate_locked(&self, g: &mut MutexGuard<'_, Inner>) -> Result<()> {
        // The old segment must be fully durable before it becomes immutable.
        let started = Instant::now();
        let records = g.written_seq.saturating_sub(g.durable_seq);
        if let Err(e) = g.file.sync(true) {
            let why = format!("sync before rotation failed: {e}");
            Self::poison(g, why.clone());
            return Err(Error::indeterminate(why));
        }
        self.sync_seconds.observe_duration(started.elapsed());
        if records > 0 {
            self.group_size.observe(records as f64);
        }
        g.durable_seq = g.durable_seq.max(g.written_seq);
        self.publish();
        let base = g.next_seq;
        let n = create_segment(
            &*self.io,
            &self.dir,
            base,
            self.opts.keys.as_deref(),
            self.opts.format_version,
        )
        .inspect_err(|e| {
            Self::poison(g, format!("segment rotation failed: {e}"));
        })?;
        let old_base = g.base_seq;
        let old_path = std::mem::replace(&mut g.path, n.path);
        g.closed.push((old_base, old_path));
        g.file = n.file;
        g.base_seq = base;
        g.len = n.header_len;
        g.header_len = n.header_len;
        g.frame_key = n.frame_key;
        tracing::debug!(base_seq = base, "rotated WAL segment");
        Ok(())
    }

    /// Make every written record durable (used by `Async` mode tickers and shutdown).
    pub fn sync(&self) -> Result<()> {
        let g = self.inner.lock();
        Self::check_poisoned(&g)?;
        let target = g.written_seq;
        if g.durable_seq >= target {
            return Ok(());
        }
        self.wait_durable(g, target)
    }

    /// Delete closed segments whose records all have `seq <= flushed_through`.
    ///
    /// The caller must guarantee those records are durable in committed
    /// segments referenced by a durable manifest. Segments are removed oldest
    /// first so a crash mid-way never leaves a gap. The active segment is
    /// never removed.
    pub fn reclaim_through(&self, flushed_through: u64) -> Result<usize> {
        let victims: Vec<PathBuf> = {
            let mut g = self.inner.lock();
            Self::check_poisoned(&g)?;
            let mut count = 0;
            for i in 0..g.closed.len() {
                let next_base = g.closed.get(i + 1).map_or(g.base_seq, |(b, _)| *b);
                if next_base.saturating_sub(1) <= flushed_through {
                    count += 1;
                } else {
                    break;
                }
            }
            g.closed.drain(..count).map(|(_, p)| p).collect()
        };
        for p in &victims {
            fsutil::remove_if_exists(p)?;
            failpoint::hit("wal::reclaim::after_delete")?;
        }
        if !victims.is_empty() {
            fsutil::fsync_dir(&self.dir)?;
            tracing::debug!(
                count = victims.len(),
                flushed_through,
                "reclaimed WAL segments"
            );
        }
        Ok(victims.len())
    }

    /// Highest sequence number written (acknowledged or not yet synced).
    pub fn last_seq(&self) -> u64 {
        self.inner.lock().written_seq
    }

    pub fn durable_seq(&self) -> u64 {
        self.inner.lock().durable_seq
    }

    pub fn is_poisoned(&self) -> bool {
        self.inner.lock().poisoned.is_some()
    }

    pub fn stats(&self) -> WalStats {
        let g = self.inner.lock();
        WalStats {
            segments: g.closed.len() + 1,
            active_segment_bytes: g.len,
            last_seq: g.written_seq,
            durable_seq: g.durable_seq,
            poisoned: g.poisoned.clone(),
            bytes_appended: g.bytes_appended,
            records_appended: g.records_appended,
        }
    }
}

#[cfg(test)]
mod tests;
