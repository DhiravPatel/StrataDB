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

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, MutexGuard};
use stratadb_common::checksum::crc32c;
use stratadb_common::{failpoint, fsutil, time, Error, IoResultExt, Result};

pub use format::RecordType;
use format::{FRAME_HEADER_LEN, HEADER_LEN};

/// When an append is acknowledged relative to fsync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Acknowledge after write(2); fsync only on rotation, explicit `sync`, and close.
    Async,
    /// Acknowledge after the record is fsynced. Concurrent appenders share syncs.
    Sync,
    /// Like `Sync`, but the sync leader waits up to `max_delay` to gather more
    /// records into one fsync. Bounded added latency, higher throughput.
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
        }
    }
}

/// Details of a tail truncation performed during recovery.
#[derive(Debug, Clone)]
pub struct Truncation {
    pub segment: PathBuf,
    pub offset: u64,
    pub bytes_discarded: u64,
    pub reason: String,
    /// True only under `ForceTruncate`: valid frames existed after the damage
    /// and were discarded.
    pub discarded_valid_frames: bool,
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
}

struct Inner {
    file: Arc<File>,
    path: PathBuf,
    base_seq: u64,
    len: u64,
    /// Closed (non-active) segments, oldest first: (base_seq, path).
    closed: Vec<(u64, PathBuf)>,
    next_seq: u64,
    written_seq: u64,
    durable_seq: u64,
    syncing: bool,
    poisoned: Option<String>,
}

/// The write-ahead log. Thread-safe; share via `Arc`.
pub struct Wal {
    dir: PathBuf,
    opts: WalOptions,
    inner: Mutex<Inner>,
    cv: Condvar,
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

fn create_segment(dir: &Path, base_seq: u64) -> Result<(File, PathBuf)> {
    let path = dir.join(format::segment_file_name(base_seq));
    let tmp = fsutil::tmp_path(&path);
    fsutil::remove_if_exists(&tmp)?;
    let mut f = fsutil::create_new_file(&tmp)?;
    let header = format::encode_header(&format::SegmentHeader {
        base_seq,
        created_ms: time::now_unix_millis(),
    });
    f.write_all(&header)
        .ctx(|| format!("writing WAL header {}", tmp.display()))?;
    f.sync_all().ctx(|| format!("fsyncing {}", tmp.display()))?;
    drop(f);
    fs::rename(&tmp, &path).ctx(|| format!("renaming {}", tmp.display()))?;
    fsutil::fsync_dir(dir)?;
    failpoint::hit("wal::rotate::after_create")?;
    let f = open_append(&path)?;
    Ok((f, path))
}

fn open_append(path: &Path) -> Result<File> {
    OpenOptions::new()
        .append(true)
        .open(path)
        .ctx(|| format!("opening {} for append", path.display()))
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
            let data = fs::read(path).ctx(|| format!("reading {loc}"))?;
            let header = format::decode_header(&data, &loc)?;
            if header.base_seq != *base {
                return Err(Error::corruption(
                    &loc,
                    format!("header base_seq {} != file name {base}", header.base_seq),
                ));
            }
            report.segments_scanned += 1;
            let mut pos = HEADER_LEN;
            let mut next = *base;
            while pos < data.len() {
                match format::parse_frame(&data[pos..]) {
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
                        if frame.seq > replay_after {
                            on_record(frame.seq, rtype, frame.payload)?;
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
                        let later = format::find_later_valid_frame(&data[pos + 1..], next);
                        if later.is_some() && opts.recovery != RecoveryPolicy::ForceTruncate {
                            return Err(Error::corruption(
                                &loc,
                                format!(
                                    "{reason} at offset {pos} is followed by valid frames; \
                                     refusing to truncate (possible mid-log corruption). \
                                     Inspect the WAL; recovery policy ForceTruncate discards them."
                                ),
                            ));
                        }
                        let f = OpenOptions::new()
                            .write(true)
                            .open(path)
                            .ctx(|| format!("opening {loc} for truncation"))?;
                        f.set_len(pos as u64).ctx(|| format!("truncating {loc}"))?;
                        f.sync_all().ctx(|| format!("fsyncing {loc}"))?;
                        tracing::warn!(segment = %loc, offset = pos, bytes = data.len() - pos,
                            %reason, "truncated invalid WAL tail");
                        report.truncation = Some(Truncation {
                            segment: path.clone(),
                            offset: pos as u64,
                            bytes_discarded: (data.len() - pos) as u64,
                            reason: reason.to_string(),
                            discarded_valid_frames: later.is_some(),
                        });
                        break;
                    }
                }
            }
            prev_next = Some(next);
        }
        report.last_seq = last_seq;

        let next_seq = last_seq + 1;
        let (file, path, base_seq, len, closed) = match segs.last() {
            // Reuse the last segment only if appending `next_seq` keeps it contiguous.
            Some((base, path)) if prev_next == Some(next_seq) => {
                let f = open_append(path)?;
                // Make recovered (possibly unsynced) bytes durable before new appends.
                f.sync_all()
                    .ctx(|| format!("fsyncing {}", path.display()))?;
                let len = f
                    .metadata()
                    .ctx(|| format!("stat {}", path.display()))?
                    .len();
                let closed = segs[..segs.len() - 1].to_vec();
                (f, path.clone(), *base, len, closed)
            }
            _ => {
                // No WAL yet, or the WAL ends before the manifest (e.g. Async
                // tail lost on power failure after a flush): start a new
                // segment so sequence numbers never repeat.
                let (f, path) = create_segment(dir, next_seq)?;
                (f, path, next_seq, HEADER_LEN as u64, segs.clone())
            }
        };

        let wal = Wal {
            dir: dir.to_path_buf(),
            opts,
            inner: Mutex::new(Inner {
                file: Arc::new(file),
                path,
                base_seq,
                len,
                closed,
                next_seq,
                written_seq: last_seq,
                durable_seq: last_seq,
                syncing: false,
                poisoned: None,
            }),
            cv: Condvar::new(),
        };
        Ok((wal, report))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
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
        if g.len > HEADER_LEN as u64 && g.len + frame.len() as u64 > self.opts.segment_max_bytes {
            self.rotate_locked(&mut g)?;
        }
        let seq = g.next_seq;
        format::encode_frame_into(
            &mut frame,
            record_type,
            seq,
            payload.len() as u32,
            payload_crc,
        );

        failpoint::hit("wal::before_write")?;
        if let Some(action) = failpoint::eval("wal::torn_write") {
            // Simulate a crash in the middle of write(2).
            let _ = (&*g.file).write_all(&frame[..frame.len() / 2]);
            let _ = g.file.sync_data();
            failpoint::fire("wal::torn_write", action)?;
        }
        if let Err(e) = (&*g.file).write_all(&frame) {
            let why = format!("write to {} failed: {e}", g.path.display());
            Self::poison(&mut g, why.clone());
            return Err(Error::indeterminate(why));
        }
        g.len += frame.len() as u64;
        g.next_seq += 1;
        g.written_seq = seq;
        if let Err(e) = failpoint::hit("wal::after_write") {
            Self::poison(&mut g, e.to_string());
            return Err(Error::indeterminate(e.to_string()));
        }
        match self.opts.durability {
            Durability::Async => Ok(seq),
            Durability::Sync | Durability::GroupCommit { .. } => {
                self.wait_durable(g, seq).map(|()| seq)
            }
        }
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
                continue;
            }
            g.syncing = true;
            if let Durability::GroupCommit { max_delay } = self.opts.durability {
                if !max_delay.is_zero() {
                    let deadline = Instant::now() + max_delay;
                    while !self.cv.wait_until(&mut g, deadline).timed_out() {}
                }
            }
            let target = g.written_seq;
            let file = Arc::clone(&g.file);
            let res = MutexGuard::unlocked(&mut g, || -> Result<()> {
                failpoint::hit("wal::before_sync")?;
                file.sync_data().ctx(|| "fsyncing WAL segment".to_owned())?;
                failpoint::hit("wal::after_sync")?;
                Ok(())
            });
            g.syncing = false;
            match res {
                Ok(()) => g.durable_seq = g.durable_seq.max(target),
                Err(e) => Self::poison(&mut g, format!("sync failed: {e}")),
            }
            self.cv.notify_all();
        }
    }

    fn rotate_locked(&self, g: &mut MutexGuard<'_, Inner>) -> Result<()> {
        // The old segment must be fully durable before it becomes immutable.
        if let Err(e) = g.file.sync_data() {
            let why = format!("sync before rotation failed: {e}");
            Self::poison(g, why.clone());
            return Err(Error::indeterminate(why));
        }
        g.durable_seq = g.durable_seq.max(g.written_seq);
        self.cv.notify_all();
        let base = g.next_seq;
        let (file, path) = create_segment(&self.dir, base).inspect_err(|e| {
            Self::poison(g, format!("segment rotation failed: {e}"));
        })?;
        let old_base = g.base_seq;
        let old_path = std::mem::replace(&mut g.path, path);
        g.closed.push((old_base, old_path));
        g.file = Arc::new(file);
        g.base_seq = base;
        g.len = HEADER_LEN as u64;
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
        }
    }
}

#[cfg(test)]
mod tests;
