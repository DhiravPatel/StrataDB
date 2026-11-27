//! Storage I/O engine (spec VIII.4, STRATA-SYS-04-001/002/009/025,
//! STRATA-SYS-02-036/037).
//!
//! Storage reads go through [`IoEngine`]: a batch of reads is submitted and
//! then completed. Two backends exist:
//! * [`PreadEngine`] is the portable `pread`/`pwrite` backend (the spec's
//!   `threadpool` backend). It completes a batch on the submitting thread.
//!   Open files live in an LRU bounded by a file-descriptor budget
//!   (`max_open_files`, default 50% of `RLIMIT_NOFILE`). When the OS refuses
//!   an `open` with `EMFILE`, cold files are closed and the open is retried
//!   instead of failing.
//! * [`SimIo`] is the deterministic in-memory backend used by tests. It
//!   injects short reads and writes, `EIO` on read and fsync, `ENOSPC`,
//!   latency spikes, and (in [`SimIo::crash`]) reordering of non-fsynced
//!   writes with torn writes at 512-byte sector granularity.
//!
//! Every backend follows the same rules, checked by one conformance suite:
//! * Short reads and writes are resubmitted for the remainder. A short read
//!   counts as end of file only at or past the file's length
//!   (STRATA-SYS-04-009).
//! * A failed read is retried once. A second failure is an error naming the
//!   file, offset and length (STRATA-SYS-04-025); partial data is never
//!   returned.
//! * A file is identified by path *and* identity (device + inode). Reopening
//!   a path whose identity changed is an error, never a silent read of a
//!   different file.
//!
//! This module is the only place that reads storage files directly; the
//! segment, storage and query crates have direct reads banned by
//! `clippy::disallowed_methods` (their `clippy.toml`).

#![allow(clippy::disallowed_methods)]

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::{Error, ErrorKind, Result};

/// An open file's identity: path, device and inode, and length at open.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileRef {
    path: Arc<Path>,
    dev: u64,
    ino: u64,
    len: u64,
}

impl FileRef {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Length when the file was opened.
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// One read of `len` bytes at `offset`.
#[derive(Debug, Clone)]
pub struct ReadReq {
    pub file: FileRef,
    pub offset: u64,
    pub len: usize,
}

/// Reads submitted to an engine, awaiting [`IoEngine::complete`].
#[derive(Debug)]
pub struct Submission {
    results: Vec<Result<Vec<u8>>>,
}

/// Counters (monotonic except `files_open`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoStats {
    pub reads: u64,
    pub bytes_read: u64,
    /// Short reads whose remainder was resubmitted.
    pub short_reads_resubmitted: u64,
    /// Failed reads retried (once each).
    pub read_retries: u64,
    /// Reads that failed after the retry.
    pub read_errors: u64,
    pub opens: u64,
    /// Files currently held open by the engine's cache.
    pub files_open: u64,
    /// Files closed to stay within the descriptor budget.
    pub evictions: u64,
    /// `EMFILE` on open, recovered by closing cold files.
    pub emfile_recoveries: u64,
    pub writes: u64,
    pub bytes_written: u64,
    pub syncs: u64,
    /// Descriptor budget of the file cache (0 = not applicable).
    pub max_open_files: u64,
    /// I/O calls allowed in flight per device (0 = not bounded; spec
    /// STRATA-SYS-04-008).
    pub max_inflight_per_device: u64,
    /// I/O calls in flight now, all devices.
    pub inflight: u64,
    /// Highest number in flight on any one device.
    pub inflight_peak: u64,
    /// Calls that waited because their device was at the limit.
    pub inflight_waits: u64,
}

/// A file being written. Methods take `&self`: positioned writes and
/// fsync on one descriptor are safe from several threads (the WAL writes
/// under its lock while the sync leader fsyncs without it).
pub trait WriteFile: Send + Sync + std::fmt::Debug {
    fn path(&self) -> &Path;
    /// Write some prefix of `data` at `offset`; returns bytes written
    /// (callers loop: see [`write_all_at`]).
    fn write_at(&self, data: &[u8], offset: u64) -> Result<usize>;
    /// Make written bytes durable: data only (`fdatasync`) or data and
    /// metadata (`fsync`).
    fn sync(&self, data_only: bool) -> Result<()>;
    /// Truncate or extend to `len` bytes (not durable until [`Self::sync`]).
    fn set_len(&self, len: u64) -> Result<()>;
    fn len(&self) -> Result<u64>;
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

/// The storage I/O interface (STRATA-SYS-04-001).
pub trait IoEngine: Send + Sync + std::fmt::Debug {
    fn name(&self) -> &'static str;
    /// Open `path` for reading; captures identity and length.
    fn open(&self, path: &Path) -> Result<FileRef>;
    /// Submit a batch of reads.
    fn submit(&self, reqs: Vec<ReadReq>) -> Submission;
    /// Wait for a submission; one result per request, in order.
    fn complete(&self, s: Submission) -> Vec<Result<Vec<u8>>>;
    /// Create `path`, which must not exist (owner-only permissions).
    fn create_new(&self, path: &Path) -> Result<Arc<dyn WriteFile>>;
    /// Open an existing `path` for writing.
    fn open_write(&self, path: &Path) -> Result<Arc<dyn WriteFile>>;
    fn stats(&self) -> IoStats;
    /// Drop any cached descriptor of `file` (after one-shot whole-file reads,
    /// so a later unlink frees the space immediately).
    fn forget(&self, _file: &FileRef) {}
}

/// Random-access read source (used by layered formats such as sealed
/// containers, which read their raw bytes through it).
pub trait RandomRead: Send + Sync + std::fmt::Debug {
    /// Fill `buf` from `offset`; an error if the range is not entirely
    /// inside the source.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A file read through an [`IoEngine`].
#[derive(Debug, Clone)]
pub struct IoFile {
    io: Arc<dyn IoEngine>,
    file: FileRef,
}

impl IoFile {
    pub fn open(io: Arc<dyn IoEngine>, path: &Path) -> Result<Self> {
        let file = io.open(path)?;
        Ok(Self { io, file })
    }

    pub fn file(&self) -> &FileRef {
        &self.file
    }

    /// Read `len` bytes at `offset` into a new buffer (no extra copy).
    pub fn read_vec(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        read_exact_at(&*self.io, &self.file, offset, len)
    }
}

impl RandomRead for IoFile {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let v = self.read_vec(offset, buf.len())?;
        buf.copy_from_slice(&v);
        Ok(())
    }

    fn len(&self) -> u64 {
        self.file.len
    }
}

/// Direct file access, for tools and tests outside the data path.
impl RandomRead for File {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        FileExt::read_exact_at(self, buf, offset)
            .map_err(|e| Error::io(format!("reading {} bytes at {offset}", buf.len()), e))
    }

    fn len(&self) -> u64 {
        self.metadata().map_or(0, |m| m.len())
    }
}

/// Whether `e` is "file not found".
pub fn is_not_found(e: &Error) -> bool {
    matches!(e, Error::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound)
}

/// Read exactly `len` bytes at `offset` of `file`.
pub fn read_exact_at(
    io: &dyn IoEngine,
    file: &FileRef,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>> {
    let s = io.submit(vec![ReadReq {
        file: file.clone(),
        offset,
        len,
    }]);
    io.complete(s)
        .pop()
        .unwrap_or_else(|| Err(Error::internal("I/O engine returned no completion")))
}

/// Crash-safe replace of `path` with `contents` through `io`: write a
/// temporary file, fsync it, rename it over `path`, fsync the directory
/// (STRATA-SYS-02-020). Failpoints `<prefix>::before_rename` and
/// `<prefix>::after_rename` mark the crash points, as in
/// [`crate::fsutil::atomic_write`].
pub fn atomic_write(
    io: &dyn IoEngine,
    path: &Path,
    contents: &[u8],
    failpoint_prefix: &str,
) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::internal(format!("{} has no parent", path.display())))?;
    let tmp = crate::fsutil::tmp_path(path);
    crate::fsutil::remove_if_exists(&tmp)?;
    let f = io.create_new(&tmp)?;
    write_all_at(&*f, contents, 0)?;
    f.sync(false)?;
    drop(f);
    crate::failpoint::hit(&format!("{failpoint_prefix}::before_rename"))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        Error::io(
            format!("renaming {} to {}", tmp.display(), path.display()),
            e,
        )
    })?;
    crate::failpoint::hit(&format!("{failpoint_prefix}::after_rename"))?;
    crate::fsutil::fsync_dir(parent)
}

/// Read a whole file (small metadata files: manifests, backup manifests).
pub fn read_file(io: &dyn IoEngine, path: &Path) -> Result<Vec<u8>> {
    let f = io.open(path)?;
    let len = usize::try_from(f.len()).map_err(|_| Error::invalid_input("file too large"))?;
    let r = read_exact_at(io, &f, 0, len);
    io.forget(&f);
    r
}

/// Write all of `data` at `offset`, resubmitting after short writes.
pub fn write_all_at(f: &dyn WriteFile, data: &[u8], offset: u64) -> Result<()> {
    let mut done = 0usize;
    while done < data.len() {
        let n = f.write_at(&data[done..], offset + done as u64)?;
        if n == 0 {
            return Err(Error::new(
                ErrorKind::Io,
                format!("write to {} made no progress", f.path().display()),
            ));
        }
        done += n;
    }
    Ok(())
}

fn read_error(path: &Path, offset: u64, len: usize, e: &std::io::Error) -> Error {
    Error::io(
        format!(
            "reading {} bytes at offset {offset} of {} (after one retry)",
            len,
            path.display()
        ),
        std::io::Error::new(e.kind(), e.to_string()),
    )
}

/// Read `len` bytes at `offset` through `read_at`, resubmitting short reads
/// and retrying a failure once. `file_len` decides when a short read is EOF.
fn read_loop(
    path: &Path,
    file_len: u64,
    offset: u64,
    len: usize,
    stats: &Counters,
    mut read_at: impl FnMut(&mut [u8], u64) -> std::io::Result<usize>,
) -> Result<Vec<u8>> {
    let end = offset
        .checked_add(len as u64)
        .filter(|e| *e <= file_len)
        .ok_or_else(|| {
            Error::corruption(
                path.display().to_string(),
                format!("read of {len} bytes at {offset} past the end ({file_len} bytes)"),
            )
        })?;
    let mut buf = vec![0u8; len];
    let mut got = 0usize;
    let mut retried = false;
    while got < len {
        match read_at(&mut buf[got..], offset + got as u64) {
            Ok(0) => {
                // Short read before the known end: the file shrank.
                return Err(Error::corruption(
                    path.display().to_string(),
                    format!(
                        "unexpected end of file at {} (expected {end} bytes)",
                        offset + got as u64
                    ),
                ));
            }
            Ok(n) => {
                got += n;
                if got < len {
                    stats.short_reads.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) if !retried => {
                retried = true;
                stats.retries.fetch_add(1, Ordering::Relaxed);
                let _ = e;
            }
            Err(e) => {
                stats.read_errors.fetch_add(1, Ordering::Relaxed);
                return Err(read_error(path, offset, len, &e));
            }
        }
    }
    stats.reads.fetch_add(1, Ordering::Relaxed);
    stats.bytes_read.fetch_add(len as u64, Ordering::Relaxed);
    Ok(buf)
}

#[derive(Debug, Default)]
struct Counters {
    reads: AtomicU64,
    bytes_read: AtomicU64,
    short_reads: AtomicU64,
    retries: AtomicU64,
    read_errors: AtomicU64,
    opens: AtomicU64,
    evictions: AtomicU64,
    emfile: AtomicU64,
    writes: AtomicU64,
    bytes_written: AtomicU64,
    syncs: AtomicU64,
}

impl Counters {
    fn snapshot(&self, files_open: u64) -> IoStats {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        IoStats {
            reads: l(&self.reads),
            bytes_read: l(&self.bytes_read),
            short_reads_resubmitted: l(&self.short_reads),
            read_retries: l(&self.retries),
            read_errors: l(&self.read_errors),
            opens: l(&self.opens),
            files_open,
            evictions: l(&self.evictions),
            emfile_recoveries: l(&self.emfile),
            writes: l(&self.writes),
            bytes_written: l(&self.bytes_written),
            syncs: l(&self.syncs),
            ..IoStats::default()
        }
    }
}

// ---------------------------------------------------------------------------
// In-flight bound per device (STRATA-SYS-04-008)

/// Default bound on I/O calls in flight per device. Typical NVMe saturates
/// at a queue depth of 128-256; the portable backend issues one call per
/// thread, so it binds only with many concurrent threads.
pub const DEFAULT_MAX_INFLIGHT_PER_DEVICE: usize = 128;

/// Counting gate per device (`st_dev`): an I/O call holds a permit while it
/// runs. Below the limit a permit is one compare-and-swap and its release
/// one atomic subtraction; the mutex and condition variable are used only
/// while a caller waits.
#[derive(Debug)]
struct DeviceGates {
    limit: usize,
    gates: Mutex<HashMap<u64, Arc<Gate>>>,
    inflight: AtomicU64,
    peak: AtomicU64,
    waits: AtomicU64,
}

#[derive(Debug, Default)]
struct Gate {
    /// Permits held.
    n: AtomicU64,
    /// Callers waiting for a permit (releasers notify only when > 0).
    waiters: AtomicU64,
    lock: Mutex<()>,
    cv: std::sync::Condvar,
}

impl Gate {
    fn try_take(&self, limit: u64) -> bool {
        let mut cur = self.n.load(Ordering::SeqCst);
        while cur < limit {
            match self
                .n
                .compare_exchange_weak(cur, cur + 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return true,
                Err(now) => cur = now,
            }
        }
        false
    }
}

/// A permit; released on drop.
struct Permit<'a> {
    gate: Arc<Gate>,
    owner: &'a DeviceGates,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.owner.inflight.fetch_sub(1, Ordering::Relaxed);
        self.gate.n.fetch_sub(1, Ordering::SeqCst);
        // SeqCst pairs with the waiter's increment of `waiters` before its
        // re-check: either it sees this release or this sees it waiting.
        if self.gate.waiters.load(Ordering::SeqCst) > 0 {
            drop(self.gate.lock.lock().unwrap_or_else(|e| e.into_inner()));
            self.gate.cv.notify_one();
        }
    }
}

impl DeviceGates {
    fn new(limit: usize) -> Self {
        Self {
            limit: limit.max(1),
            gates: Mutex::new(HashMap::new()),
            inflight: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            waits: AtomicU64::new(0),
        }
    }

    fn gate(&self, dev: u64) -> Arc<Gate> {
        let mut g = self.gates.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(g.entry(dev).or_default())
    }

    /// Wait until `gate` has room, then take a permit.
    fn acquire(&self, gate: &Arc<Gate>) -> Permit<'_> {
        let limit = self.limit as u64;
        if !gate.try_take(limit) {
            self.waits.fetch_add(1, Ordering::Relaxed);
            let mut g = gate.lock.lock().unwrap_or_else(|e| e.into_inner());
            gate.waiters.fetch_add(1, Ordering::SeqCst);
            while !gate.try_take(limit) {
                g = gate.cv.wait(g).unwrap_or_else(|e| e.into_inner());
            }
            gate.waiters.fetch_sub(1, Ordering::SeqCst);
        }
        self.peak
            .fetch_max(gate.n.load(Ordering::Relaxed), Ordering::Relaxed);
        self.inflight.fetch_add(1, Ordering::Relaxed);
        Permit {
            gate: Arc::clone(gate),
            owner: self,
        }
    }

    fn fill(&self, s: &mut IoStats) {
        s.max_inflight_per_device = self.limit as u64;
        s.inflight = self.inflight.load(Ordering::Relaxed);
        s.inflight_peak = self.peak.load(Ordering::Relaxed);
        s.inflight_waits = self.waits.load(Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Portable backend

/// Portable `pread`/`pwrite` backend with a bounded LRU of open files.
#[derive(Debug)]
pub struct PreadEngine {
    max_open: usize,
    cache: Mutex<FdCache>,
    stats: Counters,
    wstats: Arc<Counters>,
    gates: Arc<DeviceGates>,
}

#[derive(Debug, Default)]
struct FdCache {
    files: HashMap<FileRef, (Arc<File>, u64)>,
    tick: u64,
}

impl FdCache {
    fn touch(&mut self, f: &FileRef) -> Option<Arc<File>> {
        self.tick += 1;
        let t = self.tick;
        self.files.get_mut(f).map(|(file, used)| {
            *used = t;
            Arc::clone(file)
        })
    }

    /// Close least recently used files until at most `keep` remain.
    fn evict_to(&mut self, keep: usize) -> u64 {
        let mut n = 0;
        while self.files.len() > keep {
            let Some(victim) = self
                .files
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            self.files.remove(&victim);
            n += 1;
        }
        n
    }
}

fn is_emfile(e: &std::io::Error) -> bool {
    matches!(e.raw_os_error(), Some(24) | Some(23)) // EMFILE, ENFILE
}

impl PreadEngine {
    /// An engine keeping at most `max_open_files` files open (at least 8),
    /// with [`DEFAULT_MAX_INFLIGHT_PER_DEVICE`] calls in flight per device.
    pub fn new(max_open_files: usize) -> Self {
        Self::with_inflight_limit(max_open_files, DEFAULT_MAX_INFLIGHT_PER_DEVICE)
    }

    /// Like [`PreadEngine::new`] with at most `max_inflight_per_device` I/O
    /// calls (reads, writes, fsyncs) in flight on each device (at least 1).
    pub fn with_inflight_limit(max_open_files: usize, max_inflight_per_device: usize) -> Self {
        Self {
            max_open: max_open_files.max(8),
            cache: Mutex::new(FdCache::default()),
            stats: Counters::default(),
            wstats: Arc::new(Counters::default()),
            gates: Arc::new(DeviceGates::new(max_inflight_per_device)),
        }
    }

    pub fn max_open_files(&self) -> usize {
        self.max_open
    }

    fn write_file(&self, f: File, path: &Path) -> Result<Arc<dyn WriteFile>> {
        let dev = f
            .metadata()
            .map_err(|e| Error::io(format!("stat {}", path.display()), e))?
            .dev();
        Ok(Arc::new(PwriteFile {
            f,
            path: path.to_path_buf(),
            stats: Arc::clone(&self.wstats),
            gate: self.gates.gate(dev),
            gates: Arc::clone(&self.gates),
        }))
    }

    /// `File::open` that recovers from descriptor exhaustion by closing
    /// cold cached files (STRATA-SYS-02-037).
    fn open_file(&self, path: &Path) -> Result<File> {
        match File::open(path) {
            Ok(f) => Ok(f),
            Err(e) if is_emfile(&e) => {
                let closed = {
                    let mut c = self.cache.lock().unwrap_or_else(|e| e.into_inner());
                    let keep = c.files.len() / 2;
                    c.evict_to(keep)
                };
                self.stats.evictions.fetch_add(closed, Ordering::Relaxed);
                self.stats.emfile.fetch_add(1, Ordering::Relaxed);
                File::open(path).map_err(|e| Error::io(format!("opening {}", path.display()), e))
            }
            Err(e) => Err(Error::io(format!("opening {}", path.display()), e)),
        }
    }

    fn identity(path: &Path, f: &File) -> Result<FileRef> {
        let m = f
            .metadata()
            .map_err(|e| Error::io(format!("stat {}", path.display()), e))?;
        Ok(FileRef {
            path: Arc::from(path),
            dev: m.dev(),
            ino: m.ino(),
            len: m.len(),
        })
    }

    fn insert(&self, r: &FileRef, f: Arc<File>) {
        let mut c = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        c.tick += 1;
        let t = c.tick;
        c.files.insert(r.clone(), (f, t));
        let max = self.max_open;
        let n = c.evict_to(max);
        self.stats.evictions.fetch_add(n, Ordering::Relaxed);
    }

    /// The open file for `r`, reopening (and checking identity) if it was
    /// evicted.
    fn file(&self, r: &FileRef) -> Result<Arc<File>> {
        if let Some(f) = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .touch(r)
        {
            return Ok(f);
        }
        let f = self.open_file(&r.path)?;
        let now = Self::identity(&r.path, &f)?;
        if (now.dev, now.ino) != (r.dev, r.ino) {
            return Err(Error::corruption(
                r.path.display().to_string(),
                "file was replaced while open (identity changed)",
            ));
        }
        self.stats.opens.fetch_add(1, Ordering::Relaxed);
        let f = Arc::new(f);
        self.insert(r, Arc::clone(&f));
        Ok(f)
    }
}

impl IoEngine for PreadEngine {
    fn name(&self) -> &'static str {
        "pread"
    }

    fn open(&self, path: &Path) -> Result<FileRef> {
        let f = self.open_file(path)?;
        let r = Self::identity(path, &f)?;
        self.stats.opens.fetch_add(1, Ordering::Relaxed);
        self.insert(&r, Arc::new(f));
        Ok(r)
    }

    fn submit(&self, reqs: Vec<ReadReq>) -> Submission {
        // Portable backend: the batch completes on the submitting thread.
        let mut last_gate: Option<(u64, Arc<Gate>)> = None;
        let results = reqs
            .into_iter()
            .map(|q| {
                let f = self.file(&q.file)?;
                // Usually one device per batch: look its gate up once.
                let gate = match &last_gate {
                    Some((dev, g)) if *dev == q.file.dev => Arc::clone(g),
                    _ => {
                        let g = self.gates.gate(q.file.dev);
                        last_gate = Some((q.file.dev, Arc::clone(&g)));
                        g
                    }
                };
                read_loop(
                    &q.file.path,
                    q.file.len,
                    q.offset,
                    q.len,
                    &self.stats,
                    |b, o| {
                        let _permit = self.gates.acquire(&gate);
                        f.read_at(b, o)
                    },
                )
            })
            .collect();
        Submission { results }
    }

    fn complete(&self, s: Submission) -> Vec<Result<Vec<u8>>> {
        s.results
    }

    fn forget(&self, file: &FileRef) {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .files
            .remove(file);
    }

    fn create_new(&self, path: &Path) -> Result<Arc<dyn WriteFile>> {
        // Through fsutil: owner-only mode, and the power-loss recorder
        // (`simfs`) sees the new inode.
        let f = crate::fsutil::create_new_file(path)?;
        self.write_file(f, path)
    }

    fn open_write(&self, path: &Path) -> Result<Arc<dyn WriteFile>> {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| Error::io(format!("opening {} for writing", path.display()), e))?;
        self.write_file(f, path)
    }

    fn stats(&self) -> IoStats {
        let open = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .files
            .len() as u64;
        let mut s = self.stats.snapshot(open);
        s.max_open_files = self.max_open as u64;
        self.gates.fill(&mut s);
        let w = self.wstats.snapshot(0);
        s.writes = w.writes;
        s.bytes_written = w.bytes_written;
        s.syncs = w.syncs;
        s
    }
}

#[derive(Debug)]
struct PwriteFile {
    f: File,
    path: PathBuf,
    stats: Arc<Counters>,
    gate: Arc<Gate>,
    gates: Arc<DeviceGates>,
}

impl WriteFile for PwriteFile {
    fn path(&self) -> &Path {
        &self.path
    }

    fn write_at(&self, data: &[u8], offset: u64) -> Result<usize> {
        let _permit = self.gates.acquire(&self.gate);
        loop {
            match self.f.write_at(data, offset) {
                Ok(n) => {
                    self.stats.writes.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .bytes_written
                        .fetch_add(n as u64, Ordering::Relaxed);
                    return Ok(n);
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::io(format!("writing {}", self.path.display()), e)),
            }
        }
    }

    fn sync(&self, data_only: bool) -> Result<()> {
        // Through fsutil so the power-loss recorder observes every fsync.
        let _permit = self.gates.acquire(&self.gate);
        self.stats.syncs.fetch_add(1, Ordering::Relaxed);
        crate::fsutil::sync_file(&self.f, &self.path, data_only)
            .map_err(|e| Error::io(format!("fsync {}", self.path.display()), e))
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.f
            .set_len(len)
            .map_err(|e| Error::io(format!("truncating {}", self.path.display()), e))
    }

    fn len(&self) -> Result<u64> {
        self.f
            .metadata()
            .map(|m| m.len())
            .map_err(|e| Error::io(format!("stat {}", self.path.display()), e))
    }
}

// ---------------------------------------------------------------------------
// File-descriptor limit

/// Raise the soft `RLIMIT_NOFILE` to the hard limit (STRATA-SYS-02-036).
/// Returns `(old, new)` soft limits. An unlimited hard limit is capped at
/// 1,048,576 (macOS rejects unlimited; Linux caps at `nr_open`); if that is
/// refused, smaller values are tried.
pub fn raise_nofile_limit() -> Result<(u64, u64)> {
    use rustix::process::{getrlimit, setrlimit, Resource, Rlimit};
    let lim = getrlimit(Resource::Nofile);
    let old = lim.current.unwrap_or(u64::MAX);
    let hard = lim.maximum.unwrap_or(u64::MAX);
    if old >= hard {
        return Ok((old, old));
    }
    for target in [hard.min(1 << 20), hard.min(65_536), hard.min(10_240)] {
        if target <= old {
            break;
        }
        let new = Rlimit {
            current: Some(target),
            maximum: lim.maximum,
        };
        if setrlimit(Resource::Nofile, new).is_ok() {
            return Ok((old, target));
        }
    }
    Ok((old, old))
}

/// Current soft `RLIMIT_NOFILE` (`None` = unlimited).
pub fn nofile_soft_limit() -> Option<u64> {
    rustix::process::getrlimit(rustix::process::Resource::Nofile).current
}

static DEFAULT: OnceLock<Arc<dyn IoEngine>> = OnceLock::new();
static CONFIGURED_MAX_OPEN: OnceLock<usize> = OnceLock::new();
static CONFIGURED_MAX_INFLIGHT: OnceLock<usize> = OnceLock::new();

/// Set the default engine's in-flight bound per device before its first use.
pub fn configure_max_inflight_per_device(n: usize) -> Result<()> {
    if n == 0 {
        return Err(Error::invalid_input(
            "max_inflight_per_device must be at least 1",
        ));
    }
    if DEFAULT.get().is_some() {
        return Err(Error::invalid_input(
            "the I/O engine already started; set max_inflight_per_device before opening data",
        ));
    }
    CONFIGURED_MAX_INFLIGHT
        .set(n)
        .map_err(|_| Error::invalid_input("max_inflight_per_device already configured"))
}

/// Default descriptor budget: 50% of the soft `RLIMIT_NOFILE`
/// (STRATA-SYS-02-037), at least 64.
pub fn default_max_open_files() -> usize {
    let soft = nofile_soft_limit().unwrap_or(1 << 20);
    usize::try_from(soft / 2).unwrap_or(usize::MAX).max(64)
}

/// Set the default engine's `max_open_files` before its first use.
pub fn configure_max_open_files(n: usize) -> Result<()> {
    if DEFAULT.get().is_some() {
        return Err(Error::invalid_input(
            "the I/O engine already started; set max_open_files before opening data",
        ));
    }
    CONFIGURED_MAX_OPEN
        .set(n)
        .map_err(|_| Error::invalid_input("max_open_files already configured"))
}

/// The process-wide default engine (portable backend).
pub fn default_io() -> Arc<dyn IoEngine> {
    Arc::clone(DEFAULT.get_or_init(|| {
        let max = CONFIGURED_MAX_OPEN
            .get()
            .copied()
            .unwrap_or_else(default_max_open_files);
        let inflight = CONFIGURED_MAX_INFLIGHT
            .get()
            .copied()
            .unwrap_or(DEFAULT_MAX_INFLIGHT_PER_DEVICE);
        Arc::new(PreadEngine::with_inflight_limit(max, inflight))
    }))
}

// ---------------------------------------------------------------------------
// Simulation backend

/// Faults [`SimIo`] can inject (spec VIII.24).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// The next read returns fewer bytes than asked (the remainder must be
    /// resubmitted).
    ShortRead,
    /// The next read fails with `EIO`.
    ReadEio,
    /// The next write writes fewer bytes than asked.
    ShortWrite,
    /// The next fsync fails with `EIO`; unsynced writes may be lost (Linux
    /// semantics since 4.13: the error is reported once).
    FsyncEio,
    /// The next write fails with `ENOSPC`.
    Enospc,
    /// The next read or write takes this long first.
    LatencySpike(Duration),
}

#[derive(Debug, Clone)]
enum Op {
    Write(u64, Vec<u8>),
    SetLen(u64),
}

#[derive(Debug, Clone)]
struct SimFile {
    ino: u64,
    /// What reads see (page cache).
    data: Vec<u8>,
    /// What survives a crash.
    durable: Vec<u8>,
    /// Writes and truncations since the last successful fsync, in order.
    pending: Vec<Op>,
}

#[derive(Debug, Default)]
struct SimState {
    files: HashMap<PathBuf, SimFile>,
    next_ino: u64,
    /// Scripted faults: (path filter, fault, remaining count).
    faults: Vec<(Option<PathBuf>, Fault, usize)>,
}

/// Deterministic in-memory backend with fault injection.
#[derive(Debug, Default, Clone)]
pub struct SimIo {
    state: Arc<Mutex<SimState>>,
    stats: Arc<Counters>,
}

/// Sector size for torn writes.
pub const SECTOR: usize = 512;

impl SimIo {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SimState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Inject `fault` for the next `times` matching operations, on `path`
    /// only (or any file).
    pub fn inject(&self, path: Option<&Path>, fault: Fault, times: usize) {
        self.lock()
            .faults
            .push((path.map(Path::to_path_buf), fault, times));
    }

    /// Take one scripted fault matching `path` and `want`.
    fn take(&self, path: &Path, want: impl Fn(Fault) -> bool) -> Option<Fault> {
        let mut st = self.lock();
        let i = st
            .faults
            .iter()
            .position(|(p, f, n)| *n > 0 && want(*f) && p.as_deref().is_none_or(|p| p == path))?;
        let f = st.faults[i].1;
        st.faults[i].2 -= 1;
        if st.faults[i].2 == 0 {
            st.faults.remove(i);
        }
        Some(f)
    }

    fn spike(&self, path: &Path) {
        if let Some(Fault::LatencySpike(d)) =
            self.take(path, |f| matches!(f, Fault::LatencySpike(_)))
        {
            std::thread::sleep(d);
        }
    }

    /// Create a file with `data` already durable (test setup).
    pub fn put(&self, path: &Path, data: &[u8]) {
        let mut st = self.lock();
        st.next_ino += 1;
        let ino = st.next_ino;
        st.files.insert(
            path.to_path_buf(),
            SimFile {
                ino,
                data: data.to_vec(),
                durable: data.to_vec(),
                pending: Vec::new(),
            },
        );
    }

    /// Current (page-cache) contents of `path`.
    pub fn contents(&self, path: &Path) -> Option<Vec<u8>> {
        self.lock().files.get(path).map(|f| f.data.clone())
    }

    /// Delete `path` (a later `put`/`create` gets a new identity).
    pub fn remove(&self, path: &Path) {
        self.lock().files.remove(path);
    }

    /// Simulate power loss: every file keeps its durable contents plus a
    /// random subset of its unsynced writes, applied in random order, the
    /// last of which may be torn at a sector boundary. Deterministic for a
    /// given `seed`.
    pub fn crash(&self, seed: u64) {
        let mut rng = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut next = move || {
            rng = rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = rng;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        let mut st = self.lock();
        let mut paths: Vec<PathBuf> = st.files.keys().cloned().collect();
        paths.sort();
        for p in paths {
            let Some(f) = st.files.get_mut(&p) else {
                continue;
            };
            let mut img = f.durable.clone();
            let mut pending = std::mem::take(&mut f.pending);
            // Random order (Fisher-Yates), random survivors.
            for i in (1..pending.len()).rev() {
                let j = (next() % (i as u64 + 1)) as usize;
                pending.swap(i, j);
            }
            let keep = (next() % (pending.len() as u64 + 1)) as usize;
            for (k, op) in pending.into_iter().take(keep).enumerate() {
                match op {
                    Op::Write(off, data) => {
                        let torn = k + 1 == keep && next() % 2 == 0;
                        let n = if torn {
                            let sectors = data.len().div_ceil(SECTOR);
                            ((next() % sectors as u64) as usize * SECTOR).min(data.len())
                        } else {
                            data.len()
                        };
                        apply(&mut img, off, &data[..n]);
                    }
                    Op::SetLen(len) => img.resize(len as usize, 0),
                }
            }
            f.data = img.clone();
            f.durable = img;
        }
    }
}

fn apply(img: &mut Vec<u8>, off: u64, data: &[u8]) {
    let off = off as usize;
    if img.len() < off + data.len() {
        img.resize(off + data.len(), 0);
    }
    img[off..off + data.len()].copy_from_slice(data);
}

fn os(code: i32) -> std::io::Error {
    std::io::Error::from_raw_os_error(code)
}

impl IoEngine for SimIo {
    fn name(&self) -> &'static str {
        "sim"
    }

    fn open(&self, path: &Path) -> Result<FileRef> {
        let st = self.lock();
        let f = st.files.get(path).ok_or_else(|| {
            Error::io(
                format!("opening {}", path.display()),
                std::io::Error::from(std::io::ErrorKind::NotFound),
            )
        })?;
        self.stats.opens.fetch_add(1, Ordering::Relaxed);
        Ok(FileRef {
            path: Arc::from(path),
            dev: 0,
            ino: f.ino,
            len: f.data.len() as u64,
        })
    }

    fn submit(&self, reqs: Vec<ReadReq>) -> Submission {
        let results = reqs
            .into_iter()
            .map(|q| {
                self.spike(&q.file.path);
                read_loop(
                    &q.file.path,
                    q.file.len,
                    q.offset,
                    q.len,
                    &self.stats,
                    |b, o| {
                        match self.take(&q.file.path, |f| {
                            matches!(f, Fault::ShortRead | Fault::ReadEio)
                        }) {
                            Some(Fault::ReadEio) => return Err(os(5)),
                            Some(_) => {
                                // Short: at most half, at least one byte.
                                let st = self.lock();
                                let f = st.files.get(&*q.file.path).ok_or_else(|| os(2))?;
                                if f.ino != q.file.ino {
                                    return Err(std::io::Error::other("file replaced"));
                                }
                                let avail = f.data.len().saturating_sub(o as usize);
                                let n = (b.len() / 2).max(1).min(avail);
                                b[..n].copy_from_slice(&f.data[o as usize..o as usize + n]);
                                return Ok(n);
                            }
                            None => {}
                        }
                        let st = self.lock();
                        let f = st.files.get(&*q.file.path).ok_or_else(|| os(2))?;
                        if f.ino != q.file.ino {
                            return Err(std::io::Error::other("file replaced"));
                        }
                        let avail = f.data.len().saturating_sub(o as usize);
                        let n = b.len().min(avail);
                        b[..n].copy_from_slice(&f.data[o as usize..o as usize + n]);
                        Ok(n)
                    },
                )
            })
            .collect();
        Submission { results }
    }

    fn complete(&self, s: Submission) -> Vec<Result<Vec<u8>>> {
        s.results
    }

    fn create_new(&self, path: &Path) -> Result<Arc<dyn WriteFile>> {
        if self.lock().files.contains_key(path) {
            return Err(Error::io(format!("creating {}", path.display()), os(17)));
        }
        self.put(path, &[]);
        // A new inode's durable content is empty.
        if let Some(f) = self.lock().files.get_mut(path) {
            f.durable.clear();
        }
        Ok(Arc::new(SimWriteFile {
            sim: self.clone(),
            path: path.to_path_buf(),
        }))
    }

    fn open_write(&self, path: &Path) -> Result<Arc<dyn WriteFile>> {
        if !self.lock().files.contains_key(path) {
            return Err(Error::io(format!("opening {}", path.display()), os(2)));
        }
        Ok(Arc::new(SimWriteFile {
            sim: self.clone(),
            path: path.to_path_buf(),
        }))
    }

    fn stats(&self) -> IoStats {
        let n = self.lock().files.len() as u64;
        self.stats.snapshot(n)
    }
}

#[derive(Debug)]
struct SimWriteFile {
    sim: SimIo,
    path: PathBuf,
}

impl WriteFile for SimWriteFile {
    fn path(&self) -> &Path {
        &self.path
    }

    fn write_at(&self, data: &[u8], offset: u64) -> Result<usize> {
        self.sim.spike(&self.path);
        let fault = self.sim.take(&self.path, |f| {
            matches!(f, Fault::ShortWrite | Fault::Enospc)
        });
        let n = match fault {
            Some(Fault::Enospc) => {
                return Err(Error::io(
                    format!("writing {}", self.path.display()),
                    os(28),
                ))
            }
            Some(_) => (data.len() / 2).max(1).min(data.len()),
            None => data.len(),
        };
        let mut st = self.sim.lock();
        let f = st
            .files
            .get_mut(&self.path)
            .ok_or_else(|| Error::io(format!("writing {}", self.path.display()), os(2)))?;
        apply(&mut f.data, offset, &data[..n]);
        f.pending.push(Op::Write(offset, data[..n].to_vec()));
        self.sim.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.sim
            .stats
            .bytes_written
            .fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    fn sync(&self, _data_only: bool) -> Result<()> {
        self.sim.spike(&self.path);
        let eio = self
            .sim
            .take(&self.path, |f| f == Fault::FsyncEio)
            .is_some();
        let mut st = self.sim.lock();
        let f = st
            .files
            .get_mut(&self.path)
            .ok_or_else(|| Error::io(format!("fsync {}", self.path.display()), os(2)))?;
        self.sim.stats.syncs.fetch_add(1, Ordering::Relaxed);
        if eio {
            // The kernel dropped the dirty pages: they will not survive a
            // crash, and this error is the only report of it.
            f.pending.clear();
            return Err(Error::io(format!("fsync {}", self.path.display()), os(5)));
        }
        // Only writes since the last sync become durable: data whose fsync
        // failed was dropped by the kernel and is not written by a later,
        // successful fsync (it stays visible until a crash).
        for op in std::mem::take(&mut f.pending) {
            match op {
                Op::Write(off, data) => apply(&mut f.durable, off, &data),
                Op::SetLen(len) => f.durable.resize(len as usize, 0),
            }
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<()> {
        let mut st = self.sim.lock();
        let f = st
            .files
            .get_mut(&self.path)
            .ok_or_else(|| Error::io(format!("truncating {}", self.path.display()), os(2)))?;
        f.data.resize(len as usize, 0);
        f.pending.push(Op::SetLen(len));
        Ok(())
    }

    fn len(&self) -> Result<u64> {
        let st = self.sim.lock();
        st.files
            .get(&self.path)
            .map(|f| f.data.len() as u64)
            .ok_or_else(|| Error::io(format!("stat {}", self.path.display()), os(2)))
    }
}

// ---------------------------------------------------------------------------
// Fault decorator

/// Injects faults into any engine, for tests of upper layers on a real
/// file system: reads ([`Fault::ShortRead`], [`Fault::ReadEio`],
/// [`Fault::LatencySpike`]) go through the same retry and resubmission loop
/// as the backends; writes get [`Fault::ShortWrite`], [`Fault::Enospc`] and
/// [`Fault::FsyncEio`]. Faults are scoped by path suffix.
#[derive(Debug, Clone)]
pub struct FaultyIo {
    inner: Arc<dyn IoEngine>,
    faults: SimIo,
}

impl FaultyIo {
    pub fn new(inner: Arc<dyn IoEngine>) -> Self {
        Self {
            inner,
            faults: SimIo::new(),
        }
    }

    /// Inject `fault` into the next `times` matching operations on files
    /// whose path ends with the string `suffix` (any file if `None`).
    pub fn inject(&self, suffix: Option<&str>, fault: Fault, times: usize) {
        self.faults
            .lock()
            .faults
            .push((suffix.map(PathBuf::from), fault, times));
    }

    /// Take a scripted fault whose filter is a string suffix of `path`
    /// (`".log"`, `"MANIFEST.tmp"`, a file name).
    fn take(&self, path: &Path, want: impl Fn(Fault) -> bool) -> Option<Fault> {
        let mut st = self.faults.lock();
        let full = path.to_string_lossy();
        let i = st.faults.iter().position(|(p, f, n)| {
            *n > 0
                && want(*f)
                && p.as_deref()
                    .is_none_or(|p| full.ends_with(&*p.to_string_lossy()))
        })?;
        let f = st.faults[i].1;
        st.faults[i].2 -= 1;
        if st.faults[i].2 == 0 {
            st.faults.remove(i);
        }
        Some(f)
    }
}

impl IoEngine for FaultyIo {
    fn name(&self) -> &'static str {
        "faulty"
    }

    fn open(&self, path: &Path) -> Result<FileRef> {
        self.inner.open(path)
    }

    fn forget(&self, file: &FileRef) {
        self.inner.forget(file);
    }

    fn submit(&self, reqs: Vec<ReadReq>) -> Submission {
        let stats = &self.faults.stats;
        let results = reqs
            .into_iter()
            .map(|q| {
                if let Some(Fault::LatencySpike(d)) =
                    self.take(&q.file.path, |f| matches!(f, Fault::LatencySpike(_)))
                {
                    std::thread::sleep(d);
                }
                read_loop(&q.file.path, q.file.len, q.offset, q.len, stats, |b, o| {
                    let fault = self.take(&q.file.path, |f| {
                        matches!(f, Fault::ShortRead | Fault::ReadEio)
                    });
                    let n = match fault {
                        Some(Fault::ReadEio) => return Err(os(5)),
                        Some(_) => (b.len() / 2).max(1),
                        None => b.len(),
                    };
                    let v = read_exact_at(&*self.inner, &q.file, o, n)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                    b[..n].copy_from_slice(&v);
                    Ok(n)
                })
            })
            .collect();
        Submission { results }
    }

    fn complete(&self, s: Submission) -> Vec<Result<Vec<u8>>> {
        s.results
    }

    fn create_new(&self, path: &Path) -> Result<Arc<dyn WriteFile>> {
        Ok(Arc::new(FaultyWriteFile {
            inner: self.inner.create_new(path)?,
            io: self.clone(),
        }))
    }

    fn open_write(&self, path: &Path) -> Result<Arc<dyn WriteFile>> {
        Ok(Arc::new(FaultyWriteFile {
            inner: self.inner.open_write(path)?,
            io: self.clone(),
        }))
    }

    fn stats(&self) -> IoStats {
        let inner = self.inner.stats();
        let mut s = self.faults.stats.snapshot(inner.files_open);
        s.opens = inner.opens;
        s.writes = inner.writes;
        s.bytes_written = inner.bytes_written;
        s.syncs = inner.syncs;
        s.max_open_files = inner.max_open_files;
        s
    }
}

/// Write side of [`FaultyIo`]: [`Fault::ShortWrite`], [`Fault::Enospc`] and
/// [`Fault::FsyncEio`] (the fsync is not performed: the data is not durable).
#[derive(Debug)]
struct FaultyWriteFile {
    inner: Arc<dyn WriteFile>,
    io: FaultyIo,
}

impl WriteFile for FaultyWriteFile {
    fn path(&self) -> &Path {
        self.inner.path()
    }

    fn write_at(&self, data: &[u8], offset: u64) -> Result<usize> {
        let path = self.inner.path().to_path_buf();
        if let Some(Fault::LatencySpike(d)) =
            self.io.take(&path, |f| matches!(f, Fault::LatencySpike(_)))
        {
            std::thread::sleep(d);
        }
        match self
            .io
            .take(&path, |f| matches!(f, Fault::ShortWrite | Fault::Enospc))
        {
            Some(Fault::Enospc) => Err(Error::io(format!("writing {}", path.display()), os(28))),
            Some(_) => self
                .inner
                .write_at(&data[..(data.len() / 2).max(1).min(data.len())], offset),
            None => self.inner.write_at(data, offset),
        }
    }

    fn sync(&self, data_only: bool) -> Result<()> {
        let path = self.inner.path().to_path_buf();
        if self.io.take(&path, |f| f == Fault::FsyncEio).is_some() {
            return Err(Error::io(format!("fsync {}", path.display()), os(5)));
        }
        self.inner.sync(data_only)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        self.inner.set_len(len)
    }

    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn device_gate_bounds_concurrent_calls() {
        let g = Arc::new(DeviceGates::new(3));
        let gate = g.gate(7);
        let threads: Vec<_> = (0..16)
            .map(|_| {
                let (g, gate) = (Arc::clone(&g), Arc::clone(&gate));
                std::thread::spawn(move || {
                    for _ in 0..5 {
                        let _p = g.acquire(&gate);
                        std::thread::sleep(Duration::from_millis(2));
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let mut s = IoStats::default();
        g.fill(&mut s);
        assert_eq!(s.max_inflight_per_device, 3);
        assert!(s.inflight_peak <= 3 && s.inflight_peak >= 2, "{s:?}");
        assert!(s.inflight_waits > 0, "{s:?}");
        assert_eq!(s.inflight, 0);
        // Devices are independent.
        let a = g.gate(1);
        let b = g.gate(2);
        let _pa = (g.acquire(&a), g.acquire(&a), g.acquire(&a));
        let _pb = g.acquire(&b);
    }

    /// Cost of a permit next to the read it guards (run: `cargo test
    /// --release -p stratadb-common --lib io::tests::gate_cost -- --ignored
    /// --nocapture`).
    #[test]
    #[ignore = "measurement, not a check"]
    fn gate_cost() {
        let g = DeviceGates::new(128);
        let gate = g.gate(1);
        let n = 2_000_000u32;
        let t = std::time::Instant::now();
        for _ in 0..n {
            std::hint::black_box(g.acquire(&gate));
        }
        let permit = t.elapsed().as_nanos() as f64 / f64::from(n);
        let dir = tempfile::tempdir().unwrap();
        let io = PreadEngine::new(16);
        let path = dir.path().join("f");
        let w = io.create_new(&path).unwrap();
        write_all_at(&*w, &vec![7u8; 1 << 20], 0).unwrap();
        let r = io.open(&path).unwrap();
        let reads = 200_000u32;
        let t = std::time::Instant::now();
        for i in 0..reads {
            let off = u64::from(i % 255) * 4096;
            std::hint::black_box(read_exact_at(&io, &r, off, 4096).unwrap());
        }
        let read = t.elapsed().as_nanos() as f64 / f64::from(reads);
        println!("permit acquire+release {permit:.1} ns; 4 KiB cached pread through the engine {read:.0} ns ({:.1}% of it)", permit / read * 100.0);
    }

    #[test]
    fn engine_reads_writes_and_syncs_share_the_device_bound() {
        let dir = tempfile::tempdir().unwrap();
        let io = Arc::new(PreadEngine::with_inflight_limit(16, 1));
        let path = dir.path().join("data");
        let data: Vec<u8> = (0..64 * 1024u32).map(|i| (i % 251) as u8).collect();
        {
            let f = io.create_new(&path).unwrap();
            write_all_at(&*f, &data, 0).unwrap();
            f.sync(true).unwrap();
        }
        let r = io.open(&path).unwrap();
        let log = dir.path().join("log");
        let w = io.create_new(&log).unwrap();
        let readers: Vec<_> = (0..12)
            .map(|t| {
                let (io, r, data) = (Arc::clone(&io), r.clone(), data.clone());
                std::thread::spawn(move || {
                    for i in 0..200usize {
                        let off = (t * 977 + i * 131) % (data.len() - 4096);
                        let got = read_exact_at(&*io, &r, off as u64, 4096).unwrap();
                        assert_eq!(got, &data[off..off + 4096]);
                    }
                })
            })
            .collect();
        for i in 0..100u64 {
            write_all_at(&*w, b"0123456789", i * 10).unwrap();
            w.sync(true).unwrap();
        }
        for t in readers {
            t.join().unwrap();
        }
        let s = io.stats();
        assert_eq!(s.max_inflight_per_device, 1);
        assert_eq!(s.inflight_peak, 1, "{s:?}");
        assert_eq!(s.inflight, 0);
        assert!(s.reads >= 2400 && s.syncs >= 100, "{s:?}");
    }

    /// Behaviour every backend must share.
    fn conformance(io: &dyn IoEngine, dir: &Path) {
        let p = dir.join("f.bin");
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let w = io.create_new(&p).unwrap();
        write_all_at(&*w, &data[..6000], 0).unwrap();
        write_all_at(&*w, &data[6000..], 6000).unwrap();
        w.sync(false).unwrap();
        drop(w);
        let f = io.open(&p).unwrap();
        assert_eq!(f.len(), 10_000);
        assert_eq!(read_exact_at(io, &f, 0, 10_000).unwrap(), data);
        assert_eq!(read_exact_at(io, &f, 9_990, 10).unwrap(), &data[9_990..]);
        assert_eq!(read_exact_at(io, &f, 10_000, 0).unwrap(), Vec::<u8>::new());
        // Past the end is an error, not a short read.
        let e = read_exact_at(io, &f, 9_995, 10).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Corruption, "{e}");
        // Batches complete in order.
        let reqs = (0..5)
            .map(|i| ReadReq {
                file: f.clone(),
                offset: i * 100,
                len: 7,
            })
            .collect();
        let done = io.complete(io.submit(reqs));
        for (i, r) in done.into_iter().enumerate() {
            assert_eq!(r.unwrap(), &data[i * 100..i * 100 + 7]);
        }
        assert_eq!(read_file(io, &p).unwrap(), data);
        assert!(io.open(&dir.join("missing")).is_err());
        // create_new refuses an existing file; open_write needs one.
        assert!(io.create_new(&p).is_err());
        assert!(io.open_write(&dir.join("missing")).is_err());
        let w = io.open_write(&p).unwrap();
        assert_eq!(w.len().unwrap(), 10_000);
        write_all_at(&*w, b"tail", 10_000).unwrap();
        w.set_len(5_000).unwrap();
        w.sync(true).unwrap();
        assert_eq!(w.len().unwrap(), 5_000);
        drop(w);
        assert_eq!(read_file(io, &p).unwrap(), &data[..5_000]);
        let s = io.stats();
        assert!(s.reads >= 8 && s.bytes_read >= 20_000 && s.syncs <= s.writes + 1);
    }

    #[test]
    fn pread_backend_conforms() {
        let d = tempfile::tempdir().unwrap();
        conformance(&PreadEngine::new(16), d.path());
    }

    #[test]
    fn sim_backend_conforms() {
        conformance(&SimIo::new(), Path::new("/sim"));
    }

    #[test]
    fn fd_budget_lru_and_identity() {
        let d = tempfile::tempdir().unwrap();
        let io = PreadEngine::new(8);
        let refs: Vec<FileRef> = (0..40)
            .map(|i| {
                let p = d.path().join(format!("{i}"));
                std::fs::write(&p, vec![i as u8; 64]).unwrap();
                io.open(&p).unwrap()
            })
            .collect();
        assert!(io.stats().files_open <= 8, "{:?}", io.stats());
        // Evicted files reopen transparently.
        for (i, r) in refs.iter().enumerate() {
            assert_eq!(read_exact_at(&io, r, 0, 64).unwrap(), vec![i as u8; 64]);
        }
        let s = io.stats();
        assert!(s.files_open <= 8 && s.evictions >= 32, "{s:?}");
        // A replaced file (new inode at the same path) is never read
        // through an old reference.
        let p0 = d.path().join("0");
        let tmp = d.path().join("0.new");
        std::fs::write(&tmp, vec![99u8; 64]).unwrap();
        std::fs::rename(&tmp, &p0).unwrap();
        // Force eviction of file 0 so it must be reopened.
        for r in &refs[1..] {
            read_exact_at(&io, r, 0, 1).unwrap();
        }
        let e = read_exact_at(&io, &refs[0], 0, 64).unwrap_err();
        assert!(e.to_string().contains("replaced"), "{e}");
        // A new reference to the new file works.
        let fresh = io.open(&p0).unwrap();
        assert_eq!(read_exact_at(&io, &fresh, 0, 64).unwrap(), vec![99u8; 64]);
    }

    #[test]
    fn short_reads_are_resubmitted_and_eio_is_retried_once() {
        let io = SimIo::new();
        let p = Path::new("/sim/a");
        let data: Vec<u8> = (0..4096u32).map(|i| (i * 7) as u8).collect();
        io.put(p, &data);
        let f = io.open(p).unwrap();
        io.inject(Some(p), Fault::ShortRead, 5);
        assert_eq!(read_exact_at(&io, &f, 0, 4096).unwrap(), data);
        assert_eq!(io.stats().short_reads_resubmitted, 5);
        // One EIO: retried, succeeds.
        io.inject(Some(p), Fault::ReadEio, 1);
        assert_eq!(read_exact_at(&io, &f, 100, 10).unwrap(), &data[100..110]);
        assert_eq!(io.stats().read_retries, 1);
        // Two EIOs: error names the file, offset and length.
        io.inject(Some(p), Fault::ReadEio, 2);
        let e = read_exact_at(&io, &f, 100, 10).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Io);
        let m = e.to_string();
        assert!(
            m.contains("/sim/a") && m.contains("offset 100") && m.contains("10 bytes"),
            "{m}"
        );
        assert_eq!(io.stats().read_errors, 1);
        // Faults are scoped to their path.
        let q = Path::new("/sim/b");
        io.put(q, b"xyz");
        io.inject(Some(q), Fault::ReadEio, 2);
        assert_eq!(read_exact_at(&io, &f, 0, 3).unwrap(), &data[..3]);
        // A file that shrank: a short read before the known end is an error.
        let fq = io.open(q).unwrap();
        io.put(q, b"x");
        assert!(read_exact_at(&io, &fq, 0, 3).is_err());
    }

    #[test]
    fn write_faults_and_crash_semantics() {
        let io = SimIo::new();
        let p = Path::new("/sim/w");
        let w = io.create_new(p).unwrap();
        // Short writes are completed by write_all_at.
        io.inject(Some(p), Fault::ShortWrite, 3);
        write_all_at(&*w, &[1u8; 2048], 0).unwrap();
        assert_eq!(io.contents(p).unwrap(), vec![1u8; 2048]);
        // ENOSPC is an error.
        io.inject(Some(p), Fault::Enospc, 1);
        let e = write_all_at(&*w, &[2u8; 10], 2048).unwrap_err();
        assert!(
            e.to_string().contains("28") || e.to_string().to_lowercase().contains("space"),
            "{e}"
        );
        w.sync(false).unwrap();
        // Unsynced writes: visible now, may vanish at a crash.
        write_all_at(&*w, &[3u8; 1024], 2048).unwrap();
        assert_eq!(io.contents(p).unwrap().len(), 3072);
        // fsync EIO: reported once; the dirty data is gone after a crash.
        io.inject(Some(p), Fault::FsyncEio, 1);
        assert!(w.sync(false).is_err());
        assert!(w.sync(false).is_ok(), "the error is reported once");
        io.crash(1);
        assert_eq!(
            io.contents(p).unwrap(),
            vec![1u8; 2048],
            "lost after fsync EIO"
        );
        // Latency spikes delay the operation.
        io.inject(Some(p), Fault::LatencySpike(Duration::from_millis(30)), 1);
        let t = std::time::Instant::now();
        let f = io.open(p).unwrap();
        read_exact_at(&io, &f, 0, 1).unwrap();
        assert!(t.elapsed() >= Duration::from_millis(30));
    }

    #[test]
    fn crash_keeps_durable_data_and_tears_or_reorders_the_rest() {
        let mut outcomes = std::collections::BTreeSet::new();
        for seed in 0..200 {
            let io = SimIo::new();
            let p = Path::new("/sim/c");
            let w = io.create_new(p).unwrap();
            write_all_at(&*w, &[1u8; 1024], 0).unwrap();
            w.sync(false).unwrap();
            write_all_at(&*w, &[2u8; 2048], 1024).unwrap();
            write_all_at(&*w, &[3u8; 1024], 3072).unwrap();
            io.crash(seed);
            let c = io.contents(p).unwrap();
            // Durable prefix always survives.
            assert_eq!(&c[..1024], &[1u8; 1024][..], "seed {seed}");
            // Anything beyond is whole sectors of the unsynced writes.
            for (i, b) in c.iter().enumerate().skip(1024) {
                assert!(*b == 0 || *b == 2 || *b == 3, "seed {seed} byte {i}");
            }
            assert_eq!(c.len() % SECTOR, 0);
            outcomes.insert(c.len());
            // Deterministic per seed.
            let io2 = SimIo::new();
            let w2 = io2.create_new(p).unwrap();
            write_all_at(&*w2, &[1u8; 1024], 0).unwrap();
            w2.sync(false).unwrap();
            write_all_at(&*w2, &[2u8; 2048], 1024).unwrap();
            write_all_at(&*w2, &[3u8; 1024], 3072).unwrap();
            io2.crash(seed);
            assert_eq!(io2.contents(p).unwrap(), c);
        }
        // Lost, torn, reordered (a later write without the earlier one) all occur.
        assert!(outcomes.contains(&1024), "everything unsynced lost");
        assert!(outcomes.len() >= 4, "{outcomes:?}");
    }

    #[test]
    fn faulty_decorator_injects_into_a_real_backend() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("seg-1.sseg");
        let data: Vec<u8> = (0..5000u32).map(|i| (i % 13) as u8).collect();
        std::fs::write(&p, &data).unwrap();
        let io = FaultyIo::new(Arc::new(PreadEngine::new(16)));
        conformance(&io, d.path());
        let f = io.open(&p).unwrap();
        io.inject(Some("seg-1.sseg"), Fault::ShortRead, 4);
        assert_eq!(read_exact_at(&io, &f, 0, 5000).unwrap(), data);
        io.inject(Some("seg-1.sseg"), Fault::ReadEio, 1);
        assert_eq!(read_exact_at(&io, &f, 10, 5).unwrap(), &data[10..15]);
        io.inject(Some("seg-1.sseg"), Fault::ReadEio, 2);
        assert!(read_exact_at(&io, &f, 10, 5)
            .unwrap_err()
            .to_string()
            .contains("seg-1.sseg"));
        io.inject(Some("other"), Fault::ReadEio, 5);
        assert!(
            read_exact_at(&io, &f, 0, 5).is_ok(),
            "scoped by path suffix"
        );
        let s = io.stats();
        assert!(
            s.short_reads_resubmitted >= 4 && s.read_retries >= 2 && s.read_errors >= 1,
            "{s:?}"
        );
    }

    #[test]
    fn atomic_write_through_the_engine_replaces_contents() {
        let d = tempfile::tempdir().unwrap();
        let io = PreadEngine::new(16);
        let p = d.path().join("MANIFEST");
        atomic_write(&io, &p, b"one", "t").unwrap();
        atomic_write(&io, &p, b"two", "t").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"two");
        assert!(!crate::fsutil::tmp_path(&p).exists());
        assert!(io.stats().syncs >= 2 && io.stats().writes >= 2);
    }

    #[test]
    fn faulty_write_side() {
        let d = tempfile::tempdir().unwrap();
        let io = FaultyIo::new(Arc::new(PreadEngine::new(16)));
        let p = d.path().join("wal-1.log");
        let w = io.create_new(&p).unwrap();
        io.inject(Some("wal-1.log"), Fault::ShortWrite, 3);
        write_all_at(&*w, &[7u8; 4096], 0).unwrap();
        assert_eq!(
            std::fs::read(&p).unwrap(),
            vec![7u8; 4096],
            "short writes completed"
        );
        io.inject(Some("wal-1.log"), Fault::Enospc, 1);
        let e = write_all_at(&*w, &[1u8; 10], 4096).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::ResourceExhausted, "{e}");
        io.inject(Some("wal-1.log"), Fault::FsyncEio, 1);
        assert_eq!(w.sync(true).unwrap_err().kind(), ErrorKind::Io);
        w.sync(true).unwrap();
        // atomic_write surfaces a failed fsync and leaves the target alone.
        let m = d.path().join("MANIFEST");
        atomic_write(&io, &m, b"v1", "t").unwrap();
        io.inject(Some("MANIFEST.tmp"), Fault::FsyncEio, 1);
        assert!(atomic_write(&io, &m, b"v2", "t").is_err());
        assert_eq!(std::fs::read(&m).unwrap(), b"v1");
    }

    #[test]
    fn nofile_limit_can_be_read_and_raised() {
        let (old, new) = raise_nofile_limit().unwrap();
        assert!(new >= old);
        assert!(nofile_soft_limit().unwrap_or(u64::MAX) >= new.min(old));
        assert!(default_max_open_files() >= 64);
    }
}
