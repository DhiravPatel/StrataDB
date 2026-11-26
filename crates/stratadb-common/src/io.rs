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
}

/// A file being written.
pub trait WriteFile: Send + std::fmt::Debug {
    /// Write some prefix of `data` at `offset`; returns bytes written
    /// (callers loop: see [`write_all_at`]).
    fn write_at(&mut self, data: &[u8], offset: u64) -> Result<usize>;
    /// Make every written byte durable (data and the length).
    fn sync(&mut self) -> Result<()>;
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
    /// Create (or truncate) `path` for writing.
    fn create(&self, path: &Path) -> Result<Box<dyn WriteFile>>;
    fn stats(&self) -> IoStats;
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

/// Read a whole file (small metadata files: manifests, backup manifests).
pub fn read_file(io: &dyn IoEngine, path: &Path) -> Result<Vec<u8>> {
    let f = io.open(path)?;
    let len = usize::try_from(f.len()).map_err(|_| Error::invalid_input("file too large"))?;
    read_exact_at(io, &f, 0, len)
}

/// Write all of `data` at `offset`, resubmitting after short writes.
pub fn write_all_at(f: &mut dyn WriteFile, data: &[u8], offset: u64) -> Result<()> {
    let mut done = 0usize;
    while done < data.len() {
        let n = f.write_at(&data[done..], offset + done as u64)?;
        if n == 0 {
            return Err(Error::new(ErrorKind::Io, "write made no progress"));
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
            max_open_files: 0,
        }
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
    /// An engine keeping at most `max_open_files` files open (at least 8).
    pub fn new(max_open_files: usize) -> Self {
        Self {
            max_open: max_open_files.max(8),
            cache: Mutex::new(FdCache::default()),
            stats: Counters::default(),
        }
    }

    pub fn max_open_files(&self) -> usize {
        self.max_open
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
        let results = reqs
            .into_iter()
            .map(|q| {
                let f = self.file(&q.file)?;
                read_loop(
                    &q.file.path,
                    q.file.len,
                    q.offset,
                    q.len,
                    &self.stats,
                    |b, o| f.read_at(b, o),
                )
            })
            .collect();
        Submission { results }
    }

    fn complete(&self, s: Submission) -> Vec<Result<Vec<u8>>> {
        s.results
    }

    fn create(&self, path: &Path) -> Result<Box<dyn WriteFile>> {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| Error::io(format!("creating {}", path.display()), e))?;
        Ok(Box::new(PwriteFile {
            f,
            path: path.to_path_buf(),
        }))
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
        s
    }
}

#[derive(Debug)]
struct PwriteFile {
    f: File,
    path: PathBuf,
}

impl WriteFile for PwriteFile {
    fn write_at(&mut self, data: &[u8], offset: u64) -> Result<usize> {
        loop {
            match self.f.write_at(data, offset) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::io(format!("writing {}", self.path.display()), e)),
            }
        }
    }

    fn sync(&mut self) -> Result<()> {
        self.f
            .sync_all()
            .map_err(|e| Error::io(format!("fsync {}", self.path.display()), e))
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
        Arc::new(PreadEngine::new(max))
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
struct SimFile {
    ino: u64,
    /// What reads see (page cache).
    data: Vec<u8>,
    /// What survives a crash.
    durable: Vec<u8>,
    /// Writes since the last successful fsync, in order.
    pending: Vec<(u64, Vec<u8>)>,
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
            for (k, (off, data)) in pending.into_iter().take(keep).enumerate() {
                let torn = k + 1 == keep && next() % 2 == 0;
                let n = if torn {
                    let sectors = data.len().div_ceil(SECTOR);
                    ((next() % sectors as u64) as usize * SECTOR).min(data.len())
                } else {
                    data.len()
                };
                apply(&mut img, off, &data[..n]);
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

    fn create(&self, path: &Path) -> Result<Box<dyn WriteFile>> {
        self.put(path, &[]);
        Ok(Box::new(SimWriteFile {
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
    fn write_at(&mut self, data: &[u8], offset: u64) -> Result<usize> {
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
        f.pending.push((offset, data[..n].to_vec()));
        self.sim.stats.writes.fetch_add(1, Ordering::Relaxed);
        self.sim
            .stats
            .bytes_written
            .fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    fn sync(&mut self) -> Result<()> {
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
        for (off, data) in std::mem::take(&mut f.pending) {
            apply(&mut f.durable, off, &data);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fault decorator

/// Injects read faults ([`Fault::ShortRead`], [`Fault::ReadEio`],
/// [`Fault::LatencySpike`]) into reads of any engine, for tests of upper
/// layers whose writes still go to disk. The same retry and resubmission
/// loop as the backends handles the injected faults.
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

    /// Inject `fault` into the next `times` reads of files whose path ends
    /// with `suffix` (any file if `None`).
    pub fn inject(&self, suffix: Option<&str>, fault: Fault, times: usize) {
        self.faults
            .lock()
            .faults
            .push((suffix.map(PathBuf::from), fault, times));
    }

    /// Take a scripted fault whose filter is a suffix of `path`.
    fn take(&self, path: &Path, want: impl Fn(Fault) -> bool) -> Option<Fault> {
        let mut st = self.faults.lock();
        let i = st.faults.iter().position(|(p, f, n)| {
            *n > 0 && want(*f) && p.as_deref().is_none_or(|p| path.ends_with(p))
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

    fn create(&self, path: &Path) -> Result<Box<dyn WriteFile>> {
        self.inner.create(path)
    }

    fn stats(&self) -> IoStats {
        let mut s = self.faults.stats.snapshot(self.inner.stats().files_open);
        s.opens = self.inner.stats().opens;
        s
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// Behaviour every backend must share.
    fn conformance(io: &dyn IoEngine, dir: &Path) {
        let p = dir.join("f.bin");
        let data: Vec<u8> = (0..10_000u32).map(|i| (i % 251) as u8).collect();
        let mut w = io.create(&p).unwrap();
        write_all_at(w.as_mut(), &data[..6000], 0).unwrap();
        write_all_at(w.as_mut(), &data[6000..], 6000).unwrap();
        w.sync().unwrap();
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
        let mut w = io.create(p).unwrap();
        // Short writes are completed by write_all_at.
        io.inject(Some(p), Fault::ShortWrite, 3);
        write_all_at(w.as_mut(), &[1u8; 2048], 0).unwrap();
        assert_eq!(io.contents(p).unwrap(), vec![1u8; 2048]);
        // ENOSPC is an error.
        io.inject(Some(p), Fault::Enospc, 1);
        let e = write_all_at(w.as_mut(), &[2u8; 10], 2048).unwrap_err();
        assert!(
            e.to_string().contains("28") || e.to_string().to_lowercase().contains("space"),
            "{e}"
        );
        w.sync().unwrap();
        // Unsynced writes: visible now, may vanish at a crash.
        write_all_at(w.as_mut(), &[3u8; 1024], 2048).unwrap();
        assert_eq!(io.contents(p).unwrap().len(), 3072);
        // fsync EIO: reported once; the dirty data is gone after a crash.
        io.inject(Some(p), Fault::FsyncEio, 1);
        assert!(w.sync().is_err());
        assert!(w.sync().is_ok(), "the error is reported once");
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
            let mut w = io.create(p).unwrap();
            write_all_at(w.as_mut(), &[1u8; 1024], 0).unwrap();
            w.sync().unwrap();
            write_all_at(w.as_mut(), &[2u8; 2048], 1024).unwrap();
            write_all_at(w.as_mut(), &[3u8; 1024], 3072).unwrap();
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
            let mut w2 = io2.create(p).unwrap();
            write_all_at(w2.as_mut(), &[1u8; 1024], 0).unwrap();
            w2.sync().unwrap();
            write_all_at(w2.as_mut(), &[2u8; 2048], 1024).unwrap();
            write_all_at(w2.as_mut(), &[3u8; 1024], 3072).unwrap();
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
    fn nofile_limit_can_be_read_and_raised() {
        let (old, new) = raise_nofile_limit().unwrap();
        assert!(new >= old);
        assert!(nofile_soft_limit().unwrap_or(u64::MAX) >= new.min(old));
        assert!(default_max_open_files() >= 64);
    }
}
