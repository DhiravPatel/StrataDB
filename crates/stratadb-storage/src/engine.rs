//! The single-node storage engine.
//!
//! # Write path (spec §7.1 steps 10–14)
//! 1. Backpressure: refuse/stall while the frozen-memtable backlog is full.
//! 2. Encode the batch as one WAL record (atomic batch commit).
//! 3. Append to the WAL and wait for the durability boundary.
//! 4. Insert into the active memtable (now query-visible).
//! 5. Acknowledge with the commit sequence number.
//!
//! Steps 3–4 run while holding the *write gate* in shared mode. Freezing the
//! memtable takes the gate exclusively, so at freeze time every sequence
//! number the WAL has assigned is already in a memtable. This is what makes
//! `flushed_through_seq = wal.last_seq()` at freeze time safe: no record with
//! a smaller sequence number can arrive later and be lost by WAL reclamation.
//!
//! # Flush protocol (spec §8.6, ticket D-007)
//! freeze → write one segment per tenant (tmp + fsync + rename + dir fsync)
//! → reopen and validate each segment → durably commit a manifest that adds
//! the segments and advances `flushed_through_seq` → atomically swap the
//! frozen memtable for the segments in the visible state → reclaim WAL.
//!
//! Crash at any point: before the manifest commit, segment files are
//! unreferenced orphans (deleted on recovery) and the WAL still holds the
//! rows; after it, recovery skips WAL records `<= flushed_through_seq`.
//!
//! # Failure policy
//! WAL write/sync failures and manifest commit failures put the engine into a
//! *failed* state: writes and flushes are refused until restart, because the
//! durable state is no longer known. Queries remain available.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};
use stratadb_common::{failpoint, fsutil, time, Error, ErrorKind, IoResultExt, Result, TenantId};
use stratadb_schema::codec::{decode_batch, encode_batch, DecodeLimits};
use stratadb_schema::column::EventColumns;
use stratadb_schema::Event;
use stratadb_segment::{RowRef, SegmentReader};
use stratadb_wal::{Durability, RecordType, RecoveryReport, Wal, WalOptions, WalStats};

mod backup;
mod maintenance;

pub use backup::{
    restore_backup, verify_backup, BackupFile, BackupManifest, BackupReport, RestoreReport,
};
pub use maintenance::{
    CompactionOptions, CompactionReport, IndexCheck, IndexRebuildReport, RetentionReport,
    SegmentIndexes, UpgradeReport,
};

use crate::idempotency::{self, BatchMeta, DedupIndex, IdempotencyKey, Reservation};
use crate::manifest::RetentionPolicy;
use crate::manifest::{segment_path, tenant_dir, Manifest, SegmentEntry, SegmentState};
use crate::memtable::{FrozenMemtable, MemView, Memtable};

/// Engine configuration. Defaults favour durability (group commit).
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// Storage read path (spec STRATA-SYS-04-001): segment, manifest and
    /// backup reads go through this engine. `None`: the process-wide
    /// portable backend with a bounded LRU of open files (resolved when the
    /// engine opens, so it can be configured first).
    pub io: Option<Arc<dyn stratadb_common::io::IoEngine>>,
    pub data_dir: PathBuf,
    pub wal: WalOptions,
    /// Freeze the active memtable when it exceeds this many bytes ...
    pub memtable_max_bytes: usize,
    /// ... or this many rows ...
    pub memtable_max_rows: usize,
    /// ... or its oldest row is this old.
    pub memtable_max_age: Duration,
    /// Writers stall (then fail with `ResourceExhausted`) while this many
    /// frozen memtables await flush.
    pub max_frozen_memtables: usize,
    pub write_stall_timeout: Duration,
    /// Run the background flush / sync thread.
    pub background: bool,
    /// In `Async` durability, fsync the WAL at least this often.
    pub async_sync_interval: Duration,
    /// Fully verify every segment (all chunk checksums) at open, not only footers.
    pub verify_segments_on_open: bool,
    /// Flush memtables during `close()`.
    pub flush_on_close: bool,
    /// How long idempotency keys are remembered (spec §7.3).
    pub idempotency_window: Duration,
    /// Maximum remembered keys; the oldest are evicted early beyond this.
    pub idempotency_max_keys: usize,
    pub compaction: CompactionOptions,
    /// How often the background thread applies retention policies.
    pub retention_interval: Duration,
    /// Retention for tenants without an explicit policy (`None` = keep forever).
    pub default_retention: Option<Duration>,
    /// Writes are refused while free space on the data volume is below
    /// `max(disk_reserve_bytes, disk_reserve_ratio × volume size)` (spec §28.4).
    pub disk_reserve_bytes: u64,
    pub disk_reserve_ratio: f64,
    /// Build per-segment Bloom filters (trace_id, span_id, event_id, plus
    /// the string attributes listed here). Spec §12.5.
    pub bloom_enabled: bool,
    pub bloom_attribute_keys: Vec<String>,
    pub bloom_resource_keys: Vec<String>,
    /// Build per-segment trigram filters for `body` (and the string
    /// attributes listed here) to prune `LIKE`/`ILIKE` scans. Spec §12.4.
    pub ngram_enabled: bool,
    pub ngram_attribute_keys: Vec<String>,
    /// LZ4-compress segment chunks (segment format v2, RFC-0001).
    pub segment_compression: bool,
    /// Store hot attribute keys as separate columns (segment v3, RFC-0002).
    pub segment_shredding: bool,
    /// Encryption at rest (ADR-0024): source of the key-encryption key. When
    /// set, new segments, the manifest and WAL segments are encrypted; an
    /// encrypted data directory cannot be opened without it.
    pub encryption: Option<Arc<dyn stratadb_crypto::KeyProvider>>,
    /// Index in the background thread's name (`sx-maint-NN`), so that several
    /// engines in one process (e.g. the query log's) are told apart.
    pub maint_thread_index: usize,
    /// Ingest memory region (spec VIII.5.2, ARCH-01-006): memtable bytes are
    /// reserved here before the WAL append and released when flushed. When
    /// it is full, writers request a flush and wait up to
    /// `write_stall_timeout`, then fail with a retryable `ResourceExhausted`.
    pub memory: Option<Arc<stratadb_common::memory::MemoryTracker>>,
    /// Cache memory region: decoded Bloom and trigram filters are tracked
    /// here (not evictable yet, so tracked without a limit check).
    pub cache_memory: Option<Arc<stratadb_common::memory::MemoryTracker>>,
    /// Background memory region (compaction, retention and upgrade
    /// rewrites): a rewrite reserves its input rows here first; compaction
    /// is postponed while the region is full.
    pub background_memory: Option<Arc<stratadb_common::memory::MemoryTracker>>,
}

impl EngineOptions {
    /// The configured I/O engine, or the process-wide default.
    pub fn io_engine(&self) -> Arc<dyn stratadb_common::io::IoEngine> {
        self.io
            .clone()
            .unwrap_or_else(stratadb_common::io::default_io)
    }

    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            io: None,
            data_dir: data_dir.into(),
            wal: WalOptions::default(),
            memtable_max_bytes: 64 * 1024 * 1024,
            memtable_max_rows: 1_000_000,
            memtable_max_age: Duration::from_secs(60),
            max_frozen_memtables: 2,
            write_stall_timeout: Duration::from_secs(5),
            background: true,
            async_sync_interval: Duration::from_secs(1),
            verify_segments_on_open: false,
            flush_on_close: true,
            idempotency_window: Duration::from_secs(24 * 3600),
            idempotency_max_keys: 100_000,
            compaction: CompactionOptions::default(),
            retention_interval: Duration::from_secs(60),
            default_retention: None,
            disk_reserve_bytes: 512 * 1024 * 1024,
            disk_reserve_ratio: 0.02,
            bloom_enabled: true,
            bloom_attribute_keys: ["request_id", "user_id", "session_id", "correlation_id"]
                .map(String::from)
                .to_vec(),
            bloom_resource_keys: Vec::new(),
            ngram_enabled: true,
            ngram_attribute_keys: Vec::new(),
            segment_compression: true,
            segment_shredding: true,
            encryption: None,
            maint_thread_index: 0,
            memory: None,
            cache_memory: None,
            background_memory: None,
        }
    }
}

/// Which durability boundary an acknowledged write reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLevel {
    /// Written to the OS page cache (Async mode): survives process crash only.
    WalWritten,
    /// Covered by a successful WAL fsync: survives OS crash and power loss.
    WalSynced,
}

impl CommitLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            CommitLevel::WalWritten => "wal_written",
            CommitLevel::WalSynced => "wal_synced",
        }
    }
}

/// Acknowledgement for a committed batch. Rows are query-visible on return.
#[derive(Debug, Clone)]
pub struct WriteReceipt {
    /// WAL sequence number of the batch (commit identifier).
    pub commit_seq: u64,
    pub accepted: usize,
    pub level: CommitLevel,
    /// True when an idempotency key matched an earlier commit: nothing new
    /// was written and `commit_seq`/`accepted` describe the original commit.
    pub duplicate: bool,
}

/// What recovery did when the engine opened.
#[derive(Debug, Clone, Default)]
pub struct OpenReport {
    pub created: bool,
    pub manifest_version: u64,
    pub segments: usize,
    pub orphan_segments_removed: usize,
    pub segments_quarantined: usize,
    pub tmp_files_removed: usize,
    /// Obsolete (compacted/expired) segment files deleted during recovery.
    pub obsolete_deleted: usize,
    pub wal: RecoveryReport,
    pub rows_recovered_from_wal: u64,
    pub elapsed: Duration,
}

/// A committed segment with an open reader.
#[derive(Debug)]
pub struct SegmentHandle {
    pub entry: SegmentEntry,
    pub reader: SegmentReader,
    bloom: std::sync::OnceLock<Option<Charged<stratadb_segment::bloom::BloomSet>>>,
    ngram: std::sync::OnceLock<Option<Charged<stratadb_segment::ngram::NgramSet>>>,
    /// Where decoded filters are accounted.
    cache: Option<Arc<stratadb_common::memory::MemoryTracker>>,
}

/// A decoded structure and its tracked memory (released with it).
#[derive(Debug)]
struct Charged<T> {
    value: T,
    _charge: Option<stratadb_common::memory::Reservation>,
}

impl<T> Charged<T> {
    fn new(
        value: T,
        bytes: usize,
        cache: Option<&Arc<stratadb_common::memory::MemoryTracker>>,
    ) -> Self {
        Self {
            value,
            _charge: cache.map(|c| stratadb_common::memory::Reservation::forced(c, bytes as u64)),
        }
    }
}

impl SegmentHandle {
    /// The segment's Bloom filters, decoded on first use. `None` if the
    /// segment has none (older writer) or they cannot be decoded (then
    /// queries simply do not use them).
    pub fn bloom(&self) -> Option<&stratadb_segment::bloom::BloomSet> {
        self.bloom
            .get_or_init(|| {
                let x = self.reader.footer().extension(stratadb_segment::bloom::EXT_BLOOM)?;
                match stratadb_segment::bloom::BloomSet::decode(&x.data) {
                    Ok(b) => Some(Charged::new(b, x.data.len(), self.cache.as_ref())),
                    Err(e) => {
                        tracing::warn!(segment = self.entry.id, error = %e, "ignoring undecodable bloom filters");
                        None
                    }
                }
            })
            .as_ref()
            .map(|c| &c.value)
    }

    /// The segment's trigram filters, decoded on first use (`None`: absent
    /// or undecodable — then `LIKE` simply scans).
    pub fn ngram(&self) -> Option<&stratadb_segment::ngram::NgramSet> {
        self.ngram
            .get_or_init(|| {
                let x = self.reader.footer().extension(stratadb_segment::ngram::EXT_NGRAM)?;
                match stratadb_segment::ngram::NgramSet::decode(&x.data) {
                    Ok(n) => Some(Charged::new(n, x.data.len(), self.cache.as_ref())),
                    Err(e) => {
                        tracing::warn!(segment = self.entry.id, error = %e, "ignoring undecodable ngram filters");
                        None
                    }
                }
            })
            .as_ref()
            .map(|c| &c.value)
    }
}

/// Storage used by one tenant.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TenantUsage {
    pub segments: usize,
    pub segment_rows: u64,
    pub segment_bytes: u64,
    /// Approximate in-memory (not yet flushed) bytes.
    pub memtable_bytes: u64,
}

impl TenantUsage {
    pub fn total_bytes(&self) -> u64 {
        self.segment_bytes + self.memtable_bytes
    }
}

/// Result of one flush call.
#[derive(Debug, Clone, Default)]
pub struct FlushReport {
    pub memtables_flushed: usize,
    pub segments_written: usize,
    pub rows_flushed: usize,
    pub bytes_written: u64,
    pub flushed_through_seq: u64,
    pub wal_segments_reclaimed: usize,
}

/// Point-in-time statistics.
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub active_rows: usize,
    pub active_bytes: usize,
    pub frozen_memtables: usize,
    pub segments: usize,
    pub segment_bytes: u64,
    pub segment_rows: u64,
    pub manifest_version: u64,
    pub flushed_through_seq: u64,
    pub wal: WalStats,
    pub failed: Option<String>,
    pub batches_written: u64,
    pub events_written: u64,
    pub flushes: u64,
    pub flush_failures: u64,
    pub write_stalls: u64,
    pub write_rejections: u64,
    pub idempotency_keys: usize,
    pub idempotency_evictions: u64,
    pub duplicates_suppressed: u64,
    /// Obsolete segments still pinned by snapshots (awaiting deletion).
    pub obsolete_segments: usize,
    pub compactions: u64,
    pub segments_deleted: u64,
    pub retention_rows_deleted: u64,
    pub disk_free_bytes: u64,
    pub disk_total_bytes: u64,
    pub disk_write_rejections: u64,
    /// WAL fsync latency and group-commit size distributions.
    pub wal_metrics: stratadb_wal::WalMetrics,
    /// Duration of flushes that wrote at least one memtable.
    pub flush_seconds: stratadb_common::metrics::HistogramSnapshot,
    /// Engine-side INSERT stages (spec VIII.1.2, `strata_insert_stage_seconds`)
    /// in pipeline order: `admission` (backpressure wait), `wal_encode`
    /// (batch encoding), `wal_append`, `group_commit_wait`, `memtable_insert`.
    pub insert_stages: Vec<(&'static str, stratadb_common::metrics::HistogramSnapshot)>,
}

/// A consistent, tenant-scoped read view (spec §9.4): committed segments plus
/// memtable rows visible at the snapshot instant, with no row in both.
#[derive(Debug, Clone)]
pub struct Snapshot {
    tenant: TenantId,
    segments: Vec<Arc<SegmentHandle>>,
    /// Memtable rows visible at the snapshot instant: chunk pointers and
    /// counts (frozen memtables first, then the active one).
    memory: Vec<MemView>,
}

impl Snapshot {
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn segments(&self) -> &[Arc<SegmentHandle>] {
        &self.segments
    }

    /// The committed segments as executor scan sources (spec
    /// STRATA-ARCH-01-003).
    pub fn sources(&self) -> Vec<Arc<dyn crate::scan::ScanSource>> {
        self.segments
            .iter()
            .map(|s| Arc::clone(s) as Arc<dyn crate::scan::ScanSource>)
            .collect()
    }

    /// The visible memtable rows as scan sources of at most `rows` rows
    /// each (windows of column chunks; each carries its chunk's zone map).
    pub fn memory_chunks(&self, rows: usize) -> impl Iterator<Item = crate::scan::MemChunk<'_>> {
        let w = rows.max(1);
        self.memory.iter().flat_map(move |v| {
            v.chunks().flat_map(move |(c, n)| {
                (0..n).step_by(w).map(move |from| {
                    crate::scan::MemChunk::new(v.tenant(), c, from, (from + w).min(n))
                })
            })
        })
    }

    /// Memtable rows visible to this snapshot.
    pub fn memory_row_count(&self) -> usize {
        self.memory.iter().map(MemView::rows).sum()
    }

    /// The visible memtable rows of each memtable (frozen ones first).
    pub fn memory_views(&self) -> &[MemView] {
        &self.memory
    }

    /// Largest WAL sequence number of any row this snapshot can see (0 when
    /// empty). Every write acknowledged before the snapshot was taken has a
    /// sequence number `<=` this value; rows with a larger sequence number
    /// were committed after it.
    pub fn max_visible_seq(&self) -> u64 {
        let seg = self.segments.iter().map(|s| s.entry.max_seq).max();
        let mem = self.memory.iter().map(MemView::max_seq).max();
        seg.max(mem).unwrap_or(0)
    }

    /// Materialize every row, ordered by `(event_time, seq, idx)`.
    pub fn scan_all(&self) -> Result<Vec<(u64, u32, Event)>> {
        let mut out: Vec<(u64, u32, Event)> = Vec::new();
        for s in &self.segments {
            for r in s.reader.read_rows()? {
                if r.event.tenant_id != self.tenant {
                    return Err(Error::corruption(
                        s.reader.path().display().to_string(),
                        "segment contains a row of another tenant",
                    ));
                }
                out.push((r.seq, r.idx, r.event));
            }
        }
        for v in &self.memory {
            out.extend(v.materialize()?);
        }
        out.sort_by_key(|(s, i, e)| (e.event_time, *s, *i));
        Ok(out)
    }
}

#[derive(Debug)]
struct State {
    active: Arc<Memtable>,
    frozen: VecDeque<Arc<FrozenMemtable>>,
    segments: Arc<Vec<Arc<SegmentHandle>>>,
    /// Removed from the visible set; deleted once no snapshot holds them.
    obsolete: Vec<Arc<SegmentHandle>>,
}

#[derive(Debug, Default)]
struct Counters {
    batches: AtomicU64,
    events: AtomicU64,
    flushes: AtomicU64,
    flush_failures: AtomicU64,
    stalls: AtomicU64,
    rejections: AtomicU64,
    duplicates: AtomicU64,
    compactions: AtomicU64,
    segments_deleted: AtomicU64,
    retention_rows_deleted: AtomicU64,
    disk_rejections: AtomicU64,
}

/// Cached free-space measurement of the data volume.
#[derive(Debug)]
struct DiskState {
    free: u64,
    total: u64,
    checked: Option<Instant>,
}

#[derive(Debug, Default)]
struct BgSignal {
    shutdown: bool,
    flush_requested: bool,
}

struct Shared {
    opts: EngineOptions,
    dir: PathBuf,
    wal: Wal,
    write_gate: RwLock<()>,
    state: Mutex<State>,
    /// Current durable manifest; its mutex also serializes flushes.
    manifest: Mutex<Manifest>,
    failed: Mutex<Option<String>>,
    stall_cv: Condvar,
    bg: Mutex<BgSignal>,
    bg_cv: Condvar,
    counters: Counters,
    flush_seconds: stratadb_common::metrics::Histogram,
    stage_admission: stratadb_common::metrics::Histogram,
    stage_encode: stratadb_common::metrics::Histogram,
    stage_memtable: stratadb_common::metrics::Histogram,
    open_report: OpenReport,
    dedup: Mutex<DedupIndex>,
    disk: Mutex<DiskState>,
    _lock_file: File,
    /// Encryption keyring (ADR-0024); `None` for a plaintext directory.
    keys: Option<Arc<stratadb_crypto::Keyring>>,
    /// Memtable bytes reserved on the ingest tracker (released on flush and
    /// when the engine is dropped).
    ingest: IngestAccount,
}

/// Bytes this engine holds on the ingest memory tracker.
#[derive(Debug, Default)]
struct IngestAccount {
    tracker: Option<Arc<stratadb_common::memory::MemoryTracker>>,
    held: AtomicU64,
}

impl IngestAccount {
    fn release(&self, bytes: u64) {
        if let Some(t) = &self.tracker {
            let prev = self
                .held
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |h| {
                    Some(h.saturating_sub(bytes))
                })
                .unwrap_or(0);
            t.release(prev.min(bytes));
        }
    }

    fn add(&self, bytes: u64) {
        self.held.fetch_add(bytes, Ordering::AcqRel);
    }
}

impl Drop for IngestAccount {
    fn drop(&mut self) {
        if let Some(t) = &self.tracker {
            t.release(*self.held.get_mut());
        }
    }
}

/// The storage engine. Cheap to share by reference across threads.
pub struct Engine {
    shared: Arc<Shared>,
    bg_thread: Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("dir", &self.shared.dir)
            .finish_non_exhaustive()
    }
}

const LOCK_FILE: &str = "LOCK";
const WAL_DIR: &str = "wal";
const SEGMENTS_DIR: &str = "segments";
const QUARANTINE_DIR: &str = "quarantine";

// The LOCK file only carries an advisory lock (no data), so it is opened
// directly rather than through the I/O engine.
#[allow(clippy::disallowed_methods)]
fn acquire_lock(dir: &Path) -> Result<File> {
    let p = dir.join(LOCK_FILE);
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(fsutil::FILE_MODE)
        .open(&p)
        .ctx(|| format!("opening {}", p.display()))?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(std::fs::TryLockError::WouldBlock) => Err(Error::new(
            ErrorKind::Unavailable,
            format!(
                "data directory {} is in use by another process",
                dir.display()
            ),
        )),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(Error::io(format!("locking {}", p.display()), e))
        }
    }
}

fn dir_has_files(dir: &Path) -> Result<bool> {
    if !dir.is_dir() {
        return Ok(false);
    }
    if !fsutil::list_files(dir)?.is_empty() {
        return Ok(true);
    }
    for d in fsutil::list_dirs(dir)? {
        if dir_has_files(&d)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn open_segment(
    dir: &Path,
    entry: &SegmentEntry,
    full_verify: bool,
    keys: Option<&stratadb_crypto::Keyring>,
    io: &Arc<dyn stratadb_common::io::IoEngine>,
) -> Result<SegmentHandle> {
    let path = segment_path(dir, &entry.tenant, entry.id);
    let loc = path.display().to_string();
    let reader =
        SegmentReader::open_with_io(&path, keys, Arc::clone(io)).map_err(|e| match e.kind() {
            ErrorKind::Io if !path.exists() => Error::corruption(
                &loc,
                "segment referenced by the manifest is missing (restore from backup)",
            ),
            _ => e,
        })?;
    let f = reader.footer();
    if reader.footer_crc() != entry.footer_crc
        || f.segment_id != entry.id
        || f.tenant_id != entry.tenant
        || f.row_count != entry.row_count
        || f.min_seq != entry.min_seq
        || f.max_seq != entry.max_seq
    {
        return Err(Error::corruption(
            &loc,
            "segment does not match its manifest entry",
        ));
    }
    if full_verify {
        reader.verify()?;
    }
    Ok(SegmentHandle {
        entry: entry.clone(),
        reader,
        bloom: std::sync::OnceLock::new(),
        ngram: std::sync::OnceLock::new(),
        cache: None,
    })
}

impl SegmentHandle {
    /// Account this segment's decoded filters on `cache`.
    pub(crate) fn tracked(
        mut self,
        cache: Option<&Arc<stratadb_common::memory::MemoryTracker>>,
    ) -> Self {
        self.cache = cache.cloned();
        self
    }
}

impl Engine {
    /// Open or create the database in `opts.data_dir`, running crash recovery.
    pub fn open(opts: EngineOptions) -> Result<Self> {
        let started = Instant::now();
        let dir = opts.data_dir.clone();
        fsutil::create_dir_durable(&dir)?;
        let lock_file = acquire_lock(&dir)?;
        let mut report = OpenReport::default();

        // 0. Encryption keyring (ADR-0024): created before anything else is
        // written encrypted; an encrypted directory never opens without its key.
        if fsutil::remove_if_exists(&fsutil::tmp_path(&stratadb_crypto::Keyring::path(&dir)))? {
            report.tmp_files_removed += 1;
        }
        let keys: Option<Arc<stratadb_crypto::Keyring>> =
            match (&opts.encryption, stratadb_crypto::Keyring::exists(&dir)) {
                (None, false) => None,
                (None, true) => {
                    return Err(Error::new(
                        ErrorKind::PermissionDenied,
                        "this data directory is encrypted (KEYRING present); configure its \
                         key-encryption key",
                    ))
                }
                (Some(p), true) => Some(Arc::new(
                    stratadb_crypto::Keyring::load(&dir, p.as_ref())?
                        .ok_or_else(|| Error::internal("KEYRING disappeared during open"))?,
                )),
                (Some(p), false) => {
                    let k = stratadb_crypto::Keyring::create(&dir, p.as_ref())?;
                    tracing::info!(kek = k.kek_id(), "encryption at rest enabled for new files");
                    Some(Arc::new(k))
                }
            };
        let k = keys.as_deref();

        // 1. Manifest (authoritative). Never silently re-initialize over data.
        if fsutil::remove_if_exists(&fsutil::tmp_path(&dir.join(crate::manifest::MANIFEST_FILE)))? {
            report.tmp_files_removed += 1;
        }
        let mut manifest = match Manifest::load_with(&dir, k, &*opts.io_engine())? {
            Some(m) => m,
            None => {
                if dir_has_files(&dir.join(WAL_DIR))? || dir_has_files(&dir.join(SEGMENTS_DIR))? {
                    return Err(Error::corruption(
                        dir.display().to_string(),
                        "MANIFEST is missing but WAL or segment files exist; refusing to \
                         initialize over existing data",
                    ));
                }
                let m = Manifest::initial();
                m.commit_with(&dir, k, &*opts.io_engine())?;
                report.created = true;
                m
            }
        };
        // Finish deletions recorded by compaction/retention before a crash.
        if !manifest.obsolete_segments.is_empty() {
            let mut dirs = BTreeSet::new();
            for o in &manifest.obsolete_segments {
                if fsutil::remove_if_exists(&segment_path(&dir, &o.tenant, o.id))? {
                    report.obsolete_deleted += 1;
                }
                dirs.insert(tenant_dir(&dir, &o.tenant));
            }
            for d in dirs.iter().filter(|d| d.is_dir()) {
                fsutil::fsync_dir(d)?;
            }
            manifest.version += 1;
            manifest.obsolete_segments.clear();
            manifest.commit_with(&dir, k, &*opts.io_engine())?;
        }
        report.manifest_version = manifest.version;

        // 2. Segments: open every committed segment; clean up unreferenced files.
        let referenced: BTreeSet<(String, u64)> = manifest
            .segments
            .iter()
            .map(|s| (s.tenant.as_str().to_owned(), s.id))
            .collect();
        let seg_root = dir.join(SEGMENTS_DIR);
        fsutil::create_dir_durable(&seg_root)?;
        for tdir in fsutil::list_dirs(&seg_root)? {
            let tname = tdir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_owned();
            let mut changed = false;
            for file in fsutil::list_files(&tdir)? {
                let name = file
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default();
                if name.ends_with(".tmp") {
                    fsutil::remove_if_exists(&file)?;
                    report.tmp_files_removed += 1;
                    changed = true;
                    continue;
                }
                let Some(id) = stratadb_segment::parse_segment_file_name(name) else {
                    tracing::warn!(file = %file.display(), "ignoring unexpected file in segment directory");
                    continue;
                };
                if referenced.contains(&(tname.clone(), id)) {
                    continue;
                }
                if id >= manifest.next_segment_id {
                    // Output of a flush that crashed before its manifest commit;
                    // its rows are still in the WAL.
                    fsutil::remove_if_exists(&file)?;
                    report.orphan_segments_removed += 1;
                    changed = true;
                } else {
                    // Unreferenced but once allocated: never delete data we cannot
                    // prove is redundant. Move aside for operator inspection.
                    let qdir = dir.join(QUARANTINE_DIR);
                    fsutil::create_dir_durable(&qdir)?;
                    let target = qdir.join(format!("{tname}-{name}"));
                    std::fs::rename(&file, &target)
                        .ctx(|| format!("quarantining {}", file.display()))?;
                    fsutil::fsync_dir(&qdir)?;
                    tracing::warn!(file = %file.display(), "quarantined unreferenced segment");
                    report.segments_quarantined += 1;
                    changed = true;
                }
            }
            if changed {
                fsutil::fsync_dir(&tdir)?;
            }
        }
        let mut handles = Vec::with_capacity(manifest.segments.len());
        for entry in &manifest.segments {
            handles.push(Arc::new(
                open_segment(
                    &dir,
                    entry,
                    opts.verify_segments_on_open,
                    k,
                    &opts.io_engine(),
                )?
                .tracked(opts.cache_memory.as_ref()),
            ));
        }
        report.segments = handles.len();

        // 3. WAL replay into the active memtable.
        let active = Arc::new(Memtable::new());
        let lim = DecodeLimits::default();
        let mut recovered_rows = 0u64;
        let mut metas: Vec<(TenantId, BatchMeta)> = Vec::new();
        for h in &handles {
            if let Some(x) = h.reader.footer().extension(idempotency::EXT_IDEMPOTENCY) {
                let ms = idempotency::decode_extension(&x.data).map_err(|e| {
                    Error::corruption(h.reader.path().display().to_string(), e.to_string())
                })?;
                metas.extend(ms.into_iter().map(|m| (h.entry.tenant.clone(), m)));
            }
        }
        let corrupt =
            |seq: u64, e: Error| Error::corruption(format!("WAL record {seq}"), e.to_string());
        let (wal, wal_report) = Wal::open(
            &dir.join(WAL_DIR),
            WalOptions {
                keys: keys.clone(),
                io: Some(opts.wal.io.clone().unwrap_or_else(|| opts.io_engine())),
                ..opts.wal.clone()
            },
            manifest.flushed_through_seq,
            |seq, rtype, payload| {
                // Row (types 1, 2) or columnar (types 3, 4) batch body, into
                // columns: columnar records need no row intermediate
                // (spec STRATA-SYS-07-002).
                let decode = |b: &[u8]| match rtype {
                    RecordType::EventBatch | RecordType::IdempotentEventBatch => {
                        decode_batch(b, &lim).and_then(EventColumns::from_events)
                    }
                    RecordType::ColumnarEventBatch | RecordType::IdempotentColumnarEventBatch => {
                        stratadb_schema::columnar::decode_columns(b, &lim)
                    }
                };
                let (cols, meta) = match rtype {
                    RecordType::EventBatch | RecordType::ColumnarEventBatch => {
                        (decode(payload).map_err(|e| corrupt(seq, e))?, None)
                    }
                    RecordType::IdempotentEventBatch | RecordType::IdempotentColumnarEventBatch => {
                        let (key, digest, committed_ms, rest) =
                            idempotency::decode_record(payload).map_err(|e| corrupt(seq, e))?;
                        let cols = decode(rest).map_err(|e| corrupt(seq, e))?;
                        let meta = BatchMeta {
                            key,
                            digest,
                            commit_seq: seq,
                            accepted: cols.len() as u32,
                            committed_ms,
                        };
                        metas.push((cols.tenant.clone(), meta.clone()));
                        (cols, Some(meta))
                    }
                };
                recovered_rows += cols.len() as u64;
                active.insert_columns(seq, cols, meta);
                Ok(())
            },
        )?;
        let now_ms = time::now_unix_millis();
        let mut dedup = DedupIndex::new(
            i64::try_from(opts.idempotency_window.as_millis()).unwrap_or(i64::MAX),
            opts.idempotency_max_keys,
        );
        metas.sort_by_key(|(_, m)| (m.committed_ms, m.commit_seq));
        for (t, m) in metas {
            dedup.insert_recovered(&t, m, now_ms);
        }
        report.wal = wal_report;
        report.rows_recovered_from_wal = recovered_rows;
        report.elapsed = started.elapsed();
        tracing::info!(
            segments = report.segments,
            wal_rows = recovered_rows,
            orphans = report.orphan_segments_removed,
            elapsed_ms = report.elapsed.as_millis() as u64,
            "storage engine opened"
        );

        let shared = Arc::new(Shared {
            dir: dir.clone(),
            wal,
            write_gate: RwLock::new(()),
            state: Mutex::new(State {
                active,
                frozen: VecDeque::new(),
                segments: Arc::new(handles),
                obsolete: Vec::new(),
            }),
            manifest: Mutex::new(manifest),
            failed: Mutex::new(None),
            stall_cv: Condvar::new(),
            bg: Mutex::new(BgSignal::default()),
            bg_cv: Condvar::new(),
            counters: Counters::default(),
            flush_seconds: stratadb_common::metrics::Histogram::latency(),
            stage_admission: stratadb_common::metrics::Histogram::stage(),
            stage_encode: stratadb_common::metrics::Histogram::stage(),
            stage_memtable: stratadb_common::metrics::Histogram::stage(),
            open_report: report,
            dedup: Mutex::new(dedup),
            disk: Mutex::new(DiskState {
                free: u64::MAX,
                total: u64::MAX,
                checked: None,
            }),
            _lock_file: lock_file,
            keys,
            ingest: IngestAccount {
                tracker: opts.memory.clone(),
                held: AtomicU64::new(0),
            },
            opts,
        });
        // Rows replayed from the WAL are in memory already: track them
        // without a limit check (writers stall until they are flushed).
        if let Some(t) = &shared.ingest.tracker {
            let st = shared.state.lock();
            let bytes =
                (st.active.bytes() + st.frozen.iter().map(|f| f.bytes).sum::<usize>()) as u64;
            t.force_reserve(bytes);
            shared.ingest.add(bytes);
        }
        let engine = Engine {
            bg_thread: Mutex::new(None),
            shared: Arc::clone(&shared),
        };
        if shared.opts.background {
            let s = Arc::clone(&shared);
            let h = std::thread::Builder::new()
                .name(stratadb_common::threads::Role::Maint.thread_name(s.opts.maint_thread_index))
                .spawn(move || background_loop(&s))
                .ctx(|| "spawning background thread".to_owned())?;
            *engine.bg_thread.lock() = Some(h);
        }
        Ok(engine)
    }

    pub fn data_dir(&self) -> &Path {
        &self.shared.dir
    }

    pub fn open_report(&self) -> &OpenReport {
        &self.shared.open_report
    }

    /// Commit a batch of events for `tenant`. All events must carry
    /// `tenant` as their tenant id (set from the authenticated principal).
    pub fn write_batch(&self, tenant: &TenantId, events: Vec<Event>) -> Result<WriteReceipt> {
        self.shared.write_batch(tenant, events, None)
    }

    /// Like [`Engine::write_batch`], deduplicated by `key` within the
    /// configured window (see [`crate::idempotency`] for the exact contract).
    pub fn write_batch_idempotent(
        &self,
        tenant: &TenantId,
        events: Vec<Event>,
        key: Option<IdempotencyKey>,
    ) -> Result<WriteReceipt> {
        self.shared.write_batch(tenant, events, key)
    }

    /// Flush all memtables to segments now.
    pub fn flush(&self) -> Result<FlushReport> {
        self.shared.flush()
    }

    /// Create a consistent, verified backup in `dest` (must be absent or
    /// empty). See [`backup`](self) module docs for the consistency point.
    pub fn backup(&self, dest: &Path) -> Result<BackupReport> {
        self.shared.backup(dest)
    }

    /// Rewrite segments written in older formats to the current one
    /// (irreversible for older binaries: take a backup first). `dry_run`
    /// only reports what would change.
    pub fn upgrade_segments(&self, dry_run: bool) -> Result<UpgradeReport> {
        self.shared.upgrade(dry_run)
    }

    /// Index filters of every committed segment (spec §12.8). With `verify`,
    /// each segment's rows are read to check that no filter misses a stored
    /// value and whether the filters match the current configuration.
    pub fn indexes(&self, verify: bool) -> Result<Vec<SegmentIndexes>> {
        self.shared.indexes(verify)
    }

    /// Rebuild the filters of segments that are stale (configuration changed)
    /// or unsound, by rewriting them from their rows (spec §3.1). `dry_run`
    /// only reports which segments would be rebuilt.
    pub fn rebuild_indexes(&self, dry_run: bool) -> Result<IndexRebuildReport> {
        self.shared.rebuild_indexes(dry_run)
    }

    /// Run one compaction pass (at most one merge group per tenant) and delete
    /// obsolete files no snapshot still references.
    pub fn compact(&self) -> Result<CompactionReport> {
        self.shared.compact()
    }

    /// Apply retention policies as of `now_ms` (Unix milliseconds).
    pub fn enforce_retention(&self, now_ms: i64) -> Result<RetentionReport> {
        self.shared.enforce_retention(now_ms)
    }

    /// Set (or with `None`, remove) a tenant's retention policy; durable.
    pub fn set_retention(&self, tenant: &TenantId, policy: Option<RetentionPolicy>) -> Result<()> {
        self.shared.set_retention(tenant, policy)
    }

    pub fn retention_policies(&self) -> BTreeMap<TenantId, RetentionPolicy> {
        self.shared.manifest.lock().retention.clone()
    }

    /// Force a WAL sync (meaningful in `Async` mode).
    pub fn sync_wal(&self) -> Result<()> {
        self.shared.wal.sync().inspect_err(|e| self.shared.fail(e))
    }

    /// Current storage usage of one tenant (committed segments + memtables);
    /// used for storage quotas (spec §3.4).
    pub fn tenant_usage(&self, tenant: &TenantId) -> TenantUsage {
        let st = self.shared.state.lock();
        let mut u = TenantUsage::default();
        for s in st.segments.iter().filter(|s| &s.entry.tenant == tenant) {
            u.segment_bytes += s.entry.file_bytes;
            u.segment_rows += s.entry.row_count;
            u.segments += 1;
        }
        u.memtable_bytes = st.active.tenant_bytes(tenant) as u64
            + st.frozen
                .iter()
                .map(|f| f.tenant_bytes.get(tenant).copied().unwrap_or(0) as u64)
                .sum::<u64>();
        u
    }

    /// Take a consistent read view of one tenant's data.
    pub fn snapshot(&self, tenant: &TenantId) -> Snapshot {
        self.shared.snapshot(tenant)
    }

    pub fn stats(&self) -> EngineStats {
        self.shared.stats()
    }

    /// Storage read-path counters (descriptor budget, retries, short reads).
    pub fn io_stats(&self) -> stratadb_common::io::IoStats {
        self.shared.opts.io_engine().stats()
    }

    /// Error message if the engine is in the failed (fail-stop) state.
    pub fn failure(&self) -> Option<String> {
        self.shared.failed.lock().clone()
    }

    /// Verify every committed segment fully (all checksums and statistics).
    pub fn verify(&self) -> Result<usize> {
        let segs = Arc::clone(&self.shared.state.lock().segments);
        for s in segs.iter() {
            s.reader.verify()?;
        }
        Ok(segs.len())
    }

    /// Graceful shutdown: stop background work, flush (if configured and
    /// healthy), and sync the WAL.
    pub fn close(self) -> Result<()> {
        self.stop_background();
        let s = &self.shared;
        if s.failed.lock().is_some() {
            return Err(Error::unavailable(
                "engine closed in failed state; recovery will run on next open",
            ));
        }
        if s.opts.flush_on_close {
            s.flush()?;
        }
        s.wal.sync()
    }

    fn stop_background(&self) {
        {
            let mut g = self.shared.bg.lock();
            g.shutdown = true;
        }
        self.shared.bg_cv.notify_all();
        if let Some(h) = self.bg_thread.lock().take() {
            if h.join().is_err() {
                tracing::error!("background thread panicked");
            }
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.stop_background();
        // Best effort only: recovery handles anything not synced here.
        if self.shared.failed.lock().is_none() {
            if let Err(e) = self.shared.wal.sync() {
                tracing::warn!(error = %e, "WAL sync on drop failed");
            }
        }
    }
}

impl Shared {
    fn fail(&self, e: &Error) {
        if matches!(
            e.kind(),
            ErrorKind::Indeterminate | ErrorKind::Unavailable | ErrorKind::Io | ErrorKind::Internal
        ) {
            let mut f = self.failed.lock();
            if f.is_none() {
                tracing::error!(error = %e, "storage engine entering failed state");
                *f = Some(e.to_string());
            }
        }
    }

    fn check_healthy(&self) -> Result<()> {
        match &*self.failed.lock() {
            Some(why) => Err(Error::unavailable(format!(
                "storage engine is in failed state (restart required): {why}"
            ))),
            None => Ok(()),
        }
    }

    /// Refresh (at most every 200 ms) and return (free, total) bytes.
    fn disk_space(&self) -> (u64, u64) {
        let mut d = self.disk.lock();
        if d.checked
            .is_none_or(|t| t.elapsed() >= Duration::from_millis(200))
        {
            match (fs4::available_space(&self.dir), fs4::total_space(&self.dir)) {
                (Ok(f), Ok(t)) => {
                    d.free = f;
                    d.total = t;
                }
                (Err(e), _) | (_, Err(e)) => {
                    tracing::warn!(error = %e, "could not measure free disk space");
                }
            }
            d.checked = Some(Instant::now());
        }
        (d.free, d.total)
    }

    /// Fail with a retryable `resource_exhausted` if writing `extra` more
    /// bytes would take free space below the reserve.
    fn check_disk(&self, extra: u64) -> Result<()> {
        let (free, total) = self.disk_space();
        let reserve = self
            .opts
            .disk_reserve_bytes
            .max((total as f64 * self.opts.disk_reserve_ratio) as u64);
        if free < reserve.saturating_add(extra) {
            self.counters
                .disk_rejections
                .fetch_add(1, Ordering::Relaxed);
            return Err(Error::resource_exhausted(format!(
                "free disk space ({free} B) is below the reserve ({reserve} B) plus {extra} B needed; \
                 writes are paused until space is freed (retention, compaction, or more disk)"
            )));
        }
        Ok(())
    }

    fn segment_write_options(&self) -> stratadb_segment::WriteOptions {
        stratadb_segment::WriteOptions {
            compress: self.opts.segment_compression,
            shred: self.opts.segment_shredding,
        }
    }

    /// Footer extensions for a new segment: idempotency keys and Bloom filters.
    fn segment_extensions(
        &self,
        rows: &[RowRef<'_>],
        metas: &[BatchMeta],
    ) -> Vec<stratadb_segment::Extension> {
        let mut x = Vec::new();
        if !metas.is_empty() {
            x.push(stratadb_segment::Extension {
                tag: idempotency::EXT_IDEMPOTENCY,
                required: false,
                data: idempotency::encode_extension(metas),
            });
        }
        x.extend(self.index_extensions(rows));
        x
    }

    /// Index extensions (Bloom, trigram) the current configuration builds for
    /// `rows`. Deterministic: equal rows and settings give equal bytes.
    pub(crate) fn index_extensions(&self, rows: &[RowRef<'_>]) -> Vec<stratadb_segment::Extension> {
        let mut x = Vec::new();
        if self.opts.bloom_enabled {
            let b = stratadb_segment::bloom::BloomSet::build(
                rows,
                &self.opts.bloom_attribute_keys,
                &self.opts.bloom_resource_keys,
            );
            x.push(b.to_extension());
        }
        if self.opts.ngram_enabled || !self.opts.ngram_attribute_keys.is_empty() {
            let n = stratadb_segment::ngram::NgramSet::build(
                rows,
                self.opts.ngram_enabled,
                &self.opts.ngram_attribute_keys,
                &[],
            );
            x.push(n.to_extension());
        }
        x
    }

    fn request_flush(&self) {
        self.bg.lock().flush_requested = true;
        self.bg_cv.notify_all();
    }

    /// Reserve `bytes` of ingest memory. When the region is full, ask for a
    /// flush (which releases memtable memory) and wait up to the stall
    /// timeout. Returns the bytes reserved (0 without a tracker).
    fn reserve_ingest(&self, bytes: u64) -> Result<u64> {
        let Some(t) = &self.ingest.tracker else {
            return Ok(0);
        };
        if t.try_reserve(bytes).is_err() {
            self.counters.stalls.fetch_add(1, Ordering::Relaxed);
            self.request_flush();
            if let Err(e) = t.reserve_wait(bytes, self.opts.write_stall_timeout) {
                self.counters.rejections.fetch_add(1, Ordering::Relaxed);
                return Err(Error::resource_exhausted(format!(
                    "write stalled: ingest memory is full; retry with backoff ({})",
                    e.to_string().trim_start_matches("resource_exhausted: ")
                )));
            }
        }
        self.ingest.add(bytes);
        Ok(bytes)
    }

    fn active_over_threshold(&self, active: &Memtable) -> bool {
        active.bytes() >= self.opts.memtable_max_bytes
            || active.rows() >= self.opts.memtable_max_rows
            || active
                .age()
                .is_some_and(|a| a >= self.opts.memtable_max_age)
    }

    fn write_batch(
        &self,
        tenant: &TenantId,
        events: Vec<Event>,
        key: Option<IdempotencyKey>,
    ) -> Result<WriteReceipt> {
        if events.is_empty() {
            return Err(Error::invalid_input("batch contains no events"));
        }
        if events.iter().any(|e| &e.tenant_id != tenant) {
            // Defense in depth (spec §18.4): the API layer sets tenant ids
            // from the principal; a mismatch here is a bug or an attack.
            return Err(Error::invalid_input(
                "event tenant does not match the authenticated tenant",
            ));
        }
        self.check_healthy()?;
        self.check_disk(0)?;

        // Backpressure (spec §7.4): bounded frozen backlog.
        let admitted = Instant::now();
        {
            let mut st = self.state.lock();
            if st.frozen.len() >= self.opts.max_frozen_memtables {
                self.counters.stalls.fetch_add(1, Ordering::Relaxed);
                drop(st);
                self.request_flush();
                st = self.state.lock();
                let deadline = Instant::now() + self.opts.write_stall_timeout;
                while st.frozen.len() >= self.opts.max_frozen_memtables {
                    if self.stall_cv.wait_until(&mut st, deadline).timed_out()
                        && st.frozen.len() >= self.opts.max_frozen_memtables
                    {
                        self.counters.rejections.fetch_add(1, Ordering::Relaxed);
                        return Err(Error::resource_exhausted(
                            "write stalled: flush backlog is full; retry with backoff",
                        ));
                    }
                }
            }
        }

        self.stage_admission.observe_duration(admitted.elapsed());
        let n = events.len();
        let level = match self.opts.wal.durability {
            Durability::Async => CommitLevel::WalWritten,
            Durability::Sync | Durability::GroupCommit { .. } => CommitLevel::WalSynced,
        };
        let now_ms = time::now_unix_millis();
        if let Some(k) = &key {
            if let Reservation::Duplicate(m) = self.dedup.lock().reserve(tenant, k, now_ms)? {
                self.counters.duplicates.fetch_add(1, Ordering::Relaxed);
                return Ok(WriteReceipt {
                    commit_seq: m.commit_seq,
                    accepted: m.accepted as usize,
                    level,
                    duplicate: true,
                });
            }
        }
        let encoding = Instant::now();
        // WAL v3 writes the columnar batch (spec STRATA-SYS-07-002); v2 (the
        // rollback window) the row batch.
        let columnar = self.opts.wal.format_version >= 3;
        let batch = if columnar {
            match stratadb_schema::columnar::encode_batch(&events) {
                Ok(b) => b,
                Err(e) => {
                    if let Some(k) = &key {
                        self.dedup.lock().abort(tenant, k);
                    }
                    return Err(e);
                }
            }
        } else {
            encode_batch(&events)
        };
        let (rtype, payload) = match (&key, columnar) {
            (Some(k), true) => (
                RecordType::IdempotentColumnarEventBatch,
                idempotency::encode_record(k, now_ms, &batch),
            ),
            (Some(k), false) => (
                RecordType::IdempotentEventBatch,
                idempotency::encode_record(k, now_ms, &batch),
            ),
            (None, true) => (RecordType::ColumnarEventBatch, batch),
            (None, false) => (RecordType::EventBatch, batch),
        };
        self.stage_encode.observe_duration(encoding.elapsed());
        // Ingest memory (spec VIII.5.2): reserved before the WAL append, so a
        // write is refused before it is durable, never after.
        let reserved = match self.reserve_ingest(crate::memtable::batch_bytes(&events) as u64) {
            Ok(b) => b,
            Err(e) => {
                if let Some(k) = &key {
                    self.dedup.lock().abort(tenant, k);
                }
                return Err(e);
            }
        };
        let committed = self.commit(rtype, &payload, events, n, key.as_ref(), now_ms);
        if committed.is_err() {
            self.ingest.release(reserved);
        }
        let (seq, active, meta) = match committed {
            Ok(v) => v,
            Err(e) => {
                if let Some(k) = &key {
                    self.dedup.lock().abort(tenant, k);
                }
                return Err(e);
            }
        };
        if let Some(m) = meta {
            self.dedup.lock().complete(tenant, m);
        }
        self.counters.batches.fetch_add(1, Ordering::Relaxed);
        self.counters.events.fetch_add(n as u64, Ordering::Relaxed);
        if self.active_over_threshold(&active) {
            self.request_flush();
        }
        Ok(WriteReceipt {
            commit_seq: seq,
            accepted: n,
            level,
            duplicate: false,
        })
    }

    /// WAL append + memtable insert under the shared write gate.
    fn commit(
        &self,
        rtype: RecordType,
        payload: &[u8],
        events: Vec<Event>,
        n: usize,
        key: Option<&IdempotencyKey>,
        now_ms: i64,
    ) -> Result<(u64, Arc<Memtable>, Option<BatchMeta>)> {
        let _gate = self.write_gate.read();
        let seq = self
            .wal
            .append(rtype, payload)
            .inspect_err(|e| self.fail(e))?;
        if let Err(e) = failpoint::hit("engine::after_wal_commit") {
            let e = Error::indeterminate(e.to_string());
            self.fail(&e);
            return Err(e);
        }
        let meta = key.map(|k| BatchMeta {
            key: k.key().to_owned(),
            digest: *k.digest(),
            commit_seq: seq,
            accepted: n as u32,
            committed_ms: now_ms,
        });
        let inserting = Instant::now();
        let active = Arc::clone(&self.state.lock().active);
        if let Err(e) = active.insert_batch(seq, events, meta.clone()) {
            // The record is durable but not in memory: fail stop; recovery
            // replays it.
            let e =
                Error::indeterminate(format!("memtable insert failed after the WAL append: {e}"));
            self.fail(&e);
            return Err(e);
        }
        self.stage_memtable.observe_duration(inserting.elapsed());
        Ok((seq, active, meta))
    }

    /// Freeze the active memtable if non-empty.
    fn freeze_active(&self) -> Result<()> {
        let _gate = self.write_gate.write();
        // Re-check under the gate: a write that failed after its WAL append
        // marks the engine failed before releasing the gate.
        self.check_healthy()?;
        let mut st = self.state.lock();
        if st.active.is_empty() {
            return Ok(());
        }
        let through = self.wal.last_seq();
        // O(1): swap in an empty memtable; queries and flush read the old
        // one concurrently (spec STRATA-ARCH-02-014).
        let old = std::mem::replace(&mut st.active, Arc::new(Memtable::new()));
        st.frozen
            .push_back(Arc::new(FrozenMemtable::new(old, through)));
        drop(st);
        failpoint::hit("flush::after_freeze")
    }

    fn flush(&self) -> Result<FlushReport> {
        self.check_healthy()?;
        let started = Instant::now();
        let mut man = self.manifest.lock();
        self.freeze_active()?;
        let mut report = FlushReport {
            flushed_through_seq: man.flushed_through_seq,
            ..FlushReport::default()
        };
        loop {
            let Some(frozen) = self.state.lock().frozen.front().cloned() else {
                break;
            };
            match self.flush_one(&mut man, &frozen, &mut report) {
                Ok(()) => {}
                Err(e) => {
                    self.counters.flush_failures.fetch_add(1, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
        if report.memtables_flushed > 0 {
            self.counters.flushes.fetch_add(1, Ordering::Relaxed);
            self.flush_seconds.observe_duration(started.elapsed());
        }
        Ok(report)
    }

    fn flush_one(
        &self,
        man: &mut Manifest,
        frozen: &Arc<FrozenMemtable>,
        report: &mut FlushReport,
    ) -> Result<()> {
        let tenants = frozen.tenants();
        let mut next_id = man.next_segment_id;
        let mut written: Vec<PathBuf> = Vec::new();
        let mut handles = Vec::new();
        let created_ms = time::now_unix_millis();
        let result = (|| -> Result<()> {
            for tenant in &tenants {
                // One tenant at a time: its rows are materialized for the
                // segment writer and dropped before the next tenant.
                let Some(view) = frozen.view(tenant) else {
                    continue;
                };
                let owned = view.materialize()?;
                let mut rows: Vec<RowRef<'_>> = owned
                    .iter()
                    .map(|(seq, idx, event)| RowRef {
                        seq: *seq,
                        idx: *idx,
                        event,
                    })
                    .collect();
                let id = next_id;
                next_id += 1;
                let tdir = tenant_dir(&self.dir, tenant);
                fsutil::create_dir_durable(&tdir)?;
                let path = segment_path(&self.dir, tenant, id);
                written.push(path.clone());
                let metas = frozen.metas.get(tenant).map_or(&[][..], Vec::as_slice);
                let extensions = self.segment_extensions(&rows, metas);
                let ws = stratadb_segment::write_segment_io(
                    &*self.opts.io_engine(),
                    &path,
                    id,
                    tenant,
                    &mut rows,
                    created_ms,
                    extensions,
                    &self.segment_write_options(),
                    self.keys.as_deref(),
                )?;
                let f = &ws.footer;
                let entry = SegmentEntry {
                    id,
                    tenant: tenant.clone(),
                    segment_format_version: stratadb_segment::FORMAT_VERSION,
                    min_schema_version: f.min_schema_version,
                    max_schema_version: f.max_schema_version,
                    min_event_time: f.min_event_time,
                    max_event_time: f.max_event_time,
                    min_seq: f.min_seq,
                    max_seq: f.max_seq,
                    row_count: f.row_count,
                    file_bytes: ws.file_bytes,
                    footer_crc: ws.footer_crc,
                    created_ms,
                    state: SegmentState::Committed,
                    lineage: Vec::new(),
                };
                // Read back from disk before publishing (catches write-path corruption).
                let handle = open_segment(
                    &self.dir,
                    &entry,
                    true,
                    self.keys.as_deref(),
                    &self.opts.io_engine(),
                )?
                .tracked(self.opts.cache_memory.as_ref());
                report.bytes_written += ws.file_bytes;
                handles.push(Arc::new(handle));
            }
            failpoint::hit("flush::after_segment_write")
        })();
        if let Err(e) = result {
            for p in &written {
                if let Err(re) = fsutil::remove_if_exists(p) {
                    tracing::warn!(error = %re, file = %p.display(), "could not remove unpublished segment");
                }
            }
            tracing::error!(error = %e, "flush failed before manifest commit; will retry");
            return Err(e);
        }

        let mut next = man.clone();
        next.version += 1;
        next.next_segment_id = next_id;
        next.flushed_through_seq = frozen.through_seq;
        next.segments
            .extend(handles.iter().map(|h| h.entry.clone()));
        if let Err(e) = next.commit_with(&self.dir, self.keys.as_deref(), &*self.opts.io_engine()) {
            // The manifest may or may not be durable now: fail stop.
            let e = Error::indeterminate(format!("manifest commit failed: {e}"));
            self.fail(&e);
            return Err(e);
        }
        *man = next;
        if let Err(e) = failpoint::hit("flush::after_manifest_commit") {
            let e = Error::indeterminate(e.to_string());
            self.fail(&e);
            return Err(e);
        }

        {
            let mut st = self.state.lock();
            match st.frozen.front() {
                Some(f) if Arc::ptr_eq(f, frozen) => {
                    st.frozen.pop_front();
                    self.ingest.release(frozen.bytes as u64);
                }
                _ => {
                    return Err(Error::internal(
                        "frozen memtable queue changed during flush",
                    ))
                }
            }
            let mut segs: Vec<Arc<SegmentHandle>> = st.segments.iter().cloned().collect();
            segs.extend(handles.iter().cloned());
            st.segments = Arc::new(segs);
        }
        self.stall_cv.notify_all();
        report.memtables_flushed += 1;
        report.segments_written += handles.len();
        report.rows_flushed += frozen.rows;
        report.flushed_through_seq = frozen.through_seq;

        match self.wal.reclaim_through(frozen.through_seq) {
            Ok(n) => report.wal_segments_reclaimed += n,
            // Reclamation failure wastes disk but loses nothing; retried next flush.
            Err(e) => tracing::warn!(error = %e, "WAL reclamation failed"),
        }
        Ok(())
    }

    fn snapshot(&self, tenant: &TenantId) -> Snapshot {
        let st = self.state.lock();
        let segments = st
            .segments
            .iter()
            .filter(|s| &s.entry.tenant == tenant)
            .cloned()
            .collect();
        // O(chunks): views copy chunk pointers and a published count.
        let memory = st
            .frozen
            .iter()
            .filter_map(|f| f.view(tenant))
            .chain(st.active.view(tenant))
            .collect();
        Snapshot {
            tenant: tenant.clone(),
            segments,
            memory,
        }
    }

    fn stats(&self) -> EngineStats {
        let wal_metrics = self.wal.metrics();
        let (active_rows, active_bytes, frozen, segs) = {
            let st = self.state.lock();
            (
                st.active.rows(),
                st.active.bytes(),
                st.frozen.len(),
                Arc::clone(&st.segments),
            )
        };
        let (manifest_version, flushed_through_seq) = {
            let m = self.manifest.lock();
            (m.version, m.flushed_through_seq)
        };
        let c = &self.counters;
        // Take the dedup lock once: parking_lot mutexes are not reentrant, and
        // two `lock()` temporaries in one expression would deadlock.
        let (idempotency_keys, idempotency_evictions) = {
            let d = self.dedup.lock();
            (d.len(), d.evictions)
        };
        EngineStats {
            active_rows,
            active_bytes,
            frozen_memtables: frozen,
            segments: segs.len(),
            segment_bytes: segs.iter().map(|s| s.entry.file_bytes).sum(),
            segment_rows: segs.iter().map(|s| s.entry.row_count).sum(),
            manifest_version,
            flushed_through_seq,
            wal: self.wal.stats(),
            failed: self.failed.lock().clone(),
            batches_written: c.batches.load(Ordering::Relaxed),
            events_written: c.events.load(Ordering::Relaxed),
            flushes: c.flushes.load(Ordering::Relaxed),
            flush_failures: c.flush_failures.load(Ordering::Relaxed),
            write_stalls: c.stalls.load(Ordering::Relaxed),
            write_rejections: c.rejections.load(Ordering::Relaxed),
            idempotency_keys,
            idempotency_evictions,
            obsolete_segments: self.state.lock().obsolete.len(),
            compactions: c.compactions.load(Ordering::Relaxed),
            segments_deleted: c.segments_deleted.load(Ordering::Relaxed),
            retention_rows_deleted: c.retention_rows_deleted.load(Ordering::Relaxed),
            disk_free_bytes: self.disk_space().0,
            disk_total_bytes: self.disk_space().1,
            disk_write_rejections: c.disk_rejections.load(Ordering::Relaxed),
            duplicates_suppressed: c.duplicates.load(Ordering::Relaxed),
            wal_metrics: wal_metrics.clone(),
            flush_seconds: self.flush_seconds.snapshot(),
            insert_stages: vec![
                ("admission", self.stage_admission.snapshot()),
                ("wal_encode", self.stage_encode.snapshot()),
                ("wal_append", wal_metrics.append_seconds),
                ("group_commit_wait", wal_metrics.commit_wait_seconds),
                ("memtable_insert", self.stage_memtable.snapshot()),
            ],
        }
    }
}

fn background_loop(s: &Shared) {
    let tick = s
        .opts
        .async_sync_interval
        .min(s.opts.memtable_max_age / 4)
        .clamp(Duration::from_millis(10), Duration::from_secs(1));
    let mut last_sync = Instant::now();
    let mut last_retention = Instant::now();
    let mut backoff = Duration::ZERO;
    let me = stratadb_common::threads::register(stratadb_common::threads::Role::Maint);
    loop {
        me.update_cpu();
        let flush_requested = {
            let mut g = s.bg.lock();
            if !g.shutdown && !g.flush_requested {
                s.bg_cv.wait_for(&mut g, tick.max(backoff));
            }
            if g.shutdown {
                return;
            }
            std::mem::take(&mut g.flush_requested)
        };
        if s.failed.lock().is_some() {
            continue;
        }
        let needs_flush = flush_requested || {
            let st = s.state.lock();
            !st.frozen.is_empty() || s.active_over_threshold(&st.active)
        };
        if needs_flush {
            match s.flush() {
                Ok(_) => backoff = Duration::ZERO,
                Err(e) => {
                    backoff =
                        (backoff * 2).clamp(Duration::from_millis(50), Duration::from_secs(5));
                    tracing::error!(error = %e, retry_in_ms = backoff.as_millis() as u64, "background flush failed");
                }
            }
        }
        // Maintenance runs after flushing so ingestion keeps priority; each pass
        // is bounded (one merge group per tenant), so it cannot starve flushes.
        if s.opts.compaction.enabled {
            if let Err(e) = s.compact() {
                tracing::error!(error = %e, "background compaction failed");
            }
        } else if let Err(e) = s.reclaim_obsolete_now() {
            tracing::warn!(error = %e, "obsolete segment reclamation failed");
        }
        if last_retention.elapsed() >= s.opts.retention_interval {
            if let Err(e) = s.enforce_retention(time::now_unix_millis()) {
                tracing::error!(error = %e, "background retention failed");
            }
            last_retention = Instant::now();
        }
        if s.opts.wal.durability == Durability::Async
            && last_sync.elapsed() >= s.opts.async_sync_interval
        {
            if let Err(e) = s.wal.sync() {
                s.fail(&e);
            }
            last_sync = Instant::now();
        }
    }
}
