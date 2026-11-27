//! Columnar, tenant-partitioned memtable of committed-but-unflushed rows
//! (spec §9; STRATA-ARCH-02-012/013/014, STRATA-ARCH-04-001/002).
//!
//! Rows enter only *after* their WAL record reached the configured
//! durability boundary.
//!
//! # Layout
//! Each tenant's rows are stored in *column chunks*: append-only vectors,
//! one per column. A chunk's capacity doubles from [`FIRST_CHUNK_ROWS`] up
//! to [`MAX_CHUNK_ROWS`]; when one is full a new chunk is started. Nothing is
//! allocated per row on the insert path, except the values' own heap data.
//! - Fixed-width columns are atomics.
//! - Variable-width columns are write-once cells.
//!
//! # Concurrency
//! - **Writers:** appends for one tenant are serialized by that tenant's
//!   writer lock (sharded per tenant, ARCH-04-001 (b)). The writer stores a
//!   batch's values, updates the chunk zone maps, then publishes the
//!   tenant's row count with `Release`.
//! - **Readers** `load(Acquire)` the count and read only rows below it. They
//!   take no lock per row or per chunk, and a batch is visible entirely or
//!   not at all. A snapshot copies the chunk pointers and the count, at a
//!   cost of O(chunks), not O(rows).
//! - **Freezing** is O(1): the engine swaps in a new empty memtable, and the
//!   frozen one is read by flush and queries concurrently.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use parking_lot::{Mutex, RwLock};
use stratadb_common::{Error, Result, TenantId};
use stratadb_schema::column::EventColumns;
use stratadb_schema::{Event, Severity, Value};

use crate::idempotency::BatchMeta;

/// Rows of the first chunk of a tenant; later chunks double up to
/// [`MAX_CHUNK_ROWS`] (spec `memtable.chunk_rows`).
pub const FIRST_CHUNK_ROWS: usize = 64;
/// Largest chunk.
pub const MAX_CHUNK_ROWS: usize = 65_536;

/// Bytes charged per row besides `Event::approx_size` (sequence number,
/// index and cell headers).
pub const ROW_OVERHEAD: usize = 24;

/// Memtable bytes charged for one event (the ingest memory reservation
/// uses the same number, so release on flush matches exactly).
pub fn row_bytes(ev: &Event) -> usize {
    ev.approx_size() + ROW_OVERHEAD
}

/// Memtable bytes charged for a batch.
pub fn batch_bytes(events: &[Event]) -> usize {
    events.iter().map(row_bytes).sum()
}

fn columns_bytes(c: &EventColumns) -> usize {
    (0..c.len())
        .map(|i| c.row_approx_size(i) + ROW_OVERHEAD)
        .sum()
}

fn atomics<T>(n: usize, f: impl Fn() -> T) -> Box<[T]> {
    (0..n).map(|_| f()).collect()
}

fn cells<T>(n: usize) -> Box<[OnceLock<T>]> {
    (0..n).map(|_| OnceLock::new()).collect()
}

/// Min/max statistics of a chunk, updated on append. They cover every
/// row written to the chunk (a superset of the rows a reader sees), so
/// pruning on them is sound.
#[derive(Debug)]
struct Zone {
    min_time: AtomicI64,
    max_time: AtomicI64,
    min_seq: AtomicU64,
    max_seq: AtomicU64,
    /// Bit `code` set for each severity code present (codes are <= 24).
    severities: AtomicU32,
}

impl Zone {
    fn new() -> Self {
        Self {
            min_time: AtomicI64::new(i64::MAX),
            max_time: AtomicI64::new(i64::MIN),
            min_seq: AtomicU64::new(u64::MAX),
            max_seq: AtomicU64::new(0),
            severities: AtomicU32::new(0),
        }
    }
}

/// One column chunk: `cap` rows of every column, written once each.
#[derive(Debug)]
pub struct Chunk {
    /// Index of this chunk's first row within the tenant.
    start: usize,
    cap: usize,
    seq: Box<[AtomicU64]>,
    idx: Box<[AtomicU32]>,
    event_time: Box<[AtomicI64]>,
    ingest_time: Box<[AtomicI64]>,
    severity: Box<[AtomicU8]>,
    schema_version: Box<[AtomicU32]>,
    service: Box<[OnceLock<Option<String>>]>,
    severity_text: Box<[OnceLock<Option<String>>]>,
    trace_id: Box<[OnceLock<Option<String>>]>,
    span_id: Box<[OnceLock<Option<String>>]>,
    event_id: Box<[OnceLock<Option<String>>]>,
    body: Box<[OnceLock<Option<Value>>]>,
    attributes: Box<[OnceLock<BTreeMap<String, Value>>]>,
    resource_attributes: Box<[OnceLock<BTreeMap<String, Value>>]>,
    zone: Zone,
}

fn unpublished() -> Error {
    Error::internal("memtable cell below the published count is empty")
}

impl Chunk {
    fn new(start: usize, cap: usize) -> Self {
        Self {
            start,
            cap,
            seq: atomics(cap, || AtomicU64::new(0)),
            idx: atomics(cap, || AtomicU32::new(0)),
            event_time: atomics(cap, || AtomicI64::new(0)),
            ingest_time: atomics(cap, || AtomicI64::new(0)),
            severity: atomics(cap, || AtomicU8::new(0)),
            schema_version: atomics(cap, || AtomicU32::new(0)),
            service: cells(cap),
            severity_text: cells(cap),
            trace_id: cells(cap),
            span_id: cells(cap),
            event_id: cells(cap),
            body: cells(cap),
            attributes: cells(cap),
            resource_attributes: cells(cap),
            zone: Zone::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn seq(&self, i: usize) -> u64 {
        self.seq[i].load(Ordering::Relaxed)
    }

    pub fn idx(&self, i: usize) -> u32 {
        self.idx[i].load(Ordering::Relaxed)
    }

    pub fn event_time(&self, i: usize) -> i64 {
        self.event_time[i].load(Ordering::Relaxed)
    }

    pub fn ingest_time(&self, i: usize) -> i64 {
        self.ingest_time[i].load(Ordering::Relaxed)
    }

    pub fn severity(&self, i: usize) -> u8 {
        self.severity[i].load(Ordering::Relaxed)
    }

    pub fn schema_version(&self, i: usize) -> u32 {
        self.schema_version[i].load(Ordering::Relaxed)
    }

    fn cell<T>(c: &[OnceLock<T>], i: usize) -> Result<&T> {
        c[i].get().ok_or_else(unpublished)
    }

    pub fn service(&self, i: usize) -> Result<&Option<String>> {
        Self::cell(&self.service, i)
    }

    pub fn severity_text(&self, i: usize) -> Result<&Option<String>> {
        Self::cell(&self.severity_text, i)
    }

    pub fn trace_id(&self, i: usize) -> Result<&Option<String>> {
        Self::cell(&self.trace_id, i)
    }

    pub fn span_id(&self, i: usize) -> Result<&Option<String>> {
        Self::cell(&self.span_id, i)
    }

    pub fn event_id(&self, i: usize) -> Result<&Option<String>> {
        Self::cell(&self.event_id, i)
    }

    pub fn body(&self, i: usize) -> Result<&Option<Value>> {
        Self::cell(&self.body, i)
    }

    pub fn attributes(&self, i: usize) -> Result<&BTreeMap<String, Value>> {
        Self::cell(&self.attributes, i)
    }

    pub fn resource_attributes(&self, i: usize) -> Result<&BTreeMap<String, Value>> {
        Self::cell(&self.resource_attributes, i)
    }

    /// Zone statistics: (min, max) event time, (min, max) sequence number,
    /// and the severity-code bit set. Empty chunks report `min > max`.
    pub fn zone(&self) -> ((i64, i64), (u64, u64), u32) {
        let z = &self.zone;
        (
            (
                z.min_time.load(Ordering::Acquire),
                z.max_time.load(Ordering::Acquire),
            ),
            (
                z.min_seq.load(Ordering::Acquire),
                z.max_seq.load(Ordering::Acquire),
            ),
            z.severities.load(Ordering::Acquire),
        )
    }

    /// Row `i` as an event (clones the values).
    pub fn event(&self, tenant: &TenantId, i: usize) -> Result<Event> {
        let sev = self.severity(i);
        Ok(Event {
            event_time: self.event_time(i),
            ingest_time: self.ingest_time(i),
            tenant_id: tenant.clone(),
            service: self.service(i)?.clone(),
            severity: Severity::from_u8(sev)
                .ok_or_else(|| Error::internal(format!("invalid severity {sev} in memtable")))?,
            severity_text: self.severity_text(i)?.clone(),
            body: self.body(i)?.clone(),
            trace_id: self.trace_id(i)?.clone(),
            span_id: self.span_id(i)?.clone(),
            event_id: self.event_id(i)?.clone(),
            attributes: self.attributes(i)?.clone(),
            resource_attributes: self.resource_attributes(i)?.clone(),
            schema_version: self.schema_version(i),
        })
    }
}

/// One tenant's rows.
#[derive(Debug)]
struct TenantMem {
    /// Rows written (serializes appends).
    writer: Mutex<usize>,
    chunks: RwLock<Vec<Arc<Chunk>>>,
    /// Rows visible to readers (`Release` after a whole batch).
    published: AtomicUsize,
}

impl TenantMem {
    fn new() -> Self {
        Self {
            writer: Mutex::new(0),
            chunks: RwLock::new(Vec::new()),
            published: AtomicUsize::new(0),
        }
    }

    /// Append `cols` (sequence `seq`) and publish them as one unit.
    fn append(&self, seq: u64, cols: EventColumns) {
        let mut written = self.writer.lock();
        let mut chunk: Option<Arc<Chunk>> = self.chunks.read().last().cloned();
        let EventColumns {
            event_time,
            ingest_time,
            severity,
            schema_version,
            service,
            severity_text,
            trace_id,
            span_id,
            event_id,
            body,
            attributes,
            resource_attributes,
            ..
        } = cols;
        let rows = event_time
            .into_iter()
            .zip(ingest_time)
            .zip(severity)
            .zip(schema_version)
            .zip(service)
            .zip(severity_text)
            .zip(trace_id)
            .zip(span_id)
            .zip(event_id)
            .zip(body)
            .zip(attributes)
            .zip(resource_attributes)
            .enumerate();
        for (
            idx,
            (
                ((((((((((et, it_), sev), sv), service), stext), trace), span), eid), body), attrs),
                res,
            ),
        ) in rows
        {
            let row = *written;
            let c = match &chunk {
                Some(c) if row < c.start + c.cap => Arc::clone(c),
                prev => {
                    let cap = prev
                        .as_ref()
                        .map_or(FIRST_CHUNK_ROWS, |p| (p.cap * 2).min(MAX_CHUNK_ROWS));
                    let c = Arc::new(Chunk::new(row, cap));
                    self.chunks.write().push(Arc::clone(&c));
                    chunk = Some(Arc::clone(&c));
                    c
                }
            };
            let i = row - c.start;
            c.seq[i].store(seq, Ordering::Relaxed);
            c.idx[i].store(u32::try_from(idx).unwrap_or(u32::MAX), Ordering::Relaxed);
            c.event_time[i].store(et, Ordering::Relaxed);
            c.ingest_time[i].store(it_, Ordering::Relaxed);
            c.severity[i].store(sev.as_u8(), Ordering::Relaxed);
            c.schema_version[i].store(sv, Ordering::Relaxed);
            // Each cell is written exactly once (single writer, fresh row).
            let _ = c.service[i].set(service);
            let _ = c.severity_text[i].set(stext);
            let _ = c.trace_id[i].set(trace);
            let _ = c.span_id[i].set(span);
            let _ = c.event_id[i].set(eid);
            let _ = c.body[i].set(body);
            let _ = c.attributes[i].set(attrs);
            let _ = c.resource_attributes[i].set(res);
            let z = &c.zone;
            z.min_time.fetch_min(et, Ordering::AcqRel);
            z.max_time.fetch_max(et, Ordering::AcqRel);
            z.min_seq.fetch_min(seq, Ordering::AcqRel);
            z.max_seq.fetch_max(seq, Ordering::AcqRel);
            z.severities
                .fetch_or(1u32 << u32::from(sev.as_u8().min(31)), Ordering::AcqRel);
            *written += 1;
        }
        // Everything above happens-before a reader that loads this count.
        self.published.store(*written, Ordering::Release);
    }

    fn view(&self) -> (Vec<Arc<Chunk>>, usize) {
        let rows = self.published.load(Ordering::Acquire);
        // Chunks are pushed before the rows they hold are published.
        (self.chunks.read().clone(), rows)
    }
}

/// Rows of one tenant visible at one instant: chunk pointers and a count.
#[derive(Debug, Clone)]
pub struct MemView {
    tenant: TenantId,
    chunks: Vec<Arc<Chunk>>,
    rows: usize,
}

impl MemView {
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// (chunk, visible rows in it), in insertion order.
    pub fn chunks(&self) -> impl Iterator<Item = (&Arc<Chunk>, usize)> {
        self.chunks
            .iter()
            .map(move |c| (c, self.rows.saturating_sub(c.start).min(c.cap)))
            .filter(|(_, n)| *n > 0)
    }

    /// Every visible row as `(seq, idx, event)` (clones the values).
    pub fn materialize(&self) -> Result<Vec<(u64, u32, Event)>> {
        let mut out = Vec::with_capacity(self.rows);
        for (c, n) in self.chunks() {
            for i in 0..n {
                out.push((c.seq(i), c.idx(i), c.event(&self.tenant, i)?));
            }
        }
        Ok(out)
    }

    /// Largest sequence number visible (0 when empty).
    pub fn max_seq(&self) -> u64 {
        self.chunks()
            .flat_map(|(c, n)| (0..n).map(move |i| c.seq(i)))
            .max()
            .unwrap_or(0)
    }
}

#[derive(Debug, Default)]
struct Stats {
    rows: usize,
    bytes: usize,
    tenant_bytes: HashMap<TenantId, usize>,
    /// Idempotency metadata of batches in this memtable, per tenant.
    metas: HashMap<TenantId, Vec<BatchMeta>>,
    first_insert: Option<Instant>,
}

/// A memtable: active (being appended to) or frozen (read-only, awaiting
/// flush; the engine stops appending before it freezes one).
#[derive(Debug, Default)]
pub struct Memtable {
    tenants: RwLock<HashMap<TenantId, Arc<TenantMem>>>,
    stats: Mutex<Stats>,
}

/// An immutable memtable awaiting flush.
#[derive(Debug)]
pub struct FrozenMemtable {
    mem: Arc<Memtable>,
    pub metas: HashMap<TenantId, Vec<BatchMeta>>,
    pub tenant_bytes: HashMap<TenantId, usize>,
    pub rows: usize,
    pub bytes: usize,
    /// Every WAL sequence number <= this is contained in this memtable or an
    /// older (frozen or flushed) one.
    pub through_seq: u64,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    fn tenant_mem(&self, t: &TenantId) -> Arc<TenantMem> {
        if let Some(m) = self.tenants.read().get(t) {
            return Arc::clone(m);
        }
        Arc::clone(
            self.tenants
                .write()
                .entry(t.clone())
                .or_insert_with(|| Arc::new(TenantMem::new())),
        )
    }

    /// Insert a committed batch (one tenant) atomically. Its size is
    /// [`batch_bytes`].
    pub fn insert_batch(
        &self,
        seq: u64,
        events: Vec<Event>,
        meta: Option<BatchMeta>,
    ) -> Result<()> {
        let bytes = batch_bytes(&events);
        let cols = EventColumns::from_events(events)?;
        self.insert(seq, cols, bytes, meta);
        Ok(())
    }

    /// Insert a committed batch given as columns (WAL replay, no row
    /// intermediate).
    pub fn insert_columns(&self, seq: u64, cols: EventColumns, meta: Option<BatchMeta>) {
        let bytes = columns_bytes(&cols);
        self.insert(seq, cols, bytes, meta);
    }

    fn insert(&self, seq: u64, cols: EventColumns, bytes: usize, meta: Option<BatchMeta>) {
        if cols.is_empty() {
            return;
        }
        let tenant = cols.tenant.clone();
        let n = cols.len();
        self.tenant_mem(&tenant).append(seq, cols);
        let mut s = self.stats.lock();
        s.first_insert.get_or_insert_with(Instant::now);
        s.rows += n;
        s.bytes += bytes;
        *s.tenant_bytes.entry(tenant.clone()).or_insert(0) += bytes;
        if let Some(m) = meta {
            s.metas.entry(tenant).or_default().push(m);
        }
    }

    pub fn rows(&self) -> usize {
        self.stats.lock().rows
    }

    pub fn bytes(&self) -> usize {
        self.stats.lock().bytes
    }

    pub fn is_empty(&self) -> bool {
        self.stats.lock().rows == 0
    }

    /// Age of the oldest unflushed row.
    pub fn age(&self) -> Option<std::time::Duration> {
        self.stats.lock().first_insert.map(|t| t.elapsed())
    }

    /// Approximate in-memory bytes held for `tenant`.
    pub fn tenant_bytes(&self, tenant: &TenantId) -> usize {
        self.stats
            .lock()
            .tenant_bytes
            .get(tenant)
            .copied()
            .unwrap_or(0)
    }

    /// The tenant's rows visible now (O(chunks)).
    pub fn view(&self, tenant: &TenantId) -> Option<MemView> {
        let m = Arc::clone(self.tenants.read().get(tenant)?);
        let (chunks, rows) = m.view();
        (rows > 0).then(|| MemView {
            tenant: tenant.clone(),
            chunks,
            rows,
        })
    }

    /// Tenants with rows, sorted.
    pub fn tenants(&self) -> Vec<TenantId> {
        let mut v: Vec<TenantId> = self.tenants.read().keys().cloned().collect();
        v.sort();
        v
    }
}

impl FrozenMemtable {
    /// Freeze `mem` (O(tenants): copies its counters). The caller has
    /// stopped appending to it.
    pub fn new(mem: Arc<Memtable>, through_seq: u64) -> Self {
        let (rows, bytes, tenant_bytes, metas) = {
            let s = mem.stats.lock();
            (s.rows, s.bytes, s.tenant_bytes.clone(), s.metas.clone())
        };
        Self {
            mem,
            metas,
            tenant_bytes,
            rows,
            bytes,
            through_seq,
        }
    }

    pub fn tenants(&self) -> Vec<TenantId> {
        self.mem.tenants()
    }

    pub fn view(&self, tenant: &TenantId) -> Option<MemView> {
        self.mem.view(tenant)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn ev(t: &TenantId, i: i64) -> Event {
        let mut e = Event::new(t.clone(), 1_000 + i, 2_000 + i);
        e.service = (i % 2 == 0).then(|| format!("svc{}", i % 3));
        e.body = Some(Value::String(format!("row {i}")));
        e.severity = if i % 5 == 0 {
            Severity::Error
        } else {
            Severity::Info
        };
        e.attributes.insert("k".into(), Value::Int(i));
        e
    }

    #[test]
    fn rows_round_trip_across_growing_chunks() {
        let m = Memtable::new();
        let t = TenantId::new("a").unwrap();
        let mut want = Vec::new();
        let mut seq = 1;
        let mut i = 0;
        // Batches of varied size crossing several chunk boundaries.
        for n in [1usize, 63, 2, 200, 1000, 5000] {
            let evs: Vec<Event> = (0..n)
                .map(|_| {
                    i += 1;
                    ev(&t, i)
                })
                .collect();
            for (j, e) in evs.iter().enumerate() {
                want.push((seq, j as u32, e.clone()));
            }
            m.insert_batch(seq, evs, None).unwrap();
            seq += 1;
        }
        let v = m.view(&t).unwrap();
        assert_eq!(v.rows(), want.len());
        assert_eq!(v.materialize().unwrap(), want);
        let caps: Vec<usize> = v.chunks().map(|(c, _)| c.capacity()).collect();
        assert_eq!(&caps[..4], &[64, 128, 256, 512]);
        assert!(caps.iter().all(|c| *c <= MAX_CHUNK_ROWS));
        assert_eq!(v.max_seq(), seq - 1);
        assert_eq!(m.rows(), want.len());
        assert_eq!(
            m.bytes(),
            want.iter().map(|w| row_bytes(&w.2)).sum::<usize>()
        );
        // Zone maps.
        let (c, _) = v.chunks().next().unwrap();
        let ((lo, hi), (slo, shi), sev) = c.zone();
        assert_eq!((lo, hi, slo, shi), (1_001, 1_064, 1, 2));
        assert_ne!(sev & (1 << Severity::Error.as_u8()), 0);
        assert_eq!(sev & (1 << Severity::Fatal.as_u8()), 0);
        assert!(m.view(&TenantId::new("other").unwrap()).is_none());
    }

    #[test]
    fn columns_and_rows_insert_identically() {
        let t = TenantId::new("a").unwrap();
        let evs: Vec<Event> = (0..300).map(|i| ev(&t, i)).collect();
        let (a, b) = (Memtable::new(), Memtable::new());
        a.insert_batch(7, evs.clone(), None).unwrap();
        b.insert_columns(7, EventColumns::from_events(evs).unwrap(), None);
        assert_eq!(a.bytes(), b.bytes());
        assert_eq!(
            a.view(&t).unwrap().materialize().unwrap(),
            b.view(&t).unwrap().materialize().unwrap()
        );
    }

    /// A reader running next to the writer sees whole batches only, in
    /// order, and never an unwritten cell.
    #[test]
    fn concurrent_readers_see_whole_batches() {
        let m = Arc::new(Memtable::new());
        let t = TenantId::new("a").unwrap();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let (m, t, stop) = (Arc::clone(&m), t.clone(), Arc::clone(&stop));
                std::thread::spawn(move || {
                    let mut checks = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        if let Some(v) = m.view(&t) {
                            assert_eq!(v.rows() % 37, 0, "partial batch visible");
                            let rows = v.materialize().unwrap();
                            for (k, (seq, idx, e)) in rows.iter().enumerate() {
                                assert_eq!(*seq as usize, k / 37 + 1);
                                assert_eq!(*idx as usize, k % 37);
                                assert_eq!(e.event_time, 1_000 + k as i64);
                            }
                            checks += 1;
                        }
                    }
                    checks
                })
            })
            .collect();
        let mut i = -1;
        for seq in 1..=400u64 {
            let evs: Vec<Event> = (0..37)
                .map(|_| {
                    i += 1;
                    ev(&t, i)
                })
                .collect();
            m.insert_batch(seq, evs, None).unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let checks: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
        assert!(checks > 0);
        assert_eq!(m.view(&t).unwrap().rows(), 400 * 37);
    }

    #[test]
    fn mixed_tenant_batches_are_refused() {
        let m = Memtable::new();
        let (a, b) = (TenantId::new("a").unwrap(), TenantId::new("b").unwrap());
        assert!(m.insert_batch(1, vec![ev(&a, 1), ev(&b, 2)], None).is_err());
        assert!(m.insert_batch(1, vec![], None).is_err());
        assert!(m.is_empty());
    }
}
