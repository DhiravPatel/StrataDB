//! Tenant-partitioned in-memory buffer of committed-but-unflushed rows (spec §9).
//!
//! Rows enter only *after* their WAL record reached the configured durability
//! boundary. A batch is inserted under one lock acquisition, so readers see
//! all of a batch or none of it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;
use stratadb_common::TenantId;
use stratadb_schema::Event;

use crate::idempotency::BatchMeta;

/// One row: (WAL sequence number, index within the WAL batch, event).
#[derive(Debug, Clone)]
pub struct MemRow {
    pub seq: u64,
    pub idx: u32,
    pub event: Arc<Event>,
}

#[derive(Debug, Default)]
struct Inner {
    by_tenant: HashMap<TenantId, Vec<MemRow>>,
    /// Idempotency metadata of batches in this memtable, per tenant.
    metas: HashMap<TenantId, Vec<BatchMeta>>,
    tenant_bytes: HashMap<TenantId, usize>,
    rows: usize,
    bytes: usize,
    first_insert: Option<Instant>,
}

/// The active (mutable) memtable.
#[derive(Debug, Default)]
pub struct Memtable {
    inner: Mutex<Inner>,
}

/// An immutable memtable awaiting flush.
#[derive(Debug)]
pub struct FrozenMemtable {
    pub by_tenant: HashMap<TenantId, Vec<MemRow>>,
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

    /// Insert a committed batch atomically.
    pub fn insert_batch(&self, seq: u64, events: Vec<Event>, meta: Option<BatchMeta>) {
        let now = Instant::now();
        let mut g = self.inner.lock();
        g.first_insert.get_or_insert(now);
        if let (Some(m), Some(first)) = (meta, events.first()) {
            g.metas.entry(first.tenant_id.clone()).or_default().push(m);
        }
        for (idx, ev) in events.into_iter().enumerate() {
            let sz = ev.approx_size() + std::mem::size_of::<MemRow>();
            g.bytes += sz;
            *g.tenant_bytes.entry(ev.tenant_id.clone()).or_insert(0) += sz;
            g.rows += 1;
            let tenant = ev.tenant_id.clone();
            g.by_tenant.entry(tenant).or_default().push(MemRow {
                seq,
                idx: idx as u32,
                event: Arc::new(ev),
            });
        }
    }

    pub fn rows(&self) -> usize {
        self.inner.lock().rows
    }

    pub fn bytes(&self) -> usize {
        self.inner.lock().bytes
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().rows == 0
    }

    /// Age of the oldest unflushed row.
    pub fn age(&self) -> Option<std::time::Duration> {
        self.inner.lock().first_insert.map(|t| t.elapsed())
    }

    /// Approximate in-memory bytes held for `tenant`.
    pub fn tenant_bytes(&self, tenant: &TenantId) -> usize {
        self.inner
            .lock()
            .tenant_bytes
            .get(tenant)
            .copied()
            .unwrap_or(0)
    }

    /// Clone this tenant's rows (Arc clones; events are shared).
    pub fn tenant_rows(&self, tenant: &TenantId, out: &mut Vec<MemRow>) {
        if let Some(rows) = self.inner.lock().by_tenant.get(tenant) {
            out.extend(rows.iter().cloned());
        }
    }

    /// Move all rows out into a frozen memtable, leaving this one empty.
    pub fn freeze(&self, through_seq: u64) -> FrozenMemtable {
        let mut g = self.inner.lock();
        let taken = std::mem::take(&mut *g);
        FrozenMemtable {
            by_tenant: taken.by_tenant,
            metas: taken.metas,
            tenant_bytes: taken.tenant_bytes,
            rows: taken.rows,
            bytes: taken.bytes,
            through_seq,
        }
    }
}

impl FrozenMemtable {
    pub fn tenant_rows(&self, tenant: &TenantId, out: &mut Vec<MemRow>) {
        if let Some(rows) = self.by_tenant.get(tenant) {
            out.extend(rows.iter().cloned());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_by_tenant_and_freezes() {
        let m = Memtable::new();
        let a = TenantId::new("a").unwrap();
        let b = TenantId::new("b").unwrap();
        m.insert_batch(
            1,
            vec![Event::new(a.clone(), 1, 1), Event::new(b.clone(), 2, 2)],
            None,
        );
        m.insert_batch(2, vec![Event::new(a.clone(), 3, 3)], None);
        let mut out = Vec::new();
        m.tenant_rows(&a, &mut out);
        assert_eq!(
            out.iter().map(|r| (r.seq, r.idx)).collect::<Vec<_>>(),
            vec![(1, 0), (2, 0)]
        );
        assert_eq!(m.rows(), 3);
        let f = m.freeze(2);
        assert!(m.is_empty());
        assert_eq!(f.rows, 3);
        let mut out = Vec::new();
        f.tenant_rows(&b, &mut out);
        assert_eq!(out.len(), 1);
    }
}
