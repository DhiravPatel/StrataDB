//! Bounded, durable write deduplication (spec §7.3, ticket G-006).
//!
//! # Contract (documented in docs/api/http.md and ADR-0019)
//! * Scope: `(tenant, key)`. Keys are 1–128 visible ASCII characters.
//! * A retry with the same key and the same request digest within the window
//!   returns the **original** receipt and stores nothing (`duplicate = true`).
//! * The same key with a different digest is rejected (`already_exists`).
//! * A concurrent request with a key that is still being committed gets a
//!   retryable `unavailable` error.
//! * Survives restart: keys travel in the WAL record (type 2) and, after a
//!   flush, in the footer extension (tag 1) of the segment holding the rows.
//! * Bounded: entries older than the window are forgotten; under capacity
//!   pressure the oldest entries are evicted early (counted in stats). Beyond
//!   that, delivery is at-least-once. This is **not** global exactly-once.

use std::collections::{HashMap, VecDeque};

use stratadb_common::codec::{Decoder, Encoder};
use stratadb_common::{Error, ErrorKind, Result, TenantId};

/// Segment footer extension tag for idempotency metadata.
pub const EXT_IDEMPOTENCY: u16 = 1;
const RECORD_VERSION: u8 = 1;
pub const MAX_KEY_BYTES: usize = 128;

/// A client-supplied idempotency key with the digest of the request content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey {
    key: String,
    digest: [u8; 32],
}

impl IdempotencyKey {
    /// `digest` must be computed by the caller over the client request bytes
    /// (not the encoded events, which include server-assigned ingest times).
    pub fn new(key: impl Into<String>, digest: [u8; 32]) -> Result<Self> {
        let key = key.into();
        if key.is_empty()
            || key.len() > MAX_KEY_BYTES
            || !key.bytes().all(|b| (0x21..=0x7e).contains(&b))
        {
            return Err(Error::invalid_input(format!(
                "idempotency key must be 1..={MAX_KEY_BYTES} visible ASCII characters"
            )));
        }
        Ok(Self { key, digest })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }
}

/// Persisted description of one idempotent batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchMeta {
    pub key: String,
    pub digest: [u8; 32],
    pub commit_seq: u64,
    pub accepted: u32,
    pub committed_ms: i64,
}

/// WAL payload for `RecordType::IdempotentEventBatch`.
pub fn encode_record(key: &IdempotencyKey, committed_ms: i64, batch_payload: &[u8]) -> Vec<u8> {
    let mut e = Encoder::with_capacity(batch_payload.len() + 64);
    e.put_u8(RECORD_VERSION);
    e.put_str(&key.key);
    e.put_raw(&key.digest);
    e.put_ivarint(committed_ms);
    e.put_raw(batch_payload);
    e.into_inner()
}

/// Split a type-2 WAL payload into (key, digest, committed_ms, batch payload).
pub fn decode_record(payload: &[u8]) -> Result<(String, [u8; 32], i64, &[u8])> {
    let mut d = Decoder::new(payload, "idempotent WAL record");
    let v = d.get_u8()?;
    if v != RECORD_VERSION {
        return Err(Error::FormatVersion {
            what: "idempotent WAL record",
            found: u32::from(v),
            supported: RECORD_VERSION.to_string(),
        });
    }
    let key = d.get_string(MAX_KEY_BYTES)?;
    let mut digest = [0u8; 32];
    digest.copy_from_slice(d.get_raw(32)?);
    let committed_ms = d.get_ivarint()?;
    let rest = d.get_raw(d.remaining())?;
    Ok((key, digest, committed_ms, rest))
}

pub fn encode_extension(metas: &[BatchMeta]) -> Vec<u8> {
    let mut e = Encoder::new();
    e.put_uvarint(metas.len() as u64);
    for m in metas {
        e.put_str(&m.key);
        e.put_raw(&m.digest);
        e.put_uvarint(m.commit_seq);
        e.put_uvarint(u64::from(m.accepted));
        e.put_ivarint(m.committed_ms);
    }
    e.into_inner()
}

pub fn decode_extension(b: &[u8]) -> Result<Vec<BatchMeta>> {
    let mut d = Decoder::new(b, "idempotency extension");
    let n = d.get_len(1 << 24, 36)?;
    let mut out = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        let key = d.get_string(MAX_KEY_BYTES)?;
        let mut digest = [0u8; 32];
        digest.copy_from_slice(d.get_raw(32)?);
        let commit_seq = d.get_uvarint()?;
        let accepted = u32::try_from(d.get_uvarint()?)
            .map_err(|_| Error::corruption("idempotency extension", "accepted overflows u32"))?;
        out.push(BatchMeta {
            key,
            digest,
            commit_seq,
            accepted,
            committed_ms: d.get_ivarint()?,
        });
    }
    d.expect_end()?;
    Ok(out)
}

#[derive(Debug, Clone)]
enum Slot {
    InFlight,
    Committed(BatchMeta),
}

/// Outcome of [`DedupIndex::reserve`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reservation {
    /// No prior commit; the caller must `complete` or `abort`.
    Reserved,
    /// Already committed with the same digest.
    Duplicate(BatchMeta),
}

/// In-memory index of recent idempotency keys.
#[derive(Debug)]
pub struct DedupIndex {
    window_ms: i64,
    max_keys: usize,
    map: HashMap<(TenantId, String), Slot>,
    /// Committed entries in insertion order: (committed_ms, tenant, key, seq).
    order: VecDeque<(i64, TenantId, String, u64)>,
    pub evictions: u64,
}

impl DedupIndex {
    pub fn new(window_ms: i64, max_keys: usize) -> Self {
        Self {
            window_ms,
            max_keys: max_keys.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
            evictions: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn pop_front(&mut self, evicted: bool) {
        if let Some((_, t, k, seq)) = self.order.pop_front() {
            let id = (t, k);
            // A key may have been forgotten and re-used: only remove the entry
            // this order record refers to.
            if matches!(self.map.get(&id), Some(Slot::Committed(m)) if m.commit_seq == seq) {
                self.map.remove(&id);
                if evicted {
                    self.evictions += 1;
                }
            }
        }
    }

    /// Forget expired entries and enforce the capacity bound.
    pub fn prune(&mut self, now_ms: i64) {
        let cutoff = now_ms.saturating_sub(self.window_ms);
        while self.order.front().is_some_and(|(t, ..)| *t < cutoff) {
            self.pop_front(false);
        }
        while self.map.len() > self.max_keys && !self.order.is_empty() {
            self.pop_front(true);
        }
    }

    pub fn reserve(
        &mut self,
        tenant: &TenantId,
        key: &IdempotencyKey,
        now_ms: i64,
    ) -> Result<Reservation> {
        self.prune(now_ms);
        let id = (tenant.clone(), key.key.clone());
        match self.map.get(&id) {
            Some(Slot::Committed(m)) if m.digest == key.digest => {
                Ok(Reservation::Duplicate(m.clone()))
            }
            Some(Slot::Committed(_)) => Err(Error::new(
                ErrorKind::AlreadyExists,
                "idempotency key was already used with a different request body",
            )),
            Some(Slot::InFlight) => Err(Error::unavailable(
                "a request with this idempotency key is still being committed; retry",
            )),
            None => {
                self.map.insert(id, Slot::InFlight);
                Ok(Reservation::Reserved)
            }
        }
    }

    pub fn abort(&mut self, tenant: &TenantId, key: &IdempotencyKey) {
        let id = (tenant.clone(), key.key.clone());
        if matches!(self.map.get(&id), Some(Slot::InFlight)) {
            self.map.remove(&id);
        }
    }

    pub fn complete(&mut self, tenant: &TenantId, meta: BatchMeta) {
        self.order.push_back((
            meta.committed_ms,
            tenant.clone(),
            meta.key.clone(),
            meta.commit_seq,
        ));
        self.map
            .insert((tenant.clone(), meta.key.clone()), Slot::Committed(meta));
        while self.map.len() > self.max_keys && !self.order.is_empty() {
            self.pop_front(true);
        }
    }

    /// Insert metadata recovered from the WAL or segment footers. Callers
    /// should insert in `committed_ms` order.
    pub fn insert_recovered(&mut self, tenant: &TenantId, meta: BatchMeta, now_ms: i64) {
        if meta.committed_ms < now_ms.saturating_sub(self.window_ms) {
            return;
        }
        self.complete(tenant, meta);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t() -> TenantId {
        TenantId::new("t").unwrap()
    }

    fn k(s: &str, d: u8) -> IdempotencyKey {
        IdempotencyKey::new(s, [d; 32]).unwrap()
    }

    fn meta(key: &str, d: u8, seq: u64, ms: i64) -> BatchMeta {
        BatchMeta {
            key: key.into(),
            digest: [d; 32],
            commit_seq: seq,
            accepted: 3,
            committed_ms: ms,
        }
    }

    #[test]
    fn key_validation() {
        assert!(IdempotencyKey::new("", [0; 32]).is_err());
        assert!(IdempotencyKey::new("a b", [0; 32]).is_err());
        assert!(IdempotencyKey::new("x".repeat(129), [0; 32]).is_err());
        assert!(IdempotencyKey::new("req-1:abc", [0; 32]).is_ok());
    }

    #[test]
    fn reserve_complete_duplicate_conflict() {
        let mut ix = DedupIndex::new(1000, 10);
        assert_eq!(
            ix.reserve(&t(), &k("a", 1), 0).unwrap(),
            Reservation::Reserved
        );
        assert_eq!(
            ix.reserve(&t(), &k("a", 1), 0).unwrap_err().kind(),
            ErrorKind::Unavailable
        );
        ix.complete(&t(), meta("a", 1, 5, 0));
        assert_eq!(
            ix.reserve(&t(), &k("a", 1), 10).unwrap(),
            Reservation::Duplicate(meta("a", 1, 5, 0))
        );
        assert_eq!(
            ix.reserve(&t(), &k("a", 2), 10).unwrap_err().kind(),
            ErrorKind::AlreadyExists
        );
        // Other tenants are independent.
        let other = TenantId::new("u").unwrap();
        assert_eq!(
            ix.reserve(&other, &k("a", 2), 10).unwrap(),
            Reservation::Reserved
        );
        ix.abort(&other, &k("a", 2));
        assert_eq!(
            ix.reserve(&other, &k("a", 2), 10).unwrap(),
            Reservation::Reserved
        );
    }

    #[test]
    fn window_and_capacity() {
        let mut ix = DedupIndex::new(100, 2);
        ix.complete(&t(), meta("a", 1, 1, 0));
        ix.complete(&t(), meta("b", 1, 2, 50));
        ix.complete(&t(), meta("c", 1, 3, 60)); // evicts "a" (capacity)
        assert_eq!(ix.evictions, 1);
        assert_eq!(
            ix.reserve(&t(), &k("a", 9), 60).unwrap(),
            Reservation::Reserved
        );
        ix.abort(&t(), &k("a", 9));
        // At t=155, "b" (50) is expired, "c" (60) is not.
        assert_eq!(
            ix.reserve(&t(), &k("b", 9), 155).unwrap(),
            Reservation::Reserved
        );
        assert!(matches!(
            ix.reserve(&t(), &k("c", 1), 155).unwrap(),
            Reservation::Duplicate(_)
        ));
        // Expired recovered entries are skipped.
        ix.insert_recovered(&t(), meta("old", 1, 9, 0), 1000);
        assert_eq!(
            ix.reserve(&t(), &k("old", 2), 1000).unwrap(),
            Reservation::Reserved
        );
    }

    #[test]
    fn codecs_roundtrip_and_reject_garbage() {
        let m = vec![meta("a", 1, 7, -5), meta("bb", 2, 9, 1 << 40)];
        assert_eq!(decode_extension(&encode_extension(&m)).unwrap(), m);
        let rec = encode_record(&k("key", 3), 42, b"batch");
        let (key, d, ms, rest) = decode_record(&rec).unwrap();
        assert_eq!(
            (key.as_str(), d, ms, rest),
            ("key", [3; 32], 42, &b"batch"[..])
        );
        for cut in 0..rec.len() - 5 {
            assert!(decode_record(&rec[..cut]).is_err());
        }
        assert!(decode_extension(&[5]).is_err());
    }
}
