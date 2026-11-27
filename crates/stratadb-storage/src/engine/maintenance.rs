//! Compaction and retention (spec §16), built on one primitive: [`Shared::rewrite`].
//!
//! # Safety protocol (spec §16.3)
//! 1. Inputs stay committed and query-visible while the output is built.
//! 2. The output is written durably (tmp + fsync + rename + dir fsync) and
//!    fully verified by reading it back.
//! 3. One manifest commit atomically removes the inputs, adds the output, and
//!    records the inputs as `obsolete_segments`.
//! 4. The visible segment set is swapped under the state lock, so a snapshot
//!    sees either all inputs or the output — never both, never neither.
//! 5. An obsolete file is deleted only when no snapshot still holds its
//!    handle; after a crash, recovery deletes recorded obsolete files.
//!
//! Crash before step 3: the output id is >= the durable `next_segment_id`
//! (all maintenance and flushes are serialized by the manifest lock), so
//! recovery deletes it as an orphan and the inputs remain authoritative.
//! Crash after step 3: the output is committed; inputs are deleted on open.

use std::collections::BTreeSet;

use stratadb_segment::StoredRow;

use super::*;
use crate::manifest::{ObsoleteSegment, RetentionPolicy};

/// Compaction policy.
#[derive(Debug, Clone)]
pub struct CompactionOptions {
    pub enabled: bool,
    /// Merge only when a tenant has at least this many small segments.
    pub min_inputs: usize,
    pub max_inputs: usize,
    /// Segments smaller than this are compaction candidates.
    pub small_segment_bytes: u64,
    /// Stop adding inputs once their total size would exceed this.
    pub target_segment_bytes: u64,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            min_inputs: 4,
            max_inputs: 32,
            small_segment_bytes: 16 * 1024 * 1024,
            target_segment_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactionReport {
    pub groups: usize,
    pub inputs: usize,
    pub outputs: usize,
    pub rows: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub obsolete_deleted: usize,
    /// Groups postponed because the background memory region was full.
    pub deferred_for_memory: usize,
}

/// Result of [`Shared::upgrade`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpgradeReport {
    /// Segments per on-disk format version before the run.
    pub versions_before: BTreeMap<u16, usize>,
    /// Segments rewritten (older format, or plaintext while encryption is on).
    pub segments_upgraded: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub dry_run: bool,
}

/// Index filters of one segment.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentIndexes {
    pub segment_id: u64,
    pub tenant: TenantId,
    pub rows: u64,
    pub bloom_fields: Vec<String>,
    pub bloom_bytes: usize,
    pub ngram_fields: Vec<String>,
    pub ngram_bytes: usize,
    /// Present when verified.
    pub check: Option<IndexCheck>,
}

/// Result of verifying one segment's filters against its rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexCheck {
    /// Filters missing a value stored in the segment: queries could skip
    /// matching rows. Must be empty.
    pub false_negatives: Vec<String>,
    /// The stored filters are byte-identical to what the current
    /// configuration builds from the rows (false = stale settings).
    pub current: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexRebuildReport {
    pub segments_checked: usize,
    pub segments_stale: usize,
    pub segments_unsound: usize,
    pub segments_rebuilt: usize,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionReport {
    pub segments_dropped: usize,
    pub segments_rewritten: usize,
    pub rows_deleted: u64,
    pub tenants_on_hold: usize,
    pub obsolete_deleted: usize,
}

struct Rewritten {
    output: Option<Arc<SegmentHandle>>,
    rows_kept: u64,
    rows_dropped: u64,
}

/// Memory a rewrite holds: every input row decoded (encoded payload with
/// header overhead, plus the row structs).
fn rewrite_memory_estimate(inputs: &[Arc<SegmentHandle>]) -> u64 {
    inputs
        .iter()
        .map(|h| {
            let f = h.reader.footer();
            let payload: u64 = f.columns.iter().map(|c| c.raw_len).sum::<u64>()
                + f.shredded.iter().map(|c| c.raw_len).sum::<u64>();
            payload.saturating_mul(3).saturating_add(
                f.row_count
                    .saturating_mul(std::mem::size_of::<StoredRow>() as u64),
            )
        })
        .fold(0u64, u64::saturating_add)
}

fn is_memory_limit(e: &Error) -> bool {
    e.to_string().contains("MEMORY_LIMIT_EXCEEDED")
}

impl Shared {
    /// Rewrite `inputs` (one tenant, all currently committed) into at most one
    /// segment holding the rows for which `keep` is true. Caller holds the
    /// manifest lock, passed as `man`.
    fn rewrite(
        &self,
        man: &mut Manifest,
        tenant: &TenantId,
        inputs: &[Arc<SegmentHandle>],
        keep: &dyn Fn(&Event) -> bool,
    ) -> Result<Rewritten> {
        self.check_healthy()?;
        let input_ids: BTreeSet<u64> = inputs.iter().map(|h| h.entry.id).collect();
        if inputs.iter().any(|h| &h.entry.tenant != tenant)
            || input_ids
                .iter()
                .any(|id| !man.segments.iter().any(|s| s.id == *id))
        {
            return Err(Error::internal(
                "rewrite inputs must be committed segments of one tenant",
            ));
        }
        // Every input row is held in memory during the rewrite: reserve it on
        // the background region first (spec VIII.5.2 merge_memory_limit).
        let _mem = match &self.opts.background_memory {
            Some(t) => Some(stratadb_common::memory::Reservation::new(
                t,
                rewrite_memory_estimate(inputs),
                Duration::ZERO,
            )?),
            None => None,
        };
        let mut rows: Vec<StoredRow> = Vec::new();
        let mut metas = Vec::new();
        for h in inputs {
            rows.extend(h.reader.read_rows()?);
            if let Some(x) = h.reader.footer().extension(idempotency::EXT_IDEMPOTENCY) {
                metas.extend(idempotency::decode_extension(&x.data)?);
            }
        }
        let total = rows.len() as u64;
        rows.retain(|r| keep(&r.event));
        let rows_kept = rows.len() as u64;
        let now_ms = time::now_unix_millis();
        let window = i64::try_from(self.opts.idempotency_window.as_millis()).unwrap_or(i64::MAX);
        metas.retain(|m| m.committed_ms >= now_ms.saturating_sub(window));
        metas.sort_by_key(|m| m.commit_seq);

        let mut next = man.clone();
        next.version += 1;
        let mut output = None;
        if !rows.is_empty() {
            let id = man.next_segment_id;
            let path = segment_path(&self.dir, tenant, id);
            let mut refs: Vec<RowRef<'_>> = rows
                .iter()
                .map(|r| RowRef {
                    seq: r.seq,
                    idx: r.idx,
                    event: &r.event,
                })
                .collect();
            let extensions = self.segment_extensions(&refs, &metas);
            let created_ms = now_ms;
            let ws = stratadb_segment::write_segment_io(
                &*self.opts.io_engine(),
                &path,
                id,
                tenant,
                &mut refs,
                created_ms,
                extensions,
                &self.segment_write_options(),
                self.keys.as_deref(),
            )?;
            failpoint::hit("compaction::after_output_write")?;
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
                lineage: input_ids.iter().copied().collect(),
            };
            let handle = match open_segment(
                &self.dir,
                &entry,
                true,
                self.keys.as_deref(),
                &self.opts.io_engine(),
            ) {
                Ok(h) => h.tracked(self.opts.cache_memory.as_ref()),
                Err(e) => {
                    let _ = fsutil::remove_if_exists(&path);
                    return Err(e);
                }
            };
            next.next_segment_id = id + 1;
            next.segments.push(entry);
            output = Some(Arc::new(handle));
        }
        next.segments.retain(|s| !input_ids.contains(&s.id));
        next.obsolete_segments
            .extend(inputs.iter().map(|h| ObsoleteSegment {
                id: h.entry.id,
                tenant: tenant.clone(),
            }));
        if let Err(e) = next.commit_with(&self.dir, self.keys.as_deref(), &*self.opts.io_engine()) {
            let e = Error::indeterminate(format!("manifest commit failed during rewrite: {e}"));
            self.fail(&e);
            return Err(e);
        }
        *man = next;
        if let Err(e) = failpoint::hit("compaction::after_manifest_commit") {
            let e = Error::indeterminate(e.to_string());
            self.fail(&e);
            return Err(e);
        }
        {
            let mut st = self.state.lock();
            let mut segs: Vec<Arc<SegmentHandle>> = st
                .segments
                .iter()
                .filter(|s| !input_ids.contains(&s.entry.id))
                .cloned()
                .collect();
            segs.extend(output.iter().cloned());
            segs.sort_by_key(|s| s.entry.id);
            st.segments = Arc::new(segs);
            st.obsolete.extend(inputs.iter().cloned());
        }
        Ok(Rewritten {
            output,
            rows_kept,
            rows_dropped: total - rows_kept,
        })
    }

    /// Delete obsolete segment files that no snapshot references any more,
    /// then drop them from the manifest's obsolete list.
    fn reclaim_obsolete(&self, man: &mut Manifest) -> Result<usize> {
        let deletable: Vec<Arc<SegmentHandle>> = {
            let mut st = self.state.lock();
            let (del, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut st.obsolete)
                .into_iter()
                .partition(|h| Arc::strong_count(h) == 1);
            st.obsolete = keep;
            del
        };
        if deletable.is_empty() {
            return Ok(0);
        }
        let mut dirs = BTreeSet::new();
        for h in &deletable {
            let path = segment_path(&self.dir, &h.entry.tenant, h.entry.id);
            if let Err(e) = fsutil::remove_if_exists(&path) {
                self.state.lock().obsolete.extend(deletable.iter().cloned());
                return Err(e);
            }
            failpoint::hit("compaction::after_delete_obsolete")?;
            dirs.insert(tenant_dir(&self.dir, &h.entry.tenant));
        }
        for d in &dirs {
            fsutil::fsync_dir(d)?;
        }
        let gone: BTreeSet<u64> = deletable.iter().map(|h| h.entry.id).collect();
        let mut next = man.clone();
        next.version += 1;
        next.obsolete_segments.retain(|o| !gone.contains(&o.id));
        if let Err(e) = next.commit_with(&self.dir, self.keys.as_deref(), &*self.opts.io_engine()) {
            let e = Error::indeterminate(format!("manifest commit failed after deletion: {e}"));
            self.fail(&e);
            return Err(e);
        }
        *man = next;
        self.counters
            .segments_deleted
            .fetch_add(gone.len() as u64, Ordering::Relaxed);
        Ok(gone.len())
    }

    /// One compaction pass: at most one merge group per tenant.
    pub(super) fn compact(&self) -> Result<CompactionReport> {
        self.check_healthy()?;
        let c = &self.opts.compaction;
        let mut man = self.manifest.lock();
        // Plan in a scope that ends before rewriting: any handle clone kept
        // alive here would look like a reader pin to `reclaim_obsolete`.
        let plan: Vec<(TenantId, Vec<Arc<SegmentHandle>>, u64)> = {
            let segs = Arc::clone(&self.state.lock().segments);
            let mut by_tenant: BTreeMap<TenantId, Vec<Arc<SegmentHandle>>> = BTreeMap::new();
            for s in segs
                .iter()
                .filter(|s| s.entry.file_bytes < c.small_segment_bytes)
            {
                by_tenant
                    .entry(s.entry.tenant.clone())
                    .or_default()
                    .push(Arc::clone(s));
            }
            let mut plan = Vec::new();
            for (tenant, mut cands) in by_tenant {
                if cands.len() < c.min_inputs.max(2) {
                    continue;
                }
                cands.sort_by_key(|s| s.entry.min_seq);
                let mut group = Vec::new();
                let mut bytes = 0u64;
                for s in cands {
                    if group.len() >= c.max_inputs
                        || (group.len() >= 2 && bytes + s.entry.file_bytes > c.target_segment_bytes)
                    {
                        break;
                    }
                    bytes += s.entry.file_bytes;
                    group.push(s);
                }
                if group.len() >= 2 {
                    plan.push((tenant, group, bytes));
                }
            }
            plan
        };
        let mut report = CompactionReport::default();
        for (tenant, group, bytes) in plan {
            // The output can be as large as the inputs until they are deleted.
            if let Err(e) = self.check_disk(bytes) {
                tracing::warn!(error = %e, "skipping compaction: not enough disk space");
                continue;
            }
            let r = match self.rewrite(&mut man, &tenant, &group, &|_| true) {
                Ok(r) => r,
                Err(e) if e.kind() == ErrorKind::ResourceExhausted && is_memory_limit(&e) => {
                    tracing::debug!(error = %e, "compaction deferred: background memory is full");
                    report.deferred_for_memory += 1;
                    continue;
                }
                Err(e) => return Err(e),
            };
            report.groups += 1;
            report.inputs += group.len();
            report.rows += r.rows_kept;
            report.bytes_in += bytes;
            if let Some(o) = &r.output {
                report.outputs += 1;
                report.bytes_out += o.entry.file_bytes;
            }
            self.counters.compactions.fetch_add(1, Ordering::Relaxed);
        }
        report.obsolete_deleted = self.reclaim_obsolete(&mut man)?;
        Ok(report)
    }

    /// Apply retention policies as of `now_ms`.
    pub(super) fn enforce_retention(&self, now_ms: i64) -> Result<RetentionReport> {
        self.check_healthy()?;
        let mut man = self.manifest.lock();
        let default = self.opts.default_retention.map(|d| RetentionPolicy {
            max_age_ms: i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
            hold: false,
        });
        let mut report = RetentionReport::default();
        // (tenant, cutoff_ns, fully expired segments, straddling segments)
        type Plan = (
            TenantId,
            i64,
            Vec<Arc<SegmentHandle>>,
            Vec<Arc<SegmentHandle>>,
        );
        let plan: Vec<Plan> = {
            let segs = Arc::clone(&self.state.lock().segments);
            let tenants: BTreeSet<TenantId> = segs.iter().map(|s| s.entry.tenant.clone()).collect();
            let mut plan = Vec::new();
            for tenant in tenants {
                let Some(policy) = man.retention.get(&tenant).copied().or(default) else {
                    continue;
                };
                if policy.hold {
                    report.tenants_on_hold += 1;
                    continue;
                }
                let cutoff = now_ms
                    .saturating_sub(policy.max_age_ms)
                    .saturating_mul(1_000_000);
                let mine = segs.iter().filter(|s| s.entry.tenant == tenant);
                let expired = mine
                    .clone()
                    .filter(|s| s.entry.max_event_time < cutoff)
                    .cloned()
                    .collect();
                let straddle = mine
                    .filter(|s| s.entry.min_event_time < cutoff && s.entry.max_event_time >= cutoff)
                    .cloned()
                    .collect();
                plan.push((tenant, cutoff, expired, straddle));
            }
            plan
        };
        for (tenant, cutoff, expired, straddle) in plan {
            if !expired.is_empty() {
                let r = self.rewrite(&mut man, &tenant, &expired, &|_| false)?;
                report.segments_dropped += expired.len();
                report.rows_deleted += r.rows_dropped;
            }
            for s in straddle {
                let r = self.rewrite(&mut man, &tenant, &[s], &|e| e.event_time >= cutoff)?;
                report.segments_rewritten += 1;
                report.rows_deleted += r.rows_dropped;
            }
        }
        report.obsolete_deleted = self.reclaim_obsolete(&mut man)?;
        self.counters
            .retention_rows_deleted
            .fetch_add(report.rows_deleted, Ordering::Relaxed);
        Ok(report)
    }

    /// Rewrite every segment older than the current format (spec §30.2).
    /// Each segment is rewritten with the crash-safe `rewrite` protocol, so
    /// an interrupted upgrade leaves a consistent mix of versions.
    pub(super) fn upgrade(&self, dry_run: bool) -> Result<UpgradeReport> {
        self.check_healthy()?;
        let mut man = self.manifest.lock();
        let mut report = UpgradeReport {
            dry_run,
            ..Default::default()
        };
        let old: Vec<Arc<SegmentHandle>> = {
            let segs = Arc::clone(&self.state.lock().segments);
            for s in segs.iter() {
                *report
                    .versions_before
                    .entry(s.entry.segment_format_version)
                    .or_insert(0) += 1;
            }
            // Older formats, and plaintext segments once encryption is on.
            let encrypt = self.keys.is_some();
            segs.iter()
                .filter(|s| {
                    s.entry.segment_format_version < stratadb_segment::FORMAT_VERSION
                        || (encrypt && !s.reader.is_sealed())
                })
                .cloned()
                .collect()
        };
        report.bytes_before = old.iter().map(|s| s.entry.file_bytes).sum();
        if dry_run {
            return Ok(report);
        }
        for seg in old {
            // Old and new copies coexist until the old one is reclaimed.
            self.check_disk(seg.entry.file_bytes)?;
            let tenant = seg.entry.tenant.clone();
            let r = self.rewrite(&mut man, &tenant, &[seg], &|_| true)?;
            report.segments_upgraded += 1;
            report.bytes_after += r.output.as_ref().map_or(0, |o| o.entry.file_bytes);
        }
        self.reclaim_obsolete(&mut man)?;
        Ok(report)
    }

    pub(super) fn set_retention(
        &self,
        tenant: &TenantId,
        policy: Option<RetentionPolicy>,
    ) -> Result<()> {
        self.check_healthy()?;
        if let Some(p) = policy {
            if p.max_age_ms <= 0 {
                return Err(Error::invalid_input("retention max age must be positive"));
            }
        }
        let mut man = self.manifest.lock();
        let mut next = man.clone();
        next.version += 1;
        match policy {
            Some(p) => {
                next.retention.insert(tenant.clone(), p);
            }
            None => {
                next.retention.remove(tenant);
            }
        }
        if let Err(e) = next.commit_with(&self.dir, self.keys.as_deref(), &*self.opts.io_engine()) {
            let e = Error::indeterminate(format!("manifest commit failed: {e}"));
            self.fail(&e);
            return Err(e);
        }
        *man = next;
        Ok(())
    }

    /// Opportunistic deletion of obsolete files (background loop).
    pub(super) fn indexes(&self, verify: bool) -> Result<Vec<SegmentIndexes>> {
        self.check_healthy()?;
        let segs = Arc::clone(&self.state.lock().segments);
        let mut out = Vec::with_capacity(segs.len());
        for h in segs.iter() {
            let bloom = h.bloom();
            let ngram = h.ngram();
            let check = if verify {
                let rows = h.reader.read_rows()?;
                let refs: Vec<RowRef<'_>> = rows
                    .iter()
                    .map(|r| RowRef {
                        seq: r.seq,
                        idx: r.idx,
                        event: &r.event,
                    })
                    .collect();
                let mut false_negatives =
                    bloom.map(|b| b.false_negatives(&refs)).unwrap_or_default();
                false_negatives.extend(ngram.map(|n| n.false_negatives(&refs)).unwrap_or_default());
                let expected = self.index_extensions(&refs);
                let footer = h.reader.footer();
                let current = [
                    stratadb_segment::bloom::EXT_BLOOM,
                    stratadb_segment::ngram::EXT_NGRAM,
                ]
                .iter()
                .all(|tag| {
                    footer.extension(*tag).map(|x| &x.data)
                        == expected.iter().find(|x| x.tag == *tag).map(|x| &x.data)
                });
                Some(IndexCheck {
                    false_negatives,
                    current,
                })
            } else {
                None
            };
            out.push(SegmentIndexes {
                segment_id: h.entry.id,
                tenant: h.entry.tenant.clone(),
                rows: h.entry.row_count,
                bloom_fields: bloom
                    .map(|b| b.fields().map(|f| f.describe()).collect())
                    .unwrap_or_default(),
                bloom_bytes: bloom.map_or(0, |b| b.size_bytes()),
                ngram_fields: ngram
                    .map(|n| n.fields().map(|f| f.describe()).collect())
                    .unwrap_or_default(),
                ngram_bytes: ngram.map_or(0, |n| n.size_bytes()),
                check,
            });
        }
        Ok(out)
    }

    pub(super) fn rebuild_indexes(&self, dry_run: bool) -> Result<IndexRebuildReport> {
        let checked = self.indexes(true)?;
        let mut report = IndexRebuildReport {
            segments_checked: checked.len(),
            dry_run,
            ..Default::default()
        };
        let mut targets = BTreeSet::new();
        for s in &checked {
            if let Some(c) = &s.check {
                if !c.false_negatives.is_empty() {
                    report.segments_unsound += 1;
                    tracing::error!(segment = s.segment_id, filters = ?c.false_negatives,
                        "index filters miss stored values; rebuilding");
                }
                if !c.current {
                    report.segments_stale += 1;
                }
                if !c.current || !c.false_negatives.is_empty() {
                    targets.insert(s.segment_id);
                }
            }
        }
        if dry_run || targets.is_empty() {
            return Ok(report);
        }
        let mut man = self.manifest.lock();
        for id in targets {
            // The segment may have been compacted away since the check.
            let seg = {
                let segs = Arc::clone(&self.state.lock().segments);
                segs.iter().find(|h| h.entry.id == id).cloned()
            };
            let Some(seg) = seg else { continue };
            self.check_disk(seg.entry.file_bytes)?;
            let tenant = seg.entry.tenant.clone();
            self.rewrite(&mut man, &tenant, &[seg], &|_| true)?;
            report.segments_rebuilt += 1;
        }
        self.reclaim_obsolete(&mut man)?;
        Ok(report)
    }

    pub(super) fn reclaim_obsolete_now(&self) -> Result<usize> {
        if self.state.lock().obsolete.is_empty() {
            return Ok(0);
        }
        let mut man = self.manifest.lock();
        self.reclaim_obsolete(&mut man)
    }
}
