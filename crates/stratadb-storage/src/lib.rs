//! StrataDB single-node storage engine (spec §7–§10, epics C and D).
//!
//! See [`engine`] for the write path, flush protocol, and failure policy.

pub mod engine;
pub mod idempotency;
pub mod manifest;
pub mod memtable;

pub use engine::{
    restore_backup, verify_backup, BackupManifest, BackupReport, CommitLevel, CompactionOptions,
    CompactionReport, Engine, EngineOptions, EngineStats, FlushReport, IndexCheck,
    IndexRebuildReport, OpenReport, RestoreReport, RetentionReport, SegmentHandle, SegmentIndexes,
    Snapshot, TenantUsage, UpgradeReport, WriteReceipt,
};
pub use idempotency::IdempotencyKey;
pub use manifest::{Manifest, RetentionPolicy, SegmentEntry};
pub use memtable::MemRow;
pub use stratadb_wal::{Durability, RecoveryPolicy, WalMetrics, WalOptions};
