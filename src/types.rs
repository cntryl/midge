//! Shared runtime and public DTO types owned below the engine facade.
//!
//! Runtime, storage, and metadata code can depend on these types without
//! depending on the public `Engine` facade.

use crate::config::EngineHealth;

/// Column family identifier.
pub type ColumnFamilyId = u32;

/// Conflict handling policy for read-write transaction commits.
///
/// This type is shared by the public API and the runtime so the engine does not
/// maintain a parallel internal conflict enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Preserve current behavior: overlapping writers resolve by commit order.
    LastWriteWins,
    /// Abort when a write-set key or covered range changed after the start snapshot.
    AbortOnWriteConflict,
}

/// Durability level for read-path frontier checks.
///
/// This enum is for internal runtime durability tracking. Write-time
/// durability decisions use `WriteOptions::DurabilityPolicy` instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDurability {
    /// Strict - fsync on every write.
    Strict,
    /// Steady - fsync every N ms.
    Steady,
    /// `CloudPersisted` - wait for cloud backup.
    CloudPersisted,
}

/// Snapshot of read amplification metrics.
///
/// Provides visibility into read performance characteristics:
/// - How many SSTs are being touched per read
/// - L0 overlap patterns
/// - Budget violation rates
#[derive(Debug, Clone)]
pub struct ReadAmpMetricsSnapshot {
    /// Total read operations performed.
    pub reads_total: u64,
    /// Total SSTs touched across all reads.
    pub ssts_touched_total: u64,
    /// Total L0 SSTs touched.
    pub l0_ssts_touched_total: u64,
    /// Total blocks read across all operations.
    pub blocks_read_total: u64,
    /// Average SSTs touched per read.
    pub avg_ssts_per_read: f64,
    /// Average L0 SSTs touched per read.
    pub avg_l0_ssts_per_read: f64,
    /// Average blocks read per operation.
    pub avg_blocks_per_read: f64,
    /// L0 overlap rate.
    pub l0_overlap_rate: f64,
    /// SST budget violation rate.
    pub sst_budget_violation_rate: f64,
    /// Block budget violation rate.
    pub block_budget_violation_rate: f64,
}

/// Snapshot of startup recovery metrics.
#[derive(Debug, Clone)]
pub struct RecoveryMetricsSnapshot {
    pub wal_recovery_records_replayed: u64,
    pub wal_recovery_bytes_replayed: u64,
    pub intent_log_replay_runs: u64,
    pub intent_log_entries_replayed: u64,
}

/// Stable operator-facing snapshot of runtime metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RuntimeMetricsSnapshot {
    pub health: EngineHealth,
    pub current_sequence: u64,
    pub manifest_last_persisted_sequence: u64,
    pub manifest_next_wal_seq: u64,
    pub active_memtables: usize,
    pub immutable_memtables: usize,
    pub total_memtable_bytes: usize,
    pub memtable_size_limit: usize,
    pub memtable_flush_threshold: usize,
    pub max_memtable_wal_segment_gap: u64,
    pub write_stalled: bool,
    pub wal_current_segment_id: u64,
    pub wal_pending_writes: usize,
    pub wal_last_synced_seq: u64,
    pub wal_local_durable_seq: u64,
    pub wal_cloud_durable_seq: u64,
    pub pending_compactions: usize,
    pub compacting_ssts: usize,
    pub active_compactions: usize,
    pub pending_cloud_uploads: usize,
    pub active_snapshots: usize,
    pub pinned_ssts: usize,
    pub oldest_snapshot_age_seconds: u64,
    pub sst_count: usize,
    pub sst_bytes: u64,
    pub salvage_mode_opens: u64,
    pub no_space_events: u64,
    pub compactions_run: u64,
    pub compaction_bytes_rewritten: u64,
    pub compaction_failures: u64,
    pub obsolete_file_backlog: usize,
    pub write_stalls_total: u64,
    pub write_stalls_memory_total: u64,
    pub write_stalls_compaction_total: u64,
    pub write_stalls_cloud_total: u64,
    pub write_stalls_no_space_total: u64,
    pub write_conflicts_total: u64,
    pub write_conflicts_point_total: u64,
    pub write_conflicts_range_total: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub wal_append_count: u64,
    pub wal_flush_count: u64,
    pub wal_fsync_count: u64,
    pub wal_append_ns_total: u64,
    pub wal_fsync_ns_total: u64,
    pub cloud_async_wal_segments_sealed: u64,
    pub cloud_async_wal_bytes_sealed: u64,
    pub cloud_async_wal_seal_latency_us: u64,
    pub cloud_async_wal_uploads_started: u64,
    pub cloud_async_wal_uploads_completed: u64,
    pub cloud_async_wal_uploads_failed: u64,
    pub cloud_async_wal_upload_latency_us: u64,
    pub cloud_async_wal_ack_latency_us: u64,
    pub hybrid_max_local_bytes: u64,
    pub hybrid_total_committed_bytes: u64,
    pub hybrid_free_bytes: u64,
    pub hybrid_usage_percent: u32,
    pub hybrid_pending_evictions: usize,
    pub wal_recovery_records_replayed: u64,
    pub wal_recovery_bytes_replayed: u64,
    pub intent_log_replay_runs: u64,
    pub intent_log_entries_replayed: u64,
}

/// Active snapshot pin observed by the runtime.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SnapshotPinSnapshot {
    pub snapshot_id: u64,
    pub sequence: u64,
    pub age_seconds: u64,
    pub ref_count: usize,
}

/// Single SST entry in a storage layout report.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StorageFileLayout {
    pub name: String,
    pub level: u32,
    pub cf_id: ColumnFamilyId,
    pub size_bytes: u64,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
    pub smallest_seq: Option<u64>,
    pub largest_seq: Option<u64>,
}

/// Per-level storage layout summary.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StorageLayoutLevel {
    pub level: u32,
    pub file_count: usize,
    pub total_bytes: u64,
    pub files: Vec<StorageFileLayout>,
}

/// Stable operator-facing snapshot of on-disk layout and pinned state.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StorageLayoutSnapshot {
    pub health: EngineHealth,
    pub manifest_last_persisted_sequence: u64,
    pub manifest_next_wal_seq: u64,
    pub levels: Vec<StorageLayoutLevel>,
    pub active_snapshots: Vec<SnapshotPinSnapshot>,
    pub pending_compactions: usize,
    pub compacting_ssts: Vec<String>,
    pub obsolete_files: Vec<String>,
}

/// Non-mutating verification report for a storage directory.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StorageVerificationReport {
    /// Highest durable manifest edit represented by the verified layout.
    pub manifest_epoch: u64,
    pub manifest_files_verified: usize,
    pub sst_files_verified: usize,
    /// Physical SST bytes plus decoded WAL payload bytes verified.
    pub bytes_verified: u64,
    /// Checksummed SST data blocks read and decoded.
    pub data_blocks_verified: u64,
    /// Highest sequence observed in the verified WAL, if it contained records.
    pub wal_boundary: Option<u64>,
    pub wal_recovery_records_replayed: u64,
    pub wal_recovery_bytes_replayed: u64,
    pub intent_entries_loaded: usize,
    /// Whether the pass covered authoritative storage rather than a cloud cache.
    pub authoritative: bool,
    pub health: EngineHealth,
}
