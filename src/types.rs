//! Shared runtime and public DTO types owned below the engine facade.
//!
//! Runtime, storage, and metadata code can depend on these types without
//! depending on the public `Engine` facade.

use crate::common::MidgeError;
use crate::config::EngineHealth;
use bytes::Bytes;
use std::fmt;

/// Column family identifier.
pub type ColumnFamilyId = u32;

/// Logical operation associated with a versioned key.
///
/// The discriminants are part of the persisted SST and WAL-compatible state
/// representation, so this shared type owns them below either codec.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    Put = 0,
    Insert = 1,
    Delete = 2,
    // Reserved on-disk discriminant. Writers and readers reject it (#404), so
    // only tests construct it.
    #[cfg_attr(not(feature = "internal-testing"), allow(dead_code))]
    Merge = 3,
}

impl EntryType {
    /// Whether this entry writes a value (a put or an insert), as opposed to
    /// deleting one or carrying an unsupported merge operand.
    #[must_use]
    pub const fn is_value_write(self) -> bool {
        matches!(self, Self::Put | Self::Insert)
    }
}

impl fmt::Display for EntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

impl TryFrom<u8> for EntryType {
    type Error = MidgeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Put),
            1 => Ok(Self::Insert),
            2 => Ok(Self::Delete),
            // Writers never emit Merge (lsm-spec sst.md 3.2) and no read path
            // knows how to resolve an operand, so surfacing it as a value would
            // silently return a merge operand as a complete Put. Fail closed.
            3 => Err(MidgeError::CompatibilityError(
                "SST entry type 3 (Merge) is not supported".to_string(),
            )),
            _ => Err(MidgeError::Corruption(format!(
                "Invalid entry_type: {value}"
            ))),
        }
    }
}

/// Range tombstone for covering key ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTombstone {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub seq: u64,
}

impl RangeTombstone {
    #[must_use]
    pub fn new(start: Vec<u8>, end: Vec<u8>, seq: u64) -> Self {
        Self { start, end, seq }
    }

    /// Whether this tombstone is visible at `snapshot_seq` (`u64::MAX` reads
    /// the latest state).
    #[must_use]
    pub fn visible_at(&self, snapshot_seq: u64) -> bool {
        snapshot_seq == u64::MAX || self.seq <= snapshot_seq
    }

    /// Check whether a key lies in the half-open tombstone range.
    #[must_use]
    pub fn covers(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && key < self.end.as_slice()
    }
}

/// Snapshot-visible state for one versioned key.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyState {
    Absent,
    Tombstone(u64),
    Value(Bytes, u64, Option<u64>, EntryType),
}

/// Persisted content at one sequence, independent of its source. Put and
/// Insert have the same logical value identity; TTL metadata is part of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VersionContent<'a> {
    pub(crate) is_tombstone: bool,
    pub(crate) value: Option<&'a [u8]>,
    pub(crate) expiration: Option<u64>,
}

impl<'a> VersionContent<'a> {
    pub(crate) fn from_state(state: &'a KeyState) -> Option<Self> {
        match state {
            KeyState::Absent => None,
            KeyState::Tombstone(_) => Some(Self {
                is_tombstone: true,
                value: None,
                expiration: None,
            }),
            KeyState::Value(value, _, expiration, _) => Some(Self {
                is_tombstone: false,
                value: Some(value.as_ref()),
                expiration: *expiration,
            }),
        }
    }
}

/// Identical copies are valid; differing content at one sequence is corrupt.
pub(crate) fn resolve_same_sequence(
    existing: VersionContent<'_>,
    candidate: VersionContent<'_>,
) -> Result<(), ()> {
    if existing == candidate {
        Ok(())
    } else {
        Err(())
    }
}

impl fmt::Display for KeyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => write!(f, "Absent"),
            Self::Tombstone(seq) => write!(f, "Tombstone(seq={seq})"),
            Self::Value(_, seq, expiration, operation) => {
                write!(f, "Value(seq={seq}, exp={expiration:?}, op={operation})")
            }
        }
    }
}

/// Borrowed manifest proof fields for one persisted SST.
///
/// Metadata and runtime messages both construct this view while the SST
/// identity checker consumes it, so it belongs below either owner.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExpectedSst<'a> {
    pub(crate) name: &'a str,
    pub(crate) size_bytes: u64,
    pub(crate) content_crc32c: Option<u32>,
    pub(crate) smallest_key: Option<&'a [u8]>,
    pub(crate) largest_key: Option<&'a [u8]>,
    pub(crate) smallest_seq: Option<u64>,
    pub(crate) largest_seq: Option<u64>,
}

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
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDurability {
    /// Strict - fsync on every write.
    Strict,
}

/// Snapshot of read amplification metrics.
///
/// Provides visibility into read performance characteristics:
/// - How many SSTs are being touched per read
/// - L0 overlap patterns
/// - Budget violation rates
#[derive(Debug, Clone)]
pub struct ReadAmpMetricsSnapshot {
    /// Total point-read operations that completed without an error.
    ///
    /// Failed reads are excluded because their physical-work sample may be
    /// incomplete at the failure boundary.
    pub reads_total: u64,
    /// Total SSTs touched across successfully completed reads.
    pub ssts_touched_total: u64,
    /// Total L0 SSTs touched across successfully completed reads.
    pub l0_ssts_touched_total: u64,
    /// Total blocks read across successfully completed reads.
    pub blocks_read_total: u64,
    /// Average SSTs touched per successfully completed read.
    pub avg_ssts_per_read: f64,
    /// Average L0 SSTs touched per successfully completed read.
    pub avg_l0_ssts_per_read: f64,
    /// Average blocks read per successfully completed read.
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
    /// Maximum latency of one physical WAL fsync.
    pub wal_fsync_ns_max: u64,
    /// Durability waiters completed through keyed fan-out events.
    pub durability_waiters_fanned_out_total: u64,
    /// Runtime requests whose caller stopped waiting before a response arrived.
    ///
    /// Pairs with `late_runtime_responses_total` for aggregate diagnosis of
    /// ambiguous `MidgeError::Timeout` behavior. The process-wide totals cannot
    /// identify the outcome of an individual request.
    pub abandoned_runtime_requests_total: u64,
    /// Runtime responses that arrived with no caller waiting for them.
    ///
    /// Includes successful and error responses for every request kind, so this
    /// process-wide total cannot identify whether one timed-out mutation took
    /// effect.
    pub late_runtime_responses_total: u64,
    /// SST data blocks skipped after a definite bloom-filter rejection.
    pub sst_bloom_rejects_total: u64,
    /// Persisted SST block bloom filters consulted by point reads.
    pub sst_bloom_checks_total: u64,
    /// Checksummed SST data blocks read through the point/range read path.
    pub sst_data_blocks_read_total: u64,
    /// Immutable flushes waiting for the single worker. Gauge.
    pub flush_queue_depth: usize,
    /// Flush worker tasks currently executing. Gauge (zero or one).
    pub flush_inflight: usize,
    /// Immutable memtable generations enqueued since runtime startup. Counter.
    pub flush_enqueued_total: u64,
    pub flush_build_count: u64,
    pub flush_build_ns_total: u64,
    pub flush_build_ns_max: u64,
    pub flush_publish_count: u64,
    pub flush_publish_ns_total: u64,
    pub flush_publish_ns_max: u64,
    pub flush_failures_total: u64,
    pub flush_retries_total: u64,
    pub write_stall_ns_total: u64,
    pub write_stall_ns_max: u64,
    /// Elapsed nanoseconds in the currently active stall, or zero. Gauge.
    pub write_stall_active_ns: u64,
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
    /// Local disk charges and observed admission blocks; absent outside hybrid storage.
    pub local_storage: Option<HybridStorageBudgetSnapshot>,
    /// Submitted range requests from this engine's runtime SST readers.
    /// Excludes HEAD requests, startup recovery, and WAL maintenance.
    pub remote_range_requests_total: u64,
    /// Provider payload bytes returned to runtime SST readers, including short
    /// responses subsequently rejected during validation.
    pub remote_range_bytes_total: u64,
    /// Failed runtime SST range requests, including caller-observed timeouts.
    pub remote_range_failures_total: u64,
    /// Aggregate caller-observed runtime SST range latency in nanoseconds.
    pub remote_range_latency_ns_total: u64,
    /// Maximum caller-observed runtime SST range latency in nanoseconds.
    pub remote_range_latency_ns_max: u64,
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

// ---------------------------------------------------------------------------
// Local storage budget DTOs (filled by storage::hybrid)
// ---------------------------------------------------------------------------

/// The local operation whose admission most recently failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
#[repr(usize)]
pub enum StorageAdmissionKind {
    Wal,
    TransactionSpill,
    Flush,
    Compaction,
    FlushHeadroom,
    StartupResidue,
}

/// Why a local operation could not reserve its working space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageAdmissionReason {
    LocalCapacity,
    CloudUpload,
    Compaction,
}

/// Oldest rejected admission class that has not subsequently succeeded.
///
/// This records observed admission failures, not a queue of caller requests.
/// A caller can retry or abandon its operation; successful admission of the
/// same class clears its observation without hiding failures of other classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct StorageAdmissionBlock {
    pub operation: StorageAdmissionKind,
    pub reason: StorageAdmissionReason,
    pub requested_bytes: u64,
    pub free_bytes_at_rejection: u64,
    pub age_millis: u64,
    pub attempts: u64,
}

/// Non-overlapping charges in the local disk reservation ledger.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct LocalStorageUsage {
    pub wal_bytes: u64,
    pub transaction_spill_bytes: u64,
    pub resident_sst_bytes: u64,
    pub startup_residue_bytes: u64,
    pub flush_staging_reserved_bytes: u64,
    pub flush_headroom_reserved_bytes: u64,
    pub compaction_staging_reserved_bytes: u64,
    pub wal_headroom_reserved_bytes: u64,
    /// Outstanding flush/compaction reservations, excluding reusable headroom.
    /// Includes retained allowances whose scratch cleanup remains unverified.
    pub reservations: usize,
}

/// Local working-storage budget of a hybrid (cloud-backed) engine.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct HybridStorageBudgetSnapshot {
    pub max_local_bytes: u64,
    pub total_committed_bytes: u64,
    pub free_bytes: u64,
    pub usage_percent: u32,
    pub pending_evictions: usize,
    pub usage: LocalStorageUsage,
    pub blocked_admission: Option<StorageAdmissionBlock>,
    pub admission_rejections_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_entry_type_wire_tags_given_unsupported_merge() {
        // Arrange
        let unsupported_merge_tag = 3;

        // Act
        let decoded_put = EntryType::try_from(0);
        let decoded_insert = EntryType::try_from(1);
        let decoded_delete = EntryType::try_from(2);
        let rejected_merge = EntryType::try_from(unsupported_merge_tag);

        // Assert
        assert_eq!(EntryType::Put as u8, 0);
        assert_eq!(EntryType::Insert as u8, 1);
        assert_eq!(EntryType::Delete as u8, 2);
        assert_eq!(EntryType::Merge as u8, 3);
        assert!(matches!(decoded_put, Ok(EntryType::Put)));
        assert!(matches!(decoded_insert, Ok(EntryType::Insert)));
        assert!(matches!(decoded_delete, Ok(EntryType::Delete)));
        assert!(matches!(
            rejected_merge,
            Err(MidgeError::CompatibilityError(message))
                if message == "SST entry type 3 (Merge) is not supported"
        ));
    }

    #[test]
    fn should_preserve_half_open_ranges_when_rendering_snapshot_state() {
        // Arrange
        let tombstone = RangeTombstone::new(b"alpha".to_vec(), b"omega".to_vec(), 7);
        let state = KeyState::Value(Bytes::from_static(b"value"), 9, Some(11), EntryType::Insert);

        // Act
        let rendered = state.to_string();

        // Assert
        assert!(tombstone.covers(b"alpha"));
        assert!(tombstone.covers(b"middle"));
        assert!(!tombstone.covers(b"omega"));
        assert_eq!(rendered, "Value(seq=9, exp=Some(11), op=1)");
    }
}
