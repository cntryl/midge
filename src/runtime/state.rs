//! Runtime state - centralized mutable state owned by the runtime
//!
//! All engine state that can change at runtime lives here.
//! Actors read from and propose updates to this state.

use crate::common::{MidgeError, MidgeResult};
use crate::diagnostics::RuntimeDiagnostics;
use crate::metadata::Manifest;
use crate::runtime::snapshot_pins::SnapshotPinRegistry;
use crate::runtime::{IntentLogEntry, PublicationPhase};
use crate::sst::{Memtable, ReadAmpMetrics, SkipListMemtable};

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::io::traits::{Fs, FsError, FsPath};

mod flush;
mod manifest;
mod recovery;
mod snapshots;

/// Maximum size of idempotency cache to prevent unbounded growth under cloud stall
#[cfg(test)]
const MAX_IDEMPOTENCY_CACHE_SIZE: usize = 100_000;
const MAX_RECENT_DELETE_RANGES: usize = 4096;
#[derive(Debug, Clone)]
pub struct RecentDeleteRange {
    pub cf_id: crate::types::ColumnFamilyId,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub sequence: u64,
}

const INITIAL_FLUSH_RETRY_BACKOFF: Duration = Duration::from_millis(10);
const MAX_FLUSH_RETRY_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImmutableFlushPhase {
    Queued,
    Building,
    Built,
    Publishing,
    RetryPending,
}

#[derive(Clone)]
pub(crate) struct ImmutableFlush {
    pub flush_id: u64,
    pub writer_epoch: u64,
    pub memtable: Arc<SkipListMemtable>,
    pub sst_name: Option<String>,
    pub sst_seq: Option<u64>,
    pub sequence: u64,
    pub phase: ImmutableFlushPhase,
    pub built: Option<crate::runtime::actors::flush::FlushBuildOutput>,
    pub failures: u32,
    pub retry_at: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct FlushRuntimeMetrics {
    pub enqueued_total: u64,
    pub build_count: u64,
    pub build_ns_total: u64,
    pub build_ns_max: u64,
    pub publish_count: u64,
    pub publish_ns_total: u64,
    pub publish_ns_max: u64,
    pub failures_total: u64,
    pub retries_total: u64,
    pub write_stall_ns_total: u64,
    pub write_stall_ns_max: u64,
    pub write_stall_started_at: Option<Instant>,
}

/// Column family state
pub struct ColumnFamilyState {
    pub memtable: Arc<SkipListMemtable>,
    /// Immutable memtables waiting to be flushed
    pub immutable_memtables: Vec<Arc<SkipListMemtable>>,
    /// Publication state for each immutable created by the flush actor.
    ///
    /// `immutable_memtables` remains the read-path view; this queue owns the
    /// stable flush identity and retry lifecycle for those same `Arc`s.
    pub(crate) immutable_flushes: Vec<ImmutableFlush>,
    /// WAL segment ID when the active memtable first became non-empty.
    pub active_memtable_started_in_segment: u64,
}

impl ColumnFamilyState {
    pub fn new(_id: u32, _name: String) -> Self {
        Self {
            memtable: Arc::new(SkipListMemtable::new()),
            immutable_memtables: Vec::new(),
            immutable_flushes: Vec::new(),
            active_memtable_started_in_segment: 1,
        }
    }
}

/// WAL state
pub struct WalState {
    /// Current WAL segment ID
    pub current_segment_id: u64,
    /// Last synced sequence number (local durability)
    pub last_synced_seq: u64,
    /// Pending writes waiting for sync
    pub pending_writes: usize,
    /// Local durability frontier - highest sequence number fsynced locally
    pub local_durable_seq: u64,
    /// Cloud durability frontier - highest sequence number confirmed by cloud
    pub cloud_durable_seq: u64,
}

impl Default for WalState {
    fn default() -> Self {
        Self {
            current_segment_id: 1,
            last_synced_seq: 0,
            pending_writes: 0,
            local_durable_seq: 0,
            cloud_durable_seq: 0,
        }
    }
}

/// Compaction state
#[derive(Default)]
pub struct CompactionState {
    /// SSTs currently being compacted (locked from other compactions)
    pub compacting_ssts: Vec<String>,
    /// Pending compaction tasks
    pub pending_tasks: usize,
}

/// Cloud sync state
#[derive(Default)]
pub struct CloudState {
    /// SSTs pending upload
    pub pending_uploads: Vec<String>,
    /// Last checkpoint sequence uploaded to cloud
    #[cfg(test)]
    pub last_cloud_checkpoint_seq: u64,
}

/// Snapshot pinning state
///
/// Tracks active snapshots to prevent SST garbage collection
/// while snapshots are reading from those SSTs.
#[derive(Default)]
pub struct SnapshotState {
    /// Maximum time to hold a snapshot (1 hour by default)
    pub max_snapshot_lifetime: std::time::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlushReason {
    PendingImmutable,
    SizeThreshold,
    CloudSegmentGap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushCandidate {
    pub cf_id: crate::types::ColumnFamilyId,
    pub reason: FlushReason,
}

pub struct RuntimeMode {
    #[cfg(test)]
    pub read_only: bool,
    pub memory_mode: bool,
}

pub struct RecoveryStatus {
    pub policy: crate::config::RecoveryPolicy,
    pub opened_in_salvage_mode: bool,
    pub persistence_anomaly_detected: bool,
}

pub struct CompactionConfig {
    pub enabled: bool,
}

pub struct WritePressureState {
    pub stalled: bool,
}

struct RecoveryLoadState {
    opened_in_salvage_mode: bool,
    manifest: Manifest,
    intent_log: Vec<IntentLogEntry>,
}

struct WalRecoveryState {
    column_families: HashMap<u32, ColumnFamilyState>,
    recovered_sequence: u64,
    next_segment_id: u64,
    records_replayed: u64,
    bytes_replayed: u64,
    opened_in_salvage_mode: bool,
}

/// Centralized runtime state
///
/// This is the single source of truth for all mutable engine state.
/// The runtime owns this and actors propose updates via messages.
///
/// Important:
/// - This type does NOT handle per-request routing.
/// - Response routing is handled exclusively by `ResponseRouter`
///   (shared between `RuntimeHandle` and `EventLoop`).
pub struct RuntimeState {
    // === Paths ===
    pub db_path: PathBuf,
    pub wal_dir: PathBuf,
    pub sst_dir: PathBuf,

    // === Sequence Numbers ===
    /// Global monotonic sequence number
    pub sequence: u64,
    /// Next transaction ID
    pub next_txn_id: u64,
    /// Minimum sequence of a pending transaction apply (for read atomicity)
    /// When set, reads at sequences >= this value must wait for durability
    pub pending_txn_min_seq: Option<u64>,
    /// Start time of pending transaction (for Phase 3 duration metrics)
    pub pending_txn_start_time: Option<std::time::Instant>,
    /// Idempotency cache: `request_id` → (`first_sequence`, count, `confirmed_at`)
    /// Prevents duplicate sequence allocation on retry of same `request_id`.
    /// Entries cleared when durability frontier advances past `confirmed_at`.
    pub sequence_idempotency_cache: HashMap<u64, (u64, usize, u64)>,

    // === Column Families ===
    pub column_families: HashMap<u32, ColumnFamilyState>,

    // === Metadata ===
    pub manifest: Manifest,

    // Filesystem abstraction for all IO (never call std::fs directly)
    pub fs: std::sync::Arc<dyn Fs>,

    // === Subsystem State ===
    pub wal: WalState,
    pub compaction: CompactionState,
    pub cloud: CloudState,
    pub snapshots: SnapshotState,
    pub snapshot_pins: Arc<SnapshotPinRegistry>,
    /// Per-runtime read-path counters. This follows the same ownership
    /// boundary as the snapshots, readers, and block cache it measures.
    pub diagnostics: Arc<RuntimeDiagnostics>,
    /// Recently committed range deletes for strict write-conflict checks.
    pub recent_delete_ranges: Vec<RecentDeleteRange>,

    // === Configuration ===
    pub memtable_size_limit: usize,
    pub mode: RuntimeMode,
    pub recovery: RecoveryStatus,
    pub compaction_config: CompactionConfig,

    // === Intent Log & Determinism ===
    /// Deterministic intent log for recovery and replay
    pub intent_log: Vec<IntentLogEntry>,
    /// Maximum size of any memtable before write stall
    pub memtable_flush_threshold: usize,
    /// Cloud-only runtime heuristic: flush active memtables after enough WAL segment churn.
    pub cloud_eventual_flush_segment_gap: u64,
    pub write_pressure: WritePressureState,
    /// Total size of all memtables (in-memory)
    pub total_memtable_bytes: usize,
    /// Maximum number of immutable memtables per CF before write stall
    pub max_immutable_memtables: usize,
    /// Next process-local flush identity. Flush identities are stable for the
    /// lifetime of an immutable and are never inferred from SST sequence state.
    pub(crate) next_flush_id: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) flush_metrics: FlushRuntimeMetrics,

    // === Observability ===
    /// Read amplification metrics across all reads
    pub read_amp_metrics: ReadAmpMetrics,

    // Startup recovery metrics
    pub wal_recovery_records_replayed: u64,
    pub wal_recovery_bytes_replayed: u64,
    pub intent_log_replay_runs: u64,
    pub intent_log_entries_replayed: u64,

    // === Ingest control & coordination ===
    /// Ingest epoch counter used to cooperatively cancel long-running jobs.
    /// Bumping this value invalidates currently running compactions which should
    /// check the epoch and abort if it changes.
    pub ingest_epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,

    /// Number of active compaction jobs. Used so `BeginIngest` can wait until
    /// the running compactions drain.
    pub active_compactions: std::sync::Arc<std::sync::atomic::AtomicUsize>,

    /// Whether an ingest barrier is currently active. This is set at `BeginIngest`
    /// and cleared at `EndIngest` so tools and tests can detect when ingest mode
    /// is enforced.
    pub ingest_active: std::sync::Arc<std::sync::atomic::AtomicBool>,

    /// Pending CompactAll/BeginIngest requests waiting for compactions to finish.
    /// Maps `request_id` -> `completion_condition` (e.g., "`CompactAll`", "`BeginIngest`").
    pub pending_compaction_waits: parking_lot::Mutex<std::collections::HashMap<u64, String>>,
}

impl RuntimeState {
    pub fn health(&self) -> crate::config::EngineHealth {
        let residue = self.storage_residue_assessment();
        crate::runtime::storage_residue::classify_engine_health(
            crate::runtime::storage_residue::HealthInputs {
                opened_in_salvage_mode: self.opened_in_salvage_mode(),
                write_stalled: self.has_any_hard_write_stall(),
                persistence_anomaly_detected: self.persistence_anomaly_detected(),
                pending_intents: self.intent_log.len(),
                orphan_ssts: residue.orphan_ssts.len(),
            },
        )
    }

    fn snapshot_retention_metrics(&self, now: Instant) -> (usize, u64) {
        let pinned_ssts = self.get_pinned_sst_names().len();
        let oldest_snapshot_age_seconds = self.snapshot_pins.oldest_age_seconds(now).unwrap_or(0);
        (pinned_ssts, oldest_snapshot_age_seconds)
    }

    fn memtable_counts(&self) -> (usize, usize) {
        let active_memtables = self.column_families.len();
        let immutable_memtables = self
            .column_families
            .values()
            .map(|cf| cf.immutable_memtables.len())
            .sum();
        (active_memtables, immutable_memtables)
    }

    fn sst_totals(&self) -> (usize, u64) {
        (
            self.manifest.files.len(),
            self.manifest.files.iter().map(|file| file.size_bytes).sum(),
        )
    }

    #[allow(clippy::too_many_lines)]
    pub fn runtime_metrics_snapshot(&self) -> crate::types::RuntimeMetricsSnapshot {
        let now = Instant::now();
        let (pinned_ssts, oldest_snapshot_age_seconds) = self.snapshot_retention_metrics(now);
        let (active_memtables, immutable_memtables) = self.memtable_counts();
        let (sst_count, sst_bytes) = self.sst_totals();
        let residue = self.storage_residue_assessment();
        let telemetry = crate::telemetry::Telemetry::global().map(|t| t.metrics().snapshot());
        let flush_queue_depth = self
            .column_families
            .values()
            .flat_map(|cf| &cf.immutable_flushes)
            .filter(|flush| {
                matches!(
                    flush.phase,
                    ImmutableFlushPhase::Queued | ImmutableFlushPhase::RetryPending
                )
            })
            .count();
        let flush_inflight = usize::from(self.column_families.values().any(|cf| {
            cf.immutable_flushes.iter().any(|flush| {
                matches!(
                    flush.phase,
                    ImmutableFlushPhase::Building
                        | ImmutableFlushPhase::Built
                        | ImmutableFlushPhase::Publishing
                )
            })
        }));
        let write_stall_active_ns = self
            .flush_metrics
            .write_stall_started_at
            .map_or(0, |started| {
                u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
            });

        crate::types::RuntimeMetricsSnapshot {
            health: self.health(),
            current_sequence: self.sequence,
            manifest_last_persisted_sequence: self.manifest.last_persisted_sequence,
            manifest_next_wal_seq: self.manifest.next_wal_seq,
            active_memtables,
            immutable_memtables,
            total_memtable_bytes: self.total_memtable_bytes,
            memtable_size_limit: self.memtable_size_limit,
            memtable_flush_threshold: self.memtable_flush_threshold,
            max_memtable_wal_segment_gap: self.max_memtable_wal_segment_gap(),
            write_stalled: self.has_any_hard_write_stall(),
            wal_current_segment_id: self.wal.current_segment_id,
            wal_pending_writes: self.wal.pending_writes,
            wal_last_synced_seq: self.wal.last_synced_seq,
            wal_local_durable_seq: self.wal.local_durable_seq,
            wal_cloud_durable_seq: self.wal.cloud_durable_seq,
            pending_compactions: self.compaction.pending_tasks,
            compacting_ssts: self.compaction.compacting_ssts.len(),
            active_compactions: self
                .active_compactions
                .load(std::sync::atomic::Ordering::SeqCst),
            pending_cloud_uploads: self.cloud.pending_uploads.len(),
            active_snapshots: self.snapshot_pins.active_count(),
            pinned_ssts,
            oldest_snapshot_age_seconds,
            sst_count,
            sst_bytes,
            salvage_mode_opens: u64::from(self.opened_in_salvage_mode()),
            no_space_events: telemetry.as_ref().map_or(0, |m| m.no_space_events),
            compactions_run: telemetry.as_ref().map_or(0, |m| m.compactions_run),
            compaction_bytes_rewritten: telemetry
                .as_ref()
                .map_or(0, |m| m.compaction_bytes_rewritten),
            compaction_failures: telemetry.as_ref().map_or(0, |m| m.compaction_failures),
            obsolete_file_backlog: residue.orphan_ssts.len(),
            write_stalls_total: telemetry.as_ref().map_or(0, |m| m.write_stalls),
            write_stalls_memory_total: telemetry.as_ref().map_or(0, |m| m.write_stalls_memory),
            write_stalls_compaction_total: telemetry
                .as_ref()
                .map_or(0, |m| m.write_stalls_compaction),
            write_stalls_cloud_total: telemetry.as_ref().map_or(0, |m| m.write_stalls_cloud),
            write_stalls_no_space_total: telemetry.as_ref().map_or(0, |m| m.write_stalls_no_space),
            write_conflicts_total: telemetry.as_ref().map_or(0, |m| m.write_conflicts),
            write_conflicts_point_total: telemetry.as_ref().map_or(0, |m| m.write_conflicts_point),
            write_conflicts_range_total: telemetry.as_ref().map_or(0, |m| m.write_conflicts_range),
            cache_hits: telemetry.as_ref().map_or(0, |m| m.cache_hits),
            cache_misses: telemetry.as_ref().map_or(0, |m| m.cache_misses),
            wal_append_count: telemetry.as_ref().map_or(0, |m| m.wal_append_count),
            wal_flush_count: telemetry.as_ref().map_or(0, |m| m.wal_flush_count),
            wal_fsync_count: telemetry.as_ref().map_or(0, |m| m.wal_fsync_count),
            wal_append_ns_total: telemetry.as_ref().map_or(0, |m| m.wal_append_ns_total),
            wal_fsync_ns_total: telemetry.as_ref().map_or(0, |m| m.wal_fsync_ns_total),
            wal_fsync_ns_max: telemetry.as_ref().map_or(0, |m| m.wal_fsync_ns_max),
            flush_queue_depth,
            flush_inflight,
            flush_enqueued_total: self.flush_metrics.enqueued_total,
            flush_build_count: self.flush_metrics.build_count,
            flush_build_ns_total: self.flush_metrics.build_ns_total,
            flush_build_ns_max: self.flush_metrics.build_ns_max,
            flush_publish_count: self.flush_metrics.publish_count,
            flush_publish_ns_total: self.flush_metrics.publish_ns_total,
            flush_publish_ns_max: self.flush_metrics.publish_ns_max,
            flush_failures_total: self.flush_metrics.failures_total,
            flush_retries_total: self.flush_metrics.retries_total,
            write_stall_ns_total: self
                .flush_metrics
                .write_stall_ns_total
                .saturating_add(write_stall_active_ns),
            write_stall_ns_max: self
                .flush_metrics
                .write_stall_ns_max
                .max(write_stall_active_ns),
            write_stall_active_ns,
            cloud_async_wal_segments_sealed: telemetry
                .as_ref()
                .map_or(0, |m| m.cloud_async_wal_segments_sealed),
            cloud_async_wal_bytes_sealed: telemetry
                .as_ref()
                .map_or(0, |m| m.cloud_async_wal_bytes_sealed),
            cloud_async_wal_seal_latency_us: telemetry
                .as_ref()
                .map_or(0, |m| m.cloud_async_wal_seal_latency_us),
            cloud_async_wal_uploads_started: telemetry
                .as_ref()
                .map_or(0, |m| m.cloud_async_wal_uploads_started),
            cloud_async_wal_uploads_completed: telemetry
                .as_ref()
                .map_or(0, |m| m.cloud_async_wal_uploads_completed),
            cloud_async_wal_uploads_failed: telemetry
                .as_ref()
                .map_or(0, |m| m.cloud_async_wal_uploads_failed),
            cloud_async_wal_upload_latency_us: telemetry
                .as_ref()
                .map_or(0, |m| m.cloud_async_wal_upload_latency_us),
            cloud_async_wal_ack_latency_us: telemetry
                .as_ref()
                .map_or(0, |m| m.cloud_async_wal_ack_latency_us),
            hybrid_max_local_bytes: 0,
            hybrid_total_committed_bytes: 0,
            hybrid_free_bytes: 0,
            hybrid_usage_percent: 0,
            hybrid_pending_evictions: 0,
            wal_recovery_records_replayed: self.wal_recovery_records_replayed,
            wal_recovery_bytes_replayed: self.wal_recovery_bytes_replayed,
            intent_log_replay_runs: self.intent_log_replay_runs,
            intent_log_entries_replayed: self.intent_log_entries_replayed,
        }
    }

    pub fn storage_layout_snapshot(&self) -> crate::types::StorageLayoutSnapshot {
        let mut levels =
            std::collections::BTreeMap::<u32, Vec<crate::types::StorageFileLayout>>::new();
        for file in &self.manifest.files {
            levels
                .entry(file.level)
                .or_default()
                .push(crate::types::StorageFileLayout {
                    name: file.name.clone(),
                    level: file.level,
                    cf_id: file.cf_id,
                    size_bytes: file.size_bytes,
                    smallest_key: file.smallest_key.clone(),
                    largest_key: file.largest_key.clone(),
                    smallest_seq: file.smallest_seq,
                    largest_seq: file.largest_seq,
                });
        }

        let levels = levels
            .into_iter()
            .map(|(level, mut files)| {
                files.sort_by(|a, b| a.name.cmp(&b.name));
                let total_bytes = files.iter().map(|file| file.size_bytes).sum();
                crate::types::StorageLayoutLevel {
                    level,
                    file_count: files.len(),
                    total_bytes,
                    files,
                }
            })
            .collect();

        let active_snapshots = self.snapshot_pins.snapshots(Instant::now());
        let residue = self.storage_residue_assessment();

        crate::types::StorageLayoutSnapshot {
            health: self.health(),
            manifest_last_persisted_sequence: self.manifest.last_persisted_sequence,
            manifest_next_wal_seq: self.manifest.next_wal_seq,
            levels,
            active_snapshots,
            pending_compactions: self.compaction.pending_tasks,
            compacting_ssts: self.compaction.compacting_ssts.clone(),
            obsolete_files: residue.orphan_ssts,
        }
    }

    fn storage_residue_assessment(
        &self,
    ) -> crate::runtime::storage_residue::StorageResidueAssessment {
        if self.is_memory_mode() {
            return crate::runtime::storage_residue::StorageResidueAssessment::default();
        }

        crate::runtime::storage_residue::StorageResidueAssessment::scan_sst_dir(
            &self.db_path,
            &self.manifest,
        )
    }

    pub fn cleanup_storage_residue(&mut self) {
        if self.is_memory_mode() {
            return;
        }

        self.cleanup_flush_staging_residue();

        let residue = self.storage_residue_assessment();

        for temp_name in residue.sst_temp_files {
            let path = FsPath::new(crate::sst::object_key(&temp_name));
            match self.fs.remove_file(&path) {
                Ok(()) => {
                    tracing::info!(path = %path.0.as_str(), "deleted non-authoritative SST temp residue");
                }
                Err(FsError::NotFound(_)) => {}
                Err(error) => {
                    tracing::warn!(
                        path = %path.0.as_str(),
                        error = %error,
                        "failed to delete non-authoritative SST temp residue"
                    );
                }
            }
        }

        for orphan_name in residue.orphan_ssts {
            let path = FsPath::new(crate::sst::object_key(&orphan_name));
            let injected_delete_failure =
                crate::failpoints::is_active("midge::recovery::inject_orphan_sst_delete_failure");
            if injected_delete_failure {
                self.mark_persistence_anomaly();
                tracing::warn!(
                    path = %path.0.as_str(),
                    "failed to delete orphan SST residue during startup cleanup"
                );
                continue;
            }
            match self.fs.remove_file(&path) {
                Ok(()) => {
                    tracing::info!(path = %path.0.as_str(), "deleted orphan SST residue during startup cleanup");
                }
                Err(FsError::NotFound(_)) => {}
                Err(error) => {
                    self.mark_persistence_anomaly();
                    tracing::warn!(
                        path = %path.0.as_str(),
                        error = %error,
                        "failed to delete orphan SST residue during startup cleanup"
                    );
                }
            }
        }

        self.cleanup_root_staging_residue();
    }

    fn cleanup_flush_staging_residue(&mut self) {
        let staging_dir = self.sst_dir.join(".flush-staging");
        match std::fs::remove_dir_all(&staging_dir) {
            Ok(()) => {
                tracing::info!(
                    path = %staging_dir.display(),
                    "deleted non-authoritative flush staging residue during startup"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                self.mark_persistence_anomaly();
                tracing::warn!(
                    path = %staging_dir.display(),
                    %error,
                    "failed to delete non-authoritative flush staging residue"
                );
            }
        }
    }

    fn cleanup_root_staging_residue(&mut self) {
        let root = FsPath::new("");
        let mut temp_files = Vec::new();
        let mut staging_dirs = Vec::new();

        match self.fs.list_dir(&root) {
            Ok(entries) => {
                for entry in entries {
                    if entry.is_dir {
                        if entry.name == "cloud_recovery" {
                            staging_dirs.push(entry.name);
                        }
                    } else if std::path::Path::new(&entry.name)
                        .extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("tmp"))
                    {
                        temp_files.push(entry.name);
                    }
                }
            }
            Err(error) => {
                tracing::debug!(error = %error, "skipping root residue cleanup because root listing failed");
                return;
            }
        }

        temp_files.sort();
        staging_dirs.sort();

        for temp_name in temp_files {
            let path = FsPath::new(temp_name);
            match self.fs.remove_file(&path) {
                Ok(()) => {
                    tracing::info!(path = %path.0.as_str(), "deleted root staging temp residue");
                }
                Err(FsError::NotFound(_)) => {}
                Err(error) => {
                    tracing::warn!(
                        path = %path.0.as_str(),
                        error = %error,
                        "failed to delete root staging temp residue"
                    );
                }
            }
        }

        for dir_name in staging_dirs {
            let path = FsPath::new(dir_name);
            match self.fs.remove_dir_all(&path) {
                Ok(()) => {
                    tracing::info!(path = %path.0.as_str(), "deleted stale staging directory");
                }
                Err(FsError::NotFound(_)) => {}
                Err(error) => {
                    tracing::warn!(
                        path = %path.0.as_str(),
                        error = %error,
                        "failed to delete stale staging directory"
                    );
                }
            }
        }
    }

    fn ensure_directories(
        db_path: &std::path::Path,
        memory_mode: bool,
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        let wal_dir = db_path.join("wal");
        let sst_dir = db_path.join("sst");

        if !memory_mode {
            if let Err(e) = std::fs::create_dir_all(db_path) {
                tracing::warn!(error = %e, path = ?db_path, "failed to create database directory");
            }
            if let Err(e) = std::fs::create_dir_all(&wal_dir) {
                tracing::warn!(error = %e, path = ?wal_dir, "failed to create WAL directory");
            }
            if let Err(e) = std::fs::create_dir_all(&sst_dir) {
                tracing::warn!(error = %e, path = ?sst_dir, "failed to create SST directory");
            }
        }

        (wal_dir, sst_dir)
    }

    /// Allocate the next sequence number. Saturates at `u64::MAX` to avoid wrap;
    /// after that, duplicate sequence numbers would occur (documented limitation).
    pub fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }

    pub fn restore_sequence_floor_from_manifest(&mut self) {
        let sequence_floor = Self::manifest_visible_sequence_floor(&self.manifest);
        if self.manifest.last_persisted_sequence < sequence_floor {
            self.manifest.last_persisted_sequence = sequence_floor;
        }
        if self.sequence < sequence_floor {
            self.sequence = sequence_floor;
        }
        if self.wal.local_durable_seq < sequence_floor {
            self.wal.local_durable_seq = sequence_floor;
        }
        if self.wal.cloud_durable_seq < sequence_floor {
            self.wal.cloud_durable_seq = sequence_floor;
        }
    }

    /// Reset the cloud durability frontier to bytes proven durable by the
    /// manifest. Startup separately advances it through the contiguous set of
    /// validated remote WAL segments; local-only replay must never contribute
    /// to this frontier until its resumed upload is acknowledged.
    pub(crate) fn reset_cloud_durable_sequence_for_recovery(&mut self) {
        self.wal.cloud_durable_seq = Self::manifest_visible_sequence_floor(&self.manifest);
    }

    /// Check if we have cached sequences for this `request_id`.
    /// Returns (`first_sequence`, count) if found and not yet confirmed.
    #[cfg(test)]
    pub fn get_cached_sequences(&self, request_id: u64) -> Option<(u64, usize)> {
        self.sequence_idempotency_cache
            .get(&request_id)
            .map(|(first_seq, count, _confirmed_at)| (*first_seq, *count))
    }

    /// Allocate sequences idempotently.
    /// If `request_id` is already in cache, return cached sequences.
    /// Otherwise, allocate count new sequences and cache them.
    ///
    /// PHASE 0 GUARDRAIL: Enforces `MAX_IDEMPOTENCY_CACHE_SIZE` to prevent unbounded
    /// memory growth under cloud upload stalls. Evicts oldest confirmed entries
    /// when limit is exceeded.
    #[cfg(test)]
    pub fn allocate_sequences_idempotent(&mut self, request_id: u64, count: usize) -> (u64, usize) {
        // Record telemetry
        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics().record_idempotency_alloc();
        }

        // Check if we have cached sequences for this request
        if let Some((first_seq, cnt)) = self.get_cached_sequences(request_id) {
            // Cache hit!
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_idempotency_cache_hit();
            }

            tracing::debug!(
                request_id = request_id,
                first_seq = first_seq,
                count = cnt,
                "reusing cached sequences for retry"
            );
            return (first_seq, cnt);
        }

        // PHASE 0 GUARDRAIL: Enforce cache size limit
        if self.sequence_idempotency_cache.len() >= MAX_IDEMPOTENCY_CACHE_SIZE {
            // Evict oldest confirmed entries (LRU strategy)
            let mut entries: Vec<_> = self
                .sequence_idempotency_cache
                .iter()
                .filter(|(_, (_, _, confirmed_at))| *confirmed_at > 0)
                .map(|(k, v)| (*k, *v))
                .collect();

            // Sort by confirmed_at (oldest first)
            entries.sort_by_key(|(_, (_, _, confirmed_at))| *confirmed_at);

            // Evict oldest 10% to avoid thrashing
            let evict_count = (MAX_IDEMPOTENCY_CACHE_SIZE / 10).max(1);
            for (req_id, _) in entries.iter().take(evict_count) {
                self.sequence_idempotency_cache.remove(req_id);
            }

            tracing::warn!(
                cache_size = self.sequence_idempotency_cache.len(),
                evicted = evict_count,
                "idempotency cache at capacity; evicted oldest confirmed entries"
            );

            // Record eviction metric
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics()
                    .record_idempotency_cache_evictions(evict_count as u64);
            }
        }

        // Allocate new sequences
        let first_seq = self.sequence + 1;
        self.sequence += count as u64;

        // Cache the allocation with current timestamp
        // We use local_durable_seq as the confirmation frontier for cleanup
        self.sequence_idempotency_cache
            .insert(request_id, (first_seq, count, 0)); // 0 = not confirmed yet

        tracing::debug!(
            request_id = request_id,
            first_seq = first_seq,
            count = count,
            "allocated new sequences"
        );

        (first_seq, count)
    }

    /// Confirm sequences for a `request_id` (mark as durable).
    /// This updates the `confirmed_at` frontier to current `local_durable_seq`.
    pub fn confirm_sequences(&mut self, request_id: u64) {
        if let Some(entry) = self.sequence_idempotency_cache.get_mut(&request_id) {
            entry.2 = self.wal.local_durable_seq;
            tracing::debug!(
                request_id = request_id,
                confirmed_at = self.wal.local_durable_seq,
                "confirmed sequences for request"
            );
        }
    }

    /// Confirm sequences for a `request_id` with an explicit `confirmed_at` sequence
    /// Useful for `CloudAsync` paths where the confirmation frontier is `cloud_durable_seq`.
    pub fn confirm_sequences_at(&mut self, request_id: u64, confirmed_at_seq: u64) {
        if let Some(entry) = self.sequence_idempotency_cache.get_mut(&request_id) {
            entry.2 = confirmed_at_seq;
            tracing::debug!(
                request_id = request_id,
                confirmed_at = confirmed_at_seq,
                "confirmed sequences for request at explicit frontier"
            );
        }
    }

    /// Clean up idempotency cache entries that have been confirmed and are now old.
    /// Called periodically as durability frontier advances.
    pub fn cleanup_old_idempotency_entries(&mut self) {
        let current_frontier = self.wal.local_durable_seq;
        self.sequence_idempotency_cache.retain(
            |_request_id, (_first_seq, _count, confirmed_at)| {
                // Keep entries that are either:
                // 1. Not yet confirmed (confirmed_at == 0)
                // 2. Recently confirmed (confirmed_at > frontier - 100)
                // Remove old confirmed entries beyond the frontier
                *confirmed_at == 0 || *confirmed_at > current_frontier.saturating_sub(100)
            },
        );
    }

    /// Invalidate cached idempotency allocations that are part of a failed WAL
    /// segment upload. Any entry whose last allocated sequence is <= `max_sequence`
    /// is removed so that retries will allocate fresh sequences.
    pub fn invalidate_idempotency_allocations_up_to(&mut self, max_sequence: u64) {
        self.sequence_idempotency_cache
            .retain(|_request_id, (first_seq, count, _)| {
                let last_seq = first_seq.saturating_add((*count as u64).saturating_sub(1));
                // Keep entries that extend beyond the failed max_sequence
                last_seq > max_sequence
            });
    }

    /// Get the next transaction ID
    pub fn next_txn_id(&mut self) -> u64 {
        self.next_txn_id += 1;
        self.next_txn_id
    }

    /// Clear pending transaction barrier and record duration metrics (Phase 3)
    pub fn clear_pending_transaction_barrier(&mut self) {
        if let Some(start_time) = self.pending_txn_start_time {
            let duration_ms = u64::try_from(start_time.elapsed().as_millis()).unwrap_or(u64::MAX);
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_pending_txn_duration_ms(duration_ms);
            }
        }
        self.pending_txn_min_seq = None;
        self.pending_txn_start_time = None;
    }

    /// Record one durable publication intent for a stable flush identity.
    ///
    /// A retry replaces any earlier intent for the same SST in a proposed copy
    /// and installs that copy only after persistence succeeds. This prevents a
    /// failed retry from accumulating duplicate intents or mutating live state.
    pub fn get_cf(&self, cf_id: crate::types::ColumnFamilyId) -> Option<&ColumnFamilyState> {
        self.column_families.get(&cf_id)
    }

    pub fn get_cf_mut(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> Option<&mut ColumnFamilyState> {
        self.column_families.get_mut(&cf_id)
    }

    #[cfg(test)]
    pub fn create_cf(&mut self, name: String) -> MidgeResult<u32> {
        let id = u32::try_from(self.column_families.len()).map_err(|_| {
            crate::common::MidgeError::Internal("too many column families".to_string())
        })?;
        self.column_families
            .insert(id, ColumnFamilyState::new(id, name));
        Ok(id)
    }

    #[cfg(test)]
    pub fn needs_flush(&self) -> Option<u32> {
        self.next_flush_candidate(false)
            .map(|candidate| candidate.cf_id)
    }
}

#[cfg(test)]
mod tests;
