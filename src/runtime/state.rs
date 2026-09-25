//! Runtime state - centralized mutable state owned by the runtime
//!
//! All engine state that can change at runtime lives here.
//! Actors read from and propose updates to this state.

use crate::common::{MidgeError, MidgeResult};
use crate::diagnostics::RuntimeDiagnostics;
use crate::memtable::SkipListMemtable;
use crate::metadata::Manifest;
use crate::runtime::snapshot_pins::SnapshotPinRegistry;
use crate::runtime::{IntentLogEntry, PublicationPhase};

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::io::traits::{Fs, FsError, FsPath};

mod flush;
mod manifest;
mod recovery;
mod snapshots;

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
    /// Earliest WAL segment that can contain this generation's records.
    /// Missing provenance must veto retirement until the generation publishes.
    pub first_wal_segment: Option<u64>,
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
    /// `WalState::appended_bytes` when the active memtable first became
    /// non-empty; set with `active_memtable_started_in_segment`.
    pub(crate) active_memtable_started_at_wal_bytes: u64,
}

/// Root directory for SSTs a salvage open kept but could not prove
/// referenced. Startup cleanup never deletes anything under it.
pub(crate) const SALVAGE_RETAINED_DIR: &str = "salvage-retained";

impl ColumnFamilyState {
    pub fn new(_id: u32, _name: String) -> Self {
        Self {
            memtable: Arc::new(SkipListMemtable::new()),
            immutable_memtables: Vec::new(),
            immutable_flushes: Vec::new(),
            active_memtable_started_in_segment: 1,
            active_memtable_started_at_wal_bytes: 0,
        }
    }
}

/// How flush outputs get SST names in hybrid mode, per column family.
///
/// Names come from `cursor` inside a block that `manifest.next_sst_seqs`
/// reserves durably. A name is safe to upload only below `reserved_through`:
/// the reservation covering it was journaled and mirrored to cloud metadata
/// by this session. Both start empty, so each session's first reservation
/// journals and mirrors, and its cursor starts at the durable reservation,
/// past every name an earlier session handed out.
#[derive(Debug, Default)]
pub(crate) struct SstNameAllocation {
    pub(crate) cursor: HashMap<crate::types::ColumnFamilyId, u64>,
    pub(crate) reserved_through: HashMap<crate::types::ColumnFamilyId, u64>,
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
    /// Why each retained sealed local WAL segment last failed its coverage
    /// proof. In memory only; an empty map just means re-proving (#490).
    pub(crate) local_segment_proofs:
        HashMap<u64, crate::runtime::hybrid_persistence::FailedWalProof>,
    /// WAL bytes appended by this process, across segments. In memory only:
    /// it measures how much local WAL a family's memtable pins (#552).
    pub(crate) appended_bytes: u64,
}

impl Default for WalState {
    fn default() -> Self {
        Self {
            current_segment_id: 1,
            last_synced_seq: 0,
            pending_writes: 0,
            local_durable_seq: 0,
            cloud_durable_seq: 0,
            local_segment_proofs: HashMap::new(),
            appended_bytes: 0,
        }
    }
}

/// Compaction state
#[derive(Default)]
pub struct CompactionState {
    /// SSTs currently being compacted (locked from other compactions)
    pub compacting_ssts: Vec<String>,
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
    /// An active memtable started too many WAL segments ago (cloud).
    WalSegmentGap,
    /// An active memtable pins too many local WAL bytes (local).
    WalBytesGap,
}

/// Which rule, if any, flushes an active memtable that no size threshold
/// will flush soon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EventualFlush {
    /// Only size thresholds and pending retries (tests).
    #[cfg(test)]
    Disabled,
    /// Cloud: bounds the WAL catalog by segment count.
    SegmentGap,
    /// Local: bounds retained WAL by bytes. Local segments are sealed only
    /// when a flush publishes, so a segment rule would let eventual flushes
    /// trigger one another; flushes append no WAL bytes (#552).
    WalBytes,
}

/// Local eventual flush: a family is flushed once this many memtable flush
/// triggers' worth of WAL has been appended since its memtable started.
pub(crate) const LOCAL_EVENTUAL_FLUSH_TRIGGER_MULTIPLE: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushCandidate {
    pub cf_id: crate::types::ColumnFamilyId,
    pub reason: FlushReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimePersistence {
    Memory,
    Durable,
}

impl RuntimePersistence {
    const fn from_memory_mode(memory_mode: bool) -> Self {
        if memory_mode {
            Self::Memory
        } else {
            Self::Durable
        }
    }

    const fn is_memory(self) -> bool {
        matches!(self, Self::Memory)
    }

    const fn compaction_enabled(self) -> bool {
        matches!(self, Self::Durable)
    }
}

pub struct RuntimeMode {
    #[cfg(test)]
    pub read_only: bool,
    persistence: RuntimePersistence,
}

pub struct RecoveryStatus {
    pub policy: crate::config::RecoveryPolicy,
    pub opened_in_salvage_mode: bool,
    pub persistence_anomaly_detected: bool,
    /// Set by the DDL path when a remote authority switch could not be
    /// resolved. The event loop reads this instead of matching the phrase
    /// "DDL authority is ambiguous" in an error message.
    pub ddl_authority_ambiguous: bool,
    pub metadata: MetadataSync,
}

/// Whether the in-memory manifest and intent log match disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataSync {
    Current,
    /// The manifest and intent log could not be re-read after another writer
    /// advanced them on disk. Memory is behind disk, so every publication
    /// from memory is refused until a reload succeeds (#500).
    ReloadRequired,
}

pub struct CompactionConfig {
    pub enabled: bool,
}

pub struct WritePressureState {
    pub stalled: bool,
}

struct TransactionCoordination {
    next_id: u64,
    pending_started_at: Option<std::time::Instant>,
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
    /// Highest compaction output generation observed or allocated. This is
    /// recovered from canonical output names without advancing WAL authority.
    pub(crate) compaction_output_generation: u64,
    transaction: TransactionCoordination,

    // === Column Families ===
    pub column_families: HashMap<u32, ColumnFamilyState>,

    // === Metadata ===
    pub manifest: Manifest,

    // Filesystem abstraction for all IO (never call std::fs directly)
    pub fs: std::sync::Arc<dyn Fs>,
    /// The only runtime writer of the manifest journal and snapshot (#494).
    pub(crate) manifest_store: Arc<crate::metadata::store::ManifestStore>,
    /// Flush SST name allocation in hybrid mode (#491).
    pub(crate) sst_names: SstNameAllocation,
    /// Authoritative SST views used during cloud intent replay. Local SST
    /// staging is disposable and is not a prerequisite for recovery.
    pub(crate) recovery_sst_fs: Option<Arc<dyn Fs>>,
    /// Locally verified recovery copies retained only when explicit salvage
    /// cannot use the authoritative cloud object.
    pub(crate) salvaged_local_ssts: HashSet<String>,
    /// Engine-scoped TTL clock with a process-local nondecreasing floor.
    pub(crate) ttl_clock: Arc<crate::common::time::ObservedClock>,

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
    /// Cloud: flush an active memtable once it started this many WAL
    /// segments ago, bounding the WAL catalog.
    pub eventual_flush_segment_gap: u64,
    pub write_pressure: WritePressureState,
    /// Total size of all memtables (in-memory)
    pub total_memtable_bytes: usize,
    /// Maximum number of immutable memtables per CF before write stall
    pub max_immutable_memtables: usize,
    /// Soft L0 file-count trigger used to derive the internal hard admission
    /// ceiling. Kept in runtime state so WAL admission and compaction planning
    /// observe one value.
    pub(crate) l0_compaction_trigger: usize,
    /// Next process-local flush identity. Flush identities are stable for the
    /// lifetime of an immutable and are never inferred from SST sequence state.
    pub(crate) next_flush_id: u64,
    pub(crate) writer_epoch: u64,
    pub(crate) flush_metrics: FlushRuntimeMetrics,

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
    pub(crate) fn observed_time_millis(&self) -> u64 {
        self.ttl_clock.now_millis()
    }

    pub(crate) fn install_ttl_clock(&mut self, clock: Arc<crate::common::time::ObservedClock>) {
        self.ttl_clock = clock;
    }
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
        let pinned_ssts = self.snapshot_pins.observed_pinned_sst_count();
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
        let read_path = self.diagnostics.snapshot();
        // This engine's own counters: no telemetry setup needed, and another
        // engine in the process never shows up here.
        let telemetry = Some(self.diagnostics.counters());
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
            durability_waiters_fanned_out_total: 0,
            abandoned_runtime_requests_total: 0,
            late_runtime_responses_total: 0,
            sst_bloom_rejects_total: read_path.bloom_rejects,
            sst_bloom_checks_total: read_path.bloom_checks,
            sst_data_blocks_read_total: read_path.data_blocks_read,
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
            local_storage: None,
            remote_range_requests_total: 0,
            remote_range_bytes_total: 0,
            remote_range_failures_total: 0,
            remote_range_latency_ns_total: 0,
            remote_range_latency_ns_max: 0,
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
            let path = FsPath::new(crate::cloud_layout::object_key(&temp_name));
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

        if self.opened_in_salvage_mode() && !residue.orphan_ssts.is_empty() {
            // A salvaged manifest may be a fallback or a truncated replay, so
            // "not in the manifest" does not prove an SST is garbage. Move
            // every candidate out of the SST directory for operator-controlled
            // recovery: left in place, the next strict open would see a
            // readable manifest without them and delete them.
            self.quarantine_salvage_retained_ssts(&residue.orphan_ssts);
            self.cleanup_root_staging_residue();
            return;
        }

        for orphan_name in residue.orphan_ssts {
            let path = FsPath::new(crate::cloud_layout::object_key(&orphan_name));
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

    /// Move SSTs a salvage open could not prove unreferenced into
    /// `salvage-retained/<millis>/`, which startup cleanup never deletes.
    /// A file that cannot be moved stays in place and marks an anomaly.
    fn quarantine_salvage_retained_ssts(&mut self, sst_names: &[String]) {
        let millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis());
        let quarantine = FsPath::new(format!("{SALVAGE_RETAINED_DIR}/{millis}"));
        if let Err(error) = self.fs.create_dir_all(&quarantine) {
            self.mark_persistence_anomaly();
            tracing::warn!(
                error = %error,
                retained = sst_names.len(),
                "salvage mode could not create its SST quarantine; SSTs stay in place"
            );
            return;
        }
        let mut moved = 0usize;
        for name in sst_names {
            let from = FsPath::new(crate::cloud_layout::object_key(name));
            let to = FsPath::new(format!("{}/{name}", quarantine.0));
            match self.fs.rename_atomic(&from, &to) {
                Ok(()) => moved += 1,
                Err(FsError::NotFound(_)) => {}
                Err(error) => {
                    self.mark_persistence_anomaly();
                    tracing::warn!(
                        path = %from.0.as_str(),
                        error = %error,
                        "salvage mode could not quarantine an unlisted SST; it stays in place"
                    );
                }
            }
        }
        for dir in [
            FsPath::new(crate::cloud_layout::object_key("")),
            quarantine.clone(),
            FsPath::new(SALVAGE_RETAINED_DIR),
            FsPath::new(""),
        ] {
            let dir = FsPath::new(dir.0.trim_end_matches('/'));
            if let Err(error) = self.fs.sync_dir(&dir, crate::io::Durability::Durable) {
                self.mark_persistence_anomaly();
                tracing::warn!(dir = %dir.0.as_str(), error = %error, "failed to sync salvage quarantine directory");
            }
        }
        tracing::warn!(
            retained = sst_names.len(),
            moved,
            quarantine = %quarantine.0.as_str(),
            orphan_ssts = ?sst_names,
            "salvage mode quarantined SSTs missing from the recovered manifest"
        );
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
                    self.mark_persistence_anomaly();
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
            let result = if crate::failpoints::with_read_gate(|| {
                crate::failpoints::is_active("midge::recovery::inject_root_staging_delete_failure")
            }) {
                Err(FsError::Io("injected root staging cleanup failure".into()))
            } else {
                self.fs.remove_dir_all(&path)
            };
            match result {
                Ok(()) => {
                    tracing::info!(path = %path.0.as_str(), "deleted stale staging directory");
                }
                Err(FsError::NotFound(_)) => {}
                Err(error) => {
                    self.mark_persistence_anomaly();
                    tracing::warn!(
                        path = %path.0.as_str(),
                        error = %error,
                        "failed to delete stale staging directory"
                    );
                }
            }
        }
    }

    /// Physical scratch left after startup cleanup. Nested SST/WAL staging
    /// remains in their own resident-byte totals and must not be counted twice.
    pub(crate) fn retained_startup_scratch_bytes(&self) -> MidgeResult<u64> {
        if self.mode.persistence.is_memory() {
            return Ok(0);
        }
        let root = FsPath::new("");
        let mut directories = vec![root.clone()];
        let mut bytes = 0_u64;
        while let Some(directory) = directories.pop() {
            for entry in self.fs.list_dir(&directory)? {
                if directory == root {
                    let is_scratch = if entry.is_dir {
                        matches!(entry.name.as_str(), "cloud_recovery" | "txn")
                    } else {
                        std::path::Path::new(&entry.name)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("tmp"))
                    };
                    if !is_scratch {
                        continue;
                    }
                }
                let path = if directory == root {
                    FsPath::new(entry.name)
                } else {
                    FsPath::new(format!("{}/{}", directory.0, entry.name))
                };
                if entry.is_dir {
                    directories.push(path);
                } else {
                    bytes = bytes
                        .checked_add(self.fs.metadata(&path)?.len)
                        .ok_or_else(|| {
                            MidgeError::NoSpace("startup scratch byte accounting overflow".into())
                        })?;
                }
            }
        }
        Ok(bytes)
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
            // Durable: the entries must survive a crash before the first
            // acknowledged write, whatever else syncs the database root (#519).
            // A failure surfaces again when the WAL writer opens its directory.
            if let Err(e) = crate::io::durable_dir::create_dir_all_durably(db_path, &wal_dir) {
                tracing::warn!(error = %e, path = ?wal_dir, "failed to create WAL directory");
            }
            if let Err(e) = crate::io::durable_dir::create_dir_all_durably(db_path, &sst_dir) {
                tracing::warn!(error = %e, path = ?sst_dir, "failed to create SST directory");
            }
        }

        (wal_dir, sst_dir)
    }

    /// Allocate the next sequence number. Saturates at `u64::MAX` to avoid wrap;
    /// after that, duplicate sequence numbers would occur (documented limitation).
    #[cfg(test)]
    pub fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }

    pub(crate) fn next_compaction_output_generation(&mut self) -> MidgeResult<u64> {
        let current = self.compaction_output_generation.max(self.sequence);
        let next = current.checked_add(1).ok_or_else(|| {
            MidgeError::ResourceLimit("compaction output sequence exhausted".to_string())
        })?;
        self.compaction_output_generation = next;
        self.sequence = next;
        Ok(next)
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
        self.compaction_output_generation =
            self.compaction_output_generation.max(self.sequence).max(
                Self::manifest_compaction_output_generation_floor(&self.manifest),
            );
    }

    /// Reset the cloud durability frontier to bytes proven durable by the
    /// manifest. Startup separately advances it through the contiguous set of
    /// validated remote WAL segments; local-only replay must never contribute
    /// to this frontier until its resumed upload is acknowledged.
    pub(crate) fn reset_cloud_durable_sequence_for_recovery(&mut self) {
        self.wal.cloud_durable_seq = Self::manifest_visible_sequence_floor(&self.manifest);
    }

    /// Get the next transaction ID
    pub fn next_txn_id(&mut self) -> MidgeResult<u64> {
        let next = self.transaction.next_id.checked_add(1).ok_or_else(|| {
            MidgeError::ResourceLimit("transaction ID space exhausted".to_string())
        })?;
        self.transaction.next_id = next;
        Ok(next)
    }

    /// Start timing the oldest transaction still waiting for durability.
    pub(crate) fn begin_pending_transaction(&mut self) {
        self.transaction
            .pending_started_at
            .get_or_insert_with(std::time::Instant::now);
    }

    #[cfg(test)]
    pub(crate) fn pending_transaction_started(&self) -> bool {
        self.transaction.pending_started_at.is_some()
    }

    #[cfg(test)]
    pub(crate) fn transaction_id_cursor_for_test(&self) -> u64 {
        self.transaction.next_id
    }

    #[cfg(test)]
    pub(crate) fn set_transaction_id_cursor_for_test(&mut self, next_id: u64) {
        self.transaction.next_id = next_id;
    }

    /// Record how long pending transactions waited for durability, and stop
    /// timing.
    pub fn clear_pending_transaction_barrier(&mut self) {
        if let Some(start_time) = self.transaction.pending_started_at {
            let duration_ms = u64::try_from(start_time.elapsed().as_millis()).unwrap_or(u64::MAX);
            self.diagnostics.record(|m| {
                m.record_pending_txn_duration_ms(duration_ms);
            });
        }
        self.transaction.pending_started_at = None;
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
        self.next_flush_candidate(EventualFlush::Disabled)
            .map(|candidate| candidate.cf_id)
    }
}

#[cfg(test)]
mod tests;
