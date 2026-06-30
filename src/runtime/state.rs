//! Runtime state - centralized mutable state owned by the runtime
//!
//! All engine state that can change at runtime lives here.
//! Actors read from and propose updates to this state.

use crate::common::{MidgeError, MidgeResult};
use crate::metadata::Manifest;
use crate::runtime::{IntentLogEntry, PublicationPhase};
use crate::sst::{Memtable, ReadAmpMetrics, SkipListMemtable};

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use crate::io::traits::{Fs, FsError, FsPath};

/// Maximum size of idempotency cache to prevent unbounded growth under cloud stall
const MAX_IDEMPOTENCY_CACHE_SIZE: usize = 100_000;
const MAX_RECENT_DELETE_RANGES: usize = 4096;
#[derive(Debug, Clone)]
pub struct RecentDeleteRange {
    pub cf_id: crate::types::ColumnFamilyId,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub sequence: u64,
}

/// Column family state
pub struct ColumnFamilyState {
    pub id: u32,
    pub name: String,
    pub memtable: Arc<SkipListMemtable>,
    /// Immutable memtables waiting to be flushed
    pub immutable_memtables: Vec<Arc<SkipListMemtable>>,
    /// WAL segment ID when the active memtable first became non-empty.
    pub active_memtable_started_in_segment: u64,
}

impl ColumnFamilyState {
    pub fn new(id: u32, name: String) -> Self {
        Self {
            id,
            name,
            memtable: Arc::new(SkipListMemtable::new()),
            immutable_memtables: Vec::new(),
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
    pub last_cloud_checkpoint_seq: u64,
}

/// Snapshot pinning state
///
/// Tracks active snapshots to prevent SST garbage collection
/// while snapshots are reading from those SSTs.
#[derive(Default)]
pub struct SnapshotState {
    /// Active snapshots: `snapshot_id` → (sequence, `created_at`, `ref_count`, `pinned_ssts`)
    pub active_snapshots: HashMap<u64, (u64, Instant, usize, HashSet<String>)>,
    /// Maximum time to hold a snapshot (1 hour by default)
    pub max_snapshot_lifetime: std::time::Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlushReason {
    SizeThreshold,
    CloudSegmentGap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FlushCandidate {
    pub cf_id: crate::types::ColumnFamilyId,
    pub reason: FlushReason,
}

impl RuntimeState {
    pub fn memtable_flush_trigger_bytes(&self) -> usize {
        self.memtable_size_limit
            .min(self.memtable_flush_threshold)
            .max(1)
    }

    pub fn is_immutable_memtable_queue_full(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        self.column_families.get(&cf_id).is_some_and(|cf_state| {
            cf_state.immutable_memtables.len() >= self.max_immutable_memtables
        })
    }

    pub fn is_total_memtable_hard_limit_exceeded(&self) -> bool {
        self.total_memtable_bytes >= self.memtable_flush_threshold.saturating_mul(2)
    }

    /// Check whether writes must be rejected until runtime pressure clears.
    ///
    /// This intentionally excludes active memtable size: active memtable pressure
    /// should trigger flush, not block the flush that would relieve it.
    pub fn should_hard_stall_writes(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        if self.write_stalled {
            return true;
        }
        if self.is_immutable_memtable_queue_full(cf_id) {
            return true;
        }
        self.is_total_memtable_hard_limit_exceeded()
    }

    pub fn has_any_hard_write_stall(&self) -> bool {
        if self.write_stalled || self.is_total_memtable_hard_limit_exceeded() {
            return true;
        }
        self.column_families
            .keys()
            .any(|cf_id| self.is_immutable_memtable_queue_full(*cf_id))
    }

    /// Compatibility wrapper for write-admission callsites.
    pub fn should_stall_writes(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        self.should_hard_stall_writes(cf_id)
    }

    pub fn memtable_needs_flush(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        if let Some(cf_state) = self.column_families.get(&cf_id) {
            return cf_state.memtable.size_bytes() >= self.memtable_flush_trigger_bytes();
        }
        false
    }

    pub(crate) fn active_memtable_wal_segment_gap(
        &self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> u64 {
        self.column_families.get(&cf_id).map_or(0, |cf_state| {
            if cf_state.memtable.size_bytes() == 0 {
                0
            } else {
                self.wal
                    .current_segment_id
                    .saturating_sub(cf_state.active_memtable_started_in_segment)
            }
        })
    }

    pub(crate) fn max_memtable_wal_segment_gap(&self) -> u64 {
        self.column_families
            .keys()
            .map(|cf_id| self.active_memtable_wal_segment_gap(*cf_id))
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn cloud_wal_recovery_floor_segment(&self) -> Option<u64> {
        if self
            .column_families
            .values()
            .any(|cf_state| !cf_state.immutable_memtables.is_empty())
        {
            return None;
        }

        let oldest_active_segment = self
            .column_families
            .values()
            .filter(|cf_state| cf_state.memtable.size_bytes() > 0)
            .map(|cf_state| cf_state.active_memtable_started_in_segment)
            .min();

        Some(oldest_active_segment.unwrap_or(self.wal.current_segment_id))
    }

    pub(crate) fn next_flush_candidate(
        &self,
        cloud_segment_gap_enabled: bool,
    ) -> Option<FlushCandidate> {
        let flush_threshold = self.memtable_flush_trigger_bytes();

        let size_candidate = self
            .column_families
            .iter()
            .filter_map(|(cf_id, cf_state)| {
                let size = cf_state.memtable.size_bytes();
                (size >= flush_threshold).then_some((*cf_id, size))
            })
            .max_by_key(|(cf_id, size)| (*size, std::cmp::Reverse(*cf_id)));

        if let Some((cf_id, _)) = size_candidate {
            return Some(FlushCandidate {
                cf_id,
                reason: FlushReason::SizeThreshold,
            });
        }

        if !cloud_segment_gap_enabled {
            return None;
        }

        self.column_families
            .iter()
            .filter_map(|(cf_id, cf_state)| {
                if cf_state.memtable.size_bytes() == 0 {
                    return None;
                }

                let gap = self
                    .wal
                    .current_segment_id
                    .saturating_sub(cf_state.active_memtable_started_in_segment);
                (gap >= self.cloud_eventual_flush_segment_gap).then_some((*cf_id, gap))
            })
            .max_by_key(|(cf_id, gap)| (*gap, std::cmp::Reverse(*cf_id)))
            .map(|(cf_id, _)| FlushCandidate {
                cf_id,
                reason: FlushReason::CloudSegmentGap,
            })
    }

    pub(crate) fn reinitialize_active_memtable_segment_tracking(&mut self) {
        let current_segment_id = self.wal.current_segment_id;
        for cf_state in self.column_families.values_mut() {
            if cf_state.memtable.size_bytes() > 0 {
                cf_state.active_memtable_started_in_segment = current_segment_id;
            }
        }
    }
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
    /// Recently committed range deletes for strict write-conflict checks.
    pub recent_delete_ranges: Vec<RecentDeleteRange>,

    // === Configuration ===
    pub memtable_size_limit: usize,
    pub read_only: bool,
    /// If true, never touch filesystem (pure in-memory mode)
    pub memory_mode: bool,
    /// Recovery policy selected at engine open.
    pub recovery_policy: crate::config::RecoveryPolicy,
    /// True if startup had to salvage around metadata or WAL issues.
    pub opened_in_salvage_mode: bool,
    /// True when runtime detected non-fatal persistence anomalies that operators should investigate.
    pub persistence_anomaly_detected: bool,
    /// Whether background compaction is enabled
    pub enable_compaction: bool,

    // === Intent Log & Determinism ===
    /// Deterministic intent log for recovery and replay
    pub intent_log: Vec<IntentLogEntry>,
    /// Maximum size of any memtable before write stall
    pub memtable_flush_threshold: usize,
    /// Cloud-only runtime heuristic: flush active memtables after enough WAL segment churn.
    pub cloud_eventual_flush_segment_gap: u64,
    /// Write stall active flag
    pub write_stalled: bool,
    /// Total size of all memtables (in-memory)
    pub total_memtable_bytes: usize,
    /// Maximum number of immutable memtables per CF before write stall
    pub max_immutable_memtables: usize,

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

    /// Condvar used to wait for `active_compactions` == 0.
    pub active_compactions_notify: std::sync::Arc<(parking_lot::Mutex<()>, parking_lot::Condvar)>,

    /// Whether an ingest barrier is currently active. This is set at `BeginIngest`
    /// and cleared at `EndIngest` so tools and tests can detect when ingest mode
    /// is enforced.
    pub ingest_active: std::sync::Arc<std::sync::atomic::AtomicBool>,

    /// Pending CompactAll/BeginIngest requests waiting for compactions to finish.
    /// Maps `request_id` -> `completion_condition` (e.g., "`CompactAll`", "`BeginIngest`").
    pub pending_compaction_waits: parking_lot::Mutex<std::collections::HashMap<u64, String>>,
}

impl RuntimeState {
    fn manifest_visible_sequence_floor(manifest: &Manifest) -> u64 {
        let file_max_sequence = manifest
            .files
            .iter()
            .filter_map(|file| file.largest_seq.or(file.smallest_seq))
            .max()
            .unwrap_or(0);

        manifest.last_persisted_sequence.max(file_max_sequence)
    }

    /// Create new runtime state with the given database path.
    /// If `memory_mode` is true, filesystem is never touched.
    pub fn new(db_path: PathBuf, memory_mode: bool) -> Self {
        Self::try_new(db_path, memory_mode, crate::config::RecoveryPolicy::Strict)
            .expect("runtime state initialization failed")
    }

    /// Create new runtime state with an explicit recovery policy.
    pub fn try_new(
        db_path: PathBuf,
        memory_mode: bool,
        recovery_policy: crate::config::RecoveryPolicy,
    ) -> MidgeResult<Self> {
        Self::try_new_with_recovery_dir(db_path, memory_mode, None, recovery_policy)
    }

    /// Create new runtime state with an optional override for WAL recovery.
    ///
    /// When `recovery_wal_dir` is provided, recovery replays WAL from that
    /// directory (instead of `db_path/wal`). This is used for `CloudAsync` mode
    /// where cloud WAL is the source of truth.
    pub fn new_with_recovery_dir(
        db_path: PathBuf,
        memory_mode: bool,
        recovery_wal_dir: Option<PathBuf>,
    ) -> Self {
        Self::try_new_with_recovery_dir(
            db_path,
            memory_mode,
            recovery_wal_dir,
            crate::config::RecoveryPolicy::Strict,
        )
        .expect("runtime state initialization with recovery dir failed")
    }

    /// Create new runtime state with an optional override for WAL recovery and
    /// an explicit recovery policy.
    pub fn try_new_with_recovery_dir(
        db_path: PathBuf,
        memory_mode: bool,
        recovery_wal_dir: Option<PathBuf>,
        recovery_policy: crate::config::RecoveryPolicy,
    ) -> MidgeResult<Self> {
        let (wal_dir, sst_dir) = Self::ensure_directories(&db_path, memory_mode);
        let mut opened_in_salvage_mode = false;

        if !memory_mode {
            crate::metadata::ensure_or_create_format_marker(&db_path)?;
        }

        // Initialize filesystem abstraction (use MockFs in memory mode)
        let fs: std::sync::Arc<dyn Fs> = if memory_mode {
            std::sync::Arc::new(crate::io::MockFs::new())
        } else {
            std::sync::Arc::new(crate::io::real::RealFs::new(&db_path).map_err(|e| {
                MidgeError::RecoveryFailed(format!("failed to initialize filesystem: {e}"))
            })?)
        };

        // Load manifest (prefer snapshot + journal replay) — only if not in memory mode
        let manifest = if memory_mode {
            Manifest::default()
        } else {
            let strict_manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
                &fs,
                crate::config::RecoveryPolicy::Strict,
            );
            match strict_manifest {
                Ok(m) => {
                    tracing::info!("manifest loaded from disk");
                    m
                }
                Err(e) => {
                    if recovery_policy == crate::config::RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to load manifest: {e}"
                        )));
                    }
                    opened_in_salvage_mode = true;
                    tracing::warn!(
                        "failed to load manifest strictly, retrying in salvage mode: {}",
                        e
                    );
                    crate::metadata::ManifestPersistence::load_with_fs_and_policy(
                        &fs,
                        crate::config::RecoveryPolicy::Salvage,
                    )
                    .unwrap_or_else(|salvage_error| {
                        tracing::warn!(
                            "failed to load manifest in salvage mode, using default: {}",
                            salvage_error
                        );
                        Manifest::default()
                    })
                }
            }
        };

        // Load intent log if present (using FS via persistence helpers)
        let intent_log = if memory_mode {
            Vec::new()
        } else {
            let strict_intent = crate::runtime::IntentPersistence::load_with_fs_and_policy(
                &fs,
                crate::config::RecoveryPolicy::Strict,
            );
            match strict_intent {
                Ok(v) => v,
                Err(e) => {
                    if recovery_policy == crate::config::RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to load intent log: {e}"
                        )));
                    }
                    opened_in_salvage_mode = true;
                    tracing::warn!(
                        error = %e,
                        "failed to load intent log strictly, retrying in salvage mode"
                    );
                    crate::runtime::IntentPersistence::load_with_fs_and_policy(
                        &fs,
                        crate::config::RecoveryPolicy::Salvage,
                    )
                    .unwrap_or_else(|salvage_error| {
                        tracing::warn!(
                            error = %salvage_error,
                            "failed to load intent log in salvage mode, starting empty"
                        );
                        Vec::new()
                    })
                }
            }
        };

        // Default: allow up to 10 immutable memtables before stall
        let max_immutable_memtables = 10;
        let _ = max_immutable_memtables;

        let mut column_families = HashMap::new();
        column_families.insert(0, ColumnFamilyState::new(0, "default".into()));

        for cf_meta in &manifest.column_families {
            // Skip deleted column families and default CF (already added)
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                column_families.insert(
                    cf_meta.id,
                    ColumnFamilyState::new(cf_meta.id, cf_meta.name.clone()),
                );
            }
        }

        // WAL recovery (skip in memory mode)
        let replay_dir = recovery_wal_dir.as_deref().unwrap_or(&wal_dir);
        let mut wal_recovery_records_replayed = 0_u64;
        let mut wal_recovery_bytes_replayed = 0_u64;
        let wal_recovered_sequence = if !memory_mode && replay_dir.exists() {
            let mut recovery_memtables = HashMap::new();
            match crate::storage::LocalFsStorage::new(replay_dir) {
                Ok(storage) => match crate::wal::recovery::replay_wal_with_policy(
                    &storage,
                    &crate::storage::abstraction::StoragePath::new(""),
                    &mut recovery_memtables,
                    match recovery_policy {
                        crate::config::RecoveryPolicy::Strict => {
                            crate::wal::recovery::ReplayPolicy::Strict
                        }
                        crate::config::RecoveryPolicy::Salvage => {
                            crate::wal::recovery::ReplayPolicy::SalvageValidPrefix
                        }
                    },
                ) {
                    Ok(stats) => {
                        if let Some(t) = crate::telemetry::Telemetry::global() {
                            t.metrics()
                                .record_wal_recovery(stats.record_count, stats.bytes);
                        }
                        wal_recovery_records_replayed =
                            wal_recovery_records_replayed.saturating_add(stats.record_count);
                        wal_recovery_bytes_replayed =
                            wal_recovery_bytes_replayed.saturating_add(stats.bytes);

                        tracing::info!(
                            records_recovered = stats.record_count,
                            bytes_recovered = stats.bytes,
                            max_sequence = ?stats.max_sequence,
                            replay_dir = ?replay_dir,
                            replay_ms = std::time::Duration::from_nanos(
                                u64::try_from(stats.total_replay_ns).unwrap_or(u64::MAX),
                            )
                            .as_secs_f64()
                                * 1_000.0,
                            wal_read_ms = std::time::Duration::from_nanos(
                                u64::try_from(stats.wal_read_ns).unwrap_or(u64::MAX),
                            )
                            .as_secs_f64()
                                * 1_000.0,
                            apply_ms = std::time::Duration::from_nanos(
                                u64::try_from(stats.apply_ns).unwrap_or(u64::MAX),
                            )
                            .as_secs_f64()
                                * 1_000.0,
                            "WAL recovery completed successfully"
                        );
                        if stats.had_corruption {
                            opened_in_salvage_mode = true;
                            tracing::warn!(
                                replay_dir = ?replay_dir,
                                "WAL recovery salvaged a valid prefix after corruption"
                            );
                        }
                        for (cf_id, recovered_memtable) in recovery_memtables {
                            if let Some(cf_state) = column_families.get_mut(&cf_id) {
                                cf_state.memtable = recovered_memtable;
                            } else {
                                let name = format!("cf_{cf_id}");
                                let mut cf_state = ColumnFamilyState::new(cf_id, name);
                                cf_state.memtable = recovered_memtable;
                                column_families.insert(cf_id, cf_state);
                            }
                        }
                        // Restore sequence counter from recovery
                        stats.max_sequence.unwrap_or(0)
                    }
                    Err(e) => {
                        if recovery_policy == crate::config::RecoveryPolicy::Strict {
                            return Err(MidgeError::RecoveryFailed(format!(
                                "WAL recovery failed: {e}"
                            )));
                        }
                        opened_in_salvage_mode = true;
                        tracing::error!(error = %e, "WAL recovery failed, continuing without recovered state in salvage mode");
                        0
                    }
                },
                Err(e) => {
                    if recovery_policy == crate::config::RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to initialize WAL recovery storage: {e}"
                        )));
                    }
                    opened_in_salvage_mode = true;
                    tracing::error!(error = %e, "failed to initialize WAL recovery storage in salvage mode");
                    0
                }
            }
        } else {
            0
        };
        let recovered_sequence =
            wal_recovered_sequence.max(Self::manifest_visible_sequence_floor(&manifest));

        // WAL segment id recovery:
        // On restart, we must continue segment ids beyond the highest existing rotated segment
        // to avoid overwriting previously-uploaded cloud WAL segments (CloudAsync) or local
        // WAL segments (LocalDisk). The source of truth is the replay directory.
        let recovered_next_segment_id = if !memory_mode && replay_dir.exists() {
            let mut max_segment_id: u64 = 0;

            if let Ok(entries) = std::fs::read_dir(replay_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) != Some("wal") {
                        continue;
                    }
                    let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                        continue;
                    };

                    // Skip the active WAL file name if it happens to match the extension.
                    if stem.eq_ignore_ascii_case("wal") {
                        continue;
                    }

                    if let Ok(id) = stem.parse::<u64>() {
                        max_segment_id = max_segment_id.max(id);
                    }
                }
            }

            // Segment ids start at 1.
            max_segment_id.saturating_add(1).max(1)
        } else {
            1
        };

        let mut state = Self {
            db_path,
            wal_dir,
            sst_dir,
            sequence: recovered_sequence,
            next_txn_id: 0,
            pending_txn_min_seq: None,
            pending_txn_start_time: None,
            sequence_idempotency_cache: HashMap::new(),
            column_families,
            manifest,
            fs: fs.clone(),
            wal: WalState {
                current_segment_id: recovered_next_segment_id,
                local_durable_seq: recovered_sequence,
                cloud_durable_seq: recovered_sequence,
                ..WalState::default()
            },
            compaction: CompactionState::default(),
            cloud: CloudState::default(),
            snapshots: SnapshotState {
                active_snapshots: HashMap::new(),
                max_snapshot_lifetime: std::time::Duration::from_hours(1), // 1 hour default
            },
            recent_delete_ranges: Vec::new(),
            memtable_size_limit: 64 * 1024 * 1024, // 64MB
            read_only: false,
            memory_mode,
            recovery_policy,
            opened_in_salvage_mode,
            persistence_anomaly_detected: false,
            intent_log,
            memtable_flush_threshold: 64 * 1024 * 1024, // 64MB
            cloud_eventual_flush_segment_gap: crate::runtime::CloudRuntimePolicy::default()
                .eventual_flush_segment_gap,
            max_immutable_memtables: 10, // Hard limit on immutable memtable queue
            write_stalled: false,
            total_memtable_bytes: 0,
            enable_compaction: !memory_mode,
            read_amp_metrics: ReadAmpMetrics::new(),
            wal_recovery_records_replayed,
            wal_recovery_bytes_replayed,
            intent_log_replay_runs: 0,
            intent_log_entries_replayed: 0,
            ingest_epoch: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            active_compactions: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            active_compactions_notify: std::sync::Arc::new((
                parking_lot::Mutex::new(()),
                parking_lot::Condvar::new(),
            )),
            ingest_active: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            pending_compaction_waits: parking_lot::Mutex::new(std::collections::HashMap::new()),
        };
        state.reinitialize_active_memtable_segment_tracking();
        Ok(state)
    }

    pub fn health(&self) -> crate::config::EngineHealth {
        let residue = self.storage_residue_assessment();
        crate::storage::residue::classify_engine_health(crate::storage::residue::HealthInputs {
            opened_in_salvage_mode: self.opened_in_salvage_mode,
            write_stalled: self.has_any_hard_write_stall(),
            persistence_anomaly_detected: self.persistence_anomaly_detected,
            pending_intents: self.intent_log.len(),
            orphan_ssts: residue.orphan_ssts.len(),
        })
    }

    pub fn runtime_metrics_snapshot(&self) -> crate::types::RuntimeMetricsSnapshot {
        let now = Instant::now();
        let pinned_ssts = self.get_pinned_sst_names().len();
        let oldest_snapshot_age_seconds = self
            .snapshots
            .active_snapshots
            .values()
            .map(|(_sequence, created_at, _ref_count, _pinned_ssts)| {
                now.duration_since(*created_at).as_secs()
            })
            .max()
            .unwrap_or(0);

        let active_memtables = self.column_families.len();
        let immutable_memtables = self
            .column_families
            .values()
            .map(|cf| cf.immutable_memtables.len())
            .sum();
        let sst_count = self.manifest.files.len();
        let sst_bytes = self.manifest.files.iter().map(|file| file.size_bytes).sum();
        let residue = self.storage_residue_assessment();
        let telemetry = crate::telemetry::Telemetry::global().map(|t| t.metrics().snapshot());

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
            active_snapshots: self.snapshots.active_snapshots.len(),
            pinned_ssts,
            oldest_snapshot_age_seconds,
            sst_count,
            sst_bytes,
            salvage_mode_opens: u64::from(self.opened_in_salvage_mode),
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
            wal_append_count: telemetry.as_ref().map_or(0, |m| m.wal_append_count),
            wal_flush_count: telemetry.as_ref().map_or(0, |m| m.wal_flush_count),
            wal_fsync_count: telemetry.as_ref().map_or(0, |m| m.wal_fsync_count),
            wal_append_ns_total: telemetry.as_ref().map_or(0, |m| m.wal_append_ns_total),
            wal_fsync_ns_total: telemetry.as_ref().map_or(0, |m| m.wal_fsync_ns_total),
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

        let now = Instant::now();
        let mut active_snapshots: Vec<_> = self
            .snapshots
            .active_snapshots
            .iter()
            .map(
                |(snapshot_id, (sequence, created_at, ref_count, _pinned_ssts))| {
                    crate::types::SnapshotPinSnapshot {
                        snapshot_id: *snapshot_id,
                        sequence: *sequence,
                        age_seconds: now.duration_since(*created_at).as_secs(),
                        ref_count: *ref_count,
                    }
                },
            )
            .collect();
        active_snapshots.sort_by_key(|snapshot| snapshot.snapshot_id);
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

    pub fn mark_persistence_anomaly(&mut self) {
        self.persistence_anomaly_detected = true;
    }

    fn storage_residue_assessment(&self) -> crate::storage::residue::StorageResidueAssessment {
        if self.memory_mode {
            return crate::storage::residue::StorageResidueAssessment::default();
        }

        crate::storage::residue::StorageResidueAssessment::scan_sst_dir(
            &self.db_path,
            &self.manifest,
        )
    }

    pub fn cleanup_storage_residue(&mut self) {
        if self.memory_mode {
            return;
        }

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
                fail::eval("midge::recovery::inject_orphan_sst_delete_failure", |_| {
                    true
                })
                .unwrap_or(false);
            if injected_delete_failure {
                self.persistence_anomaly_detected = true;
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
                    self.persistence_anomaly_detected = true;
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

    /// Check if we have cached sequences for this `request_id`.
    /// Returns (`first_sequence`, count) if found and not yet confirmed.
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

    /// Get current size of idempotency cache (for telemetry)
    pub fn idempotency_cache_size(&self) -> usize {
        self.sequence_idempotency_cache.len()
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

    /// Append an intent entry and persist the intent log to disk.
    pub fn append_intent(&mut self, entry: crate::runtime::IntentLogEntry) -> MidgeResult<()> {
        self.intent_log.push(entry);
        // Persist intent log unless running in memory mode
        if !self.memory_mode {
            crate::runtime::IntentPersistence::save(&self.db_path, &self.intent_log)
                .map_err(crate::common::MidgeError::Internal)?;
        }
        Ok(())
    }

    /// Append an intent entry WITHOUT persisting (used for batched writes).
    /// Caller MUST call `persist_intent_log()` after all batch entries are added.
    #[inline]
    pub fn append_intent_deferred(&mut self, entry: crate::runtime::IntentLogEntry) {
        self.intent_log.push(entry);
    }

    /// Persist the intent log to disk (call after batched `append_intent_deferred` calls).
    pub fn persist_intent_log(&self) -> MidgeResult<()> {
        if !self.memory_mode {
            crate::runtime::IntentPersistence::save(&self.db_path, &self.intent_log)
                .map_err(crate::common::MidgeError::Internal)?;
        }
        Ok(())
    }

    pub fn record_compaction_publication_intent(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        removed: Vec<String>,
        added: Vec<crate::runtime::FileMeta>,
    ) -> MidgeResult<()> {
        self.intent_log.retain(|entry| {
            !matches!(
                entry,
                crate::runtime::IntentLogEntry::CompactionPublish {
                    added: existing_added,
                    ..
                } if Self::same_file_name_set(
                    existing_added.iter().map(|meta| meta.name.as_str()),
                    added.iter().map(|meta| meta.name.as_str()),
                )
            )
        });
        self.intent_log
            .push(crate::runtime::IntentLogEntry::CompactionPublish {
                phase: PublicationPhase::OutputDurable,
                cf_id,
                removed,
                added,
            });
        self.persist_intent_log()
    }

    pub fn transition_flush_publication_intent(
        &mut self,
        sst_name: &str,
        phase: PublicationPhase,
    ) -> MidgeResult<()> {
        for entry in &mut self.intent_log {
            if let crate::runtime::IntentLogEntry::FlushPublish {
                phase: current_phase,
                file_meta,
                ..
            } = entry
            {
                if file_meta.name == sst_name {
                    *current_phase = phase;
                }
            }
        }
        self.persist_intent_log()
    }

    pub fn clear_flush_publication_intent(&mut self, sst_name: &str) -> MidgeResult<()> {
        self.intent_log.retain(|entry| {
            !matches!(
                entry,
                crate::runtime::IntentLogEntry::FlushPublish { file_meta, .. }
                    if file_meta.name == sst_name
            )
        });
        self.persist_intent_log()
    }

    pub fn transition_compaction_publication_intent(
        &mut self,
        output_ssts: &[String],
        phase: PublicationPhase,
    ) -> MidgeResult<()> {
        for entry in &mut self.intent_log {
            if let crate::runtime::IntentLogEntry::CompactionPublish {
                phase: current_phase,
                added,
                ..
            } = entry
            {
                if Self::same_file_name_set(
                    added.iter().map(|meta| meta.name.as_str()),
                    output_ssts.iter().map(String::as_str),
                ) {
                    *current_phase = phase;
                }
            }
        }
        self.persist_intent_log()
    }

    pub fn clear_compaction_publication_intent(
        &mut self,
        output_ssts: &[String],
    ) -> MidgeResult<()> {
        self.intent_log.retain(|entry| {
            !matches!(
                entry,
                crate::runtime::IntentLogEntry::CompactionPublish { added, .. }
                    if Self::same_file_name_set(
                        added.iter().map(|meta| meta.name.as_str()),
                        output_ssts.iter().map(String::as_str),
                    )
            )
        });
        self.persist_intent_log()
    }

    fn same_file_name_set<'a, I, J>(left: I, right: J) -> bool
    where
        I: Iterator<Item = &'a str>,
        J: Iterator<Item = &'a str>,
    {
        let mut left_names: Vec<_> = left.collect();
        let mut right_names: Vec<_> = right.collect();
        left_names.sort_unstable();
        right_names.sort_unstable();
        left_names == right_names
    }

    fn manifest_has_file(&self, sst_name: &str) -> bool {
        self.manifest.files.iter().any(|file| file.name == sst_name)
    }

    fn manifest_reflects_compaction(
        &self,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
    ) -> bool {
        removed.iter().all(|name| !self.manifest_has_file(name))
            && added
                .iter()
                .all(|file_meta| self.manifest_has_file(&file_meta.name))
    }

    fn insert_manifest_file_if_missing(&mut self, file_meta: &crate::runtime::FileMeta) -> bool {
        if self.manifest_has_file(&file_meta.name) {
            return false;
        }

        self.manifest.files.push(crate::metadata::FileMeta {
            name: file_meta.name.clone(),
            level: file_meta.level,
            size_bytes: file_meta.size_bytes,
            content_crc32c: file_meta.content_crc32c,
            cf_id: file_meta.cf_id,
            smallest_key: file_meta.smallest_key.clone(),
            largest_key: file_meta.largest_key.clone(),
            smallest_seq: file_meta.smallest_seq,
            largest_seq: file_meta.largest_seq,
            ..Default::default()
        });
        true
    }

    fn apply_compaction_to_manifest(
        &mut self,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
    ) -> bool {
        let mut changed = false;
        self.manifest.files.retain(|file| {
            let keep = !removed.contains(&file.name);
            if !keep {
                changed = true;
            }
            keep
        });

        for file_meta in added {
            changed |= self.insert_manifest_file_if_missing(file_meta);
        }

        changed
    }

    fn append_manifest_add_sst(&self, file_meta: &crate::runtime::FileMeta) -> MidgeResult<()> {
        if self.memory_mode {
            return Ok(());
        }

        crate::metadata::append_edit(
            &self.db_path,
            &crate::metadata::ManifestEdit::AddSst(crate::metadata::FileMeta {
                name: file_meta.name.clone(),
                level: file_meta.level,
                size_bytes: file_meta.size_bytes,
                content_crc32c: file_meta.content_crc32c,
                cf_id: file_meta.cf_id,
                smallest_key: file_meta.smallest_key.clone(),
                largest_key: file_meta.largest_key.clone(),
                smallest_seq: file_meta.smallest_seq,
                largest_seq: file_meta.largest_seq,
                ..Default::default()
            }),
        )
    }

    fn append_manifest_compaction_batch(
        &self,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
    ) -> MidgeResult<()> {
        if self.memory_mode {
            return Ok(());
        }

        let mut edits = Vec::with_capacity(removed.len() + added.len());
        for name in removed {
            edits.push(crate::metadata::ManifestEdit::RemoveSst { name: name.clone() });
        }
        for file_meta in added {
            edits.push(crate::metadata::ManifestEdit::AddSst(
                crate::metadata::FileMeta {
                    name: file_meta.name.clone(),
                    level: file_meta.level,
                    size_bytes: file_meta.size_bytes,
                    content_crc32c: file_meta.content_crc32c,
                    cf_id: file_meta.cf_id,
                    smallest_key: file_meta.smallest_key.clone(),
                    largest_key: file_meta.largest_key.clone(),
                    smallest_seq: file_meta.smallest_seq,
                    largest_seq: file_meta.largest_seq,
                    ..Default::default()
                },
            ));
        }

        if edits.is_empty() {
            return Ok(());
        }

        crate::metadata::append_edit_batch(&self.db_path, &edits)
    }

    fn persist_manifest_checkpoint(&self) -> MidgeResult<()> {
        if self.memory_mode {
            return Ok(());
        }

        crate::metadata::ManifestPersistence::save(&self.db_path, &self.manifest)
            .map_err(crate::common::MidgeError::Internal)
    }

    fn handle_recovery_issue(&mut self, message: String) -> MidgeResult<bool> {
        if self.recovery_policy == crate::config::RecoveryPolicy::Strict {
            return Err(MidgeError::RecoveryFailed(message));
        }

        self.opened_in_salvage_mode = true;
        self.persistence_anomaly_detected = true;
        tracing::warn!("{}", message);
        Ok(false)
    }

    fn validate_recovered_sst(&mut self, sst_name: &str) -> MidgeResult<bool> {
        let path = self.sst_dir.join(sst_name);
        if !path.exists() {
            return self.handle_recovery_issue(format!(
                "recovery intent references missing SST '{sst_name}'"
            ));
        }

        match crate::sst::fs::SstFileIo::open_with_real_fs(&path) {
            Ok(_) => Ok(true),
            Err(error) => self.handle_recovery_issue(format!(
                "recovery intent references invalid SST '{sst_name}': {error}"
            )),
        }
    }

    fn delete_sst_if_exists(&mut self, sst_name: &str) -> MidgeResult<()> {
        let path = self.sst_dir.join(sst_name);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                if self.recovery_policy == crate::config::RecoveryPolicy::Strict {
                    Err(MidgeError::RecoveryFailed(format!(
                        "failed to delete SST '{sst_name}' during recovery: {error}"
                    )))
                } else {
                    self.opened_in_salvage_mode = true;
                    self.persistence_anomaly_detected = true;
                    tracing::warn!(
                        sst_name,
                        error = %error,
                        "failed to delete SST during salvage recovery"
                    );
                    Ok(())
                }
            }
        }
    }

    fn replay_flush_publish_intent(
        &mut self,
        phase: PublicationPhase,
        file_meta: &crate::runtime::FileMeta,
    ) -> MidgeResult<bool> {
        if self.manifest_has_file(&file_meta.name) {
            tracing::info!(sst_name = %file_meta.name, "flush publish intent already reflected in manifest");
            return Ok(false);
        }

        match phase {
            PublicationPhase::OutputDurable => {
                let path = self.sst_dir.join(&file_meta.name);
                if path.exists() {
                    if !self.validate_recovered_sst(&file_meta.name)? {
                        return Ok(false);
                    }
                    self.delete_sst_if_exists(&file_meta.name)?;
                    tracing::info!(
                        sst_name = %file_meta.name,
                        "deleted orphan flush SST left behind before manifest publication"
                    );
                } else {
                    tracing::info!(
                        sst_name = %file_meta.name,
                        "flush publication intent references no remaining SST; treating it as an already-cleaned orphan"
                    );
                }
                Ok(false)
            }
            PublicationPhase::ManifestPublished => {
                if !self.validate_recovered_sst(&file_meta.name)? {
                    return Ok(false);
                }

                self.append_manifest_add_sst(file_meta)?;
                let changed = self.insert_manifest_file_if_missing(file_meta);
                tracing::info!(
                    sst_name = %file_meta.name,
                    "replayed manifest-published flush intent"
                );
                Ok(changed)
            }
        }
    }

    fn replay_compaction_publish_intent(
        &mut self,
        phase: PublicationPhase,
        removed: &[String],
        added: &[crate::runtime::FileMeta],
    ) -> MidgeResult<bool> {
        for file_meta in added {
            if !self.validate_recovered_sst(&file_meta.name)? {
                return Ok(false);
            }
        }

        let mut manifest_changed = false;
        if !self.manifest_reflects_compaction(removed, added) {
            self.append_manifest_compaction_batch(removed, added)?;
            manifest_changed |= self.apply_compaction_to_manifest(removed, added);
            tracing::info!(
                removed_count = removed.len(),
                added_count = added.len(),
                "replayed compaction publication into manifest"
            );
        }

        if phase == PublicationPhase::ManifestPublished
            || self.manifest_reflects_compaction(removed, added)
        {
            for sst_name in removed {
                self.delete_sst_if_exists(sst_name)?;
            }
        }

        Ok(manifest_changed)
    }

    /// Replay intent log to recover incomplete mutations
    /// Called during startup to apply any interrupted manifest or durability changes
    pub fn replay_intent_log(&mut self) -> MidgeResult<()> {
        if self.intent_log.is_empty() {
            return Ok(());
        }

        if let Some(t) = crate::telemetry::Telemetry::global() {
            t.metrics()
                .record_intent_log_replay(self.intent_log.len() as u64);
        }

        self.intent_log_replay_runs = self.intent_log_replay_runs.saturating_add(1);
        self.intent_log_entries_replayed = self
            .intent_log_entries_replayed
            .saturating_add(self.intent_log.len() as u64);

        tracing::info!(
            intent_count = self.intent_log.len(),
            "replaying intent log during recovery"
        );

        let intents = self.intent_log.clone();
        let mut manifest_changed = false;

        for intent in &intents {
            match intent {
                crate::runtime::IntentLogEntry::FlushPublish {
                    phase, file_meta, ..
                } => {
                    manifest_changed |= self.replay_flush_publish_intent(*phase, file_meta)?;
                }
                crate::runtime::IntentLogEntry::CompactionPublish {
                    phase,
                    removed,
                    added,
                    ..
                } => {
                    manifest_changed |=
                        self.replay_compaction_publish_intent(*phase, removed, added)?;
                }
                crate::runtime::IntentLogEntry::CompactionApplied { removed, added } => {
                    manifest_changed |= self.replay_compaction_publish_intent(
                        PublicationPhase::ManifestPublished,
                        removed,
                        added,
                    )?;
                }
                crate::runtime::IntentLogEntry::SstAdded { file_meta } => {
                    manifest_changed |= self.replay_flush_publish_intent(
                        PublicationPhase::ManifestPublished,
                        file_meta,
                    )?;
                }
                _ => {
                    // Other intents (WalSynced, DataUploaded, etc.) don't require replay
                    // They are informational and don't affect the recoverable state
                }
            }
        }

        if manifest_changed {
            if let Err(error) = self.persist_manifest_checkpoint() {
                let _ = self.handle_recovery_issue(format!(
                    "failed to persist manifest checkpoint after intent replay: {error}"
                ))?;
            }
        }

        self.restore_sequence_floor_from_manifest();

        // Clear the intent log after successful replay
        // New intents will be written during normal operation
        self.intent_log.clear();
        if !self.memory_mode {
            if let Err(error) =
                crate::runtime::IntentPersistence::save(&self.db_path, &self.intent_log)
            {
                let _ = self.handle_recovery_issue(format!(
                    "failed to clear intent log after replay: {error}"
                ))?;
            }
        }

        tracing::info!("intent log replay complete and cleared");
        Ok(())
    }

    /// Register a new snapshot to prevent SST garbage collection
    /// while the snapshot is active and performing range scans.
    ///
    /// Returns true if snapshot was registered successfully.
    /// Returns false if `snapshot_id` already exists (duplicate registration).
    pub fn register_snapshot(
        &mut self,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    ) -> bool {
        if self.snapshots.active_snapshots.contains_key(&snapshot_id) {
            tracing::warn!(snapshot_id, "Attempted to register duplicate snapshot ID");
            return false;
        }

        let pinned_ssts = pinned_sst_names.into_iter().collect::<HashSet<_>>();

        self.snapshots
            .active_snapshots
            .insert(snapshot_id, (sequence, Instant::now(), 1, pinned_ssts));
        self.prune_recent_delete_ranges_by_snapshot_horizon();

        tracing::trace!(snapshot_id, sequence, "Snapshot registered for SST pinning");

        true
    }

    /// Unregister a snapshot, allowing SSTs referenced by it to be garbage collected.
    pub fn unregister_snapshot(&mut self, snapshot_id: u64) {
        if self
            .snapshots
            .active_snapshots
            .remove(&snapshot_id)
            .is_some()
        {
            self.prune_recent_delete_ranges_by_snapshot_horizon();
            tracing::trace!(snapshot_id, "Snapshot unregistered; SSTs eligible for GC");
        }
    }

    /// Get list of SSTs referenced by active snapshots.
    /// These SSTs must NOT be deleted during garbage collection.
    pub fn get_pinned_sst_names(&self) -> HashSet<String> {
        let mut pinned = HashSet::new();

        for (snapshot_id, (_snapshot_seq, created_at, _ref_count, pinned_ssts)) in
            &self.snapshots.active_snapshots
        {
            // Check if snapshot has exceeded max lifetime
            let age = Instant::now().duration_since(*created_at);
            if age > self.snapshots.max_snapshot_lifetime {
                tracing::warn!(
                    snapshot_id,
                    age_secs = age.as_secs(),
                    max_secs = self.snapshots.max_snapshot_lifetime.as_secs(),
                    "Long-lived snapshot exceeds max lifetime (should be auto-closed)"
                );
                // Note: we don't auto-close here; that's for the caller to decide
                // but we log it for alerting
            }

            pinned.extend(pinned_ssts.iter().cloned());
        }

        pinned
    }

    /// Check for timed-out snapshots and return count
    pub fn count_timed_out_snapshots(&self) -> usize {
        self.snapshots
            .active_snapshots
            .iter()
            .filter(|(_id, (_seq, created_at, _ref_count, _pinned_ssts))| {
                Instant::now().duration_since(*created_at) > self.snapshots.max_snapshot_lifetime
            })
            .count()
    }

    /// Remove snapshots that exceeded max lifetime and return the number evicted.
    pub fn evict_timed_out_snapshots(&mut self) -> usize {
        let now = Instant::now();
        let max_lifetime = self.snapshots.max_snapshot_lifetime;

        let mut timed_out_ids = Vec::new();
        for (snapshot_id, (_seq, created_at, _ref_count, _pinned_ssts)) in
            &self.snapshots.active_snapshots
        {
            if now.duration_since(*created_at) > max_lifetime {
                timed_out_ids.push(*snapshot_id);
            }
        }

        let timed_out_count = timed_out_ids.len();
        for snapshot_id in timed_out_ids {
            self.snapshots.active_snapshots.remove(&snapshot_id);
            tracing::warn!(
                snapshot_id,
                max_secs = max_lifetime.as_secs(),
                "Evicted timed-out snapshot"
            );
        }

        if timed_out_count > 0 {
            self.prune_recent_delete_ranges_by_snapshot_horizon();
        }

        timed_out_count
    }

    /// Return the oldest active snapshot sequence, if any snapshots are pinned.
    pub fn oldest_active_snapshot_sequence(&self) -> Option<u64> {
        self.snapshots
            .active_snapshots
            .values()
            .map(|(sequence, _created_at, _ref_count, _pinned_ssts)| *sequence)
            .min()
    }

    pub fn record_delete_range(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        start_key: &[u8],
        end_key: &[u8],
        sequence: u64,
    ) {
        self.recent_delete_ranges.push(RecentDeleteRange {
            cf_id,
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
            sequence,
        });

        let overflow = self
            .recent_delete_ranges
            .len()
            .saturating_sub(MAX_RECENT_DELETE_RANGES);
        if overflow > 0 {
            self.recent_delete_ranges.drain(0..overflow);
        }

        self.prune_recent_delete_ranges_by_snapshot_horizon();
    }

    fn prune_recent_delete_ranges_by_snapshot_horizon(&mut self) {
        let Some(oldest_snapshot_seq) = self.oldest_active_snapshot_sequence() else {
            self.recent_delete_ranges.clear();
            return;
        };

        self.recent_delete_ranges
            .retain(|entry| entry.sequence > oldest_snapshot_seq);
    }

    pub fn latest_covering_delete_range_sequence(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
    ) -> Option<u64> {
        self.recent_delete_ranges
            .iter()
            .rev()
            .find(|entry| {
                entry.cf_id == cf_id
                    && key >= entry.start_key.as_slice()
                    && key < entry.end_key.as_slice()
            })
            .map(|entry| entry.sequence)
    }

    pub fn latest_overlapping_delete_range_sequence(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<u64> {
        self.recent_delete_ranges
            .iter()
            .rev()
            .find(|entry| {
                entry.cf_id == cf_id
                    && entry.start_key.as_slice() < end_key
                    && entry.end_key.as_slice() > start_key
            })
            .map(|entry| entry.sequence)
    }

    pub fn get_cf(&self, cf_id: crate::types::ColumnFamilyId) -> Option<&ColumnFamilyState> {
        self.column_families.get(&cf_id)
    }

    pub fn get_cf_mut(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> Option<&mut ColumnFamilyState> {
        self.column_families.get_mut(&cf_id)
    }

    pub fn create_cf(&mut self, name: String) -> MidgeResult<u32> {
        let id = u32::try_from(self.column_families.len()).map_err(|_| {
            crate::common::MidgeError::Internal("too many column families".to_string())
        })?;
        self.column_families
            .insert(id, ColumnFamilyState::new(id, name));
        Ok(id)
    }

    pub fn needs_flush(&self) -> Option<u32> {
        self.next_flush_candidate(false)
            .map(|candidate| candidate.cf_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grow_active_memtable(
        state: &mut RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        target_bytes: usize,
    ) {
        let mut seq = 1_u64;
        while state
            .get_cf(cf_id)
            .expect("column family")
            .memtable
            .size_bytes()
            < target_bytes
        {
            let cf = state.get_cf(cf_id).expect("column family");
            cf.memtable
                .put_with_seq(
                    format!("key_{seq:06}").into_bytes(),
                    vec![0xA5; 128],
                    seq,
                    None,
                )
                .expect("seed memtable");
            seq += 1;
        }

        state.total_memtable_bytes = state
            .get_cf(cf_id)
            .expect("column family")
            .memtable
            .size_bytes();
    }

    // =========== ColumnFamilyState Tests ===========

    #[test]
    fn should_create_column_family_with_unique_id() {
        // Arrange
        let id = 42;
        let name = "test_cf".to_string();

        // Act
        let cf = ColumnFamilyState::new(id, name.clone());

        // Assert
        assert_eq!(cf.id, 42);
        assert_eq!(cf.name, "test_cf");
        assert!(cf.immutable_memtables.is_empty());
    }

    #[test]
    fn should_track_immutable_memtables_in_cf_state() {
        // Arrange
        let mut cf = ColumnFamilyState::new(1, "cf".to_string());
        let imm_memtable = Arc::new(SkipListMemtable::new());

        // Act
        cf.immutable_memtables.push(imm_memtable.clone());
        cf.immutable_memtables.push(imm_memtable.clone());

        // Assert
        assert_eq!(cf.immutable_memtables.len(), 2);
    }

    #[test]
    fn should_select_size_threshold_flush_candidate_before_cloud_gap_candidate() {
        let mut state = RuntimeState::new("/tmp/test_midge".into(), false);
        state.memtable_flush_threshold = 4 * 1024;
        state.memtable_size_limit = 1024 * 1024;
        state.wal.current_segment_id = state.cloud_eventual_flush_segment_gap + 50;

        grow_active_memtable(&mut state, 0, 5 * 1024);

        let other_cf_id = state
            .create_cf("other".to_string())
            .expect("create other cf");
        {
            let cf_state = state.get_cf(other_cf_id).expect("other cf");
            cf_state
                .memtable
                .put_with_seq(b"other".to_vec(), b"value".to_vec(), 1, None)
                .expect("seed other cf");
        }
        state
            .get_cf_mut(other_cf_id)
            .expect("other cf")
            .active_memtable_started_in_segment = 1;

        let candidate = state
            .next_flush_candidate(true)
            .expect("flush candidate should exist");
        assert_eq!(candidate.cf_id, 0);
        assert_eq!(candidate.reason, FlushReason::SizeThreshold);
    }

    #[test]
    fn should_select_cloud_gap_flush_candidate_when_cloud_mode_and_gap_exceeded() {
        let mut state = RuntimeState::new("/tmp/test_midge".into(), false);
        state.memtable_flush_threshold = 1024 * 1024;
        state.memtable_size_limit = 1024 * 1024;
        state.wal.current_segment_id = state.cloud_eventual_flush_segment_gap + 1;

        {
            let cf_state = state.get_cf(0).expect("default cf");
            cf_state
                .memtable
                .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)
                .expect("seed default cf");
        }
        state
            .get_cf_mut(0)
            .expect("default cf")
            .active_memtable_started_in_segment = 1;

        let candidate = state
            .next_flush_candidate(true)
            .expect("cloud gap flush candidate should exist");
        assert_eq!(candidate.cf_id, 0);
        assert_eq!(candidate.reason, FlushReason::CloudSegmentGap);
    }

    #[test]
    fn should_not_select_cloud_gap_flush_candidate_when_gap_mode_disabled() {
        let mut state = RuntimeState::new("/tmp/test_midge".into(), false);
        state.memtable_flush_threshold = 1024 * 1024;
        state.memtable_size_limit = 1024 * 1024;
        state.wal.current_segment_id = state.cloud_eventual_flush_segment_gap + 10;

        {
            let cf_state = state.get_cf(0).expect("default cf");
            cf_state
                .memtable
                .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)
                .expect("seed default cf");
        }
        state
            .get_cf_mut(0)
            .expect("default cf")
            .active_memtable_started_in_segment = 1;

        assert!(state.next_flush_candidate(false).is_none());
    }

    #[test]
    fn should_report_max_memtable_wal_segment_gap_for_non_empty_memtables() {
        let mut state = RuntimeState::new("/tmp/test_midge".into(), false);
        state.wal.current_segment_id = state.cloud_eventual_flush_segment_gap + 20;

        {
            let cf_state = state.get_cf(0).expect("default cf");
            cf_state
                .memtable
                .put_with_seq(b"default".to_vec(), b"value".to_vec(), 1, None)
                .expect("seed default cf");
        }
        state
            .get_cf_mut(0)
            .expect("default cf")
            .active_memtable_started_in_segment = 10;

        let cf_id = state.create_cf("secondary".to_string()).expect("create cf");
        {
            let cf_state = state.get_cf(cf_id).expect("secondary cf");
            cf_state
                .memtable
                .put_with_seq(b"secondary".to_vec(), b"value".to_vec(), 2, None)
                .expect("seed secondary cf");
        }
        state
            .get_cf_mut(cf_id)
            .expect("secondary cf")
            .active_memtable_started_in_segment = 1;

        assert_eq!(
            state.max_memtable_wal_segment_gap(),
            state.wal.current_segment_id.saturating_sub(1)
        );
    }

    #[test]
    fn should_flush_but_not_hard_stall_when_active_memtable_exceeds_flush_threshold() {
        let mut state = RuntimeState::new("/tmp/test_midge".into(), false);
        state.memtable_size_limit = 1024 * 1024;
        state.memtable_flush_threshold = 4 * 1024;
        state.total_memtable_bytes = 0;

        grow_active_memtable(&mut state, 0, 5 * 1024);

        assert_eq!(state.needs_flush(), Some(0));
        assert!(!state.should_hard_stall_writes(0));
        assert!(!state.should_stall_writes(0));
        assert!(!state.has_any_hard_write_stall());
    }

    #[test]
    fn should_hard_stall_when_immutable_memtable_queue_is_full() {
        let mut state = RuntimeState::new("/tmp/test_midge".into(), false);
        state.max_immutable_memtables = 1;
        state
            .get_cf_mut(0)
            .expect("default cf")
            .immutable_memtables
            .push(Arc::new(SkipListMemtable::new()));

        assert!(state.is_immutable_memtable_queue_full(0));
        assert!(state.should_hard_stall_writes(0));
        assert!(state.has_any_hard_write_stall());
    }

    #[test]
    fn should_hard_stall_when_total_memtable_memory_exceeds_limit() {
        let mut state = RuntimeState::new("/tmp/test_midge".into(), false);
        state.memtable_flush_threshold = 1024;
        state.total_memtable_bytes = 2 * 1024;

        assert!(state.is_total_memtable_hard_limit_exceeded());
        assert!(state.should_hard_stall_writes(0));
        assert!(state.has_any_hard_write_stall());
    }

    #[test]
    fn should_hard_stall_when_external_backpressure_sets_write_stalled() {
        let mut state = RuntimeState::new("/tmp/test_midge".into(), false);
        state.write_stalled = true;

        assert!(state.should_hard_stall_writes(0));
        assert!(state.has_any_hard_write_stall());
        assert!(state.runtime_metrics_snapshot().write_stalled);
    }

    // =========== WalState Tests ===========

    #[test]
    fn should_initialize_wal_state_with_defaults() {
        // Arrange
        // (no setup)

        // Act
        let wal = WalState::default();

        // Assert
        assert_eq!(wal.current_segment_id, 1);
        assert_eq!(wal.last_synced_seq, 0);
        assert_eq!(wal.pending_writes, 0);
        assert_eq!(wal.local_durable_seq, 0);
        assert_eq!(wal.cloud_durable_seq, 0);
    }

    #[test]
    fn should_maintain_wal_durability_frontiers() {
        // Arrange
        let wal = WalState {
            last_synced_seq: 10,
            local_durable_seq: 10,
            pending_writes: 5,
            cloud_durable_seq: 8,
            ..Default::default()
        };

        // Act
        // (none)

        // Assert - Verify monotonicity constraints
        assert!(wal.cloud_durable_seq <= wal.local_durable_seq);
        assert!(wal.local_durable_seq >= wal.last_synced_seq);
        assert!(wal.pending_writes < usize::MAX);
    }

    #[test]
    fn should_track_segment_rotation() {
        // Arrange
        let mut wal = WalState::default();
        let initial_segment = wal.current_segment_id;

        // Act
        wal.current_segment_id += 1;
        wal.current_segment_id += 1;

        // Assert
        assert_eq!(wal.current_segment_id, initial_segment + 2);
    }

    // =========== CompactionState Tests ===========

    #[test]
    fn should_initialize_compaction_state() {
        // Arrange
        // (no setup)

        // Act
        let compaction = CompactionState::default();

        // Assert
        assert!(compaction.compacting_ssts.is_empty());
        assert_eq!(compaction.pending_tasks, 0);
    }

    #[test]
    fn should_track_compacting_ssts() {
        // Arrange
        let mut compaction = CompactionState::default();

        // Act
        compaction
            .compacting_ssts
            .push(crate::sst::file_name(0, 0, 1));
        compaction
            .compacting_ssts
            .push(crate::sst::file_name(0, 0, 2));
        compaction.pending_tasks = 2;

        // Assert
        assert_eq!(compaction.compacting_ssts.len(), 2);
        assert_eq!(compaction.pending_tasks, 2);
    }

    // =========== CloudState Tests ===========

    #[test]
    fn should_initialize_cloud_state() {
        // Arrange
        // (no setup)

        // Act
        let cloud = CloudState::default();

        // Assert
        assert!(cloud.pending_uploads.is_empty());
        assert_eq!(cloud.last_cloud_checkpoint_seq, 0);
    }

    #[test]
    fn should_track_pending_uploads() {
        // Arrange
        let mut cloud = CloudState::default();

        // Act
        cloud.pending_uploads.push(crate::sst::file_name(0, 0, 1));
        cloud.last_cloud_checkpoint_seq = 100;

        // Assert
        assert_eq!(cloud.pending_uploads.len(), 1);
        assert_eq!(cloud.last_cloud_checkpoint_seq, 100);
    }

    // =========== RuntimeState Tests ===========

    #[test]
    fn should_create_runtime_state_in_memory_mode() {
        // Arrange
        // (no setup)

        // Act
        let state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Assert
        assert!(state.memory_mode);
        assert_eq!(state.sequence, 0);
        assert_eq!(state.next_txn_id, 0);
        assert!(state.column_families.contains_key(&0)); // Default CF
        assert_eq!(state.column_families.len(), 1);
    }

    #[test]
    fn should_initialize_default_column_family() {
        // Arrange
        // (no setup)

        // Act
        let state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Assert
        let cf0 = state.get_cf(0).expect("Default CF should exist");
        assert_eq!(cf0.id, 0);
        assert_eq!(cf0.name, "default");
    }

    #[test]
    fn should_evict_timed_out_snapshots_when_enforcement_runs() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.snapshots.max_snapshot_lifetime = std::time::Duration::from_millis(0);
        assert!(state.register_snapshot(1, 10, vec!["001.sst".to_string()]));
        assert_eq!(state.snapshots.active_snapshots.len(), 1);

        // Act
        let evicted = state.evict_timed_out_snapshots();

        // Assert
        assert_eq!(evicted, 1);
        assert!(state.snapshots.active_snapshots.is_empty());
    }

    #[test]
    fn should_preserve_non_expired_snapshots_when_enforcement_runs() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.snapshots.max_snapshot_lifetime = std::time::Duration::from_hours(1);
        assert!(state.register_snapshot(2, 11, vec!["002.sst".to_string()]));

        // Act
        let evicted = state.evict_timed_out_snapshots();

        // Assert
        assert_eq!(evicted, 0);
        assert_eq!(state.snapshots.active_snapshots.len(), 1);
    }

    #[test]
    fn should_prune_recent_delete_ranges_when_no_active_snapshots() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state.record_delete_range(0, b"a", b"z", 10);

        // Act
        state.prune_recent_delete_ranges_by_snapshot_horizon();

        // Assert
        assert!(state.recent_delete_ranges.is_empty());
    }

    #[test]
    fn should_retain_only_delete_ranges_newer_than_oldest_snapshot_when_pruning() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        assert!(state.register_snapshot(100, 20, vec![]));
        assert!(state.register_snapshot(101, 40, vec![]));

        state.recent_delete_ranges.push(RecentDeleteRange {
            cf_id: 0,
            start_key: b"a".to_vec(),
            end_key: b"c".to_vec(),
            sequence: 10,
        });
        state.recent_delete_ranges.push(RecentDeleteRange {
            cf_id: 0,
            start_key: b"c".to_vec(),
            end_key: b"e".to_vec(),
            sequence: 21,
        });
        state.recent_delete_ranges.push(RecentDeleteRange {
            cf_id: 0,
            start_key: b"e".to_vec(),
            end_key: b"g".to_vec(),
            sequence: 35,
        });

        // Act
        state.prune_recent_delete_ranges_by_snapshot_horizon();

        // Assert
        assert_eq!(state.recent_delete_ranges.len(), 2);
        assert!(state
            .recent_delete_ranges
            .iter()
            .all(|entry| entry.sequence > 20));
    }

    #[test]
    fn should_increment_sequence_numbers_monotonically() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        let initial = state.sequence;

        // Act
        let seq1 = state.next_sequence();
        let seq2 = state.next_sequence();
        let seq3 = state.next_sequence();

        // Assert
        assert_eq!(seq1, initial + 1);
        assert_eq!(seq2, initial + 2);
        assert_eq!(seq3, initial + 3);
        assert!(seq1 < seq2 && seq2 < seq3);
    }

    #[test]
    fn should_increment_transaction_ids_monotonically() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        let txn1 = state.next_txn_id();
        let txn2 = state.next_txn_id();
        let txn3 = state.next_txn_id();

        // Assert
        assert_eq!(txn1, 1);
        assert_eq!(txn2, 2);
        assert_eq!(txn3, 3);
        assert!(txn1 < txn2 && txn2 < txn3);
    }

    #[test]
    fn should_create_new_column_family() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        let cf_id = state
            .create_cf("test_cf".to_string())
            .expect("create_cf should succeed");

        // Assert
        assert_eq!(cf_id, 1); // After default (0)
        assert!(state.column_families.contains_key(&cf_id));
        let cf = state.get_cf(cf_id).expect("Created CF should exist");
        assert_eq!(cf.name, "test_cf");
    }

    #[test]
    fn should_get_column_family_by_id() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state
            .create_cf("my_cf".to_string())
            .expect("create_cf should succeed");

        // Act
        let cf = state.get_cf(1);

        // Assert
        assert!(cf.is_some());
        assert_eq!(cf.unwrap().name, "my_cf");
    }

    #[test]
    fn should_return_none_for_nonexistent_column_family() {
        // Arrange
        let state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        // (none)

        // Assert
        assert!(state.get_cf(999).is_none());
    }

    #[test]
    fn should_get_mutable_column_family() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);
        state
            .create_cf("mutable_cf".to_string())
            .expect("create_cf should succeed");

        // Act
        {
            let cf_mut = state.get_cf_mut(1).expect("get_cf_mut should succeed");
            cf_mut
                .immutable_memtables
                .push(Arc::new(SkipListMemtable::new()));
        }

        // Assert
        let cf = state.get_cf(1).expect("CF should exist");
        assert_eq!(cf.immutable_memtables.len(), 1);
    }

    #[test]
    fn should_load_intent_log_on_startup() {
        // Arrange: write an intent file to the test dir and then create RuntimeState
        let test_dir = std::env::temp_dir().join("midge_state_intent_test");
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).expect("create test dir");
        crate::metadata::ensure_or_create_format_marker(&test_dir).expect("create format marker");

        let intents = vec![crate::runtime::IntentLogEntry::WalSynced {
            segment_id: 2,
            seqno: 99,
        }];
        crate::runtime::IntentPersistence::save(&test_dir, &intents).expect("save intents");

        // Act: create runtime state for that path (not memory mode)
        let state = RuntimeState::new(test_dir.clone(), false);

        // Assert: intent log was loaded
        assert!(
            !state.intent_log.is_empty(),
            "intent log should be loaded from disk"
        );
        assert!(
            matches!(
                state.intent_log[0],
                crate::runtime::IntentLogEntry::WalSynced {
                    segment_id: 2,
                    seqno: 99
                }
            ),
            "expected WalSynced entry with segment_id=2, seqno=99, got: {:?}",
            state.intent_log[0]
        );
    }

    #[test]
    fn should_track_wal_state_separately() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        state.wal.current_segment_id = 5;
        state.wal.pending_writes = 10;

        // Assert
        assert_eq!(state.wal.current_segment_id, 5);
        assert_eq!(state.wal.pending_writes, 10);
    }

    #[test]
    fn should_track_compaction_state_separately() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        state
            .compaction
            .compacting_ssts
            .push(crate::sst::file_name(0, 0, 1));
        state.compaction.pending_tasks = 3;

        // Assert
        assert_eq!(state.compaction.compacting_ssts.len(), 1);
        assert_eq!(state.compaction.pending_tasks, 3);
    }

    #[test]
    fn should_track_cloud_state_separately() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        state
            .cloud
            .pending_uploads
            .push(crate::sst::file_name(0, 0, 50));
        state.cloud.last_cloud_checkpoint_seq = 50;

        // Assert
        assert_eq!(state.cloud.pending_uploads.len(), 1);
        assert_eq!(state.cloud.last_cloud_checkpoint_seq, 50);
    }

    #[test]
    fn should_maintain_memtable_size_limit() {
        // Arrange
        let state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        // (none)

        // Assert
        assert!(state.memtable_size_limit > 0);
        assert_eq!(state.memtable_size_limit, 64 * 1024 * 1024); // 64MB
    }

    #[test]
    fn should_respect_read_only_flag() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Assert - Initially not read-only
        assert!(!state.read_only);

        // Act - Set read-only
        state.read_only = true;

        // Assert
        assert!(state.read_only);
    }

    #[test]
    fn should_handle_multiple_column_families() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        let cf1 = state
            .create_cf("cf1".to_string())
            .expect("create_cf should succeed");
        let cf2 = state
            .create_cf("cf2".to_string())
            .expect("create_cf should succeed");
        let cf3 = state
            .create_cf("cf3".to_string())
            .expect("create_cf should succeed");

        // Assert
        assert_eq!(state.column_families.len(), 4); // default + 3 created
        assert!(state.get_cf(cf1).is_some());
        assert!(state.get_cf(cf2).is_some());
        assert!(state.get_cf(cf3).is_some());
    }

    #[test]
    fn should_track_all_state_components_independently() {
        // Arrange
        let mut state = RuntimeState::new("/tmp/test_midge".into(), true);

        // Act
        let seq1 = state.next_sequence();
        let txn1 = state.next_txn_id();
        state.wal.pending_writes = 5;
        state.compaction.pending_tasks = 2;
        state.cloud.last_cloud_checkpoint_seq = 100;

        // Assert
        assert_eq!(seq1, 1);
        assert_eq!(txn1, 1);
        assert_eq!(state.wal.pending_writes, 5);
        assert_eq!(state.compaction.pending_tasks, 2);
        assert_eq!(state.cloud.last_cloud_checkpoint_seq, 100);
    }
}
