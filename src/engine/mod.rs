//! Main KV store engine
//!
//! Public API for database operations.
//!
//! The engine provides a transaction-scoped API for all data operations.
//! All reads and writes execute within explicit transactions.
//!
//! Key responsibilities:
//! - Transaction lifecycle (begin_tx, commit, rollback)
//! - Column family management
//! - Flush and compaction control
//! - Metrics and observability
//!
//! Point operations (get, put, delete, scan) are methods on Transaction.
//! Range tombstones are engine-level operations scoped to a column family.

use crate::common::{MidgeError, MidgeResult};
use crate::engine::api::DurabilityPolicy as ApiDurabilityPolicy;
use crate::runtime::{
    next_request_id, Runtime, RuntimeHandle, RuntimeMsg, RuntimeResponse, RuntimeState,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

static IN_MEMORY_OPEN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) mod api;
mod ingest;

pub use api::{
    Direction, Goal, Key, MemoryBudget, OpenOptions, Query, RecoveryPolicy, ScanIterator, Storage,
    Transaction, TransactionMode, Value, WorkloadProfile, WriteOptions,
};
/// Registry of column families, keyed by column family ID
type ColumnFamilyRegistry = dashmap::DashMap<ColumnFamilyId, ColumnFamilyHandle>;

/// Column family identifier — simple u32 alias.
pub type ColumnFamilyId = u32;

/// Column family handle for API operations
#[derive(Debug, Clone)]
pub struct ColumnFamilyHandle {
    id: ColumnFamilyId,
    name: String,
}

/// Snapshot of read amplification metrics
///
/// Provides visibility into read performance characteristics:
/// - How many SSTs are being touched per read
/// - L0 overlap patterns (most expensive)
/// - Budget violation rates
///
/// Use these metrics to:
/// - Monitor read amplification trends
/// - Tune compaction triggers
/// - Identify hot access patterns
#[derive(Debug, Clone)]
pub struct ReadAmpMetricsSnapshot {
    /// Total read operations performed
    pub reads_total: u64,
    /// Total SSTs touched across all reads
    pub ssts_touched_total: u64,
    /// Total L0 SSTs touched (always fully scanned due to overlap)
    pub l0_ssts_touched_total: u64,
    /// Total blocks read across all operations
    pub blocks_read_total: u64,
    /// Average SSTs touched per read
    pub avg_ssts_per_read: f64,
    /// Average L0 SSTs touched per read
    pub avg_l0_ssts_per_read: f64,
    /// Average blocks read per operation
    pub avg_blocks_per_read: f64,
    /// L0 overlap rate (fraction of SST touches that are L0)
    pub l0_overlap_rate: f64,
    /// SST budget violation rate (reads exceeding MAX_SSTS_PER_READ)
    pub sst_budget_violation_rate: f64,
    /// Block budget violation rate (reads exceeding MAX_BLOCKS_PER_READ)
    pub block_budget_violation_rate: f64,
}

/// Snapshot of startup recovery metrics.
///
/// These counters capture what was replayed during engine open:
/// - WAL records and bytes recovered
/// - Intent-log replay runs and entries processed
#[derive(Debug, Clone)]
pub struct RecoveryMetricsSnapshot {
    pub wal_recovery_records_replayed: u64,
    pub wal_recovery_bytes_replayed: u64,
    pub intent_log_replay_runs: u64,
    pub intent_log_entries_replayed: u64,
}

/// High-level engine health state for operators and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum EngineHealth {
    Healthy,
    Degraded,
    SalvageMode,
    WriteStalled,
    Corrupt,
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
    pub wal_append_count: u64,
    pub wal_flush_count: u64,
    pub wal_fsync_count: u64,
    pub wal_append_ns_total: u64,
    pub wal_fsync_ns_total: u64,
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
    pub manifest_files_verified: usize,
    pub sst_files_verified: usize,
    pub wal_recovery_records_replayed: u64,
    pub wal_recovery_bytes_replayed: u64,
    pub intent_entries_loaded: usize,
    pub health: EngineHealth,
}

impl ColumnFamilyHandle {
    pub fn new(id: ColumnFamilyId, name: String) -> Self {
        Self { id, name }
    }

    pub fn id(&self) -> ColumnFamilyId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Snapshot of runtime configuration that can be restored later.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IngestModeSnapshot {
    pub memtable_size_limit: usize,
    pub memtable_flush_threshold: usize,
    pub enable_compaction: bool,
    pub wal_durability_policy: crate::wal::DurabilityPolicy,
    pub wal_batch_config: crate::wal::policy::BatchConfig,
}
/// The main Midge KV store
///
/// This is a thin façade over the runtime. All state and background work
/// is managed by the runtime actors.
pub struct Engine {
    /// Runtime (owns the event loop thread)
    _runtime: Option<Runtime>,
    /// Handle to submit work to the runtime
    runtime_handle: RuntimeHandle,
    /// Database path
    db_path: PathBuf,
    /// Pure in-memory mode flag.
    memory_mode: bool,
    /// True when opened in cloud-backed mode.
    cloud_mode: bool,
    /// Recovery policy used for this open.
    recovery_policy: RecoveryPolicy,
    /// Latest committed sequence observed by the engine.
    ///
    /// Sequence numbers are allocated inside the runtime (at WAL append time) and
    /// returned via `RuntimeResponse::WalAppended { sequence, .. }`.
    sequence: std::sync::atomic::AtomicU64,
    /// Next snapshot ID counter (local only, not related to sequence numbers)
    next_snapshot_id: std::sync::atomic::AtomicU64,
    /// Column families registry (CF ID -> Handle)
    column_families: ColumnFamilyRegistry,
    /// Primary instance lease (enforces single-writer exclusivity)
    _lease: Option<Arc<dyn crate::lease::PrimaryLease>>,
    /// Primary instance lease guard
    _lease_guard: Option<crate::lease::LeaseGuard>,
    /// Lease heartbeat (keeps lease renewed)
    _lease_heartbeat: Option<std::sync::Mutex<crate::lease::LeaseHeartbeat>>,
    /// Per-CF ingest coordinators for write batching
    ingest_coordinators: dashmap::DashMap<ColumnFamilyId, Arc<ingest::IngestCoordinator>>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        let drop_start = std::time::Instant::now();
        tracing::debug!("Engine dropping, initiating cleanup");

        // Stop lease heartbeat first
        if let Some(heartbeat_mutex) = self._lease_heartbeat.take() {
            if let Ok(mut heartbeat) = heartbeat_mutex.lock() {
                heartbeat.stop();
                tracing::trace!("Engine: lease heartbeat stopped");
            }
        }

        // Shutdown all ingest coordinators
        let ingest_count = self.ingest_coordinators.len();
        for entry in self.ingest_coordinators.iter() {
            entry.value().shutdown();
        }
        tracing::trace!(count = ingest_count, "Engine: ingest coordinators shutdown");

        // Gracefully shutdown the runtime when engine is dropped
        // Send shutdown message first
        let _ = self.runtime_handle.shutdown();
        // Then drop the runtime which will wait for the thread to finish
        self._runtime.take();
        tracing::trace!("Engine: runtime shutdown complete");

        // Release lease via the PrimaryLease interface
        if let Some(lease) = self._lease.take() {
            let _ = lease.release();
            tracing::trace!("Engine: lease released");
        }

        // Drop the guard last
        self._lease_guard.take();

        tracing::debug!(
            elapsed_ms = drop_start.elapsed().as_millis(),
            "Engine cleanup complete"
        );
    }
}

impl Engine {
    fn verify_storage_path_internal(
        db_path: &Path,
        runtime_health: Option<EngineHealth>,
    ) -> MidgeResult<StorageVerificationReport> {
        crate::metadata::validate_format_marker(db_path)?;

        let manifest =
            crate::metadata::ManifestPersistence::load_with_policy(db_path, RecoveryPolicy::Strict)
                .map_err(MidgeError::RecoveryFailed)?;
        let intents =
            crate::runtime::IntentPersistence::load_with_policy(db_path, RecoveryPolicy::Strict)
                .map_err(MidgeError::RecoveryFailed)?;

        let fs =
            std::sync::Arc::new(crate::io::real::RealFs::new(db_path).map_err(|e| {
                MidgeError::RecoveryFailed(format!("failed to open filesystem: {e}"))
            })?) as Arc<dyn crate::io::Fs>;

        for file in &manifest.files {
            let rel = std::path::PathBuf::from("sst").join(&file.name);
            let path_str = rel.to_string_lossy().to_string();
            crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&fs)).map_err(|e| {
                MidgeError::Corruption(format!("failed to verify SST {}: {}", file.name, e))
            })?;
        }

        let wal_storage =
            crate::storage::LocalFsStorage::new(db_path.join("wal")).map_err(|e| {
                MidgeError::RecoveryFailed(format!("failed to open WAL directory: {e}"))
            })?;
        let wal_stats = crate::wal::recovery::replay_wal_with_policy(
            &wal_storage,
            &crate::storage::abstraction::StoragePath::new(""),
            &mut std::collections::HashMap::new(),
            crate::wal::recovery::ReplayPolicy::Strict,
        )
        .map_err(|e| MidgeError::RecoveryFailed(format!("WAL verification failed: {e}")))?;

        let residue =
            crate::storage::residue::StorageResidueAssessment::scan_sst_dir(db_path, &manifest);
        let health = match runtime_health {
            Some(EngineHealth::Healthy) | None => crate::storage::residue::classify_engine_health(
                crate::storage::residue::HealthInputs {
                    opened_in_salvage_mode: false,
                    write_stalled: false,
                    persistence_anomaly_detected: false,
                    pending_intents: intents.len(),
                    orphan_ssts: residue.orphan_ssts.len(),
                },
            ),
            Some(other) => other,
        };

        Ok(StorageVerificationReport {
            manifest_files_verified: manifest.files.len(),
            sst_files_verified: manifest.files.len(),
            wal_recovery_records_replayed: wal_stats.record_count,
            wal_recovery_bytes_replayed: wal_stats.bytes,
            intent_entries_loaded: intents.len(),
            health,
        })
    }

    fn list_obsolete_sst_files(
        db_path: &Path,
        manifest: &crate::metadata::Manifest,
    ) -> Vec<String> {
        crate::storage::residue::StorageResidueAssessment::scan_sst_dir(db_path, manifest)
            .orphan_ssts
    }

    fn ensure_local_sst_cache_from_cloud(
        state: &mut RuntimeState,
        cloud_root: &Path,
    ) -> MidgeResult<()> {
        let remote_sst_dir = cloud_root.join("sst");
        let mut retained_files = Vec::with_capacity(state.manifest.files.len());
        let mut manifest_changed = false;

        for file in state.manifest.files.clone() {
            let remote_path = remote_sst_dir.join(&file.name);
            let remote_valid = remote_path.exists()
                && crate::sst::fs::SstFileIo::open_with_real_fs(&remote_path).is_ok();

            if !remote_valid {
                if state.recovery_policy == RecoveryPolicy::Strict {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "authoritative cloud SST '{}' is missing or corrupt",
                        file.name
                    )));
                }

                state.opened_in_salvage_mode = true;
                state.persistence_anomaly_detected = true;
                manifest_changed = true;
                let local_path = state.sst_dir.join(&file.name);
                let _ = std::fs::remove_file(&local_path);
                tracing::warn!(
                    sst_name = %file.name,
                    "dropping manifest SST because authoritative cloud object is missing or corrupt"
                );
                continue;
            }

            let local_path = state.sst_dir.join(&file.name);
            let local_valid = local_path.exists()
                && crate::sst::fs::SstFileIo::open_with_real_fs(&local_path).is_ok();

            if !local_valid {
                if let Some(parent) = local_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|error| {
                        MidgeError::RecoveryFailed(format!(
                            "failed to create local SST cache directory '{}': {}",
                            parent.display(),
                            error
                        ))
                    })?;
                }

                if local_path.exists() {
                    let _ = std::fs::remove_file(&local_path);
                }

                if let Err(error) = std::fs::copy(&remote_path, &local_path) {
                    if state.recovery_policy == RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to restore local SST cache for '{}' from cloud: {}",
                            file.name, error
                        )));
                    }

                    state.opened_in_salvage_mode = true;
                    state.persistence_anomaly_detected = true;
                    manifest_changed = true;
                    tracing::warn!(
                        sst_name = %file.name,
                        error = %error,
                        "dropping manifest SST because local cache restore from cloud failed"
                    );
                    continue;
                }

                if let Err(error) = crate::sst::fs::SstFileIo::open_with_real_fs(&local_path) {
                    if state.recovery_policy == RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "restored local SST cache for '{}' is invalid: {}",
                            file.name, error
                        )));
                    }

                    state.opened_in_salvage_mode = true;
                    state.persistence_anomaly_detected = true;
                    manifest_changed = true;
                    let _ = std::fs::remove_file(&local_path);
                    tracing::warn!(
                        sst_name = %file.name,
                        error = %error,
                        "dropping manifest SST because restored local cache is invalid"
                    );
                    continue;
                }
            }

            retained_files.push(file);
        }

        if manifest_changed {
            state.manifest.files = retained_files;
            crate::metadata::ManifestPersistence::save(&state.db_path, &state.manifest)
                .map_err(MidgeError::Internal)?;
            state.restore_sequence_floor_from_manifest();
        }

        Ok(())
    }

    /// Open a database with explicit environment selection.
    ///
    /// The storage backend is specified by `OpenOptions.storage`. There is no
    /// inference from paths or sentinel strings.
    pub fn open(opts: OpenOptions) -> MidgeResult<Self> {
        let start = std::time::Instant::now();
        let (db_path, memory_mode) = match &opts.storage {
            Storage::InMemory => (
                {
                    let counter = IN_MEMORY_OPEN_COUNTER.fetch_add(1, Ordering::SeqCst);
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos())
                        .unwrap_or(0);
                    PathBuf::from(format!(
                        "target/tmp/midge_test_memory_{}_{}_{}",
                        std::process::id(),
                        counter,
                        timestamp
                    ))
                },
                true,
            ),
            Storage::Local { path } => (path.clone(), false),
            Storage::Cloud {
                local_cache_path, ..
            } => (local_cache_path.clone(), false),
        };

        // Only touch filesystem if not in memory mode
        if !memory_mode {
            let _ = std::fs::create_dir_all(&db_path);
        }

        // ═══════════════════════════════════════════════════════════════════════════
        // PHASE 1: ACQUIRE PRIMARY LEASE (MUST HAPPEN BEFORE ENGINE STARTS)
        // ═══════════════════════════════════════════════════════════════════════════
        //
        // This enforces single-instance exclusivity. If another instance holds the
        // lease, this call will fail immediately with a clear error message.
        //
        // CRITICAL: Lease acquisition MUST occur before:
        // - WAL recovery
        // - Runtime startup
        // - Any data operations
        //
        // Failure to acquire lease is FATAL and prevents engine startup.

        let lease = crate::lease::create_lease(&opts.storage).map_err(|e| {
            MidgeError::Internal(format!("failed to create lease for storage backend: {}", e))
        })?;

        let lease_guard = lease.clone().try_acquire().map_err(|e| match e {
            crate::lease::LeaseError::AcquisitionFailed(msg) => MidgeError::Internal(format!(
                "FATAL: another Midge instance is already running against this storage. \
                 Only one writable instance is allowed at a time. Error: {}",
                msg
            )),
            crate::lease::LeaseError::IoError(msg) => {
                MidgeError::Internal(format!("lease acquisition I/O error: {}", msg))
            }
            _ => MidgeError::Internal(format!("lease acquisition failed: {}", e)),
        })?;

        tracing::warn!(
            holder_id = %lease.holder_id(),
            storage = ?opts.storage,
            epoch = lease.epoch(),
            "primary lease acquired - this instance is now the exclusive writer"
        );

        // Extract writer epoch from the lease for fencing.
        let writer_epoch = lease.epoch();

        // Extract the leader store (if available) for sync-boundary epoch validation.
        let leader_store = lease.get_leader_store();

        // Build runtime state/config.
        // Cloud storage mode uses CloudAsync durability + HybridStorage.
        // Local/Memory modes use Batched durability with optional custom batch config.

        // Shared lease health flag — heartbeat sets it to false on renewal failure;
        // the event loop checks it before accepting new writes.
        let lease_healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let mut cloud_root = None;
        let (mut state, runtime_config) = match &opts.storage {
            Storage::Cloud { .. } => {
                let cloud = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
                    &db_path,
                )?;
                cloud_root = Some(cloud.cloud_root.clone());

                let state = RuntimeState::try_new_with_recovery_dir(
                    db_path.clone(),
                    memory_mode,
                    Some(cloud.recovery_cloud_wal_dir.clone()),
                    opts.recovery_policy,
                )?;

                let config = crate::runtime::RuntimeConfig {
                    wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
                    hybrid_storage: Some(cloud.hybrid_storage),
                    hybrid_storage_events: Some(cloud.events),
                    compression_policy: opts.compression_policy.clone(),
                    writer_epoch,
                    lease_healthy: Some(Arc::clone(&lease_healthy)),
                    leader_store: leader_store.clone(),
                    ..Default::default()
                };

                (state, config)
            }
            _ => {
                // Local or Memory mode: use Batched durability with optional batch config from OpenOptions
                let batch_config = opts.wal_batch_config.unwrap_or_default();

                let config = crate::runtime::RuntimeConfig {
                    wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
                    wal_batch_config: batch_config,
                    compression_policy: opts.compression_policy.clone(),
                    writer_epoch,
                    lease_healthy: Some(Arc::clone(&lease_healthy)),
                    leader_store: leader_store.clone(),
                    ..Default::default()
                };

                (
                    RuntimeState::try_new(db_path.clone(), memory_mode, opts.recovery_policy)?,
                    config,
                )
            }
        };

        // 🔑 CRITICAL: Replay intent log to recover any interrupted mutations
        // Must happen BEFORE runtime starts processing messages
        state.replay_intent_log()?;
        if let Some(root) = cloud_root.as_deref() {
            Self::ensure_local_sst_cache_from_cloud(&mut state, root)?;
        }
        state.cleanup_storage_residue();
        let recovered_sequence = state.sequence;
        let recovered_cf_metas = state.manifest.column_families.clone();

        // Start runtime
        let (runtime_inst, _) = Runtime::new()?;
        let (runtime, runtime_handle) = runtime_inst.start_with_config(state, runtime_config)?;

        // Apply derived OpenOptions to runtime: memtable limits and compaction
        let mut memtable_size_limit = opts.memtable_size_limit;
        let mut memtable_flush_threshold = opts.memtable_size_limit;

        // If user specified an explicit memory budget, derive thresholds from it
        if let crate::MemoryBudget::Bytes(n) = opts.memory_budget {
            // Use half of the memory budget as flush threshold so that
            // `state.should_stall_writes` (which checks `total_memtable_bytes >= flush_threshold * 2`)
            // will stall when total memtable bytes approaches the configured budget.
            let flush = n / 2;
            memtable_flush_threshold = flush.max(1);
            memtable_size_limit = memtable_flush_threshold;
        }

        let request_id = crate::runtime::next_request_id()?;
        let resp = runtime_handle.send_and_wait(crate::runtime::RuntimeMsg::SetRuntimeConfig {
            request_id,
            memtable_size_limit: Some(memtable_size_limit),
            memtable_flush_threshold: Some(memtable_flush_threshold),
            enable_compaction: None,
            wal_durability_policy: None,
            wal_batch_config: None,
        })?;

        match resp {
            crate::runtime::RuntimeResponse::Ok { .. } => {}
            crate::runtime::RuntimeResponse::Error { error, .. } => return Err(error),
            _ => {
                return Err(crate::common::MidgeError::Internal(
                    "unexpected response to SetRuntimeConfig".to_string(),
                ))
            }
        }

        let column_families = dashmap::DashMap::new();
        // Ensure default column family is always present
        let default_handle = ColumnFamilyHandle::new(0, "default".to_string());
        column_families.insert(default_handle.id(), default_handle);

        // Initialize ingest coordinators
        let ingest_coordinators = dashmap::DashMap::new();
        let default_coordinator =
            Arc::new(ingest::IngestCoordinator::new(0, runtime_handle.clone())?);
        ingest_coordinators.insert(0, default_coordinator);

        // ═══════════════════════════════════════════════════════════════════════════
        // PHASE 2: START LEASE HEARTBEAT (AFTER ENGINE STARTS)
        // ═══════════════════════════════════════════════════════════════════════════
        //
        // The heartbeat loop renews the lease periodically to maintain exclusivity.
        // If renewal fails, the heartbeat will mark itself unhealthy.
        //
        let mut lease_heartbeat =
            crate::lease::LeaseHeartbeat::new_with_healthy(Arc::clone(&lease), lease_healthy);
        lease_heartbeat.start();

        // Check heartbeat health immediately after start to catch early failures.
        // Subsequent health monitoring is the caller's responsibility via
        // `Engine::is_primary_lease_healthy()`. Applications should poll this
        // periodically (e.g., every 10-30 seconds) and trigger graceful shutdown
        // if the lease is lost. The heartbeat thread itself stops on renewal
        // failure and marks `is_healthy() == false`.
        if !lease_heartbeat.is_healthy() {
            return Err(crate::common::MidgeError::Internal(
                "lease heartbeat failed immediately after start".to_string(),
            ));
        }

        tracing::info!(db_path = %db_path.display(), open_ms = start.elapsed().as_secs_f64() * 1000.0, "engine open completed");

        // Rebuild local CF handles from the same recovered manifest state the
        // runtime validated during open.
        for cf_meta in &recovered_cf_metas {
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                let handle = ColumnFamilyHandle::new(cf_meta.id, cf_meta.name.clone());
                column_families.insert(cf_meta.id, handle);

                // Start coordinator for loaded CF
                let coordinator = Arc::new(ingest::IngestCoordinator::new(
                    cf_meta.id,
                    runtime_handle.clone(),
                )?);
                ingest_coordinators.insert(cf_meta.id, coordinator);
            }
        }

        Ok(Self {
            _runtime: Some(runtime),
            runtime_handle,
            db_path,
            memory_mode,
            cloud_mode: matches!(&opts.storage, Storage::Cloud { .. }),
            recovery_policy: opts.recovery_policy,
            sequence: std::sync::atomic::AtomicU64::new(recovered_sequence),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
            column_families,
            _lease: Some(lease),
            _lease_guard: Some(lease_guard),
            _lease_heartbeat: Some(std::sync::Mutex::new(lease_heartbeat)),
            ingest_coordinators,
        })
    }

    /// Get an existing column family by name.
    ///
    /// Returns None if the column family doesn't exist.
    pub fn get_column_family(&self, name: &str) -> Option<ColumnFamilyHandle> {
        for entry in self.column_families.iter() {
            if entry.value().name() == name {
                return Some(entry.value().clone());
            }
        }
        None
    }

    /// Check if the primary instance lease is healthy.
    ///
    /// Returns `true` if this instance holds a valid, renewable lease.
    /// Returns `false` if lease renewal has failed, indicating this instance
    /// should stop accepting writes.
    ///
    /// ## Observability
    ///
    /// Applications should monitor this value and trigger alerts or graceful
    /// shutdown if it becomes false. Loss of lease means another instance may
    /// be attempting to take over, or there is a network/storage issue.
    ///
    /// ## Recommendation
    ///
    /// Poll this method periodically (e.g., every 10-30 seconds) and:
    /// - Log a warning if it returns false
    /// - Stop accepting new writes
    /// - Trigger graceful shutdown
    pub fn is_primary_lease_healthy(&self) -> bool {
        if let Some(ref heartbeat_mutex) = self._lease_heartbeat {
            if let Ok(heartbeat) = heartbeat_mutex.lock() {
                return heartbeat.is_healthy();
            }
        }
        // If we can't lock or lease is not present, assume unhealthy
        false
    }

    /// Open a database with the provided MidgeOptions.
    ///
    /// Convenience method for testkit and standard testing patterns.
    pub fn open_with_options(opts: crate::testkit::MidgeOptions) -> MidgeResult<Self> {
        let open_opts = opts.to_open_options();
        Self::open(open_opts)
    }

    /// Fetch the current runtime configuration snapshot for diagnostics or restoration.
    pub(crate) fn get_runtime_config(&self) -> MidgeResult<IngestModeSnapshot> {
        let request_id = crate::runtime::next_request_id()?;
        let resp = self
            .runtime_handle
            .send_and_wait(crate::runtime::RuntimeMsg::GetRuntimeConfig { request_id })?;
        match resp {
            crate::runtime::RuntimeResponse::RuntimeConfigSnapshot {
                memtable_size_limit,
                memtable_flush_threshold,
                enable_compaction,
                wal_durability_policy,
                wal_batch_config,
                ..
            } => Ok(IngestModeSnapshot {
                memtable_size_limit,
                memtable_flush_threshold,
                enable_compaction,
                wal_durability_policy,
                wal_batch_config,
            }),
            _ => Err(crate::common::MidgeError::Internal(
                "unexpected response to GetRuntimeConfig".to_string(),
            )),
        }
    }

    /// Check if ingest batching should be used based on durability policy.
    ///
    /// Return whether an ingest barrier is currently active.
    ///
    /// Ingest batching is orthogonal to cloud durability. Cloud-backed async mode
    /// still makes writes visible after the local WAL append barrier; it simply
    /// advances cloud durability later in the background.
    pub(crate) fn is_ingesting(&self) -> MidgeResult<bool> {
        let request_id = crate::runtime::next_request_id()?;
        let resp = self
            .runtime_handle
            .send_and_wait(crate::runtime::RuntimeMsg::GetIngestState { request_id })?;
        match resp {
            crate::runtime::RuntimeResponse::IngestState { ingest_active, .. } => Ok(ingest_active),
            _ => Err(crate::common::MidgeError::Internal(
                "unexpected response to GetIngestState".to_string(),
            )),
        }
    }

    /// Enter a temporary ingest mode: disable compaction, relax WAL, increase memtable limits.
    /// Returns the previous configuration snapshot which can be used to restore state.
    pub(crate) fn enter_ingest_mode(&self) -> MidgeResult<IngestModeSnapshot> {
        // Capture current runtime config so we can restore it later
        let prev = self.get_runtime_config()?;

        // Step 1: Apply performance-oriented runtime knobs (larger memtable, batched WAL)
        let request_id = crate::runtime::next_request_id()?;
        let target_mem = (prev.memtable_size_limit.max(64 * 1024 * 1024)).saturating_mul(4); // grow 4x
        let batch_cfg = prev.wal_batch_config;
        let resp =
            self.runtime_handle
                .send_and_wait(crate::runtime::RuntimeMsg::SetRuntimeConfig {
                    request_id,
                    memtable_size_limit: Some(target_mem),
                    memtable_flush_threshold: Some(target_mem),
                    enable_compaction: Some(false), // also set false here for a fast path
                    wal_durability_policy: Some(crate::wal::DurabilityPolicy::Batched),
                    wal_batch_config: Some(batch_cfg),
                })?;

        match resp {
            crate::runtime::RuntimeResponse::Ok { .. } => {
                // Step 2: Ensure a hard ingest barrier: begin ingest (blocks until inflight compactions drain)
                let bid = crate::runtime::next_request_id()?;
                let br = self
                    .runtime_handle
                    .send_and_wait(crate::runtime::RuntimeMsg::BeginIngest { request_id: bid })?;
                match br {
                    crate::runtime::RuntimeResponse::Ok { .. } => Ok(prev),
                    crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
                    _ => Err(crate::common::MidgeError::Internal(
                        "unexpected response to BeginIngest".to_string(),
                    )),
                }
            }
            crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(crate::common::MidgeError::Internal(
                "unexpected response to SetRuntimeConfig".to_string(),
            )),
        }
    }

    /// Restore runtime configuration from a previously-captured snapshot.
    pub(crate) fn exit_ingest_mode(&self, prev: IngestModeSnapshot) -> MidgeResult<()> {
        // Step 1: End ingest barrier (flush outstanding memtables and bump epoch)
        let bid = crate::runtime::next_request_id()?;
        let br = self
            .runtime_handle
            .send_and_wait(crate::runtime::RuntimeMsg::EndIngest { request_id: bid })?;
        match br {
            crate::runtime::RuntimeResponse::Ok { .. } => {
                // Step 2: Restore previous runtime configuration
                let request_id = crate::runtime::next_request_id()?;
                let resp = self.runtime_handle.send_and_wait(
                    crate::runtime::RuntimeMsg::SetRuntimeConfig {
                        request_id,
                        memtable_size_limit: Some(prev.memtable_size_limit),
                        memtable_flush_threshold: Some(prev.memtable_flush_threshold),
                        enable_compaction: Some(prev.enable_compaction),
                        wal_durability_policy: Some(prev.wal_durability_policy),
                        wal_batch_config: Some(prev.wal_batch_config),
                    },
                )?;

                match resp {
                    crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
                    crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
                    _ => Err(crate::common::MidgeError::Internal(
                        "unexpected response to SetRuntimeConfig".to_string(),
                    )),
                }
            }
            crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(crate::common::MidgeError::Internal(
                "unexpected response to EndIngest".to_string(),
            )),
        }
    }

    // ========================================================================
    // Range Operations
    // ========================================================================

    /// Apply a range tombstone to a specific column family.
    ///
    /// The tombstone covers `[start_key, end_key)`.
    /// Range deletes are intentionally not part of the transaction API.
    pub fn delete_range(
        &self,
        cf: &ColumnFamilyHandle,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        opts: api::WriteOptions,
    ) -> MidgeResult<()> {
        let durability_policy = self.effective_wal_durability_policy(opts)?;
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::WalAppendDeleteRange {
                request_id: next_request_id()?,
                cf_id: cf.id(),
                start_key,
                end_key,
                durability_policy: Some(durability_policy),
            })?;

        match response {
            RuntimeResponse::WalAppended { sequence, .. } => {
                self.sequence.store(sequence, Ordering::SeqCst);
                self.finalize_write_durability(sequence, opts)
            }
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to delete_range".to_string(),
            )),
        }
    }

    /// Flush all pending writes to disk (used by tests)
    pub(crate) fn sync(&self) -> MidgeResult<()> {
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalSync {
            request_id: next_request_id()?,
        })?;

        match response {
            RuntimeResponse::Ok { .. } => Ok(()),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to sync".to_string(),
            )),
        }
    }

    fn effective_wal_durability_policy(
        &self,
        opts: api::WriteOptions,
    ) -> MidgeResult<crate::wal::DurabilityPolicy> {
        if self.cloud_mode {
            return Ok(match opts.policy() {
                ApiDurabilityPolicy::BestEffort => crate::wal::DurabilityPolicy::BestEffort,
                ApiDurabilityPolicy::Buffered
                | ApiDurabilityPolicy::Sync
                | ApiDurabilityPolicy::CloudStrict => crate::wal::DurabilityPolicy::CloudAsync,
            });
        }

        if opts.is_cloud_strict() {
            return Err(MidgeError::InvalidArgument(
                "cloud_strict requires cloud-backed storage".to_string(),
            ));
        }

        Ok(opts.to_wal_durability_policy())
    }

    fn finalize_write_durability(&self, sequence: u64, opts: api::WriteOptions) -> MidgeResult<()> {
        if self.cloud_mode {
            if opts.is_sync() || opts.is_cloud_strict() {
                let response = self
                    .runtime_handle
                    .send_and_wait(RuntimeMsg::SealWalForCloud {
                        request_id: next_request_id()?,
                        sequence,
                        wait_for_ack: opts.is_cloud_strict(),
                    })?;

                return match response {
                    RuntimeResponse::Ok { .. } => Ok(()),
                    RuntimeResponse::Error { error, .. } => Err(error),
                    _ => Err(MidgeError::Internal(
                        "Unexpected response to SealWalForCloud".to_string(),
                    )),
                };
            }

            return Ok(());
        }

        if opts.is_sync() {
            self.sync()?;
        }

        Ok(())
    }

    /// Force a flush of a specific column family
    pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::FlushMemtable {
                request_id: next_request_id()?,
                cf_id: cf.id(),
            })?;

        match response {
            RuntimeResponse::Ok { .. } | RuntimeResponse::FlushComplete { .. } => Ok(()),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to flush".to_string(),
            )),
        }
    }

    /// Begin a new transaction
    ///
    /// # Arguments
    /// * `cf_id` - Column family ID
    /// * `mode` - Transaction mode (ReadOnly or ReadWrite)
    ///
    /// # Panics
    /// Panics if called while ingest mode is active. This is a programmer error:
    /// transactions must not be started during ingest. Complete the ingest first.
    pub fn begin_tx(
        &self,
        cf_id: ColumnFamilyId,
        mode: api::TransactionMode,
    ) -> MidgeResult<api::Transaction> {
        // ─────────────────────────────────────────────────────────────────────────
        // HARD INVARIANT: No transactions while ingest is active.
        // ─────────────────────────────────────────────────────────────────────────
        assert!(
            !self.is_ingesting().unwrap_or(false),
            "BUG: begin_tx called while ingest mode is active. \
             Violated invariant: transactions must not be started during ingest. \
             Correct ordering: exit_ingest_mode() BEFORE begin_tx()."
        );

        let txn_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Fast path: read snapshot from lock-free ArcSwap cache (no event loop round-trip).
        let cache_guard = self.runtime_handle.snapshot_cache.load();
        let start_sequence = cache_guard.sequence;
        let read_snapshot = cache_guard
            .cf_snapshots
            .get(&cf_id)
            .map(|data| data.snapshot.clone());
        // Drop the guard ASAP to avoid holding the ArcSwap lease.
        drop(cache_guard);

        Ok(api::Transaction::new(
            self.runtime_handle.clone(),
            txn_id,
            cf_id,
            mode,
            start_sequence,
            read_snapshot,
            self.cloud_mode,
        ))
    }

    /// Commit a transaction atomically
    ///
    /// # Arguments
    /// * `txn` - Transaction to commit
    /// * `opts` - Write options specifying durability guarantees
    ///
    /// # Errors
    /// - `MidgeError::WriteStall` if memory budget is exceeded. Client must retry
    ///   after backoff (10-100ms exponential recommended).
    pub fn commit(&self, txn: api::Transaction, opts: api::WriteOptions) -> MidgeResult<()> {
        // ReadOnly transactions are a no-op for commit
        if txn.is_read_only() {
            return Ok(());
        }

        // NOTE: BestEffort is only safe for bulk loads and initialization where:
        // - Data loss is acceptable (test/setup phase)
        // - Durability is not required before client sees results
        // - Engine crash/restart is followed by reload
        // NEVER use BestEffort for production data or measured workloads.

        if !txn.has_writes() {
            // Read-write transaction with no writes
            // Apply sync if requested
            if opts.is_sync() {
                self.sync()?;
            }
            return Ok(());
        }

        // Route all transactional writes through the ingest coordinator so every
        // operation shape shares the same backpressure, write-group batching, and
        // atomic ApplyTransaction path.
        let cf_id_for_check = txn.cf_id();

        let coordinator = self
            .ingest_coordinators
            .get(&cf_id_for_check)
            .ok_or_else(|| {
                MidgeError::InvalidArgument(format!(
                    "column family {} does not exist",
                    cf_id_for_check
                ))
            })?;

        let ops = txn.into_runtime_ops();

        let durability_policy = Some(self.effective_wal_durability_policy(opts)?);
        let sequence = coordinator.submit_ops(&self.runtime_handle, ops, durability_policy)?;

        // Update engine's sequence to reflect completed writes
        self.sequence.store(sequence, Ordering::SeqCst);

        self.finalize_write_durability(sequence, opts)
    }

    /// Wait for a write stall to clear for `cf_id`.
    ///
    /// Returns `Ok(true)` if the stall cleared within `timeout`, `Ok(false)` on timeout.
    pub fn wait_for_write_stall_clear(
        &self,
        cf_id: ColumnFamilyId,
        timeout: Duration,
    ) -> MidgeResult<bool> {
        let request_id = next_request_id()?;

        let msg = RuntimeMsg::WaitForWriteStallClear { request_id, cf_id };
        let resp = self.runtime_handle.send_and_wait_timeout(msg, timeout)?;

        match resp {
            Some(RuntimeResponse::Ok { .. }) => Ok(true),
            Some(RuntimeResponse::Error { error, .. }) => Err(error),
            Some(other) => Err(MidgeError::Internal(format!(
                "Unexpected response to WaitForWriteStallClear: {:?}",
                other
            ))),
            None => {
                // Best-effort cancel: prevents waiter accumulation under timeouts.
                let _ = self
                    .runtime_handle
                    .send(RuntimeMsg::CancelWaitForWriteStallClear {
                        wait_request_id: request_id,
                    });
                Ok(false)
            }
        }
    }

    /// Rollback a transaction.
    ///
    /// This is a no-op because Midge transactions are designed for atomic commit only.
    /// Writes are accumulated in the transaction and only applied when `commit()` is called.
    /// Simply dropping the transaction (without commit) discards all pending writes.
    pub fn rollback_transaction(&self, _txn: api::Transaction) -> MidgeResult<()> {
        Ok(())
    }

    // === Internal Transaction Helpers ===

    /// Read a key at a specific sequence (for transaction use)
    pub(crate) fn read_at_sequence(
        &self,
        cf_id: ColumnFamilyId,
        key: &[u8],
        sequence: u64,
    ) -> MidgeResult<Option<bytes::Bytes>> {
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::Read {
            request_id: next_request_id()?,
            cf_id,
            key: key.to_vec(),
            sequence,
            requested_durability: api::Durability::Steady,
        })?;

        match response {
            RuntimeResponse::ReadValue { value, .. } => Ok(value.map(bytes::Bytes::from)),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to read_at_sequence".to_string(),
            )),
        }
    }

    /// Scan a range at a specific sequence (for transaction use)
    pub(crate) fn scan_at_sequence(
        &self,
        cf_id: ColumnFamilyId,
        start: &[u8],
        end: &[u8],
        sequence: u64,
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::RangeScan {
            request_id: next_request_id()?,
            cf_id,
            start: start.to_vec(),
            end: end.to_vec(),
            sequence,
            requested_durability: api::Durability::Steady,
        })?;

        match response {
            RuntimeResponse::RangeScanResults { results, .. } => Ok(results
                .into_iter()
                .map(|(k, v)| (bytes::Bytes::from(k), bytes::Bytes::from(v)))
                .collect()),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to scan_at_sequence".to_string(),
            )),
        }
    }

    /// Shutdown the engine gracefully
    pub fn shutdown(self) -> MidgeResult<()> {
        self.runtime_handle.shutdown()
    }

    // === Column Family Lifecycle ===

    /// Create a new column family with the given name
    pub fn create_column_family(&self, name: &str) -> MidgeResult<ColumnFamilyHandle> {
        let response = self.runtime_handle.send_and_wait_filtered(
            RuntimeMsg::ManifestCreateColumnFamily {
                request_id: next_request_id()?,
                name: name.to_string(),
            },
            |resp| {
                matches!(
                    resp,
                    RuntimeResponse::ColumnFamilyCreated { .. } | RuntimeResponse::Error { .. }
                )
            },
        )?;

        match response {
            RuntimeResponse::ColumnFamilyCreated { cf_id, .. } => {
                let handle = ColumnFamilyHandle::new(cf_id, name.to_string());
                // Register CF in local registry
                self.column_families.insert(cf_id, handle.clone());

                // Start ingest coordinator for new CF
                let coordinator = Arc::new(ingest::IngestCoordinator::new(
                    cf_id,
                    self.runtime_handle.clone(),
                )?);
                self.ingest_coordinators.insert(cf_id, coordinator);
                self.runtime_handle
                    .send_and_wait(RuntimeMsg::ManifestPersist {
                        request_id: next_request_id()?,
                    })?;

                Ok(handle)
            }
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to create_column_family".to_string(),
            )),
        }
    }

    /// Drop a column family by ID
    /// Drop a column family (used by tests)
    pub fn drop_column_family(&self, cf_id: ColumnFamilyId) -> MidgeResult<()> {
        let response = self.runtime_handle.send_and_wait_filtered(
            RuntimeMsg::ManifestDropColumnFamily {
                request_id: next_request_id()?,
                cf_id,
            },
            |resp| {
                matches!(
                    resp,
                    RuntimeResponse::Ok { .. } | RuntimeResponse::Error { .. }
                )
            },
        )?;

        match response {
            RuntimeResponse::Ok { .. } => {
                // Shutdown coordinator for this CF
                if let Some((_, coordinator)) = self.ingest_coordinators.remove(&cf_id) {
                    coordinator.shutdown();
                }

                // Remove from local registry
                self.column_families.remove(&cf_id);

                // Persist manifest to disk
                let _persist_response =
                    self.runtime_handle
                        .send_and_wait(RuntimeMsg::ManifestPersist {
                            request_id: next_request_id()?,
                        })?;

                Ok(())
            }
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to drop_column_family".to_string(),
            )),
        }
    }

    /// List all active column families
    pub fn list_column_families(&self) -> MidgeResult<Vec<ColumnFamilyHandle>> {
        Ok(self
            .column_families
            .iter()
            .map(|ref_multi| ref_multi.value().clone())
            .collect())
    }

    /// Compact all data (schedule compactions and wait for completion)
    pub fn compact_all(&self) -> MidgeResult<()> {
        let request_id = next_request_id()?;
        let resp = self
            .runtime_handle
            .send_and_wait(crate::runtime::RuntimeMsg::CompactAll { request_id })?;

        match resp {
            crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
            crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(crate::common::MidgeError::Internal(
                "unexpected response to CompactAll".to_string(),
            )),
        }
    }

    /// Get read amplification metrics snapshot
    ///
    /// Returns current read amplification statistics including:
    /// - SSTs touched per read
    /// - L0 overlap patterns
    /// - Budget violation rates
    ///
    /// Use this for monitoring read performance and tuning compaction triggers.
    /// Get read amplification metrics (used by tests)
    pub fn get_read_amp_metrics(&self) -> MidgeResult<ReadAmpMetricsSnapshot> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetReadAmpMetrics {
                request_id: next_request_id()?,
            })?;

        match response {
            RuntimeResponse::ReadAmpMetricsSnapshot {
                reads_total,
                ssts_touched_total,
                l0_ssts_touched_total,
                blocks_read_total,
                avg_ssts_per_read,
                avg_l0_ssts_per_read,
                avg_blocks_per_read,
                l0_overlap_rate,
                sst_budget_violation_rate,
                block_budget_violation_rate,
                ..
            } => Ok(ReadAmpMetricsSnapshot {
                reads_total,
                ssts_touched_total,
                l0_ssts_touched_total,
                blocks_read_total,
                avg_ssts_per_read,
                avg_l0_ssts_per_read,
                avg_blocks_per_read,
                l0_overlap_rate,
                sst_budget_violation_rate,
                block_budget_violation_rate,
            }),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response from GetReadAmpMetrics".to_string(),
            )),
        }
    }

    /// Get startup recovery metrics snapshot.
    ///
    /// Returns counters from the runtime's recovery phase executed during engine open.
    pub fn get_recovery_metrics(&self) -> MidgeResult<RecoveryMetricsSnapshot> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetRecoveryMetrics {
                request_id: next_request_id()?,
            })?;

        match response {
            RuntimeResponse::RecoveryMetricsSnapshot {
                wal_recovery_records_replayed,
                wal_recovery_bytes_replayed,
                intent_log_replay_runs,
                intent_log_entries_replayed,
                ..
            } => Ok(RecoveryMetricsSnapshot {
                wal_recovery_records_replayed,
                wal_recovery_bytes_replayed,
                intent_log_replay_runs,
                intent_log_entries_replayed,
            }),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response from GetRecoveryMetrics".to_string(),
            )),
        }
    }

    /// Get an operator-facing snapshot of runtime metrics and health.
    pub fn get_runtime_metrics(&self) -> MidgeResult<RuntimeMetricsSnapshot> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetRuntimeMetrics {
                request_id: next_request_id()?,
            })?;

        match response {
            RuntimeResponse::RuntimeMetricsSnapshot { snapshot, .. } => Ok(snapshot),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response from GetRuntimeMetrics".to_string(),
            )),
        }
    }

    /// Get a stable snapshot of the current SST layout and pinned snapshot state.
    pub fn get_storage_layout(&self) -> MidgeResult<StorageLayoutSnapshot> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetStorageLayout {
                request_id: next_request_id()?,
            })?;

        match response {
            RuntimeResponse::StorageLayoutSnapshot { snapshot, .. } => Ok(snapshot),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response from GetStorageLayout".to_string(),
            )),
        }
    }

    /// Run a non-mutating integrity pass over manifest, intent-log, WAL, and SST files.
    pub fn verify_storage(&self) -> MidgeResult<StorageVerificationReport> {
        if self.memory_mode {
            return Err(MidgeError::NotSupported(
                "storage verification is not supported in memory mode".to_string(),
            ));
        }

        Self::verify_storage_path_internal(
            &self.db_path,
            match self.get_runtime_metrics() {
                Ok(snapshot) => Some(snapshot.health),
                Err(_) => Some(EngineHealth::Corrupt),
            },
        )
    }

    /// Verify a storage directory without opening a runtime.
    pub fn verify_path(path: impl Into<PathBuf>) -> MidgeResult<StorageVerificationReport> {
        Self::verify_storage_path_internal(&path.into(), None)
    }

    // === Internal helpers ===
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Tests for ColumnFamilyId invariants
    // ============================================================================

    #[test]
    fn should_use_zero_as_default_column_family_id() {
        // Arrange / Act
        let cf_id: ColumnFamilyId = 0;

        // Assert
        assert_eq!(cf_id, 0);
    }

    #[test]
    fn should_preserve_custom_column_family_id_value() {
        // Arrange
        let custom_id: ColumnFamilyId = 42;

        // Assert
        assert_eq!(custom_id, 42);
    }

    #[test]
    fn should_support_column_family_id_equality() {
        // Arrange
        let id1: ColumnFamilyId = 5;
        let id2: ColumnFamilyId = 5;
        let id3: ColumnFamilyId = 6;

        // Assert
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn should_support_column_family_id_hashing() {
        // Arrange
        use std::collections::HashMap;
        let mut map = HashMap::new();
        let id: ColumnFamilyId = 10;

        // Act
        map.insert(id, "value");

        // Assert: should be retrievable by id
        assert_eq!(map.get(&id), Some(&"value"));
    }

    // ============================================================================
    // Tests for ColumnFamilyHandle invariants
    // ============================================================================

    #[test]
    fn should_create_column_family_handle_with_id_and_name() {
        // Arrange
        let cf_id: ColumnFamilyId = 5;
        let name = "my_cf".to_string();

        // Act
        let handle = ColumnFamilyHandle::new(cf_id, name.clone());

        // Assert
        assert_eq!(handle.id(), cf_id);
        assert_eq!(handle.name(), "my_cf");
    }

    #[test]
    fn should_preserve_column_family_handle_identity() {
        // Arrange
        let cf_id: ColumnFamilyId = 10;
        let name = "test_cf".to_string();
        let handle = ColumnFamilyHandle::new(cf_id, name);

        // Assert: id() and name() return exact values
        assert_eq!(handle.id(), 10);
        assert_eq!(handle.name(), "test_cf");
    }

    #[test]
    fn should_clone_column_family_handle() {
        // Arrange
        let handle1 = ColumnFamilyHandle::new(7, "cf".to_string());

        // Act
        let handle2 = handle1.clone();

        // Assert
        assert_eq!(handle1.id(), handle2.id());
        assert_eq!(handle1.name(), handle2.name());
    }

    #[test]
    fn should_support_empty_column_family_name() {
        // Arrange / Act
        let handle = ColumnFamilyHandle::new(1, "".to_string());

        // Assert
        assert_eq!(handle.name(), "");
    }

    #[test]
    fn should_handle_unicode_column_family_names() {
        // Arrange
        let unicode_name = "数据_测试".to_string();

        // Act
        let handle = ColumnFamilyHandle::new(1, unicode_name.clone());

        // Assert
        assert_eq!(handle.name(), unicode_name);
    }

    // ============================================================================
    // Tests for ColumnFamilyId special values
    // ============================================================================

    #[test]
    fn should_handle_maximum_column_family_id() {
        // Arrange / Act
        let max_id: ColumnFamilyId = u32::MAX;

        // Assert
        assert_eq!(max_id, u32::MAX);
    }

    #[test]
    fn should_handle_zero_column_family_id() {
        // Arrange / Act
        let zero_id: ColumnFamilyId = 0;

        // Assert
        assert_eq!(zero_id, 0);
    }

    #[test]
    fn should_distinguish_between_different_column_family_ids() {
        // Arrange
        let id_vec: [ColumnFamilyId; 4] = [0, 1, 100, u32::MAX];

        // Act
        let unique_count = id_vec
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len();

        // Assert: all IDs are unique
        assert_eq!(unique_count, 4);
    }

    #[test]
    fn memory_flush_and_compact_noop() {
        // Open a memory-mode engine and verify flush/compact succeed and do not touch disk
        let opts = crate::testkit::MidgeOptions {
            storage_mode: crate::testkit::StorageMode::Memory,
            ..Default::default()
        };

        let engine = Engine::open_with_options(opts).expect("open memory engine");
        let cf = engine
            .create_column_family("test")
            .expect("create column family");
        // These operations should be no-ops and return Ok
        engine.flush_cf(&cf).expect("memory flush should succeed");
        engine
            .compact_all()
            .expect("memory compact_all should succeed");
    }

    // ============================================================================
    // Tests for ColumnFamilyHandle creation invariants
    // ============================================================================

    #[test]
    fn should_create_handle_for_default_column_family() {
        // Arrange / Act
        let handle = ColumnFamilyHandle::new(0, "default".to_string());

        // Assert
        assert_eq!(handle.id(), 0);
        assert_eq!(handle.name(), "default");
    }

    #[test]
    fn should_create_multiple_handles_with_different_ids() {
        // Arrange / Act
        let handle1 = ColumnFamilyHandle::new(1, "cf1".to_string());
        let handle2 = ColumnFamilyHandle::new(2, "cf2".to_string());
        let handle3 = ColumnFamilyHandle::new(3, "cf3".to_string());

        // Assert: all distinct
        assert_ne!(handle1.id(), handle2.id());
        assert_ne!(handle2.id(), handle3.id());
        assert_ne!(handle1.id(), handle3.id());
    }

    #[test]
    fn should_preserve_handle_identity_after_clone() {
        // Arrange
        let original = ColumnFamilyHandle::new(99, "original_name".to_string());

        // Act
        let cloned = original.clone();

        // Assert: cloned is identical
        assert_eq!(original.id(), cloned.id());
        assert_eq!(original.name(), cloned.name());

        // And original still works
        assert_eq!(original.id(), 99);
    }

    // ============================================================================
    // Tests for debug trait implementation
    // ============================================================================

    #[test]
    fn should_format_column_family_handle_for_debug() {
        // Arrange
        let handle = ColumnFamilyHandle::new(5, "test".to_string());

        // Act
        let debug_str = format!("{:?}", handle);

        // Assert: should be debuggable
        assert!(!debug_str.is_empty());
    }

    // ============================================================================
    // Tests for trait bounds enforcement
    // ============================================================================

    #[test]
    fn should_support_column_family_id_in_hashmap() {
        // Arrange
        use std::collections::HashMap;
        let mut map: HashMap<ColumnFamilyId, String> = HashMap::new();

        // Act
        map.insert(1, "cf1".to_string());
        map.insert(2, "cf2".to_string());

        // Assert
        assert_eq!(map.get(&1), Some(&"cf1".to_string()));
        assert_eq!(map.get(&2), Some(&"cf2".to_string()));
    }

    #[test]
    fn should_support_column_family_handle_in_vector() {
        // Arrange
        // Act
        let handles = [
            ColumnFamilyHandle::new(0, "default".to_string()),
            ColumnFamilyHandle::new(1, "secondary".to_string()),
        ];

        // Assert
        assert_eq!(handles.len(), 2);
        assert_eq!(handles[0].name(), "default");
        assert_eq!(handles[1].name(), "secondary");
    }
}
