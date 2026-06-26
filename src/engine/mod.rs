//! Main KV store engine
//!
//! Public API for database operations.
//!
//! The engine provides a transaction-scoped API for all data operations.
//! All reads and writes execute within explicit transactions.
//!
//! Key responsibilities:
//! - Transaction lifecycle entry (begin_tx)
//! - Column family management
//! - Flush and compaction control
//! - Metrics and observability
//!
//! Point operations (get, put, delete, scan) are methods on Transaction.
//! Transaction finalization and range tombstones are also transaction-scoped.

use crate::common::{MidgeError, MidgeResult};
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
    AzureCredentialSource, CloudCredentialSource, CloudProviderConfig, Direction, GcsApiStyle,
    GcsCredentialSource, Goal, IsolationLevel, Key, MemoryBudget, OpenOptions, Query,
    RecoveryPolicy, S3CredentialSource, ScanIterator, Storage, Transaction, TransactionMode, Value,
    WorkloadProfile, WriteOptions,
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
    pub l0_compaction_trigger: usize,
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
    sequence: Arc<std::sync::atomic::AtomicU64>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct CloudSstRecoveryProof {
    name: String,
    expected_size_bytes: Option<u64>,
    expected_crc32c: Option<u32>,
}

impl CloudSstRecoveryProof {
    #[cfg(test)]
    fn name_only(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expected_size_bytes: None,
            expected_crc32c: None,
        }
    }

    fn from_manifest(file: &crate::metadata::FileMeta) -> Self {
        Self {
            name: file.name.clone(),
            expected_size_bytes: Some(file.size_bytes),
            expected_crc32c: file.content_crc32c,
        }
    }

    fn from_runtime(file: &crate::runtime::FileMeta) -> Self {
        Self {
            name: file.name.clone(),
            expected_size_bytes: Some(file.size_bytes),
            expected_crc32c: file.content_crc32c,
        }
    }

    fn merge_from(&mut self, other: Self) {
        if self.expected_size_bytes.is_none() {
            self.expected_size_bytes = other.expected_size_bytes;
        }
        if self.expected_crc32c.is_none() {
            self.expected_crc32c = other.expected_crc32c;
        }
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
            let remote_valid = Self::local_sst_file_matches_manifest(&remote_path, &file);

            if !remote_valid {
                if state.recovery_policy == RecoveryPolicy::Strict {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "authoritative cloud SST '{}' is missing, corrupt, or size-mismatched",
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
            let local_valid = Self::local_sst_file_matches_manifest(&local_path, &file);

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

    fn validate_sst_bytes_against_proof(
        sst_name: &str,
        data: &[u8],
        expected_size_bytes: Option<u64>,
        expected_crc32c: Option<u32>,
    ) -> MidgeResult<()> {
        if let Some(expected_size_bytes) = expected_size_bytes {
            if data.len() as u64 != expected_size_bytes {
                return Err(MidgeError::RecoveryFailed(format!(
                    "SST '{}' size {} does not match manifest {}",
                    sst_name,
                    data.len(),
                    expected_size_bytes
                )));
            }
        }

        if let Some(expected_crc32c) = expected_crc32c {
            let actual_crc32c = crc32c::crc32c(data);
            if actual_crc32c != expected_crc32c {
                return Err(MidgeError::RecoveryFailed(format!(
                    "SST '{}' content crc32c {:08x} does not match manifest {:08x}",
                    sst_name, actual_crc32c, expected_crc32c
                )));
            }
        }

        Ok(())
    }

    fn local_sst_file_matches_proof(
        path: &Path,
        sst_name: &str,
        expected_size_bytes: Option<u64>,
        expected_crc32c: Option<u32>,
    ) -> bool {
        if !path.exists() {
            return false;
        }

        if let Some(expected_size_bytes) = expected_size_bytes {
            match std::fs::metadata(path) {
                Ok(metadata) if metadata.len() == expected_size_bytes => {}
                _ => return false,
            }
        }

        if expected_crc32c.is_some() {
            let Ok(data) = std::fs::read(path) else {
                return false;
            };
            if Self::validate_sst_bytes_against_proof(
                sst_name,
                &data,
                expected_size_bytes,
                expected_crc32c,
            )
            .is_err()
            {
                return false;
            }
        }

        crate::sst::fs::SstFileIo::open_with_real_fs(path).is_ok()
    }

    fn local_sst_file_matches_manifest(path: &Path, file: &crate::metadata::FileMeta) -> bool {
        Self::local_sst_file_matches_proof(
            path,
            &file.name,
            Some(file.size_bytes),
            file.content_crc32c,
        )
    }

    fn blocking_cloud_list(
        cloud: &crate::storage::cloud::CloudStorage,
        prefix: &str,
    ) -> MidgeResult<Vec<String>> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_list(prefix.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::ListComplete { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(keys) => Ok(keys),
                crate::storage::cloud::CloudOutcome::Err(error) => Err(MidgeError::Internal(
                    format!("cloud list '{}': {}", prefix, error),
                )),
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud list response for '{}': {:?}",
                prefix, other
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud list '{}' timed out or failed: {}",
                prefix, error
            ))),
        }
    }

    fn blocking_cloud_get(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Vec<u8>> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(key.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::GetComplete { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(data) => Ok(data),
                crate::storage::cloud::CloudOutcome::Err(error) => Err(MidgeError::Internal(
                    format!("cloud get '{}': {}", key, error),
                )),
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud get response for '{}': {:?}",
                key, other
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud get '{}' timed out or failed: {}",
                key, error
            ))),
        }
    }

    fn blocking_cloud_get_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Option<Vec<u8>>> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(key.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::GetComplete { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(data) => Ok(Some(data)),
                crate::storage::cloud::CloudOutcome::Err(error)
                    if crate::storage::cloud::is_not_found_error(&error) =>
                {
                    Ok(None)
                }
                crate::storage::cloud::CloudOutcome::Err(error) => Err(MidgeError::Internal(
                    format!("cloud get '{}': {}", key, error),
                )),
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud get response for '{}': {:?}",
                key, other
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud get '{}' timed out or failed: {}",
                key, error
            ))),
        }
    }

    fn blocking_cloud_head_optional(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Option<crate::storage::cloud::ObjectMetadata>> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_head(key.to_string(), tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::HeadComplete { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(metadata) => Ok(Some(metadata)),
                crate::storage::cloud::CloudOutcome::Err(error)
                    if crate::storage::cloud::is_not_found_error(&error) =>
                {
                    Ok(None)
                }
                crate::storage::cloud::CloudOutcome::Err(error) => Err(MidgeError::Internal(
                    format!("cloud head '{}': {}", key, error),
                )),
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud head response for '{}': {:?}",
                key, other
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud head '{}' timed out or failed: {}",
                key, error
            ))),
        }
    }

    fn blocking_cloud_put(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
        data: Vec<u8>,
    ) -> MidgeResult<()> {
        Self::blocking_cloud_put_with_headers(cloud, key, data, vec![])
    }

    fn blocking_cloud_put_with_headers(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> MidgeResult<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_put(key.to_string(), data, headers, tx);
        match rx.recv_timeout(Duration::from_secs(30)) {
            Ok(crate::storage::cloud::CloudEvent::PutComplete { result, .. }) => match result {
                crate::storage::cloud::CloudOutcome::Ok(()) => Ok(()),
                crate::storage::cloud::CloudOutcome::Err(error) => Err(MidgeError::Internal(
                    format!("cloud put '{}': {}", key, error),
                )),
            },
            Ok(other) => Err(MidgeError::Internal(format!(
                "unexpected cloud put response for '{}': {:?}",
                key, other
            ))),
            Err(error) => Err(MidgeError::Internal(format!(
                "cloud put '{}' timed out or failed: {}",
                key, error
            ))),
        }
    }

    fn remote_manifest_sequence_from_metadata(
        file_name: &str,
        data: &[u8],
    ) -> MidgeResult<Option<u64>> {
        match file_name {
            "manifest.json" | "manifest.snapshot.json" => {
                let manifest: crate::metadata::Manifest =
                    serde_json::from_slice(data).map_err(|error| {
                        MidgeError::Internal(format!(
                            "cloud metadata '{}' is invalid: {}",
                            file_name, error
                        ))
                    })?;
                Ok(Some(manifest.last_persisted_sequence))
            }
            _ => Ok(None),
        }
    }

    fn load_local_manifest_for_cloud_metadata_mirror(
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<crate::metadata::Manifest> {
        let fs: Arc<dyn crate::io::traits::Fs> = Arc::new(crate::io::RealFs::new(db_path)?);
        crate::metadata::ManifestPersistence::load_with_fs_and_policy(&fs, recovery_policy)
            .map_err(MidgeError::Internal)
    }

    fn ensure_remote_manifest_metadata_not_ahead(
        cloud: &crate::storage::cloud::CloudStorage,
        local_sequence: u64,
    ) -> MidgeResult<()> {
        for file_name in ["manifest.snapshot.json", "manifest.json"] {
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let Some(data) = Self::blocking_cloud_get_optional(cloud, &key)? else {
                continue;
            };
            let Some(remote_sequence) =
                Self::remote_manifest_sequence_from_metadata(file_name, &data)?
            else {
                continue;
            };
            if remote_sequence > local_sequence {
                return Err(MidgeError::Internal(format!(
                    "stale cloud metadata mirror rejected: remote {file_name} is ahead of local manifest ({remote_sequence} > {local_sequence})"
                )));
            }
        }

        Ok(())
    }

    fn blocking_conditional_cloud_metadata_put(
        cloud: &crate::storage::cloud::CloudStorage,
        file_name: &str,
        key: &str,
        data: Vec<u8>,
        local_manifest_sequence: u64,
    ) -> MidgeResult<()> {
        let headers = match Self::blocking_cloud_head_optional(cloud, key)? {
            Some(metadata) => {
                let etag = metadata.etag.trim().to_string();
                if etag.is_empty() {
                    return Err(MidgeError::Internal(format!(
                        "cloud metadata '{}' cannot be conditionally updated without an etag",
                        key
                    )));
                }
                let current = Self::blocking_cloud_get_optional(cloud, key)?.ok_or_else(|| {
                    MidgeError::Internal(format!(
                        "cloud metadata '{}' disappeared after HEAD precondition",
                        key
                    ))
                })?;
                if let Some(remote_sequence) =
                    Self::remote_manifest_sequence_from_metadata(file_name, &current)?
                {
                    if remote_sequence > local_manifest_sequence {
                        return Err(MidgeError::Internal(format!(
                            "stale cloud metadata mirror rejected: remote {file_name} is ahead of local manifest ({remote_sequence} > {local_manifest_sequence})"
                        )));
                    }
                }
                vec![("If-Match".to_string(), etag)]
            }
            None => vec![("If-None-Match".to_string(), "*".to_string())],
        };

        Self::blocking_cloud_put_with_headers(cloud, key, data, headers)
    }

    fn recovery_staging_fs(db_path: &Path) -> MidgeResult<Arc<dyn crate::io::traits::Fs>> {
        let real = crate::io::real::RealFs::new(db_path).map_err(|error| {
            MidgeError::RecoveryFailed(format!(
                "failed to initialize recovery staging filesystem: {}",
                error
            ))
        })?;
        Ok(Arc::new(real))
    }

    fn hydrate_cloud_metadata(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        let staging_fs = Self::recovery_staging_fs(db_path)?;
        let mut metadata_objects = Vec::new();
        let mut snapshot_sequence = None;
        let mut manifest_sequence = None;
        let mut has_manifest_journal = false;

        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            let data = match Self::blocking_cloud_get_optional(cloud, &key) {
                Ok(Some(data)) => data,
                Ok(None) => continue,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    tracing::warn!(%error, key = %key, "skipping cloud metadata object during salvage open");
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to download cloud metadata '{}': {}",
                        key, error
                    )))
                }
            };

            if file_name == &"manifest.journal" {
                has_manifest_journal = true;
            }
            if let Some(sequence) = Self::remote_manifest_sequence_from_metadata(file_name, &data)?
            {
                match *file_name {
                    "manifest.snapshot.json" => snapshot_sequence = Some(sequence),
                    "manifest.json" => manifest_sequence = Some(sequence),
                    _ => {}
                }
            }

            metadata_objects.push((*file_name, data));
        }

        let mut metadata_to_skip = None;
        if !has_manifest_journal {
            if let (Some(snapshot), Some(manifest)) = (snapshot_sequence, manifest_sequence) {
                if snapshot != manifest {
                    if recovery_policy == RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "mixed cloud manifest metadata without journal: manifest.snapshot.json sequence {snapshot}, manifest.json sequence {manifest}"
                        )));
                    }
                    let skip_metadata = if manifest >= snapshot {
                        "manifest.snapshot.json"
                    } else {
                        "manifest.json"
                    };
                    metadata_to_skip = Some(skip_metadata);
                    tracing::warn!(
                        snapshot_sequence = snapshot,
                        manifest_sequence = manifest,
                        skip = skip_metadata,
                        "skipping mixed cloud manifest metadata during salvage open"
                    );
                }
            }
        }

        for (file_name, data) in metadata_objects {
            if metadata_to_skip == Some(file_name) {
                continue;
            }
            let temp_path = crate::io::traits::FsPath::new(format!("{file_name}.tmp"));
            let target_path = crate::io::traits::FsPath::new(file_name);
            crate::io::staging::stage_bytes(
                &staging_fs,
                &temp_path,
                &target_path,
                &data,
                MidgeError::RecoveryFailed,
            )?;
        }

        Ok(())
    }

    fn mirror_cloud_metadata(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        let local_manifest = match Self::load_local_manifest_for_cloud_metadata_mirror(
            db_path,
            recovery_policy,
        ) {
            Ok(manifest) => manifest,
            Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                tracing::warn!(%error, "skipping metadata mirror during salvage open because local manifest could not be loaded");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let local_manifest_sequence = local_manifest.last_persisted_sequence;

        Self::ensure_remote_manifest_metadata_not_ahead(cloud, local_manifest_sequence)?;

        for file_name in crate::storage::cloud::CLOUD_METADATA_FILES {
            let local_path = db_path.join(file_name);
            if !local_path.exists() {
                continue;
            }

            let data = match std::fs::read(&local_path) {
                Ok(data) => data,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    tracing::warn!(%error, file = %local_path.display(), "skipping metadata mirror during salvage open");
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to read local metadata '{}': {}",
                        local_path.display(),
                        error
                    )))
                }
            };

            let key = crate::storage::cloud::cloud_metadata_key(file_name);
            if let Err(error) = Self::blocking_conditional_cloud_metadata_put(
                cloud,
                file_name,
                &key,
                data,
                local_manifest_sequence,
            ) {
                if recovery_policy == RecoveryPolicy::Salvage {
                    tracing::warn!(%error, key = %key, "skipping metadata mirror during salvage open");
                    continue;
                }
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to mirror cloud metadata '{}': {}",
                    key, error
                )));
            }
        }

        Ok(())
    }

    fn materialize_cloud_wal_recovery_dir(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<PathBuf> {
        let recovery_wal_dir = db_path.join("cloud_recovery").join("wal");
        if recovery_wal_dir.exists() {
            std::fs::remove_dir_all(&recovery_wal_dir).map_err(|error| {
                MidgeError::RecoveryFailed(format!(
                    "failed to clear cloud WAL recovery directory '{}': {}",
                    recovery_wal_dir.display(),
                    error
                ))
            })?;
        }
        std::fs::create_dir_all(&recovery_wal_dir).map_err(|error| {
            MidgeError::RecoveryFailed(format!(
                "failed to create cloud WAL recovery directory '{}': {}",
                recovery_wal_dir.display(),
                error
            ))
        })?;

        let keys = match Self::blocking_cloud_list(cloud, "wal/") {
            Ok(keys) => keys,
            Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                tracing::warn!(%error, "could not list cloud WAL objects during salvage open");
                return Ok(recovery_wal_dir);
            }
            Err(error) => {
                return Err(MidgeError::RecoveryFailed(format!(
                    "failed to list cloud WAL objects: {}",
                    error
                )))
            }
        };

        let mut segment_keys: std::collections::BTreeMap<u64, String> =
            std::collections::BTreeMap::new();
        let staging_fs = Self::recovery_staging_fs(db_path)?;

        for key in keys {
            let logical_key = cloud.strip_namespace(&key);
            let Some(file_name) = logical_key.strip_prefix("wal/") else {
                continue;
            };
            if file_name.is_empty() || file_name.contains('/') {
                continue;
            }

            let Some(segment_id) = crate::wal::parse_segment_id(logical_key) else {
                continue;
            };

            let prefer_candidate = segment_keys
                .get(&segment_id)
                .map(|existing_key| {
                    existing_key != &crate::wal::cloud_segment_object_key(segment_id)
                        && logical_key == crate::wal::cloud_segment_object_key(segment_id)
                })
                .unwrap_or(true);

            if prefer_candidate {
                segment_keys.insert(segment_id, logical_key.to_string());
            }
        }

        for (segment_id, logical_key) in segment_keys {
            let data = match Self::blocking_cloud_get(cloud, &logical_key) {
                Ok(data) => data,
                Err(error) if recovery_policy == RecoveryPolicy::Salvage => {
                    tracing::warn!(%error, key = %logical_key, "skipping cloud WAL object during salvage open");
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to download cloud WAL '{}': {}",
                        logical_key, error
                    )))
                }
            };

            let staged_file_name = crate::wal::cloud_segment_file_name(segment_id);
            let temp_path = crate::io::traits::FsPath::new(format!(
                "cloud_recovery/wal/{staged_file_name}.tmp"
            ));
            let target_path =
                crate::io::traits::FsPath::new(format!("cloud_recovery/wal/{staged_file_name}"));
            crate::io::staging::stage_bytes(
                &staging_fs,
                &temp_path,
                &target_path,
                &data,
                MidgeError::RecoveryFailed,
            )?;
        }

        Ok(recovery_wal_dir)
    }

    fn ensure_local_sst_cache_from_cloud_storage(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
    ) -> MidgeResult<()> {
        let staging_fs = state.fs.clone();
        let mut retained_files = Vec::with_capacity(state.manifest.files.len());
        let mut manifest_changed = false;

        for file in state.manifest.files.clone() {
            let cloud_key = crate::sst::object_key(&file.name);
            let local_path = state.sst_dir.join(&file.name);
            let local_valid = Self::local_sst_file_matches_manifest(&local_path, &file);
            if local_valid {
                match Self::blocking_cloud_head_optional(cloud, &cloud_key) {
                    Ok(Some(metadata)) if metadata.size == file.size_bytes => {
                        if file.content_crc32c.is_some() {
                            match Self::blocking_cloud_get_optional(cloud, &cloud_key) {
                                Ok(Some(data)) => {
                                    if let Err(error) = Self::validate_sst_bytes_against_proof(
                                        &file.name,
                                        &data,
                                        Some(file.size_bytes),
                                        file.content_crc32c,
                                    ) {
                                        if state.recovery_policy == RecoveryPolicy::Strict {
                                            return Err(error);
                                        }
                                        tracing::warn!(%error, sst_name = %file.name, "retaining locally valid manifest SST during salvage despite invalid cloud object");
                                        state.opened_in_salvage_mode = true;
                                        state.persistence_anomaly_detected = true;
                                    }
                                }
                                Ok(None) => {
                                    if state.recovery_policy == RecoveryPolicy::Strict {
                                        return Err(MidgeError::RecoveryFailed(format!(
                                            "authoritative cloud SST '{}' is missing",
                                            file.name
                                        )));
                                    }
                                    tracing::warn!(
                                        sst_name = %file.name,
                                        "retaining locally valid manifest SST during salvage despite missing cloud object"
                                    );
                                    state.opened_in_salvage_mode = true;
                                    state.persistence_anomaly_detected = true;
                                }
                                Err(error) if state.recovery_policy == RecoveryPolicy::Salvage => {
                                    tracing::warn!(%error, sst_name = %file.name, "retaining locally valid manifest SST during salvage despite remote content validation failure");
                                    state.opened_in_salvage_mode = true;
                                    state.persistence_anomaly_detected = true;
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    }
                    Ok(Some(metadata)) => {
                        if state.recovery_policy == RecoveryPolicy::Strict {
                            return Err(MidgeError::RecoveryFailed(format!(
                                "authoritative cloud SST '{}' size {} does not match manifest {}",
                                file.name, metadata.size, file.size_bytes
                            )));
                        }
                        state.opened_in_salvage_mode = true;
                        state.persistence_anomaly_detected = true;
                        tracing::warn!(
                            sst_name = %file.name,
                            cloud_size = metadata.size,
                            manifest_size = file.size_bytes,
                            "retaining locally valid manifest SST during salvage despite cloud size mismatch"
                        );
                    }
                    Ok(None) => {
                        if state.recovery_policy == RecoveryPolicy::Strict {
                            return Err(MidgeError::RecoveryFailed(format!(
                                "authoritative cloud SST '{}' is missing",
                                file.name
                            )));
                        }
                        state.opened_in_salvage_mode = true;
                        state.persistence_anomaly_detected = true;
                        tracing::warn!(
                            sst_name = %file.name,
                            "retaining locally valid manifest SST during salvage despite missing cloud object"
                        );
                    }
                    Err(error) if state.recovery_policy == RecoveryPolicy::Salvage => {
                        tracing::warn!(%error, sst_name = %file.name, "retaining locally valid manifest SST during salvage despite remote validation failure");
                        state.opened_in_salvage_mode = true;
                        state.persistence_anomaly_detected = true;
                    }
                    Err(error) => {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to validate cloud SST '{}': {}",
                            file.name, error
                        )));
                    }
                }
            } else {
                let data = match Self::blocking_cloud_get_optional(cloud, &cloud_key) {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        if state.recovery_policy == RecoveryPolicy::Strict {
                            return Err(MidgeError::RecoveryFailed(format!(
                                "authoritative cloud SST '{}' is missing",
                                file.name
                            )));
                        }
                        state.opened_in_salvage_mode = true;
                        state.persistence_anomaly_detected = true;
                        manifest_changed = true;
                        continue;
                    }
                    Err(error) if state.recovery_policy == RecoveryPolicy::Salvage => {
                        tracing::warn!(%error, sst_name = %file.name, "dropping manifest SST during salvage restore");
                        state.opened_in_salvage_mode = true;
                        state.persistence_anomaly_detected = true;
                        manifest_changed = true;
                        continue;
                    }
                    Err(error) => {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to restore cloud SST '{}': {}",
                            file.name, error
                        )));
                    }
                };

                if let Err(error) = Self::validate_sst_bytes_against_proof(
                    &file.name,
                    &data,
                    Some(file.size_bytes),
                    file.content_crc32c,
                ) {
                    if state.recovery_policy == RecoveryPolicy::Strict {
                        return Err(error);
                    }
                    state.opened_in_salvage_mode = true;
                    state.persistence_anomaly_detected = true;
                    manifest_changed = true;
                    continue;
                }

                let temp_path =
                    crate::io::traits::FsPath::new(crate::sst::temp_object_key(&file.name));
                let target_path =
                    crate::io::traits::FsPath::new(crate::sst::object_key(&file.name));
                crate::io::staging::stage_bytes(
                    &staging_fs,
                    &temp_path,
                    &target_path,
                    &data,
                    MidgeError::RecoveryFailed,
                )?;

                if let Err(error) = crate::sst::fs::SstFileIo::open_with_real_fs(&local_path) {
                    if state.recovery_policy == RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "restored cloud SST '{}' is invalid: {}",
                            file.name, error
                        )));
                    }
                    state.opened_in_salvage_mode = true;
                    state.persistence_anomaly_detected = true;
                    manifest_changed = true;
                    let _ = std::fs::remove_file(&local_path);
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

    fn ensure_named_sst_cache_from_cloud_storage(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
        sst_proofs: impl IntoIterator<Item = CloudSstRecoveryProof>,
    ) -> MidgeResult<()> {
        let staging_fs = state.fs.clone();

        for proof in sst_proofs {
            let sst_name = proof.name;
            let cloud_key = crate::sst::object_key(&sst_name);
            let local_path = state.sst_dir.join(&sst_name);
            let local_valid = Self::local_sst_file_matches_proof(
                &local_path,
                &sst_name,
                proof.expected_size_bytes,
                proof.expected_crc32c,
            );
            if local_valid {
                match Self::blocking_cloud_head_optional(cloud, &cloud_key) {
                    Ok(Some(metadata))
                        if proof
                            .expected_size_bytes
                            .is_none_or(|expected| metadata.size == expected) =>
                    {
                        if proof.expected_crc32c.is_some() {
                            match Self::blocking_cloud_get_optional(cloud, &cloud_key) {
                                Ok(Some(data)) => {
                                    if let Err(error) = Self::validate_sst_bytes_against_proof(
                                        &sst_name,
                                        &data,
                                        proof.expected_size_bytes,
                                        proof.expected_crc32c,
                                    ) {
                                        if state.recovery_policy == RecoveryPolicy::Strict {
                                            return Err(error);
                                        }
                                        state.opened_in_salvage_mode = true;
                                        state.persistence_anomaly_detected = true;
                                        tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage content validation");
                                    }
                                }
                                Ok(None) => {
                                    if state.recovery_policy == RecoveryPolicy::Strict {
                                        return Err(MidgeError::RecoveryFailed(format!(
                                            "authoritative cloud SST '{}' is missing",
                                            sst_name
                                        )));
                                    }
                                    state.opened_in_salvage_mode = true;
                                    state.persistence_anomaly_detected = true;
                                    tracing::warn!(
                                        sst_name = %sst_name,
                                        "skipping cloud SST staging because authoritative object is missing"
                                    );
                                }
                                Err(error) if state.recovery_policy == RecoveryPolicy::Salvage => {
                                    state.opened_in_salvage_mode = true;
                                    state.persistence_anomaly_detected = true;
                                    tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage content validation");
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    }
                    Ok(Some(metadata)) => {
                        let error = MidgeError::RecoveryFailed(format!(
                            "authoritative cloud SST '{}' size {} does not match expected {:?}",
                            sst_name, metadata.size, proof.expected_size_bytes
                        ));
                        if state.recovery_policy == RecoveryPolicy::Strict {
                            return Err(error);
                        }
                        state.opened_in_salvage_mode = true;
                        state.persistence_anomaly_detected = true;
                        tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage size validation");
                    }
                    Ok(None) => {
                        if state.recovery_policy == RecoveryPolicy::Strict {
                            return Err(MidgeError::RecoveryFailed(format!(
                                "authoritative cloud SST '{}' is missing",
                                sst_name
                            )));
                        }
                        state.opened_in_salvage_mode = true;
                        state.persistence_anomaly_detected = true;
                        tracing::warn!(
                            sst_name = %sst_name,
                            "skipping cloud SST staging because authoritative object is missing"
                        );
                    }
                    Err(error) if state.recovery_policy == RecoveryPolicy::Salvage => {
                        state.opened_in_salvage_mode = true;
                        state.persistence_anomaly_detected = true;
                        tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage validation");
                    }
                    Err(error) => {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "failed to validate cloud SST '{}': {}",
                            sst_name, error
                        )))
                    }
                }
                continue;
            }

            let data = match Self::blocking_cloud_get_optional(cloud, &cloud_key) {
                Ok(Some(data)) => data,
                Ok(None) => {
                    if state.recovery_policy == RecoveryPolicy::Strict {
                        return Err(MidgeError::RecoveryFailed(format!(
                            "authoritative cloud SST '{}' is missing",
                            sst_name
                        )));
                    }
                    state.opened_in_salvage_mode = true;
                    state.persistence_anomaly_detected = true;
                    tracing::warn!(
                        sst_name = %sst_name,
                        "skipping cloud SST staging because authoritative object is missing"
                    );
                    continue;
                }
                Err(error) if state.recovery_policy == RecoveryPolicy::Salvage => {
                    state.opened_in_salvage_mode = true;
                    state.persistence_anomaly_detected = true;
                    tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage");
                    continue;
                }
                Err(error) => {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "failed to restore cloud SST '{}': {}",
                        sst_name, error
                    )))
                }
            };

            if let Err(error) = Self::validate_sst_bytes_against_proof(
                &sst_name,
                &data,
                proof.expected_size_bytes,
                proof.expected_crc32c,
            ) {
                if state.recovery_policy == RecoveryPolicy::Strict {
                    return Err(error);
                }
                state.opened_in_salvage_mode = true;
                state.persistence_anomaly_detected = true;
                tracing::warn!(%error, sst_name = %sst_name, "skipping cloud SST staging during salvage proof validation");
                continue;
            }

            let temp_path = crate::io::traits::FsPath::new(crate::sst::temp_object_key(&sst_name));
            let target_path = crate::io::traits::FsPath::new(crate::sst::object_key(&sst_name));
            crate::io::staging::stage_bytes(
                &staging_fs,
                &temp_path,
                &target_path,
                &data,
                MidgeError::RecoveryFailed,
            )?;

            if let Err(error) = crate::sst::fs::SstFileIo::open_with_real_fs(&local_path) {
                if state.recovery_policy == RecoveryPolicy::Strict {
                    return Err(MidgeError::RecoveryFailed(format!(
                        "restored cloud SST '{}' is invalid: {}",
                        sst_name, error
                    )));
                }
                state.opened_in_salvage_mode = true;
                state.persistence_anomaly_detected = true;
                let _ = std::fs::remove_file(&local_path);
                tracing::warn!(
                    sst_name = %sst_name,
                    error = %error,
                    "discarding invalid cloud SST during salvage staging"
                );
            }
        }

        Ok(())
    }

    fn cloud_recovery_sst_proofs_for_intent_replay(
        state: &RuntimeState,
    ) -> Vec<CloudSstRecoveryProof> {
        let mut proofs = std::collections::BTreeMap::<String, CloudSstRecoveryProof>::new();
        for file in &state.manifest.files {
            proofs
                .entry(file.name.clone())
                .and_modify(|proof| proof.merge_from(CloudSstRecoveryProof::from_manifest(file)))
                .or_insert_with(|| CloudSstRecoveryProof::from_manifest(file));
        }
        for intent in &state.intent_log {
            match intent {
                crate::runtime::IntentLogEntry::FlushPublish { file_meta, .. }
                | crate::runtime::IntentLogEntry::SstAdded { file_meta } => {
                    proofs
                        .entry(file_meta.name.clone())
                        .and_modify(|proof| {
                            proof.merge_from(CloudSstRecoveryProof::from_runtime(file_meta))
                        })
                        .or_insert_with(|| CloudSstRecoveryProof::from_runtime(file_meta));
                }
                crate::runtime::IntentLogEntry::CompactionPublish { added, .. }
                | crate::runtime::IntentLogEntry::CompactionApplied { added, .. } => {
                    for file_meta in added {
                        proofs
                            .entry(file_meta.name.clone())
                            .and_modify(|proof| {
                                proof.merge_from(CloudSstRecoveryProof::from_runtime(file_meta))
                            })
                            .or_insert_with(|| CloudSstRecoveryProof::from_runtime(file_meta));
                    }
                }
                _ => {}
            }
        }
        proofs.into_values().collect()
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
            }
            | Storage::CloudSimulated {
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
        let mut cloud_storage_for_restore: Option<Arc<crate::storage::cloud::CloudStorage>> = None;
        let cloud_runtime_policy = opts.cloud_runtime_policy.clone().unwrap_or_default();
        let (mut state, runtime_config) = match &opts.storage {
            Storage::CloudSimulated { .. } => {
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
                    cloud_runtime_policy: cloud_runtime_policy.clone(),
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
            Storage::Cloud {
                provider, prefix, ..
            } => {
                let cloud_storage =
                    crate::storage::providers::build_cloud_storage(provider, prefix)?;
                Self::hydrate_cloud_metadata(&cloud_storage, &db_path, opts.recovery_policy)?;
                let recovery_wal_dir = Self::materialize_cloud_wal_recovery_dir(
                    &cloud_storage,
                    &db_path,
                    opts.recovery_policy,
                )?;

                let local_backend = Arc::new(crate::storage::filesystem::FileSystem::new(
                    db_path.join("hybrid_local"),
                )?);
                let cloud_backend: Arc<dyn crate::storage::StorageBackend> = cloud_storage.clone();

                let (tx, rx) = crossbeam::channel::unbounded::<crate::storage::StorageEvent>();
                let hybrid_storage =
                    Arc::new(crate::storage::HybridStorage::new_with_event_sender(
                        local_backend,
                        cloud_backend,
                        tx,
                    ));

                let state = RuntimeState::try_new_with_recovery_dir(
                    db_path.clone(),
                    memory_mode,
                    Some(recovery_wal_dir),
                    opts.recovery_policy,
                )?;

                let config = crate::runtime::RuntimeConfig {
                    wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
                    cloud_runtime_policy: cloud_runtime_policy.clone(),
                    hybrid_storage: Some(hybrid_storage),
                    hybrid_storage_events: Some(rx),
                    cloud_metadata_storage: Some(cloud_storage.clone()),
                    compression_policy: opts.compression_policy.clone(),
                    writer_epoch,
                    lease_healthy: Some(Arc::clone(&lease_healthy)),
                    leader_store: leader_store.clone(),
                    ..Default::default()
                };

                cloud_storage_for_restore = Some(cloud_storage);
                (state, config)
            }
            _ => {
                // Local or Memory mode: use Batched durability with optional batch config from OpenOptions
                let batch_config = opts.wal_batch_config.unwrap_or_default();

                let config = crate::runtime::RuntimeConfig {
                    wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
                    wal_batch_config: batch_config,
                    cloud_runtime_policy: cloud_runtime_policy.clone(),
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

        if let Some(cloud_storage) = cloud_storage_for_restore.as_deref() {
            let sst_proofs = Self::cloud_recovery_sst_proofs_for_intent_replay(&state);
            Self::ensure_named_sst_cache_from_cloud_storage(&mut state, cloud_storage, sst_proofs)?;
        }

        // 🔑 CRITICAL: Replay intent log to recover any interrupted mutations.
        // For real cloud mode, SSTs referenced by publish intents have already
        // been staged locally so replay validation can stay local and strict.
        state.replay_intent_log()?;
        if let Some(root) = cloud_root.as_deref() {
            Self::ensure_local_sst_cache_from_cloud(&mut state, root)?;
        }
        if let Some(cloud_storage) = cloud_storage_for_restore.as_deref() {
            Self::ensure_local_sst_cache_from_cloud_storage(&mut state, cloud_storage)?;
            Self::mirror_cloud_metadata(cloud_storage, &db_path, opts.recovery_policy)?;
        }
        state.cleanup_storage_residue();
        let recovered_sequence = state.sequence;
        let recovered_cf_metas = state.manifest.column_families.clone();

        // Start runtime
        let (runtime_inst, _) = Runtime::new()?;
        let (runtime, runtime_handle) = runtime_inst.start_with_config(state, runtime_config)?;

        // Apply derived OpenOptions to runtime: preserve the legacy
        // `MemoryBudget::Bytes(n) -> n / 2` runtime memtable sizing unless the
        // caller explicitly overrode memtable sizing via OpenOptions.
        let memtable_size_limit = opts.runtime_memtable_size_limit();
        let memtable_flush_threshold = opts.runtime_memtable_flush_threshold();

        let request_id = crate::runtime::next_request_id()?;
        let resp = runtime_handle.send_and_wait(crate::runtime::RuntimeMsg::SetRuntimeConfig {
            request_id,
            memtable_size_limit: Some(memtable_size_limit),
            memtable_flush_threshold: Some(memtable_flush_threshold),
            enable_compaction: None,
            l0_compaction_trigger: Some(opts.l0_compaction_trigger()),
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
            cloud_mode: matches!(
                &opts.storage,
                Storage::Cloud { .. } | Storage::CloudSimulated { .. }
            ),
            recovery_policy: opts.recovery_policy,
            sequence: Arc::new(std::sync::atomic::AtomicU64::new(recovered_sequence)),
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
                l0_compaction_trigger,
                wal_durability_policy,
                wal_batch_config,
                ..
            } => Ok(IngestModeSnapshot {
                memtable_size_limit,
                memtable_flush_threshold,
                enable_compaction,
                l0_compaction_trigger,
                wal_durability_policy,
                wal_batch_config,
            }),
            _ => Err(crate::common::MidgeError::Internal(
                "unexpected response to GetRuntimeConfig".to_string(),
            )),
        }
    }

    pub(crate) fn set_runtime_compaction_enabled(&self, enabled: bool) -> MidgeResult<()> {
        let request_id = crate::runtime::next_request_id()?;
        let resp =
            self.runtime_handle
                .send_and_wait(crate::runtime::RuntimeMsg::SetRuntimeConfig {
                    request_id,
                    memtable_size_limit: None,
                    memtable_flush_threshold: None,
                    enable_compaction: Some(enabled),
                    l0_compaction_trigger: None,
                    wal_durability_policy: None,
                    wal_batch_config: None,
                })?;

        match resp {
            crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
            crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(crate::common::MidgeError::Internal(
                "unexpected response to SetRuntimeConfig".to_string(),
            )),
        }
    }

    pub(crate) fn kick_runtime_compaction_once(&self) -> MidgeResult<()> {
        let request_id = crate::runtime::next_request_id()?;
        let resp = self
            .runtime_handle
            .send_and_wait(crate::runtime::RuntimeMsg::CheckCompaction { request_id })?;

        match resp {
            crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
            crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(crate::common::MidgeError::Internal(
                "unexpected response to CheckCompaction".to_string(),
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
                    l0_compaction_trigger: None,
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
                        l0_compaction_trigger: Some(prev.l0_compaction_trigger),
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
        let pinned_sst_names = read_snapshot
            .as_ref()
            .map(|snapshot| {
                snapshot
                    .sst_files
                    .iter()
                    .map(|file| file.name.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        // Drop the guard ASAP to avoid holding the ArcSwap lease.
        drop(cache_guard);

        match self
            .runtime_handle
            .send_and_wait(RuntimeMsg::RegisterSnapshot {
                request_id: next_request_id()?,
                snapshot_id: txn_id,
                sequence: start_sequence,
                pinned_sst_names,
            })? {
            RuntimeResponse::Ok { .. } => {}
            RuntimeResponse::Error { error, .. } => return Err(error),
            _ => {
                return Err(MidgeError::Internal(
                    "Unexpected response to RegisterSnapshot".to_string(),
                ))
            }
        }

        let coordinator = self.ingest_coordinators.get(&cf_id).ok_or_else(|| {
            MidgeError::InvalidArgument(format!("column family {} does not exist", cf_id))
        })?;

        Ok(api::Transaction::new(api::TransactionInit {
            runtime_handle: self.runtime_handle.clone(),
            coordinator: coordinator.clone(),
            sequence_publisher: Arc::clone(&self.sequence),
            id: txn_id,
            cf_id,
            mode,
            start_sequence,
            read_snapshot,
            cloud_mode: self.cloud_mode,
        }))
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
            RuntimeResponse::RuntimeMetricsSnapshot { snapshot, .. } => Ok(*snapshot),
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
        // Arrange
        let cf_id: ColumnFamilyId = 0;

        // Act

        // Assert
        assert_eq!(cf_id, 0);
    }

    #[test]
    fn should_preserve_custom_column_family_id_value() {
        // Arrange
        let custom_id: ColumnFamilyId = 42;

        // Act

        // Assert
        assert_eq!(custom_id, 42);
    }

    #[test]
    fn should_support_column_family_id_equality() {
        // Arrange
        let id1: ColumnFamilyId = 5;
        let id2: ColumnFamilyId = 5;
        let id3: ColumnFamilyId = 6;

        // Act

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

    #[test]
    fn should_apply_open_options_l0_compaction_trigger_to_runtime() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let opts = OpenOptions::local(temp_dir.path())
            .goal(Goal::Throughput)
            .workload(WorkloadProfile::WriteHeavy)
            .build();
        let expected_trigger = opts.l0_compaction_trigger();

        let engine = Engine::open(opts).expect("open engine");
        let runtime_config = engine
            .get_runtime_config()
            .expect("read runtime configuration");

        assert_eq!(
            runtime_config.l0_compaction_trigger, expected_trigger,
            "runtime compaction actor should use the OpenOptions-derived L0 trigger"
        );
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

        // Act

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
        // Arrange
        let handle = ColumnFamilyHandle::new(1, "".to_string());

        // Act

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
        // Arrange
        let max_id: ColumnFamilyId = u32::MAX;

        // Act

        // Assert
        assert_eq!(max_id, u32::MAX);
    }

    #[test]
    fn should_handle_zero_column_family_id() {
        // Arrange
        let zero_id: ColumnFamilyId = 0;

        // Act

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
    fn should_treat_flush_compact_as_noop_in_memory_mode() {
        // Arrange
        let opts = crate::testkit::MidgeOptions {
            storage_mode: crate::testkit::StorageMode::Memory,
            ..Default::default()
        };

        // Act
        let engine = Engine::open_with_options(opts).expect("open memory engine");
        let cf = engine
            .create_column_family("test")
            .expect("create column family");

        // Assert
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
        // Arrange
        let handle = ColumnFamilyHandle::new(0, "default".to_string());

        // Act

        // Assert
        assert_eq!(handle.id(), 0);
        assert_eq!(handle.name(), "default");
    }

    #[test]
    fn should_create_multiple_handles_with_different_ids() {
        // Arrange
        let handle1 = ColumnFamilyHandle::new(1, "cf1".to_string());
        let handle2 = ColumnFamilyHandle::new(2, "cf2".to_string());
        let handle3 = ColumnFamilyHandle::new(3, "cf3".to_string());

        // Act

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

    #[test]
    fn should_stage_cloud_wal_segments_with_canonical_padded_names() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());

        Engine::blocking_cloud_put(&cloud, "wal/1.wal", b"legacy".to_vec())
            .expect("upload legacy wal object");
        Engine::blocking_cloud_put(
            &cloud,
            &crate::wal::cloud_segment_object_key(1),
            b"canonical".to_vec(),
        )
        .expect("upload canonical wal object");
        Engine::blocking_cloud_put(&cloud, "wal/wal_000002.log", b"second".to_vec())
            .expect("upload legacy log-style wal object");

        let staged_dir = Engine::materialize_cloud_wal_recovery_dir(
            &cloud,
            temp_dir.path(),
            RecoveryPolicy::Strict,
        )
        .expect("materialize cloud wal recovery dir");

        let mut staged_files: Vec<String> = std::fs::read_dir(&staged_dir)
            .expect("read staged wal dir")
            .map(|entry| {
                entry
                    .expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        staged_files.sort();

        assert_eq!(
            staged_files,
            vec![
                crate::wal::cloud_segment_file_name(1),
                crate::wal::cloud_segment_file_name(2),
            ]
        );
        assert_eq!(
            std::fs::read(staged_dir.join(crate::wal::cloud_segment_file_name(1)))
                .expect("read staged wal 1"),
            b"canonical"
        );
        assert_eq!(
            std::fs::read(staged_dir.join(crate::wal::cloud_segment_file_name(2)))
                .expect("read staged wal 2"),
            b"second"
        );
    }

    #[test]
    fn should_not_overwrite_newer_remote_manifest_metadata_during_engine_mirror() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
        let local_manifest = crate::metadata::Manifest {
            last_persisted_sequence: 20,
            ..Default::default()
        };
        crate::metadata::ManifestPersistence::save(temp_dir.path(), &local_manifest)
            .expect("save local manifest");
        let remote_manifest = crate::metadata::Manifest {
            last_persisted_sequence: 21,
            ..Default::default()
        };
        Engine::blocking_cloud_put(
            &cloud,
            "metadata/manifest.json",
            serde_json::to_vec_pretty(&remote_manifest).expect("serialize remote manifest"),
        )
        .expect("upload newer remote manifest");

        let error = Engine::mirror_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Strict)
            .expect_err("newer remote manifest metadata must reject stale engine mirror");

        assert!(
            error.to_string().contains("newer")
                || error.to_string().contains("ahead")
                || error.to_string().contains("stale"),
            "unexpected stale engine metadata mirror error: {error}"
        );
        let retained: crate::metadata::Manifest = serde_json::from_slice(
            &Engine::blocking_cloud_get(&cloud, "metadata/manifest.json")
                .expect("download retained remote manifest"),
        )
        .expect("parse retained remote manifest");
        assert_eq!(
            retained.last_persisted_sequence, 21,
            "engine metadata mirror must not overwrite newer remote manifest"
        );
    }

    #[test]
    fn should_hydrate_cloud_metadata_when_listing_is_stale_but_object_is_readable() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let inner = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let backend = Arc::new(ListOmittingCloudBackend::new(
            Arc::clone(&inner),
            "metadata/",
        ));
        let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
        let remote_manifest = crate::metadata::Manifest {
            last_persisted_sequence: 42,
            ..Default::default()
        };
        Engine::blocking_cloud_put(
            &cloud,
            "metadata/manifest.json",
            serde_json::to_vec_pretty(&remote_manifest).expect("serialize remote manifest"),
        )
        .expect("upload readable remote manifest metadata");

        Engine::hydrate_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Strict)
            .expect("stale metadata list must not hide directly readable metadata");

        let hydrated = crate::metadata::ManifestPersistence::load(temp_dir.path())
            .expect("load hydrated manifest");
        assert_eq!(
            hydrated.last_persisted_sequence, 42,
            "metadata hydration must probe known metadata keys directly"
        );
    }

    #[test]
    fn should_reject_mixed_cloud_manifest_metadata_without_journal() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
        let snapshot_manifest = crate::metadata::Manifest {
            last_persisted_sequence: 10,
            ..Default::default()
        };
        let current_manifest = crate::metadata::Manifest {
            last_persisted_sequence: 11,
            ..Default::default()
        };
        Engine::blocking_cloud_put(
            &cloud,
            "metadata/manifest.snapshot.json",
            serde_json::to_vec_pretty(&snapshot_manifest).expect("serialize snapshot manifest"),
        )
        .expect("upload stale snapshot");
        Engine::blocking_cloud_put(
            &cloud,
            "metadata/manifest.json",
            serde_json::to_vec_pretty(&current_manifest).expect("serialize current manifest"),
        )
        .expect("upload newer manifest");

        let error = Engine::hydrate_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Strict)
            .expect_err("strict hydration must reject mixed manifest metadata without journal");

        assert!(
            error.to_string().contains("mixed")
                || error.to_string().contains("inconsistent")
                || error.to_string().contains("sequence"),
            "unexpected mixed metadata error: {error}"
        );
    }

    #[test]
    fn should_salvage_mixed_cloud_manifest_metadata_by_retaining_highest_sequence() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let backend = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let cloud = crate::storage::cloud::CloudStorage::new(backend, "midge".to_string());
        let snapshot_manifest = crate::metadata::Manifest {
            last_persisted_sequence: 10,
            ..Default::default()
        };
        let current_manifest = crate::metadata::Manifest {
            last_persisted_sequence: 11,
            ..Default::default()
        };
        Engine::blocking_cloud_put(
            &cloud,
            "metadata/manifest.snapshot.json",
            serde_json::to_vec_pretty(&snapshot_manifest).expect("serialize snapshot manifest"),
        )
        .expect("upload stale snapshot");
        Engine::blocking_cloud_put(
            &cloud,
            "metadata/manifest.json",
            serde_json::to_vec_pretty(&current_manifest).expect("serialize current manifest"),
        )
        .expect("upload newer manifest");

        Engine::hydrate_cloud_metadata(&cloud, temp_dir.path(), RecoveryPolicy::Salvage)
            .expect("salvage hydration should retain the highest sequence manifest metadata");

        let hydrated = crate::metadata::ManifestPersistence::load(temp_dir.path())
            .expect("load salvaged manifest metadata");
        assert_eq!(
            hydrated.last_persisted_sequence, 11,
            "salvage hydration must not let a stale snapshot hide a newer manifest"
        );
    }

    struct ListOmittingCloudBackend {
        inner: Arc<crate::storage::cloud::MockCloudBackend>,
        omitted_prefix: String,
    }

    impl ListOmittingCloudBackend {
        fn new(
            inner: Arc<crate::storage::cloud::MockCloudBackend>,
            omitted_prefix: impl Into<String>,
        ) -> Self {
            Self {
                inner,
                omitted_prefix: omitted_prefix.into(),
            }
        }
    }

    impl crate::storage::cloud::CloudBackend for ListOmittingCloudBackend {
        fn submit_put(
            &self,
            key: String,
            data: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            self.inner.submit_put(key, data, headers, callback);
        }

        fn submit_get(&self, key: String, callback: crate::storage::cloud::CloudCallback) {
            self.inner.submit_get(key, callback);
        }

        fn submit_get_range(
            &self,
            key: String,
            start: u64,
            end: Option<u64>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            self.inner.submit_get_range(key, start, end, callback);
        }

        fn submit_delete(
            &self,
            key: String,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            self.inner.submit_delete(key, headers, callback);
        }

        fn submit_list(&self, prefix: String, callback: crate::storage::cloud::CloudCallback) {
            if prefix.ends_with(&self.omitted_prefix) {
                let _ = callback.send(crate::storage::cloud::CloudEvent::ListComplete {
                    prefix,
                    result: crate::storage::cloud::CloudOutcome::Ok(Vec::new()),
                });
                return;
            }
            self.inner.submit_list(prefix, callback);
        }

        fn submit_head(&self, key: String, callback: crate::storage::cloud::CloudCallback) {
            self.inner.submit_head(key, callback);
        }
    }

    fn test_sst_bytes_with_key_value(key: &[u8], value: &[u8]) -> Vec<u8> {
        use crate::sst::traits::SstFactory;

        let factory = crate::sst::FsSstFactoryIo::new(Arc::new(crate::io::MockFs::new()), 4096);
        let mut writer = factory.create().expect("create test sst writer");
        writer
            .add_with_meta(key, Some(value), 1, 0, None)
            .expect("write test sst entry");
        writer.finish_bytes().expect("finish test sst bytes")
    }

    fn test_sst_bytes_with_value(value: &[u8]) -> Vec<u8> {
        test_sst_bytes_with_key_value(b"cloud-list-key", value)
    }

    fn test_sst_bytes() -> Vec<u8> {
        test_sst_bytes_with_value(b"cloud-list-value")
    }

    fn same_size_sst_with_different_crc(bytes: &[u8]) -> Vec<u8> {
        assert!(bytes.len() > 32, "test SST must include an extended footer");
        let mut changed = bytes.to_vec();
        let footer_block_bloom_byte = changed.len() - 16;
        changed[footer_block_bloom_byte] ^= 0x01;
        assert_eq!(changed.len(), bytes.len());
        assert_ne!(crc32c::crc32c(&changed), crc32c::crc32c(bytes));

        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir.path().join("changed.sst");
        std::fs::write(&path, &changed).expect("write changed SST");
        crate::sst::fs::SstFileIo::open_with_real_fs(&path)
            .expect("changed same-size SST should remain structurally readable");

        changed
    }

    fn cloud_with_stale_sst_listing() -> crate::storage::cloud::CloudStorage {
        let inner = Arc::new(crate::storage::cloud::MockCloudBackend::new());
        let backend = Arc::new(ListOmittingCloudBackend::new(Arc::clone(&inner), "sst/"));
        crate::storage::cloud::CloudStorage::new(backend, "midge".to_string())
    }

    #[test]
    fn should_restore_manifest_sst_when_cloud_listing_is_stale_but_object_is_readable() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Strict,
        )
        .expect("create runtime state");
        let sst_name = crate::sst::file_name(0, 0, 1);
        let sst_bytes = test_sst_bytes();
        state.manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: sst_bytes.len() as u64,
            cf_id: 0,
            sst_seq: 1,
            smallest_key: Some(b"cloud-list-key".to_vec()),
            largest_key: Some(b"cloud-list-key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        });
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), sst_bytes)
            .expect("upload test sst");

        Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect("stale list should not make readable manifest SST unrecoverable");

        assert!(
            state.sst_dir.join(&sst_name).exists(),
            "readable cloud SST should be restored despite stale LIST"
        );
    }

    #[test]
    fn should_reject_manifest_sst_when_cloud_object_size_differs_from_manifest() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Strict,
        )
        .expect("create runtime state");
        let sst_name = crate::sst::file_name(0, 0, 3);
        let committed_sst_bytes = test_sst_bytes_with_value(b"manifest-sized-value");
        let wrong_sst_bytes = test_sst_bytes_with_value(b"different-cloud-object-bytes");
        assert_ne!(
            committed_sst_bytes.len(),
            wrong_sst_bytes.len(),
            "test must use a valid cloud SST with different size than the committed manifest"
        );
        state.manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: committed_sst_bytes.len() as u64,
            cf_id: 0,
            sst_seq: 3,
            smallest_key: Some(b"cloud-list-key".to_vec()),
            largest_key: Some(b"cloud-list-key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        });
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), wrong_sst_bytes)
            .expect("upload wrong-sized but structurally valid test sst");

        let error = Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect_err("strict recovery must reject wrong-sized authoritative cloud SST");

        assert!(
            error.to_string().contains("size"),
            "unexpected wrong-sized cloud SST recovery error: {error}"
        );
        assert!(
            !state.sst_dir.join(&sst_name).exists(),
            "wrong-sized cloud SST must not be installed into the local cache"
        );
    }

    #[test]
    fn should_reject_manifest_sst_when_same_size_cloud_object_crc_differs() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Strict,
        )
        .expect("create runtime state");
        let sst_name = crate::sst::file_name(0, 0, 4);
        let wrong_sst_bytes = test_sst_bytes();
        let expected_crc = crc32c::crc32c(&wrong_sst_bytes) ^ 0xffff_ffff;
        state.manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: wrong_sst_bytes.len() as u64,
            content_crc32c: Some(expected_crc),
            cf_id: 0,
            sst_seq: 4,
            smallest_key: Some(b"cloud-list-key".to_vec()),
            largest_key: Some(b"cloud-list-key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        });
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), wrong_sst_bytes)
            .expect("upload same-sized but wrong-content test sst");

        let error = Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect_err("strict recovery must reject wrong-content authoritative cloud SST");

        assert!(
            error.to_string().contains("crc") || error.to_string().contains("content"),
            "unexpected wrong-content cloud SST recovery error: {error}"
        );
        assert!(
            !state.sst_dir.join(&sst_name).exists(),
            "wrong-content cloud SST must not be installed into the local cache"
        );
    }

    #[test]
    fn should_replace_wrong_sized_local_sst_cache_from_authoritative_cloud_object() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Strict,
        )
        .expect("create runtime state");
        let sst_name = crate::sst::file_name(0, 0, 5);
        let committed_sst_bytes = test_sst_bytes_with_value(b"manifest-sized-value");
        let stale_local_sst_bytes = test_sst_bytes_with_value(b"different-local-cache-bytes");
        assert_ne!(
            committed_sst_bytes.len(),
            stale_local_sst_bytes.len(),
            "test must use a stale local SST with different size than the committed manifest"
        );
        state.manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: committed_sst_bytes.len() as u64,
            cf_id: 0,
            sst_seq: 4,
            smallest_key: Some(b"cloud-list-key".to_vec()),
            largest_key: Some(b"cloud-list-key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        });
        std::fs::write(state.sst_dir.join(&sst_name), stale_local_sst_bytes)
            .expect("write stale local SST cache");
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(
            &cloud,
            &crate::sst::object_key(&sst_name),
            committed_sst_bytes.clone(),
        )
        .expect("upload authoritative manifest-sized test sst");

        Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect("wrong-sized local cache should be restored from authoritative cloud SST");

        assert_eq!(
            std::fs::read(state.sst_dir.join(&sst_name)).expect("read restored local SST"),
            committed_sst_bytes,
            "local SST cache must be replaced with the manifest-sized cloud object"
        );
    }

    #[test]
    fn should_replace_same_size_wrong_crc_local_sst_cache_from_authoritative_cloud_object() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Strict,
        )
        .expect("create runtime state");
        let sst_name = crate::sst::file_name(0, 0, 6);
        let committed_sst_bytes = test_sst_bytes();
        let stale_local_sst_bytes = same_size_sst_with_different_crc(&committed_sst_bytes);
        state.manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: committed_sst_bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(&committed_sst_bytes)),
            cf_id: 0,
            sst_seq: 6,
            smallest_key: Some(b"cloud-list-key".to_vec()),
            largest_key: Some(b"cloud-list-key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        });
        std::fs::write(state.sst_dir.join(&sst_name), stale_local_sst_bytes)
            .expect("write stale same-size local SST cache");
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(
            &cloud,
            &crate::sst::object_key(&sst_name),
            committed_sst_bytes.clone(),
        )
        .expect("upload authoritative manifest-crc test sst");

        Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect("same-size wrong local cache should be restored from authoritative cloud SST");

        assert_eq!(
            std::fs::read(state.sst_dir.join(&sst_name)).expect("read restored local SST"),
            committed_sst_bytes,
            "local SST cache must be replaced with the manifest-crc cloud object"
        );
    }

    #[test]
    fn should_salvage_retain_manifest_sst_when_local_cache_is_valid_but_cloud_crc_differs() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Salvage,
        )
        .expect("create runtime state");
        let sst_name = crate::sst::file_name(0, 0, 7);
        let committed_sst_bytes = test_sst_bytes();
        let wrong_cloud_sst_bytes = same_size_sst_with_different_crc(&committed_sst_bytes);
        state.manifest.files.push(crate::metadata::FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: committed_sst_bytes.len() as u64,
            content_crc32c: Some(crc32c::crc32c(&committed_sst_bytes)),
            cf_id: 0,
            sst_seq: 7,
            smallest_key: Some(b"cloud-list-key".to_vec()),
            largest_key: Some(b"cloud-list-key".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(1),
            ..Default::default()
        });
        std::fs::write(state.sst_dir.join(&sst_name), &committed_sst_bytes)
            .expect("write valid local SST cache");
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(
            &cloud,
            &crate::sst::object_key(&sst_name),
            wrong_cloud_sst_bytes,
        )
        .expect("upload wrong-content cloud SST");

        Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, &cloud)
            .expect("salvage should keep a manifest SST when the local cache is valid");

        assert!(
            state
                .manifest
                .files
                .iter()
                .any(|file| file.name == sst_name),
            "salvage must not drop a manifest SST that still has a valid local recoverable copy"
        );
        assert_eq!(
            std::fs::read(state.sst_dir.join(&sst_name)).expect("read retained local SST"),
            committed_sst_bytes,
            "valid local SST cache must remain intact"
        );
        assert!(
            state.persistence_anomaly_detected,
            "salvage should still surface the invalid cloud copy as a persistence anomaly"
        );
    }

    #[test]
    fn should_stage_intent_replay_sst_when_cloud_listing_is_stale_but_object_is_readable() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Strict,
        )
        .expect("create runtime state");
        let sst_name = crate::sst::file_name(0, 0, 2);
        let sst_bytes = test_sst_bytes();
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), sst_bytes)
            .expect("upload intent replay sst");

        Engine::ensure_named_sst_cache_from_cloud_storage(
            &mut state,
            &cloud,
            vec![CloudSstRecoveryProof::name_only(sst_name.clone())],
        )
        .expect("stale list should not make readable intent SST unstaged");

        assert!(
            state.sst_dir.join(&sst_name).exists(),
            "readable cloud SST should be staged despite stale LIST"
        );
    }

    #[test]
    fn should_reject_intent_replay_sst_when_cloud_object_crc_differs_from_intent() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let mut state = crate::runtime::RuntimeState::try_new(
            temp_dir.path().to_path_buf(),
            false,
            RecoveryPolicy::Strict,
        )
        .expect("create runtime state");
        let sst_name = crate::sst::file_name(0, 0, 7);
        let sst_bytes = test_sst_bytes();
        let expected_crc = crc32c::crc32c(&sst_bytes) ^ 0xffff_ffff;
        state
            .intent_log
            .push(crate::runtime::IntentLogEntry::SstAdded {
                file_meta: crate::runtime::FileMeta {
                    name: sst_name.clone(),
                    level: 0,
                    size_bytes: sst_bytes.len() as u64,
                    content_crc32c: Some(expected_crc),
                    cf_id: 0,
                    smallest_key: Some(b"cloud-list-key".to_vec()),
                    largest_key: Some(b"cloud-list-key".to_vec()),
                    smallest_seq: Some(1),
                    largest_seq: Some(1),
                },
            });
        let cloud = cloud_with_stale_sst_listing();
        Engine::blocking_cloud_put(&cloud, &crate::sst::object_key(&sst_name), sst_bytes)
            .expect("upload intent SST with mismatched content proof");

        let proofs = Engine::cloud_recovery_sst_proofs_for_intent_replay(&state);
        let error = Engine::ensure_named_sst_cache_from_cloud_storage(&mut state, &cloud, proofs)
            .expect_err("strict recovery must reject intent SST with mismatched content proof");

        assert!(
            error.to_string().contains("crc") || error.to_string().contains("content"),
            "unexpected intent SST proof error: {error}"
        );
        assert!(
            !state.sst_dir.join(&sst_name).exists(),
            "intent SST with mismatched proof must not be staged"
        );
    }
}
