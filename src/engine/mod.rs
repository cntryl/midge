//! Main KV store engine
//!
//! Public API for database operations.
//!
//! The engine provides a transaction-scoped API for all data operations.
//! All reads and writes execute within explicit transactions.
//!
//! Key responsibilities:
//! - Transaction lifecycle entry (`begin_tx`)
//! - Column family management
//! - Flush and compaction control
//! - Metrics and observability
//!
//! Point operations (get, put, delete, scan) are methods on Transaction.
//! Transaction finalization and range tombstones are also transaction-scoped.

use crate::common::{MidgeError, MidgeResult};
#[cfg(test)]
use crate::runtime::RuntimeState;
use crate::runtime::{next_request_id, Runtime, RuntimeHandle, RuntimeMsg, RuntimeResponse};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

static IN_MEMORY_OPEN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) mod api;
mod ingest;
mod startup;

pub use crate::config::EngineHealth;
pub use crate::types::{
    ColumnFamilyId, ReadAmpMetricsSnapshot, RecoveryMetricsSnapshot, RuntimeMetricsSnapshot,
    StorageLayoutSnapshot, StorageVerificationReport,
};
pub use api::{
    Direction, Goal, IsolationLevel, Key, MemoryBudget, OpenOptions, Query, RecoveryPolicy,
    ScanIterator, Storage, Transaction, TransactionMode, Value, WorkloadProfile, WriteOptions,
};
/// Registry of column families, keyed by column family ID
type ColumnFamilyRegistry = dashmap::DashMap<ColumnFamilyId, ColumnFamilyHandle>;

/// Column family handle for API operations
#[derive(Debug, Clone)]
pub struct ColumnFamilyHandle {
    id: ColumnFamilyId,
    name: String,
}

impl ColumnFamilyHandle {
    #[must_use]
    pub fn new(id: ColumnFamilyId, name: String) -> Self {
        Self { id, name }
    }

    #[must_use]
    pub fn id(&self) -> ColumnFamilyId {
        self.id
    }

    #[must_use]
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
    runtime: Option<Runtime>,
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
    lease: Option<Arc<dyn crate::lease::PrimaryLease>>,
    /// Primary instance lease guard
    lease_guard: Option<crate::lease::LeaseGuard>,
    /// Lease heartbeat (keeps lease renewed)
    lease_heartbeat: Option<std::sync::Mutex<crate::lease::LeaseHeartbeat>>,
    /// Per-CF ingest coordinators for write batching
    ingest_coordinators: dashmap::DashMap<ColumnFamilyId, Arc<ingest::IngestCoordinator>>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        let drop_start = std::time::Instant::now();
        tracing::debug!("Engine dropping, initiating cleanup");

        // Stop lease heartbeat first
        if let Some(heartbeat_mutex) = self.lease_heartbeat.take() {
            if let Ok(mut heartbeat) = heartbeat_mutex.lock() {
                heartbeat.stop();
                tracing::trace!("Engine: lease heartbeat stopped");
            }
        }

        // Shutdown all ingest coordinators
        let ingest_count = self.ingest_coordinators.len();
        for entry in &self.ingest_coordinators {
            entry.value().shutdown();
        }
        tracing::trace!(count = ingest_count, "Engine: ingest coordinators shutdown");

        // Gracefully shutdown the runtime when engine is dropped
        // Send shutdown message first
        let _ = self.runtime_handle.shutdown();
        // Then drop the runtime which will wait for the thread to finish
        self.runtime.take();
        tracing::trace!("Engine: runtime shutdown complete");

        // Release lease via the PrimaryLease interface
        if let Some(lease) = self.lease.take() {
            let _ = lease.release();
            tracing::trace!("Engine: lease released");
        }

        // Drop the guard last
        self.lease_guard.take();

        tracing::debug!(
            elapsed_ms = drop_start.elapsed().as_millis(),
            "Engine cleanup complete"
        );
    }
}

#[cfg(test)]
type CloudSstRecoveryProof = startup::CloudSstRecoveryProof;

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

    #[cfg(test)]
    fn blocking_cloud_get(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Vec<u8>> {
        startup::CloudStartupRecovery::blocking_cloud_get(cloud, key)
    }

    #[cfg(test)]
    fn blocking_cloud_put(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
        data: Vec<u8>,
    ) -> MidgeResult<()> {
        startup::CloudStartupRecovery::blocking_cloud_put(cloud, key, data)
    }

    #[cfg(test)]
    fn hydrate_cloud_metadata(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        startup::CloudStartupRecovery::hydrate_cloud_metadata(cloud, db_path, recovery_policy)
    }

    #[cfg(test)]
    fn mirror_cloud_metadata(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<()> {
        startup::CloudStartupRecovery::mirror_cloud_metadata(cloud, db_path, recovery_policy)
    }

    #[cfg(test)]
    fn materialize_cloud_wal_recovery_dir(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<PathBuf> {
        startup::CloudStartupRecovery::materialize_cloud_wal_recovery_dir(
            cloud,
            db_path,
            recovery_policy,
        )
    }

    #[cfg(test)]
    fn ensure_local_sst_cache_from_cloud_storage(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
    ) -> MidgeResult<()> {
        startup::CloudStartupRecovery::ensure_local_sst_cache_from_cloud_storage(state, cloud)
    }

    #[cfg(test)]
    fn ensure_named_sst_cache_from_cloud_storage(
        state: &mut RuntimeState,
        cloud: &crate::storage::cloud::CloudStorage,
        sst_proofs: impl IntoIterator<Item = CloudSstRecoveryProof>,
    ) -> MidgeResult<()> {
        startup::CloudStartupRecovery::ensure_named_sst_cache_from_cloud_storage(
            state, cloud, sst_proofs,
        )
    }

    #[cfg(test)]
    fn cloud_recovery_sst_proofs_for_intent_replay(
        state: &RuntimeState,
    ) -> Vec<CloudSstRecoveryProof> {
        startup::CloudStartupRecovery::cloud_recovery_sst_proofs_for_intent_replay(state)
    }

    /// Open a database with explicit environment selection.
    ///
    /// The storage backend is specified by `OpenOptions.storage`. There is no
    /// inference from paths or sentinel strings.
    ///
    /// # Errors
    ///
    /// Returns an error when the engine cannot initialize its storage, runtime,
    /// manifest, or recovery state.
    pub fn open(opts: OpenOptions) -> MidgeResult<Self> {
        startup::EngineStartup::open_owned(opts)
    }

    /// Get an existing column family by name.
    ///
    /// Returns None if the column family doesn't exist.
    pub fn get_column_family(&self, name: &str) -> Option<ColumnFamilyHandle> {
        for entry in &self.column_families {
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
        if let Some(ref heartbeat_mutex) = self.lease_heartbeat {
            if let Ok(heartbeat) = heartbeat_mutex.lock() {
                return heartbeat.is_healthy();
            }
        }
        // If we can't lock or lease is not present, assume unhealthy
        false
    }

    /// Open a database with the provided `MidgeOptions`.
    ///
    /// Convenience method for testkit and standard testing patterns.
    ///
    /// # Errors
    ///
    /// Returns an error when engine startup fails for the derived open options.
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
    pub(crate) fn exit_ingest_mode(&self, prev: &IngestModeSnapshot) -> MidgeResult<()> {
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
    ///
    /// # Errors
    ///
    /// Returns an error when the flush cannot be scheduled or completed.
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
    /// * `mode` - Transaction mode (`ReadOnly` or `ReadWrite`)
    ///
    /// # Errors
    ///
    /// Returns an error when snapshot registration fails or the column family does
    /// not exist.
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
            MidgeError::InvalidArgument(format!("column family {cf_id} does not exist"))
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
    ///
    /// # Errors
    ///
    /// Returns an error when the wait request cannot be sent or the runtime reports a failure.
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
                "Unexpected response to WaitForWriteStallClear: {other:?}"
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
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown coordination fails.
    pub fn shutdown(self) -> MidgeResult<()> {
        self.runtime_handle.shutdown()
    }

    // === Column Family Lifecycle ===

    /// Create a new column family with the given name
    ///
    /// # Errors
    ///
    /// Returns an error when the column family cannot be created or persisted.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the column family cannot be dropped or persisted.
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
    ///
    /// # Errors
    ///
    /// This method currently does not return an error, but preserves a result-based
    /// API for compatibility with future runtime-backed implementations.
    pub fn list_column_families(&self) -> MidgeResult<Vec<ColumnFamilyHandle>> {
        Ok(self
            .column_families
            .iter()
            .map(|ref_multi| ref_multi.value().clone())
            .collect())
    }

    /// Compact all data (schedule compactions and wait for completion)
    ///
    /// # Errors
    ///
    /// Returns an error when compaction scheduling or completion fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a metrics snapshot.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a recovery snapshot.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a metrics snapshot.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a storage-layout snapshot.
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
    ///
    /// # Errors
    ///
    /// Returns an error when verification is unsupported or the verification pass fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error when the verification pass fails.
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
        let handle = ColumnFamilyHandle::new(1, String::new());

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
        let debug_str = format!("{handle:?}");

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
            key: &str,
            data: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            self.inner.submit_put(key, data, headers, callback);
        }

        fn submit_get(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
            self.inner.submit_get(key, callback);
        }

        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            self.inner.submit_get_range(key, start, end, callback);
        }

        fn submit_delete(
            &self,
            key: &str,
            headers: Vec<(String, String)>,
            callback: crate::storage::cloud::CloudCallback,
        ) {
            self.inner.submit_delete(key, headers, callback);
        }

        fn submit_list(&self, prefix: &str, callback: crate::storage::cloud::CloudCallback) {
            if prefix.ends_with(&self.omitted_prefix) {
                let _ = callback.send(crate::storage::cloud::CloudEvent::List {
                    prefix: prefix.to_string(),
                    result: crate::storage::cloud::CloudOutcome::Ok(Vec::new()),
                });
                return;
            }
            self.inner.submit_list(prefix, callback);
        }

        fn submit_head(&self, key: &str, callback: crate::storage::cloud::CloudCallback) {
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
            state.persistence_anomaly_detected(),
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
