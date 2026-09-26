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
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

static IN_MEMORY_OPEN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) mod api;
mod backup;
mod ingest;
mod lease_state;
mod metrics;
mod startup;
mod verification;

pub use crate::types::ColumnFamilyId;
pub use api::{
    BlockCachePolicy, CloudWritePolicy, ConflictPolicy, Direction, DurabilityPolicy, Goal,
    IteratorState, Key, MemoryBudget, OpenOptions, OpenOptionsBuilder, Query, RecoveryPolicy,
    ScanIterator, Storage, Transaction, TransactionMode, Value, WorkloadProfile, WriteOptions,
};
pub use backup::{BackupManifest, BackupObject, BackupStorageKind};
/// Registry of column families, keyed by column family ID
type ColumnFamilyRegistry = dashmap::DashMap<ColumnFamilyId, ColumnFamilyHandle>;

use lease_state::LeaseState;
pub use metrics::EngineMetrics;
pub use verification::StorageVerifier;

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
    /// True when cloud mode uses the in-process filesystem simulator.
    simulated_cloud_mode: bool,
    /// Latest committed sequence observed by the engine.
    ///
    /// Sequence numbers are allocated inside the runtime (at WAL append time) and
    /// returned via `RuntimeResponse::WalAppended { sequence, .. }`.
    sequence: Arc<std::sync::atomic::AtomicU64>,
    /// Next snapshot ID counter (local only, not related to sequence numbers)
    next_snapshot_id: std::sync::atomic::AtomicU64,
    /// Column families registry (CF ID -> Handle)
    column_families: ColumnFamilyRegistry,
    lease_state: LeaseState,
    /// Per-CF ingest coordinators for write batching
    ingest_coordinators: dashmap::DashMap<ColumnFamilyId, Arc<ingest::IngestCoordinator>>,
    /// Shared bounded pool for resident transaction intents.
    transaction_memory_pool: Arc<crate::runtime::transaction_spill::TransactionMemoryPool>,
    ttl_clock: Arc<crate::common::time::ObservedClock>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        let ingest_count = self.ingest_coordinators.len();
        self.ingest_coordinators.clear();
        tracing::trace!(count = ingest_count, "Engine: ingest coordinators dropped");

        // Runtime teardown can legitimately wait for a transaction owned by
        // the dropping thread or for a blocked storage worker. Hand that wait
        // to a detached reaper and move all fencing resources with it. The
        // lease remains renewed and owned until every runtime worker exits.
        self.lease_state.detach_reaper(self.runtime.take());
    }
}

#[cfg(test)]
type CloudSstRecoveryProof = startup::CloudSstRecoveryProof;

impl Engine {
    #[cfg(test)]
    fn blocking_cloud_get(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
    ) -> MidgeResult<Vec<u8>> {
        startup::cloud_io::BlockingCloudIo::new(cloud).get(key)
    }

    #[cfg(test)]
    fn blocking_cloud_put(
        cloud: &crate::storage::cloud::CloudStorage,
        key: &str,
        data: Vec<u8>,
    ) -> MidgeResult<()> {
        startup::cloud_io::BlockingCloudIo::new(cloud).put(key, data)
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
        if !self.runtime_handle.is_open() {
            return None;
        }
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
        if let Some(ref heartbeat_mutex) = self.lease_state.heartbeat {
            if let Ok(heartbeat) = heartbeat_mutex.lock() {
                return heartbeat.is_healthy();
            }
        }
        // If we can't lock or lease is not present, assume unhealthy
        false
    }

    /// Check if ingest batching should be used based on durability policy.
    ///
    /// Return whether an ingest barrier is currently active.
    ///
    /// Ingest batching is orthogonal to cloud durability. Cloud-backed async mode
    /// still makes writes visible after the local WAL append barrier; it simply
    /// advances cloud durability later in the background.
    pub(crate) fn is_ingesting(&self) -> bool {
        self.runtime_handle.ingest_active()
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
            RuntimeResponse::Ok { .. } => Ok(()),
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
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called while ingest mode is active.
    pub fn begin_tx(
        &self,
        cf_id: ColumnFamilyId,
        mode: api::TransactionMode,
    ) -> MidgeResult<api::Transaction> {
        let is_read_only = mode == api::TransactionMode::ReadOnly;
        if is_read_only {
            self.runtime_handle.diagnostics.record_read_only_begin_tx();
        }

        if self.is_ingesting() {
            return Err(MidgeError::InvalidArgument(
                "cannot begin a transaction while ingest mode is active; end the ingest barrier first"
                    .to_string(),
            ));
        }

        let runtime_transaction_guard = self.runtime_handle.acquire_transaction_guard()?;
        // Hold the registry's shared acquisition guard from snapshot capture
        // through pin publication. GC samples pins under the exclusive guard,
        // so an obsolete SST cannot be deleted in the capture/register window.
        // GC only try-locks it, because this thread may wait on the event loop
        // (a snapshot-cache miss) while holding the guard.
        // Any snapshot captured below starts at or after the committed
        // sequence read here, so it is a safe floor for the compaction horizon.
        let _snapshot_acquisition = self
            .runtime_handle
            .begin_snapshot_acquisition(self.sequence.load(std::sync::atomic::Ordering::SeqCst));

        let Some(coordinator) = self
            .ingest_coordinators
            .get(&cf_id)
            .map(|entry| Arc::clone(entry.value()))
        else {
            return Err(MidgeError::InvalidArgument(format!(
                "column family {cf_id} does not exist"
            )));
        };

        let (start_sequence, read_snapshot) =
            self.acquire_transaction_snapshot(cf_id, is_read_only)?;

        let read_snapshot = Arc::new(
            (*read_snapshot)
                .clone()
                .with_read_time_millis(self.ttl_clock.now_millis()),
        );

        let pinned_sst_names = read_snapshot.pinned_sst_names();

        let txn_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if !self.runtime_handle.register_snapshot_pin_while_acquiring(
            txn_id,
            start_sequence,
            pinned_sst_names,
        ) {
            return Err(MidgeError::Internal(format!(
                "snapshot {txn_id} is already registered"
            )));
        }
        self.runtime_handle.diagnostics.record_snapshot_register();

        Ok(api::Transaction::new(api::TransactionInit {
            runtime_handle: self.runtime_handle.clone(),
            coordinator,
            sequence_publisher: Arc::clone(&self.sequence),
            id: txn_id,
            cf_id,
            mode,
            start_sequence,
            read_snapshot: Some(read_snapshot),
            cloud_mode: self.cloud_mode,
            db_path: self.db_path.clone(),
            memory_mode: self.memory_mode,
            transaction_memory_pool: Arc::clone(&self.transaction_memory_pool),
            runtime_transaction_guard,
        }))
    }

    fn acquire_transaction_snapshot(
        &self,
        cf_id: ColumnFamilyId,
        is_read_only: bool,
    ) -> MidgeResult<(u64, Arc<crate::runtime::ReadSnapshot>)> {
        let committed_sequence = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let cache_guard = self.runtime_handle.snapshot_cache.load();
        let cached_snapshot = cache_guard
            .cf_snapshots
            .get(&cf_id)
            .map(|data| Arc::clone(&data.snapshot));
        let cached_sequence = cache_guard.sequence;
        let cached_snapshot = cached_snapshot.filter(|_| cached_sequence >= committed_sequence);
        drop(cache_guard);

        if let Some(snapshot) = cached_snapshot {
            if is_read_only {
                self.runtime_handle
                    .diagnostics
                    .record_read_only_snapshot_cache_hit();
            }
            return Ok((cached_sequence, snapshot));
        }

        if is_read_only {
            self.runtime_handle
                .diagnostics
                .record_read_only_snapshot_cache_miss();
        }
        match self
            .runtime_handle
            .send_and_wait(RuntimeMsg::BeginTransaction {
                request_id: next_request_id()?,
                cf_id,
            })? {
            RuntimeResponse::BeginTransactionResult {
                start_sequence,
                snapshot: Some(snapshot),
                ..
            } => Ok((start_sequence, snapshot)),
            RuntimeResponse::BeginTransactionResult { snapshot: None, .. } => Err(
                MidgeError::InvalidArgument(format!("column family {cf_id} does not exist")),
            ),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to BeginTransaction".to_string(),
            )),
        }
    }

    /// Capture read-path diagnostics owned by this engine's runtime.
    ///
    /// This doc-hidden benchmark hook deliberately scopes the snapshot to one
    /// engine, so another engine in the same process cannot contaminate a
    /// before/after measurement window.
    #[cfg(feature = "internal-testing")]
    #[must_use]
    #[doc(hidden)]
    pub fn read_path_diagnostics_snapshot_for_benchmarks(
        &self,
    ) -> crate::diagnostics::ReadPathDiagnosticsSnapshot {
        self.runtime_handle.read_path_diagnostics_snapshot()
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

    /// Shutdown the engine gracefully within `timeout`.
    ///
    /// Once shutdown begins, the engine remains in the closing state and
    /// rejects new work. `Busy` and caller-deadline `Timeout` leave cleanup
    /// retryable; callers may release active transactions or wait for blocked
    /// storage I/O and invoke this method again. A cloud-upload drain `Timeout`
    /// is the runtime's terminal durability result and is replayed on later
    /// calls. The caller timeout does not cancel in-flight durability I/O.
    /// Writer fencing remains held until every runtime worker has exited.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::Busy` while transactions remain active,
    /// `MidgeError::Timeout` when the deadline elapses, or a durability error
    /// reported by the runtime during its final flush. Returns
    /// `MidgeError::ResourceLimit` if the fencing cleanup worker cannot start.
    pub fn shutdown(&mut self, timeout: Duration) -> MidgeResult<()> {
        let started = std::time::Instant::now();
        if self.lease_state.pending_cleanup.is_some() {
            return self.lease_state.wait_for_cleanup(timeout);
        }

        let Some(runtime) = self.runtime.as_mut() else {
            if !self.lease_state.has_resources() {
                return self
                    .runtime_handle
                    .shutdown(timeout.saturating_sub(started.elapsed()));
            }
            self.lease_state.schedule_cleanup(Ok(()))?;
            return self
                .lease_state
                .wait_for_cleanup(timeout.saturating_sub(started.elapsed()));
        };
        let shutdown_result = self.runtime_handle.shutdown(timeout);
        if matches!(&shutdown_result, Err(MidgeError::Busy(_))) {
            return shutdown_result;
        }
        let remaining = timeout.saturating_sub(started.elapsed());
        if !runtime.wait_for_exit(remaining) {
            if matches!(&shutdown_result, Err(MidgeError::Timeout(_))) {
                let runtime = self.runtime.take().ok_or_else(|| {
                    MidgeError::Internal("runtime cleanup requested without a runtime".to_string())
                })?;
                self.ingest_coordinators.clear();
                if let Err((error, runtime)) = self
                    .lease_state
                    .schedule_runtime_cleanup(runtime, self.runtime_handle.clone())
                {
                    self.runtime = Some(runtime);
                    return Err(error);
                }
                return shutdown_result;
            }
            return Err(MidgeError::Timeout(
                "runtime workers did not terminate before shutdown deadline".to_string(),
            ));
        }

        self.runtime.take();
        self.ingest_coordinators.clear();
        if !self.lease_state.has_resources() {
            return shutdown_result;
        }
        self.lease_state.schedule_cleanup(shutdown_result)?;
        self.lease_state
            .wait_for_cleanup(timeout.saturating_sub(started.elapsed()))
    }

    // === Column Family Lifecycle ===

    /// Checkpoint the manifest after a column-family change has committed.
    ///
    /// The change is already durable in the manifest journal and applied to
    /// the local registry, so a checkpoint failure must not report the DDL as
    /// rejected: a caller would retry a create that exists or a drop that
    /// already happened. The runtime records a failed checkpoint as a
    /// persistence anomaly; a request that never reached it is only logged.
    fn checkpoint_after_committed_ddl(&self, operation: &'static str) {
        if self.cloud_mode {
            return;
        }
        let outcome = next_request_id().and_then(|request_id| {
            self.runtime_handle
                .send_and_wait(RuntimeMsg::ManifestPersist { request_id })
        });
        match outcome {
            Ok(RuntimeResponse::Error { error, .. }) | Err(error) => {
                tracing::warn!(operation, %error, "manifest checkpoint after committed DDL failed");
            }
            Ok(_) => {}
        }
    }

    /// Create a new column family with the given name
    ///
    /// # Errors
    ///
    /// Names must be non-empty UTF-8 strings of at most 255 bytes, must not
    /// contain NUL, and must not be the reserved name `default`.
    ///
    /// Returns [`MidgeError::InvalidArgument`] when a name violates those
    /// restrictions or DDL is attempted during ingest mode. Returns
    /// [`MidgeError::ResourceLimit`] when the column-family ID space is
    /// exhausted. Other errors report persistence or runtime failures.
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
                let coordinator = Arc::new(ingest::IngestCoordinator::new(cf_id));
                self.ingest_coordinators.insert(cf_id, coordinator);
                self.checkpoint_after_committed_ddl("create_column_family");

                Ok(handle)
            }
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to create_column_family".to_string(),
            )),
        }
    }

    /// Drop a column family by ID after all committed memtable data has been
    /// flushed.
    ///
    /// # Errors
    ///
    /// Returns [`MidgeError::InvalidArgument`] for the default, missing, or
    /// already-dropped column family and when DDL is attempted during ingest
    /// mode.
    ///
    /// Returns [`MidgeError::UnflushedDataPresent`] -- and only that error --
    /// when committed data remains in the active memtable. That is the sole
    /// signal that licenses [`Self::drop_column_family_discarding_unflushed`];
    /// ask [`MidgeError::licenses_unflushed_discard`] rather than matching on
    /// a variant.
    ///
    /// Every other error, [`MidgeError::Busy`] included, means the drop did
    /// not happen. `Busy` here reports in-flight flush publication, a remote
    /// DDL registry CAS conflict, an active storage-verification barrier, or
    /// shutdown -- conditions that clear on their own and say nothing about
    /// unflushed data. Retry the safe drop; never escalate to the destructive
    /// variant on anything but the licence above.
    pub fn drop_column_family(&self, cf_id: ColumnFamilyId) -> MidgeResult<()> {
        self.drop_column_family_inner(cf_id, false)
    }

    /// Destructively drop a column family even when committed data remains in
    /// its active memtable.
    ///
    /// Flush and compaction publication already in flight are still quiesced
    /// before the drop. This method makes the otherwise rejected data loss
    /// explicit at the call site.
    ///
    /// # Errors
    ///
    /// Returns [`MidgeError::InvalidArgument`] for the default, missing, or
    /// already-dropped column family and when DDL is attempted during ingest
    /// mode. Other errors report persistence or runtime failures.
    pub fn drop_column_family_discarding_unflushed(
        &self,
        cf_id: ColumnFamilyId,
    ) -> MidgeResult<()> {
        self.drop_column_family_inner(cf_id, true)
    }

    fn drop_column_family_inner(
        &self,
        cf_id: ColumnFamilyId,
        discard_unflushed: bool,
    ) -> MidgeResult<()> {
        let response = self.runtime_handle.send_and_wait_filtered(
            RuntimeMsg::ManifestDropColumnFamily {
                request_id: next_request_id()?,
                cf_id,
                discard_unflushed,
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
                // Drop coordinator for this CF
                self.ingest_coordinators.remove(&cf_id);

                // Remove from local registry
                self.column_families.remove(&cf_id);
                self.checkpoint_after_committed_ddl("drop_column_family");

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
        self.runtime_handle.ensure_open()?;
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

    /// Return the dedicated runtime observability façade.
    #[must_use]
    pub fn metrics(&self) -> EngineMetrics {
        EngineMetrics::new(self.runtime_handle.clone())
    }

    /// Return the dedicated online storage-integrity façade.
    #[must_use]
    pub fn storage_verifier(&self) -> StorageVerifier {
        StorageVerifier::new(
            self.runtime_handle.clone(),
            self.db_path.clone(),
            self.memory_mode,
            self.cloud_mode,
        )
    }

    // === Internal helpers ===
}

#[cfg(test)]
mod tests;
