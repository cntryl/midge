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
mod verification;

pub use crate::config::EngineHealth;
pub use crate::types::{
    ColumnFamilyId, ReadAmpMetricsSnapshot, RecoveryMetricsSnapshot, RuntimeMetricsSnapshot,
    StorageLayoutSnapshot, StorageVerificationReport,
};
pub use api::{
    BlockCachePolicy, CloudWritePolicy, ConflictPolicy, Direction, Goal, Key, MemoryBudget,
    OpenOptions, OpenOptionsBuilder, Query, RecoveryPolicy, ScanIterator, Storage, Transaction,
    TransactionMode, Value, WorkloadProfile, WriteOptions,
};
/// Registry of column families, keyed by column family ID
type ColumnFamilyRegistry = dashmap::DashMap<ColumnFamilyId, ColumnFamilyHandle>;

type FencingResources = (
    Option<std::sync::Mutex<crate::lease::LeaseHeartbeat>>,
    Option<Arc<dyn crate::lease::PrimaryLease>>,
    Option<crate::lease::LeaseGuard>,
);

enum PendingFencingCleanup {
    Known {
        completion: crossbeam::channel::Receiver<()>,
        terminal_result: MidgeResult<()>,
    },
    Runtime {
        completion: crossbeam::channel::Receiver<MidgeResult<()>>,
    },
}

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
    /// Detached fencing cleanup retained after a bounded shutdown times out.
    pending_fencing_cleanup: Option<PendingFencingCleanup>,
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

        let runtime = self.runtime.take();
        let lease_heartbeat = self.lease_heartbeat.take();
        let lease = self.lease.take();
        let lease_guard = self.lease_guard.take();
        if runtime.is_none()
            && lease_heartbeat.is_none()
            && lease.is_none()
            && lease_guard.is_none()
        {
            return;
        }

        // Runtime teardown can legitimately wait for a transaction owned by
        // the dropping thread or for a blocked storage worker. Hand that wait
        // to a detached reaper and move all fencing resources with it. The
        // lease remains renewed and owned until every runtime worker exits.
        let spawn_result = std::thread::Builder::new()
            .name("midge-engine-reaper".to_string())
            .spawn(move || {
                drop(runtime);
                Self::release_fencing_parts(lease_heartbeat, lease, lease_guard);
                tracing::debug!("Engine reaper cleanup complete");
            });
        if let Err(error) = spawn_result {
            tracing::error!(%error, "failed to spawn engine cleanup reaper");
        }
    }
}

#[cfg(test)]
type CloudSstRecoveryProof = startup::CloudSstRecoveryProof;

impl Engine {
    fn release_fencing_parts(
        lease_heartbeat: Option<std::sync::Mutex<crate::lease::LeaseHeartbeat>>,
        lease: Option<Arc<dyn crate::lease::PrimaryLease>>,
        lease_guard: Option<crate::lease::LeaseGuard>,
    ) {
        if let Some(heartbeat_mutex) = lease_heartbeat {
            let mut heartbeat = heartbeat_mutex
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            heartbeat.stop();
            tracing::trace!("Engine: lease heartbeat stopped");
        }
        if let Some(lease) = lease {
            loop {
                match lease.release() {
                    Ok(()) => break,
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "primary lease release failed; cleanup reaper will retry"
                        );
                        std::thread::sleep(Duration::from_millis(1));
                    }
                }
            }
            tracing::trace!("Engine: lease released");
        }
        drop(lease_guard);
    }

    fn has_fencing_resources(&self) -> bool {
        self.lease_heartbeat.is_some() || self.lease.is_some() || self.lease_guard.is_some()
    }

    fn schedule_fencing_cleanup(&mut self, terminal_result: MidgeResult<()>) -> MidgeResult<()> {
        debug_assert!(self.pending_fencing_cleanup.is_none());

        let resources: FencingResources = (
            self.lease_heartbeat.take(),
            self.lease.take(),
            self.lease_guard.take(),
        );
        let retained_resources = Arc::new(std::sync::Mutex::new(Some(resources)));
        let worker_resources = Arc::clone(&retained_resources);
        let (completion_tx, completion_rx) = crossbeam::channel::bounded(1);
        let spawn_result = std::thread::Builder::new()
            .name("midge-fencing-reaper".to_string())
            .spawn(move || {
                let resources = worker_resources
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some((lease_heartbeat, lease, lease_guard)) = resources {
                    Self::release_fencing_parts(lease_heartbeat, lease, lease_guard);
                }
                let _ = completion_tx.send(());
                tracing::debug!("Engine fencing reaper cleanup complete");
            });

        match spawn_result {
            Ok(worker) => {
                drop(worker);
                self.pending_fencing_cleanup = Some(PendingFencingCleanup::Known {
                    completion: completion_rx,
                    terminal_result,
                });
                Ok(())
            }
            Err(error) => {
                let (lease_heartbeat, lease, lease_guard) = retained_resources
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .ok_or_else(|| {
                        MidgeError::Internal(
                            "fencing resources disappeared after cleanup reaper spawn failure"
                                .to_string(),
                        )
                    })?;
                self.lease_heartbeat = lease_heartbeat;
                self.lease = lease;
                self.lease_guard = lease_guard;
                tracing::error!(%error, "failed to spawn fencing cleanup reaper");
                match terminal_result {
                    Ok(()) => Err(MidgeError::ResourceLimit(format!(
                        "failed to spawn fencing cleanup reaper: {error}"
                    ))),
                    Err(terminal_error) => Err(terminal_error),
                }
            }
        }
    }

    fn schedule_runtime_fencing_cleanup(&mut self) -> MidgeResult<()> {
        debug_assert!(self.pending_fencing_cleanup.is_none());
        let runtime = self.runtime.take().ok_or_else(|| {
            MidgeError::Internal("runtime cleanup requested without a runtime".to_string())
        })?;
        self.ingest_coordinators.clear();
        let resources: FencingResources = (
            self.lease_heartbeat.take(),
            self.lease.take(),
            self.lease_guard.take(),
        );
        let retained_cleanup = Arc::new(std::sync::Mutex::new(Some((runtime, resources))));
        let worker_cleanup = Arc::clone(&retained_cleanup);
        let runtime_handle = self.runtime_handle.clone();
        let (completion_tx, completion_rx) = crossbeam::channel::bounded(1);
        let spawn_result = std::thread::Builder::new()
            .name("midge-runtime-fencing-reaper".to_string())
            .spawn(move || {
                let cleanup = worker_cleanup
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                let Some((runtime, resources)) = cleanup else {
                    return;
                };
                let terminal_result = runtime_handle.shutdown(Duration::MAX);
                drop(runtime);
                Self::release_fencing_parts(resources.0, resources.1, resources.2);
                let _ = completion_tx.send(terminal_result);
                tracing::debug!("Engine runtime and fencing reaper cleanup complete");
            });
        match spawn_result {
            Ok(worker) => {
                drop(worker);
                self.pending_fencing_cleanup = Some(PendingFencingCleanup::Runtime {
                    completion: completion_rx,
                });
                Ok(())
            }
            Err(error) => {
                let (runtime, resources) = retained_cleanup
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .ok_or_else(|| {
                        MidgeError::Internal(
                            "runtime fencing resources disappeared after cleanup reaper spawn failure"
                                .to_string(),
                        )
                    })?;
                self.runtime = Some(runtime);
                self.lease_heartbeat = resources.0;
                self.lease = resources.1;
                self.lease_guard = resources.2;
                Err(MidgeError::ResourceLimit(format!(
                    "failed to spawn runtime fencing cleanup reaper: {error}"
                )))
            }
        }
    }

    fn wait_for_fencing_cleanup(&mut self, timeout: Duration) -> MidgeResult<()> {
        enum CleanupWait {
            KnownComplete,
            RuntimeComplete(MidgeResult<()>),
            Timeout,
            Disconnected,
        }

        let cleanup = self
            .pending_fencing_cleanup
            .as_ref()
            .ok_or_else(|| MidgeError::Internal("fencing cleanup was not scheduled".to_string()))?;
        let wait_result = match cleanup {
            PendingFencingCleanup::Known { completion, .. } => {
                match completion.recv_timeout(timeout) {
                    Ok(()) => CleanupWait::KnownComplete,
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => CleanupWait::Timeout,
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        CleanupWait::Disconnected
                    }
                }
            }
            PendingFencingCleanup::Runtime { completion } => {
                match completion.recv_timeout(timeout) {
                    Ok(result) => CleanupWait::RuntimeComplete(result),
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => CleanupWait::Timeout,
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        CleanupWait::Disconnected
                    }
                }
            }
        };

        match wait_result {
            CleanupWait::KnownComplete => {
                let cleanup = self.pending_fencing_cleanup.take().ok_or_else(|| {
                    MidgeError::Internal("fencing cleanup result was lost".to_string())
                })?;
                let PendingFencingCleanup::Known {
                    terminal_result, ..
                } = cleanup
                else {
                    return Err(MidgeError::Internal(
                        "fencing cleanup result kind changed while waiting".to_string(),
                    ));
                };
                terminal_result
            }
            CleanupWait::RuntimeComplete(result) => {
                self.pending_fencing_cleanup.take();
                result
            }
            CleanupWait::Timeout => Err(MidgeError::Timeout(
                "fencing cleanup did not complete before shutdown deadline".to_string(),
            )),
            CleanupWait::Disconnected => {
                self.pending_fencing_cleanup.take();
                Err(MidgeError::Internal(
                    "fencing cleanup reaper terminated without reporting completion".to_string(),
                ))
            }
        }
    }

    fn verify_storage_path_internal(
        db_path: &Path,
        runtime_health: Option<EngineHealth>,
    ) -> MidgeResult<StorageVerificationReport> {
        verification::verify_storage_path(db_path, runtime_health)
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
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    ) -> MidgeResult<PathBuf> {
        startup::CloudStartupRecovery::materialize_cloud_wal_recovery_dir(
            cloud,
            db_path,
            recovery_policy,
            catalog,
        )
        .map(|plan| plan.replay_dir)
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
        if let Some(ref heartbeat_mutex) = self.lease_heartbeat {
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
        // Hold the registry acquisition write guard from snapshot capture
        // through pin publication. GC samples the same registry under a read
        // guard, so an obsolete SST cannot be deleted in the capture/register
        // window.
        let _snapshot_acquisition = self.runtime_handle.begin_snapshot_acquisition();

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

        let pinned_sst_names = read_snapshot
            .sst_files
            .iter()
            .map(|file| file.name.clone())
            .collect::<Vec<_>>();

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
    /// rejects new work. `Busy` and `Timeout` leave it retryable; callers may
    /// release active transactions or wait for blocked storage I/O and invoke
    /// this method again. The timeout bounds the caller's wait; it does not
    /// detach or cancel in-flight durability I/O. Writer fencing remains held
    /// by the cleanup reaper until every runtime worker has actually exited.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::Busy` while transactions remain active,
    /// `MidgeError::Timeout` when the deadline elapses, or a durability error
    /// reported by the runtime during its final flush. Returns
    /// `MidgeError::ResourceLimit` if the fencing cleanup worker cannot start.
    pub fn shutdown(&mut self, timeout: Duration) -> MidgeResult<()> {
        let started = std::time::Instant::now();
        if self.pending_fencing_cleanup.is_some() {
            return self.wait_for_fencing_cleanup(timeout);
        }

        let Some(runtime) = self.runtime.as_mut() else {
            if !self.has_fencing_resources() {
                return self
                    .runtime_handle
                    .shutdown(timeout.saturating_sub(started.elapsed()));
            }
            self.schedule_fencing_cleanup(Ok(()))?;
            return self.wait_for_fencing_cleanup(timeout.saturating_sub(started.elapsed()));
        };
        let shutdown_result = self.runtime_handle.shutdown(timeout);
        if matches!(&shutdown_result, Err(MidgeError::Busy(_))) {
            return shutdown_result;
        }
        if matches!(&shutdown_result, Err(MidgeError::Timeout(_))) {
            self.schedule_runtime_fencing_cleanup()?;
            return shutdown_result;
        }

        let remaining = timeout.saturating_sub(started.elapsed());
        if !runtime.wait_for_exit(remaining) {
            return Err(MidgeError::Timeout(
                "runtime workers did not terminate before shutdown deadline".to_string(),
            ));
        }

        self.runtime.take();
        self.ingest_coordinators.clear();
        if !self.has_fencing_resources() {
            return shutdown_result;
        }
        self.schedule_fencing_cleanup(shutdown_result)?;
        self.wait_for_fencing_cleanup(timeout.saturating_sub(started.elapsed()))
    }

    // === Column Family Lifecycle ===

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
                if !self.cloud_mode {
                    self.runtime_handle
                        .send_and_wait(RuntimeMsg::ManifestPersist {
                            request_id: next_request_id()?,
                        })?;
                }

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
    /// mode. Returns [`MidgeError::Busy`] when committed data remains in the
    /// active memtable; call [`Self::drop_column_family_discarding_unflushed`]
    /// only when intentionally discarding it. Other errors report persistence
    /// or runtime failures.
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
                if !self.cloud_mode {
                    self.runtime_handle
                        .send_and_wait(RuntimeMsg::ManifestPersist {
                            request_id: next_request_id()?,
                        })?;
                }

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

    /// Get an operator-facing snapshot of runtime metrics and health, bounded by `timeout`.
    ///
    /// Unlike [`Engine::get_runtime_metrics`], this never blocks past `timeout`: if the
    /// runtime cannot respond in time, the pending response slot is unregistered (so it
    /// cannot leak) and `MidgeError::Timeout` is returned.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime cannot provide a metrics snapshot, or
    /// `MidgeError::Timeout` if `timeout` elapses first.
    pub fn get_runtime_metrics_with_timeout(
        &self,
        timeout: Duration,
    ) -> MidgeResult<RuntimeMetricsSnapshot> {
        if timeout.is_zero() {
            return Err(MidgeError::Timeout(
                "get_runtime_metrics_with_timeout deadline is zero".to_string(),
            ));
        }

        let response = self.runtime_handle.send_and_wait_timeout(
            RuntimeMsg::GetRuntimeMetrics {
                request_id: next_request_id()?,
            },
            timeout,
        )?;

        match response {
            Some(RuntimeResponse::RuntimeMetricsSnapshot { snapshot, .. }) => Ok(*snapshot),
            Some(RuntimeResponse::Error { error, .. }) => Err(error),
            Some(other) => Err(MidgeError::Internal(format!(
                "Unexpected response from GetRuntimeMetrics: {other:?}"
            ))),
            None => Err(MidgeError::Timeout(
                "get_runtime_metrics_with_timeout exceeded the deadline".to_string(),
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
    /// The caller-provided timeout covers runtime health capture, verification-barrier
    /// acquisition, the storage pass, and barrier release. If the storage pass itself
    /// outlives the deadline, a background verifier retains the barrier until it can
    /// release it safely.
    ///
    /// # Errors
    ///
    /// Returns an error when verification is unsupported, times out, or fails.
    pub fn verify_storage(&self, timeout: Duration) -> MidgeResult<StorageVerificationReport> {
        self.runtime_handle.ensure_open()?;
        if self.memory_mode {
            return Err(MidgeError::NotSupported(
                "storage verification is not supported in memory mode".to_string(),
            ));
        }

        if timeout.is_zero() {
            return Err(MidgeError::Timeout(
                "online storage verification deadline is zero".to_string(),
            ));
        }

        let started = std::time::Instant::now();
        let health_request_id = next_request_id()?;
        let runtime_health = match self.runtime_handle.send_and_wait_timeout(
            RuntimeMsg::GetRuntimeMetrics {
                request_id: health_request_id,
            },
            timeout,
        )? {
            Some(RuntimeResponse::RuntimeMetricsSnapshot { snapshot, .. }) => snapshot.health,
            Some(RuntimeResponse::Error { error, .. }) => return Err(error),
            Some(other) => {
                return Err(MidgeError::Internal(format!(
                    "unexpected runtime-health response before verification: {other:?}"
                )));
            }
            None => {
                return Err(MidgeError::Timeout(
                    "runtime health capture exceeded the verification deadline".to_string(),
                ));
            }
        };
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(MidgeError::Timeout(
                "online storage verification deadline elapsed during health capture".to_string(),
            ));
        }

        verification::verify_storage_online(
            &self.runtime_handle,
            self.db_path.clone(),
            runtime_health,
            self.cloud_mode,
            remaining,
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
mod tests;
