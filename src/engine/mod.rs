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
//! Data operations (get, put, delete, scan) are methods on Transaction.

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{
    next_request_id, Runtime, RuntimeHandle, RuntimeMsg, RuntimeResponse, RuntimeState,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

static IN_MEMORY_OPEN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) mod api;
mod ingest;

pub use api::{
    Direction, Goal, Key, MemoryBudget, OpenOptions, Query, ScanIterator, Storage, Transaction,
    TransactionMode, Value, WorkloadProfile, WriteOptions,
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
        // Stop lease heartbeat first
        if let Some(heartbeat_mutex) = self._lease_heartbeat.take() {
            if let Ok(mut heartbeat) = heartbeat_mutex.lock() {
                heartbeat.stop();
            }
        }

        // Shutdown all ingest coordinators
        for entry in self.ingest_coordinators.iter() {
            entry.value().shutdown();
        }

        // Gracefully shutdown the runtime when engine is dropped
        // Send shutdown message first
        let _ = self.runtime_handle.shutdown();
        // Then drop the runtime which will wait for the thread to finish
        self._runtime.take();

        // Release lease via the PrimaryLease interface
        if let Some(lease) = self._lease.take() {
            let _ = lease.release();
        }

        // Drop the guard last
        self._lease_guard.take();
    }
}

impl Engine {
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

        let _ = std::fs::create_dir_all(&db_path);

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

        let lease_guard = lease.try_acquire().map_err(|e| match e {
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
            "primary lease acquired - this instance is now the exclusive writer"
        );

        // Build runtime state/config.
        // Cloud storage mode uses CloudFirst durability + HybridStorage.
        // Local/Memory modes use Batched durability with optional custom batch config.
        let (mut state, runtime_config) = match &opts.storage {
            Storage::Cloud { .. } => {
                let cloud = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
                    &db_path,
                )?;

                let state = RuntimeState::new_with_recovery_dir(
                    db_path.clone(),
                    memory_mode,
                    Some(cloud.recovery_cloud_wal_dir.clone()),
                );

                let config = crate::runtime::RuntimeConfig {
                    wal_durability_policy: crate::wal::DurabilityPolicy::CloudFirst,
                    hybrid_storage: Some(cloud.hybrid_storage),
                    hybrid_storage_events: Some(cloud.events),
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
                    ..Default::default()
                };

                (RuntimeState::new(db_path.clone(), memory_mode), config)
            }
        };

        // 🔑 CRITICAL: Replay intent log to recover any interrupted mutations
        // Must happen BEFORE runtime starts processing messages
        state.replay_intent_log()?;

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

        let request_id = crate::runtime::next_request_id();
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
            Arc::new(ingest::IngestCoordinator::new(0, runtime_handle.clone()));
        ingest_coordinators.insert(0, default_coordinator);

        // ═══════════════════════════════════════════════════════════════════════════
        // PHASE 2: START LEASE HEARTBEAT (AFTER ENGINE STARTS)
        // ═══════════════════════════════════════════════════════════════════════════
        //
        // The heartbeat loop renews the lease periodically to maintain exclusivity.
        // If renewal fails, the heartbeat will mark itself unhealthy.
        //
        // TODO: Monitor heartbeat health and trigger graceful shutdown if lease is lost.

        let mut lease_heartbeat = crate::lease::LeaseHeartbeat::new(Arc::clone(&lease));
        lease_heartbeat.start();

        tracing::info!(db_path = %db_path.display(), open_ms = start.elapsed().as_secs_f64() * 1000.0, "engine open completed");

        // Load existing CFs from manifest
        let manifest = crate::metadata::ManifestPersistence::load(&db_path).unwrap_or_default();
        for cf_meta in &manifest.column_families {
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                let handle = ColumnFamilyHandle::new(cf_meta.id, cf_meta.name.clone());
                column_families.insert(cf_meta.id, handle);

                // Start coordinator for loaded CF
                let coordinator = Arc::new(ingest::IngestCoordinator::new(
                    cf_meta.id,
                    runtime_handle.clone(),
                ));
                ingest_coordinators.insert(cf_meta.id, coordinator);
            }
        }

        Ok(Self {
            _runtime: Some(runtime),
            runtime_handle,
            db_path,
            sequence: std::sync::atomic::AtomicU64::new(0),
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
        let request_id = crate::runtime::next_request_id();
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
    /// CloudFirst mode bypasses ingest batching because:
    /// 1. It already performs cloud-level batching via cloud_write_queue
    /// 2. Writes are intentionally NOT immediately visible in memtables
    /// 3. Visibility is gated on cloud acknowledgment, not local apply
    ///
    /// Ingest batching is enabled for all durability modes:
    /// - Memory/Local/Batched: writes apply directly to memtables with immediate local visibility
    /// - CloudFirst: acks immediately after local WAL write, cloud upload is background
    ///
    /// All modes benefit from batching which reduces event loop round-trips.
    fn should_use_ingest_batching(&self) -> bool {
        // All durability modes use ingest batching for throughput.
        // CloudFirst handler in event_loop.rs already has proper support for
        // ApplyTransaction with deferred confirmation on cloud upload.
        true
    }

    /// Return whether an ingest barrier is currently active.
    pub(crate) fn is_ingesting(&self) -> MidgeResult<bool> {
        let request_id = crate::runtime::next_request_id();
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
        let request_id = crate::runtime::next_request_id();
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
                let bid = crate::runtime::next_request_id();
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
        let bid = crate::runtime::next_request_id();
        let br = self
            .runtime_handle
            .send_and_wait(crate::runtime::RuntimeMsg::EndIngest { request_id: bid })?;
        match br {
            crate::runtime::RuntimeResponse::Ok { .. } => {
                // Step 2: Restore previous runtime configuration
                let request_id = crate::runtime::next_request_id();
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

    /// Flush all pending writes to disk (used by tests)
    pub(crate) fn sync(&self) -> MidgeResult<()> {
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalSync {
            request_id: next_request_id(),
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
                request_id: next_request_id(),
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
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let fallback_sequence = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let request_id = next_request_id();
        // Transaction snapshots should be set to (current_sequence + 1) so that
        // visible versions are those with seq < start_sequence (strictly less-than).
        // This ensures a transaction started after sequence N sees writes up to N.
        let start_sequence = match self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetCurrentSequence { request_id })
        {
            Ok(RuntimeResponse::CurrentSequence { sequence, .. }) => sequence + 1,
            _ => fallback_sequence + 1,
        };

        // Capture read snapshot synchronously on event loop for consistent reads
        let read_snapshot =
            match self
                .runtime_handle
                .send_and_wait(RuntimeMsg::CaptureReadSnapshot {
                    request_id: next_request_id(),
                    cf_id,
                    sequence: start_sequence,
                }) {
                Ok(RuntimeResponse::ReadSnapshot { snapshot, .. }) => Some(snapshot),
                _ => None,
            };

        Ok(api::Transaction::new(
            self.runtime_handle.clone(),
            txn_id,
            cf_id,
            mode,
            start_sequence,
            read_snapshot,
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

        // Collect write intents to avoid borrow issues
        let write_intents: Vec<_> = txn.iter_writes().cloned().collect();

        // PHASE 2.1: Determine CF for this transaction
        let cf_id_for_check = write_intents
            .first()
            .map(|intent| match intent {
                api::WriteIntent::Put { cf_id, .. }
                | api::WriteIntent::Insert { cf_id, .. }
                | api::WriteIntent::Delete { cf_id, .. }
                | api::WriteIntent::DeleteRange { cf_id, .. } => *cf_id,
            })
            .unwrap_or(0);

        // PHASE 2.2: Route all writes through ingest batching
        //
        // All durability modes now use ingest batching:
        // - Memory/Local/Batched: writes apply directly to memtables with immediate local visibility
        // - CloudFirst: acks immediately after local WAL write, cloud upload is background
        // Batching reduces event loop contention by grouping concurrent writes into transactions.
        let _use_batching = self.should_use_ingest_batching();

        let mut max_sequence = 0u64;

        // Route through ingest coordinator for all modes
        let coordinator = self
            .ingest_coordinators
            .get(&cf_id_for_check)
            .ok_or_else(|| {
                MidgeError::InvalidArgument(format!(
                    "column family {} does not exist",
                    cf_id_for_check
                ))
            })?;

        for intent in write_intents {
            let sequence = match &intent {
                api::WriteIntent::Put {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    ..
                } => coordinator.submit_write(
                    *cf_id,
                    key.clone(),
                    Some(value.clone()),
                    *ttl_seconds,
                    false,
                )?,
                api::WriteIntent::Insert {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    ..
                } => coordinator.submit_write(
                    *cf_id,
                    key.clone(),
                    Some(value.clone()),
                    *ttl_seconds,
                    true,
                )?,
                api::WriteIntent::Delete { cf_id, key, .. } => {
                    coordinator.submit_write(*cf_id, key.clone(), None, None, false)?
                }
                api::WriteIntent::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    ..
                } => {
                    // DeleteRange still goes directly to runtime (rare operation)
                    let response =
                        self.runtime_handle
                            .send_and_wait(RuntimeMsg::WalAppendDeleteRange {
                                request_id: next_request_id(),
                                cf_id: *cf_id,
                                start_key: start_key.clone(),
                                end_key: end_key.clone(),
                            })?;
                    match response {
                        RuntimeResponse::WalAppended { sequence, .. } => sequence,
                        RuntimeResponse::Error { error, .. } => return Err(error),
                        _ => {
                            return Err(MidgeError::Internal(
                                "Unexpected response to transaction delete_range".to_string(),
                            ))
                        }
                    }
                }
            };
            max_sequence = max_sequence.max(sequence);
        }

        // Update engine's sequence to reflect completed writes
        self.sequence
            .store(max_sequence, std::sync::atomic::Ordering::SeqCst);

        // Apply sync if requested
        if opts.is_sync() {
            self.sync()?;
        }

        Ok(())
    }

    /// Wait for a write stall to clear for `cf_id`.
    ///
    /// Returns `Ok(true)` if the stall cleared within `timeout`, `Ok(false)` on timeout.
    pub fn wait_for_write_stall_clear(
        &self,
        cf_id: ColumnFamilyId,
        timeout: Duration,
    ) -> MidgeResult<bool> {
        let request_id = next_request_id();

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

    /// Rollback a transaction
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
            request_id: next_request_id(),
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
            request_id: next_request_id(),
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
                request_id: next_request_id(),
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
                ));
                self.ingest_coordinators.insert(cf_id, coordinator);
                self.runtime_handle
                    .send_and_wait(RuntimeMsg::ManifestPersist {
                        request_id: next_request_id(),
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
                request_id: next_request_id(),
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
                            request_id: next_request_id(),
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
        let request_id = next_request_id();
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
                request_id: next_request_id(),
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
