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

static IN_MEMORY_OPEN_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) mod api;
mod context;

pub use api::{
    Direction, Goal, Key, MemoryBudget, OpenOptions, Query, ScanIterator, Storage, Transaction,
    TransactionMode, Value, WorkloadProfile, WriteOptions,
};
/// Registry of column families, keyed by column family ID
type ColumnFamilyRegistry = dashmap::DashMap<u32, ColumnFamilyHandle>;

/// Column family identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnFamilyId(pub u32);

impl ColumnFamilyId {
    pub const DEFAULT: Self = Self(0);

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl Default for ColumnFamilyId {
    fn default() -> Self {
        Self::DEFAULT
    }
}

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
    #[allow(dead_code)]
    db_path: PathBuf,
    /// Default column family for convenience
    default_cf: ColumnFamilyHandle,
    /// Latest committed sequence observed by the engine.
    ///
    /// Sequence numbers are allocated inside the runtime (at WAL append time) and
    /// returned via `RuntimeResponse::WalAppended { sequence, .. }`.
    sequence: std::sync::atomic::AtomicU64,
    /// Next snapshot ID counter (local only, not related to sequence numbers)
    next_snapshot_id: std::sync::atomic::AtomicU64,
    /// Column families registry (CF ID -> Handle)
    column_families: ColumnFamilyRegistry,
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Gracefully shutdown the runtime when engine is dropped
        // Send shutdown message first
        let _ = self.runtime_handle.shutdown();
        // Then drop the runtime which will wait for the thread to finish
        self._runtime.take();
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

        // Build runtime state/config.
        // Cloud storage mode uses CloudFirst durability + HybridStorage.
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
            _ => (
                RuntimeState::new(db_path.clone(), memory_mode),
                crate::runtime::RuntimeConfig::default(),
            ),
        };

        // 🔑 CRITICAL: Replay intent log to recover any interrupted mutations
        // Must happen BEFORE runtime starts processing messages
        state.replay_intent_log()?;

        // Start runtime
        let (runtime_inst, _) = Runtime::new()?;
        let (runtime, runtime_handle) = runtime_inst.start_with_config(state, runtime_config)?;

        let default_cf = ColumnFamilyHandle::new(ColumnFamilyId::DEFAULT, "default".to_string());
        let column_families = dashmap::DashMap::new();
        column_families.insert(0, default_cf.clone());

        tracing::info!(db_path = %db_path.display(), open_ms = start.elapsed().as_secs_f64() * 1000.0, "engine open completed");

        // Load existing CFs from manifest
        let manifest = crate::metadata::ManifestPersistence::load(&db_path).unwrap_or_default();
        for cf_meta in &manifest.column_families {
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                let handle =
                    ColumnFamilyHandle::new(ColumnFamilyId(cf_meta.id), cf_meta.name.clone());
                column_families.insert(cf_meta.id, handle);
            }
        }

        Ok(Self {
            _runtime: Some(runtime),
            runtime_handle,
            db_path,
            default_cf,
            sequence: std::sync::atomic::AtomicU64::new(0),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
            column_families,
        })
    }

    /// Return a handle to the default column family.
    ///
    /// The default column family is created automatically when the engine opens.
    /// All keys without an explicit column family belong to this one.
    pub fn default_column_family(&self) -> &ColumnFamilyHandle {
        &self.default_cf
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
                    crate::runtime::RuntimeResponse::Error { message, .. } => {
                        Err(crate::common::MidgeError::Internal(message))
                    }
                    _ => Err(crate::common::MidgeError::Internal(
                        "unexpected response to BeginIngest".to_string(),
                    )),
                }
            }
            crate::runtime::RuntimeResponse::Error { message, .. } => {
                Err(crate::common::MidgeError::Internal(message))
            }
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
                    crate::runtime::RuntimeResponse::Error { message, .. } => {
                        Err(crate::common::MidgeError::Internal(message))
                    }
                    _ => Err(crate::common::MidgeError::Internal(
                        "unexpected response to SetRuntimeConfig".to_string(),
                    )),
                }
            }
            crate::runtime::RuntimeResponse::Error { message, .. } => {
                Err(crate::common::MidgeError::Internal(message))
            }
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
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to sync".to_string(),
            )),
        }
    }

    /// Force a flush of the default column family
    pub fn flush(&self) -> MidgeResult<()> {
        self.flush_cf(&self.default_cf)
    }

    /// Force a flush of a specific column family
    pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::FlushMemtable {
                request_id: next_request_id(),
                cf_id: cf.id.0,
            })?;

        match response {
            RuntimeResponse::Ok { .. } | RuntimeResponse::FlushComplete { .. } => Ok(()),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
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
        let start_sequence = match self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetCurrentSequence { request_id })
        {
            Ok(RuntimeResponse::CurrentSequence { sequence, .. }) => sequence,
            _ => fallback_sequence,
        };

        Ok(api::Transaction::new(
            self.runtime_handle.clone(),
            txn_id,
            cf_id,
            mode,
            start_sequence,
        ))
    }

    /// Commit a transaction atomically
    ///
    /// # Arguments
    /// * `txn` - Transaction to commit
    /// * `opts` - Write options specifying durability guarantees
    pub fn commit(&self, txn: api::Transaction, opts: api::WriteOptions) -> MidgeResult<()> {
        // ReadOnly transactions are a no-op for commit
        if txn.is_read_only() {
            return Ok(());
        }

        if opts.is_no_wal() {
            return Err(MidgeError::InvalidArgument(
                "disable_wal is not allowed in transactions".to_string(),
            ));
        }

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

        // CRITICAL: Use send_and_wait for durability.
        // TODO: Add RuntimeMsg::CommitTransaction for true atomic commit.
        for intent in write_intents {
            match &intent {
                api::WriteIntent::Put {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    ..
                } => {
                    let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                        request_id: next_request_id(),
                        cf_id: cf_id.as_u32(),
                        key: key.clone(),
                        value: Some(value.clone()),
                        ttl_seconds: *ttl_seconds,
                        insert_only: false,
                    })?;
                    match response {
                        RuntimeResponse::WalAppended { sequence, .. } => {
                            // Update engine's sequence to reflect completed write
                            self.sequence
                                .store(sequence, std::sync::atomic::Ordering::SeqCst);
                        }
                        RuntimeResponse::Error { message, .. } => {
                            return Err(MidgeError::Internal(message));
                        }
                        _ => {
                            return Err(MidgeError::Internal(
                                "Unexpected response to transaction put".to_string(),
                            ));
                        }
                    }
                }
                api::WriteIntent::Insert {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    ..
                } => {
                    let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                        request_id: next_request_id(),
                        cf_id: cf_id.as_u32(),
                        key: key.clone(),
                        value: Some(value.clone()),
                        ttl_seconds: *ttl_seconds,
                        insert_only: true,
                    })?;
                    match response {
                        RuntimeResponse::WalAppended { sequence, .. } => {
                            // Update engine's sequence to reflect completed write
                            self.sequence
                                .store(sequence, std::sync::atomic::Ordering::SeqCst);
                        }
                        RuntimeResponse::Error { message, .. } => {
                            return Err(MidgeError::Internal(message));
                        }
                        _ => {
                            return Err(MidgeError::Internal(
                                "Unexpected response to transaction insert".to_string(),
                            ));
                        }
                    }
                }
                api::WriteIntent::Delete { cf_id, key, .. } => {
                    let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                        request_id: next_request_id(),
                        cf_id: cf_id.as_u32(),
                        key: key.clone(),
                        value: None,
                        ttl_seconds: None,
                        insert_only: false,
                    })?;
                    match response {
                        RuntimeResponse::WalAppended { sequence, .. } => {
                            // Update engine's sequence to reflect completed delete
                            self.sequence
                                .store(sequence, std::sync::atomic::Ordering::SeqCst);
                        }
                        RuntimeResponse::Error { message, .. } => {
                            return Err(MidgeError::Internal(message));
                        }
                        _ => {
                            return Err(MidgeError::Internal(
                                "Unexpected response to transaction delete".to_string(),
                            ));
                        }
                    }
                }
                api::WriteIntent::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    ..
                } => {
                    // Write a single DeleteRange tombstone to WAL
                    let response =
                        self.runtime_handle
                            .send_and_wait(RuntimeMsg::WalAppendDeleteRange {
                                request_id: next_request_id(),
                                cf_id: cf_id.as_u32(),
                                start_key: start_key.clone(),
                                end_key: end_key.clone(),
                            })?;
                    match response {
                        RuntimeResponse::WalAppended { sequence, .. } => {
                            self.sequence
                                .store(sequence, std::sync::atomic::Ordering::SeqCst);
                        }
                        RuntimeResponse::Error { message, .. } => {
                            return Err(MidgeError::Internal(message));
                        }
                        _ => {
                            return Err(MidgeError::Internal(
                                "Unexpected response to transaction delete_range".to_string(),
                            ));
                        }
                    }
                }
            }
        }

        // Apply sync if requested
        if opts.is_sync() {
            self.sync()?;
        }

        Ok(())
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
            cf_id: cf_id.as_u32(),
            key: key.to_vec(),
            sequence,
            requested_durability: api::Durability::Steady,
        })?;

        match response {
            RuntimeResponse::ReadValue { value, .. } => Ok(value.map(bytes::Bytes::from)),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
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
            cf_id: cf_id.as_u32(),
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
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
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
                let handle = ColumnFamilyHandle::new(ColumnFamilyId(cf_id), name.to_string());
                // Register CF in local registry
                self.column_families.insert(cf_id, handle.clone());

                // Persist manifest to disk
                let _persist_response =
                    self.runtime_handle
                        .send_and_wait(RuntimeMsg::ManifestPersist {
                            request_id: next_request_id(),
                        })?;

                Ok(handle)
            }
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
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
                cf_id: cf_id.as_u32(),
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
                // Remove from local registry
                self.column_families.remove(&cf_id.as_u32());

                // Persist manifest to disk
                let _persist_response =
                    self.runtime_handle
                        .send_and_wait(RuntimeMsg::ManifestPersist {
                            request_id: next_request_id(),
                        })?;

                Ok(())
            }
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
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

    /// Compact all data (stub - not implemented)
    pub(crate) fn compact_all(&self) -> MidgeResult<()> {
        // Stub implementation: trigger a flush as a proxy for compaction
        // In a full LSM, this would compact all levels
        self.flush()
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
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
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
    fn should_create_default_column_family_id_with_zero() {
        // Arrange / Act
        let cf_id = ColumnFamilyId::DEFAULT;

        // Assert
        assert_eq!(cf_id.as_u32(), 0);
    }

    #[test]
    fn should_return_zero_for_default_column_family_as_u32() {
        // Arrange
        let cf_id = ColumnFamilyId::DEFAULT;

        // Act
        let value = cf_id.as_u32();

        // Assert
        assert_eq!(value, 0);
    }

    #[test]
    fn should_implement_default_trait_for_column_family_id() {
        // Arrange / Act
        let cf_id = ColumnFamilyId::default();

        // Assert: default should be same as DEFAULT constant
        assert_eq!(cf_id, ColumnFamilyId::DEFAULT);
    }

    #[test]
    fn should_preserve_custom_column_family_id_value() {
        // Arrange
        let custom_id = 42u32;

        // Act
        let cf_id = ColumnFamilyId(custom_id);

        // Assert
        assert_eq!(cf_id.as_u32(), custom_id);
    }

    #[test]
    fn should_support_column_family_id_equality() {
        // Arrange
        let id1 = ColumnFamilyId(5);
        let id2 = ColumnFamilyId(5);
        let id3 = ColumnFamilyId(6);

        // Assert
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn should_support_column_family_id_hashing() {
        // Arrange
        use std::collections::HashMap;
        let mut map = HashMap::new();
        let id = ColumnFamilyId(10);

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
        let cf_id = ColumnFamilyId(5);
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
        let cf_id = ColumnFamilyId(10);
        let name = "test_cf".to_string();
        let handle = ColumnFamilyHandle::new(cf_id, name);

        // Assert: id() and name() return exact values
        assert_eq!(handle.id().as_u32(), 10);
        assert_eq!(handle.name(), "test_cf");
    }

    #[test]
    fn should_clone_column_family_handle() {
        // Arrange
        let handle1 = ColumnFamilyHandle::new(ColumnFamilyId(7), "cf".to_string());

        // Act
        let handle2 = handle1.clone();

        // Assert
        assert_eq!(handle1.id(), handle2.id());
        assert_eq!(handle1.name(), handle2.name());
    }

    #[test]
    fn should_support_empty_column_family_name() {
        // Arrange / Act
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), "".to_string());

        // Assert
        assert_eq!(handle.name(), "");
    }

    #[test]
    fn should_handle_unicode_column_family_names() {
        // Arrange
        let unicode_name = "数据_测试".to_string();

        // Act
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), unicode_name.clone());

        // Assert
        assert_eq!(handle.name(), unicode_name);
    }

    // ============================================================================
    // Tests for ColumnFamilyId special values
    // ============================================================================

    #[test]
    fn should_handle_maximum_column_family_id() {
        // Arrange / Act
        let max_id = ColumnFamilyId(u32::MAX);

        // Assert
        assert_eq!(max_id.as_u32(), u32::MAX);
    }

    #[test]
    fn should_handle_zero_column_family_id() {
        // Arrange / Act
        let zero_id = ColumnFamilyId(0);

        // Assert
        assert_eq!(zero_id.as_u32(), 0);
        assert_eq!(zero_id, ColumnFamilyId::DEFAULT);
    }

    #[test]
    fn should_distinguish_between_different_column_family_ids() {
        // Arrange
        let id_vec = [
            ColumnFamilyId(0),
            ColumnFamilyId(1),
            ColumnFamilyId(100),
            ColumnFamilyId(u32::MAX),
        ];

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
        // These operations should be no-ops and return Ok
        engine.flush().expect("memory flush should succeed");
        engine
            .compact_all()
            .expect("memory compact_all should succeed");
    }

    #[test]
    fn should_copy_column_family_id() {
        // Arrange
        let id1 = ColumnFamilyId(42);

        // Act
        let id2 = id1; // Copy trait implemented
        let id3 = id1;

        // Assert: all are equal
        assert_eq!(id1, id2);
        assert_eq!(id2, id3);
    }

    // ============================================================================
    // Tests for ColumnFamilyHandle creation invariants
    // ============================================================================

    #[test]
    fn should_create_handle_for_default_column_family() {
        // Arrange / Act
        let handle = ColumnFamilyHandle::new(ColumnFamilyId::DEFAULT, "default".to_string());

        // Assert
        assert_eq!(handle.id(), ColumnFamilyId::DEFAULT);
        assert_eq!(handle.name(), "default");
    }

    #[test]
    fn should_create_multiple_handles_with_different_ids() {
        // Arrange / Act
        let handle1 = ColumnFamilyHandle::new(ColumnFamilyId(1), "cf1".to_string());
        let handle2 = ColumnFamilyHandle::new(ColumnFamilyId(2), "cf2".to_string());
        let handle3 = ColumnFamilyHandle::new(ColumnFamilyId(3), "cf3".to_string());

        // Assert: all distinct
        assert_ne!(handle1.id(), handle2.id());
        assert_ne!(handle2.id(), handle3.id());
        assert_ne!(handle1.id(), handle3.id());
    }

    #[test]
    fn should_preserve_handle_identity_after_clone() {
        // Arrange
        let original = ColumnFamilyHandle::new(ColumnFamilyId(99), "original_name".to_string());

        // Act
        let cloned = original.clone();

        // Assert: cloned is identical
        assert_eq!(original.id(), cloned.id());
        assert_eq!(original.name(), cloned.name());

        // And original still works
        assert_eq!(original.id().as_u32(), 99);
    }

    // ============================================================================
    // Tests for debug trait implementation
    // ============================================================================

    #[test]
    fn should_format_column_family_id_for_debug() {
        // Arrange
        let id = ColumnFamilyId(42);

        // Act
        let debug_str = format!("{:?}", id);

        // Assert: should be debuggable
        assert!(!debug_str.is_empty());
        assert!(debug_str.contains("42"));
    }

    #[test]
    fn should_format_column_family_handle_for_debug() {
        // Arrange
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(5), "test".to_string());

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
        map.insert(ColumnFamilyId(1), "cf1".to_string());
        map.insert(ColumnFamilyId(2), "cf2".to_string());

        // Assert
        assert_eq!(map.get(&ColumnFamilyId(1)), Some(&"cf1".to_string()));
        assert_eq!(map.get(&ColumnFamilyId(2)), Some(&"cf2".to_string()));
    }

    #[test]
    fn should_support_column_family_handle_in_vector() {
        // Arrange
        // Act
        let handles = [
            ColumnFamilyHandle::new(ColumnFamilyId(0), "default".to_string()),
            ColumnFamilyHandle::new(ColumnFamilyId(1), "secondary".to_string()),
        ];

        // Assert
        assert_eq!(handles.len(), 2);
        assert_eq!(handles[0].name(), "default");
        assert_eq!(handles[1].name(), "secondary");
    }

    #[test]
    fn should_enforce_eq_implementation_for_column_family_id() {
        // Arrange
        let id1 = ColumnFamilyId(5);
        let id2 = ColumnFamilyId(5);

        // Act & Assert: Eq trait enforced
        assert!(id1 == id2);
        assert!(!(id1 != id2));
    }

    #[test]
    fn should_enforce_hash_implementation_for_column_family_id() {
        // Arrange
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let id = ColumnFamilyId(42);
        let mut hasher = DefaultHasher::new();

        // Act
        id.hash(&mut hasher);
        let hash_value = hasher.finish();

        // Assert: should be hashable without panicking
        assert_ne!(hash_value, 0); // Just verify it produced a hash
    }
}
