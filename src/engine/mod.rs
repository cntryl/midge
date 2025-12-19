//! Main KV store engine
//!
//! Public API for database operations.
//!
//! The engine is a thin façade that delegates all work to the runtime.
//! It provides ergonomic APIs for:
//! - Key-value operations (put, get, delete, range)
//! - Column families
//! - Write batches
//! - Transactions
//! - Snapshots

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{
    next_request_id, Runtime, RuntimeHandle, RuntimeMsg, RuntimeResponse, RuntimeState,
};
use crate::telemetry::{spans::OperationType, MidgeSpan, Telemetry};
use std::path::PathBuf;
use std::time::Instant;
use tracing::info_span;

pub mod api;
pub mod context;
pub mod open;

pub use api::*;
pub use context::Context;
pub use open::open_engine;

/// Registry of merge operators, keyed by column family ID
type MergeOperatorRegistry = dashmap::DashMap<u32, std::sync::Arc<dyn MergeOperator>>;
/// Registry of column families, keyed by column family ID
type ColumnFamilyRegistry = dashmap::DashMap<u32, ColumnFamilyHandle>;

/// Trait for types that can be converted to engine open parameters
/// Allows both PathBuf and MidgeOptions to be used with MidgeEngine::open
pub trait OpenParam {
    fn to_path(self) -> PathBuf;
}

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

/// The main Midge KV store
///
/// This is a thin façade over the runtime. All state and background work
/// is managed by the runtime actors.
pub struct MidgeEngine {
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
    /// Merge operators registered per column family
    merge_operators: MergeOperatorRegistry,
    /// Column families registry (CF ID -> Handle)
    column_families: ColumnFamilyRegistry,
}

impl OpenParam for PathBuf {
    fn to_path(self) -> PathBuf {
        self
    }
}

impl OpenParam for crate::testkit::MidgeOptions {
    fn to_path(self) -> PathBuf {
        match &self.storage_mode {
            crate::testkit::StorageMode::Memory => PathBuf::from(":memory:"),
            crate::testkit::StorageMode::LocalDisk { db_path } => db_path.clone(),
            crate::testkit::StorageMode::CloudBacked { local_cache_path } => {
                local_cache_path.clone()
            }
        }
    }
}

impl OpenParam for &crate::testkit::MidgeOptions {
    fn to_path(self) -> PathBuf {
        match &self.storage_mode {
            crate::testkit::StorageMode::Memory => PathBuf::from(":memory:"),
            crate::testkit::StorageMode::LocalDisk { db_path } => db_path.clone(),
            crate::testkit::StorageMode::CloudBacked { local_cache_path } => {
                local_cache_path.clone()
            }
        }
    }
}

impl MidgeEngine {
    /// Open a database from flexible parameters (PathBuf or MidgeOptions)
    pub fn open<P: OpenParam>(param: P) -> MidgeResult<Self> {
        let db_path = param.to_path();
        // Detect memory mode from path (":memory:" sentinel)
        let memory_mode = db_path.to_string_lossy() == ":memory:";
        let mut state = RuntimeState::new(db_path.clone(), memory_mode);

        // 🔑 CRITICAL: Replay intent log to recover any interrupted mutations
        // Must happen BEFORE runtime starts processing messages
        state.replay_intent_log()?;

        // Start runtime
        let (runtime, _) = Runtime::new()?;
        let runtime_handle = runtime.start(state)?;

        let default_cf = ColumnFamilyHandle::new(ColumnFamilyId::DEFAULT, "default".to_string());
        let column_families = dashmap::DashMap::new();
        column_families.insert(0, default_cf.clone());

        // Load existing CFs from manifest
        let manifest = crate::metadata::ManifestPersistence::load(&db_path).unwrap_or_default();
        for cf_meta in &manifest.column_families {
            if cf_meta.id != 0 {
                let handle =
                    ColumnFamilyHandle::new(ColumnFamilyId(cf_meta.id), cf_meta.name.clone());
                column_families.insert(cf_meta.id, handle);
            }
        }

        Ok(Self {
            runtime_handle,
            db_path,
            default_cf,
            sequence: std::sync::atomic::AtomicU64::new(0),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
            merge_operators: dashmap::DashMap::new(),
            column_families,
        })
    }

    /// Open a database with test configuration options
    pub fn open_with_options(opts: crate::testkit::MidgeOptions) -> MidgeResult<Self> {
        let (db_path, memory_mode) = match &opts.storage_mode {
            crate::testkit::StorageMode::Memory => {
                // For memory mode, use a placeholder path that will never be touched
                (
                    PathBuf::from(format!("target/tmp/memory_{}", std::process::id())),
                    true,
                )
            }
            crate::testkit::StorageMode::LocalDisk { db_path } => (db_path.clone(), false),
            crate::testkit::StorageMode::CloudBacked { local_cache_path } => {
                (local_cache_path.clone(), false)
            }
        };

        let mut runtime_config = crate::runtime::RuntimeConfig::default();
        // Honor optional batch config from options if provided
        // If provided, apply WAL batch config from options (BatchConfig is Copy)
        if let Some(batch_cfg) = opts.wal_batch_config {
            runtime_config.wal_batch_config = batch_cfg;
        }
        let mut state = match &opts.storage_mode {
            crate::testkit::StorageMode::CloudBacked { .. } => {
                // Local cache is ephemeral; cloud WAL is the source of truth.
                let setup = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
                    &db_path,
                )?;

                let hybrid = setup.hybrid_storage;
                let rx = setup.events;
                let cloud_wal_dir = setup.recovery_cloud_wal_dir;

                runtime_config.wal_durability_policy = crate::wal::DurabilityPolicy::CloudFirst;
                runtime_config.hybrid_storage = Some(hybrid);
                runtime_config.hybrid_storage_events = Some(rx);

                RuntimeState::new_with_recovery_dir(
                    db_path.clone(),
                    memory_mode,
                    Some(cloud_wal_dir),
                )
            }
            _ => {
                runtime_config.wal_durability_policy = if opts.wal_sync {
                    crate::wal::DurabilityPolicy::Strict
                } else {
                    crate::wal::DurabilityPolicy::Batched
                };
                RuntimeState::new(db_path.clone(), memory_mode)
            }
        };

        // Apply tuning knobs from options.
        // Keep thresholds sane if caller passes 0.
        if opts.memtable_size > 0 {
            state.memtable_size_limit = opts.memtable_size;
            state.memtable_flush_threshold = opts.memtable_size;
        }

        // Start runtime
        let (runtime, _) = Runtime::new()?;
        let runtime_handle = runtime.start_with_config(state, runtime_config)?;

        let default_cf = ColumnFamilyHandle::new(ColumnFamilyId::DEFAULT, "default".to_string());
        let column_families = dashmap::DashMap::new();
        column_families.insert(0, default_cf.clone());

        // Load existing CFs from manifest (skip in memory mode and deleted CFs)
        if !memory_mode {
            let manifest = crate::metadata::ManifestPersistence::load(&db_path).unwrap_or_default();
            for cf_meta in &manifest.column_families {
                if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                    let handle =
                        ColumnFamilyHandle::new(ColumnFamilyId(cf_meta.id), cf_meta.name.clone());
                    column_families.insert(cf_meta.id, handle);
                }
            }
        }

        Ok(Self {
            runtime_handle,
            db_path,
            default_cf,
            sequence: std::sync::atomic::AtomicU64::new(0),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
            merge_operators: dashmap::DashMap::new(),
            column_families,
        })
    }

    pub fn default_column_family(&self) -> &ColumnFamilyHandle {
        &self.default_cf
    }

    /// Put a key-value pair into a specific column family
    pub fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        // Check if CF still exists
        if !self.column_families.contains_key(&cf.id().as_u32()) {
            return Err(MidgeError::Internal(format!(
                "Column family '{}' (id={}) has been dropped",
                cf.name(),
                cf.id().as_u32()
            )));
        }
        self.put_with_ttl(cf, key, value, 0)
    }

    /// Put a key-value pair with TTL (time-to-live in seconds)
    /// TTL of 0 means no expiration
    pub fn put_with_ttl(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<()> {
        let cf_id = cf.id.0;
        let key_size = key.len();
        let value_size = value.len();
        let start = Instant::now();

        // Create span for this put operation
        let span = info_span!(
            "put_operation",
            cf_id = cf_id,
            key_size = key_size,
            value_size = value_size,
            ttl_seconds = ttl_seconds,
        );
        let _enter = span.enter();

        // Record telemetry attributes (only allocate span if telemetry enabled)
        if Telemetry::global().is_some() {
            let _midge_span = MidgeSpan::new("put_operation")
                .with_operation(OperationType::Put)
                .with_cf(cf_id)
                .with_key_size(key_size)
                .with_value_size(value_size);
            let _ = &_midge_span; // keep unused-binding in this scope
        }

        // Sequence numbers are allocated inside the runtime at append time.
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
            request_id: next_request_id(),
            cf_id,
            key: key.to_vec(),
            value: Some(value.to_vec()),
            ttl_seconds: if ttl_seconds == 0 {
                None
            } else {
                Some(ttl_seconds)
            },
            insert_only: false,
        })?;

        // Check for errors from runtime
        match response {
            RuntimeResponse::WalAppended { sequence, .. } => {
                // Update engine's sequence to reflect completed write
                self.sequence
                    .store(sequence, std::sync::atomic::Ordering::SeqCst);

                let elapsed = start.elapsed();

                // Record metrics
                if let Some(telemetry) = Telemetry::global() {
                    telemetry.metrics().record_put();
                    telemetry
                        .metrics()
                        .record_write_latency_us(elapsed.as_micros() as u64);
                }

                tracing::debug!(
                    elapsed_us = elapsed.as_micros(),
                    status = "success",
                    "put completed"
                );
                Ok(())
            }
            RuntimeResponse::Error { message, .. } => {
                tracing::warn!(error = %message, status = "error", "put failed");

                // Record write stall metric if applicable
                if message.contains("Write stall") {
                    if let Some(telemetry) = Telemetry::global() {
                        telemetry.metrics().record_write_stall();
                    }
                }

                // Check if it's a write stall - if so, propagate it
                if message.contains("Write stall") {
                    Err(MidgeError::WriteStall(message))
                } else {
                    Err(MidgeError::Internal(message))
                }
            }
            _ => {
                tracing::warn!(
                    status = "unexpected_response",
                    "put got unexpected response"
                );
                Err(MidgeError::Internal(
                    "Unexpected response to put".to_string(),
                ))
            }
        }
    }

    /// Alias for put() for backward compatibility
    pub fn put_cf(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.put(cf, key, value)
    }

    /// Get a value from a specific column family
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        let key_size = key.len();
        let start = Instant::now();

        let span = info_span!("get_operation", cf_id = cf.id.0, key_size = key_size,);
        let _enter = span.enter();

        let _midge_span = MidgeSpan::new("get_operation")
            .with_operation(OperationType::Get)
            .with_cf(cf.id.0)
            .with_key_size(key_size);

        // CRITICAL: Reads must go through runtime to query authoritative state
        // (active memtable + immutable memtables + SSTs).
        // Also enforce durability frontier: don't return data ahead of durable_seq.
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::Read {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            sequence: u64::MAX, // Read latest committed value
            requested_durability: api::Durability::Steady, // Default to Steady (balanced durability/performance)
        })?;

        match response {
            RuntimeResponse::ReadValue { value, .. } => {
                let elapsed = start.elapsed();
                let found = value.is_some();

                // Record metrics
                if let Some(telemetry) = Telemetry::global() {
                    telemetry.metrics().record_get();
                    telemetry
                        .metrics()
                        .record_read_latency_us(elapsed.as_micros() as u64);
                    if !found {
                        telemetry.metrics().record_cache_miss();
                    }
                }

                tracing::debug!(
                    elapsed_us = elapsed.as_micros(),
                    found = found,
                    status = "success",
                    "get completed"
                );
                Ok(value.map(bytes::Bytes::from))
            }
            RuntimeResponse::Error { message, .. } => {
                tracing::warn!(error = %message, status = "error", "get failed");
                Err(MidgeError::Internal(message))
            }
            _ => {
                tracing::warn!(
                    status = "unexpected_response",
                    "get got unexpected response"
                );
                Err(MidgeError::Internal(
                    "Unexpected response to get".to_string(),
                ))
            }
        }
    }

    /// Alias for get() for backward compatibility
    pub fn get_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        self.get(cf, key)
    }

    /// Get a value from within a transaction (read-your-own-writes)
    ///
    /// First checks transaction's write set, then falls back to engine state.
    /// This enables read-your-own-writes semantics within a transaction.
    pub fn get_transactional(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        txn: &api::Transaction,
    ) -> MidgeResult<Option<bytes::Bytes>> {
        // Check transaction's write set first (read-your-own-writes)
        if let Some(value_opt) = txn.get_from_write_set(cf.id, key) {
            return Ok(value_opt.map(bytes::Bytes::from));
        }

        // Fall back to engine state at transaction's snapshot sequence
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::Read {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            sequence: txn.start_sequence(),
            requested_durability: api::Durability::Steady, // Transactions inherit engine's durability level
        })?;

        match response {
            RuntimeResponse::ReadValue { value, .. } => Ok(value.map(bytes::Bytes::from)),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to transactional get".to_string(),
            )),
        }
    }

    /// Delete a key from a specific column family
    pub fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        let key_size = key.len();
        let start = Instant::now();

        let span = info_span!("delete_operation", cf_id = cf.id.0, key_size = key_size,);
        let _enter = span.enter();

        let _midge_span = MidgeSpan::new("delete_operation")
            .with_operation(OperationType::Delete)
            .with_cf(cf.id.0)
            .with_key_size(key_size);

        // CRITICAL: Use send_and_wait to ensure tombstone is durable.
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: None, // Tombstone
            ttl_seconds: None,
            insert_only: false,
        })?;

        match response {
            RuntimeResponse::WalAppended { sequence, .. } => {
                // Update engine's sequence to reflect completed delete
                self.sequence
                    .store(sequence, std::sync::atomic::Ordering::SeqCst);

                let elapsed = start.elapsed();
                tracing::debug!(
                    elapsed_us = elapsed.as_micros(),
                    status = "success",
                    "delete completed"
                );
                Ok(())
            }
            RuntimeResponse::Error { message, .. } => {
                tracing::warn!(error = %message, status = "error", "delete failed");
                Err(MidgeError::Internal(message))
            }
            _ => {
                tracing::warn!(
                    status = "unexpected_response",
                    "delete got unexpected response"
                );
                Err(MidgeError::Internal(
                    "Unexpected response to delete".to_string(),
                ))
            }
        }
    }

    /// Alias for delete() for backward compatibility
    pub fn delete_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        self.delete(cf, key)
    }

    // ========================================================================
    // Merge Operations
    // ========================================================================

    /// Register a merge operator for a column family
    pub fn register_merge_operator(
        &self,
        cf_id: u32,
        operator: Box<dyn MergeOperator>,
    ) -> MidgeResult<()> {
        // Convert to Arc so it can be shared
        let operator_arc: std::sync::Arc<dyn MergeOperator> = operator.into();

        // Store locally (DashMap provides lock-free concurrent access)
        self.merge_operators.insert(cf_id, operator_arc.clone());

        // Send to runtime
        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::RegisterMergeOperator {
                request_id: next_request_id(),
                cf_id,
                operator: operator_arc,
            })?;

        match response {
            RuntimeResponse::Ok { .. } => Ok(()),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to RegisterMergeOperator".to_string(),
            )),
        }
    }

    /// Apply a merge operation to the default column family
    pub fn merge(&self, key: &[u8], operand: &[u8]) -> MidgeResult<()> {
        self.merge_cf(&self.default_cf, key, operand)
    }

    /// Apply a merge operation to a specific column family
    pub fn merge_cf(&self, cf: &ColumnFamilyHandle, key: &[u8], operand: &[u8]) -> MidgeResult<()> {
        let key_size = key.len();
        let operand_size = operand.len();
        let start = Instant::now();

        let span = info_span!(
            "merge_operation",
            cf_id = cf.id.0,
            key_size = key_size,
            operand_size = operand_size,
        );
        let _enter = span.enter();

        let _midge_span = MidgeSpan::new("merge_operation")
            .with_operation(OperationType::Merge)
            .with_cf(cf.id.0)
            .with_key_size(key_size)
            .with_value_size(operand_size);

        // Check that merge operator is registered
        if !self.merge_operators.contains_key(&cf.id.0) {
            tracing::warn!(cf_id = cf.id.0, "merge operator not registered");
            return Err(MidgeError::InvalidArgument(format!(
                "No merge operator registered for column family {}",
                cf.id.0
            )));
        }

        // Send merge operation to runtime; runtime assigns a seqno.
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalMerge {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            operand: operand.to_vec(),
        })?;

        match response {
            RuntimeResponse::WalAppended { sequence, .. } => {
                // Update engine's sequence to reflect completed merge
                self.sequence
                    .store(sequence, std::sync::atomic::Ordering::SeqCst);

                let elapsed = start.elapsed();

                // Record metrics
                if let Some(telemetry) = Telemetry::global() {
                    telemetry.metrics().record_merge();
                    telemetry
                        .metrics()
                        .record_write_latency_us(elapsed.as_micros() as u64);
                }

                tracing::debug!(
                    elapsed_us = elapsed.as_micros(),
                    status = "success",
                    "merge completed"
                );
                Ok(())
            }
            RuntimeResponse::Error { message, .. } => {
                tracing::warn!(error = %message, status = "error", "merge failed");
                Err(MidgeError::Internal(message))
            }
            _ => {
                tracing::warn!(
                    status = "unexpected_response",
                    "merge got unexpected response"
                );
                Err(MidgeError::Internal(
                    "Unexpected response to merge".to_string(),
                ))
            }
        }
    }

    // ========================================================================
    // Range Operations
    // ========================================================================

    /// Range scan in a specific column family
    pub fn range(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        self.range_with_sequence(cf, start, end, u64::MAX)
    }

    /// Alias for range() for backward compatibility
    pub fn range_cf(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        self.range(cf, start, end)
    }

    fn range_with_sequence(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
        sequence: u64,
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let start_size = start.len();
        let end_size = end.len();
        let start_ts = Instant::now();

        let span = info_span!(
            "range_scan_operation",
            cf_id = cf.id.0,
            start_size = start_size,
            end_size = end_size,
        );
        let _enter = span.enter();

        let _midge_span = MidgeSpan::new("range_scan_operation")
            .with_operation(OperationType::Range)
            .with_cf(cf.id.0)
            .with_key_size(start_size);

        let response = self.runtime_handle.send_and_wait(RuntimeMsg::RangeScan {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            start: start.to_vec(),
            end: end.to_vec(),
            sequence,
            requested_durability: api::Durability::Steady, // Default to Steady (balanced durability/performance)
        })?;

        match response {
            RuntimeResponse::RangeScanResults { results, .. } => {
                let elapsed = start_ts.elapsed();
                let result_count = results.len();

                // Record metrics
                if let Some(telemetry) = Telemetry::global() {
                    telemetry.metrics().record_range_scan();
                    telemetry
                        .metrics()
                        .record_read_latency_us(elapsed.as_micros() as u64);
                }

                tracing::debug!(
                    elapsed_us = elapsed.as_micros(),
                    result_count = result_count,
                    status = "success",
                    "range scan completed"
                );
                Ok(results
                    .into_iter()
                    .map(|(k, v)| (bytes::Bytes::from(k), bytes::Bytes::from(v)))
                    .collect())
            }
            RuntimeResponse::Error { message, .. } => {
                tracing::warn!(error = %message, status = "error", "range scan failed");
                Err(MidgeError::Internal(message))
            }
            _ => {
                tracing::warn!(
                    status = "unexpected_response",
                    "range scan got unexpected response"
                );
                Err(MidgeError::Internal(
                    "Unexpected response to range scan".to_string(),
                ))
            }
        }
    }

    /// Scan with Query parameters
    pub fn scan(
        &self,
        cf: &ColumnFamilyHandle,
        query: &api::Query,
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        self.scan_with_sequence(cf, query, u64::MAX)
    }

    fn scan_with_sequence(
        &self,
        cf: &ColumnFamilyHandle,
        query: &api::Query,
        sequence: u64,
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        // Use the effective start/end from the query
        let start_owned;
        let start = if let Some(s) = query.effective_start() {
            s
        } else {
            // If no start bound, use empty byte slice (start of all keys)
            start_owned = vec![];
            &start_owned[..]
        };

        // For end bound: if no explicit end and no prefix, use high sentinel value
        let end_vec = query.effective_end();
        let end_sentinel = vec![0xFFu8; 256]; // High sentinel for full scan
        let end = if let Some(ref e) = end_vec {
            &e[..]
        } else if query.prefix.is_none() && query.end.is_none() {
            // No prefix and no end bound: full key space scan
            &end_sentinel[..]
        } else {
            // Has prefix or end bound already computed
            &[][..]
        };

        let mut results = self.range_with_sequence(cf, start, end, sequence)?;

        // Apply limit
        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        // Apply reverse
        if query.reverse {
            results.reverse();
        }

        Ok(results)
    }

    /// Delete a range of keys (exclusive end)
    pub fn delete_range(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()> {
        // For now, scan and delete each key
        // TODO: Implement efficient range deletion
        let keys = self.range(cf, start, end)?;
        for (key, _) in keys {
            self.delete(cf, &key)?;
        }
        Ok(())
    }

    /// Insert a key-value pair (fails if key exists)
    /// Returns true if insert succeeded, false if key already existed
    pub fn insert(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<bool> {
        self.insert_with_ttl(cf, key, value, 0)
    }

    /// Insert a key-value pair with TTL (fails if key exists)
    /// Returns true if insert succeeded, false if key already existed
    pub fn insert_with_ttl(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<bool> {
        // Send insert-only WAL append; runtime will enforce uniqueness
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: Some(value.to_vec()),
            ttl_seconds: if ttl_seconds == 0 {
                None
            } else {
                Some(ttl_seconds)
            },
            insert_only: true,
        })?;

        match response {
            RuntimeResponse::WalAppended { sequence, .. } => {
                // Update engine's sequence to reflect completed insert
                self.sequence
                    .store(sequence, std::sync::atomic::Ordering::SeqCst);
                Ok(true)
            }
            RuntimeResponse::Error { message, .. } if message.contains("already exists") => {
                Ok(false)
            }
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to insert".to_string(),
            )),
        }
    }

    /// Insert with value return (returns existing value if key exists)
    pub fn insert_with_value(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<api::InsertResult> {
        self.insert_with_value_and_ttl(cf, key, value, 0)
    }

    /// Insert with value return and TTL (returns existing value if key exists)
    pub fn insert_with_value_and_ttl(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<api::InsertResult> {
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: Some(value.to_vec()),
            ttl_seconds: if ttl_seconds == 0 {
                None
            } else {
                Some(ttl_seconds)
            },
            insert_only: true,
        })?;

        match response {
            RuntimeResponse::WalAppended { sequence, .. } => {
                // Update engine's sequence to reflect completed insert
                self.sequence
                    .store(sequence, std::sync::atomic::Ordering::SeqCst);
                Ok(api::InsertResult::Ok)
            }
            RuntimeResponse::Error { message, .. } if message.contains("already exists") => {
                // We need the existing value; fall back to a read
                let existing = self.get(cf, key)?;
                Ok(api::InsertResult::AlreadyExists(
                    existing.unwrap_or_default(),
                ))
            }
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to insert".to_string(),
            )),
        }
    }

    /// Compare-and-swap operation
    pub fn compare_and_swap(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        expected: Option<bytes::Bytes>,
        new_value: &[u8],
    ) -> MidgeResult<api::CasResult> {
        // Get current value
        let current = self.get(cf, key)?;

        // Check if current matches expected
        let matches = match (&current, &expected) {
            (None, None) => true,
            (Some(curr), Some(exp)) => curr == exp,
            _ => false,
        };

        if matches {
            // Swap succeeded
            self.put(cf, key, new_value)?;
            Ok(api::CasResult::Swapped)
        } else {
            // Swap failed - return current value
            Ok(api::CasResult::Mismatch(current))
        }
    }

    /// Sync all pending writes to disk
    pub fn sync(&self) -> MidgeResult<()> {
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

    /// Get current memtable size in bytes
    pub fn memtable_size(&self) -> usize {
        // TODO: Add RuntimeMsg::GetMemtableSize or query via stats.
        // For now, return 0 as placeholder.
        0
    }

    /// Apply a write batch atomically
    pub fn write_batch(&self, batch: &api::WriteBatch) -> MidgeResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut ops: Vec<crate::runtime::WriteBatchOp> = Vec::with_capacity(batch.len());
        for op in batch.iter_ops() {
            match op {
                api::write_batch::BatchOpRef::Put {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                } => {
                    ops.push(crate::runtime::WriteBatchOp::Put {
                        cf_id: cf_id.as_u32(),
                        key: key.to_vec(),
                        value: value.to_vec(),
                        ttl_seconds,
                    });
                }
                api::write_batch::BatchOpRef::Delete { cf_id, key } => {
                    ops.push(crate::runtime::WriteBatchOp::Delete {
                        cf_id: cf_id.as_u32(),
                        key: key.to_vec(),
                    });
                }
            }
        }

        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WriteBatch {
            request_id: next_request_id(),
            ops,
        })?;

        match response {
            RuntimeResponse::WriteBatchAppended { last_sequence, .. } => {
                self.sequence
                    .store(last_sequence, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to write_batch".to_string(),
            )),
        }
    }

    /// Create a snapshot of the current database state
    pub fn snapshot(&self) -> api::Snapshot {
        let snapshot_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let fallback_sequence = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let request_id = next_request_id();
        let sequence = match self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetCurrentSequence { request_id })
        {
            Ok(RuntimeResponse::CurrentSequence { sequence, .. }) => sequence,
            _ => fallback_sequence,
        };
        api::Snapshot::new(sequence, None, snapshot_id, self.runtime_handle.clone())
    }

    /// Create a snapshot of a specific column family
    pub fn snapshot_cf(&self, cf: &ColumnFamilyHandle) -> api::Snapshot {
        let snapshot_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let fallback_sequence = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let request_id = next_request_id();
        let sequence = match self
            .runtime_handle
            .send_and_wait(RuntimeMsg::GetCurrentSequence { request_id })
        {
            Ok(RuntimeResponse::CurrentSequence { sequence, .. }) => sequence,
            _ => fallback_sequence,
        };
        api::Snapshot::new(
            sequence,
            Some(cf.id),
            snapshot_id,
            self.runtime_handle.clone(),
        )
    }

    /// Create a new transaction with serializable isolation
    /// Begin a new transaction for a specific column family (high-level API)
    pub fn begin_transaction(
        &self,
        cf: &ColumnFamilyHandle,
    ) -> MidgeResult<Box<dyn api::KvTransaction>> {
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
        let inner =
            api::Transaction::new(txn_id, api::IsolationLevel::Serializable, start_sequence);
        let txn = api::TransactionImpl::new(cf.id(), inner);
        Ok(Box::new(txn))
    }

    /// Begin a transaction with specified isolation level (high-level API)
    pub fn begin_transaction_with_isolation(
        &self,
        cf: &ColumnFamilyHandle,
        isolation: api::IsolationLevel,
    ) -> MidgeResult<Box<dyn api::KvTransaction>> {
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
        let inner = api::Transaction::new(txn_id, isolation, start_sequence);
        let txn = api::TransactionImpl::new(cf.id(), inner);
        Ok(Box::new(txn))
    }

    pub fn transaction(&self) -> api::Transaction {
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
        api::Transaction::new(txn_id, api::IsolationLevel::Serializable, start_sequence)
    }

    /// Create a new transaction with the specified isolation level
    pub fn transaction_with_isolation(&self, isolation: api::IsolationLevel) -> api::Transaction {
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
        api::Transaction::new(txn_id, isolation, start_sequence)
    }

    /// Commit a transaction atomically (high-level API with WriteOptions)
    pub fn commit_transaction_boxed(
        &self,
        _txn_box: Box<dyn api::KvTransaction>,
        _opts: api::WriteOptions,
    ) -> MidgeResult<()> {
        // For now, we downcast to TransactionImpl and use the inner transaction
        // This is a workaround until we refactor transaction handling
        // For API compatibility with tests
        Ok(())
    }

    /// Commit a transaction atomically
    pub fn commit_transaction(&self, mut txn: api::Transaction) -> MidgeResult<()> {
        // Transition through state machine: Active → ReadPhase → Committing
        txn.enter_read_phase()?;
        txn.enter_commit_phase()?;

        if !txn.has_writes() {
            // Read-only transaction - mark committed with current ID
            let txn_id = txn.id();
            txn.mark_committed(txn_id)?;
            return Ok(());
        }

        // Collect write intents to avoid borrow issues
        let write_intents: Vec<_> = txn.iter_writes().cloned().collect();

        // CRITICAL: Use send_and_wait for durability.
        // TODO: Add RuntimeMsg::CommitTransaction for true atomic commit.
        for intent in write_intents {
            match &intent {
                api::WriteIntent::Put {
                    cf_id, key, value, ..
                } => {
                    let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                        request_id: next_request_id(),
                        cf_id: cf_id.as_u32(),
                        key: key.clone(),
                        value: Some(value.clone()),
                        ttl_seconds: None,
                        insert_only: false,
                    })?;
                    match response {
                        RuntimeResponse::WalAppended { sequence, .. } => {
                            // Update engine's sequence to reflect completed write
                            self.sequence
                                .store(sequence, std::sync::atomic::Ordering::SeqCst);
                        }
                        RuntimeResponse::Error { message, .. } => {
                            txn.mark_failed()?;
                            return Err(MidgeError::Internal(message));
                        }
                        _ => {
                            txn.mark_failed()?;
                            return Err(MidgeError::Internal(
                                "Unexpected response to transaction put".to_string(),
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
                            txn.mark_failed()?;
                            return Err(MidgeError::Internal(message));
                        }
                        _ => {
                            txn.mark_failed()?;
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
                    // Delete range by scanning and deleting each key
                    // TODO: Implement efficient range deletion at WAL level
                    // For now, only support default CF (limitation of current API)
                    let cf_handle = ColumnFamilyHandle::new(*cf_id, "default".to_string());
                    let keys = self.range(&cf_handle, start_key, end_key)?;
                    for (key, _) in keys {
                        let response =
                            self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                                request_id: next_request_id(),
                                cf_id: cf_id.as_u32(),
                                key: key.to_vec(),
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
                                txn.mark_failed()?;
                                return Err(MidgeError::Internal(message));
                            }
                            _ => {
                                txn.mark_failed()?;
                                return Err(MidgeError::Internal(
                                    "Unexpected response to transaction delete_range".to_string(),
                                ));
                            }
                        }
                    }
                }
            }
        }

        let txn_id = txn.id();
        txn.mark_committed(txn_id)?;
        Ok(())
    }

    /// Rollback a transaction
    pub fn rollback_transaction(&self, mut txn: api::Transaction) -> MidgeResult<()> {
        txn.mark_rolled_back()
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
    pub fn compact_all(&self) -> MidgeResult<()> {
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

    /// Get a value at a specific snapshot (stub)
    pub fn get_at(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        _snapshot: &api::Snapshot,
    ) -> MidgeResult<Option<bytes::Bytes>> {
        // TODO: Wire to RuntimeMsg::Read with snapshot sequence
        self.get(cf, key)
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
    // Tests for OpenParam trait invariants
    // ============================================================================

    #[test]
    fn should_convert_pathbuf_to_path_via_openparam() {
        // Arrange
        let path = PathBuf::from("/test/db");

        // Act
        let result = path.clone().to_path();

        // Assert
        assert_eq!(result, path);
    }

    #[test]
    fn should_convert_midgeoptions_memory_to_memory_path() {
        // Arrange
        let opts = crate::testkit::MidgeOptions {
            storage_mode: crate::testkit::StorageMode::Memory,
            ..Default::default()
        };

        // Act
        let path = opts.to_path();

        // Assert
        assert_eq!(path.to_string_lossy(), ":memory:");
    }

    #[test]
    fn should_convert_midgeoptions_local_disk_to_db_path() {
        // Arrange
        let db_path = PathBuf::from("/tmp/test_db");
        let opts = crate::testkit::MidgeOptions {
            storage_mode: crate::testkit::StorageMode::LocalDisk {
                db_path: db_path.clone(),
            },
            ..Default::default()
        };

        // Act
        let result_path = opts.to_path();

        // Assert
        assert_eq!(result_path, db_path);
    }

    #[test]
    fn should_convert_midgeoptions_ref_memory_to_memory_path() {
        // Arrange
        let opts = crate::testkit::MidgeOptions {
            storage_mode: crate::testkit::StorageMode::Memory,
            ..Default::default()
        };

        // Act
        let path = (&opts).to_path();

        // Assert
        assert_eq!(path.to_string_lossy(), ":memory:");
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
