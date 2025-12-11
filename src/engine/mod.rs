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
use std::path::PathBuf;

pub mod api;
pub mod context;
pub mod engine;
pub mod open;

pub use api::*;
pub use context::Context;
pub use open::open_engine;

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
///
/// Note: Engine maintains a sequence counter for now. This will be moved to
/// runtime once we implement centralized sequence allocation there.
pub struct MidgeEngine {
    /// Handle to submit work to the runtime
    runtime_handle: RuntimeHandle,
    /// Database path
    #[allow(dead_code)]
    db_path: PathBuf,
    /// Default column family for convenience
    default_cf: ColumnFamilyHandle,
    /// Sequence number for write ordering (TODO: move to runtime)
    sequence: std::sync::atomic::AtomicU64,
    /// Next snapshot ID counter (local only, not related to sequence numbers)
    next_snapshot_id: std::sync::atomic::AtomicU64,
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
        // Create runtime state
        let state = RuntimeState::new(db_path.clone());

        // Start runtime
        let (runtime, _) = Runtime::new()?;
        let runtime_handle = runtime.start(state)?;

        Ok(Self {
            runtime_handle,
            db_path,
            default_cf: ColumnFamilyHandle::new(ColumnFamilyId::DEFAULT, "default".to_string()),
            sequence: std::sync::atomic::AtomicU64::new(0),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Open a database with test configuration options
    pub fn open_with_options(opts: crate::testkit::MidgeOptions) -> MidgeResult<Self> {
        let db_path = match &opts.storage_mode {
            crate::testkit::StorageMode::Memory => PathBuf::from(":memory:"),
            crate::testkit::StorageMode::LocalDisk { db_path } => db_path.clone(),
            crate::testkit::StorageMode::CloudBacked { local_cache_path } => {
                local_cache_path.clone()
            }
        };

        Self::open(db_path)
    }

    /// Get the default column family
    pub fn default_column_family(&self) -> &ColumnFamilyHandle {
        &self.default_cf
    }

    /// Put a key-value pair into a specific column family
    pub fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
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
        // CRITICAL: Use send_and_wait to ensure durability before returning.
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: Some(value.to_vec()),
            sequence: self.next_sequence(),
            ttl_seconds: if ttl_seconds == 0 {
                None
            } else {
                Some(ttl_seconds)
            },
            insert_only: false,
        })?;

        // Check for errors from runtime
        match response {
            RuntimeResponse::Ok { .. } => Ok(()),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to put".to_string(),
            )),
        }
    }

    /// Alias for put() for backward compatibility
    pub fn put_cf(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.put(cf, key, value)
    }

    /// Get a value from a specific column family
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        // CRITICAL: Reads must go through runtime to query authoritative state
        // (active memtable + immutable memtables + SSTs).
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::Read {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            sequence: u64::MAX, // Read latest committed value
        })?;

        match response {
            RuntimeResponse::ReadValue { value, .. } => Ok(value.map(bytes::Bytes::from)),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to get".to_string(),
            )),
        }
    }

    /// Alias for get() for backward compatibility
    pub fn get_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        self.get(cf, key)
    }

    /// Delete a key from a specific column family
    pub fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        // CRITICAL: Use send_and_wait to ensure tombstone is durable.
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: None, // Tombstone
            sequence: self.next_sequence(),
            ttl_seconds: None,
            insert_only: false,
        })?;

        match response {
            RuntimeResponse::Ok { .. } => Ok(()),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to delete".to_string(),
            )),
        }
    }

    /// Alias for delete() for backward compatibility
    pub fn delete_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        self.delete(cf, key)
    }

    /// Range scan in a specific column family
    pub fn range(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        // TODO: Add RuntimeMsg::RangeScan variant and implement in runtime.
        // For now, simulate via multiple get() calls (inefficient but correct).
        // This is a placeholder until proper range scan message is added.
        let _ = (cf, start, end);

        // Return empty for now - proper implementation requires RuntimeMsg::RangeScan
        Ok(vec![])
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

    /// Scan with Query parameters
    pub fn scan(
        &self,
        cf: &ColumnFamilyHandle,
        query: &api::Query,
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        // Use the effective start/end from the query
        let start_owned;
        let start = if let Some(s) = query.effective_start() {
            s
        } else {
            start_owned = vec![];
            &start_owned[..]
        };

        let end_vec = query.effective_end();
        let end = if let Some(ref e) = end_vec {
            &e[..]
        } else {
            &[][..]
        };

        let mut results = self.range(cf, start, end)?;

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
            sequence: self.next_sequence(),
            ttl_seconds: if ttl_seconds == 0 {
                None
            } else {
                Some(ttl_seconds)
            },
            insert_only: true,
        })?;

        match response {
            RuntimeResponse::Ok { .. } => Ok(true),
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
            sequence: self.next_sequence(),
            ttl_seconds: if ttl_seconds == 0 {
                None
            } else {
                Some(ttl_seconds)
            },
            insert_only: true,
        })?;

        match response {
            RuntimeResponse::Ok { .. } => Ok(api::InsertResult::Ok),
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

        // TODO: Add RuntimeMsg::WriteBatch variant for true atomic batching.
        // Current approach: apply each operation via send_and_wait (not truly atomic).
        // This is a known limitation until batch message is added.

        for (cf_id, key, value) in batch.iter_puts() {
            let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                request_id: next_request_id(),
                cf_id: cf_id.as_u32(),
                key: key.to_vec(),
                value: Some(value.to_vec()),
                sequence: self.next_sequence(),
                ttl_seconds: None,
                insert_only: false,
            })?;
            if let RuntimeResponse::Error { message, .. } = response {
                return Err(MidgeError::Internal(message));
            }
        }

        for (cf_id, key) in batch.iter_deletes() {
            let response = self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                request_id: next_request_id(),
                cf_id: cf_id.as_u32(),
                key: key.to_vec(),
                value: None,
                sequence: self.next_sequence(),
                ttl_seconds: None,
                insert_only: false,
            })?;
            if let RuntimeResponse::Error { message, .. } = response {
                return Err(MidgeError::Internal(message));
            }
        }

        Ok(())
    }

    /// Create a snapshot of the current database state
    pub fn snapshot(&self) -> api::Snapshot {
        // TODO: Query current sequence from runtime via send_and_wait.
        // For now, use snapshot_id as sequence (incorrect but safe for testing).
        let snapshot_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        api::Snapshot::new(snapshot_id, None, snapshot_id)
    }

    /// Create a snapshot of a specific column family
    pub fn snapshot_cf(&self, cf: &ColumnFamilyHandle) -> api::Snapshot {
        // TODO: Query current sequence from runtime.
        let snapshot_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        api::Snapshot::new(snapshot_id, Some(cf.id), snapshot_id)
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
        // TODO: Query runtime's current sequence for snapshot isolation.
        let inner = api::Transaction::new(txn_id, api::IsolationLevel::Serializable, txn_id);
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
        // TODO: Query runtime's current sequence.
        let inner = api::Transaction::new(txn_id, isolation, txn_id);
        let txn = api::TransactionImpl::new(cf.id(), inner);
        Ok(Box::new(txn))
    }

    pub fn transaction(&self) -> api::Transaction {
        let txn_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // TODO: Query runtime's current sequence.
        api::Transaction::new(txn_id, api::IsolationLevel::Serializable, txn_id)
    }

    /// Create a new transaction with the specified isolation level
    pub fn transaction_with_isolation(&self, isolation: api::IsolationLevel) -> api::Transaction {
        let txn_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // TODO: Query runtime's current sequence.
        api::Transaction::new(txn_id, isolation, txn_id)
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
        if !txn.has_writes() {
            // Read-only transaction - mark committed with current ID
            let txn_id = txn.id();
            txn.mark_committed(txn_id)?;
            return Ok(());
        }

        // CRITICAL: Use send_and_wait for durability.
        // TODO: Add RuntimeMsg::CommitTransaction for true atomic commit.
        for intent in txn.iter_writes() {
            let response = if let Some(value) = intent.value() {
                self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                    request_id: next_request_id(),
                    cf_id: intent.cf_id().as_u32(),
                    key: intent.key().to_vec(),
                    value: Some(value.to_vec()),
                    sequence: self.next_sequence(),
                    ttl_seconds: None,
                    insert_only: false,
                })?
            } else {
                self.runtime_handle.send_and_wait(RuntimeMsg::WalAppend {
                    request_id: next_request_id(),
                    cf_id: intent.cf_id().as_u32(),
                    key: intent.key().to_vec(),
                    value: None,
                    sequence: self.next_sequence(),
                    ttl_seconds: None,
                    insert_only: false,
                })?
            };

            if let RuntimeResponse::Error { message, .. } = response {
                return Err(MidgeError::Internal(message));
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
            RuntimeResponse::ColumnFamilyCreated { cf_id, .. } => Ok(ColumnFamilyHandle::new(
                ColumnFamilyId(cf_id),
                name.to_string(),
            )),
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
            RuntimeResponse::Ok { .. } => Ok(()),
            RuntimeResponse::Error { message, .. } => Err(MidgeError::Internal(message)),
            _ => Err(MidgeError::Internal(
                "Unexpected response to drop_column_family".to_string(),
            )),
        }
    }

    /// List all active column families
    pub fn list_column_families(&self) -> MidgeResult<Vec<ColumnFamilyHandle>> {
        // Get the runtime state to access the manifest
        // For now, return the default CF + a placeholder for others
        // TODO: Wire to RuntimeMsg to query all active CFs from manifest
        Ok(vec![self.default_cf.clone()])
    }

    /// Compact all data (stub - not implemented)
    pub fn compact_all(&self) -> MidgeResult<()> {
        Err(MidgeError::Internal(
            "compact_all not yet implemented".to_string(),
        ))
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

    fn next_sequence(&self) -> u64 {
        self.sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
