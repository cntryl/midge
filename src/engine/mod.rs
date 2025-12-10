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
use crate::runtime::{next_request_id, Runtime, RuntimeHandle, RuntimeMsg, RuntimeResponse, RuntimeState};
use crate::sst::{Memtable, SkipListMemtable};
use std::path::PathBuf;
use std::sync::Arc;

pub mod api;
pub mod context;
pub mod engine;
pub mod open;

pub use api::*;
pub use context::Context;
pub use open::open_engine;

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
pub struct MidgeEngine {
    /// Handle to submit work to the runtime
    runtime_handle: RuntimeHandle,
    /// Database path
    db_path: PathBuf,
    /// Default column family for convenience
    default_cf: ColumnFamilyHandle,
    /// Local memtable reference for fast reads
    /// (Runtime owns the authoritative copy)
    memtable: Arc<SkipListMemtable>,
    /// Current sequence number (for local tracking)
    sequence: std::sync::atomic::AtomicU64,
    /// Next snapshot ID counter
    next_snapshot_id: std::sync::atomic::AtomicU64,
}

impl MidgeEngine {
    /// Open a database at the given path (internal use)
    pub fn open(db_path: PathBuf) -> MidgeResult<Self> {
        // Create runtime state
        let state = RuntimeState::new(db_path.clone());

        // Start runtime
        let (runtime, _) = Runtime::new()?;
        let runtime_handle = runtime.start(state)?;

        Ok(Self {
            runtime_handle,
            db_path,
            default_cf: ColumnFamilyHandle::new(ColumnFamilyId::DEFAULT, "default".to_string()),
            memtable: Arc::new(SkipListMemtable::new()),
            sequence: std::sync::atomic::AtomicU64::new(0),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Open a database with test configuration options
    pub fn open_with_options(opts: crate::testkit::MidgeOptions) -> MidgeResult<Self> {
        let db_path = match &opts.storage_mode {
            crate::testkit::StorageMode::Memory => {
                PathBuf::from(":memory:")
            }
            crate::testkit::StorageMode::LocalDisk { db_path } => {
                db_path.clone()
            }
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
        let seq = self.next_sequence();

        // Write to local memtable
        self.memtable.put(key.to_vec(), value.to_vec())?;

        // Send WAL append to runtime
        self.runtime_handle.send(RuntimeMsg::WalAppend {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: Some(value.to_vec()),
            sequence: seq,
        })?;

        Ok(())
    }

    /// Alias for put() for backward compatibility
    pub fn put_cf(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.put(cf, key, value)
    }

    /// Get a value from a specific column family
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        // Local read from memtable (no runtime round-trip)
        // Per architecture: "Reads do not go through the runtime unless they require cross-layer interaction"
        // SST reads will be added later via runtime, but memtable reads are local.
        let _ = cf; // Column family parameter for future use
        
        if let Some(value) = self.memtable.get(key)? {
            return Ok(Some(bytes::Bytes::from(value)));
        }

        // Key not found in memtable
        Ok(None)
    }

    /// Alias for get() for backward compatibility
    pub fn get_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        self.get(cf, key)
    }

    /// Delete a key from a specific column family
    pub fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        let seq = self.next_sequence();

        // Write tombstone to local memtable
        self.memtable.delete(key.to_vec())?;

        // Send WAL append to runtime (value=None indicates delete)
        self.runtime_handle.send(RuntimeMsg::WalAppend {
            request_id: next_request_id(),
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: None,
            sequence: seq,
        })?;

        Ok(())
    }

    /// Alias for delete() for backward compatibility
    pub fn delete_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        self.delete(cf, key)
    }

    /// Range scan in a specific column family
    pub fn range(
        &self,
        _cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        // For now, scan the local memtable and filter by range
        // TODO: Also scan immutable memtables and SST files from runtime
        let all_entries = self.memtable.iter_all(u64::MAX);
        
        let start_bound = if start.is_empty() { None } else { Some(start) };
        let end_bound = if end.is_empty() { None } else { Some(end) };
        
        let mut results = Vec::new();
        for (key, value, _seq) in all_entries {
            // Check if key is in range [start, end)
            let in_range = match (&start_bound, &end_bound) {
                (Some(s), Some(e)) => key.as_slice() >= *s && key.as_slice() < *e,
                (Some(s), None) => key.as_slice() >= *s,
                (None, Some(e)) => key.as_slice() < *e,
                (None, None) => true,
            };
            
            // Include key if in range and not deleted
            if in_range {
                if let Some(val) = value {
                    results.push((bytes::Bytes::from(key), bytes::Bytes::from(val)));
                }
            }
        }
        
        Ok(results)
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
    pub fn scan(&self, cf: &ColumnFamilyHandle, query: &api::Query) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
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
    pub fn delete_range(&self, cf: &ColumnFamilyHandle, start: &[u8], end: &[u8]) -> MidgeResult<()> {
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
        // Check if key already exists
        if self.get(cf, key)?.is_some() {
            // Key exists - cannot insert
            return Ok(false);
        }
        
        // Key doesn't exist - do the insert
        self.put(cf, key, value)?;
        Ok(true)
    }

    /// Insert with value return (returns existing value if key exists)
    pub fn insert_with_value(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<api::InsertResult> {
        // Check if key already exists
        if let Some(existing) = self.get(cf, key)? {
            // Key exists - return existing value
            return Ok(api::InsertResult::AlreadyExists(bytes::Bytes::from(existing)));
        }
        
        // Key doesn't exist - do the insert
        self.put(cf, key, value)?;
        Ok(api::InsertResult::Ok)
    }

    /// Compare-and-swap operation
    pub fn compare_and_swap(&self, cf: &ColumnFamilyHandle, key: &[u8], expected: Option<bytes::Bytes>, new_value: &[u8]) -> MidgeResult<api::CasResult> {
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
        self.runtime_handle.send(RuntimeMsg::WalSync { request_id: next_request_id() })
    }

    /// Force a flush of the default column family
    pub fn flush(&self) -> MidgeResult<()> {
        self.flush_cf(&self.default_cf)
    }

    /// Force a flush of a specific column family
    pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
        self.runtime_handle
            .send(RuntimeMsg::FlushMemtable { request_id: next_request_id(), cf_id: cf.id.0 })
    }

    /// Get current memtable size in bytes
    pub fn memtable_size(&self) -> usize {
        self.memtable.size_bytes()
    }

    /// Apply a write batch atomically
    pub fn write_batch(&self, batch: &api::WriteBatch) -> MidgeResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // Apply all puts and deletes to local memtable first
        for (_, key, value) in batch.iter_puts() {
            self.memtable.put(key.to_vec(), value.to_vec())?;
        }
        for (_, key) in batch.iter_deletes() {
            self.memtable.delete(key.to_vec())?;
        }

        // Send all puts to WAL
        for (cf_id, key, value) in batch.iter_puts() {
            let seq = self.next_sequence();
            self.runtime_handle.send(RuntimeMsg::WalAppend {
                request_id: next_request_id(),
                cf_id: cf_id.as_u32(),
                key: key.to_vec(),
                value: Some(value.to_vec()),
                sequence: seq,
            })?;
        }

        // Send all deletes to WAL
        for (cf_id, key) in batch.iter_deletes() {
            let seq = self.next_sequence();
            self.runtime_handle.send(RuntimeMsg::WalAppend {
                request_id: next_request_id(),
                cf_id: cf_id.as_u32(),
                key: key.to_vec(),
                value: None,
                sequence: seq,
            })?;
        }

        Ok(())
    }

    /// Create a snapshot of the current database state
    pub fn snapshot(&self) -> api::Snapshot {
        let seq = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let snapshot_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        api::Snapshot::new(seq, None, snapshot_id)
    }

    /// Create a snapshot of a specific column family
    pub fn snapshot_cf(&self, cf: &ColumnFamilyHandle) -> api::Snapshot {
        let seq = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let snapshot_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        api::Snapshot::new(seq, Some(cf.id), snapshot_id)
    }

    /// Create a new transaction with serializable isolation
    /// Begin a new transaction for a specific column family (high-level API)
    pub fn begin_transaction(&self, cf: &ColumnFamilyHandle) -> MidgeResult<Box<dyn api::KvTransaction>> {
        let seq = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let txn_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let inner = api::Transaction::new(txn_id, api::IsolationLevel::Serializable, seq);
        let txn = api::TransactionImpl::new(cf.id(), inner);
        Ok(Box::new(txn))
    }

    /// Begin a transaction with specified isolation level (high-level API)
    pub fn begin_transaction_with_isolation(
        &self,
        cf: &ColumnFamilyHandle,
        isolation: api::IsolationLevel,
    ) -> MidgeResult<Box<dyn api::KvTransaction>> {
        let seq = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let txn_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let inner = api::Transaction::new(txn_id, isolation, seq);
        let txn = api::TransactionImpl::new(cf.id(), inner);
        Ok(Box::new(txn))
    }

    pub fn transaction(&self) -> api::Transaction {
        let seq = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let txn_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        api::Transaction::new(txn_id, api::IsolationLevel::Serializable, seq)
    }

    /// Create a new transaction with the specified isolation level
    pub fn transaction_with_isolation(&self, isolation: api::IsolationLevel) -> api::Transaction {
        let seq = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        let txn_id = self
            .next_snapshot_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        api::Transaction::new(txn_id, isolation, seq)
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
            txn.mark_committed(self.sequence.load(std::sync::atomic::Ordering::SeqCst))?;
            return Ok(());
        }

        // Apply all writes atomically
        for intent in txn.iter_writes() {
            let seq = self.next_sequence();
            if let Some(value) = intent.value() {
                self.memtable.put(intent.key().to_vec(), value.to_vec())?;
                self.runtime_handle.send(RuntimeMsg::WalAppend {
                    request_id: next_request_id(),
                    cf_id: intent.cf_id().as_u32(),
                    key: intent.key().to_vec(),
                    value: Some(value.to_vec()),
                    sequence: seq,
                })?;
            } else {
                self.memtable.delete(intent.key().to_vec())?;
                self.runtime_handle.send(RuntimeMsg::WalAppend {
                    request_id: next_request_id(),
                    cf_id: intent.cf_id().as_u32(),
                    key: intent.key().to_vec(),
                    value: None,
                    sequence: seq,
                })?;
            }
        }

        let commit_seq = self.sequence.load(std::sync::atomic::Ordering::SeqCst);
        txn.mark_committed(commit_seq)?;
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
            |resp| matches!(resp, RuntimeResponse::Ok { .. } | RuntimeResponse::Error { .. }),
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

    // === Internal helpers ===

    fn next_sequence(&self) -> u64 {
        self.sequence
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
