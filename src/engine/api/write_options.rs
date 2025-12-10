//! Write options for transactions and operations

use crate::common::MidgeResult;

/// Options for write operations (transactions and batch writes)
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Whether to synchronously wait for the write to be durable
    pub sync: bool,
    /// Disable WAL for this write (unsafe, not recommended)
    pub disable_wal: bool,
}

impl WriteOptions {
    /// Create default write options
    pub fn new() -> Self {
        Self {
            sync: false,
            disable_wal: false,
        }
    }

    /// Enable synchronous durability
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }

    /// Disable WAL (dangerous - only for testing)
    pub fn disable_wal(mut self) -> Self {
        self.disable_wal = true;
        self
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// User-facing transaction trait
///
/// Provides ergonomic interface for transactional operations.
/// Transactions are scoped to a single column family for safety.
pub trait KvTransaction: Send {
    /// Read a key within the transaction
    fn get(&self, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>>;

    /// Write a key within the transaction
    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Delete a key within the transaction
    fn delete(&mut self, key: &[u8]) -> MidgeResult<()>;

    /// Get the transaction ID
    fn id(&self) -> u64;

    /// Check if transaction is active
    fn is_active(&self) -> bool;
}

/// Concrete transaction implementation wrapping api::Transaction
pub struct TransactionImpl {
    inner: crate::engine::api::Transaction,
    cf_id: crate::engine::ColumnFamilyId,
}

impl TransactionImpl {
    /// Create a new transaction for a specific column family
    pub fn new(cf_id: crate::engine::ColumnFamilyId, txn: crate::engine::api::Transaction) -> Self {
        Self { inner: txn, cf_id }
    }

    /// Get the inner transaction
    pub fn inner(&self) -> &crate::engine::api::Transaction {
        &self.inner
    }

    /// Consume and return the inner transaction
    pub fn into_inner(self) -> crate::engine::api::Transaction {
        self.inner
    }
}

impl KvTransaction for TransactionImpl {
    fn get(&self, _key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        // For now, transactions don't track reads (no MVCC implemented)
        // This is a stub for API compatibility
        Ok(None)
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.inner.put(self.cf_id, key.to_vec(), value.to_vec())
    }

    fn delete(&mut self, key: &[u8]) -> MidgeResult<()> {
        self.inner.delete(self.cf_id, key.to_vec())
    }

    fn id(&self) -> u64 {
        self.inner.id()
    }

    fn is_active(&self) -> bool {
        self.inner.state() == crate::engine::api::TransactionState::Active
    }
}
