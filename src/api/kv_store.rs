//! Simple key-value store trait for external integrations.

use crate::error::MidgeResult;
use bytes::Bytes;
use std::sync::Arc;
use super::write_options::WriteOptions;

/// Simple key-value store interface for embedded database operations.
///
/// This trait provides a clean, minimal API for interacting with the database,
/// including single operations, batches, scans, and transactions.
pub trait KvStore: Send + Sync {
    /// Insert a key-value pair. Fails if the key already exists.
    fn insert(&self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Write a key-value pair to the store. Overwrites if key exists.
    fn put(&self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Read a value by key. Returns `None` if the key doesn't exist.
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>>;

    /// Scan a range of keys. Returns key-value pairs where `start <= key < end`.
    fn scan(&self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>>;

    /// Delete a key from the store.
    fn delete(&self, key: &[u8]) -> MidgeResult<()>;

    /// Execute a batch of mixed operations (put, insert, delete) atomically.
    fn batch(&self, operations: Vec<BatchOperation>) -> MidgeResult<()>;

    /// Delete a range of keys where `start <= key < end`.
    fn delete_range(&self, start: &[u8], end: &[u8]) -> MidgeResult<()>;

    /// Begin a new transaction with snapshot isolation.
    ///
    /// The returned transaction object stages operations in memory.
    /// Call `commit_transaction` to apply changes atomically.
    fn begin_transaction(&self) -> MidgeResult<Box<dyn KvTransaction>>;

    /// Commit a transaction atomically.
    ///
    /// # Arguments
    /// * `txn` - The transaction to commit (obtained from `begin_transaction`)
    /// * `opts` - Write options controlling durability (sync/no-sync)
    ///
    /// # Errors
    /// Returns an error if a conflict is detected or the transaction has expired.
    fn commit_transaction(
        &self,
        txn: Box<dyn KvTransaction>,
        opts: WriteOptions,
    ) -> MidgeResult<()>;

    /// Rollback a transaction, discarding all staged operations.
    fn rollback_transaction(&self, txn: Box<dyn KvTransaction>) -> MidgeResult<()>;
}

/// Batch operation for atomic execution of mixed operations.
#[derive(Debug, Clone)]
pub enum BatchOperation {
    /// Insert operation (fails if key exists)
    Insert { key: Vec<u8>, value: Vec<u8> },
    /// Put operation (overwrites if key exists)
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Delete operation
    Delete { key: Vec<u8> },
}

/// Transaction interface for staging database operations.
///
/// Transactions provide snapshot isolation and ACID guarantees. Operations
/// are staged in memory and applied atomically when committed via
/// `KvStore::commit_transaction`.
///
/// Reads within a transaction see a consistent snapshot of the database at
/// the time the transaction began, plus any uncommitted writes from the
/// transaction itself.
///
/// # Internal Implementation Note
/// This trait requires `Any` to support internal downcasting by the engine.
pub trait KvTransaction: Send + Sync + std::any::Any {
    /// Stage a put operation. Overwrites if key exists.
    fn put(&mut self, key: Bytes, value: Bytes) -> MidgeResult<()>;

    /// Read a value within the transaction's snapshot.
    ///
    /// Returns the value as it existed when the transaction began, or any
    /// uncommitted write from this transaction. Returns `None` if the key
    /// doesn't exist or has been deleted.
    fn get(&mut self, key: &[u8]) -> MidgeResult<Option<Bytes>>;

    /// Stage a delete operation for a single key.
    fn delete(&mut self, key: Bytes) -> MidgeResult<()>;

    /// Scan a range of keys within the transaction's snapshot.
    ///
    /// Returns key-value pairs where `start <= key < end`, as they existed
    /// when the transaction began, plus any uncommitted writes from this transaction.
    fn scan(&mut self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>>;

    /// Stage a delete-range operation. Deletes all keys where `start <= key < end`.
    fn delete_range(&mut self, start: Bytes, end: Bytes) -> MidgeResult<()>;
}

pub type DynKvStore = Arc<dyn KvStore>;
