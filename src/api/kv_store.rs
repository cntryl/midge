//! Simple key-value store trait for external integrations.
//!
//! This defines the external-facing API surface for all Midge-backed key-value stores.
//! It abstracts over multiple backends (in-memory, filesystem, cloud) while ensuring
//! a consistent, column-family–scoped data model and transactional guarantees.

use super::write_options::WriteOptions;
use crate::error::MidgeResult;
use bytes::Bytes;
use std::sync::Arc;

/// Represents a batch of operations executed atomically within a single column family.
///
/// All operations in the batch are applied in order and succeed or fail together.
#[derive(Debug, Clone)]
pub enum BatchOperation {
    /// Insert a key-value pair. Fails if the key already exists.
    Insert { key: Vec<u8>, value: Vec<u8> },

    /// Put a key-value pair. Overwrites the value if the key already exists.
    Put { key: Vec<u8>, value: Vec<u8> },

    /// Delete a single key.
    Delete { key: Vec<u8> },

    /// Delete all keys in the range `[start, end)`.
    DeleteRange { start: Vec<u8>, end: Vec<u8> },
}

/// A minimal, backend-agnostic key-value store interface.
///
/// All operations are scoped to a specific column family, making multi-tenancy
/// and data isolation explicit and first-class concepts.
///
/// # Design Philosophy
/// Column families are not an afterthought—they are the primary means of data organization.
/// Every operation takes a `ColumnFamilyHandle` to enforce explicit scoping and prevent
/// accidental cross-tenant or cross-domain data access.
pub trait KvStore: Send + Sync {
    // ==================== Column Family Management ====================

    /// Create a new column family with the given configuration.
    ///
    /// # Arguments
    /// * `name` — Unique column family name.
    /// * `config` — Configuration defining storage and compaction behavior.
    ///
    /// # Returns
    /// A handle to the newly created column family.
    ///
    /// # Errors
    /// Returns an error if a column family with the same name already exists.
    fn create_column_family(
        &self,
        name: &str,
        config: super::column_family::ColumnFamilyConfig,
    ) -> MidgeResult<super::column_family::ColumnFamilyHandle>;

    /// Retrieve a handle to an existing column family by name.
    ///
    /// # Errors
    /// Returns an error if no such column family exists.
    fn column_family(&self, name: &str) -> MidgeResult<super::column_family::ColumnFamilyHandle>;

    /// Get a handle to the default column family.
    ///
    /// The default family is always present and cannot be dropped.
    fn default_column_family(&self) -> super::column_family::ColumnFamilyHandle;

    /// List all available column families in the store.
    fn list_column_families(&self) -> Vec<super::column_family::ColumnFamilyHandle>;

    /// Drop a column family and permanently delete all associated data.
    ///
    /// # Warning
    /// This operation is **irreversible**.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The column family does not exist, or
    /// - The default column family is targeted.
    fn drop_column_family(&self, cf: &super::column_family::ColumnFamilyHandle) -> MidgeResult<()>;

    // ==================== Data Operations ====================

    /// Insert a key-value pair. Fails if the key already exists.
    fn insert(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()>;

    /// Write a key-value pair. Overwrites the value if the key exists.
    fn put(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()>;

    /// Read a value by key. Returns `Ok(None)` if the key is not found.
    fn get(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<Option<Bytes>>;

    /// Delete a single key.
    fn delete(&self, cf: &super::column_family::ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()>;

    /// Delete all keys in the range `[start, end)`.
    fn delete_range(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()>;

    /// Scan keys in the range `[start, end)`, returning `(key, value)` pairs in sorted order.
    fn scan(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(Bytes, Bytes)>>;

    // ==================== Batch Operations ====================

    /// Execute a set of mixed operations atomically within a single column family.
    ///
    /// All operations are applied in order and committed atomically.
    fn batch(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        operations: Vec<BatchOperation>,
    ) -> MidgeResult<()>;

    // ==================== Transactions ====================

    /// Begin a new transaction with snapshot isolation, scoped to a single column family.
    ///
    /// The returned transaction stages operations in memory until committed.
    ///
    /// # Design Note
    /// Transactions are intentionally limited to a single column family for simplicity
    /// and performance. For multi-CF atomicity, use multiple transactions with
    /// application-level coordination.
    fn begin_transaction(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
    ) -> MidgeResult<Box<dyn KvTransaction>>;

    /// Commit a transaction atomically.
    ///
    /// # Arguments
    /// * `txn` — The transaction returned by [`begin_transaction`](Self::begin_transaction).
    /// * `opts` — Write options controlling durability (e.g., sync vs. async).
    ///
    /// # Errors
    /// Returns an error if the transaction has expired or conflicts were detected.
    fn commit_transaction(
        &self,
        txn: Box<dyn KvTransaction>,
        opts: WriteOptions,
    ) -> MidgeResult<()>;

    /// Roll back a transaction, discarding all staged operations.
    fn rollback_transaction(&self, txn: Box<dyn KvTransaction>) -> MidgeResult<()>;
}

/// Transaction interface for staging operations within a consistent snapshot.
///
/// Transactions guarantee ACID semantics at the column-family level.
/// All operations are isolated and applied atomically on commit.
///
/// Reads within a transaction see a consistent snapshot as of `begin_transaction`,
/// including uncommitted writes staged within the same transaction.
///
/// # Implementation Note
/// The `Any` bound allows backends to perform downcasting for internal optimizations.
pub trait KvTransaction: Send + Sync + std::any::Any {
    /// Stage a `put` operation. Overwrites if the key already exists.
    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Read a value within the transaction’s snapshot or staged state.
    ///
    /// Returns `None` if the key does not exist or has been deleted.
    fn get(&mut self, key: &[u8]) -> MidgeResult<Option<Bytes>>;

    /// Stage a single-key deletion.
    fn delete(&mut self, key: &[u8]) -> MidgeResult<()>;

    /// Scan a key range `[start, end)` within the transaction’s snapshot and staged writes.
    fn scan(&mut self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>>;

    /// Stage a delete-range operation for all keys in `[start, end)`.
    fn delete_range(&mut self, start: &[u8], end: &[u8]) -> MidgeResult<()>;
}

/// Shared trait object alias for dynamic dispatch.
pub type DynKvStore = Arc<dyn KvStore>;
