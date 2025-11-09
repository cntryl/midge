//! Simple key-value store trait for external integrations.
//!
//! Defines a backend-agnostic API for Midge key-value stores.
//! Supports column-family scoping, atomic batches, transactions,
//! and advanced operations like compare-and-swap and merge.

use super::write_options::WriteOptions;
use crate::error::MidgeResult;
use bytes::Bytes;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum BatchOperation {
    /// Insert a key-value pair. Fails if the key already exists.
    Insert { key: Vec<u8>, value: Vec<u8> },

    /// Put a key-value pair. Overwrites if the key already exists.
    Put { key: Vec<u8>, value: Vec<u8> },

    /// Delete a single key.
    Delete { key: Vec<u8> },

    /// Delete all keys in the range `[start, end)`.
    DeleteRange { start: Vec<u8>, end: Vec<u8> },

    /// Compare and swap. Writes `new_value` if the existing value equals `expected`.
    CompareAndSwap {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new_value: Vec<u8>,
    },

    /// Merge `value` into the existing value using backend-specific semantics.
    Merge { key: Vec<u8>, value: Vec<u8> },
}

pub trait KvStore: Send + Sync {
    // ==================== Column Families ====================

    fn create_column_family(
        &self,
        name: &str,
        config: super::column_family::ColumnFamilyConfig,
    ) -> MidgeResult<super::column_family::ColumnFamilyHandle>;

    fn column_family(&self, name: &str) -> MidgeResult<super::column_family::ColumnFamilyHandle>;
    fn default_column_family(&self) -> super::column_family::ColumnFamilyHandle;
    fn list_column_families(&self) -> Vec<super::column_family::ColumnFamilyHandle>;
    fn drop_column_family(&self, cf: &super::column_family::ColumnFamilyHandle) -> MidgeResult<()>;

    // ==================== Core Data Ops ====================

    fn insert(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()>;

    fn put(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()>;

    fn get(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<Option<Bytes>>;

    fn delete(&self, cf: &super::column_family::ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()>;

    fn delete_range(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()>;

    fn scan(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(Bytes, Bytes)>>;

    /// Atomically compare and swap a key's value.
    ///
    /// - If `expected` is `None`, succeeds only if the key does not exist.
    /// - Returns `Ok(true)` if the swap succeeded, `Ok(false)` otherwise.
    fn compare_and_swap(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> MidgeResult<bool>;

    /// Merge a value into an existing key using backend-defined semantics.
    ///
    /// Merge operations are associative and may be applied lazily.
    fn merge(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()>;

    // ==================== Batch Ops ====================

    fn batch(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
        operations: Vec<BatchOperation>,
    ) -> MidgeResult<()>;

    // ==================== Transactions ====================

    fn begin_transaction(
        &self,
        cf: &super::column_family::ColumnFamilyHandle,
    ) -> MidgeResult<Box<dyn KvTransaction>>;

    fn commit_transaction(
        &self,
        txn: Box<dyn KvTransaction>,
        opts: WriteOptions,
    ) -> MidgeResult<()>;

    fn rollback_transaction(&self, txn: Box<dyn KvTransaction>) -> MidgeResult<()>;
}

pub trait KvTransaction: Send + Sync + std::any::Any {
    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;
    fn get(&mut self, key: &[u8]) -> MidgeResult<Option<Bytes>>;
    fn delete(&mut self, key: &[u8]) -> MidgeResult<()>;
    fn scan(&mut self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>>;
    fn delete_range(&mut self, start: &[u8], end: &[u8]) -> MidgeResult<()>;

    /// Stage a compare-and-swap within this transaction.
    fn compare_and_swap(
        &mut self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> MidgeResult<bool>;

    /// Stage a merge operation within this transaction.
    fn merge(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;
}

pub type DynKvStore = Arc<dyn KvStore>;
