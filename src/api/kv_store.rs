//! Simple key-value store trait for external integrations.

use crate::error::MidgeResult;
use bytes::Bytes;
use std::sync::Arc;

/// Simple key-value store interface.
pub trait KvStore: Send + Sync {
    fn put(&self, key: &[u8], value: &[u8]) -> MidgeResult<()>;
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>>;
    fn delete(&self, key: &[u8]) -> MidgeResult<()>;
    fn put_batch(&self, writes: Vec<(Vec<u8>, Vec<u8>)>) -> MidgeResult<()>;
    fn delete_batch(&self, keys: Vec<Vec<u8>>) -> MidgeResult<()>;
    fn scan(&self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>>;
    fn flush(&self) -> MidgeResult<()>;

    /// Begin a new transaction with snapshot isolation.
    fn begin_transaction(&self) -> MidgeResult<Box<dyn KvTransaction>>;
}

/// Transaction with snapshot isolation and ACID guarantees.
pub trait KvTransaction: Send {
    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>>;
    fn delete(&mut self, key: &[u8]) -> MidgeResult<()>;
    fn scan(&self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>>;

    /// Commit the transaction. Returns error if conflicts detected.
    fn commit(self: Box<Self>) -> MidgeResult<()>;

    /// Rollback the transaction.
    fn rollback(self: Box<Self>) -> MidgeResult<()>;
}

pub type DynKvStore = Arc<dyn KvStore>;
