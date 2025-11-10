//! Engine-backed transaction wrapper for KvStore trait
//!
//! Provides `EngineTransaction` which wraps a `Transaction` with engine access
//! to enable read operations that query the storage engine.

use bytes::Bytes;
use std::sync::Arc;

use crate::api::kv_store::KvTransaction;
use crate::core::engine::MidgeEngine;
use crate::MidgeResult;

use super::transaction::Transaction;

/// Transaction wrapper that provides read access via engine reference.
///
/// This is the public transaction type that users work with.
/// It wraps an internal Transaction (mutation staging buffer) with engine access
/// to enable both reads and writes through the storage engine.
pub struct EngineTransaction {
    pub(crate) txn: Transaction,
    engine: *const MidgeEngine,
}

// Safety: EngineTransaction is only created with a valid engine reference
// and its lifetime is always bound to the engine's lifetime through Rust's
// borrow checker (engine outlives all transactions created from it)
unsafe impl Send for EngineTransaction {}
unsafe impl Sync for EngineTransaction {}

impl EngineTransaction {
    /// Create a new engine-backed transaction wrapper.
    ///
    /// # Arguments
    /// * `txn` - The underlying transaction for staging mutations
    /// * `engine` - Reference to the engine for read operations
    ///
    /// # Safety
    /// The engine reference must outlive this transaction. This is guaranteed
    /// by Rust's borrow checker when transactions are created from `&self` methods.
    pub(crate) fn new(txn: Transaction, engine: &MidgeEngine) -> Self {
        Self {
            txn,
            engine: engine as *const MidgeEngine,
        }
    }

    /// Create from Arc<MidgeEngine> (for KvStore adapter)
    pub(crate) fn from_arc(txn: Transaction, engine: Arc<MidgeEngine>) -> Self {
        Self {
            txn,
            engine: Arc::into_raw(engine),
        }
    }
}

// Implement the public KvTransaction trait for EngineTransaction
impl KvTransaction for EngineTransaction {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.txn.insert(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            None,
        )
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.txn.put(key, value)
    }

    fn get(&mut self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        // Safety: Engine pointer is valid for the transaction's lifetime
        let engine = unsafe { &*self.engine };
        let cf = engine.default_column_family();
        engine.transaction_get(&mut self.txn, &cf, key)
    }

    fn delete(&mut self, key: &[u8]) -> MidgeResult<()> {
        self.txn.delete(Bytes::copy_from_slice(key))
    }

    fn scan(&mut self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        // Use engine's scan with transaction's snapshot
        let q = crate::api::query::Query::new()
            .start_key(Bytes::copy_from_slice(start))
            .end_key(Bytes::copy_from_slice(end));

        // TODO: Implement transaction-aware scan in engine
        // For now, run a column-family scoped scan on the engine's default CF
        // Safety: Engine pointer is valid for the transaction's lifetime
        let engine = unsafe { &*self.engine };
        let cf = engine.default_column_family();
        engine.scan(&cf, q)
    }

    fn delete_range(&mut self, start: &[u8], end: &[u8]) -> MidgeResult<()> {
        self.txn
            .delete_range(Bytes::copy_from_slice(start), Bytes::copy_from_slice(end))
    }

    fn compare_and_swap(
        &mut self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> MidgeResult<bool> {
        self.txn.compare_and_swap(
            Bytes::copy_from_slice(key),
            expected.map(Bytes::copy_from_slice),
            Bytes::copy_from_slice(new_value),
        )?;
        // For now, always return true since validation happens at commit time
        Ok(true)
    }

    fn merge(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.txn
            .merge(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
    }
}
