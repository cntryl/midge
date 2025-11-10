//! Engine-backed transaction wrapper for KvStore trait
//!
//! Provides `EngineTransaction` which wraps a `Transaction` with engine access
//! to enable read operations that query the storage engine.

use bytes::Bytes;
use std::sync::Arc;

use crate::api::kv_store::KvTransaction;
use crate::api::transaction::Transaction;
use crate::core::engine::MidgeEngine;
use crate::MidgeResult;

/// Transaction wrapper that provides read access via engine reference.
///
/// Used internally when transactions are created through the KvStore trait.
/// This wrapper bridges the public Transaction API with engine internals,
/// allowing transaction-aware reads from the storage engine.
pub struct EngineTransaction {
    txn: Transaction,
    engine: Arc<MidgeEngine>,
}

impl EngineTransaction {
    /// Create a new engine-backed transaction wrapper.
    ///
    /// # Arguments
    /// * `txn` - The underlying transaction for staging mutations
    /// * `engine` - Reference to the engine for read operations
    pub fn new(txn: Transaction, engine: Arc<MidgeEngine>) -> Self {
        Self { txn, engine }
    }
}

// Implement the public KvTransaction trait for EngineTransaction
impl KvTransaction for EngineTransaction {
    fn put(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.txn.put(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            None,
        )
    }

    fn get(&mut self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        let cf = self.engine.default_column_family();
        self.engine.transaction_get(&mut self.txn, &cf, key)
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
        let cf = self.engine.default_column_family();
        self.engine.scan(&cf, q)
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

    fn into_transaction(
        self: Box<Self>,
    ) -> Result<Transaction, Box<dyn KvTransaction>> {
        // Extract the transaction from EngineTransaction
        Ok(self.txn)
    }
}
