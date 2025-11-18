//! KvStore trait implementation for MidgeEngine.
//!
//! This module provides a composition-based adapter that implements the external
//! KvStore trait. This design separates the public API from engine internals and
//! avoids awkward trait implementations on Arc<T>.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{MidgeError, MidgeResult};

use super::MidgeEngine;

/// Adapter that exposes MidgeEngine functionality through the KvStore trait.
///
/// This uses **composition over inheritance** - the adapter wraps the engine
/// and delegates to it, rather than implementing the trait directly on Arc<MidgeEngine>.
///
/// Benefits:
/// - Clean separation between public API and internal implementation
/// - Type-safe: no downcasting required
/// - Flexible: can add caching, metrics, or other cross-cutting concerns
/// - Easier to test: can mock the adapter independently
pub struct KvStoreAdapter {
    engine: Arc<MidgeEngine>,
}

impl KvStoreAdapter {
    /// Create a new KvStore adapter wrapping the given engine.
    pub fn new(engine: Arc<MidgeEngine>) -> Self {
        Self { engine }
    }
}

impl crate::api::kv_store::KvStore for KvStoreAdapter {
    // ==================== Column Family Management ====================

    fn create_column_family(
        &self,
        name: &str,
        config: crate::api::column_family::ColumnFamilyConfig,
    ) -> MidgeResult<crate::api::column_family::ColumnFamilyId> {
        let handle = self.engine.create_column_family(name, config)?;
        Ok(handle.id())
    }

    fn column_family(&self, name: &str) -> MidgeResult<crate::api::column_family::ColumnFamilyId> {
        let handle = self.engine.get_column_family(name)?;
        Ok(handle.id())
    }

    fn default_column_family(&self) -> crate::api::column_family::ColumnFamilyId {
        self.engine.default_column_family().id()
    }

    fn list_column_families(&self) -> Vec<crate::api::column_family::ColumnFamilyId> {
        self.engine
            .list_column_families()
            .into_iter()
            .map(|h| h.id())
            .collect()
    }

    fn drop_column_family(&self, cf: crate::api::column_family::ColumnFamilyId) -> MidgeResult<()> {
        // Resolve handle by scanning engine handles (non-hot path)
        if let Some(handle) = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
        {
            return self.engine.drop_column_family(&handle);
        }
        Err(MidgeError::invalid_config(format!(
            "Column family id {} does not exist",
            cf.as_u32()
        )))
    }

    // ==================== Data Operations (CF-Scoped) ====================

    fn put(
        &self,
        cf: crate::api::column_family::ColumnFamilyId,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;
        self.engine.put(&handle, key, value)
    }

    fn get(
        &self,
        cf: crate::api::column_family::ColumnFamilyId,
        key: &[u8],
    ) -> MidgeResult<Option<Bytes>> {
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;
        self.engine.get(&handle, key)
    }

    fn delete(&self, cf: crate::api::column_family::ColumnFamilyId, key: &[u8]) -> MidgeResult<()> {
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;
        self.engine.delete(&handle, key)
    }

    fn scan(
        &self,
        cf: crate::api::column_family::ColumnFamilyId,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let q = crate::api::query::Query::new()
            .start_key(Bytes::copy_from_slice(start))
            .end_key(Bytes::copy_from_slice(end));
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;
        self.engine.scan(&handle, q)
    }

    fn delete_range(
        &self,
        cf: crate::api::column_family::ColumnFamilyId,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()> {
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;
        self.engine.delete_range(&handle, start, end)
    }

    fn insert(
        &self,
        cf: crate::api::column_family::ColumnFamilyId,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        // Enforce insert uniqueness at commit time using a short transaction
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;

        let mut etxn = self.engine.begin_transaction(&handle)?;
        // Use CF-specific insert via internal transaction API
        etxn.txn.insert_cf(
            cf,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            None,
        )?;
        self.engine
            .commit_transaction(etxn, crate::api::WriteOptions::default())
    }

    fn compare_and_swap(
        &self,
        cf: crate::api::column_family::ColumnFamilyId,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> MidgeResult<bool> {
        // Perform CAS via a short transaction and interpret commit outcome
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;

        let mut etxn = self.engine.begin_transaction(&handle)?;
        etxn.txn.compare_and_swap_cf(
            cf,
            Bytes::copy_from_slice(key),
            expected.map(Bytes::copy_from_slice),
            Bytes::copy_from_slice(new_value),
        )?;
        match self
            .engine
            .commit_transaction(etxn, crate::api::WriteOptions::default())
        {
            Ok(()) => Ok(true),
            Err(e) => {
                if matches!(e, MidgeError::TransactionConflict { .. }) {
                    // CAS precondition failed at commit
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn merge(
        &self,
        cf: crate::api::column_family::ColumnFamilyId,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        // Delegate to the merge operation which handles merge operators
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;
        self.engine.merge_cf(&handle, key, value)
    }

    // ==================== Batch Operations ====================

    fn batch(
        &self,
        cf: crate::api::column_family::ColumnFamilyId,
        operations: Vec<crate::api::kv_store::BatchOperation>,
    ) -> MidgeResult<()> {
        // Apply each operation individually to the specified CF
        // For atomic multi-operation batches, use write_batch() with WriteBatch
        let handle = self
            .engine
            .list_column_families()
            .into_iter()
            .find(|h| h.id() == cf)
            .ok_or_else(|| {
                MidgeError::invalid_config(format!("cf id {} not found", cf.as_u32()))
            })?;
        for op in operations {
            match op {
                crate::api::kv_store::BatchOperation::Insert { key, value } => {
                    // Enforce at commit time via per-op short transaction
                    let mut etxn = self.engine.begin_transaction(&handle)?;
                    etxn.txn.insert_cf(
                        cf,
                        Bytes::from(key.clone()),
                        Bytes::from(value.clone()),
                        None,
                    )?;
                    self.engine
                        .commit_transaction(etxn, crate::api::WriteOptions::default())?;
                }
                crate::api::kv_store::BatchOperation::Put { key, value } => {
                    self.engine.put(&handle, &key, &value)?;
                }
                crate::api::kv_store::BatchOperation::Delete { key } => {
                    self.engine.delete(&handle, &key)?;
                }
                crate::api::kv_store::BatchOperation::DeleteRange { start, end } => {
                    self.engine.delete_range(&handle, &start, &end)?;
                }
                crate::api::kv_store::BatchOperation::CompareAndSwap {
                    key,
                    expected,
                    new_value,
                } => {
                    // Enforce at commit time via per-op short transaction
                    let mut etxn = self.engine.begin_transaction(&handle)?;
                    etxn.txn.compare_and_swap_cf(
                        cf,
                        Bytes::from(key.clone()),
                        expected.clone().map(Bytes::from),
                        Bytes::from(new_value.clone()),
                    )?;
                    // Interpret CAS failure as Ok(false) by swallowing conflict
                    if let Err(e) = self
                        .engine
                        .commit_transaction(etxn, crate::api::WriteOptions::default())
                    {
                        if !matches!(e, MidgeError::TransactionConflict { .. }) {
                            return Err(e);
                        }
                        // else: CAS failed; continue
                    }
                }
                crate::api::kv_store::BatchOperation::Merge { key, value } => {
                    // Delegate to merge operation which handles merge operators
                    self.engine.merge_cf(&handle, &key, &value)?;
                }
            }
        }
        Ok(())
    }

    // ==================== Transactions ====================

    fn begin_transaction(
        &self,
        _cf: crate::api::column_family::ColumnFamilyId,
    ) -> MidgeResult<Box<dyn crate::api::kv_store::KvTransaction>> {
        // Transactions work across all column families
        // The CF parameter is accepted for trait compatibility but transactions
        // are not scoped to a single CF - operations within the transaction
        // can target any CF via the EngineTransaction methods
        let txn_id = self.engine.txn_id.fetch_add(1, Ordering::SeqCst);
        let begin_sequence = self.engine.seq.load(Ordering::SeqCst);
        let txn = crate::core::transaction::Transaction::new(txn_id, begin_sequence);
        let engine_txn =
            crate::core::transaction::EngineTransaction::from_arc(txn, Arc::clone(&self.engine));
        Ok(Box::new(engine_txn))
    }

    fn commit_transaction(
        &self,
        txn: Box<dyn crate::api::kv_store::KvTransaction>,
        opts: crate::api::WriteOptions,
    ) -> MidgeResult<()> {
        self.engine.check_read_only()?;

        // Downcast to EngineTransaction to access internals
        // KvTransaction: Any allows us to downcast from the trait object
        let any_txn: Box<dyn std::any::Any> = txn;
        let engine_txn = any_txn
            .downcast::<crate::core::transaction::EngineTransaction>()
            .map_err(|_| MidgeError::internal("Transaction type not supported"))?;

        // Extract the internal Transaction by moving out of the box
        let txn = engine_txn.txn;

        // Check if transaction is expired (timeout)
        if txn.is_expired() {
            return Err(MidgeError::transaction_conflict("transaction timed out"));
        }

        // Allocate commit sequence for conflict detection
        let _commit_seq = self.engine.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Commit the transaction mutations
        let muts = txn.commit()?;
        self.engine.batch_internal(muts, opts.sync)
    }

    fn rollback_transaction(
        &self,
        txn: Box<dyn crate::api::kv_store::KvTransaction>,
    ) -> MidgeResult<()> {
        // Downcast to EngineTransaction to access internals
        let any_txn: Box<dyn std::any::Any> = txn;
        let engine_txn = any_txn
            .downcast::<crate::core::transaction::EngineTransaction>()
            .map_err(|_| MidgeError::internal("Transaction type not supported"))?;

        // Extract the internal Transaction by moving out of the box
        let _txn = engine_txn.txn;

        // Transaction is dropped here, releasing its resources
        Ok(())
    }
}
