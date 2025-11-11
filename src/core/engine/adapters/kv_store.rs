//! KvStore trait implementation for MidgeEngine.
//!
//! This module provides a composition-based adapter that implements the external
//! KvStore trait. This design separates the public API from engine internals and
//! avoids awkward trait implementations on Arc<T>.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{MidgeError, MidgeResult};

use super::super::MidgeEngine;

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
    ) -> MidgeResult<crate::api::column_family::ColumnFamilyHandle> {
        self.engine.create_column_family(name, config)
    }

    fn column_family(
        &self,
        name: &str,
    ) -> MidgeResult<crate::api::column_family::ColumnFamilyHandle> {
        self.engine.get_column_family(name)
    }

    fn default_column_family(&self) -> crate::api::column_family::ColumnFamilyHandle {
        self.engine.default_column_family()
    }

    fn list_column_families(&self) -> Vec<crate::api::column_family::ColumnFamilyHandle> {
        self.engine.list_column_families()
    }

    fn drop_column_family(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
    ) -> MidgeResult<()> {
        self.engine.drop_column_family(cf)
    }

    // ==================== Data Operations (CF-Scoped) ====================

    fn put(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        self.engine.put(cf, key, value)
    }

    fn get(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<Option<Bytes>> {
        self.engine.get(cf, key)
    }

    fn delete(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<()> {
        self.engine.delete(cf, key)
    }

    fn scan(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let q = crate::api::query::Query::new()
            .start_key(Bytes::copy_from_slice(start))
            .end_key(Bytes::copy_from_slice(end));
        self.engine.scan(cf, q)
    }

    fn delete_range(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()> {
        self.engine.delete_range(cf, start, end)
    }

    fn insert(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        // KvStore::insert is currently an alias for put
        // Use insert_with_value() for insert-if-absent semantics
        self.engine.put(cf, key, value)
    }

    fn compare_and_swap(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> MidgeResult<bool> {
        // Read current value
        let current = self.engine.get(cf, key)?;

        // Check if current value matches expected
        let matches = match (current.as_ref(), expected) {
            (None, None) => true,
            (Some(c), Some(e)) => c.as_ref() == e,
            _ => false,
        };

        // If matches, perform the swap
        if matches {
            self.engine.put(cf, key, new_value)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn merge(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        // Delegate to the merge operation which handles merge operators
        self.engine.merge_cf(cf, key, value)
    }

    // ==================== Batch Operations ====================

    fn batch(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        operations: Vec<crate::api::kv_store::BatchOperation>,
    ) -> MidgeResult<()> {
        // Apply each operation individually to the specified CF
        // For atomic multi-operation batches, use write_batch() with WriteBatch
        for op in operations {
            match op {
                crate::api::kv_store::BatchOperation::Insert { key, value } => {
                    self.engine.put(cf, &key, &value)?;
                }
                crate::api::kv_store::BatchOperation::Put { key, value } => {
                    self.engine.put(cf, &key, &value)?;
                }
                crate::api::kv_store::BatchOperation::Delete { key } => {
                    self.engine.delete(cf, &key)?;
                }
                crate::api::kv_store::BatchOperation::DeleteRange { start, end } => {
                    self.engine.delete_range(cf, &start, &end)?;
                }
                crate::api::kv_store::BatchOperation::CompareAndSwap {
                    key,
                    expected,
                    new_value,
                } => {
                    // For batch operations, CAS is not atomic across the batch
                    // Each CAS is applied individually
                    let current = self.engine.get(cf, &key)?;
                    let matches = match (current.as_ref(), expected.as_ref()) {
                        (None, None) => true,
                        (Some(c), Some(e)) => c.as_ref() == e.as_slice(),
                        _ => false,
                    };
                    if matches {
                        self.engine.put(cf, &key, &new_value)?;
                    }
                }
                crate::api::kv_store::BatchOperation::Merge { key, value } => {
                    // Delegate to merge operation which handles merge operators
                    self.engine.merge_cf(cf, &key, &value)?;
                }
            }
        }
        Ok(())
    }

    // ==================== Transactions ====================

    fn begin_transaction(
        &self,
        _cf: &crate::api::column_family::ColumnFamilyHandle,
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

        // Register transaction with manager (tracks read/write sets)
        let write_set = txn
            .write_set()
            .clone()
            .into_iter()
            .map(|(cf, key)| crate::core::transaction::Key::new(cf, key))
            .collect::<HashSet<_>>();
        let read_set = txn
            .read_set()
            .clone()
            .into_iter()
            .map(|(cf, key)| crate::core::transaction::Key::new(cf, key))
            .collect::<HashSet<_>>();
        let read_versions = txn
            .read_versions()
            .clone()
            .into_iter()
            .map(|((cf, key), v)| (crate::core::transaction::Key::new(cf, key), v))
            .collect::<HashMap<_, _>>();

        if let Err(e) = self.engine.txn_manager.begin(
            txn.txn_id(),
            txn.begin_seq(),
            write_set,
            read_set,
            read_versions,
        ) {
            return Err(MidgeError::transaction_conflict(e));
        }

        // Update wait-for graph and check for deadlocks before commit
        if let Err(e) = self.engine.txn_manager.update_wait_for_graph(txn.txn_id()) {
            self.engine.txn_manager.abort(txn.txn_id());
            return Err(MidgeError::transaction_conflict(e));
        }

        // Check for deadlocks in wait-for graph
        if let Some((victim_id, cycle)) = self.engine.txn_manager.check_for_deadlock() {
            // If this transaction is the victim, abort it
            if victim_id == txn.txn_id() {
                self.engine.txn_manager.abort(txn.txn_id());
                return Err(MidgeError::deadlock(victim_id, cycle));
            }
            // Otherwise, abort the victim transaction (it will fail when it tries to commit)
        }

        // Allocate commit sequence for conflict detection
        let commit_seq = self.engine.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Check for conflicts using transaction manager
        let txn_id = txn.txn_id();
        match self.engine.txn_manager.try_commit(txn_id, commit_seq) {
            Ok(()) => {
                // No conflicts, proceed with commit
                let muts = txn.commit()?;
                self.engine.batch_internal(muts, opts.sync)
            }
            Err(e) => {
                // Conflict detected, abort transaction
                self.engine.txn_manager.abort(txn_id);
                Err(MidgeError::transaction_conflict(e))
            }
        }
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
        let txn = engine_txn.txn;

        // Abort the transaction in the transaction manager
        self.engine.txn_manager.abort(txn.txn_id());

        // Transaction is dropped here, releasing its resources
        Ok(())
    }
}
