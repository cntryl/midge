//! KvStore trait implementation for MidgeEngine.
//!
//! This module implements the external KvStore trait, which provides
//! a CF-scoped key-value API for external callers. The implementation
//! delegates to the appropriate operation modules (reads, writes, mutations, transactions).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::error::{MidgeError, MidgeResult};

use super::super::MidgeEngine;

// Implement the external KvStore trait for Arc<MidgeEngine> so external callers
// can use the engine via the `DynKvStore = Arc<dyn KvStore>` abstraction.
// Using Arc allows transactions to hold a reference to the engine for reads.
impl crate::api::kv_store::KvStore for Arc<MidgeEngine> {
    // ==================== Column Family Management ====================

    fn create_column_family(
        &self,
        name: &str,
        config: crate::api::column_family::ColumnFamilyConfig,
    ) -> MidgeResult<crate::api::column_family::ColumnFamilyHandle> {
        self.as_ref().create_column_family(name, config)
    }

    fn column_family(
        &self,
        name: &str,
    ) -> MidgeResult<crate::api::column_family::ColumnFamilyHandle> {
        self.as_ref().get_column_family(name)
    }

    fn default_column_family(&self) -> crate::api::column_family::ColumnFamilyHandle {
        self.as_ref().default_column_family()
    }

    fn list_column_families(&self) -> Vec<crate::api::column_family::ColumnFamilyHandle> {
        self.as_ref().list_column_families()
    }

    fn drop_column_family(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
    ) -> MidgeResult<()> {
        self.as_ref().drop_column_family(cf)
    }

    // ==================== Data Operations (CF-Scoped) ====================

    fn put(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        self.as_ref().put(cf, key, value)
    }

    fn get(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<Option<Bytes>> {
        self.as_ref().get(cf, key)
    }

    fn delete(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<()> {
        self.as_ref().delete(cf, key)
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
        self.as_ref().scan(cf, q)
    }

    fn delete_range(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()> {
        self.as_ref().delete_range(cf, start, end)
    }

    fn insert(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        // KvStore::insert is currently an alias for put
        // Use insert_with_value() for insert-if-absent semantics
        self.as_ref().put(cf, key, value)
    }

    fn compare_and_swap(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> MidgeResult<bool> {
        // Read current value
        let current = self.as_ref().get(cf, key)?;

        // Check if current value matches expected
        let matches = match (current.as_ref(), expected) {
            (None, None) => true,
            (Some(c), Some(e)) => c.as_ref() == e,
            _ => false,
        };

        // If matches, perform the swap
        if matches {
            self.as_ref().put(cf, key, new_value)?;
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
        self.as_ref().merge_cf(cf, key, value)
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
                    self.as_ref().put(cf, &key, &value)?;
                }
                crate::api::kv_store::BatchOperation::Put { key, value } => {
                    self.as_ref().put(cf, &key, &value)?;
                }
                crate::api::kv_store::BatchOperation::Delete { key } => {
                    self.as_ref().delete(cf, &key)?;
                }
                crate::api::kv_store::BatchOperation::DeleteRange { start, end } => {
                    self.as_ref().delete_range(cf, &start, &end)?;
                }
                crate::api::kv_store::BatchOperation::CompareAndSwap {
                    key,
                    expected,
                    new_value,
                } => {
                    // For batch operations, CAS is not atomic across the batch
                    // Each CAS is applied individually
                    let current = self.as_ref().get(cf, &key)?;
                    let matches = match (current.as_ref(), expected.as_ref()) {
                        (None, None) => true,
                        (Some(c), Some(e)) => c.as_ref() == e.as_slice(),
                        _ => false,
                    };
                    if matches {
                        self.as_ref().put(cf, &key, &new_value)?;
                    }
                }
                crate::api::kv_store::BatchOperation::Merge { key, value } => {
                    // Delegate to merge operation which handles merge operators
                    self.as_ref().merge_cf(cf, &key, &value)?;
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
        let txn_id = self.txn_id.fetch_add(1, Ordering::SeqCst);
        let begin_sequence = self.seq.load(Ordering::SeqCst);
        let txn = crate::api::Transaction::new(txn_id, begin_sequence);
        let engine_txn = crate::api::transaction::EngineTransaction::new(txn, Arc::clone(self));
        Ok(Box::new(engine_txn))
    }

    fn commit_transaction(
        &self,
        txn: Box<dyn crate::api::kv_store::KvTransaction>,
        opts: crate::api::WriteOptions,
    ) -> MidgeResult<()> {
        self.check_read_only()?;

        // Downcast to EngineTransaction to extract the Transaction
        let engine_txn = (txn as Box<dyn std::any::Any>)
            .downcast::<crate::api::transaction::EngineTransaction>()
            .map_err(|_| MidgeError::internal("Failed to downcast transaction"))?;

        let txn = engine_txn.into_inner();

        // Check if transaction is expired (timeout)
        if txn.is_expired() {
            return Err(MidgeError::transaction_conflict("transaction timed out"));
        }

        // Register transaction with manager (tracks read/write sets)
        let write_set = txn
            .write_set()
            .clone()
            .into_iter()
            .map(|(cf, key)| crate::core::transaction_manager::Key::new(cf, key))
            .collect::<HashSet<_>>();
        let read_set = txn
            .read_set()
            .clone()
            .into_iter()
            .map(|(cf, key)| crate::core::transaction_manager::Key::new(cf, key))
            .collect::<HashSet<_>>();
        let read_versions = txn
            .read_versions()
            .clone()
            .into_iter()
            .map(|((cf, key), v)| (crate::core::transaction_manager::Key::new(cf, key), v))
            .collect::<HashMap<_, _>>();

        if let Err(e) = self.txn_manager.begin(
            txn.txn_id(),
            txn.begin_sequence(),
            write_set,
            read_set,
            read_versions,
        ) {
            return Err(MidgeError::transaction_conflict(e));
        }

        // Update wait-for graph and check for deadlocks before commit
        if let Err(e) = self.txn_manager.update_wait_for_graph(txn.txn_id()) {
            self.txn_manager.abort(txn.txn_id());
            return Err(MidgeError::transaction_conflict(e));
        }

        // Check for deadlocks in wait-for graph
        if let Some((victim_id, cycle)) = self.txn_manager.check_for_deadlock() {
            // If this transaction is the victim, abort it
            if victim_id == txn.txn_id() {
                self.txn_manager.abort(txn.txn_id());
                return Err(MidgeError::deadlock(victim_id, cycle));
            }
            // Otherwise, abort the victim transaction (it will fail when it tries to commit)
        }

        // Allocate commit sequence for conflict detection
        let commit_seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Check for conflicts using transaction manager
        let txn_id = txn.txn_id();
        match self.txn_manager.try_commit(txn_id, commit_seq) {
            Ok(()) => {
                // No conflicts, proceed with commit
                let muts = txn.commit()?;
                self.batch_internal(muts, opts.sync)
            }
            Err(e) => {
                // Conflict detected, abort transaction
                self.txn_manager.abort(txn_id);
                Err(MidgeError::transaction_conflict(e))
            }
        }
    }

    fn rollback_transaction(
        &self,
        txn: Box<dyn crate::api::kv_store::KvTransaction>,
    ) -> MidgeResult<()> {
        // Downcast to EngineTransaction to extract the Transaction
        let engine_txn = (txn as Box<dyn std::any::Any>)
            .downcast::<crate::api::transaction::EngineTransaction>()
            .map_err(|_| MidgeError::internal("Failed to downcast transaction"))?;

        let txn = engine_txn.into_inner();

        // Abort the transaction in the transaction manager
        self.txn_manager.abort(txn.txn_id());

        // Transaction is dropped here, releasing its resources
        Ok(())
    }
}
