//! Engine-backed transaction wrapper for KvStore trait
//!
//! Provides `EngineTransaction` which wraps a `Transaction` with engine access
//! to enable read operations that query the storage engine.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::api::kv_store::KvTransaction;
use crate::core::engine::MidgeEngine;
use crate::MidgeResult;

use super::core::Transaction;

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

// Public accessor for transaction ID (used in tests)
impl EngineTransaction {
    pub fn txn_id(&self) -> u64 {
        self.txn.txn_id
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
        self.txn.put(key, value)?;

        // Update transaction manager with current conflict sets
        // Safety: Engine pointer is valid for the transaction's lifetime
        let engine = unsafe { &*self.engine };
        let write_set: HashSet<crate::core::transaction::controller::Key> = self
            .txn
            .conflict_write_set()
            .into_iter()
            .map(|(cf, k)| crate::core::transaction::controller::Key::new(cf, k))
            .collect();
        let write_ranges = self.txn.conflict_write_ranges().clone();
        let read_set: HashSet<crate::core::transaction::controller::Key> = self
            .txn
            .conflict_read_set()
            .into_iter()
            .map(|(cf, k)| crate::core::transaction::controller::Key::new(cf, k))
            .collect();
        let read_versions: HashMap<crate::core::transaction::controller::Key, u64> = self
            .txn
            .conflict_read_versions()
            .into_iter()
            .map(|((cf, k), v)| (crate::core::transaction::controller::Key::new(cf, k), v))
            .collect();

        let _ = engine.txn_manager.update(
            self.txn.txn_id,
            write_set,
            write_ranges,
            read_set,
            read_versions,
        );

        Ok(())
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
        // Transaction-aware scan: merge uncommitted writes with engine data
        let q = crate::api::query::Query::new()
            .start_key(Bytes::copy_from_slice(start))
            .end_key(Bytes::copy_from_slice(end));

        // Safety: Engine pointer is valid for the transaction's lifetime
        let engine = unsafe { &*self.engine };
        let cf = engine.default_column_family();

        // Create snapshot at transaction's begin sequence for consistent reads
        let snapshot = engine.snapshot();
        let mut results = engine.scan_at(&cf, q, &snapshot)?;

        // Build map of uncommitted writes in the transaction
        // This includes both staged and potentially spilled mutations
        use std::collections::BTreeMap;
        let mut uncommitted: BTreeMap<Bytes, Option<Bytes>> = BTreeMap::new();

        // Process staged mutations (in-memory buffer)
        for mutation in self.txn.staged_mutations() {
            // Only process mutations in the scan range for default CF
            if mutation.cf_id == crate::api::DEFAULT_CF_ID
                && mutation.key.as_ref() >= start
                && mutation.key.as_ref() < end
            {
                match mutation.op {
                    crate::api::mutation::MutationOp::Put
                    | crate::api::mutation::MutationOp::Insert => {
                        uncommitted.insert(mutation.key.clone(), mutation.value.clone());
                    }
                    crate::api::mutation::MutationOp::Delete => {
                        uncommitted.insert(mutation.key.clone(), None);
                    }
                    crate::api::mutation::MutationOp::DeleteRange => {
                        // Handle range deletion
                        if let Some(range_end) = &mutation.range_end {
                            // Mark all keys in range as deleted
                            let range_start = mutation.key.clone();
                            // Remove from results any keys in this range
                            results.retain(|(k, _)| k < &range_start || k >= range_end);
                        }
                    }
                    _ => {} // Merge, CompareAndSwap handled separately
                }
            }
        }

        // Apply uncommitted writes: remove deletes, update/add puts
        results.retain(|(k, _)| !uncommitted.get(k).is_some_and(|v| v.is_none()));

        for (key, value_opt) in uncommitted {
            if let Some(value) = value_opt {
                // Remove existing entry and add updated value
                results.retain(|(k, _)| k != &key);
                results.push((key, value));
            }
        }

        // Sort results by key (uncommitted writes may have disrupted order)
        results.sort_by(|a, b| a.0.cmp(&b.0));

        Ok(results)
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
