//! Transaction operations for MidgeEngine
//!
//! This module contains transaction commit logic and transaction-aware reads.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;

use crate::api::column_family::ColumnFamilyHandle;
use crate::api::mutation::Mutation;
use crate::api::transaction::Transaction;
use crate::common::timestamp;
use crate::core::wal_replay::wal_record_encoded_len;
use crate::error::{MidgeError, MidgeResult};
use crate::manifest::Manifest;
use crate::wal::WalOpKind;

use super::super::MidgeEngine;

impl MidgeEngine {
    /// Internal batch implementation with explicit sync control.
    ///
    /// Used by `batch()` (with database-level sync) and `commit_transaction()`
    /// (with per-transaction sync).
    pub(crate) fn batch_internal(
        &self,
        mutations: Vec<Mutation>,
        sync: bool,
    ) -> MidgeResult<()> {
        self.check_read_only()?;

        if mutations.is_empty() {
            return Ok(());
        }

        // Allocate a transaction ID for this batch
        let txn_id = self.txn_id.fetch_add(1, Ordering::SeqCst);

        // Write TxnBegin marker
        let begin_seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let begin_rec = crate::wal::WalRecord::new_txn_begin(txn_id, begin_seq);
        self.wal_coordinator.append_record(&begin_rec)?;

        // Pre-compute a sequence per mutation to keep ordering stable for MemTable apply
        let mut seqs: Vec<u64> = Vec::with_capacity(mutations.len());
        for m in &mutations {
            let (kind, vlen, rend_len) = match m.op {
                crate::api::mutation::MutationOp::Put
                | crate::api::mutation::MutationOp::Insert
                | crate::api::mutation::MutationOp::Merge => {
                    (WalOpKind::Put, m.value.as_ref().map(|v| v.len()), None)
                }
                crate::api::mutation::MutationOp::CompareAndSwap => {
                    // CAS uses Put WAL record; validation happens at apply time
                    (WalOpKind::Put, m.value.as_ref().map(|v| v.len()), None)
                }
                crate::api::mutation::MutationOp::Delete => (WalOpKind::Delete, None, None),
                crate::api::mutation::MutationOp::DeleteRange => (
                    WalOpKind::DeleteRange,
                    None,
                    m.range_end.as_ref().map(|r| r.len()),
                ),
            };
            let predicted = wal_record_encoded_len(kind, m.key.len(), vlen, rend_len);
            if self.wal_coordinator.current_pos().saturating_add(predicted)
                > self.wal_buffer_size as u64
            {
                // Rotate before appending this record
                // For transactions with multiple CFs, flush the default CF
                // TODO: Track which CF triggered the WAL rotation and flush that one
                let _ = self.rollover_and_queue_flush(crate::api::column_family::DEFAULT_CF_ID);
            }
            let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
            seqs.push(seq);
            let ttl_seconds = m.ttl.map(|d| d.as_secs()).unwrap_or(0);

            // Build record with txn_id
            let mut record = match m.op {
                crate::api::mutation::MutationOp::Put
                | crate::api::mutation::MutationOp::Insert
                | crate::api::mutation::MutationOp::CompareAndSwap
                | crate::api::mutation::MutationOp::Merge => {
                    let expiration = if ttl_seconds > 0 {
                        let now = timestamp::now_millis();
                        Some(now + (ttl_seconds * 1000))
                    } else {
                        None
                    };
                    let mut rec = crate::wal::WalRecord::new_cf(
                        m.cf_id,
                        WalOpKind::Put,
                        m.key.clone(),
                        m.value.clone(),
                        seq,
                    );
                    rec.expiration = expiration;
                    rec
                }
                crate::api::mutation::MutationOp::Delete => crate::wal::WalRecord::new_cf(
                    m.cf_id,
                    WalOpKind::Delete,
                    m.key.clone(),
                    None,
                    seq,
                ),
                crate::api::mutation::MutationOp::DeleteRange => {
                    if let Some(end) = m.range_end.as_ref() {
                        crate::wal::WalRecord::new_delete_range(
                            m.cf_id,
                            m.key.clone(),
                            end.clone(),
                            seq,
                        )
                    } else {
                        // Skip if no end provided
                        continue;
                    }
                }
            };

            // Set txn_id
            record.txn_id = Some(txn_id);
            self.wal_coordinator.append_record(&record)?;
        }

        // Write TxnCommit marker
        let commit_seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let commit_rec = crate::wal::WalRecord::new_txn_commit(txn_id, commit_seq);
        self.wal_coordinator.append_record(&commit_rec)?;

        // Apply to MemTable (with per-mutation seqs preserved)
        for (i, m) in mutations.into_iter().enumerate() {
            let s = seqs[i];
            let cf_id = m.cf_id;

            // Skip mutations for dropped column families
            if self.cf_set.cfs.get(&cf_id.as_u32()).is_none() {
                continue;
            }

            match m.op {
                crate::api::mutation::MutationOp::Put
                | crate::api::mutation::MutationOp::Insert => {
                    if let Some(v) = m.value {
                        self.with_cf_memtable_mut(cf_id, |mt| mt.put_with_seq(&m.key, &v, s));
                    }
                }
                crate::api::mutation::MutationOp::CompareAndSwap => {
                    // CAS validation should happen before reaching here
                    // At commit time, we just apply as a regular put
                    if let Some(v) = m.value {
                        self.with_cf_memtable_mut(cf_id, |mt| mt.put_with_seq(&m.key, &v, s));
                    }
                }
                crate::api::mutation::MutationOp::Merge => {
                    // TODO: Implement proper merge semantics with merge operators
                    // For now, treat as a regular put
                    if let Some(v) = m.value {
                        self.with_cf_memtable_mut(cf_id, |mt| mt.put_with_seq(&m.key, &v, s));
                    }
                }
                crate::api::mutation::MutationOp::Delete => {
                    self.with_cf_memtable_mut(cf_id, |mt| mt.delete_with_seq(&m.key, s));
                }
                crate::api::mutation::MutationOp::DeleteRange => {
                    if let Some(end) = m.range_end.as_ref() {
                        self.with_cf_memtable_mut(cf_id, |mt| {
                            mt.delete_range_with_seq(&m.key, end.as_ref(), s)
                        });
                    } else {
                        // If no end provided, treat as no-op for safety
                    }
                }
            }
        }
        // Durability for the batch
        if sync {
            let _ = self.wal_coordinator.sync();
        }
        // OPTIMIZATION: When wal_sync=false, don't flush on every write.
        if self.with_default_memtable(|mt| mt.is_full(self.memtable_size)) {
            let _ = self.flush();
        }
        // No post-append rotation; we rotated before to avoid splitting

        Ok(())
    }

    /// Commit a Transaction by applying its staged mutations to WAL and MemTable.
    ///
    /// The `opts` parameter allows per-transaction control over durability:
    /// - `WriteOptions::sync()` - fsync immediately for strict durability
    /// - `WriteOptions::no_sync()` - defer sync for better performance
    /// - `WriteOptions::default()` - use database-level `wal_sync` setting
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions, WriteOptions};
    /// # use bytes::Bytes;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// // Critical transaction - sync immediately
    /// let mut txn = engine.begin_transaction();
    /// txn.put(Bytes::from("account:1"), Bytes::from("balance:1000"), None);
    /// engine.commit_transaction(txn, WriteOptions::sync()).unwrap();
    ///
    /// // Non-critical transaction - amortize sync cost
    /// let mut txn2 = engine.begin_transaction();
    /// txn2.put(Bytes::from("cache:key"), Bytes::from("value"), None);
    /// engine.commit_transaction(txn2, WriteOptions::no_sync()).unwrap();
    /// ```
    pub fn commit_transaction(
        &self,
        txn: Transaction,
        opts: crate::api::WriteOptions,
    ) -> MidgeResult<()> {
        self.check_read_only()?;

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
        // This ensures each committing transaction has a unique sequence number
        let commit_seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Check for conflicts using transaction manager
        match self.txn_manager.try_commit(txn.txn_id(), commit_seq) {
            Ok(()) => {
                // No conflicts, proceed with commit
                let muts = txn.commit()?;
                self.batch_internal(muts, opts.sync)
            }
            Err(e) => {
                // Conflict detected, abort transaction
                self.txn_manager.abort(txn.txn_id());
                Err(MidgeError::transaction_conflict(e))
            }
        }
    }

    /// Get a value within a transaction's snapshot isolation.
    ///
    /// This method reads the value as it existed at the transaction's begin_sequence,
    /// enforcing snapshot isolation. The read is automatically tracked for conflict detection.
    ///
    /// # Arguments
    ///
    /// * `txn` - Mutable reference to the transaction
    /// * `cf` - Column family handle to read from
    /// * `key` - The key to read
    ///
    /// # Returns
    ///
    /// Returns the value at the transaction's snapshot, or None if the key doesn't exist
    /// or was deleted before the transaction began.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # use bytes::Bytes;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let cf = engine.default_column_family();
    /// let mut txn = engine.begin_transaction();
    ///
    /// // Read with snapshot isolation
    /// if let Some(value) = engine.transaction_get(&mut txn, &cf, b"key").unwrap() {
    ///     println!("Value: {:?}", value);
    /// }
    ///
    /// // Reads are tracked for conflict detection
    /// txn.put(Bytes::from("other_key"), Bytes::from("value"), None).unwrap();
    /// engine.commit_transaction(txn, cntryl_midge::WriteOptions::default()).unwrap();
    /// ```
    pub fn transaction_get(
        &self,
        txn: &mut Transaction,
        cf: &ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<Option<Bytes>> {
        let cf_id = cf.id();

        // First check transaction's local staged mutations
        if let Some(local_result) = txn.get_local(cf_id.as_u32(), key) {
            // Track the read at current sequence (local reads see latest)
            let seq = self.seq.load(Ordering::SeqCst);
            txn.track_read(cf_id.as_u32(), Bytes::copy_from_slice(key), seq);
            return Ok(local_result);
        }

        let begin_seq = txn.begin_sequence();

        // Check MemTable with snapshot isolation
        if let Some(result) = self.with_cf_memtable(cf_id, |mt| mt.get_at(key, begin_seq)) {
            match result {
                Some(v) => {
                    txn.track_read(cf_id.as_u32(), Bytes::copy_from_slice(key), begin_seq);
                    return Ok(Some(v));
                }
                None => {
                    // Explicit tombstone within snapshot
                    txn.track_read(cf_id.as_u32(), Bytes::copy_from_slice(key), begin_seq);
                    return Ok(None);
                }
            }
        }

        // Check SSTs with snapshot isolation
        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        for sst_name in manifest.ssts.iter().rev() {
            {
                let sst_path = self.sst_dir.join(sst_name);
                match self.sst_reader_factory.open(&sst_path) {
                    Ok(reader) => match reader.get_state_at(key, begin_seq) {
                        Ok(crate::sst::KeyState::Value(v, seq, exp)) => {
                            // Check expiration
                            if let Some(exp_ms) = exp {
                                let now = timestamp::now_millis();
                                if now >= exp_ms {
                                    txn.track_read(
                                        cf_id.as_u32(),
                                        Bytes::copy_from_slice(key),
                                        seq,
                                    );
                                    return Ok(None);
                                }
                            }
                            txn.track_read(cf_id.as_u32(), Bytes::copy_from_slice(key), seq);
                            return Ok(Some(v));
                        }
                        Ok(crate::sst::KeyState::Tombstone(seq)) => {
                            txn.track_read(cf_id.as_u32(), Bytes::copy_from_slice(key), seq);
                            return Ok(None);
                        }
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                    Err(_) => continue,
                }
            }
        }

        // Not found anywhere
        txn.track_read(cf_id.as_u32(), Bytes::copy_from_slice(key), begin_seq);
        Ok(None)
    }

    /// Check if a key exists within a transaction's snapshot isolation.
    ///
    /// This is equivalent to `transaction_get()` but only returns whether the key exists.
    pub fn transaction_exists(
        &self,
        txn: &mut Transaction,
        cf: &ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<bool> {
        self.transaction_get(txn, cf, key).map(|opt| opt.is_some())
    }
}
