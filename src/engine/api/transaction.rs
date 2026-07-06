//! Transaction API for multi-key ACID operations
//!
//! Provides transaction support with:
//! - Multi-key atomic operations
//! - Snapshot isolation with repeatable reads
//! - Rollback and commit semantics
//! - Column-family scoped transactions

use crate::common::{MidgeError, MidgeResult};
use crate::engine::api::write_options::effective_wal_durability_policy;
use crate::engine::ingest::IngestCoordinator;
use crate::engine::ColumnFamilyId;
use crate::runtime::RuntimeHandle;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Transaction mode controls read/write capabilities
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only transaction; writes forbidden
    ReadOnly,
    /// Read-write transaction; point writes are allowed
    ReadWrite,
}

/// Transaction isolation policy for read-write commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Current behavior: concurrent write conflicts resolve by commit order.
    LastWriteWins,
    /// Abort commit when a write-set key changed after transaction start snapshot.
    AbortOnWriteConflict,
}

/// Pending write intent collected within a transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteIntent {
    /// Put operation (upsert)
    Put {
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    },
    /// Insert operation (error if exists)
    Insert {
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    },
    /// Delete operation
    Delete { cf_id: ColumnFamilyId, key: Vec<u8> },
    /// Delete range operation (atomic range tombstone)
    DeleteRange {
        cf_id: ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl WriteIntent {
    pub(crate) fn into_runtime_op(self) -> crate::runtime::TransactionOp {
        match self {
            Self::Put {
                cf_id,
                key,
                value,
                ttl_seconds,
            } => crate::runtime::TransactionOp::Put {
                cf_id,
                key: bytes::Bytes::from(key),
                value: bytes::Bytes::from(value),
                ttl_seconds,
                insert_only: false,
            },
            Self::Insert {
                cf_id,
                key,
                value,
                ttl_seconds,
            } => crate::runtime::TransactionOp::Put {
                cf_id,
                key: bytes::Bytes::from(key),
                value: bytes::Bytes::from(value),
                ttl_seconds,
                insert_only: true,
            },
            Self::Delete { cf_id, key } => crate::runtime::TransactionOp::Delete {
                cf_id,
                key: bytes::Bytes::from(key),
            },
            Self::DeleteRange {
                cf_id,
                start_key,
                end_key,
            } => crate::runtime::TransactionOp::DeleteRange {
                cf_id,
                start_key: bytes::Bytes::from(start_key),
                end_key: bytes::Bytes::from(end_key),
            },
        }
    }

    /// Create a put intent (upsert)
    fn put(cf_id: ColumnFamilyId, key: Vec<u8>, value: Vec<u8>, ttl_seconds: Option<u64>) -> Self {
        Self::Put {
            cf_id,
            key,
            value,
            ttl_seconds,
        }
    }

    /// Create an insert intent (error if exists)
    fn insert(
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    ) -> Self {
        Self::Insert {
            cf_id,
            key,
            value,
            ttl_seconds,
        }
    }

    /// Create a delete intent
    fn delete(cf_id: ColumnFamilyId, key: Vec<u8>) -> Self {
        Self::Delete { cf_id, key }
    }

    /// Create a delete range intent
    fn delete_range(cf_id: ColumnFamilyId, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        Self::DeleteRange {
            cf_id,
            start_key,
            end_key,
        }
    }
}

/// Transaction for multi-key ACID operations
///
/// Collects read and write intents, validates them, and commits atomically.
/// Thread-safe; multiple transactions can be in flight simultaneously.
pub struct Transaction {
    runtime_handle: RuntimeHandle,
    coordinator: Arc<IngestCoordinator>,
    sequence_publisher: Arc<AtomicU64>,
    /// Unique transaction ID
    id: u64,
    /// Column family this transaction is bound to
    cf_id: ColumnFamilyId,
    /// Transaction mode (`ReadOnly` or `ReadWrite`)
    mode: TransactionMode,
    /// Isolation behavior used during commit conflict handling.
    isolation_level: IsolationLevel,
    /// Write set: sequence of write intents
    write_set: Vec<WriteIntent>,
    /// Start sequence number (snapshot point)
    start_sequence: u64,
    /// Immutable snapshot for direct read execution (bypasses event loop)
    read_snapshot: Option<Arc<crate::runtime::ReadSnapshot>>,
    /// True when opened against cloud-backed storage.
    cloud_mode: bool,
    /// Whether this transaction is currently registered as an active snapshot.
    snapshot_registered: bool,
}

enum WriteSetLookup {
    Present(Vec<u8>),
    Deleted,
}

struct CommitTiming {
    started_at: Instant,
    sample: crate::diagnostics::TransactionCommitTimingSample,
}

impl CommitTiming {
    fn maybe_start() -> Option<Self> {
        crate::diagnostics::transaction_commit_timing_enabled().then(|| {
            crate::diagnostics::clear_current_transaction_submit_timing();
            Self {
                started_at: Instant::now(),
                sample: crate::diagnostics::TransactionCommitTimingSample::default(),
            }
        })
    }

    fn phase_start(timing: Option<&Self>) -> Option<Instant> {
        timing.map(|_| Instant::now())
    }

    fn record_submit(timing: &mut Option<Self>, started_at: Option<Instant>) {
        if let (Some(timing), Some(started_at)) = (timing.as_mut(), started_at) {
            let submit_timing = crate::diagnostics::take_current_transaction_submit_timing();
            timing.sample.submit_apply_transaction_ns = duration_as_nanos(started_at.elapsed());
            timing.sample.write_group_leader_collect_ns = submit_timing.leader_collect;
            timing.sample.write_group_runtime_apply_ns = submit_timing.runtime_apply;
            timing.sample.write_group_follower_wait_ns = submit_timing.follower_wait;
        }
    }

    fn record_durability(timing: &mut Option<Self>, started_at: Option<Instant>) {
        if let (Some(timing), Some(started_at)) = (timing.as_mut(), started_at) {
            timing.sample.durability_finalize_ns = duration_as_nanos(started_at.elapsed());
        }
    }

    fn record_unregister(timing: &mut Option<Self>, started_at: Option<Instant>) {
        if let (Some(timing), Some(started_at)) = (timing.as_mut(), started_at) {
            timing.sample.unregister_snapshot_ns = duration_as_nanos(started_at.elapsed());
        }
    }

    fn finish(timing: Option<Self>, succeeded: bool) {
        if let Some(mut timing) = timing {
            timing.sample.commit_total_ns = duration_as_nanos(timing.started_at.elapsed());
            timing.sample.succeeded = succeeded;
            crate::diagnostics::record_transaction_commit_timing(timing.sample);
        }
    }
}

fn duration_as_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) struct TransactionInit {
    pub(crate) runtime_handle: RuntimeHandle,
    pub(crate) coordinator: Arc<IngestCoordinator>,
    pub(crate) sequence_publisher: Arc<AtomicU64>,
    pub(crate) id: u64,
    pub(crate) cf_id: ColumnFamilyId,
    pub(crate) mode: TransactionMode,
    pub(crate) start_sequence: u64,
    pub(crate) read_snapshot: Option<Arc<crate::runtime::ReadSnapshot>>,
    pub(crate) cloud_mode: bool,
}

impl Transaction {
    /// Create a new transaction with the given ID and mode.
    pub(crate) fn new(init: TransactionInit) -> Self {
        Self {
            runtime_handle: init.runtime_handle,
            coordinator: init.coordinator,
            sequence_publisher: init.sequence_publisher,
            id: init.id,
            cf_id: init.cf_id,
            mode: init.mode,
            isolation_level: IsolationLevel::LastWriteWins,
            write_set: Vec::new(),
            start_sequence: init.start_sequence,
            read_snapshot: init.read_snapshot,
            cloud_mode: init.cloud_mode,
            snapshot_registered: true,
        }
    }

    /// Add a put (upsert) to the transaction's write set
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only transaction.
    pub fn put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        self.write_set
            .push(WriteIntent::put(self.cf_id, key, value, ttl_seconds));
        Ok(())
    }

    /// Add an insert (error if exists) to the transaction's write set
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only transaction.
    pub fn insert(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        self.write_set
            .push(WriteIntent::insert(self.cf_id, key, value, ttl_seconds));
        Ok(())
    }

    /// Add a delete to the transaction's write set
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only transaction.
    pub fn delete(&mut self, key: Vec<u8>) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        self.write_set.push(WriteIntent::delete(self.cf_id, key));
        Ok(())
    }

    /// Add a delete range (atomic tombstone) to the transaction's write set
    ///
    /// Deletes all keys in the range [`start_key`, `end_key`) atomically as part of
    /// the transaction commit. This is atomic with any puts/deletes in the same transaction.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::InvalidArgument` when called on a read-only transaction
    /// or when `start_key > end_key`.
    pub fn delete_range(&mut self, start_key: Vec<u8>, end_key: Vec<u8>) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        if start_key.as_slice() > end_key.as_slice() {
            return Err(MidgeError::InvalidArgument(
                "delete_range requires start_key <= end_key".to_string(),
            ));
        }
        self.write_set
            .push(WriteIntent::delete_range(self.cf_id, start_key, end_key));
        Ok(())
    }

    // === Internal helpers for engine/mod.rs commit logic ===

    pub(crate) fn is_read_only(&self) -> bool {
        matches!(self.mode, TransactionMode::ReadOnly)
    }

    pub(crate) fn has_writes(&self) -> bool {
        !self.write_set.is_empty()
    }

    pub(crate) fn cf_id(&self) -> ColumnFamilyId {
        self.cf_id
    }

    #[must_use]
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    pub fn set_isolation_level(&mut self, isolation_level: IsolationLevel) {
        self.isolation_level = isolation_level;
    }

    /// Commit this transaction with the provided durability options.
    ///
    /// # Errors
    ///
    /// Returns an error when commit coordination, WAL durability, or cloud durability
    /// confirmation fails.
    pub fn commit(mut self, opts: crate::engine::api::WriteOptions) -> MidgeResult<()> {
        let mut timing = CommitTiming::maybe_start();

        if self.is_read_only() {
            let unregister_started_at = CommitTiming::phase_start(timing.as_ref());
            self.unregister_snapshot();
            CommitTiming::record_unregister(&mut timing, unregister_started_at);
            CommitTiming::finish(timing, true);
            return Ok(());
        }

        if !self.has_writes() {
            let durability_started_at = CommitTiming::phase_start(timing.as_ref());
            let sync_result = if opts.is_sync() { self.sync() } else { Ok(()) };
            CommitTiming::record_durability(&mut timing, durability_started_at);
            let unregister_started_at = CommitTiming::phase_start(timing.as_ref());
            self.unregister_snapshot();
            CommitTiming::record_unregister(&mut timing, unregister_started_at);
            CommitTiming::finish(timing, sync_result.is_ok());
            return sync_result;
        }

        let runtime_ops = self.take_runtime_ops();
        let isolation_policy = match self.isolation_level() {
            IsolationLevel::LastWriteWins => {
                crate::runtime::TransactionIsolationPolicy::LastWriteWins
            }
            IsolationLevel::AbortOnWriteConflict => {
                crate::runtime::TransactionIsolationPolicy::AbortOnWriteConflict
            }
        };

        let durability_policy = Some(effective_wal_durability_policy(self.cloud_mode, opts)?);
        let submit_started_at = CommitTiming::phase_start(timing.as_ref());
        let collect_submit_timing = timing.is_some();
        let commit_result = self.coordinator.submit_ops(
            &self.runtime_handle,
            runtime_ops,
            durability_policy,
            Some(self.start_sequence()),
            isolation_policy,
            collect_submit_timing,
        );
        CommitTiming::record_submit(&mut timing, submit_started_at);

        let result = match commit_result {
            Ok(sequence) => {
                self.sequence_publisher.store(sequence, Ordering::SeqCst);
                let durability_started_at = CommitTiming::phase_start(timing.as_ref());
                let result = self.finalize_write_durability(sequence, opts);
                CommitTiming::record_durability(&mut timing, durability_started_at);
                result
            }
            Err(error) => Err(error),
        };

        let unregister_started_at = CommitTiming::phase_start(timing.as_ref());
        self.unregister_snapshot();
        CommitTiming::record_unregister(&mut timing, unregister_started_at);
        CommitTiming::finish(timing, result.is_ok());
        result
    }

    /// Roll back this transaction and unregister its snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if snapshot cleanup fails.
    pub fn rollback(mut self) -> MidgeResult<()> {
        self.unregister_snapshot();
        Ok(())
    }

    pub(crate) fn take_runtime_ops(&mut self) -> Vec<crate::runtime::TransactionOp> {
        std::mem::take(&mut self.write_set)
            .into_iter()
            .map(WriteIntent::into_runtime_op)
            .collect()
    }

    pub(crate) fn unregister_snapshot(&mut self) {
        if !self.snapshot_registered {
            return;
        }

        let _ = self.runtime_handle.unregister_snapshot_pin(self.id);
        crate::diagnostics::record_snapshot_unregister();
        self.snapshot_registered = false;
    }

    #[must_use]
    pub fn start_sequence(&self) -> u64 {
        self.start_sequence
    }

    fn sync(&self) -> MidgeResult<()> {
        let response = self
            .runtime_handle
            .send_and_wait(crate::runtime::RuntimeMsg::WalSync {
                request_id: crate::runtime::next_request_id()?,
            })?;

        match response {
            crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
            crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to sync".to_string(),
            )),
        }
    }

    fn finalize_write_durability(
        &self,
        sequence: u64,
        opts: crate::engine::api::WriteOptions,
    ) -> MidgeResult<()> {
        if self.cloud_mode {
            if opts.is_sync() || opts.is_cloud_strict() {
                let response = self.runtime_handle.send_and_wait(
                    crate::runtime::RuntimeMsg::SealWalForCloud {
                        request_id: crate::runtime::next_request_id()?,
                        sequence,
                        wait_for_ack: opts.is_cloud_strict(),
                    },
                )?;

                return match response {
                    crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
                    crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
                    _ => Err(MidgeError::Internal(
                        "Unexpected response to SealWalForCloud".to_string(),
                    )),
                };
            }

            return Ok(());
        }

        if opts.is_sync() {
            self.sync()?;
        }

        Ok(())
    }

    /// Get a value within this transaction (read-your-own-writes semantics)
    ///
    /// This is the primary way to read data. Checks the transaction's write set first,
    /// then falls back to the engine state at the transaction's snapshot sequence.
    ///
    /// Executes directly against the immutable snapshot (no message passing).
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction snapshot is unavailable.
    pub fn get(&self, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        // Check transaction's write set first (read-your-own-writes)
        if let Some(value) = self.get_from_write_set(key) {
            return Ok(match value {
                WriteSetLookup::Present(bytes) => Some(bytes::Bytes::from(bytes)),
                WriteSetLookup::Deleted => None,
            });
        }

        // Execute directly against snapshot (bypasses event loop)
        let snapshot = self.read_snapshot.as_ref().ok_or_else(|| {
            MidgeError::Internal("read snapshot not available - this is a bug".to_string())
        })?;
        let value = snapshot.get(key, self.start_sequence);
        Ok(value.map(bytes::Bytes::from))
    }

    /// Range scan within this transaction
    ///
    /// Returns an iterator over all key-value pairs matching the query,
    /// visible at this transaction's snapshot sequence.
    ///
    /// Query must explicitly specify all scan parameters (start, end, direction, limit).
    /// Executes directly against the immutable snapshot (no message passing).
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction snapshot is unavailable.
    pub fn scan(&self, query: &super::query::Query) -> MidgeResult<super::iterator::Iterator> {
        let start = query.effective_start().unwrap_or(&[]);
        let end_vec = query.effective_end().unwrap_or_default();
        let end = if end_vec.is_empty() {
            &[][..]
        } else {
            &end_vec[..]
        };

        // Execute directly against snapshot (bypasses event loop)
        let snapshot = self.read_snapshot.as_ref().ok_or_else(|| {
            MidgeError::Internal("read snapshot not available - this is a bug".to_string())
        })?;
        let base_results = snapshot
            .range_scan(start, end, self.start_sequence)
            .into_iter()
            .map(|(k, v)| (bytes::Bytes::from(k), bytes::Bytes::from(v)))
            .collect::<Vec<_>>();

        let mut merged: BTreeMap<Vec<u8>, Option<bytes::Bytes>> = BTreeMap::new();

        for (key, value) in base_results {
            merged.insert(key.to_vec(), Some(value));
        }

        for intent in &self.write_set {
            match intent {
                WriteIntent::Put { key, value, .. } | WriteIntent::Insert { key, value, .. } => {
                    merged.insert(key.clone(), Some(bytes::Bytes::from(value.clone())));
                }
                WriteIntent::Delete { key, .. } => {
                    merged.insert(key.clone(), None);
                }
                WriteIntent::DeleteRange {
                    start_key, end_key, ..
                } => {
                    // Mark all keys in [start_key, end_key) as deleted
                    let mut to_delete = Vec::new();
                    for (key, _) in merged.range(start_key.clone()..end_key.clone()) {
                        to_delete.push(key.clone());
                    }
                    for key in to_delete {
                        merged.insert(key, None);
                    }
                }
            }
        }

        let mut results = Vec::new();
        for (key, value_opt) in merged {
            if let Some(value) = value_opt {
                results.push((key, value.to_vec()));
            }
        }

        // Apply limit (respect direction semantics: for Reverse keep the last N elements)
        if let Some(limit) = query.limit {
            if query.direction == super::iterator::Direction::Forward {
                if results.len() > limit {
                    results.truncate(limit);
                }
            } else if results.len() > limit {
                let start = results.len() - limit;
                results = results[start..].to_vec();
            }
        }

        // Apply direction
        let iter = match query.direction {
            super::iterator::Direction::Forward => super::iterator::Iterator::forward(results),
            super::iterator::Direction::Reverse => super::iterator::Iterator::reverse(results),
        };

        Ok(iter)
    }

    fn get_from_write_set(&self, key: &[u8]) -> Option<WriteSetLookup> {
        for intent in self.write_set.iter().rev() {
            match intent {
                WriteIntent::Put { key: k, value, .. }
                | WriteIntent::Insert { key: k, value, .. }
                    if k.as_slice() == key =>
                {
                    return Some(WriteSetLookup::Present(value.clone()));
                }
                WriteIntent::Delete { key: k, .. } if k.as_slice() == key => {
                    return Some(WriteSetLookup::Deleted);
                }
                WriteIntent::DeleteRange {
                    start_key, end_key, ..
                } if key >= start_key.as_slice() && key < end_key.as_slice() => {
                    return Some(WriteSetLookup::Deleted);
                }
                _ => {}
            }
        }

        None
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.unregister_snapshot();
    }
}
