//! Transaction API for multi-key ACID operations
//!
//! Provides transaction support with:
//! - Multi-key atomic operations
//! - Snapshot isolation with repeatable reads
//! - Rollback and commit semantics
//! - Column-family scoped transactions

use crate::common::{MidgeError, MidgeResult};
use crate::engine::ColumnFamilyId;
use crate::runtime::{next_request_id, RuntimeHandle, RuntimeMsg, RuntimeResponse};
use std::collections::BTreeMap;
use std::sync::Arc;

/// Transaction mode controls read/write capabilities
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only transaction; writes forbidden
    ReadOnly,
    /// Read-write transaction; all operations allowed
    ReadWrite,
}

/// Pending write intent collected within a transaction.
#[derive(Debug, Clone)]
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
    /// Delete range operation
    DeleteRange {
        cf_id: ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl WriteIntent {
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
    /// Unique transaction ID
    id: u64,
    /// Column family this transaction is bound to
    cf_id: ColumnFamilyId,
    /// Transaction mode (ReadOnly or ReadWrite)
    mode: TransactionMode,
    /// Write set: sequence of write intents
    write_set: Vec<WriteIntent>,
    /// Start sequence number (snapshot point)
    start_sequence: u64,
    /// Immutable snapshot for direct read execution (bypasses event loop)
    read_snapshot: Option<Arc<crate::runtime::ReadSnapshot>>,
}

impl Transaction {
    /// Create a new transaction with the given ID and mode.
    pub(crate) fn new(
        runtime_handle: RuntimeHandle,
        id: u64,
        cf_id: ColumnFamilyId,
        mode: TransactionMode,
        start_sequence: u64,
        read_snapshot: Option<Arc<crate::runtime::ReadSnapshot>>,
    ) -> Self {
        Self {
            runtime_handle,
            id,
            cf_id,
            mode,
            write_set: Vec::new(),
            start_sequence,
            read_snapshot,
        }
    }

    /// Add a put (upsert) to the transaction's write set
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
    pub fn delete(&mut self, key: Vec<u8>) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        self.write_set.push(WriteIntent::delete(self.cf_id, key));
        Ok(())
    }

    /// Add a delete_range to the transaction's write set
    pub fn delete_range(&mut self, start_key: Vec<u8>, end_key: Vec<u8>) -> MidgeResult<()> {
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }

        // Add single delete_range intent - commit will handle it efficiently
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

    pub(crate) fn iter_writes(&self) -> impl Iterator<Item = &WriteIntent> {
        self.write_set.iter()
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub fn start_sequence(&self) -> u64 {
        self.start_sequence
    }

    /// Get a value within this transaction (read-your-own-writes semantics)
    ///
    /// This is the primary way to read data. Checks the transaction's write set first,
    /// then falls back to the engine state at the transaction's snapshot sequence.
    ///
    /// Executes directly against the immutable snapshot (no message passing).
    pub fn get(&self, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        // Check transaction's write set first (read-your-own-writes)
        if let Some(value_opt) = self.get_from_write_set(key) {
            return Ok(value_opt.map(bytes::Bytes::from));
        }

        // Execute directly against snapshot (bypasses event loop)
        if let Some(snapshot) = &self.read_snapshot {
            let value = snapshot.get(key, self.start_sequence);
            return Ok(value.map(bytes::Bytes::from));
        }

        // Fallback: legacy message passing (should not happen in normal operation)
        let response = self.runtime_handle.send_and_wait(RuntimeMsg::Read {
            request_id: next_request_id(),
            cf_id: self.cf_id,
            key: key.to_vec(),
            sequence: self.start_sequence,
            requested_durability: super::Durability::Steady,
        })?;

        match response {
            RuntimeResponse::ReadValue { value, .. } => Ok(value.map(bytes::Bytes::from)),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to Transaction::get".to_string(),
            )),
        }
    }

    /// Range scan within this transaction
    ///
    /// Returns an iterator over all key-value pairs matching the query,
    /// visible at this transaction's snapshot sequence.
    ///
    /// Query must explicitly specify all scan parameters (start, end, direction, limit).
    /// Executes directly against the immutable snapshot (no message passing).
    pub fn scan(&self, query: &super::query::Query) -> MidgeResult<super::iterator::Iterator> {
        let start = query.effective_start().unwrap_or(&[]);
        let end_vec = query.effective_end().unwrap_or_default();
        let end = if end_vec.is_empty() {
            &[][..]
        } else {
            &end_vec[..]
        };

        // Execute directly against snapshot (bypasses event loop)
        let base_results = if let Some(snapshot) = &self.read_snapshot {
            snapshot
                .range_scan(start, end, self.start_sequence)
                .into_iter()
                .map(|(k, v)| (bytes::Bytes::from(k), bytes::Bytes::from(v)))
                .collect::<Vec<_>>()
        } else {
            // Fallback: legacy message passing (should not happen in normal operation)
            let response = self.runtime_handle.send_and_wait(RuntimeMsg::RangeScan {
                request_id: next_request_id(),
                cf_id: self.cf_id,
                start: start.to_vec(),
                end: end.to_vec(),
                sequence: self.start_sequence,
                requested_durability: super::Durability::Steady,
            })?;

            match response {
                RuntimeResponse::RangeScanResults { results, .. } => results
                    .into_iter()
                    .map(|(k, v)| (bytes::Bytes::from(k), bytes::Bytes::from(v)))
                    .collect::<Vec<_>>(),
                RuntimeResponse::Error { error, .. } => return Err(error),
                _ => {
                    return Err(MidgeError::Internal(
                        "Unexpected response to Transaction::scan".to_string(),
                    ))
                }
            }
        };

        let mut merged: BTreeMap<Vec<u8>, Option<bytes::Bytes>> = BTreeMap::new();

        for (key, value) in base_results {
            merged.insert(key.to_vec(), Some(value));
        }

        for intent in self.write_set.iter() {
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
                    // Apply delete-range tombstone: mark all keys within [start_key, end_key) as deleted
                    // Collect keys to avoid mutating map while iterating
                    let keys_to_tombstone: Vec<Vec<u8>> = merged
                        .range(start_key.clone()..end_key.clone())
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in keys_to_tombstone {
                        merged.insert(k, None);
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

    /// Commit this transaction atomically.
    ///
    /// Durability is explicit and ONLY happens here.
    pub fn commit(self, opts: super::WriteOptions) -> MidgeResult<()> {
        // ReadOnly transactions are a no-op for commit.
        if self.is_read_only() {
            return Ok(());
        }

        if opts.is_best_effort() {
            return Err(MidgeError::InvalidArgument(
                "BestEffort is not allowed for Transaction::commit".to_string(),
            ));
        }

        if self.write_set.is_empty() {
            return Ok(());
        }

        let ops = self
            .write_set
            .into_iter()
            .map(|intent| -> MidgeResult<crate::runtime::TransactionOp> {
                Ok(match intent {
                    WriteIntent::Put {
                        key,
                        value,
                        ttl_seconds,
                        ..
                    } => crate::runtime::TransactionOp::Put {
                        cf_id: self.cf_id,
                        key: bytes::Bytes::from(key),
                        value: bytes::Bytes::from(value),
                        ttl_seconds,
                        insert_only: false,
                    },
                    WriteIntent::Insert {
                        key,
                        value,
                        ttl_seconds,
                        ..
                    } => crate::runtime::TransactionOp::Put {
                        cf_id: self.cf_id,
                        key: bytes::Bytes::from(key),
                        value: bytes::Bytes::from(value),
                        ttl_seconds,
                        insert_only: true,
                    },
                    WriteIntent::Delete { key, .. } => crate::runtime::TransactionOp::Delete {
                        cf_id: self.cf_id,
                        key: bytes::Bytes::from(key),
                    },
                    WriteIntent::DeleteRange { .. } => {
                        return Err(MidgeError::InvalidArgument(
                            "DeleteRange not supported in ApplyTransaction".to_string(),
                        ));
                    }
                })
            })
            .collect::<MidgeResult<Vec<_>>>()?;

        let response = self
            .runtime_handle
            .send_and_wait(RuntimeMsg::ApplyTransaction {
                request_id: next_request_id(),
                ops,
            })?;

        match response {
            RuntimeResponse::TransactionApplied { .. } => {
                if opts.is_sync() {
                    let sync_resp = self.runtime_handle.send_and_wait(RuntimeMsg::WalSync {
                        request_id: next_request_id(),
                    })?;
                    match sync_resp {
                        RuntimeResponse::Ok { .. } => {}
                        RuntimeResponse::Error { error, .. } => {
                            return Err(error);
                        }
                        _ => {
                            return Err(MidgeError::Internal(
                                "Unexpected response to RuntimeMsg::WalSync".to_string(),
                            ));
                        }
                    }
                }

                Ok(())
            }
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to Transaction::commit".to_string(),
            )),
        }
    }

    fn get_from_write_set(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        for intent in self.write_set.iter().rev() {
            match intent {
                WriteIntent::Put { key: k, value, .. }
                | WriteIntent::Insert { key: k, value, .. }
                    if k.as_slice() == key =>
                {
                    return Some(Some(value.clone()));
                }
                WriteIntent::Delete { key: k, .. } if k.as_slice() == key => return Some(None),
                _ => {}
            }
        }

        None
    }
}
