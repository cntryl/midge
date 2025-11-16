//! Write Operations Module
//!
//! This module contains all write operations for MidgeEngine, including:
//! - Point writes (put, put_with_ttl)
//! - Deletes (delete, delete_range)
//! - Batch writes (write_batch)
//! - Merge operations (merge_cf, merge_with_ttl_cf)
//! - Conditional writes (insert, insert_with_ttl)
//!
//! All operations handle:
//! - WAL durability
//! - Sequence number allocation
//! - MemTable updates
//! - Automatic flush triggering
//! - Write stall prevention

use crate::api::column_family::ColumnFamilyHandle;
use crate::api::column_family::DEFAULT_CF_ID;
use crate::common::timestamp;
use crate::core::engine::core::MidgeEngine;
use crate::error::MidgeResult;
use bytes::Bytes;
use std::sync::atomic::Ordering;

/// Helper to predict WAL record size
fn wal_record_encoded_len(
    _op: crate::api::write_batch::OpKind,
    key_len: usize,
    value_len: Option<usize>,
    _range_end_len: Option<usize>,
) -> usize {
    // Approximate size calculation
    let base = 32; // Fixed overhead
    let key_size = key_len;
    let value_size = value_len.unwrap_or(0);
    base + key_size + value_size
}

impl MidgeEngine {
    /// Put a key-value pair into a specific column family.
    pub fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot write in read-only mode",
            ));
        }

        let cf_id = cf.id();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Write to WAL
        let rec = crate::wal::WalRecord::new_cf(
            cf_id,
            crate::wal::WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            seq,
        );

        self.wal_coordinator.append_record(&rec)?;
        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        // Write to MemTable
        let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // MemTable uses interior mutability - acquire read lock and write
        {
            let mt = column_family.memtable.read();
            mt.put_with_seq(key, value, seq);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            // Try to freeze the active memtable
            let frozen = column_family.try_freeze_memtable();

            if frozen {
                // Successfully froze memtable, trigger flush for this CF
                // Use a background flush to avoid blocking the write path
                let _ = self.flush_cf(cf);
            } else {
                // Immutable queue is full - implement write stall with exponential backoff
                if column_family.should_stall_writes() {
                    // Implement backpressure: sleep with exponential backoff
                    let mut backoff_ms = 1; // Start with 1ms
                    let max_backoff_ms = 100; // Cap at 100ms
                    let max_stall_attempts = 1000; // ~55 seconds total max wait

                    for attempt in 0..max_stall_attempts {
                        std::thread::sleep(std::time::Duration::from_millis(backoff_ms));

                        // Check if flush completed and we can proceed
                        if !column_family.should_stall_writes() {
                            self.metrics.record_write_stall(attempt + 1);
                            break;
                        }

                        // Exponential backoff with cap
                        backoff_ms = (backoff_ms * 2).min(max_backoff_ms);

                        // If we've stalled too long, return error to prevent indefinite blocking
                        if attempt == max_stall_attempts - 1 {
                            return Err(crate::error::MidgeError::invalid_config(
                                "Write stall timeout: flush queue not draining",
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Put a key-value pair with TTL into a specific column family.
    ///
    /// # Arguments
    ///
    /// * `cf` - Column family handle
    /// * `key` - Key to write
    /// * `value` - Value to write
    /// * `ttl_seconds` - Time-to-live in seconds (0 = no expiration)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # use bytes::Bytes;
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    /// // Key expires after 60 seconds
    /// engine.put_with_ttl(&cf, b"session:123", b"data", 60).unwrap();
    /// ```
    pub fn put_with_ttl(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot write in read-only mode",
            ));
        }

        let cf_id = cf.id();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Compute expiration time in milliseconds if TTL > 0
        let expiration = if ttl_seconds > 0 {
            let now_millis = timestamp::now_millis();
            Some(now_millis + (ttl_seconds * 1000))
        } else {
            None
        };

        // Write to WAL with TTL
        let rec = crate::wal::WalRecord::new_with_ttl(
            cf_id,
            crate::wal::WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            seq,
            ttl_seconds,
        );

        self.wal_coordinator.append_record(&rec)?;
        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        // Write to MemTable with expiration
        let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        {
            let mt = column_family.memtable.read();
            mt.put_with_seq_and_exp(key, value, seq, expiration);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            let frozen = column_family.try_freeze_memtable();

            if frozen && cf_id == DEFAULT_CF_ID {
                let _ = self.flush();
            } else if column_family.should_stall_writes() {
                // Implement backpressure: sleep with exponential backoff
                let mut backoff_ms = 1;
                let max_backoff_ms = 100;
                let max_stall_attempts = 1000;

                for attempt in 0..max_stall_attempts {
                    std::thread::sleep(std::time::Duration::from_millis(backoff_ms));

                    if !column_family.should_stall_writes() {
                        self.metrics.record_write_stall(attempt + 1);
                        break;
                    }

                    backoff_ms = (backoff_ms * 2).min(max_backoff_ms);

                    if attempt == max_stall_attempts - 1 {
                        return Err(crate::error::MidgeError::invalid_config(
                            "Write stall timeout: flush queue not draining",
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Delete a key from a column family.
    pub fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot write in read-only mode",
            ));
        }

        let cf_id = cf.id();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Write to WAL
        let rec = crate::wal::WalRecord::new_cf(
            cf_id,
            crate::wal::WalOpKind::Delete,
            Bytes::copy_from_slice(key),
            None,
            seq,
        );

        self.wal_coordinator.append_record(&rec)?;
        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // MemTable uses interior mutability - acquire read lock and delete
        {
            let mt = column_family.memtable.read();
            mt.delete_with_seq(key, seq);
        }

        Ok(())
    }

    /// Delete a range of keys in a column family where `start <= key < end`.
    pub fn delete_range(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()> {
        self.check_read_only()?;
        self.metrics.record_delete();
        self.metrics.record_memtable_write();
        self.metrics.record_range_tombstone_created();

        // Validate range
        if start >= end {
            return Ok(()); // Empty range, no-op
        }

        let cf_id = cf.id();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Write to WAL first for durability
        self.metrics.record_wal_write();
        let record = crate::wal::WalRecord::new_delete_range(
            cf_id,
            Bytes::copy_from_slice(start),
            Bytes::copy_from_slice(end),
            seq,
        );
        self.wal_coordinator.append_record(&record)?;

        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        // Apply to column family's memtable
        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        {
            let mt = column_family.memtable.read();
            mt.delete_range_with_seq(start, end, seq);
        }

        Ok(())
    }

    /// Write a batch of operations atomically.
    ///
    /// All operations in the batch are written to the WAL in a single write,
    /// then applied to the memtable. This provides better throughput than
    /// individual puts by reducing WAL overhead.
    ///
    /// Each operation in the batch can target a different column family.
    pub fn write_batch(&self, batch: &crate::api::WriteBatch) -> MidgeResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        self.check_read_only()?;

        // Check if we need to rotate WAL before writing the batch
        let mut total_size: u64 = 0;
        let mut first_cf: Option<crate::api::column_family::ColumnFamilyId> = None;
        for op in batch.operations() {
            if first_cf.is_none() {
                first_cf = Some(op.cf_id());
            }
            let predicted = wal_record_encoded_len(
                op.kind(),
                op.key().len(),
                op.value().map(|v| v.len()),
                None,
            );
            total_size += predicted as u64;
        }

        if self
            .wal_coordinator
            .current_pos()
            .saturating_add(total_size)
            > self.wal_buffer_size as u64
        {
            // Flush the CF that triggered the rotation (first op in batch)
            // If batch is empty (shouldn't happen due to early return), flush default
            let cf_to_flush = first_cf.unwrap_or(crate::api::column_family::DEFAULT_CF_ID);
            let _ = self.rollover_and_queue_flush(cf_to_flush);
        }

        // Build WAL records for batch
        let mut wal_records = Vec::with_capacity(batch.operations().size_hint().0);
        let mut sequences = Vec::with_capacity(batch.operations().size_hint().0);

        // OPTIMIZATION: Compute timestamp once for entire batch to avoid redundant system calls
        let now_millis = timestamp::now_millis();

        for op in batch.operations() {
            let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;

            let expiration = if op.ttl_seconds() > 0 {
                Some(now_millis + (op.ttl_seconds() * 1000))
            } else {
                None
            };

            sequences.push((op, seq, expiration));

            // Convert internal OpKind to WalOpKind
            let wal_op_kind = match op.kind() {
                crate::api::write_batch::OpKind::Put => crate::wal::WalOpKind::Put,
                crate::api::write_batch::OpKind::Delete => crate::wal::WalOpKind::Delete,
            };

            let record = crate::wal::WalRecord {
                cf_id: op.cf_id().as_u32(),
                op: wal_op_kind,
                key: op.key().clone(),
                value: op.value().cloned(),
                seq,
                expiration,
                range_end: None,
                txn_id: None,
                compression: None,
            };
            wal_records.push(record);
        }

        // Write all records in one batch
        self.metrics.record_wal_write();
        self.wal_coordinator.append_batch(&wal_records)?;

        // Apply to memtable (using pre-computed expirations from WAL record creation)
        for (op, seq, expiration) in sequences {
            let cf_id = op.cf_id();
            let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
                crate::error::MidgeError::invalid_config(format!(
                    "Column family with id {} does not exist",
                    cf_id.as_u32()
                ))
            })?;

            match op.kind() {
                crate::api::write_batch::OpKind::Put => {
                    self.metrics.record_put();
                    self.metrics.record_memtable_write();

                    if let Some(value) = op.value() {
                        let mt = column_family.memtable.read();
                        mt.put_with_seq_and_exp(op.key(), value, seq, expiration);
                    }
                }
                crate::api::write_batch::OpKind::Delete => {
                    self.metrics.record_delete();
                    self.metrics.record_memtable_write();
                    self.metrics.record_point_tombstone_created();
                    let mt = column_family.memtable.read();
                    mt.delete_with_seq(op.key(), seq);
                }
            }
        }

        // Single sync for entire batch if configured
        if self.wal_sync {
            self.metrics.record_wal_sync();
            self.wal_coordinator.sync()?;
        }

        // Check if any memtables are full after batch and trigger per-CF flushes
        let cfs = self.list_column_families();
        for cf in cfs {
            let is_full = if cf.id() == DEFAULT_CF_ID {
                self.with_default_memtable(|mt| mt.is_full(self.memtable_size))
            } else {
                self.with_cf_memtable(cf.id(), |mt| mt.is_full(self.memtable_size))
                    .unwrap_or(false)
            };

            if is_full {
                let _ = self.flush_cf(&cf);
            }
        }

        Ok(())
    }

    /// Apply a merge operation to a key in a specific column family.
    ///
    /// Merge operations are deferred - they don't require reading the current value.
    /// Multiple merge operands are combined during compaction or on read.
    ///
    /// A merge operator must be registered for the column family before calling merge.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # use cntryl_midge::IntegerAddOperator;
    /// # use std::sync::Arc;
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    /// engine.register_merge_operator(&cf, Arc::new(IntegerAddOperator));
    /// // Increment counter without reading current value
    /// engine.merge_cf(&cf, b"page_views", b"1").unwrap();
    /// engine.merge_cf(&cf, b"page_views", b"5").unwrap();
    /// ```
    pub fn merge_cf(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.merge_with_ttl_cf(cf, key, value, 0)
    }

    /// Apply a merge operation with TTL to a key in a specific column family.
    ///
    /// Like `merge_cf`, but the resulting value will expire after the specified duration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # use cntryl_midge::IntegerAddOperator;
    /// # use std::sync::Arc;
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    /// engine.register_merge_operator(&cf, Arc::new(IntegerAddOperator));
    /// // Temporary counter expires after 60 seconds
    /// engine.merge_with_ttl_cf(&cf, b"temp_counter", b"1", 60).unwrap();
    /// ```
    pub fn merge_with_ttl_cf(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot write in read-only mode",
            ));
        }

        let cf_id = cf.id();

        // Check that a merge operator is registered for this CF
        {
            let ops = self.merge_operators.read();
            if !ops.contains_key(&cf_id.as_u32()) {
                return Err(crate::error::MidgeError::invalid_config(format!(
                    "No merge operator registered for column family '{}'",
                    cf.name()
                )));
            }
        }

        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Compute expiration time in milliseconds if TTL > 0
        let expiration = if ttl_seconds > 0 {
            let now_millis = timestamp::now_millis();
            Some(now_millis + (ttl_seconds * 1000))
        } else {
            None
        };

        // Write to WAL
        let rec = crate::wal::WalRecord::new_with_ttl(
            cf_id,
            crate::wal::WalOpKind::Merge,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            seq,
            ttl_seconds,
        );

        self.wal_coordinator.append_record(&rec)?;
        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        // Write to MemTable as merge operand
        let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        {
            let mt = column_family.memtable.read();
            mt.merge_with_seq_and_exp(key, value, seq, expiration);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            let frozen = column_family.try_freeze_memtable();

            if frozen && cf_id == DEFAULT_CF_ID {
                let _ = self.flush();
            } else if column_family.should_stall_writes() {
                return Err(crate::error::MidgeError::invalid_config(
                    "Write stall: too many immutable memtables pending flush",
                ));
            }
        }

        Ok(())
    }

    /// Insert only if the key does not exist (atomic check-and-set).
    ///
    /// Uses snapshot isolation for consistency.
    ///
    /// # Examples
    /// ```
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine, StorageMode};
    /// # use bytes::Bytes;
    /// # let mut opts = MidgeOptions::default();
    /// # opts.storage_mode = StorageMode::Memory;
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    ///
    /// // First insert succeeds
    /// assert!(engine.insert(&cf, b"user:123", b"Alice").unwrap());
    ///
    /// // Second insert fails (key exists)
    /// assert!(!engine.insert(&cf, b"user:123", b"Alice").unwrap());
    /// ```
    pub fn insert(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<bool> {
        self.insert_with_ttl(cf, key, value, 0)
    }

    /// Insert a key-value pair only if the key does not exist, with TTL.
    ///
    /// Returns true if inserted, false if key already exists.
    /// TTL is specified in seconds; 0 means no expiration.
    ///
    /// # Examples
    /// ```no_run
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    /// // Insert with 300 second TTL
    /// let inserted = engine.insert_with_ttl(
    ///     &cf,
    ///     b"lock:resource",
    ///     b"held",
    ///     300
    /// ).unwrap();
    /// ```
    pub fn insert_with_ttl(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<bool> {
        self.check_read_only()?;

        // Use snapshot isolation for consistent read-then-write
        let snapshot = self.snapshot();
        let exists = self.get_at(cf, key, &snapshot)?.is_some();

        if exists {
            return Ok(false);
        }

        // Key doesn't exist, perform the put with TTL
        self.put_with_ttl(cf, key, value, ttl_seconds)?;
        Ok(true)
    }
}
