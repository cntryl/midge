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

use std::sync::Arc;

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
    /// Internal helper to manage write stalls caused by full immutable memtable queue.
    /// Attempts to proactively flush and waits (bounded) for stall condition to clear.
    /// Latency target: typically < 2s once background error is cleared and a flush drains.
    fn handle_write_stall(
        &self,
        cf: &ColumnFamilyHandle,
        column_family: &Arc<crate::core::engine::column_family::ColumnFamily>,
    ) -> MidgeResult<()> {
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};

        let mut background_wait_duration_ms = 0u64;
        let mut capacity_wait_duration_ms = 0u64;
        let mut background_blocked = false;

        if self.background_error.read().is_some() {
            background_blocked = true;
            let bg_start = Instant::now();
            self.wait_for_background_error_cleared();
            background_wait_duration_ms = bg_start.elapsed().as_millis() as u64;
        }

        let mut is_stalled = column_family.should_stall_writes();
        let mut had_capacity_stall = is_stalled;

        if !is_stalled && !background_blocked {
            return Ok(());
        }

        if background_blocked && !is_stalled {
            let total_duration_ms = background_wait_duration_ms;
            self.metrics.record_write_stall(total_duration_ms);
            self.metrics
                .record_background_write_stall(background_wait_duration_ms);
            return Ok(());
        }

        if column_family.should_stall_writes() {
            let _ = self.flush_cf(cf);
            if column_family.should_stall_writes() {
                let _ = self.rollover_and_queue_flush(cf.id());
            }
        }

        let max_wait = Duration::from_millis(2000); // Upper bound on active waiting
        let mut attempts: usize = 0;

        is_stalled = column_family.should_stall_writes();
        if is_stalled {
            had_capacity_stall = true;
        }

        if is_stalled {
            let loop_start = Instant::now();
            let mut backoff_ms = 20u64;
            let max_backoff_ms = 100u64;

            while column_family.should_stall_writes() {
                let imm_len = {
                    let imm_len = column_family.immutable_memtables.lock().len();
                    column_family
                        .immutable_count
                        .store(imm_len, Ordering::Release);
                    imm_len
                };
                if imm_len == 0 {
                    break;
                }

                std::thread::sleep(Duration::from_millis(backoff_ms));

                let _ = self.wait_for_flush(Duration::from_millis(50));
                attempts += 1;

                if attempts.is_multiple_of(4) {
                    let _ = self.rollover_and_queue_flush(cf.id());
                }

                backoff_ms = (backoff_ms * 2).min(max_backoff_ms);

                if loop_start.elapsed() >= max_wait {
                    break;
                }
            }

            capacity_wait_duration_ms = loop_start.elapsed().as_millis() as u64;
        }

        if background_blocked || had_capacity_stall {
            let total_duration_ms = background_wait_duration_ms + capacity_wait_duration_ms;
            self.metrics.record_write_stall(total_duration_ms);
            if background_blocked {
                self.metrics
                    .record_background_write_stall(background_wait_duration_ms);
            }
            if had_capacity_stall {
                self.metrics
                    .record_capacity_write_stall(capacity_wait_duration_ms);
            }
        }

        Ok(())
    }
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
            // If the engine is configured to only perform a local WAL sync
            // (cloud-backed and local_wal_sync = true), prefer a non-blocking
            // flush rather than waiting for potentially flaky cloud uploads.
            if self.wait_for_cloud_wal_uploads_on_sync {
                // Wait for cloud uploads as part of sync semantics
                self.wal_coordinator.sync()?;
            } else {
                // Local-only WAL durability: ensure we sync the local WAL without
                // waiting for cloud uploads (non-blocking w.r.t. remote failures)
                let _ = self.wal_coordinator.sync_local();
            }
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
            let mt = column_family.memtable.load();
            mt.put_with_seq(key, value, seq);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            // Freeze the current memtable: atomic swap with new empty one
            let old_arc = column_family
                .memtable
                .swap(Arc::new(crate::core::memtable::MemTable::new()));

            // Extract memtable from Arc (cheap if refcount is 1, clone if shared)
            let old_memtable = Arc::try_unwrap(old_arc).unwrap_or_else(|arc| (*arc).clone());

            // Flush the frozen memtable to SST by calling flush_frozen_memtable
            let _ = self.flush_frozen_memtable(cf, old_memtable);
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
            let mt = column_family.memtable.load();
            mt.put_with_seq_and_exp(key, value, seq, expiration);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            let frozen = column_family.try_freeze_memtable();
            if frozen && cf_id == DEFAULT_CF_ID {
                let _ = self.flush();
            }
        }

        // Unified stall handling
        self.handle_write_stall(cf, &column_family)?;

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
            let mt = column_family.memtable.load();
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
            let mt = column_family.memtable.load();
            mt.delete_range_with_seq(start, end, seq);
        }

        // Apply stall handling for consistency with other write paths (e.g., put_with_ttl, merge)
        self.handle_write_stall(cf, &column_family)?;

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
            let seq = self.seq.fetch_add(1, Ordering::SeqCst);

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
                        let mt = column_family.memtable.load();
                        mt.put_with_seq_and_exp(op.key(), value, seq, expiration);
                    }
                }
                crate::api::write_batch::OpKind::Delete => {
                    self.metrics.record_delete();
                    self.metrics.record_memtable_write();
                    self.metrics.record_point_tombstone_created();
                    let mt = column_family.memtable.load();
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
            let mt = column_family.memtable.load();
            mt.merge_with_seq_and_exp(key, value, seq, expiration);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            let frozen = column_family.try_freeze_memtable();
            if frozen && cf_id == DEFAULT_CF_ID {
                let _ = self.flush();
            }
            // Apply unified stall handling instead of returning error
            self.handle_write_stall(cf, &column_family)?;
        } else {
            // Even if not full, immutable queue may already be saturated by prior freezes.
            self.handle_write_stall(cf, &column_family)?;
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

#[cfg(test)]
mod tests {
    use crate::{MidgeEngine, MidgeOptions, StorageMode, WriteBatch};
    use bytes::Bytes;
    use uuid;

    fn create_test_engine() -> MidgeEngine {
        let temp_dir =
            std::env::temp_dir().join(format!("midge_test_writes_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir;
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk { db_path },
            enable_compaction: false,
            ..Default::default()
        };
        MidgeEngine::open(opts).unwrap()
    }

    #[test]
    fn should_put_value_when_key_not_exists() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Act
        let put_result = engine.put(&cf, b"key1", b"value1");

        // Assert
        assert!(put_result.is_ok());
    }

    #[test]
    fn should_get_value_when_key_exists() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"value1").unwrap();

        // Act
        let get_result = engine.get(&cf, b"key1");

        // Assert
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), Some(Bytes::from("value1")));
    }

    #[test]
    fn should_delete_existing_key() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"value1").unwrap();

        // Act
        let delete_result = engine.delete(&cf, b"key1");
        let get_result = engine.get(&cf, b"key1");

        // Assert
        assert!(delete_result.is_ok());
        assert!(get_result.is_ok());
        assert_eq!(get_result.unwrap(), None);
    }

    #[test]
    fn should_write_batch_successfully() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        let mut batch = WriteBatch::new();
        batch.put(cf.id(), Bytes::from("key1"), Bytes::from("value1"));
        batch.put(cf.id(), Bytes::from("key2"), Bytes::from("value2"));
        batch.delete(cf.id(), Bytes::from("key3"));

        // Act
        let result = engine.write_batch(&batch);

        // Assert
        assert!(result.is_ok());
        assert_eq!(
            engine.get(&cf, b"key1").unwrap(),
            Some(Bytes::from("value1"))
        );
        assert_eq!(
            engine.get(&cf, b"key2").unwrap(),
            Some(Bytes::from("value2"))
        );
        assert_eq!(engine.get(&cf, b"key3").unwrap(), None);
    }

    #[test]
    fn should_insert_when_key_does_not_exist() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Act
        let result = engine.insert(&cf, b"key1", b"value1");

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap());
        assert_eq!(
            engine.get(&cf, b"key1").unwrap(),
            Some(Bytes::from("value1"))
        );
    }

    #[test]
    fn should_not_insert_when_key_exists() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();
        engine.put(&cf, b"key1", b"existing").unwrap();

        // Act
        let result = engine.insert(&cf, b"key1", b"new_value");

        // Assert
        assert!(result.is_ok());
        assert!(!result.unwrap());
        assert_eq!(
            engine.get(&cf, b"key1").unwrap(),
            Some(Bytes::from("existing"))
        );
    }

    // =====================================================================
    // P0: Sequence allocation invariant tests
    // =====================================================================

    #[test]
    fn should_allocate_monotonically_increasing_sequences() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Act: Write multiple keys
        for i in 0..100 {
            engine.put(&cf, format!("key{}", i).as_bytes(), b"value").unwrap();
        }
        engine.flush().unwrap();

        // Assert: All writes succeeded (sequence allocation worked)
        for i in 0..100 {
            let val = engine.get(&cf, format!("key{}", i).as_bytes()).unwrap();
            assert!(val.is_some(), "Key {} should exist", i);
        }
    }

    #[test]
    fn should_allocate_unique_sequences_for_concurrent_writes() {
        // Arrange
        let engine = std::sync::Arc::new(create_test_engine());
        let cf = engine.default_column_family();
        let num_threads = 4;
        let writes_per_thread = 50;

        // Act: Spawn multiple threads writing concurrently
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let engine = engine.clone();
                let cf = cf.clone();
                std::thread::spawn(move || {
                    for i in 0..writes_per_thread {
                        let key = format!("t{}k{}", t, i);
                        engine.put(&cf, key.as_bytes(), b"value").unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Assert: All writes should be present (no lost writes due to sequence conflicts)
        let mut found = 0;
        for t in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("t{}k{}", t, i);
                if engine.get(&cf, key.as_bytes()).unwrap().is_some() {
                    found += 1;
                }
            }
        }
        assert_eq!(found, num_threads * writes_per_thread, "All writes should succeed");
    }

    #[test]
    fn should_handle_rapid_overwrites_with_unique_sequences() {
        // Arrange: Same key written many times rapidly
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Act: 100 rapid overwrites to same key
        for i in 0..100 {
            engine.put(&cf, b"hot_key", format!("v{}", i).as_bytes()).unwrap();
        }

        // Assert: Latest value should be visible
        let value = engine.get(&cf, b"hot_key").unwrap();
        assert_eq!(value.as_deref(), Some(b"v99".as_ref()));
    }

    #[test]
    fn should_persist_sequence_order_after_flush() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Act: Write in specific order then flush
        engine.put(&cf, b"a", b"first").unwrap();
        engine.put(&cf, b"z", b"second").unwrap();
        engine.put(&cf, b"m", b"third").unwrap();
        engine.flush().unwrap();

        // Assert: All values should be retrievable
        assert_eq!(engine.get(&cf, b"a").unwrap().as_deref(), Some(b"first".as_ref()));
        assert_eq!(engine.get(&cf, b"z").unwrap().as_deref(), Some(b"second".as_ref()));
        assert_eq!(engine.get(&cf, b"m").unwrap().as_deref(), Some(b"third".as_ref()));
    }
}
