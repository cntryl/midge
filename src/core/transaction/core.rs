use crate::api::mutation::{Mutation, MutationOp};
use crate::core::transaction::{ConflictTracker, SpillManager};
use crate::error::MidgeError;
use bytes::Bytes;
use std::time::{Duration, Instant};

/// Internal transaction staging buffer for mutations and conflict tracking.
///
/// Responsibilities:
/// - Stage mutations (put/delete/etc.) in memory with optional spill-to-disk
/// - Track read/write sets for optimistic concurrency
/// - Enforce timeouts and memory thresholds
/// - Manage commit/rollback lifecycle
///
/// Not user-facing; wrapped by `EngineTransaction`.
pub struct Transaction {
    #[allow(dead_code)]
    pub(crate) txn_id: u64,
    begin_seq: u64,
    deadline: Option<Instant>,
    completed: bool,

    // Mutation staging
    staged: Vec<Mutation>,
    mem_limit: usize,
    mem_used: usize,
    spill: SpillManager,

    // Conflict tracking
    conflicts: ConflictTracker,
}

impl Transaction {
    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------
    pub fn new(txn_id: u64, begin_seq: u64) -> Self {
        Self::with_options(txn_id, begin_seq, None, 100 * 1024 * 1024)
    }

    pub fn with_options(
        txn_id: u64,
        begin_seq: u64,
        timeout: Option<Duration>,
        mem_limit: usize,
    ) -> Self {
        let created_at = Instant::now();
        let deadline = timeout.map(|t| created_at + t);
        Self {
            txn_id,
            begin_seq,
            deadline,
            completed: false,
            staged: Vec::new(),
            mem_limit,
            mem_used: 0,
            spill: SpillManager::new(txn_id),
            conflicts: ConflictTracker::new(),
        }
    }

    // -------------------------------------------------------------------------
    // Basic accessors
    // -------------------------------------------------------------------------
    pub(crate) fn begin_seq(&self) -> u64 {
        self.begin_seq
    }
    pub(crate) fn is_expired(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() > d)
    }
    
    /// Access staged mutations for transaction-aware operations (e.g., scans).
    /// Returns a slice of in-memory staged mutations.
    pub(crate) fn staged_mutations(&self) -> &[Mutation] {
        &self.staged
    }

    // -------------------------------------------------------------------------
    // Conflict tracking
    // -------------------------------------------------------------------------
    pub(crate) fn track_read(&mut self, cf: u32, key: Bytes, ver: u64) {
        self.conflicts.track_read(cf, key, ver);
    }
    pub(crate) fn track_write(&mut self, cf: u32, key: Bytes) {
        self.conflicts.track_write(cf, key);
    }
    pub(crate) fn track_write_range(&mut self, cf: u32, start_key: Bytes, end_key: Bytes) {
        self.conflicts.track_write_range(cf, start_key, end_key);
    }

    // -------------------------------------------------------------------------
    // Spill management
    // -------------------------------------------------------------------------
    fn maybe_spill(&mut self) -> Result<(), MidgeError> {
        if let Some(m) = self.staged.last() {
            let sz = m.key.len()
                + m.value.as_ref().map_or(0, |v| v.len())
                + m.range_end.as_ref().map_or(0, |v| v.len());
            self.mem_used += sz;
            if self.mem_used >= self.mem_limit {
                self.spill_to_disk()?;
            }
        }
        Ok(())
    }

    fn spill_to_disk(&mut self) -> Result<(), MidgeError> {
        self.spill.spill_to_disk(&self.staged)?;
        self.staged.clear();
        self.mem_used = 0;
        Ok(())
    }

    fn read_spilled(&self) -> Result<Vec<Mutation>, MidgeError> {
        self.spill.read_spill_files()
    }

    fn cleanup_spills(&mut self) {
        self.spill.cleanup_spill_files();
    }

    // -------------------------------------------------------------------------
    // Mutation helpers
    // -------------------------------------------------------------------------
    fn stage(
        &mut self,
        cf: crate::api::ColumnFamilyId,
        m: Mutation,
        key: Bytes,
    ) -> Result<(), MidgeError> {
        if self.completed {
            return Err(MidgeError::internal("cannot modify completed transaction"));
        }

        // Track the operation for conflict detection
        match m.op {
            crate::api::MutationOp::DeleteRange => {
                if let Some(end_key) = &m.range_end {
                    self.track_write_range(cf.as_u32(), key, end_key.clone());
                } else {
                    // Fallback to tracking just the start key if no end key
                    self.track_write(cf.as_u32(), key);
                }
            }
            _ => {
                self.track_write(cf.as_u32(), key);
            }
        }

        self.staged.push(m);
        self.maybe_spill()
    }

    #[inline]
    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<(), MidgeError> {
        self.put_cf(
            crate::api::DEFAULT_CF_ID,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(val),
            None,
        )
    }

    pub fn put_with_ttl(
        &mut self,
        key: &[u8],
        val: &[u8],
        ttl: Duration,
    ) -> Result<(), MidgeError> {
        self.put_cf(
            crate::api::DEFAULT_CF_ID,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(val),
            Some(ttl),
        )
    }

    pub fn put_cf(
        &mut self,
        cf: crate::api::ColumnFamilyId,
        key: Bytes,
        val: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), MidgeError> {
        let m = Mutation::put_cf(cf, key.clone(), val, ttl);
        self.stage(cf, m, key)
    }

    pub fn insert(
        &mut self,
        key: Bytes,
        val: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), MidgeError> {
        self.insert_cf(crate::api::DEFAULT_CF_ID, key, val, ttl)
    }

    pub fn insert_cf(
        &mut self,
        cf: crate::api::ColumnFamilyId,
        key: Bytes,
        val: Bytes,
        ttl: Option<Duration>,
    ) -> Result<(), MidgeError> {
        let m = Mutation::insert_cf(cf, key.clone(), val, ttl);
        self.stage(cf, m, key)
    }

    pub fn delete(&mut self, key: Bytes) -> Result<(), MidgeError> {
        self.delete_cf(crate::api::DEFAULT_CF_ID, key)
    }

    pub fn delete_cf(
        &mut self,
        cf: crate::api::ColumnFamilyId,
        key: Bytes,
    ) -> Result<(), MidgeError> {
        let m = Mutation::delete_cf(cf, key.clone());
        self.stage(cf, m, key)
    }

    pub fn delete_range(&mut self, start: Bytes, end: Bytes) -> Result<(), MidgeError> {
        self.delete_range_cf(crate::api::DEFAULT_CF_ID, start, end)
    }

    pub fn delete_range_cf(
        &mut self,
        cf: crate::api::ColumnFamilyId,
        start: Bytes,
        end: Bytes,
    ) -> Result<(), MidgeError> {
        let m = Mutation::delete_range_cf(cf, start.clone(), end);
        self.stage(cf, m, start)
    }

    pub fn compare_and_swap(
        &mut self,
        key: Bytes,
        expected: Option<Bytes>,
        new_val: Bytes,
    ) -> Result<(), MidgeError> {
        self.compare_and_swap_cf(crate::api::DEFAULT_CF_ID, key, expected, new_val)
    }

    pub fn compare_and_swap_cf(
        &mut self,
        cf: crate::api::ColumnFamilyId,
        key: Bytes,
        expected: Option<Bytes>,
        new_val: Bytes,
    ) -> Result<(), MidgeError> {
        let m = Mutation::compare_and_swap_cf(cf, key.clone(), expected, new_val);
        self.stage(cf, m, key)
    }

    pub fn merge(&mut self, key: Bytes, val: Bytes) -> Result<(), MidgeError> {
        self.merge_cf(crate::api::DEFAULT_CF_ID, key, val)
    }

    pub fn merge_cf(
        &mut self,
        cf: crate::api::ColumnFamilyId,
        key: Bytes,
        val: Bytes,
    ) -> Result<(), MidgeError> {
        let m = Mutation::merge_cf(cf, key.clone(), val);
        self.stage(cf, m, key)
    }

    // -------------------------------------------------------------------------
    // Read helpers
    // -------------------------------------------------------------------------
    pub fn get_local(&self, cf: u32, key: &[u8]) -> Option<Option<Bytes>> {
        self.staged.iter().rev().find_map(|m| {
            if m.cf_id.as_u32() != cf {
                return None;
            }
            match &m.op {
                MutationOp::DeleteRange => {
                    if let Some(end) = &m.range_end {
                        if m.key.as_ref() <= key && key < end.as_ref() {
                            return Some(None);
                        }
                    }
                    None
                }
                _ if m.key.as_ref() == key => Some(m.value.clone()),
                _ => None,
            }
        })
    }

    // -------------------------------------------------------------------------
    // Commit / Rollback
    // -------------------------------------------------------------------------
    pub fn commit(mut self) -> Result<Vec<Mutation>, MidgeError> {
        if self.completed {
            return Err(MidgeError::internal("transaction already completed"));
        }
        self.completed = true;

        let mut all = if self.spill.has_spill_files() {
            self.read_spilled()?
        } else {
            Vec::new()
        };
        all.extend(std::mem::take(&mut self.staged));
        self.cleanup_spills();
        Ok(all)
    }

    pub fn rollback(&mut self) {
        if !self.completed {
            self.staged.clear();
            self.cleanup_spills();
            self.completed = true;
        }
    }

    /// Return a clone of the tracked write set for external conflict checks.
    pub(crate) fn conflict_write_set(&self) -> std::collections::HashSet<(u32, Bytes)> {
        self.conflicts.write_set().clone()
    }

    /// Return a clone of the tracked write ranges for external conflict checks.
    pub(crate) fn conflict_write_ranges(&self) -> &std::collections::HashSet<(u32, Bytes, Bytes)> {
        self.conflicts.write_ranges()
    }

    /// Return a clone of the tracked read set for external conflict checks.
    pub(crate) fn conflict_read_set(&self) -> std::collections::HashSet<(u32, Bytes)> {
        self.conflicts.read_set().clone()
    }

    /// Return a clone of the tracked read versions map.
    pub(crate) fn conflict_read_versions(&self) -> std::collections::HashMap<(u32, Bytes), u64> {
        self.conflicts.read_versions().clone()
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.staged.clear();
        self.cleanup_spills();
        self.completed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Write Staging Tests
    // ========================================================================

    #[test]
    fn should_stage_put_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(b"key1", b"value1").unwrap();
        txn.put(b"key2", b"value2").unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 2);
        assert_eq!(mutations[0].key, Bytes::from("key1"));
        assert_eq!(mutations[0].value, Some(Bytes::from("value1")));
        assert_eq!(mutations[1].key, Bytes::from("key2"));
        assert_eq!(mutations[1].value, Some(Bytes::from("value2")));
    }

    #[test]
    fn should_stage_delete_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.delete(Bytes::from("deleted_key")).unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].key, Bytes::from("deleted_key"));
        assert!(matches!(mutations[0].op, MutationOp::Delete));
    }

    #[test]
    fn should_stage_delete_range_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.delete_range(Bytes::from("start"), Bytes::from("end"))
            .unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].key, Bytes::from("start"));
        assert_eq!(mutations[0].range_end, Some(Bytes::from("end")));
    }

    #[test]
    fn should_stage_merge_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.merge(Bytes::from("key"), Bytes::from("value")).unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(matches!(mutations[0].op, MutationOp::Merge));
    }

    #[test]
    fn should_preserve_mutation_order_given_multiple_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(b"k1", b"v1").unwrap();
        txn.delete(Bytes::from("k2")).unwrap();
        txn.put(b"k3", b"v3").unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 3);
        assert_eq!(mutations[0].key, Bytes::from("k1"));
        assert_eq!(mutations[1].key, Bytes::from("k2"));
        assert_eq!(mutations[2].key, Bytes::from("k3"));
    }

    // ========================================================================
    // TTL Tests
    // ========================================================================

    #[test]
    fn should_attach_ttl_to_put_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put_with_ttl(b"key", b"value", Duration::from_secs(60))
            .unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(mutations[0].ttl.is_some());
    }

    #[test]
    fn should_attach_ttl_to_insert_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.insert_cf(
            crate::api::DEFAULT_CF_ID,
            Bytes::from("key"),
            Bytes::from("value"),
            Some(Duration::from_secs(120)),
        )
        .unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(mutations[0].ttl.is_some());
    }

    // ========================================================================
    // Local Read Tests
    // ========================================================================

    #[test]
    fn should_read_uncommitted_put_from_staging() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"key", b"value").unwrap();

        // Act
        let result = txn.get_local(crate::api::DEFAULT_CF_ID.as_u32(), b"key");

        // Assert
        assert_eq!(result, Some(Some(Bytes::from("value"))));
    }

    #[test]
    fn should_return_none_for_uncommitted_delete_from_staging() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.delete(Bytes::from("key")).unwrap();

        // Act
        let result = txn.get_local(crate::api::DEFAULT_CF_ID.as_u32(), b"key");

        // Assert
        assert_eq!(result, Some(None));
    }

    #[test]
    fn should_read_latest_value_given_multiple_puts_same_key() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"key", b"v1").unwrap();
        txn.put(b"key", b"v2").unwrap();

        // Act
        let result = txn.get_local(crate::api::DEFAULT_CF_ID.as_u32(), b"key");

        // Assert
        assert_eq!(result, Some(Some(Bytes::from("v2"))));
    }

    #[test]
    fn should_not_find_key_in_different_cf() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"key", b"value").unwrap();

        // Act
        let result = txn.get_local(999, b"key");

        // Assert
        assert_eq!(result, None);
    }

    #[test]
    fn should_handle_delete_range_in_local_get() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.delete_range(Bytes::from("a"), Bytes::from("z"))
            .unwrap();

        // Act - key is in range
        let result1 = txn.get_local(crate::api::DEFAULT_CF_ID.as_u32(), b"m");

        // Assert
        assert_eq!(result1, Some(None));
    }

    #[test]
    fn should_not_apply_delete_range_outside_bounds() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"key", b"value").unwrap();
        txn.delete_range(Bytes::from("a"), Bytes::from("k"))
            .unwrap(); // key > k

        // Act
        let result = txn.get_local(crate::api::DEFAULT_CF_ID.as_u32(), b"key");

        // Assert
        assert_eq!(result, Some(Some(Bytes::from("value"))));
    }

    // ========================================================================
    // Timeout Tests
    // ========================================================================

    #[test]
    fn should_not_expire_given_no_deadline_set() {
        // Arrange
        let txn = Transaction::new(1, 100);

        // Act
        let expired = txn.is_expired();

        // Assert
        assert!(!expired);
    }

    #[test]
    fn should_not_expire_given_deadline_not_reached() {
        // Arrange
        let txn = Transaction::with_options(1, 100, Some(Duration::from_secs(10)), 1024);

        // Act
        let expired = txn.is_expired();

        // Assert
        assert!(!expired);
    }

    #[test]
    fn should_expire_given_deadline_exceeded() {
        // Arrange
        let txn = Transaction::with_options(1, 100, Some(Duration::from_nanos(1)), 1024);

        // Act
        std::thread::sleep(Duration::from_millis(1));
        let expired = txn.is_expired();

        // Assert
        assert!(expired);
    }

    // ========================================================================
    // Memory Tracking Tests
    // ========================================================================

    #[test]
    fn should_track_memory_usage_given_put_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(b"key", b"value").unwrap();

        // Assert
        assert!(txn.mem_used > 0);
    }

    #[test]
    fn should_accumulate_memory_given_multiple_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(b"k1", b"v1").unwrap();
        let mem1 = txn.mem_used;

        txn.put(b"k2", b"v2").unwrap();
        let mem2 = txn.mem_used;

        // Assert
        assert!(mem2 > mem1);
    }

    #[test]
    fn should_count_delete_memory_usage_given_delete_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.delete(Bytes::from("deleted_key")).unwrap();

        // Assert
        assert!(txn.mem_used > 0);
    }

    // ========================================================================
    // Transaction Lifecycle Tests
    // ========================================================================

    #[test]
    fn should_mark_completed_given_commit() {
        // Arrange
        let txn = Transaction::new(1, 100);

        // Act
        let result = txn.commit();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_fail_given_double_commit() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.rollback();

        // Act
        let result = txn.commit();

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_not_return_mutations_given_rollback() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"key", b"value").unwrap();

        // Act
        txn.rollback();
        let result = txn.commit();

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_return_staged_mutations_given_successful_commit() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"k1", b"v1").unwrap();
        txn.put(b"k2", b"v2").unwrap();

        // Act
        let mutations = txn.commit().expect("commit");

        // Assert
        assert_eq!(mutations.len(), 2);
    }

    #[test]
    fn should_have_correct_begin_sequence_given_new_transaction() {
        // Arrange
        let begin_seq = 42;

        // Act
        let txn = Transaction::new(1, begin_seq);

        // Assert
        assert_eq!(txn.begin_seq(), begin_seq);
    }

    // ========================================================================
    // Spill-to-Disk Tests
    // ========================================================================

    #[test]
    fn should_spill_to_disk_given_exceed_threshold_when_staging_writes() {
        // Arrange
        let memory_threshold = 100; // Very small threshold to trigger spill
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = vec![b'x'; 200]; // Larger than threshold

        // Act
        txn.put(b"key1", &large_value).unwrap();

        // Assert - memory should be reset after spill
        assert_eq!(txn.mem_used, 0, "Memory should be reset after spill");
        assert_eq!(
            txn.staged.len(),
            0,
            "Staged mutations should be cleared after spill"
        );
    }

    #[test]
    fn should_read_from_spill_file_given_large_transaction_when_commit() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let value1 = vec![b'a'; 100];
        let value2 = vec![b'b'; 20];

        // Act
        txn.put(b"spilled_key", &value1).unwrap();
        txn.put(b"memory_key", &value2).unwrap();
        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(
            mutations.len(),
            2,
            "Should have both spilled and in-memory mutations"
        );

        // Verify mutations are in correct order
        assert_eq!(mutations[0].key, Bytes::from("spilled_key"));
        assert_eq!(mutations[0].value.as_ref().map(|v| v.len()), Some(100));
        assert_eq!(mutations[1].key, Bytes::from("memory_key"));
        assert_eq!(mutations[1].value.as_ref().map(|v| v.len()), Some(20));
    }

    #[test]
    fn should_preserve_mutation_order_given_spill_and_memory_mutations() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        // Act
        txn.put(b"key1", &[b'a'; 100]).unwrap(); // Spill
        txn.put(b"key2", b"small").unwrap(); // Memory
        txn.put(b"key3", &[b'b'; 100]).unwrap(); // Spill
        txn.put(b"key4", b"tiny").unwrap(); // Memory

        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(mutations.len(), 4);
        assert_eq!(mutations[0].key, Bytes::from("key1"));
        assert_eq!(mutations[1].key, Bytes::from("key2"));
        assert_eq!(mutations[2].key, Bytes::from("key3"));
        assert_eq!(mutations[3].key, Bytes::from("key4"));
    }

    #[test]
    fn should_handle_delete_operations_in_spill_file() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        txn.put(b"key1", &[b'a'; 100]).unwrap();
        txn.delete(Bytes::from("key2")).unwrap(); // Small, stays in memory
        txn.put(b"key3", &[b'b'; 100]).unwrap();

        // Act
        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(mutations.len(), 3);
        assert!(matches!(mutations[0].op, MutationOp::Put));
        assert!(matches!(mutations[1].op, MutationOp::Delete));
        assert!(matches!(mutations[2].op, MutationOp::Put));
    }

    #[test]
    fn should_handle_delete_range_in_spill_file() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        // Act
        txn.put(b"key1", &[b'a'; 100]).unwrap();
        txn.delete_range(Bytes::from("start"), Bytes::from("end"))
            .unwrap();

        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(mutations.len(), 2);
        assert!(matches!(mutations[0].op, MutationOp::Put));
        assert!(matches!(mutations[1].op, MutationOp::DeleteRange));
        assert_eq!(mutations[1].range_end, Some(Bytes::from("end")));
    }

    #[test]
    fn should_spill_to_disk_given_exceed_memory_threshold() {
        // Arrange
        let memory_threshold = 1024; // 1KB threshold
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        // Act - Add 2KB of data (force spilling)
        for i in 0..2 {
            txn.put(format!("key{:03}", i).as_bytes(), &vec![0xAB; 1024])
                .unwrap();
        }

        let mutations = txn.commit().unwrap();

        // Assert - All mutations should be present
        assert_eq!(mutations.len(), 2, "Should have all mutations after spill");
        assert_eq!(mutations[0].key, Bytes::from("key000"));
        assert_eq!(mutations[1].key, Bytes::from("key001"));
    }

    #[test]
    fn should_handle_large_values_in_spill() {
        // Arrange
        let memory_threshold = 256;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        // Act - Write large value
        let large_value = vec![0xCC; 10000];
        txn.put(b"large_key", &large_value).unwrap();

        let mutations = txn.commit().unwrap();

        // Assert - Large value preserved
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].value, Some(Bytes::from(large_value)));
    }

    #[test]
    fn should_cleanup_spill_on_drop() {
        // Arrange
        let memory_threshold = 256;

        // Act - Spill data then drop without committing
        {
            let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
            txn.put(b"key", &vec![0; 1000]).unwrap();
            // Transaction dropped here without commit or rollback
        }

        // Assert - Should not panic or leave files (tested by not crashing)
        // Cleanup is automatic via Drop trait
    }

    // ========================================================================
    // Transaction State Tests
    // ========================================================================

    #[test]
    fn should_reject_put_given_rolled_back_transaction() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"key1", b"value1").unwrap();
        txn.rollback();

        // Act
        let result = txn.put(b"key2", b"value2");

        // Assert
        assert!(
            result.is_err(),
            "Should reject put on rolled-back transaction"
        );
        assert!(result.unwrap_err().to_string().contains("completed"));
    }

    #[test]
    fn should_reject_delete_given_rolled_back_transaction() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.rollback();

        // Act
        let result = txn.delete(Bytes::from("key"));

        // Assert
        assert!(
            result.is_err(),
            "Should reject delete on rolled-back transaction"
        );
    }

    #[test]
    fn should_reject_commit_given_already_completed_transaction() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"key", b"value").unwrap();
        txn.rollback(); // This sets completed = true

        // Act
        let result = txn.commit();

        // Assert
        assert!(
            result.is_err(),
            "Should reject commit on already completed transaction"
        );
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("already completed"));
    }

    #[test]
    fn should_allow_rollback_given_already_rolled_back_transaction() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.rollback();

        // Act
        txn.rollback(); // Second rollback should be idempotent

        // Assert - No panic, rollback is idempotent
    }

    #[test]
    fn should_not_add_mutations_after_rollback() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(b"key1", b"value1").unwrap();
        txn.rollback();

        // Act - Try to add more mutations
        let result = txn.put(b"key2", b"value2");

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_support_column_family_operations() {
        // Arrange
        let cf_id = crate::api::ColumnFamilyId::new(5);
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put_cf(cf_id, Bytes::from("key"), Bytes::from("value"), None)
            .unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].cf_id, cf_id);
    }

    #[test]
    fn should_support_compare_and_swap_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.compare_and_swap(
            Bytes::from("key"),
            Some(Bytes::from("expected")),
            Bytes::from("new_value"),
        )
        .unwrap();

        // Assert
        let mutations = txn.commit().unwrap();
        assert_eq!(mutations.len(), 1);
        assert!(matches!(mutations[0].op, MutationOp::CompareAndSwap));
    }
}
