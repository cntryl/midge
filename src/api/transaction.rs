use super::mutation::{Mutation, MutationOp};
use crate::core::transaction::{ConflictTracker, SpillManager};
use crate::error::MidgeError;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Synchronous transaction object that stages mutations in-memory.
///
/// This is a lightweight, crate-root Transaction intended to be used where
/// a full internal engine transaction API is not available. It implements
/// the common semantics: staged mutation list, last-write-wins local reads,
/// commit/rollback lifecycle, with optional conflict detection.
pub struct Transaction {
    txn_id: u64,
    begin_sequence: u64,
    commit_sequence: Option<u64>,
    staged: Vec<Mutation>,
    completed: bool,

    // ACID enhancements
    conflict_tracker: ConflictTracker,
    #[allow(dead_code)]
    created_at: Instant,
    deadline: Option<Instant>,

    // Spill-to-disk tracking
    memory_threshold: usize,
    current_memory: usize,
    spill_manager: SpillManager,
}

impl Transaction {
    pub fn new(txn_id: u64, begin_sequence: u64) -> Self {
        Self::with_options(txn_id, begin_sequence, None, 100 * 1024 * 1024)
    }

    pub fn with_options(
        txn_id: u64,
        begin_sequence: u64,
        timeout: Option<std::time::Duration>,
        memory_threshold: usize,
    ) -> Self {
        let created_at = Instant::now();
        let deadline = timeout.map(|d| created_at + d);

        Self {
            txn_id,
            begin_sequence,
            commit_sequence: None,
            staged: Vec::new(),
            completed: false,
            conflict_tracker: ConflictTracker::new(),
            created_at,
            deadline,
            memory_threshold,
            current_memory: 0,
            spill_manager: SpillManager::new(txn_id),
        }
    }

    pub fn txn_id(&self) -> u64 {
        self.txn_id
    }

    pub fn begin_sequence(&self) -> u64 {
        self.begin_sequence
    }

    #[inline]
    pub fn commit_sequence(&self) -> Option<u64> {
        self.commit_sequence
    }

    /// Check if transaction has exceeded deadline
    pub fn is_expired(&self) -> bool {
        if let Some(deadline) = self.deadline {
            Instant::now() > deadline
        } else {
            false
        }
    }

    /// Track a read operation for conflict detection
    pub fn track_read(&mut self, cf_id: u32, key: Bytes, version: u64) {
        self.conflict_tracker.track_read(cf_id, key, version);
    }

    /// Get the write set (keys modified by this transaction)
    pub fn write_set(&self) -> &HashSet<(u32, Bytes)> {
        self.conflict_tracker.write_set()
    }

    /// Get the read set (keys read by this transaction)
    pub fn read_set(&self) -> &HashSet<(u32, Bytes)> {
        self.conflict_tracker.read_set()
    }

    /// Get the read versions map (keys -> sequence numbers)
    pub fn read_versions(&self) -> &HashMap<(u32, Bytes), u64> {
        self.conflict_tracker.read_versions()
    }

    /// Get read version for a key
    pub fn read_version(&self, cf_id: u32, key: &[u8]) -> Option<u64> {
        self.conflict_tracker.read_version(cf_id, key)
    }

    /// Check if there's a write-write conflict with given write set
    pub fn has_write_conflict(&self, other_writes: &HashSet<(u32, Bytes)>) -> bool {
        self.conflict_tracker.has_write_conflict(other_writes)
    }

    fn update_memory_usage_and_spill(&mut self) -> Result<(), MidgeError> {
        // Calculate size of last staged mutation
        if let Some(mutation) = self.staged.last() {
            let size = mutation.key.len()
                + mutation.value.as_ref().map(|v| v.len()).unwrap_or(0)
                + mutation.range_end.as_ref().map(|v| v.len()).unwrap_or(0);
            self.current_memory += size;

            // Check if we need to spill to disk
            if self.current_memory >= self.memory_threshold {
                self.spill_to_disk()?;
            }
        }

        Ok(())
    }

    /// Spill currently staged mutations to a temporary file and clear memory
    fn spill_to_disk(&mut self) -> Result<(), MidgeError> {
        // Delegate to SpillManager
        self.spill_manager.spill_to_disk(&self.staged)?;

        // Clear staged mutations and reset memory counter
        self.staged.clear();
        self.current_memory = 0;

        Ok(())
    }

    /// Read mutations from spill files
    fn read_spill_files(&self) -> Result<Vec<Mutation>, MidgeError> {
        // Delegate to SpillManager
        self.spill_manager.read_spill_files()
    }

    /// Cleanup all spill files
    fn cleanup_spill_files(&mut self) {
        // Delegate to SpillManager
        self.spill_manager.cleanup_spill_files();
    }

    /// Get the number of spill files (for testing).
    #[cfg(test)]
    pub(crate) fn spill_file_count(&self) -> usize {
        self.spill_manager.spill_file_count()
    }

    /// Get the spill file paths (for testing).
    #[cfg(test)]
    pub(crate) fn spill_file_paths(&self) -> &[std::path::PathBuf] {
        self.spill_manager.spill_file_paths()
    }

    fn track_write(&mut self, cf_id: u32, key: Bytes) {
        self.conflict_tracker.track_write(cf_id, key);
    }

    #[inline]
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MidgeError> {
        self.put_cf(
            crate::api::DEFAULT_CF_ID,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            None,
        )
    }

    #[inline]
    pub fn put_with_ttl(
        &mut self,
        key: &[u8],
        value: &[u8],
        ttl: std::time::Duration,
    ) -> Result<(), MidgeError> {
        self.put_cf(
            crate::api::DEFAULT_CF_ID,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            Some(ttl),
        )
    }

    #[inline]
    pub fn put_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::put_cf(cf_id, key, value, ttl);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn insert(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), MidgeError> {
        self.insert_cf(crate::api::DEFAULT_CF_ID, key, value, ttl)
    }

    #[inline]
    pub fn insert_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::insert_cf(cf_id, key, value, ttl);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn delete(&mut self, key: Bytes) -> Result<(), MidgeError> {
        self.delete_cf(crate::api::DEFAULT_CF_ID, key)
    }

    #[inline]
    pub fn delete_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::delete_cf(cf_id, key);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn delete_range(&mut self, start: Bytes, end: Bytes) -> Result<(), MidgeError> {
        self.delete_range_cf(crate::api::DEFAULT_CF_ID, start, end)
    }

    #[inline]
    pub fn delete_range_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        start: Bytes,
        end: Bytes,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), start.clone());
        let mutation = Mutation::delete_range_cf(cf_id, start, end);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn compare_and_swap(
        &mut self,
        key: Bytes,
        expected: Option<Bytes>,
        new_value: Bytes,
    ) -> Result<(), MidgeError> {
        self.compare_and_swap_cf(crate::api::DEFAULT_CF_ID, key, expected, new_value)
    }

    #[inline]
    pub fn compare_and_swap_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
        expected: Option<Bytes>,
        new_value: Bytes,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::compare_and_swap_cf(cf_id, key, expected, new_value);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn merge(&mut self, key: Bytes, value: Bytes) -> Result<(), MidgeError> {
        self.merge_cf(crate::api::DEFAULT_CF_ID, key, value)
    }

    #[inline]
    pub fn merge_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
        value: Bytes,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::merge_cf(cf_id, key, value);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    /// Local get resolves staged mutations only: returns Some(Some(value)) if
    /// a staged Put/Insert is present, Some(None) if a staged Delete applies,
    /// or None if the key is not present in the staged set and caller should
    /// consult the underlying DB.
    ///
    /// Note: This only checks in-memory staged mutations for performance.
    /// Spill files are only read during commit.
    #[inline]
    pub fn get_local(&self, cf_id: u32, key: &[u8]) -> Option<Option<Bytes>> {
        for m in self.staged.iter().rev() {
            // Only consider staged mutations for the requested column family
            if m.cf_id.as_u32() != cf_id {
                continue;
            }
            match &m.op {
                MutationOp::DeleteRange => {
                    if let Some(end) = &m.range_end {
                        if m.key.as_ref() <= key && key < end.as_ref() {
                            return Some(None);
                        }
                    }
                }
                _ => {
                    if m.key.as_ref() == key {
                        return Some(m.value.clone());
                    }
                }
            }
        }
        None
    }

    pub fn exists_local(&self, cf_id: u32, key: &[u8]) -> bool {
        matches!(self.get_local(cf_id, key), Some(Some(_)))
    }

    /// Consume the transaction and return staged mutations for the caller
    /// to apply to the underlying engine. Marks the transaction completed.
    /// Merges mutations from spill files with in-memory staged mutations.
    pub fn commit(mut self) -> Result<Vec<Mutation>, MidgeError> {
        if self.completed {
            return Err(MidgeError::internal("transaction already completed"));
        }
        self.completed = true;

        // Read mutations from spill files if any exist
        let mut all_mutations = if self.spill_manager.has_spill_files() {
            self.read_spill_files()?
        } else {
            Vec::new()
        };

        // Append in-memory staged mutations
        all_mutations.extend(std::mem::take(&mut self.staged));

        // Cleanup spill files after successful read
        self.cleanup_spill_files();

        Ok(all_mutations)
    }

    /// Rollback clears staged mutations and marks completed.
    pub fn rollback(&mut self) {
        if !self.completed {
            self.staged.clear();
            self.cleanup_spill_files();
            self.completed = true;
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.completed {
            // Auto-rollback on drop for safety
            self.staged.clear();
            self.cleanup_spill_files();
            self.completed = true;
        } else {
            // Still cleanup spill files even if completed (defensive programming)
            self.cleanup_spill_files();
        }
    }
}

// Re-export common types so downstream code can `use cntryl_midge::Transaction;`
pub use Transaction as Tx;

// Implement the public KvTransaction trait for the crate Transaction so that the
// crate-local Transaction can be used wherever the generic KvTransaction trait
// is expected by external integrations (though reads won't work without engine reference).
impl super::kv_store::KvTransaction for Transaction {
    fn put(&mut self, key: &[u8], value: &[u8]) -> crate::MidgeResult<()> {
        Transaction::put(self, key, value)
    }

    fn get(&mut self, _key: &[u8]) -> crate::MidgeResult<Option<Bytes>> {
        Err(crate::MidgeError::internal(
            "Transaction reads require engine context. Use KvStore::begin_transaction() instead.",
        ))
    }

    fn delete(&mut self, key: &[u8]) -> crate::MidgeResult<()> {
        Transaction::delete(self, Bytes::copy_from_slice(key))
    }

    fn scan(&mut self, _start: &[u8], _end: &[u8]) -> crate::MidgeResult<Vec<(Bytes, Bytes)>> {
        Err(crate::MidgeError::internal(
            "Transaction scans require engine context. Use KvStore::begin_transaction() instead.",
        ))
    }

    fn delete_range(&mut self, start: &[u8], end: &[u8]) -> crate::MidgeResult<()> {
        Transaction::delete_range(
            self,
            Bytes::copy_from_slice(start),
            Bytes::copy_from_slice(end),
        )
    }

    fn compare_and_swap(
        &mut self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> crate::MidgeResult<bool> {
        Transaction::compare_and_swap(
            self,
            Bytes::copy_from_slice(key),
            expected.map(Bytes::copy_from_slice),
            Bytes::copy_from_slice(new_value),
        )?;
        // For now, always return true since validation happens at commit time
        Ok(true)
    }

    fn merge(&mut self, key: &[u8], value: &[u8]) -> crate::MidgeResult<()> {
        Transaction::merge(
            self,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
        )
    }

    fn into_transaction(
        self: Box<Self>,
    ) -> Result<Transaction, Box<dyn super::kv_store::KvTransaction>> {
        // This is already a Transaction, so just return it
        Ok(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Transaction Write-Set Tracking Tests
    // ========================================================================

    #[test]
    fn should_track_write_set_given_put_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(Bytes::from("key1"), Bytes::from("value1")).unwrap();
        txn.put(Bytes::from("key2"), Bytes::from("value2")).unwrap();

        // Assert
        assert!(txn
            .write_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"))));
        assert!(txn
            .write_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"))));
        assert_eq!(txn.write_set().len(), 2);
    }

    #[test]
    fn should_track_write_set_given_delete_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.delete(Bytes::from("deleted_key")).unwrap();

        // Assert
        assert!(txn.write_set().contains(&(
            crate::api::DEFAULT_CF_ID.as_u32(),
            Bytes::from("deleted_key")
        )));
    }

    #[test]
    fn should_track_write_set_given_delete_range_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.delete_range(Bytes::from("start"), Bytes::from("end"))
            .unwrap();

        // Assert
        assert!(txn
            .write_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("start"))));
    }

    #[test]
    fn should_not_duplicate_keys_in_write_set_given_multiple_puts() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(Bytes::from("key"), Bytes::from("v1")).unwrap();
        txn.put(Bytes::from("key"), Bytes::from("v2")).unwrap();
        txn.put(Bytes::from("key"), Bytes::from("v3")).unwrap();

        // Assert
        assert_eq!(txn.write_set().len(), 1);
    }

    // ========================================================================
    // Transaction Read-Set Tracking Tests
    // ========================================================================

    #[test]
    fn should_track_read_set_given_track_read_called() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"), 50);
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"), 75);

        // Assert
        assert!(txn
            .read_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"))));
        assert!(txn
            .read_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"))));
    }

    #[test]
    fn should_store_read_version_given_track_read_called() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key"), 42);

        // Assert
        assert_eq!(
            txn.read_version(crate::api::DEFAULT_CF_ID.as_u32(), b"key"),
            Some(42)
        );
    }

    #[test]
    fn should_update_read_version_given_multiple_reads() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key"), 10);
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key"), 20);

        // Assert
        assert_eq!(
            txn.read_version(crate::api::DEFAULT_CF_ID.as_u32(), b"key"),
            Some(20)
        );
    }

    #[test]
    fn should_return_none_given_key_not_read() {
        // Arrange
        let txn = Transaction::new(1, 100);

        // Act
        let version = txn.read_version(crate::api::DEFAULT_CF_ID.as_u32(), b"never_read");

        // Assert
        assert_eq!(version, None);
    }

    // ========================================================================
    // Conflict Detection Tests
    // ========================================================================

    #[test]
    fn should_detect_write_conflict_given_overlapping_write_sets() {
        // Arrange
        let mut txn1 = Transaction::new(1, 100);
        let mut txn2 = Transaction::new(2, 100);

        txn1.put(Bytes::from("key1"), Bytes::from("v1")).unwrap();
        txn1.put(Bytes::from("key2"), Bytes::from("v2")).unwrap();

        txn2.put(Bytes::from("key2"), Bytes::from("v3")).unwrap();
        txn2.put(Bytes::from("key3"), Bytes::from("v4")).unwrap();

        // Act
        let has_conflict = txn1.has_write_conflict(txn2.write_set());

        // Assert
        assert!(has_conflict, "Should detect conflict on key2");
    }

    #[test]
    fn should_not_detect_conflict_given_disjoint_write_sets() {
        // Arrange
        let mut txn1 = Transaction::new(1, 100);
        let mut txn2 = Transaction::new(2, 100);

        txn1.put(Bytes::from("key1"), Bytes::from("v1")).unwrap();
        txn2.put(Bytes::from("key2"), Bytes::from("v2")).unwrap();

        // Act
        let has_conflict = txn1.has_write_conflict(txn2.write_set());

        // Assert
        assert!(!has_conflict);
    }

    #[test]
    fn should_detect_conflict_given_delete_and_put_same_key() {
        // Arrange
        let mut txn1 = Transaction::new(1, 100);
        let mut txn2 = Transaction::new(2, 100);

        txn1.delete(Bytes::from("key")).unwrap();
        txn2.put(Bytes::from("key"), Bytes::from("value")).unwrap();

        // Act
        let has_conflict = txn1.has_write_conflict(txn2.write_set());

        // Assert
        assert!(has_conflict);
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
        let txn = Transaction::with_options(1, 100, Some(std::time::Duration::from_secs(10)), 1024);

        // Act
        let expired = txn.is_expired();

        // Assert
        assert!(!expired);
    }

    #[test]
    fn should_expire_given_deadline_exceeded() {
        // Arrange
        let txn = Transaction::with_options(1, 100, Some(std::time::Duration::from_nanos(1)), 1024);

        // Act
        std::thread::sleep(std::time::Duration::from_millis(1));
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
        txn.put(Bytes::from("key"), Bytes::from("value")).unwrap();

        // Assert
        assert!(txn.current_memory > 0);
    }

    #[test]
    fn should_accumulate_memory_given_multiple_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(Bytes::from("k1"), Bytes::from("v1")).unwrap();
        let mem1 = txn.current_memory;

        txn.put(Bytes::from("k2"), Bytes::from("v2")).unwrap();
        let mem2 = txn.current_memory;

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
        assert!(txn.current_memory > 0);
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
    fn should_clear_staged_mutations_given_rollback() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(Bytes::from("key"), Bytes::from("value")).unwrap();

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
        txn.put(Bytes::from("k1"), Bytes::from("v1")).unwrap();
        txn.put(Bytes::from("k2"), Bytes::from("v2")).unwrap();

        // Act
        let mutations = txn.commit().expect("commit");

        // Assert
        assert_eq!(mutations.len(), 2);
    }

    #[test]
    fn should_have_correct_sequence_numbers_given_new_transaction() {
        // Arrange
        let begin_seq = 42;

        // Act
        let txn = Transaction::new(1, begin_seq);

        // Assert
        assert_eq!(txn.begin_sequence(), begin_seq);
        assert_eq!(txn.commit_sequence(), None);
    }

    #[test]
    fn should_return_read_versions_given_tracked_reads() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"), 50);
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"), 75);

        // Assert
        let read_versions = txn.read_versions();
        assert_eq!(read_versions.len(), 2);
        assert_eq!(
            read_versions.get(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"))),
            Some(&50)
        );
        assert_eq!(
            read_versions.get(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"))),
            Some(&75)
        );
    }

    // ========================================================================
    // Spill-to-Disk Tests
    // ========================================================================

    #[test]
    fn should_spill_to_disk_given_exceed_threshold_when_staging_writes() {
        // Arrange
        let memory_threshold = 100; // Very small threshold to trigger spill
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = Bytes::from(vec![b'x'; 200]); // Larger than threshold

        // Act
        txn.put(Bytes::from("key1"), large_value.clone()).unwrap();

        // Assert
        assert_eq!(
            txn.spill_file_count(),
            1,
            "Should have created one spill file"
        );
        assert_eq!(txn.current_memory, 0, "Memory should be reset after spill");
        assert_eq!(
            txn.staged.len(),
            0,
            "Staged mutations should be cleared after spill"
        );

        // Verify spill file exists
        assert!(
            txn.spill_file_paths()[0].exists(),
            "Spill file should exist on disk"
        );
    }

    #[test]
    fn should_read_from_spill_file_given_large_transaction_when_get() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let value1 = Bytes::from(vec![b'a'; 100]);
        let value2 = Bytes::from(vec![b'b'; 20]);

        // Act
        txn.put(Bytes::from("spilled_key"), value1.clone()).unwrap();
        txn.put(Bytes::from("memory_key"), value2.clone()).unwrap();
        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(
            mutations.len(),
            2,
            "Should have both spilled and in-memory mutations"
        );

        // Verify mutations are in correct order
        assert_eq!(mutations[0].key, Bytes::from("spilled_key"));
        assert_eq!(mutations[0].value, Some(value1));
        assert_eq!(mutations[1].key, Bytes::from("memory_key"));
        assert_eq!(mutations[1].value, Some(value2));
    }

    #[test]
    fn should_cleanup_spill_file_given_transaction_commit_when_completed() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = Bytes::from(vec![b'x'; 100]);

        // Act
        txn.put(Bytes::from("key"), large_value).unwrap();

        let spill_path = txn.spill_file_paths()[0].to_path_buf();
        assert!(spill_path.exists(), "Spill file should exist before commit");

        txn.commit().unwrap();

        // Assert
        assert!(
            !spill_path.exists(),
            "Spill file should be cleaned up after commit"
        );
    }

    #[test]
    fn should_cleanup_spill_file_given_transaction_abort_when_rolled_back() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = Bytes::from(vec![b'x'; 100]);

        // Act
        txn.put(Bytes::from("key"), large_value).unwrap();

        let spill_path = txn.spill_file_paths()[0].to_path_buf();
        assert!(
            spill_path.exists(),
            "Spill file should exist before rollback"
        );

        txn.rollback();

        // Assert
        assert!(
            !spill_path.exists(),
            "Spill file should be cleaned up after rollback"
        );
        assert_eq!(
            txn.spill_file_count(),
            0,
            "Spill files list should be empty"
        );
    }

    #[test]
    fn should_handle_multiple_spill_files_given_very_large_transaction() {
        // Arrange
        let memory_threshold = 100;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = Bytes::from(vec![b'x'; 150]);

        // Act
        txn.put(Bytes::from("key1"), large_value.clone()).unwrap();
        assert_eq!(
            txn.spill_file_count(),
            1,
            "Should have one spill file after first write"
        );

        txn.put(Bytes::from("key2"), large_value.clone()).unwrap();
        assert_eq!(
            txn.spill_file_count(),
            2,
            "Should have two spill files after second write"
        );

        txn.put(Bytes::from("key3"), large_value.clone()).unwrap();

        // Assert
        assert_eq!(txn.spill_file_count(), 3, "Should have three spill files");

        // Verify all spill files exist
        for spill_path in txn.spill_file_paths() {
            assert!(spill_path.exists(), "Each spill file should exist");
        }

        // Commit and verify all data is merged
        let mutations = txn.commit().unwrap();
        assert_eq!(
            mutations.len(),
            3,
            "Should have all mutations from all spill files"
        );
    }

    #[test]
    fn should_preserve_mutation_order_given_spill_and_memory_mutations() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        // Act
        txn.put(Bytes::from("key1"), Bytes::from(vec![b'a'; 100]))
            .unwrap(); // Spill
        txn.put(Bytes::from("key2"), Bytes::from("small")).unwrap(); // Memory
        txn.put(Bytes::from("key3"), Bytes::from(vec![b'b'; 100]))
            .unwrap(); // Spill
        txn.put(Bytes::from("key4"), Bytes::from("tiny")).unwrap(); // Memory

        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(mutations.len(), 4);
        assert_eq!(mutations[0].key, Bytes::from("key1"));
        assert_eq!(mutations[1].key, Bytes::from("key2"));
        assert_eq!(mutations[2].key, Bytes::from("key3"));
        assert_eq!(mutations[3].key, Bytes::from("key4"));
    }

    #[test]
    fn should_cleanup_spill_files_on_drop_given_incomplete_transaction() {
        // Arrange
        let memory_threshold = 50;
        let spill_path;

        // Act
        {
            let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
            let large_value = Bytes::from(vec![b'x'; 100]);
            txn.put(Bytes::from("key"), large_value).unwrap();

            spill_path = txn.spill_file_paths()[0].to_path_buf();
            assert!(spill_path.exists(), "Spill file should exist before drop");

            // Transaction dropped here without commit or rollback
        }

        // Assert
        assert!(
            !spill_path.exists(),
            "Spill file should be cleaned up on drop"
        );
    }

    #[test]
    fn should_handle_delete_operations_in_spill_file() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        txn.put(Bytes::from("key1"), Bytes::from(vec![b'a'; 100]))
            .unwrap();
        txn.delete(Bytes::from("key2")).unwrap(); // Small, stays in memory
        txn.put(Bytes::from("key3"), Bytes::from(vec![b'b'; 100]))
            .unwrap();

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
        txn.put(Bytes::from("key1"), Bytes::from(vec![b'a'; 100]))
            .unwrap();
        txn.delete_range(Bytes::from("start"), Bytes::from("end"))
            .unwrap();

        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(mutations.len(), 2);
        assert!(matches!(mutations[0].op, MutationOp::Put));
        assert!(matches!(mutations[1].op, MutationOp::DeleteRange));
        assert_eq!(mutations[1].range_end, Some(Bytes::from("end")));
    }
}
