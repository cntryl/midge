//! Transaction API for multi-key ACID operations
//!
//! Provides transaction support with:
//! - Multi-key atomic operations
//! - Snapshot isolation with repeatable reads
//! - Rollback and commit semantics
//! - Column-family scoped transactions

use crate::common::{MidgeError, MidgeResult};
use crate::engine::ColumnFamilyId;
use std::collections::{BTreeMap, HashMap};

/// Read set entry: (value, sequence number)
type ReadSetEntry = (Option<Vec<u8>>, u64);

/// Read set: (cf_id, key) → (value, sequence)
type ReadSet = HashMap<(ColumnFamilyId, Vec<u8>), ReadSetEntry>;

/// Transaction mode controls read/write capabilities
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionMode {
    /// Read-only transaction; writes forbidden
    ReadOnly,
    /// Read-write transaction; all operations allowed
    ReadWrite,
}

/// Isolation level for transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Dirty reads allowed; no consistency guarantees
    ReadUncommitted,
    /// No dirty reads; consistent reads at commit time
    ReadCommitted,
    /// Full snapshot isolation; LWW-based with sequence-number visibility
    #[default]
    Serializable,
}

/// Transaction state machine
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction active; reads/writes allowed
    Active,
    /// Read phase complete; waiting to commit
    ReadPhase,
    /// Commit in progress
    Committing,
    /// Successfully committed
    Committed,
    /// Rolled back before commit
    RolledBack,
    /// Commit failed; rolled back
    CommitFailed,
}

/// Write intent: pending put, insert, delete, or delete_range operation
#[derive(Debug, Clone)]
pub enum WriteIntent {
    /// Put operation (upsert)
    Put {
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
        sequence: u64,
    },
    /// Insert operation (error if exists)
    Insert {
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
        sequence: u64,
    },
    /// Delete operation
    Delete {
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64,
    },
    /// Delete range operation [start_key, end_key)
    DeleteRange {
        cf_id: ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        sequence: u64,
    },
}

impl WriteIntent {
    /// Create a put intent (upsert)
    pub fn put(
        cf_id: ColumnFamilyId,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    ) -> Self {
        Self::Put {
            cf_id,
            key,
            value,
            ttl_seconds,
            sequence: 0,
        }
    }

    /// Create an insert intent (error if exists)
    pub fn insert(
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
            sequence: 0,
        }
    }

    /// Create a delete intent
    pub fn delete(cf_id: ColumnFamilyId, key: Vec<u8>) -> Self {
        Self::Delete {
            cf_id,
            key,
            sequence: 0,
        }
    }

    /// Create a delete_range intent
    pub fn delete_range(cf_id: ColumnFamilyId, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        Self::DeleteRange {
            cf_id,
            start_key,
            end_key,
            sequence: 0,
        }
    }

    pub fn cf_id(&self) -> ColumnFamilyId {
        match self {
            Self::Put { cf_id, .. }
            | Self::Insert { cf_id, .. }
            | Self::Delete { cf_id, .. }
            | Self::DeleteRange { cf_id, .. } => *cf_id,
        }
    }

    pub fn key(&self) -> Option<&[u8]> {
        match self {
            Self::Put { key, .. }
            | Self::Insert { key, .. }
            | Self::Delete { key, .. } => Some(key),
            Self::DeleteRange { .. } => None,
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. } | Self::Insert { value, .. } => Some(value),
            _ => None,
        }
    }

    pub fn ttl_seconds(&self) -> Option<u64> {
        match self {
            Self::Put { ttl_seconds, .. } | Self::Insert { ttl_seconds, .. } => *ttl_seconds,
            _ => None,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    pub fn is_put(&self) -> bool {
        matches!(self, Self::Put { .. })
    }

    pub fn is_insert(&self) -> bool {
        matches!(self, Self::Insert { .. })
    }

    pub fn is_delete_range(&self) -> bool {
        matches!(self, Self::DeleteRange { .. })
    }

    pub fn sequence(&self) -> u64 {
        match self {
            Self::Put { sequence, .. }
            | Self::Insert { sequence, .. }
            | Self::Delete { sequence, .. }
            | Self::DeleteRange { sequence, .. } => *sequence,
        }
    }

    pub fn set_sequence(&mut self, seq: u64) {
        match self {
            Self::Put { sequence, .. }
            | Self::Insert { sequence, .. }
            | Self::Delete { sequence, .. }
            | Self::DeleteRange { sequence, .. } => *sequence = seq,
        }
    }

    pub fn start_key(&self) -> Option<&[u8]> {
        match self {
            Self::DeleteRange { start_key, .. } => Some(start_key),
            _ => None,
        }
    }

    pub fn end_key(&self) -> Option<&[u8]> {
        match self {
            Self::DeleteRange { end_key, .. } => Some(end_key),
            _ => None,
        }
    }
}

/// Transaction for multi-key ACID operations
///
/// Collects read and write intents, validates them, and commits atomically.
/// Thread-safe; multiple transactions can be in flight simultaneously.
pub struct Transaction {
    /// Pointer to the engine (not owned, engine must outlive transaction)
    engine: *const crate::engine::MidgeEngine,
    /// Unique transaction ID
    id: u64,
    /// Column family this transaction is bound to
    cf_id: ColumnFamilyId,
    /// Transaction mode (ReadOnly or ReadWrite)
    mode: TransactionMode,
    /// Isolation level
    isolation: IsolationLevel,
    /// Current state
    state: TransactionState,
    /// Read set: (cf_id, key) → (value, sequence)
    read_set: ReadSet,
    /// Write set: sequence of write intents
    write_set: Vec<WriteIntent>,
    /// Start sequence number (snapshot point)
    start_sequence: u64,
    /// Commit sequence number (filled on commit)
    commit_sequence: Option<u64>,
}

impl Transaction {
    /// Create a new transaction with the given ID, mode, and isolation level
    pub fn new(
        engine: *const crate::engine::MidgeEngine,
        id: u64,
        cf_id: ColumnFamilyId,
        mode: TransactionMode,
        isolation: IsolationLevel,
        start_sequence: u64,
    ) -> Self {
        Self {
            engine,
            id,
            cf_id,
            mode,
            isolation,
            state: TransactionState::Active,
            read_set: HashMap::new(),
            write_set: Vec::new(),
            start_sequence,
            commit_sequence: None,
        }
    }

    /// Get a reference to the engine (unsafe, but lifetime is guaranteed by engine ownership)
    fn engine(&self) -> &crate::engine::MidgeEngine {
        unsafe { &*self.engine }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn cf_id(&self) -> ColumnFamilyId {
        self.cf_id
    }

    pub fn mode(&self) -> TransactionMode {
        self.mode
    }

    pub fn is_read_only(&self) -> bool {
        matches!(self.mode, TransactionMode::ReadOnly)
    }

    pub fn is_read_write(&self) -> bool {
        matches!(self.mode, TransactionMode::ReadWrite)
    }

    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation
    }

    pub fn state(&self) -> TransactionState {
        self.state
    }

    pub fn start_sequence(&self) -> u64 {
        self.start_sequence
    }

    pub fn commit_sequence(&self) -> Option<u64> {
        self.commit_sequence
    }

    /// Add a read to the transaction's read set
    pub fn read(
        &mut self,
        cf_id: ColumnFamilyId,
        key: &[u8],
        value: Option<Vec<u8>>,
        sequence: u64,
    ) -> MidgeResult<()> {
        if self.state != TransactionState::Active {
            return Err(crate::common::MidgeError::InvalidArgument(format!(
                "Cannot read in {:?} state",
                self.state
            )));
        }
        self.read_set
            .insert((cf_id, key.to_vec()), (value, sequence));
        Ok(())
    }

    /// Add a put (upsert) to the transaction's write set
    pub fn put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<()> {
        if self.state != TransactionState::Active {
            return Err(MidgeError::InvalidArgument(format!(
                "Cannot write in {:?} state",
                self.state
            )));
        }
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
        if self.state != TransactionState::Active {
            return Err(MidgeError::InvalidArgument(format!(
                "Cannot write in {:?} state",
                self.state
            )));
        }
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
        if self.state != TransactionState::Active {
            return Err(MidgeError::InvalidArgument(format!(
                "Cannot write in {:?} state",
                self.state
            )));
        }
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
        if self.state != TransactionState::Active {
            return Err(MidgeError::InvalidArgument(format!(
                "Cannot write in {:?} state",
                self.state
            )));
        }
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot write in ReadOnly transaction".to_string(),
            ));
        }
        self.write_set
            .push(WriteIntent::delete_range(self.cf_id, start_key, end_key));
        Ok(())
    }

    /// Get a value within this transaction (read-your-own-writes semantics)
    ///
    /// This is the primary way to read data. Checks the transaction's write set first,
    /// then falls back to the engine state at the transaction's snapshot sequence.
    pub fn get(&self, key: &[u8]) -> MidgeResult<Option<bytes::Bytes>> {
        // Check transaction's write set first (read-your-own-writes)
        if let Some(value_opt) = self.get_from_write_set(self.cf_id, key) {
            return Ok(value_opt.map(bytes::Bytes::from));
        }

        // Fall back to engine state at transaction's snapshot sequence
        self.engine()
            .read_at_sequence(self.cf_id, key, self.start_sequence)
    }

    /// Range scan within this transaction
    ///
    /// Returns all key-value pairs in the range [start, end) visible at this transaction's
    /// snapshot sequence.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        let base_results =
            self.engine()
                .scan_at_sequence(self.cf_id, start, end, self.start_sequence)?;

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
                    let start = start_key.as_slice();
                    let end = end_key.as_slice();
                    merged.retain(|existing_key, _| {
                        existing_key.as_slice() < start || existing_key.as_slice() >= end
                    });
                }
                _ => {}
            }
        }

        let mut results = Vec::new();
        for (key, value_opt) in merged {
            if let Some(value) = value_opt {
                results.push((bytes::Bytes::from(key), value));
            }
        }

        Ok(results)
    }

    /// Range scan with Query parameters within this transaction
    pub fn scan_range(
        &self,
        query: &super::Query,
    ) -> MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        // Use the effective start/end from the query
        let start_owned;
        let start = if let Some(s) = query.effective_start() {
            s
        } else {
            start_owned = vec![];
            &start_owned[..]
        };

        let end_vec = query.effective_end();
        let end_sentinel = vec![0xFFu8; 256];
        let end = if let Some(ref e) = end_vec {
            &e[..]
        } else if query.prefix.is_none() && query.end.is_none() {
            &end_sentinel[..]
        } else {
            &[][..]
        };

        let mut results = self.scan(start, end)?;

        // Apply limit if specified
        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Compare-and-swap operation within this transaction
    ///
    /// Atomically checks if the current value matches `expected`, and if so, puts `new_value`.
    /// Returns whether the swap succeeded.
    pub fn compare_and_swap(
        &mut self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        new_value: Vec<u8>,
        ttl_seconds: Option<u64>,
    ) -> MidgeResult<bool> {
        if self.state != TransactionState::Active {
            return Err(MidgeError::InvalidArgument(format!(
                "Cannot compare_and_swap in {:?} state",
                self.state
            )));
        }
        if self.is_read_only() {
            return Err(MidgeError::InvalidArgument(
                "Cannot compare_and_swap in ReadOnly transaction".to_string(),
            ));
        }

        // Get current value (from write set or engine)
        let current = self.get(&key)?;

        // Check if current matches expected
        let matches = match (&current, &expected) {
            (None, None) => true,
            (Some(curr), Some(exp)) => curr.as_ref() == exp.as_slice(),
            _ => false,
        };

        if matches {
            // Swap succeeded - add put to write set
            self.put(key, new_value, ttl_seconds)?;
            Ok(true)
        } else {
            // Swap failed - current value doesn't match expected
            Ok(false)
        }
    }

    /// Read a key from the transaction's write set (read-your-own-writes)
    ///
    /// Returns the value from write intents if present (including tombstones).
    /// Returns None if key not in write set (caller should check engine).
    pub fn get_from_write_set(&self, cf_id: ColumnFamilyId, key: &[u8]) -> Option<Option<Vec<u8>>> {
        // Scan write set in reverse (most recent write wins)
        for intent in self.write_set.iter().rev() {
            if intent.cf_id() == cf_id {
                match intent {
                    WriteIntent::Put { key: k, value, .. } if k.as_slice() == key => {
                        return Some(Some(value.clone()));
                    }
                    WriteIntent::Delete { key: k, .. } if k.as_slice() == key => {
                        return Some(None); // Tombstone
                    }
                    WriteIntent::DeleteRange {
                        start_key, end_key, ..
                    } => {
                        // Check if key falls in range [start_key, end_key)
                        if key >= start_key.as_slice() && key < end_key.as_slice() {
                            return Some(None); // Deleted by range
                        }
                    }
                    _ => {}
                }
            }
        }
        None // Key not in write set
    }

    /// Get the read set
    pub fn read_set(&self) -> &ReadSet {
        &self.read_set
    }

    /// Get the write set
    pub fn write_set(&self) -> &[WriteIntent] {
        &self.write_set
    }

    /// Iterate over write intents
    pub fn iter_writes(&self) -> impl Iterator<Item = &WriteIntent> {
        self.write_set.iter()
    }

    /// Get write count
    pub fn write_count(&self) -> usize {
        self.write_set.len()
    }

    /// Get read count
    pub fn read_count(&self) -> usize {
        self.read_set.len()
    }

    /// Check if transaction has any writes
    pub fn has_writes(&self) -> bool {
        !self.write_set.is_empty()
    }

    /// Check if transaction has any reads
    pub fn has_reads(&self) -> bool {
        !self.read_set.is_empty()
    }

    /// Result-like expect wrapper for Transaction errors
    ///
    /// Returns self if transaction is valid and active.
    /// Panics with message if transaction is in invalid state.
    pub fn expect(self, msg: &str) -> Self {
        if self.state == TransactionState::Active {
            self
        } else {
            panic!("{}: transaction in {:?} state", msg, self.state)
        }
    }

    /// Mark transaction as committed
    pub fn mark_committed(&mut self, commit_seq: u64) -> MidgeResult<()> {
        if self.state != TransactionState::Committing {
            return Err(crate::common::MidgeError::InvalidArgument(format!(
                "Cannot commit from {:?} state",
                self.state
            )));
        }
        self.state = TransactionState::Committed;
        self.commit_sequence = Some(commit_seq);
        Ok(())
    }

    /// Mark transaction as rolled back
    pub fn mark_rolled_back(&mut self) -> MidgeResult<()> {
        self.state = TransactionState::RolledBack;
        self.write_set.clear();
        self.read_set.clear();
        Ok(())
    }

    /// Mark transaction as failed
    pub fn mark_failed(&mut self) -> MidgeResult<()> {
        self.state = TransactionState::CommitFailed;
        self.write_set.clear();
        self.read_set.clear();
        Ok(())
    }

    /// Transition to read phase
    pub fn enter_read_phase(&mut self) -> MidgeResult<()> {
        if self.state != TransactionState::Active {
            return Err(crate::common::MidgeError::InvalidArgument(format!(
                "Cannot enter read phase from {:?} state",
                self.state
            )));
        }
        self.state = TransactionState::ReadPhase;
        Ok(())
    }

    /// Transition to commit phase
    pub fn enter_commit_phase(&mut self) -> MidgeResult<()> {
        if self.state != TransactionState::ReadPhase {
            return Err(crate::common::MidgeError::InvalidArgument(format!(
                "Cannot enter commit phase from {:?} state",
                self.state
            )));
        }
        self.state = TransactionState::Committing;
        Ok(())
    }

    /// Clear the transaction state (for reuse)
    pub fn clear(&mut self) {
        self.read_set.clear();
        self.write_set.clear();
        self.commit_sequence = None;
    }
}

/// Auto-rollback on drop if not already committed/rolled back
impl Drop for Transaction {
    fn drop(&mut self) {
        // If transaction is still active or in read phase, automatically rollback
        match self.state {
            TransactionState::Active
            | TransactionState::ReadPhase
            | TransactionState::Committing => {
                let _ = self.mark_rolled_back();
            }
            TransactionState::Committed
            | TransactionState::RolledBack
            | TransactionState::CommitFailed => {
                // Already in a terminal state, nothing to do
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_transaction_when_given_id() {
        // Arrange
        let engine_ptr = std::ptr::null();
        let id = 42;
        let cf_id = ColumnFamilyId::DEFAULT;
        let mode = TransactionMode::ReadWrite;
        let isolation = IsolationLevel::Serializable;
        let start_seq = 100;

        // Act
        let txn = Transaction::new(engine_ptr, id, cf_id, mode, isolation, start_seq);

        // Assert
        assert_eq!(txn.id(), id);
        assert_eq!(txn.cf_id(), cf_id);
        assert_eq!(txn.isolation_level(), isolation);
        assert_eq!(txn.start_sequence(), start_seq);
        assert_eq!(txn.state(), TransactionState::Active);
        assert_eq!(txn.write_count(), 0);
        assert_eq!(txn.read_count(), 0);
    }

    #[test]
    fn should_add_puts_to_write_set_when_put_called() {
        // Arrange
        let mut txn = Transaction::new(
            std::ptr::null(),
            1,
            ColumnFamilyId::DEFAULT,
            TransactionMode::ReadWrite,
            IsolationLevel::Serializable,
            0,
        );
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        // Act
        txn.put(key.clone(), value.clone(), None).unwrap();

        // Assert
        assert_eq!(txn.write_count(), 1);
        assert!(txn.has_writes());
        assert!(txn.iter_writes().next().unwrap().is_put());
    }

    #[test]
    fn should_add_deletes_to_write_set_when_delete_called() {
        // Arrange
        let mut txn = Transaction::new(
            std::ptr::null(),
            1,
            ColumnFamilyId::DEFAULT,
            TransactionMode::ReadWrite,
            IsolationLevel::Serializable,
            0,
        );
        let key = vec![1, 2, 3];

        // Act
        txn.delete(key.clone()).unwrap();

        // Assert
        assert_eq!(txn.write_count(), 1);
        assert!(txn.has_writes());
        assert!(txn.iter_writes().next().unwrap().is_delete());
    }

    #[test]
    fn should_track_reads_in_read_set_when_read_called() {
        // Arrange
        let mut txn = Transaction::new(
            std::ptr::null(),
            1,
            ColumnFamilyId::DEFAULT,
            TransactionMode::ReadWrite,
            IsolationLevel::Serializable,
            0,
        );
        let cf_id = ColumnFamilyId::DEFAULT;
        let key = vec![1, 2, 3];
        let value = Some(vec![4, 5, 6]);
        let sequence = 50;

        // Act
        txn.read(cf_id, &key, value.clone(), sequence).unwrap();

        // Assert
        assert_eq!(txn.read_count(), 1);
        assert!(txn.has_reads());
        assert_eq!(
            txn.read_set().get(&(cf_id, key)).map(|r| r.1),
            Some(sequence)
        );
    }

    #[test]
    fn should_transition_through_states_when_commit_sequence_executed() {
        // Arrange
        let mut txn = Transaction::new(
            std::ptr::null(),
            1,
            ColumnFamilyId::DEFAULT,
            TransactionMode::ReadWrite,
            IsolationLevel::Serializable,
            0,
        );
        txn.put(vec![1], vec![2], None).unwrap();

        // Act
        txn.enter_read_phase().unwrap();
        assert_eq!(txn.state(), TransactionState::ReadPhase);

        txn.enter_commit_phase().unwrap();
        assert_eq!(txn.state(), TransactionState::Committing);

        txn.mark_committed(200).unwrap();

        // Assert
        assert_eq!(txn.state(), TransactionState::Committed);
        assert_eq!(txn.commit_sequence(), Some(200));
    }

    #[test]
    fn should_support_mixed_operations_when_put_and_delete_called() {
        // Arrange
        let mut txn = Transaction::new(
            std::ptr::null(),
            1,
            ColumnFamilyId::DEFAULT,
            TransactionMode::ReadWrite,
            IsolationLevel::Serializable,
            0,
        );

        // Act
        txn.put(vec![1], vec![100], None).unwrap();
        txn.delete(vec![2]).unwrap();
        txn.put(vec![3], vec![200], None).unwrap();
        txn.delete(vec![4]).unwrap();

        // Assert
        assert_eq!(txn.write_count(), 4);
        let writes: Vec<_> = txn.iter_writes().collect();
        assert!(writes[0].is_put());
        assert!(writes[1].is_delete());
        assert!(writes[2].is_put());
        assert!(writes[3].is_delete());
    }

    #[test]
    fn should_reject_operations_when_not_active() {
        // Arrange
        let mut txn = Transaction::new(
            std::ptr::null(),
            1,
            ColumnFamilyId::DEFAULT,
            TransactionMode::ReadWrite,
            IsolationLevel::Serializable,
            0,
        );

        // Act
        txn.enter_read_phase().unwrap();
        let put_result = txn.put(vec![1], vec![2], None);
        let delete_result = txn.delete(vec![3]);
        let read_result = txn.read(ColumnFamilyId::DEFAULT, &[4], None, 0);

        // Assert
        assert!(put_result.is_err());
        assert!(delete_result.is_err());
        assert!(read_result.is_err());
    }

    #[test]
    fn should_clear_state_when_clear_called() {
        // Arrange
        let mut txn = Transaction::new(
            std::ptr::null(),
            1,
            ColumnFamilyId::DEFAULT,
            TransactionMode::ReadWrite,
            IsolationLevel::Serializable,
            0,
        );
        txn.put(vec![1], vec![100], None).unwrap();
        txn.read(ColumnFamilyId::DEFAULT, &[2], Some(vec![200]), 50)
            .unwrap();
        assert_eq!(txn.write_count(), 1);
        assert_eq!(txn.read_count(), 1);

        // Act
        txn.clear();

        // Assert
        assert_eq!(txn.write_count(), 0);
        assert_eq!(txn.read_count(), 0);
    }

    #[test]
    fn should_rollback_transaction_when_mark_rolled_back_called() {
        // Arrange
        let mut txn = Transaction::new(
            std::ptr::null(),
            1,
            ColumnFamilyId::DEFAULT,
            TransactionMode::ReadWrite,
            IsolationLevel::Serializable,
            0,
        );
        txn.put(vec![1], vec![100], None).unwrap();

        // Act
        txn.mark_rolled_back().unwrap();

        // Assert
        assert_eq!(txn.state(), TransactionState::RolledBack);
        assert_eq!(txn.write_count(), 0);
        assert_eq!(txn.read_count(), 0);
    }
}
