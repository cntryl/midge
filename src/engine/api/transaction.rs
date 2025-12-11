//! Transaction API for multi-key ACID operations
//!
//! Provides transaction support with:
//! - Multi-key atomic operations
//! - Isolation levels (Read Uncommitted, Read Committed, Serializable)
//! - Rollback and commit semantics
//! - MVCC-based consistency

use crate::common::MidgeResult;
use crate::engine::ColumnFamilyId;
use std::collections::HashMap;

/// Read set entry: (value, sequence number)
type ReadSetEntry = (Option<Vec<u8>>, u64);

/// Read set: (cf_id, key) → (value, sequence)
type ReadSet = HashMap<(ColumnFamilyId, Vec<u8>), ReadSetEntry>;

/// Isolation level for transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Dirty reads allowed; no consistency guarantees
    ReadUncommitted,
    /// No dirty reads; consistent reads at commit time
    ReadCommitted,
    /// Full snapshot isolation; MVCC-based
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

/// Write intent: pending put or delete operation
#[derive(Debug, Clone)]
pub struct WriteIntent {
    /// Column family ID
    cf_id: ColumnFamilyId,
    /// Key being written
    key: Vec<u8>,
    /// Value for puts; None for deletes
    value: Option<Vec<u8>>,
    /// Sequence number when written
    sequence: u64,
}

impl WriteIntent {
    /// Create a put intent
    pub fn put(cf_id: ColumnFamilyId, key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            cf_id,
            key,
            value: Some(value),
            sequence: 0,
        }
    }

    /// Create a delete intent
    pub fn delete(cf_id: ColumnFamilyId, key: Vec<u8>) -> Self {
        Self {
            cf_id,
            key,
            value: None,
            sequence: 0,
        }
    }

    pub fn cf_id(&self) -> ColumnFamilyId {
        self.cf_id
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub fn is_delete(&self) -> bool {
        self.value.is_none()
    }

    pub fn is_put(&self) -> bool {
        self.value.is_some()
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn set_sequence(&mut self, seq: u64) {
        self.sequence = seq;
    }
}

/// Transaction for multi-key ACID operations
///
/// Collects read and write intents, validates them, and commits atomically.
/// Thread-safe; multiple transactions can be in flight simultaneously.
#[derive(Debug)]
pub struct Transaction {
    /// Unique transaction ID
    id: u64,
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
    /// Create a new transaction with the given ID and isolation level
    pub fn new(id: u64, isolation: IsolationLevel, start_sequence: u64) -> Self {
        Self {
            id,
            isolation,
            state: TransactionState::Active,
            read_set: HashMap::new(),
            write_set: Vec::new(),
            start_sequence,
            commit_sequence: None,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
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

    /// Add a put to the transaction's write set
    pub fn put(&mut self, cf_id: ColumnFamilyId, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()> {
        if self.state != TransactionState::Active {
            return Err(crate::common::MidgeError::InvalidArgument(format!(
                "Cannot write in {:?} state",
                self.state
            )));
        }
        self.write_set.push(WriteIntent::put(cf_id, key, value));
        Ok(())
    }

    /// Add a delete to the transaction's write set
    pub fn delete(&mut self, cf_id: ColumnFamilyId, key: Vec<u8>) -> MidgeResult<()> {
        if self.state != TransactionState::Active {
            return Err(crate::common::MidgeError::InvalidArgument(format!(
                "Cannot write in {:?} state",
                self.state
            )));
        }
        self.write_set.push(WriteIntent::delete(cf_id, key));
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_transaction_when_given_id() {
        // Arrange
        let id = 42;
        let isolation = IsolationLevel::Serializable;
        let start_seq = 100;

        // Act
        let txn = Transaction::new(id, isolation, start_seq);

        // Assert
        assert_eq!(txn.id(), id);
        assert_eq!(txn.isolation_level(), isolation);
        assert_eq!(txn.start_sequence(), start_seq);
        assert_eq!(txn.state(), TransactionState::Active);
        assert_eq!(txn.write_count(), 0);
        assert_eq!(txn.read_count(), 0);
    }

    #[test]
    fn should_add_puts_to_write_set_when_put_called() {
        // Arrange
        let mut txn = Transaction::new(1, IsolationLevel::Serializable, 0);
        let cf_id = ColumnFamilyId::DEFAULT;
        let key = vec![1, 2, 3];
        let value = vec![4, 5, 6];

        // Act
        txn.put(cf_id, key.clone(), value.clone()).unwrap();

        // Assert
        assert_eq!(txn.write_count(), 1);
        assert!(txn.has_writes());
        assert!(txn.iter_writes().next().unwrap().is_put());
    }

    #[test]
    fn should_add_deletes_to_write_set_when_delete_called() {
        // Arrange
        let mut txn = Transaction::new(1, IsolationLevel::Serializable, 0);
        let cf_id = ColumnFamilyId::DEFAULT;
        let key = vec![1, 2, 3];

        // Act
        txn.delete(cf_id, key.clone()).unwrap();

        // Assert
        assert_eq!(txn.write_count(), 1);
        assert!(txn.has_writes());
        assert!(txn.iter_writes().next().unwrap().is_delete());
    }

    #[test]
    fn should_track_reads_in_read_set_when_read_called() {
        // Arrange
        let mut txn = Transaction::new(1, IsolationLevel::Serializable, 0);
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
        let mut txn = Transaction::new(1, IsolationLevel::Serializable, 0);
        txn.put(ColumnFamilyId::DEFAULT, vec![1], vec![2]).unwrap();

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
        let mut txn = Transaction::new(1, IsolationLevel::Serializable, 0);
        let cf_id = ColumnFamilyId::DEFAULT;

        // Act
        txn.put(cf_id, vec![1], vec![100]).unwrap();
        txn.delete(cf_id, vec![2]).unwrap();
        txn.put(cf_id, vec![3], vec![200]).unwrap();
        txn.delete(cf_id, vec![4]).unwrap();

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
        let mut txn = Transaction::new(1, IsolationLevel::Serializable, 0);

        // Act
        txn.enter_read_phase().unwrap();
        let put_result = txn.put(ColumnFamilyId::DEFAULT, vec![1], vec![2]);
        let delete_result = txn.delete(ColumnFamilyId::DEFAULT, vec![3]);
        let read_result = txn.read(ColumnFamilyId::DEFAULT, &[4], None, 0);

        // Assert
        assert!(put_result.is_err());
        assert!(delete_result.is_err());
        assert!(read_result.is_err());
    }

    #[test]
    fn should_clear_state_when_clear_called() {
        // Arrange
        let mut txn = Transaction::new(1, IsolationLevel::Serializable, 0);
        txn.put(ColumnFamilyId::DEFAULT, vec![1], vec![100])
            .unwrap();
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
        let mut txn = Transaction::new(1, IsolationLevel::Serializable, 0);
        txn.put(ColumnFamilyId::DEFAULT, vec![1], vec![100])
            .unwrap();

        // Act
        txn.mark_rolled_back().unwrap();

        // Assert
        assert_eq!(txn.state(), TransactionState::RolledBack);
        assert_eq!(txn.write_count(), 0);
        assert_eq!(txn.read_count(), 0);
    }
}
