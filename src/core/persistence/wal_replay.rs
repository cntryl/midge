//! WAL replay logic for database recovery
//!
//! This module handles replaying WAL records into memtables during recovery.
//! It depends on `core::memtable` and belongs in the core engine orchestration,
//! not in the WAL subsystem itself.

use crate::core::memtable::MemTable;
use crate::wal::{WalOpKind, WalRecord};

/// Replay WAL records into the appropriate column families based on cf_id.
///
/// This function is called during database recovery to restore state from the WAL.
/// Records for non-existent column families (e.g., dropped CFs) are silently ignored.
///
/// Transactions are handled atomically: records belonging to a transaction are
/// buffered until a TxnCommit is found. If no matching commit is found, the
/// entire transaction is discarded (crash atomicity).
///
/// # Arguments
///
/// * `cf_map` - Map of column family ID to memtable (uses shared refs due to interior mutability)
/// * `records` - Slice of WAL records to replay
///
/// # Returns
///
/// Maximum sequence number seen during replay
#[allow(dead_code)] // Used in tests
pub(crate) fn replay_wal_to_memtables(
    cf_map: &mut std::collections::HashMap<u32, &MemTable>,
    records: &[WalRecord],
) -> u64 {
    replay_wal_to_memtables_after_seq(cf_map, records, 0)
}

/// Replay WAL records to memtables, skipping records with sequence <= skip_before_seq.
/// This is used during recovery to avoid replaying records that were already flushed to SST.
pub(crate) fn replay_wal_to_memtables_after_seq(
    cf_map: &mut std::collections::HashMap<u32, &MemTable>,
    records: &[WalRecord],
    skip_before_seq: u64,
) -> u64 {
    use std::collections::HashMap;

    let mut max_seq = 0u64;
    let mut txn_buffers: HashMap<u64, Vec<WalRecord>> = HashMap::new();
    let mut committed_txns: std::collections::HashSet<u64> = std::collections::HashSet::new();

    // First pass: identify committed transactions
    for rec in records {
        // Skip records already persisted. Special case: skip_before_seq=0 means nothing persisted yet
        // (since sequences start at 0), so don't skip anything. Otherwise skip rec.seq <= skip_before_seq.
        if skip_before_seq > 0 && rec.seq <= skip_before_seq {
            continue;
        }

        if rec.op == WalOpKind::TxnCommit {
            if let Some(txn_id) = rec.txn_id {
                committed_txns.insert(txn_id);
            }
        }
    }

    // Second pass: buffer transactional ops and apply committed ones
    for rec in records {
        // Skip records already persisted. Special case: skip_before_seq=0 means nothing persisted yet
        // (since sequences start at 0), so don't skip anything. Otherwise skip rec.seq <= skip_before_seq.
        if skip_before_seq > 0 && rec.seq <= skip_before_seq {
            continue;
        }

        max_seq = max_seq.max(rec.seq);

        match rec.op {
            WalOpKind::TxnBegin => {
                // Initialize buffer for this transaction
                if let Some(txn_id) = rec.txn_id {
                    txn_buffers.entry(txn_id).or_default();
                }
            }
            WalOpKind::TxnCommit => {
                // Apply buffered ops for this transaction if they exist
                if let Some(txn_id) = rec.txn_id {
                    if let Some(buffered) = txn_buffers.remove(&txn_id) {
                        for op_rec in buffered {
                            apply_record_to_memtable(cf_map, op_rec);
                        }
                    }
                }
            }
            _ => {
                // Regular op: check if it's part of a transaction
                if let Some(txn_id) = rec.txn_id {
                    // Buffer if part of a transaction (clone needed as we're taking from slice)
                    txn_buffers.entry(txn_id).or_default().push(rec.clone());
                } else {
                    // Non-transactional: apply immediately (clone needed as we're taking from slice)
                    apply_record_to_memtable(cf_map, rec.clone());
                }
            }
        }
    }

    max_seq
}

/// Apply a single WAL record to the appropriate memtable.
fn apply_record_to_memtable(
    cf_map: &mut std::collections::HashMap<u32, &MemTable>,
    rec: WalRecord,
) {
    let cf_id = rec.column_family_id().as_u32();

    // Get the CF memtable by ID, skip if CF doesn't exist (was dropped)
    if let Some(memtable) = cf_map.get(&cf_id) {
        match rec.op {
            WalOpKind::Put | WalOpKind::Insert => {
                if let Some(value) = rec.value {
                    memtable.put_with_seq_and_exp(
                        rec.key.as_ref(),
                        value.as_ref(),
                        rec.seq,
                        rec.expiration,
                    );
                }
            }
            WalOpKind::Delete => {
                memtable.delete_with_seq(rec.key.as_ref(), rec.seq);
            }
            WalOpKind::DeleteRange => {
                if let Some(range_end) = rec.range_end {
                    memtable.delete_range_with_seq(rec.key.as_ref(), range_end.as_ref(), rec.seq);
                }
            }
            WalOpKind::Merge => {
                // Merge operations are stored like Put in the memtable
                // Resolution happens at read time
                if let Some(value) = rec.value {
                    memtable.put_with_seq_and_exp(
                        rec.key.as_ref(),
                        value.as_ref(),
                        rec.seq,
                        rec.expiration,
                    );
                }
            }
            WalOpKind::TxnBegin | WalOpKind::TxnCommit => {
                // Markers are not applied to memtable
            }
        }
    }
}

/// WAL replay iterator for database recovery.
pub struct WalReplayIterator;

/// Compute the encoded length of a WAL record in the FS WAL format.
///
/// This is used to predict when a WAL buffer will fill up and needs rotation.
///
/// WAL format v4:
/// ```text
/// CRC32(4) + type(1) + cf_id(4) + seq(8) + varint(keylen) + key + varint(vallen) + val
/// For DeleteRange: also includes varint(range_end_len) + range_end
/// ```
///
/// # Arguments
/// * `kind` - The operation kind (Put, Delete, DeleteRange, etc.)
/// * `key_len` - Length of the key in bytes (or range start for DeleteRange)
/// * `val_len` - Optional length of the value in bytes
/// * `range_end_len` - Optional length of range_end for DeleteRange operations
///
/// # Returns
/// Total encoded size in bytes
pub(crate) fn wal_record_encoded_len(
    kind: WalOpKind,
    key_len: usize,
    val_len: Option<usize>,
    range_end_len: Option<usize>,
) -> u64 {
    let _ = kind; // kind is 1 byte
    let vlen = val_len.unwrap_or(0);
    let mut size = 4
        + 1
        + 4
        + 8
        + varint_len_u32(key_len as u32)
        + key_len
        + varint_len_u32(vlen as u32)
        + vlen;

    // Add range_end if present (for DeleteRange)
    if let Some(rend_len) = range_end_len {
        size += varint_len_u32(rend_len as u32) + rend_len;
    }

    size as u64
}

/// Calculate the encoded length of a u32 varint.
///
/// Varint encoding uses 7 bits per byte with the high bit as a continuation flag.
/// This determines how many bytes will be needed to encode a given integer.
///
/// # Arguments
/// * `v` - The u32 value to encode
///
/// # Returns
/// Number of bytes needed (1-5)
fn varint_len_u32(mut v: u32) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        n += 1;
        v >>= 7;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::memtable::MemTable;
    use bytes::Bytes;
    use std::collections::HashMap;

    fn create_test_record(
        seq: u64,
        op: WalOpKind,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> WalRecord {
        WalRecord {
            seq,
            op,
            key: Bytes::from(key),
            value: value.map(Bytes::from),
            txn_id: None,
            range_end: None,
            expiration: None,
            cf_id: 0,
            compression: None,
        }
    }

    #[test]
    fn should_replay_put_operation() {
        // Arrange
        let memtable = MemTable::new();
        let mut cf_map = HashMap::new();
        cf_map.insert(0, &memtable);
        let records = vec![create_test_record(
            1,
            WalOpKind::Put,
            b"key1".to_vec(),
            Some(b"value1".to_vec()),
        )];

        // Act
        let max_seq = replay_wal_to_memtables(&mut cf_map, &records);

        // Assert
        assert_eq!(max_seq, 1);
        assert_eq!(memtable.get(b"key1"), Some(Bytes::from("value1")));
    }

    #[test]
    fn should_replay_delete_operation() {
        // Arrange
        let memtable = MemTable::new();
        memtable.put(b"key1", b"value1");
        let mut cf_map = HashMap::new();
        cf_map.insert(0, &memtable);
        let records = vec![create_test_record(
            2,
            WalOpKind::Delete,
            b"key1".to_vec(),
            None,
        )];

        // Act
        let max_seq = replay_wal_to_memtables(&mut cf_map, &records);

        // Assert
        assert_eq!(max_seq, 2);
        assert_eq!(memtable.get(b"key1"), None);
    }

    #[test]
    fn should_skip_records_before_sequence() {
        // Arrange
        let memtable = MemTable::new();
        let mut cf_map = HashMap::new();
        cf_map.insert(0, &memtable);
        let records = vec![
            create_test_record(
                1,
                WalOpKind::Put,
                b"key1".to_vec(),
                Some(b"value1".to_vec()),
            ),
            create_test_record(
                5,
                WalOpKind::Put,
                b"key2".to_vec(),
                Some(b"value2".to_vec()),
            ),
            create_test_record(
                10,
                WalOpKind::Put,
                b"key3".to_vec(),
                Some(b"value3".to_vec()),
            ),
        ];

        // Act
        let max_seq = replay_wal_to_memtables_after_seq(&mut cf_map, &records, 5);

        // Assert
        assert_eq!(max_seq, 10);
        assert_eq!(memtable.get(b"key1"), None);
        assert_eq!(memtable.get(b"key2"), None);
        assert_eq!(memtable.get(b"key3"), Some(Bytes::from("value3")));
    }

    #[test]
    fn should_replay_committed_transaction() {
        // Arrange
        let memtable = MemTable::new();
        let mut cf_map = HashMap::new();
        cf_map.insert(0, &memtable);
        let records = vec![
            WalRecord {
                seq: 1,
                op: WalOpKind::TxnBegin,
                key: Bytes::new(),
                value: None,
                txn_id: Some(100),
                range_end: None,
                expiration: None,
                cf_id: 0,
                compression: None,
            },
            WalRecord {
                seq: 2,
                op: WalOpKind::Put,
                key: Bytes::from("key1"),
                value: Some(Bytes::from("value1")),
                txn_id: Some(100),
                range_end: None,
                expiration: None,
                cf_id: 0,
                compression: None,
            },
            WalRecord {
                seq: 3,
                op: WalOpKind::TxnCommit,
                key: Bytes::new(),
                value: None,
                txn_id: Some(100),
                range_end: None,
                expiration: None,
                cf_id: 0,
                compression: None,
            },
        ];

        // Act
        let max_seq = replay_wal_to_memtables(&mut cf_map, &records);

        // Assert
        assert_eq!(max_seq, 3);
        assert_eq!(memtable.get(b"key1"), Some(Bytes::from("value1")));
    }

    #[test]
    fn should_discard_uncommitted_transaction() {
        // Arrange
        let memtable = MemTable::new();
        let mut cf_map = HashMap::new();
        cf_map.insert(0, &memtable);
        let records = vec![
            WalRecord {
                seq: 1,
                op: WalOpKind::TxnBegin,
                key: Bytes::new(),
                value: None,
                txn_id: Some(100),
                range_end: None,
                expiration: None,
                cf_id: 0,
                compression: None,
            },
            WalRecord {
                seq: 2,
                op: WalOpKind::Put,
                key: Bytes::from("key1"),
                value: Some(Bytes::from("value1")),
                txn_id: Some(100),
                range_end: None,
                expiration: None,
                cf_id: 0,
                compression: None,
            },
        ];

        // Act
        let max_seq = replay_wal_to_memtables(&mut cf_map, &records);

        // Assert
        assert_eq!(max_seq, 2);
        assert_eq!(memtable.get(b"key1"), None);
    }

    #[test]
    fn should_ignore_records_for_nonexistent_cf() {
        // Arrange
        let memtable = MemTable::new();
        let mut cf_map = HashMap::new();
        cf_map.insert(0, &memtable);
        let records = vec![WalRecord {
            seq: 1,
            op: WalOpKind::Put,
            key: Bytes::from("key1"),
            value: Some(Bytes::from("value1")),
            txn_id: None,
            range_end: None,
            expiration: None,
            cf_id: 99,
            compression: None,
        }];

        // Act
        let max_seq = replay_wal_to_memtables(&mut cf_map, &records);

        // Assert
        assert_eq!(max_seq, 1);
        assert_eq!(memtable.get(b"key1"), None);
    }

    #[test]
    fn should_calculate_varint_len_for_small_values() {
        // Arrange

        // Act
        let len0 = varint_len_u32(0);
        let len127 = varint_len_u32(127);

        // Assert
        assert_eq!(len0, 1);
        assert_eq!(len127, 1);
    }

    #[test]
    fn should_calculate_varint_len_for_medium_values() {
        // Arrange

        // Act
        let len128 = varint_len_u32(128);
        let len16383 = varint_len_u32(16383);

        // Assert
        assert_eq!(len128, 2);
        assert_eq!(len16383, 2);
    }

    #[test]
    fn should_calculate_varint_len_for_large_values() {
        // Arrange

        // Act
        let len16384 = varint_len_u32(16384);
        let len_max = varint_len_u32(u32::MAX);

        // Assert
        assert_eq!(len16384, 3);
        assert_eq!(len_max, 5);
    }

    #[test]
    fn should_calculate_wal_record_encoded_len_for_put() {
        // Arrange
        let key_len = 10;
        let val_len = Some(20);

        // Act
        let len = wal_record_encoded_len(WalOpKind::Put, key_len, val_len, None);

        // Assert
        assert!(len > 0);
        assert!(len >= (4 + 1 + 4 + 8 + key_len + 20) as u64);
    }

    #[test]
    fn should_calculate_wal_record_encoded_len_for_delete() {
        // Arrange
        let key_len = 10;

        // Act
        let len = wal_record_encoded_len(WalOpKind::Delete, key_len, None, None);

        // Assert
        assert!(len > 0);
        assert!(len >= (4 + 1 + 4 + 8 + key_len) as u64);
    }

    #[test]
    fn should_calculate_wal_record_encoded_len_for_delete_range() {
        // Arrange
        let key_len = 10;
        let range_end_len = Some(15);

        // Act
        let len = wal_record_encoded_len(WalOpKind::DeleteRange, key_len, None, range_end_len);

        // Assert
        assert!(len > 0);
        assert!(len >= (4 + 1 + 4 + 8 + key_len + 15) as u64);
    }

    #[test]
    fn should_return_max_sequence_from_empty_records() {
        // Arrange
        let memtable = MemTable::new();
        let mut cf_map = HashMap::new();
        cf_map.insert(0, &memtable);
        let records: Vec<WalRecord> = vec![];

        // Act
        let max_seq = replay_wal_to_memtables(&mut cf_map, &records);

        // Assert
        assert_eq!(max_seq, 0);
    }
}
