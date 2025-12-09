//! WAL record loading and replay for memtables.

use std::sync::atomic::Ordering;

use crate::core::data_structures::skiplist::OpType;
use crate::error::MidgeResult;
use crate::wal::WalRecord;

use super::core::MemTable;

/// Load WAL records into a memtable.
///
/// This is used during database recovery to replay the WAL and reconstruct
/// the in-memory state. Each WAL record is applied to the memtable according
/// to its operation type.
pub(super) fn load_from_wal(memtable: &MemTable, records: Vec<WalRecord>) -> MidgeResult<()> {
    for rec in records {
        match rec.op {
            crate::wal::WalOpKind::Put | crate::wal::WalOpKind::Insert => {
                let k = rec.key;
                let v = rec.value;
                let add = k.len() + v.as_ref().map(|x| x.len()).unwrap_or(0);
                memtable.bytes.fetch_add(add, Ordering::Relaxed);
                memtable
                    .inner
                    .upsert_exp(k, v, rec.seq, rec.expiration, OpType::Put);
            }
            crate::wal::WalOpKind::Delete => {
                let k = rec.key;
                // store tombstone; count key size as storage overhead
                let add = k.len();
                memtable.bytes.fetch_add(add, Ordering::Relaxed);
                // Tombstones never have expiration
                memtable
                    .inner
                    .upsert_exp(k, None, rec.seq, None, OpType::Delete);
            }
            crate::wal::WalOpKind::DeleteRange => {
                // Range deletes are handled through range_tombstones, not the skiplist
                if let Some(range_end) = rec.range_end {
                    memtable
                        .range_tombstones
                        .push(rec.key.to_vec(), range_end.to_vec(), rec.seq);
                    // Count range tombstone storage overhead
                    let add = rec.key.len() + range_end.len();
                    memtable.bytes.fetch_add(add, Ordering::Relaxed);
                }
            }
            crate::wal::WalOpKind::Merge => {
                // Merge operations are stored with OpType::Merge in the skiplist
                // The merge resolution happens at read time, not write time
                let k = rec.key;
                let v = rec.value;
                let add = k.len() + v.as_ref().map(|x| x.len()).unwrap_or(0);
                memtable.bytes.fetch_add(add, Ordering::Relaxed);
                memtable
                    .inner
                    .upsert_exp(k, v, rec.seq, rec.expiration, OpType::Merge);
            }
            crate::wal::WalOpKind::TxnBegin | crate::wal::WalOpKind::TxnCommit => {
                // Transaction markers are not stored in memtable
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalOpKind;
    use bytes::Bytes;

    #[test]
    fn should_load_put_records_from_wal() {
        // Arrange
        let mt = MemTable::new();
        let records = vec![
            WalRecord {
                op: WalOpKind::Put,
                cf_id: 0,
                key: Bytes::from_static(b"key1"),
                value: Some(Bytes::from_static(b"val1")),
                seq: 1,
                expiration: None,
                range_end: None,
                compression: None,
                txn_id: None,
            },
            WalRecord {
                op: WalOpKind::Put,
                cf_id: 0,
                key: Bytes::from_static(b"key2"),
                value: Some(Bytes::from_static(b"val2")),
                seq: 2,
                expiration: None,
                range_end: None,
                compression: None,
                txn_id: None,
            },
        ];

        // Act
        load_from_wal(&mt, records).unwrap();

        // Assert
        assert_eq!(mt.get(b"key1"), Some(Bytes::from_static(b"val1")));
        assert_eq!(mt.get(b"key2"), Some(Bytes::from_static(b"val2")));
    }

    #[test]
    fn should_load_delete_records_from_wal() {
        // Arrange
        let mt = MemTable::new();
        let records = vec![
            WalRecord {
                op: WalOpKind::Put,
                cf_id: 0,
                key: Bytes::from_static(b"key1"),
                value: Some(Bytes::from_static(b"val1")),
                seq: 1,
                expiration: None,
                range_end: None,
                compression: None,
                txn_id: None,
            },
            WalRecord {
                op: WalOpKind::Delete,
                cf_id: 0,
                key: Bytes::from_static(b"key1"),
                value: None,
                seq: 2,
                expiration: None,
                range_end: None,
                compression: None,
                txn_id: None,
            },
        ];

        // Act
        load_from_wal(&mt, records).unwrap();

        // Assert
        assert_eq!(mt.get(b"key1"), None);
    }

    #[test]
    fn should_load_merge_records_from_wal() {
        // Arrange
        let mt = MemTable::new();
        let records = vec![WalRecord {
            op: WalOpKind::Merge,
            cf_id: 0,
            key: Bytes::from_static(b"key1"),
            value: Some(Bytes::from_static(b"val1")),
            seq: 1,
            expiration: None,
            range_end: None,
            compression: None,
            txn_id: None,
        }];

        // Act
        load_from_wal(&mt, records).unwrap();

        // Assert
        // The value should be stored (merge resolution happens at read time)
        assert!(mt.get(b"key1").is_some());
    }

    #[test]
    fn should_ignore_transaction_markers_when_loading() {
        // Arrange
        let mt = MemTable::new();
        let records = vec![
            WalRecord {
                op: WalOpKind::TxnBegin,
                cf_id: 0,
                key: Bytes::from_static(b"txn1"),
                value: None,
                seq: 1,
                expiration: None,
                range_end: None,
                compression: None,
                txn_id: None,
            },
            WalRecord {
                op: WalOpKind::TxnCommit,
                cf_id: 0,
                key: Bytes::from_static(b"txn1"),
                value: None,
                seq: 2,
                expiration: None,
                range_end: None,
                compression: None,
                txn_id: None,
            },
        ];

        // Act
        load_from_wal(&mt, records).unwrap();

        // Assert
        assert_eq!(mt.size_bytes(), 0);
    }
}
