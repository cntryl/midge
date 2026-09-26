//! Transaction operations prepared for publication after the WAL append.
//!
//! Cloud upload admission and backpressure live in the production
//! `HybridStorage` upload queue. This module intentionally contains no shadow
//! queue or test-only durability policy.

/// Transaction operation ready for memtable application.
#[derive(Debug)]
pub enum TransactionApplyOp {
    Put {
        op: crate::wal::WalOpKind,
        cf_id: crate::types::ColumnFamilyId,
        key: bytes::Bytes,
        value: bytes::Bytes,
        expiration: Option<u64>,
        sequence: u64,
    },
    Delete {
        cf_id: crate::types::ColumnFamilyId,
        key: bytes::Bytes,
        sequence: u64,
    },
    DeleteRange {
        cf_id: crate::types::ColumnFamilyId,
        start_key: bytes::Bytes,
        end_key: bytes::Bytes,
        sequence: u64,
    },
}

struct TransactionRecordFields<'a> {
    cf_id: crate::types::ColumnFamilyId,
    op: crate::wal::WalOpKind,
    key: &'a bytes::Bytes,
    value: Option<&'a bytes::Bytes>,
    expiration: Option<u64>,
    range_end: Option<&'a bytes::Bytes>,
    sequence: u64,
}

impl TransactionApplyOp {
    /// One conversion owns op kind and TTL semantics for resident and spilled
    /// transactions. Both WAL encoders and memtable publication use its result.
    pub(super) fn from_source(
        op: crate::runtime::TransactionOp,
        sequence: u64,
        commit_time_millis: u64,
    ) -> Self {
        match op {
            crate::runtime::TransactionOp::Put {
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            } => Self::Put {
                op: if insert_only {
                    crate::wal::WalOpKind::Insert
                } else {
                    crate::wal::WalOpKind::Put
                },
                cf_id,
                key,
                value,
                expiration: crate::common::time::expiration_from_ttl(
                    ttl_seconds,
                    commit_time_millis,
                ),
                sequence,
            },
            crate::runtime::TransactionOp::Delete { cf_id, key } => Self::Delete {
                cf_id,
                key,
                sequence,
            },
            crate::runtime::TransactionOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
            } => Self::DeleteRange {
                cf_id,
                start_key,
                end_key,
                sequence,
            },
        }
    }

    fn record_fields(&self) -> TransactionRecordFields<'_> {
        match self {
            Self::Put {
                op,
                cf_id,
                key,
                value,
                expiration,
                sequence,
            } => TransactionRecordFields {
                cf_id: *cf_id,
                op: *op,
                key,
                value: Some(value),
                expiration: *expiration,
                range_end: None,
                sequence: *sequence,
            },
            Self::Delete {
                cf_id,
                key,
                sequence,
            } => TransactionRecordFields {
                cf_id: *cf_id,
                op: crate::wal::WalOpKind::Delete,
                key,
                value: None,
                expiration: None,
                range_end: None,
                sequence: *sequence,
            },
            Self::DeleteRange {
                cf_id,
                start_key,
                end_key,
                sequence,
            } => TransactionRecordFields {
                cf_id: *cf_id,
                op: crate::wal::WalOpKind::DeleteRange,
                key: start_key,
                value: None,
                expiration: None,
                range_end: Some(end_key),
                sequence: *sequence,
            },
        }
    }

    pub(super) fn batch_record(
        &self,
        txn_id: u64,
        writer_epoch: u64,
    ) -> crate::wal::encoding::TxnBatchEncodeRecord<'_> {
        let fields = self.record_fields();
        crate::wal::encoding::TxnBatchEncodeRecord {
            cf_id: fields.cf_id,
            op: fields.op,
            key: fields.key.as_ref(),
            value: fields.value.map(bytes::Bytes::as_ref),
            seq: fields.sequence,
            expiration: fields.expiration,
            range_end: fields.range_end.map(bytes::Bytes::as_ref),
            txn_id: Some(txn_id),
            writer_epoch,
        }
    }

    pub(super) fn wal_record(&self, txn_id: u64, writer_epoch: u64) -> crate::wal::WalRecord {
        let fields = self.record_fields();
        let mut record = crate::wal::WalRecord::new_cf(
            fields.cf_id,
            fields.op,
            fields.key.clone(),
            fields.value.cloned(),
            fields.sequence,
            writer_epoch,
        );
        record.expiration = fields.expiration;
        record.range_end = fields.range_end.cloned();
        record.txn_id = Some(txn_id);
        record
    }
}

#[cfg(test)]
mod tests {
    use super::TransactionApplyOp;
    use crate::runtime::TransactionOp;
    use bytes::Bytes;

    #[test]
    fn should_encode_matching_batch_and_spill_records_for_every_transaction_op() {
        // Arrange
        let ops = [
            TransactionOp::Put {
                cf_id: 1,
                key: Bytes::from_static(b"put"),
                value: Bytes::from_static(b"value"),
                ttl_seconds: Some(5),
                insert_only: false,
            },
            TransactionOp::Put {
                cf_id: 1,
                key: Bytes::from_static(b"insert"),
                value: Bytes::from_static(b"once"),
                ttl_seconds: Some(10),
                insert_only: true,
            },
            TransactionOp::Delete {
                cf_id: 1,
                key: Bytes::from_static(b"gone"),
            },
            TransactionOp::DeleteRange {
                cf_id: 1,
                start_key: Bytes::from_static(b"a"),
                end_key: Bytes::from_static(b"z"),
            },
        ];

        for (ordinal, op) in ops.into_iter().enumerate() {
            // Act
            let prepared = TransactionApplyOp::from_source(op, 41 + ordinal as u64, 1_000);
            let batch = prepared.batch_record(7, 3);
            let spill = prepared.wal_record(7, 3);

            // Assert
            assert_eq!(batch.cf_id, spill.cf_id);
            assert_eq!(batch.op, spill.op);
            assert_eq!(batch.key, spill.key.as_ref());
            assert_eq!(batch.value, spill.value.as_deref());
            assert_eq!(batch.seq, spill.seq);
            assert_eq!(batch.expiration, spill.expiration);
            assert_eq!(batch.range_end, spill.range_end.as_deref());
            assert_eq!(batch.txn_id, spill.txn_id);
            assert_eq!(batch.writer_epoch, spill.writer_epoch);
        }
    }
}
