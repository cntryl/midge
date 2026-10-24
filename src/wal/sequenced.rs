//! Sequenced logical WAL units owned by a writer request.
//!
//! Sequence and TTL decisions precede submission. The physical writer builds
//! the nested transaction, then applies the existing outer compression, frame
//! and CRC encoding without patching compressed bytes.

use super::encoding::TxnBatchEncodeRecord;
use super::{WalOpKind, WalRecord};
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct SequencedWalUnit {
    pub(crate) txn_id: u64,
    pub(crate) begin_sequence: u64,
    pub(crate) commit_sequence: u64,
    pub(crate) cf_id: u32,
    pub(crate) operations: Vec<WalRecord>,
}

impl SequencedWalUnit {
    pub(crate) fn retained_memory_bound(&self) -> MidgeResult<usize> {
        // Raw Bytes, nested payload, compressor output and outer frame can
        // coexist. The final request metadata and coalescing copy are charged
        // too; the reservation stays with the physical request until terminal.
        let metadata = self.operations.len().saturating_mul(
            std::mem::size_of::<WalRecord>() + std::mem::size_of::<TxnBatchEncodeRecord<'_>>(),
        );
        self.frame_size_bound()?
            .checked_mul(6)
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .and_then(|bytes| bytes.checked_add(metadata))
            .ok_or_else(|| MidgeError::ResourceLimit("WAL encode memory bound overflow".into()))
    }
    pub(crate) fn last_sequence(&self) -> u64 {
        self.commit_sequence
    }

    pub(crate) fn cf_id(&self) -> u32 {
        self.cf_id
    }

    pub(crate) fn frame_size_bound(&self) -> MidgeResult<usize> {
        let payload = self.nested_payload_len()?;
        let template = self.transaction_outer(None);
        super::encoding::record_frame_size_bound(&template)?
            .checked_add(5)
            .and_then(|size| size.checked_add(payload))
            .ok_or_else(|| MidgeError::ResourceLimit("WAL frame bound overflow".into()))
    }

    pub(crate) fn nested_payload_len(&self) -> MidgeResult<usize> {
        super::encoding::txn_batch_payload_records_encoded_len(&transaction_views(&self.operations))
    }

    pub(crate) fn encode_record(&self) -> MidgeResult<WalRecord> {
        let payload = super::encoding::encode_txn_batch_payload_records(
            self.txn_id,
            self.begin_sequence,
            self.commit_sequence,
            self.cf_id,
            &transaction_views(&self.operations),
        )?;
        Ok(self.transaction_outer(Some(payload)))
    }

    fn transaction_outer(&self, value: Option<Bytes>) -> WalRecord {
        let mut outer = WalRecord::new_cf(
            self.cf_id,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            value,
            self.commit_sequence,
        );
        outer.txn_id = Some(self.txn_id);
        outer
    }
}

fn transaction_views(records: &[WalRecord]) -> Vec<TxnBatchEncodeRecord<'_>> {
    records
        .iter()
        .map(|record| TxnBatchEncodeRecord {
            cf_id: record.cf_id,
            op: record.op,
            key: record.key.as_ref(),
            value: record.value.as_deref(),
            seq: record.seq,
            expiration: record.expiration,
            range_end: record.range_end.as_deref(),
            txn_id: record.txn_id,
        })
        .collect()
}
