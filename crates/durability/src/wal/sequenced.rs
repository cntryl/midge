//! Sequenced logical WAL units owned by a writer request.
//!
//! Sequence and TTL decisions precede submission. The physical writer builds
//! the nested transaction, then applies the existing outer compression, frame
//! and CRC encoding without patching compressed bytes.

use super::encoding::{self, txn_batch_views, TxnBatchEncodeRecord};
use super::{WalOpKind, WalRecord};
use bytes::Bytes;
use cntryl_midge_core::common::{MidgeError, MidgeResult};

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct SequencedWalUnit {
    pub txn_id: u64,
    pub begin_sequence: u64,
    pub commit_sequence: u64,
    pub cf_id: u32,
    pub operations: Vec<WalRecord>,
}

impl SequencedWalUnit {
    pub fn retained_memory_bound(&self) -> MidgeResult<usize> {
        // Raw Bytes, nested payload, compressor output and outer frame can
        // coexist. The final request metadata and coalescing copy are charged
        // too; the reservation stays with the physical request until terminal.
        // Retain the established descriptor allowance for writer admission.
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

    pub fn frame_size_bound(&self) -> MidgeResult<usize> {
        let payload = self.nested_payload_len()?;
        let template = self.transaction_outer(None);
        super::encoding::record_frame_size_bound(&template)?
            .checked_add(5)
            .and_then(|size| size.checked_add(payload))
            .ok_or_else(|| MidgeError::ResourceLimit("WAL frame bound overflow".into()))
    }

    pub fn nested_payload_len(&self) -> MidgeResult<usize> {
        encoding::txn_batch_payload_records_encoded_len(txn_batch_views(&self.operations))
    }

    pub fn encode_record(&self) -> MidgeResult<WalRecord> {
        let payload = encoding::encode_txn_batch_payload_iter(
            self.txn_id,
            self.begin_sequence,
            self.commit_sequence,
            self.cf_id,
            txn_batch_views(&self.operations),
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
