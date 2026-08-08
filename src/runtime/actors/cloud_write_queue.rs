//! Transaction operations prepared by the WAL actor for memtable application.
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
