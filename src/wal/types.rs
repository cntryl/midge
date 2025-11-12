//! Core WAL data types
//!
//! This module defines the fundamental types used across all WAL implementations.

use crate::api::column_family::ColumnFamilyId;
use crate::common::timestamp;

/// Position/offset in the WAL file/stream.
pub type WalPos = u64;

/// WAL operation kinds
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum WalOpKind {
    Put,
    Insert,
    Delete,
    /// Delete all keys in range [key, range_end)
    DeleteRange,
    /// Merge operation (deferred value combination)
    Merge,
    /// Begin a transaction
    TxnBegin,
    /// Commit a transaction
    TxnCommit,
}

impl WalOpKind {
    /// Convert operation to wire format (TLV encoding).
    ///
    /// Wire format values:
    /// - 0: Put
    /// - 1: Insert
    /// - 2: Delete
    /// - 3: DeleteRange
    /// - 4: TxnBegin
    /// - 5: TxnCommit
    /// - 6: Merge
    #[inline]
    pub fn to_wire_format(self) -> u8 {
        match self {
            WalOpKind::Put => 0,
            WalOpKind::Insert => 1,
            WalOpKind::Delete => 2,
            WalOpKind::DeleteRange => 3,
            WalOpKind::TxnBegin => 4,
            WalOpKind::TxnCommit => 5,
            WalOpKind::Merge => 6,
        }
    }

    /// Parse operation from wire format (TLV encoding).
    #[inline]
    pub fn from_wire_format(byte: u8) -> crate::error::MidgeResult<Self> {
        match byte {
            0 => Ok(WalOpKind::Put),
            1 => Ok(WalOpKind::Insert),
            2 => Ok(WalOpKind::Delete),
            3 => Ok(WalOpKind::DeleteRange),
            4 => Ok(WalOpKind::TxnBegin),
            5 => Ok(WalOpKind::TxnCommit),
            6 => Ok(WalOpKind::Merge),
            _ => Err(crate::error::MidgeError::Corruption {
                message: format!("Invalid WAL operation type: {}", byte),
            }),
        }
    }
}

/// WAL synchronization modes
///
/// Controls when the WAL is flushed to disk.
///
/// **Naming convention**: Each variant communicates *when* data reaches disk,
/// not implementation details. This makes the behavior clear without documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalSyncMode {
    /// No explicit sync - let OS decide when to flush (fastest, least durable)
    /// Throughput: Baseline (≈100%)
    /// Data loss window: Unbounded (OS decides)
    NoSync,
    
    /// Sync after every write (slowest, most durable)
    /// Throughput: ≈5-10% of NoSync (per-write fsync overhead)
    /// Data loss window: 0 writes (durable immediately)
    EveryWrite,
    
    /// Batched sync - group commits together for amortized fsync overhead (balanced, default)
    /// Throughput: ≈50-80% of NoSync (amortized fsync across batch)
    /// Data loss window: Current batch (typically 1-100ms, configurable)
    #[default]
    BatchedSync,
}

/// A single WAL record using TLV encoding format.
///
/// Keys and values are stored as `bytes::Bytes` for efficient serialization
/// and cheap cloning. Each record includes a sequence number for MVCC.
///
/// ## Fields
/// - `cf_id`: Column family ID (0 = default CF)
/// - `op`: Operation type (Put, Delete, DeleteRange, TxnBegin, TxnCommit)
/// - `key`: Key for the operation (empty for transaction markers)
/// - `value`: Value for Put/Insert operations
/// - `seq`: Sequence number for MVCC
/// - `expiration`: Optional TTL (Unix timestamp in milliseconds)
/// - `range_end`: Exclusive end key for DeleteRange operations
/// - `txn_id`: Transaction ID for transactional operations
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalRecord {
    /// Column family ID this record belongs to.
    #[serde(default)]
    pub cf_id: u32,

    /// Operation type (Put, Insert, Delete, DeleteRange, TxnBegin, TxnCommit)
    pub op: WalOpKind,

    /// Key for the operation (or range start for DeleteRange)
    pub key: bytes::Bytes,

    /// Value for Put/Insert operations, None for Delete/DeleteRange
    pub value: Option<bytes::Bytes>,

    /// Sequence number assigned by the engine
    pub seq: u64,

    /// Optional expiration timestamp (Unix time in milliseconds).
    /// When set, the record should be considered expired when current time > expiration.
    #[serde(default)]
    pub expiration: Option<u64>,

    /// Optional range end for DeleteRange operations (exclusive).
    /// For DeleteRange, `key` is the inclusive start and `range_end` is the exclusive end of the range to delete.
    #[serde(default)]
    pub range_end: Option<bytes::Bytes>,

    /// Optional transaction ID for transactional operations.
    /// Added in WAL format v5. When set, this operation is part of a transaction
    /// and should only be applied if a TxnCommit with the same txn_id is present.
    #[serde(default)]
    pub txn_id: Option<u64>,

    /// Optional compression type for the value.
    /// When set, indicates the value is stored compressed in the WAL.
    /// 0 = None, 1 = Snappy, 2 = LZ4
    #[serde(default)]
    pub compression: Option<u8>,
}

impl WalRecord {
    /// Create a new WAL record for the default column family.
    pub fn new(op: WalOpKind, key: bytes::Bytes, value: Option<bytes::Bytes>, seq: u64) -> Self {
        Self {
            cf_id: 0, // Default CF
            op,
            key,
            value,
            seq,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        }
    }

    /// Create a new WAL record for a specific column family.
    pub fn new_cf(
        cf_id: ColumnFamilyId,
        op: WalOpKind,
        key: bytes::Bytes,
        value: Option<bytes::Bytes>,
        seq: u64,
    ) -> Self {
        Self {
            cf_id: cf_id.as_u32(),
            op,
            key,
            value,
            seq,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        }
    }

    /// Create a new WAL record with TTL support.
    ///
    /// # Arguments
    ///
    /// * `cf_id` - Column family ID
    /// * `op` - Operation kind (Put, Insert, Delete)
    /// * `key` - Key bytes
    /// * `value` - Optional value bytes
    /// * `seq` - Sequence number
    /// * `ttl_seconds` - Time-to-live in seconds (0 = no expiration)
    pub fn new_with_ttl(
        cf_id: ColumnFamilyId,
        op: WalOpKind,
        key: bytes::Bytes,
        value: Option<bytes::Bytes>,
        seq: u64,
        ttl_seconds: u64,
    ) -> Self {
        let expiration = if ttl_seconds > 0 {
            let now = timestamp::now_millis();
            Some(now + (ttl_seconds * 1000))
        } else {
            None
        };

        Self {
            cf_id: cf_id.as_u32(),
            op,
            key,
            value,
            seq,
            expiration,
            range_end: None,
            txn_id: None,
            compression: None,
        }
    }

    /// Check if this record has expired
    pub fn is_expired(&self) -> bool {
        if let Some(exp) = self.expiration {
            let now = timestamp::now_millis();
            now > exp
        } else {
            false
        }
    }

    /// Create a new DeleteRange WAL record.
    ///
    /// # Arguments
    ///
    /// * `cf_id` - Column family ID
    /// * `start` - Inclusive start key of the range
    /// * `end` - Exclusive end key of the range
    /// * `seq` - Sequence number
    pub fn new_delete_range(
        cf_id: ColumnFamilyId,
        start: bytes::Bytes,
        end: bytes::Bytes,
        seq: u64,
    ) -> Self {
        Self {
            cf_id: cf_id.as_u32(),
            op: WalOpKind::DeleteRange,
            key: start,
            value: None,
            seq,
            expiration: None,
            range_end: Some(end),
            txn_id: None,
            compression: None,
        }
    }

    /// Create a TxnBegin marker record.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - Transaction ID
    /// * `seq` - Sequence number
    pub fn new_txn_begin(txn_id: u64, seq: u64) -> Self {
        Self {
            cf_id: 0,
            op: WalOpKind::TxnBegin,
            key: bytes::Bytes::new(), // Empty for markers
            value: None,
            seq,
            expiration: None,
            range_end: None,
            txn_id: Some(txn_id),
            compression: None,
        }
    }

    /// Create a TxnCommit marker record.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - Transaction ID
    /// * `seq` - Sequence number
    pub fn new_txn_commit(txn_id: u64, seq: u64) -> Self {
        Self {
            cf_id: 0,
            op: WalOpKind::TxnCommit,
            key: bytes::Bytes::new(), // Empty for markers
            value: None,
            seq,
            expiration: None,
            range_end: None,
            txn_id: Some(txn_id),
            compression: None,
        }
    }

    /// Get the column family ID for this record.
    pub fn column_family_id(&self) -> ColumnFamilyId {
        ColumnFamilyId::new(self.cf_id)
    }
}
