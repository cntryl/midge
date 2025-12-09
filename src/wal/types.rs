//! Core WAL data types
//!
//! This module defines the fundamental types used across all WAL implementations.

use crate::common::MidgeResult;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Position/offset in the WAL file/stream.
pub type WalPos = u64;

/// Column family ID type
pub type ColumnFamilyId = u32;

/// Statistics collected during WAL recovery.
#[derive(Debug, Clone, Default)]
pub struct WalRecoveryStats {
    /// Total number of WAL files processed
    pub files_processed: usize,
    /// Number of records successfully recovered
    pub records_recovered: usize,
    /// Number of corrupted records skipped
    pub records_skipped: usize,
    /// Total bytes successfully read
    pub bytes_recovered: u64,
    /// Total bytes skipped due to corruption
    pub bytes_skipped: u64,
    /// Locations where corruption was detected (file_path, position)
    pub corruption_locations: Vec<(String, u64)>,
    /// Whether recovery encountered any corruption
    pub had_corruption: bool,
}

impl WalRecoveryStats {
    /// Create new empty recovery stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge stats from another recovery operation
    pub fn merge(&mut self, other: &WalRecoveryStats) {
        self.files_processed += other.files_processed;
        self.records_recovered += other.records_recovered;
        self.records_skipped += other.records_skipped;
        self.bytes_recovered += other.bytes_recovered;
        self.bytes_skipped += other.bytes_skipped;
        self.corruption_locations
            .extend(other.corruption_locations.clone());
        self.had_corruption |= other.had_corruption;
    }

    /// Record a corruption event
    pub fn record_corruption(&mut self, file_path: &str, position: u64, bytes_lost: u64) {
        self.had_corruption = true;
        self.records_skipped += 1;
        self.bytes_skipped += bytes_lost;
        self.corruption_locations
            .push((file_path.to_string(), position));
    }

    /// Record a successful record recovery
    pub fn record_success(&mut self, record_bytes: u64) {
        self.records_recovered += 1;
        self.bytes_recovered += record_bytes;
    }
}

/// WAL operation kinds
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
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
    pub fn from_wire_format(byte: u8) -> MidgeResult<Self> {
        match byte {
            0 => Ok(WalOpKind::Put),
            1 => Ok(WalOpKind::Insert),
            2 => Ok(WalOpKind::Delete),
            3 => Ok(WalOpKind::DeleteRange),
            4 => Ok(WalOpKind::TxnBegin),
            5 => Ok(WalOpKind::TxnCommit),
            6 => Ok(WalOpKind::Merge),
            _ => Err(crate::common::MidgeError::Corruption(format!(
                "Invalid WAL operation type: {}",
                byte
            ))),
        }
    }
}

/// WAL synchronization modes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalSyncMode {
    /// No explicit sync - let OS decide (fastest, least durable)
    NoSync,

    /// Sync after every write (slowest, most durable)
    EveryWrite,

    /// Batched sync - group commits together (balanced, default)
    #[default]
    BatchedSync,
}

/// A single WAL record using TLV encoding format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    /// Column family ID this record belongs to.
    #[serde(default)]
    pub cf_id: ColumnFamilyId,

    /// Operation type (Put, Insert, Delete, DeleteRange, TxnBegin, TxnCommit)
    pub op: WalOpKind,

    /// Key for the operation (or range start for DeleteRange)
    pub key: Bytes,

    /// Value for Put/Insert operations, None for Delete/DeleteRange
    pub value: Option<Bytes>,

    /// Sequence number assigned by the engine
    pub seq: u64,

    /// Optional expiration timestamp (Unix time in milliseconds).
    #[serde(default)]
    pub expiration: Option<u64>,

    /// Optional range end for DeleteRange operations (exclusive).
    #[serde(default)]
    pub range_end: Option<Bytes>,

    /// Optional transaction ID for transactional operations.
    #[serde(default)]
    pub txn_id: Option<u64>,

    /// Optional compression type for the value.
    #[serde(default)]
    pub compression: Option<u8>,
}

impl WalRecord {
    /// Create a new WAL record for the default column family.
    pub fn new(op: WalOpKind, key: Bytes, value: Option<Bytes>, seq: u64) -> Self {
        Self {
            cf_id: 0,
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
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
    ) -> Self {
        Self {
            cf_id,
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
    pub fn new_with_ttl(
        cf_id: ColumnFamilyId,
        op: WalOpKind,
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        ttl_seconds: u64,
    ) -> Self {
        let expiration = if ttl_seconds > 0 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            Some(now + (ttl_seconds * 1000))
        } else {
            None
        };

        Self {
            cf_id,
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
        if let Some(exp_time) = self.expiration {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            now > exp_time
        } else {
            false
        }
    }

    /// Get the size in bytes of this record when serialized
    pub fn estimated_size(&self) -> usize {
        let key_size = self.key.len();
        let value_size = self.value.as_ref().map(|v| v.len()).unwrap_or(0);
        let range_end_size = self.range_end.as_ref().map(|r| r.len()).unwrap_or(0);
        4 + 1 + 8 + 4 + key_size + 4 + value_size + range_end_size + 20
    }
}
