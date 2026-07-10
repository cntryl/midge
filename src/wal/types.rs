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

/// WAL operation kinds
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum WalOpKind {
    Put,
    Insert,
    Delete,
    /// Delete all keys in range [key, `range_end`)
    DeleteRange,
    /// Begin a transaction
    TxnBegin,
    /// Commit a transaction
    TxnCommit,
    /// Atomic transaction batch encoded in a single physical WAL frame.
    TxnBatch,
}

impl WalOpKind {
    /// Convert operation to wire format (TLV encoding).
    #[inline]
    #[must_use]
    pub fn to_wire_format(self) -> u8 {
        match self {
            WalOpKind::Put => 0,
            WalOpKind::Insert => 1,
            WalOpKind::Delete => 2,
            WalOpKind::DeleteRange => 3,
            WalOpKind::TxnBegin => 4,
            WalOpKind::TxnCommit => 5,
            WalOpKind::TxnBatch => 6,
        }
    }

    /// Parse operation from wire format (TLV encoding).
    #[inline]
    ///
    /// # Errors
    ///
    /// Returns an error if the byte does not map to a known WAL operation kind.
    pub fn from_wire_format(byte: u8) -> MidgeResult<Self> {
        match byte {
            0 => Ok(WalOpKind::Put),
            1 => Ok(WalOpKind::Insert),
            2 => Ok(WalOpKind::Delete),
            3 => Ok(WalOpKind::DeleteRange),
            4 => Ok(WalOpKind::TxnBegin),
            5 => Ok(WalOpKind::TxnCommit),
            6 => Ok(WalOpKind::TxnBatch),
            _ => Err(crate::common::MidgeError::Corruption(format!(
                "Invalid WAL operation type: {byte}"
            ))),
        }
    }
}

/// A single WAL record using TLV encoding format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    /// Column family ID this record belongs to.
    #[serde(default)]
    pub cf_id: ColumnFamilyId,

    /// Operation type (Put, Insert, Delete, `DeleteRange`, `TxnBegin`, `TxnCommit`, `TxnBatch`)
    pub op: WalOpKind,

    /// Key for the operation (or range start for `DeleteRange`)
    pub key: Bytes,

    /// Value for Put/Insert operations, None for Delete/DeleteRange
    pub value: Option<Bytes>,

    /// Sequence number assigned by the engine
    pub seq: u64,

    /// Optional expiration timestamp (Unix time in milliseconds).
    #[serde(default)]
    pub expiration: Option<u64>,

    /// Optional range end for `DeleteRange` operations (exclusive).
    #[serde(default)]
    pub range_end: Option<Bytes>,

    /// Optional transaction ID for transactional operations.
    #[serde(default)]
    pub txn_id: Option<u64>,

    /// Optional compression type for the value.
    #[serde(default)]
    pub compression: Option<u8>,

    /// Writer fencing epoch (monotonic fencing token).
    /// Stamped by the writer and encoded into WAL so storage/replay
    /// can enforce fencing semantics.  Required on every record.
    pub writer_epoch: u64,
}

impl WalRecord {
    /// Create a new WAL record for the default column family.
    pub fn new(
        op: WalOpKind,
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        writer_epoch: u64,
    ) -> Self {
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
            writer_epoch,
        }
    }

    /// Create a new WAL record for a specific column family.
    pub fn new_cf(
        cf_id: ColumnFamilyId,
        op: WalOpKind,
        key: Bytes,
        value: Option<Bytes>,
        seq: u64,
        writer_epoch: u64,
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
            writer_epoch,
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
        writer_epoch: u64,
    ) -> Self {
        let expiration = if ttl_seconds > 0 {
            let now = crate::common::time::unix_time_millis();
            Some(now.saturating_add(ttl_seconds.saturating_mul(1_000)))
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
            writer_epoch,
        }
    }

    /// Check if this record has expired
    pub fn is_expired(&self) -> bool {
        crate::common::time::is_expired_at(self.expiration, crate::common::time::unix_time_millis())
    }

    /// Get the size in bytes of this record when serialized
    pub fn estimated_size(&self) -> usize {
        let key_size = self.key.len();
        let value_size = self.value.as_ref().map_or(0, bytes::Bytes::len);
        let range_end_size = self.range_end.as_ref().map_or(0, bytes::Bytes::len);
        4 + 1 + 8 + 4 + key_size + 4 + value_size + range_end_size + 20 + 8
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_check_expiration_correctly() {
        // Arrange - record that expired long ago
        let mut expired_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
            1,
        );
        expired_record.expiration = Some(1); // 1 ms after epoch

        // Act
        let is_expired = expired_record.is_expired();

        // Assert
        assert!(is_expired);
    }

    #[test]
    fn should_check_non_expired_record() {
        // Arrange - record with far future expiration
        let mut future_record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
            1,
        );
        future_record.expiration = Some(u64::MAX);

        // Act
        let is_expired = future_record.is_expired();

        // Assert
        assert!(!is_expired);
    }

    #[test]
    fn should_consider_record_without_expiration_as_not_expired() {
        // Arrange
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            1,
            1,
        );

        // Act
        let is_expired = record.is_expired();

        // Assert
        assert!(!is_expired);
    }

    #[test]
    fn should_estimate_size_for_put_operation() {
        // Arrange
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"mykey"),
            Some(Bytes::from_static(b"myvalue")),
            42,
            1,
        );

        // Act
        let size = record.estimated_size();

        // Assert - should include all fields
        // 4 (cf_id) + 1 (op) + 8 (seq) + 4 (key len) + 5 (key) + 4 (value len) + 7 (value) + 20 (overhead) = 53
        assert!(size >= 53);
    }

    #[test]
    fn should_estimate_size_without_value() {
        // Arrange
        let record = WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"key"), None, 1, 1);

        // Act
        let size = record.estimated_size();

        // Assert - should not include value length
        assert!(size >= 4 + 1 + 8 + 4 + 3 + 4 + 20);
    }

    #[test]
    fn should_round_trip_ttl_record() {
        // Arrange
        let record = WalRecord::new_with_ttl(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
            3600, // 1 hour
            1,
        );

        // Act
        let has_expiration = record.expiration.is_some();

        // Assert
        assert!(has_expiration);
        assert!(!record.is_expired()); // Should not be expired immediately
    }

    #[test]
    fn should_wire_format_all_operation_kinds() {
        // Arrange (implicit - no setup needed)

        // Act
        let put = WalOpKind::Put.to_wire_format();
        let insert = WalOpKind::Insert.to_wire_format();
        let delete = WalOpKind::Delete.to_wire_format();
        let delete_range = WalOpKind::DeleteRange.to_wire_format();
        let txn_begin = WalOpKind::TxnBegin.to_wire_format();
        let txn_commit = WalOpKind::TxnCommit.to_wire_format();
        let txn_batch = WalOpKind::TxnBatch.to_wire_format();

        // Assert
        assert_eq!(put, 0);
        assert_eq!(insert, 1);
        assert_eq!(delete, 2);
        assert_eq!(delete_range, 3);
        assert_eq!(txn_begin, 4);
        assert_eq!(txn_commit, 5);
        assert_eq!(txn_batch, 6);
    }

    #[test]
    fn should_parse_wire_format_for_all_kinds() {
        // Arrange (implicit - no setup needed)

        // Act
        let put = WalOpKind::from_wire_format(0).unwrap();
        let insert = WalOpKind::from_wire_format(1).unwrap();
        let delete = WalOpKind::from_wire_format(2).unwrap();
        let delete_range = WalOpKind::from_wire_format(3).unwrap();
        let txn_begin = WalOpKind::from_wire_format(4).unwrap();
        let txn_commit = WalOpKind::from_wire_format(5).unwrap();
        let txn_batch = WalOpKind::from_wire_format(6).unwrap();

        // Assert
        assert_eq!(put, WalOpKind::Put);
        assert_eq!(insert, WalOpKind::Insert);
        assert_eq!(delete, WalOpKind::Delete);
        assert_eq!(delete_range, WalOpKind::DeleteRange);
        assert_eq!(txn_begin, WalOpKind::TxnBegin);
        assert_eq!(txn_commit, WalOpKind::TxnCommit);
        assert_eq!(txn_batch, WalOpKind::TxnBatch);
    }

    #[test]
    fn should_reject_invalid_wire_format() {
        // Arrange
        let invalid_format_code = 255;

        // Act
        let result = WalOpKind::from_wire_format(invalid_format_code);

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid WAL operation"));
    }
}
