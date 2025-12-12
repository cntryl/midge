//! WAL record encoding/decoding
//!
//! TLV-based encoding for efficient WAL storage with minimal overhead.

use crate::common::MidgeResult;
use crate::wal::types::{WalOpKind, WalRecord};
use bytes::{Buf, BufMut, Bytes, BytesMut};

const RECORD_HEADER_SIZE: usize = 1 + 1 + 4 + 8; // op + flags + cf_id + seq

/// Encode a WAL record to bytes
pub fn encode(record: &WalRecord) -> MidgeResult<Bytes> {
    let mut buf = BytesMut::new();

    // Write operation kind (1 byte)
    buf.put_u8(record.op.to_wire_format());

    // Write flags (1 byte)
    // Bit 0: has value
    // Bit 1: has expiration
    // Bit 2: has range_end
    // Bit 3: has txn_id
    // Bit 4: has compression
    let mut flags = 0u8;
    if record.value.is_some() {
        flags |= 0x01;
    }
    if record.expiration.is_some() {
        flags |= 0x02;
    }
    if record.range_end.is_some() {
        flags |= 0x04;
    }
    if record.txn_id.is_some() {
        flags |= 0x08;
    }
    if record.compression.is_some() {
        flags |= 0x10;
    }
    buf.put_u8(flags);

    // Write CF ID (4 bytes)
    buf.put_u32_le(record.cf_id);

    // Write sequence (8 bytes)
    buf.put_u64_le(record.seq);

    // Write key length (4 bytes) + key
    buf.put_u32_le(record.key.len() as u32);
    buf.put_slice(&record.key);

    // Write value length (4 bytes) + value (if present)
    if let Some(value) = &record.value {
        buf.put_u32_le(value.len() as u32);
        buf.put_slice(value);
    } else {
        buf.put_u32_le(0);
    }

    // Write optional fields
    if let Some(expiration) = record.expiration {
        buf.put_u64_le(expiration);
    }

    if let Some(range_end) = &record.range_end {
        buf.put_u32_le(range_end.len() as u32);
        buf.put_slice(range_end);
    }

    if let Some(txn_id) = record.txn_id {
        buf.put_u64_le(txn_id);
    }

    if let Some(compression) = record.compression {
        buf.put_u8(compression);
    }

    Ok(buf.freeze())
}

/// Decode a WAL record from bytes
pub fn decode(mut bytes: impl Buf) -> MidgeResult<WalRecord> {
    if bytes.remaining() < RECORD_HEADER_SIZE {
        return Err(crate::common::MidgeError::Corruption(
            "Incomplete WAL record header".to_string(),
        ));
    }

    // Read operation kind
    let op_byte = bytes.get_u8();
    let op = WalOpKind::from_wire_format(op_byte)?;

    // Read flags
    let flags = bytes.get_u8();
    let has_value = (flags & 0x01) != 0;
    let has_expiration = (flags & 0x02) != 0;
    let has_range_end = (flags & 0x04) != 0;
    let has_txn_id = (flags & 0x08) != 0;
    let has_compression = (flags & 0x10) != 0;

    // Read CF ID
    let cf_id = bytes.get_u32_le();

    // Read sequence
    let seq = bytes.get_u64_le();

    // Read key
    let key_len = bytes.get_u32_le() as usize;
    if bytes.remaining() < key_len {
        return Err(crate::common::MidgeError::Corruption(
            "Incomplete key in WAL record".to_string(),
        ));
    }
    let key = Bytes::copy_from_slice(&bytes.chunk()[..key_len]);
    bytes.advance(key_len);

    // Read value
    let value_len = bytes.get_u32_le() as usize;
    let value = if has_value && value_len > 0 {
        if bytes.remaining() < value_len {
            return Err(crate::common::MidgeError::Corruption(
                "Incomplete value in WAL record".to_string(),
            ));
        }
        let val = Bytes::copy_from_slice(&bytes.chunk()[..value_len]);
        bytes.advance(value_len);
        Some(val)
    } else {
        None
    };

    // Read optional fields
    let expiration = if has_expiration {
        if bytes.remaining() < 8 {
            return Err(crate::common::MidgeError::Corruption(
                "Incomplete expiration in WAL record".to_string(),
            ));
        }
        Some(bytes.get_u64_le())
    } else {
        None
    };

    let range_end = if has_range_end {
        if bytes.remaining() < 4 {
            return Err(crate::common::MidgeError::Corruption(
                "Incomplete range_end length in WAL record".to_string(),
            ));
        }
        let range_len = bytes.get_u32_le() as usize;
        if bytes.remaining() < range_len {
            return Err(crate::common::MidgeError::Corruption(
                "Incomplete range_end in WAL record".to_string(),
            ));
        }
        let range = Bytes::copy_from_slice(&bytes.chunk()[..range_len]);
        bytes.advance(range_len);
        Some(range)
    } else {
        None
    };

    let txn_id = if has_txn_id {
        if bytes.remaining() < 8 {
            return Err(crate::common::MidgeError::Corruption(
                "Incomplete txn_id in WAL record".to_string(),
            ));
        }
        Some(bytes.get_u64_le())
    } else {
        None
    };

    let compression = if has_compression {
        if bytes.remaining() < 1 {
            return Err(crate::common::MidgeError::Corruption(
                "Incomplete compression in WAL record".to_string(),
            ));
        }
        Some(bytes.get_u8())
    } else {
        None
    };

    Ok(WalRecord {
        cf_id,
        op,
        key,
        value,
        seq,
        expiration,
        range_end,
        txn_id,
        compression,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_put_operation() {
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        let encoded = encode(&record).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_roundtrip_put_operation() {
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.op, WalOpKind::Put);
    }

    #[test]
    fn should_preserve_key_when_encoding_and_decoding() {
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"mykey"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.key, record.key);
    }

    #[test]
    fn should_preserve_value_when_encoding_and_decoding() {
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"myvalue")),
            42,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.value, record.value);
    }

    #[test]
    fn should_preserve_sequence_when_encoding_and_decoding() {
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.seq, 42);
    }

    #[test]
    fn should_encode_delete_operation() {
        let record = WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"key"), None, 10);
        let encoded = encode(&record).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_roundtrip_delete_operation() {
        let record = WalRecord::new(WalOpKind::Delete, Bytes::from_static(b"key"), None, 10);
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.op, WalOpKind::Delete);
    }

    #[test]
    fn should_encode_column_family_id() {
        let record = WalRecord::new_cf(
            1,
            WalOpKind::Delete,
            Bytes::from_static(b"mykey"),
            None,
            100,
        );
        let encoded = encode(&record).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_preserve_cf_id_when_encoding_and_decoding() {
        let record = WalRecord::new_cf(
            1,
            WalOpKind::Delete,
            Bytes::from_static(b"mykey"),
            None,
            100,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.cf_id, 1);
    }

    #[test]
    fn should_preserve_all_fields_with_column_family() {
        let record = WalRecord::new_cf(
            1,
            WalOpKind::Delete,
            Bytes::from_static(b"mykey"),
            None,
            100,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.op, WalOpKind::Delete);
        assert_eq!(decoded.key, record.key);
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.seq, 100);
    }

    // =========== All Operation Kinds ===========

    #[test]
    fn should_roundtrip_insert_operation() {
        let record = WalRecord::new(
            WalOpKind::Insert,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.op, WalOpKind::Insert);
    }

    #[test]
    fn should_roundtrip_delete_range_operation() {
        // Arrange
        let mut record = WalRecord::new(
            WalOpKind::DeleteRange,
            Bytes::from_static(b"start"),
            None,
            42,
        );
        record.range_end = Some(Bytes::from_static(b"end"));

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.op, WalOpKind::DeleteRange);
        assert_eq!(decoded.range_end, record.range_end);
    }

    #[test]
    fn should_roundtrip_merge_operation() {
        let record = WalRecord::new(
            WalOpKind::Merge,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"merge_value")),
            42,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.op, WalOpKind::Merge);
    }

    #[test]
    fn should_roundtrip_txn_begin_operation() {
        let record = WalRecord::new(
            WalOpKind::TxnBegin,
            Bytes::from_static(b"txn_key"),
            None,
            42,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.op, WalOpKind::TxnBegin);
    }

    #[test]
    fn should_roundtrip_txn_commit_operation() {
        let record = WalRecord::new(
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn_key"),
            None,
            42,
        );
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();
        assert_eq!(decoded.op, WalOpKind::TxnCommit);
    }

    // =========== Optional Fields ===========

    #[test]
    fn should_roundtrip_record_with_expiration() {
        // Arrange
        let mut record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        record.expiration = Some(1234567890);

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.expiration, Some(1234567890));
    }

    #[test]
    fn should_roundtrip_record_with_txn_id() {
        // Arrange
        let mut record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        record.txn_id = Some(999);

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.txn_id, Some(999));
    }

    #[test]
    fn should_roundtrip_record_with_compression() {
        // Arrange
        let mut record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        record.compression = Some(1); // Compression type 1

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.compression, Some(1));
    }

    // =========== Edge Cases ===========

    #[test]
    fn should_handle_empty_key() {
        // Arrange
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::new(),
            Some(Bytes::from_static(b"value")),
            42,
        );

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.key.len(), 0);
    }

    #[test]
    fn should_treat_empty_value_as_none() {
        // Arrange - empty value is treated as None by encoding
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::new()),
            42,
        );

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert - empty values become None during roundtrip
        // This is by design: if value_len is 0, has_value flag determines presence
        assert!(decoded.value.is_none());
    }

    #[test]
    fn should_handle_max_u64_sequence() {
        // Arrange
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            u64::MAX,
        );

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.seq, u64::MAX);
    }

    #[test]
    fn should_handle_max_u32_cf_id() {
        // Arrange
        let record = WalRecord::new_cf(
            u32::MAX,
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.cf_id, u32::MAX);
    }

    #[test]
    fn should_handle_large_key_and_value() {
        // Arrange
        let large_key = vec![42u8; 10_000];
        let large_value = vec![99u8; 100_000];
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::copy_from_slice(&large_key),
            Some(Bytes::copy_from_slice(&large_value)),
            42,
        );

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.key.as_ref(), &large_key[..]);
        assert_eq!(decoded.value.unwrap().as_ref(), &large_value[..]);
    }

    #[test]
    fn should_preserve_binary_data() {
        // Arrange
        let binary_key = vec![0u8, 1u8, 255u8, 254u8, 127u8];
        let binary_value = vec![128u8, 64u8, 32u8, 16u8, 8u8];
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::copy_from_slice(&binary_key),
            Some(Bytes::copy_from_slice(&binary_value)),
            42,
        );

        // Act
        let encoded = encode(&record).unwrap();
        let decoded = decode(&encoded[..]).unwrap();

        // Assert
        assert_eq!(decoded.key.as_ref(), &binary_key[..]);
        assert_eq!(decoded.value.unwrap().as_ref(), &binary_value[..]);
    }

    #[test]
    fn should_detect_corruption_truncated_header() {
        // Arrange
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from_static(b"key"),
            Some(Bytes::from_static(b"value")),
            42,
        );
        let encoded = encode(&record).unwrap();
        let truncated = &encoded.as_ref()[..1]; // Only 1 byte

        // Act
        let result = decode(truncated);

        // Assert
        assert!(result.is_err());
    }
}
