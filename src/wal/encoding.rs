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
        let record = WalRecord::new(
            WalOpKind::Delete,
            Bytes::from_static(b"key"),
            None,
            10,
        );
        let encoded = encode(&record).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_roundtrip_delete_operation() {
        let record = WalRecord::new(
            WalOpKind::Delete,
            Bytes::from_static(b"key"),
            None,
            10,
        );
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
}
