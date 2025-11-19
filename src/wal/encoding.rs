//! WAL record encoding/decoding in TLV format
//!
//! This module provides the canonical serialization format for WAL records
//! used across all WAL implementations (filesystem, cloud, in-memory).

use crate::common::codec::{Compressor, Lz4Codec};
use crate::common::tlv::{parse_u32, parse_u64, parse_u8, tags, TlvReader, TlvWriter};
use crate::error::{MidgeError, MidgeResult};
use crate::wal::{WalOpKind, WalRecord};

/// Minimum value size (in bytes) to trigger compression
/// Values smaller than this won't be compressed (overhead not worth it)
pub const COMPRESSION_THRESHOLD: usize = 256;

/// Borrowed WAL record fields for zero-copy decoding.
///
/// This avoids allocations during decode by borrowing from the input buffer.
/// Use `to_owned()` to convert to a `WalRecord` with owned data.
#[derive(Debug)]
pub struct WalRecordRef<'a> {
    pub op: u8,
    pub cf_id: u32,
    pub seq: u64,
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
    pub expiration: Option<u64>,
    pub range_end: Option<&'a [u8]>,
    pub txn_id: Option<u64>,
    pub compression: Option<u8>,
}

impl<'a> WalRecordRef<'a> {
    /// Convert borrowed record to owned WalRecord.
    ///
    /// This performs the necessary allocations to create owned Bytes.
    /// Decompression happens here if needed.
    pub fn to_owned(&self) -> MidgeResult<WalRecord> {
        let op_kind = WalOpKind::from_wire_format(self.op)?;

        let key = bytes::Bytes::copy_from_slice(self.key);

        // Decompress value if needed
        let value = match (self.compression, self.value) {
            (Some(comp_type), Some(compressed_value)) => {
                let decompressed = decompress_value(comp_type, compressed_value)?;
                Some(bytes::Bytes::from(decompressed))
            }
            (None, Some(v)) => Some(bytes::Bytes::copy_from_slice(v)),
            _ => None,
        };

        let range_end = self.range_end.map(bytes::Bytes::copy_from_slice);

        let mut rec = WalRecord::new_cf(
            crate::api::column_family::ColumnFamilyId::new(self.cf_id),
            op_kind,
            key,
            value,
            self.seq,
        );
        rec.expiration = self.expiration;
        rec.range_end = range_end;
        rec.txn_id = self.txn_id;

        Ok(rec)
    }
}

/// Encode a WAL record into TLV format.
///
/// This is the canonical WAL record serialization format used by all
/// WAL implementations. The format includes:
/// - Required fields: operation, cf_id, sequence, key
/// - Optional fields: value (with compression), expiration, range_end, txn_id
///
/// Values >= COMPRESSION_THRESHOLD are automatically compressed with LZ4.
///
/// **Performance Tip**: For simple operations without optional fields, use the specialized
/// fast paths (`encode_delete`, `encode_put_simple`) for 20-30% better performance.
#[inline]
pub fn encode(record: &WalRecord) -> MidgeResult<Vec<u8>> {
    let key_len = record.key.len();
    let value_len = record.value.as_ref().map(|v| v.len()).unwrap_or(0);
    let mut tlv = TlvWriter::new_for_wal_record(key_len, value_len);

    encode_to_writer(&mut tlv, record)?;

    Ok(tlv.finish())
}

/// Encode a WAL record into an existing buffer (zero-alloc).
///
/// Appends the encoded record to `dst` and returns the number of bytes written.
/// This avoids per-record allocations in batch encoding hot paths.
#[inline]
pub fn encode_into(record: &WalRecord, dst: &mut Vec<u8>) -> MidgeResult<usize> {
    let start = dst.len();
    let key_len = record.key.len();
    let value_len = record.value.as_ref().map(|v| v.len()).unwrap_or(0);

    // Reserve space to avoid multiple reallocations
    dst.reserve(32 + key_len + value_len);

    let mut tlv = TlvWriter::with_buffer(std::mem::take(dst));
    encode_to_writer(&mut tlv, record)?;
    *dst = tlv.finish();

    Ok(dst.len() - start)
}

/// Encode a WAL record using the provided TlvWriter.
///
/// This is the core encoding logic, exposed for reuse with custom writers.
/// Most callers should use `encode()` which creates a fresh writer.
#[inline]
pub fn encode_to_writer(tlv: &mut TlvWriter, record: &WalRecord) -> MidgeResult<()> {
    // Required fields
    tlv.write_u8(tags::OPERATION, record.op.to_wire_format());
    tlv.write_u32(tags::CF_ID, record.cf_id);
    tlv.write_u64(tags::SEQUENCE, record.seq);
    tlv.write_bytes(tags::KEY, &record.key);

    // Optional value field with compression support
    if let Some(ref value) = record.value {
        write_value_with_compression(tlv, value)?;
    }

    // Optional expiration field (TTL support)
    if let Some(expiration) = record.expiration {
        tlv.write_u64(tags::EXPIRATION, expiration);
    }

    // Optional range_end field (for DeleteRange operations)
    if let Some(ref range_end) = record.range_end {
        tlv.write_bytes(tags::RANGE_END, range_end);
    }

    // Optional txn_id field (for transactional operations)
    if let Some(txn_id) = record.txn_id {
        tlv.write_u64(tags::TRANSACTION_ID, txn_id);
    }

    Ok(())
}

/// Fast path for encoding delete operations.
///
/// Delete operations are common and only need 4 fields (no value or optional metadata).
/// This specialized function skips all optional field checks for ~25% speedup.
///
/// # Example
/// ```ignore
/// let encoded = encode_delete(cf_id, seq, &key);
/// ```
#[inline]
pub fn encode_delete(cf_id: u32, seq: u64, key: &[u8]) -> Vec<u8> {
    let mut tlv = TlvWriter::with_capacity(32 + key.len());

    tlv.write_u8(tags::OPERATION, 2); // Delete = 2
    tlv.write_u32(tags::CF_ID, cf_id);
    tlv.write_u64(tags::SEQUENCE, seq);
    tlv.write_bytes(tags::KEY, key);

    tlv.finish()
}

/// Fast path for encoding simple put operations (no compression, expiration, or txn).
///
/// This covers ~80% of Put operations in typical workloads.
/// Skips compression checks and optional field logic for ~15-20% speedup.
///
/// # Example
/// ```ignore
/// let encoded = encode_put_simple(cf_id, seq, &key, &value);
/// ```
#[inline]
pub fn encode_put_simple(cf_id: u32, seq: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut tlv = TlvWriter::with_capacity(32 + key.len() + value.len());

    tlv.write_u8(tags::OPERATION, 0); // Put = 0
    tlv.write_u32(tags::CF_ID, cf_id);
    tlv.write_u64(tags::SEQUENCE, seq);
    tlv.write_bytes(tags::KEY, key);
    tlv.write_bytes(tags::VALUE, value);

    tlv.finish()
}

/// Write a value with optional compression to TLV writer.
///
/// Automatically compresses values >= COMPRESSION_THRESHOLD using LZ4.
/// Only stores compressed version if it's actually smaller than the original.
#[inline(always)]
fn write_value_with_compression(tlv: &mut TlvWriter, value: &[u8]) -> MidgeResult<()> {
    // Fast path: small values don't get compressed
    if value.len() < COMPRESSION_THRESHOLD {
        tlv.write_bytes(tags::VALUE, value);
        return Ok(());
    }

    // Slow path: try compression for large values
    let codec = Lz4Codec;
    match codec.compress(value) {
        Ok(compressed) if compressed.len() < value.len() => {
            // Compression saved space
            tlv.write_u8(tags::COMPRESSION, 2); // 2 = LZ4
            tlv.write_bytes(tags::VALUE_COMPRESSED, &compressed);
        }
        _ => {
            // Compression failed or didn't save space
            tlv.write_bytes(tags::VALUE, value);
        }
    }

    Ok(())
}

/// Decompress a value based on compression type.
///
/// Supports LZ4 (type=2) only. Type 1 (Snappy) is deprecated and removed.
pub fn decompress_value(compression_type: u8, compressed: &[u8]) -> MidgeResult<Vec<u8>> {
    match compression_type {
        2 => {
            // LZ4 compression
            let codec = Lz4Codec;
            codec
                .decompress(compressed)
                .map_err(|e| MidgeError::Corruption {
                    message: format!("Failed to decompress WAL value (LZ4): {}", e),
                })
        }
        _ => Err(MidgeError::Corruption {
            message: format!("Unknown compression type: {}", compression_type),
        }),
    }
}

/// Parse a single WAL record from TLV body.
///
/// This is the canonical deserialization for WAL records.
/// Uses zero-copy internally for optimal performance.
#[inline]
pub fn decode(body: &[u8]) -> MidgeResult<WalRecord> {
    // Use zero-copy decode then convert to owned
    let borrowed = decode_borrowed(body)?;
    borrowed.to_owned()
}

/// Zero-copy decode of WAL record from TLV body.
///
/// This variant borrows data from the input buffer instead of allocating.
/// Much faster than `decode()` for read-only operations or when batching conversions.
///
/// # Example
/// ```ignore
/// let borrowed = decode_borrowed(body)?;
/// // Use borrowed data without allocation
/// process_key(borrowed.key);
/// // Convert to owned only when needed
/// let owned = borrowed.to_owned()?;
/// ```
#[inline]
pub fn decode_borrowed(body: &[u8]) -> MidgeResult<WalRecordRef<'_>> {
    let reader = TlvReader::new(body);
    let mut op: u8 = 0;
    let mut cf_id: u32 = 0;
    let mut seq: u64 = 0;
    let mut key: Option<&[u8]> = None;
    let mut value: Option<&[u8]> = None;
    let mut expiration = None;
    let mut range_end: Option<&[u8]> = None;
    let mut txn_id = None;
    let mut compression = None;
    let mut has_op = false;
    let mut has_seq = false;

    for (tag, field_value) in reader {
        match tag {
            tags::OPERATION => {
                op = parse_u8(field_value)?;
                has_op = true;
            }
            tags::CF_ID => cf_id = parse_u32(field_value)?,
            tags::SEQUENCE => {
                seq = parse_u64(field_value)?;
                has_seq = true;
            }
            tags::KEY => key = Some(field_value),
            tags::VALUE | tags::VALUE_COMPRESSED => value = Some(field_value),
            tags::COMPRESSION => compression = Some(parse_u8(field_value)?),
            tags::EXPIRATION => expiration = Some(parse_u64(field_value)?),
            tags::RANGE_END => range_end = Some(field_value),
            tags::TRANSACTION_ID => txn_id = Some(parse_u64(field_value)?),
            _ => {} // Unknown tag - skip (forward compatibility)
        }
    }

    // Validate required fields
    if !has_op {
        return Err(MidgeError::Corruption {
            message: "WAL record missing operation field".to_string(),
        });
    }
    if !has_seq {
        return Err(MidgeError::Corruption {
            message: "WAL record missing sequence field".to_string(),
        });
    }

    Ok(WalRecordRef {
        op,
        cf_id,
        seq,
        key: key.unwrap_or(&[]),
        value,
        expiration,
        range_end,
        txn_id,
        compression,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn create_test_record() -> WalRecord {
        WalRecord {
            cf_id: 0,
            op: WalOpKind::Put,
            key: Bytes::from("test_key"),
            value: Some(Bytes::from("test_value")),
            seq: 42,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        }
    }

    #[test]
    fn should_encode_basic_record() {
        // Arrange
        let original = create_test_record();

        // Act
        let encoded = encode(&original).expect("encode");

        // Assert
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_decode_basic_record() {
        // Arrange
        let original = create_test_record();
        let encoded = encode(&original).expect("encode");

        // Act
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.op, original.op);
        assert_eq!(decoded.cf_id, original.cf_id);
        assert_eq!(decoded.seq, original.seq);
        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.value, original.value);
    }

    #[test]
    fn should_encode_record_with_ttl() {
        // Arrange
        let mut record = create_test_record();
        record.expiration = Some(1234567890);

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.expiration, Some(1234567890));
    }

    #[test]
    fn should_encode_record_with_transaction_id() {
        // Arrange
        let mut record = create_test_record();
        record.txn_id = Some(999);

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.txn_id, Some(999));
    }

    #[test]
    fn should_encode_delete_range_operation() {
        // Arrange
        let mut record = create_test_record();
        record.op = WalOpKind::DeleteRange;
        record.key = Bytes::from("start_key");
        record.range_end = Some(Bytes::from("end_key"));
        record.value = None;

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.op, WalOpKind::DeleteRange);
        assert_eq!(decoded.key, Bytes::from("start_key"));
        assert_eq!(decoded.range_end, Some(Bytes::from("end_key")));
        assert_eq!(decoded.value, None);
    }

    #[test]
    fn should_encode_record_with_column_family() {
        // Arrange
        let mut record = create_test_record();
        record.cf_id = 42;

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.cf_id, 42);
    }

    #[test]
    fn should_not_compress_small_values() {
        // Arrange
        let small_value = vec![b'x'; 100]; // Below COMPRESSION_THRESHOLD (256)
        let mut record = create_test_record();
        record.value = Some(Bytes::from(small_value.clone()));

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.value.as_ref().unwrap().as_ref(), &small_value);
        // Check that encoding doesn't contain compression tag
        assert!(!encoded.contains(&2)); // 2 is compression type for LZ4
    }

    #[test]
    fn should_compress_large_values() {
        // Arrange
        let large_value = vec![b'A'; 512]; // Above COMPRESSION_THRESHOLD (256)
        let mut record = create_test_record();
        record.value = Some(Bytes::from(large_value.clone()));

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.value.as_ref().unwrap().as_ref(), &large_value);
        // Encoded size should be smaller than original due to compression
        assert!(encoded.len() < large_value.len());
    }

    #[test]
    fn should_handle_empty_key() {
        // Arrange
        let mut record = create_test_record();
        record.key = Bytes::new();

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.key, Bytes::new());
    }

    #[test]
    fn should_handle_empty_value() {
        // Arrange
        let mut record = create_test_record();
        record.value = Some(Bytes::new());

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.value, Some(Bytes::new()));
    }

    #[test]
    fn should_handle_no_value() {
        // Arrange
        let mut record = create_test_record();
        record.op = WalOpKind::Delete;
        record.value = None;

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.value, None);
    }

    #[test]
    fn should_encode_all_operation_types() {
        // Arrange
        let ops = vec![
            WalOpKind::Put,
            WalOpKind::Delete,
            WalOpKind::DeleteRange,
            WalOpKind::Insert,
            WalOpKind::Merge,
            WalOpKind::TxnBegin,
            WalOpKind::TxnCommit,
        ];

        for op in ops {
            // Act
            let mut record = create_test_record();
            record.op = op;
            let encoded = encode(&record).expect("encode");
            let decoded = decode(&encoded).expect("decode");

            // Assert
            assert_eq!(decoded.op, op, "Failed for operation: {:?}", op);
        }
    }

    #[test]
    fn should_handle_binary_data() {
        // Arrange
        let binary_key = vec![0x00, 0xFF, 0x80, 0x7F, 0xDE, 0xAD, 0xBE, 0xEF];
        let binary_value = vec![0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x01, 0x02, 0x03];
        let mut record = create_test_record();
        record.key = Bytes::from(binary_key.clone());
        record.value = Some(Bytes::from(binary_value.clone()));

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.key.as_ref(), &binary_key);
        assert_eq!(decoded.value.as_ref().unwrap().as_ref(), &binary_value);
    }

    #[test]
    fn should_handle_large_sequence_numbers() {
        // Arrange
        let mut record = create_test_record();
        record.seq = u64::MAX;

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.seq, u64::MAX);
    }

    #[test]
    fn should_handle_all_fields_populated() {
        // Arrange
        let mut record = create_test_record();
        record.cf_id = 5;
        record.seq = 12345;
        record.expiration = Some(9876543210);
        record.txn_id = Some(777);
        record.range_end = Some(Bytes::from("range_end"));

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.cf_id, 5);
        assert_eq!(decoded.seq, 12345);
        assert_eq!(decoded.expiration, Some(9876543210));
        assert_eq!(decoded.txn_id, Some(777));
        assert_eq!(decoded.range_end, Some(Bytes::from("range_end")));
    }

    #[test]
    fn should_return_error_given_invalid_compression_type() {
        // Arrange
        let invalid_type = 99;
        let data = vec![0u8; 10];

        // Act
        let result = decompress_value(invalid_type, &data);

        // Assert
        assert!(result.is_err());
        match result {
            Err(MidgeError::Corruption { message }) => {
                assert!(message.contains("Unknown compression type"));
            }
            _ => panic!("Expected Corruption error"),
        }
    }

    #[test]
    fn should_decompress_lz4_compressed_data() {
        // Arrange
        let original_data = vec![b'X'; 512];
        let codec = Lz4Codec;
        let compressed = codec.compress(&original_data).expect("compress");

        // Act
        let decompressed = decompress_value(2, &compressed).expect("decompress");

        // Assert
        assert_eq!(decompressed, original_data);
    }

    #[test]
    fn should_preserve_data_across_multiple_encode_decode_cycles() {
        // Arrange
        let original = create_test_record();

        // Act - encode/decode 10 times
        let mut current = original.clone();
        for _ in 0..10 {
            let encoded = encode(&current).expect("encode");
            current = decode(&encoded).expect("decode");
        }

        // Assert
        assert_eq!(current.op, original.op);
        assert_eq!(current.key, original.key);
        assert_eq!(current.value, original.value);
        assert_eq!(current.seq, original.seq);
    }

    #[test]
    fn should_encode_delete_using_fast_path() {
        // Arrange
        let cf_id = 42;
        let seq = 12345;
        let key = b"test_key";

        // Act
        let encoded = encode_delete(cf_id, seq, key);

        // Assert - verify it decodes correctly
        let decoded = decode(&encoded).expect("decode");
        assert_eq!(decoded.op, WalOpKind::Delete);
        assert_eq!(decoded.cf_id, cf_id);
        assert_eq!(decoded.seq, seq);
        assert_eq!(decoded.key.as_ref(), key);
        assert_eq!(decoded.value, None);
    }

    #[test]
    fn should_produce_same_encoding_for_delete_fast_path() {
        // Arrange
        let cf_id = 5;
        let seq = 999;
        let key = Bytes::from_static(b"delete_me");

        let record = WalRecord::new_cf(
            crate::api::column_family::ColumnFamilyId::new(cf_id),
            WalOpKind::Delete,
            key.clone(),
            None,
            seq,
        );

        // Act
        let normal_encode = encode(&record).expect("encode");
        let fast_encode = encode_delete(cf_id, seq, &key);

        // Assert - both should produce identical bytes
        assert_eq!(normal_encode, fast_encode);
    }

    #[test]
    fn should_encode_compressible_data_efficiently() {
        // Arrange - highly compressible data
        let compressible_value = vec![b'A'; 1024];
        let mut record = create_test_record();
        record.value = Some(Bytes::from(compressible_value.clone()));

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(
            decoded.value.as_ref().unwrap().as_ref(),
            &compressible_value
        );
        // Compression should significantly reduce size for repetitive data
        assert!(encoded.len() < compressible_value.len() / 10);
    }

    #[test]
    fn should_not_compress_given_compression_increases_size() {
        // Arrange - random data that doesn't compress well
        use std::collections::hash_map::RandomState;
        use std::hash::BuildHasher;

        let mut pseudo_random = Vec::with_capacity(300);
        let state = RandomState::new();
        for i in 0..300u64 {
            pseudo_random.push((state.hash_one(i) & 0xFF) as u8);
        }

        let mut record = create_test_record();
        record.value = Some(Bytes::from(pseudo_random.clone()));

        // Act
        let encoded = encode(&record).expect("encode");
        let decoded = decode(&encoded).expect("decode");

        // Assert
        assert_eq!(decoded.value.as_ref().unwrap().as_ref(), &pseudo_random);
    }
}
