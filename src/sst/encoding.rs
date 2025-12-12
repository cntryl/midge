//! SST entry encoding/decoding in TLV format

use crate::common::MidgeError;
use crate::common::MidgeResult;
use bytes::{BufMut, BytesMut};

/// Restart point interval for block building
/// Every RESTART_INTERVAL entries, a restart point is stored
/// for fast binary search within blocks
pub const RESTART_INTERVAL: usize = 16;

/// TLV tags for SST entries
pub mod tags {
    pub const SHARED_PREFIX_LEN: u8 = 1;
    pub const KEY_DELTA: u8 = 2;
    pub const VALUE: u8 = 3;
    pub const SEQUENCE: u8 = 4;
    pub const ENTRY_TYPE: u8 = 5;
    pub const EXPIRATION: u8 = 6;
}

/// Encode a single SST entry in TLV format
///
/// Format:
/// - SHARED_PREFIX_LEN (varint32): bytes shared with previous key
/// - KEY_DELTA (bytes): suffix of key after shared prefix
/// - VALUE (bytes): optional value (omitted for tombstones)
/// - SEQUENCE (u64): sequence number
/// - ENTRY_TYPE (u8): 0=Put, 1=Insert, 2=Delete, 3=Merge
/// - EXPIRATION (u64): optional TTL expiration timestamp
pub fn encode(
    key_delta: &[u8],
    shared_len: u32,
    value: Option<&[u8]>,
    seq: u64,
    entry_type: u8,
    expiration: Option<u64>,
) -> Vec<u8> {
    let mut buf = BytesMut::new();

    // Write shared prefix length as tagged varint
    encode_varint_with_tag(&mut buf, tags::SHARED_PREFIX_LEN, shared_len);

    // Write key delta
    encode_bytes_with_tag(&mut buf, tags::KEY_DELTA, key_delta);

    // Write value if present
    let is_tombstone = entry_type == 2;
    let user_value = value.unwrap_or(&[]);
    if !is_tombstone || !user_value.is_empty() {
        encode_bytes_with_tag(&mut buf, tags::VALUE, user_value);
    }

    // Write sequence number
    encode_u64_with_tag(&mut buf, tags::SEQUENCE, seq);

    // Write entry type
    encode_u8_with_tag(&mut buf, tags::ENTRY_TYPE, entry_type);

    // Write expiration if present
    if let Some(exp) = expiration {
        encode_u64_with_tag(&mut buf, tags::EXPIRATION, exp);
    }

    buf.to_vec()
}

/// Parsed SST entry from encoded data
#[derive(Debug, Clone)]
pub struct TlvEntry {
    pub shared_len: u32,
    pub key_delta: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub sequence: u64,
    pub entry_type: u8,
    pub expiration: Option<u64>,
    pub bytes_consumed: usize,
}

/// Decode a single TLV entry from data starting at offset
pub fn decode(data: &[u8], offset: usize) -> MidgeResult<(TlvEntry, usize)> {
    if offset >= data.len() {
        return Err(MidgeError::Corruption("Offset beyond data length".into()));
    }

    let mut cursor = offset;
    let mut shared_len = 0u32;
    let mut shared_len_seen = false;
    let mut key_delta = Vec::new();
    let mut value: Option<Vec<u8>> = None;
    let mut sequence = 0u64;
    let mut entry_type = 0u8;
    let mut expiration: Option<u64> = None;

    // Parse TLV fields until we hit end of data or next entry
    loop {
        if cursor >= data.len() {
            break;
        }

        let (tag, tag_data, consumed) = decode_tlv_field(&data[cursor..])?;
        if tag == 0 {
            break; // End of entry
        }

        match tag {
            _ if tag != tags::SHARED_PREFIX_LEN => {
                // If we haven't seen shared prefix length but see another tag,
                // this may be start of next entry
                if !shared_len_seen && key_delta.is_empty() {
                    break;
                }
            }
            _ => {}
        }

        match tag {
            tags::SHARED_PREFIX_LEN => {
                shared_len = decode_varint32(tag_data)?;
                shared_len_seen = true;
            }
            tags::KEY_DELTA => {
                key_delta = tag_data.to_vec();
            }
            tags::VALUE => {
                value = Some(tag_data.to_vec());
            }
            tags::SEQUENCE => {
                if tag_data.len() == 8 {
                    sequence = u64::from_be_bytes([
                        tag_data[0],
                        tag_data[1],
                        tag_data[2],
                        tag_data[3],
                        tag_data[4],
                        tag_data[5],
                        tag_data[6],
                        tag_data[7],
                    ]);
                }
            }
            tags::ENTRY_TYPE => {
                if !tag_data.is_empty() {
                    entry_type = tag_data[0];
                }
            }
            tags::EXPIRATION => {
                if tag_data.len() == 8 {
                    expiration = Some(u64::from_be_bytes([
                        tag_data[0],
                        tag_data[1],
                        tag_data[2],
                        tag_data[3],
                        tag_data[4],
                        tag_data[5],
                        tag_data[6],
                        tag_data[7],
                    ]));
                }
            }
            _ => {}
        }

        cursor += consumed;
    }

    if key_delta.is_empty() {
        return Err(MidgeError::Corruption(
            "Missing key_delta in TLV entry".into(),
        ));
    }

    Ok((
        TlvEntry {
            shared_len,
            key_delta,
            value,
            sequence,
            entry_type,
            expiration,
            bytes_consumed: cursor - offset,
        },
        cursor,
    ))
}

// ─── Helper functions ────────────────────────────────────────────────────

fn encode_varint32(buf: &mut BytesMut, mut value: u32) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

fn encode_varint_with_tag(buf: &mut BytesMut, tag: u8, value: u32) {
    buf.put_u8(tag);
    // Encode the varint, then put its length as length byte
    let mut temp = BytesMut::new();
    encode_varint32(&mut temp, value);
    buf.put_u8(temp.len() as u8);
    buf.put_slice(&temp);
}

fn encode_bytes_with_tag(buf: &mut BytesMut, tag: u8, data: &[u8]) {
    buf.put_u8(tag);
    encode_varint32(buf, data.len() as u32);
    buf.put_slice(data);
}

fn encode_u64_with_tag(buf: &mut BytesMut, tag: u8, value: u64) {
    buf.put_u8(tag);
    buf.put_u8(8); // 8 bytes for u64
    buf.put_u64(value);
}

fn encode_u8_with_tag(buf: &mut BytesMut, tag: u8, value: u8) {
    buf.put_u8(tag);
    buf.put_u8(1); // 1 byte
    buf.put_u8(value);
}

fn decode_varint32(data: &[u8]) -> MidgeResult<u32> {
    let mut result = 0u32;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        if i >= 5 {
            return Err(MidgeError::Corruption("varint32 overflow".into()));
        }
        result |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(MidgeError::Corruption("varint32 incomplete".into()))
}

fn decode_tlv_field(data: &[u8]) -> MidgeResult<(u8, &[u8], usize)> {
    if data.is_empty() {
        return Ok((0, &[], 0));
    }

    let tag = data[0];
    if tag == 0 {
        return Ok((0, &[], 1));
    }

    if data.len() < 2 {
        return Err(MidgeError::Corruption("TLV field too short".into()));
    }

    let len = data[1] as usize;
    if data.len() < 2 + len {
        return Err(MidgeError::Corruption("TLV field data truncated".into()));
    }

    let value = &data[2..2 + len];
    Ok((tag, value, 2 + len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_key_delta() {
        // Arrange & Act
        let encoded = encode(b"mykey", 0, None, 0, 0, None);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.key_delta, b"mykey");
    }

    #[test]
    fn should_encode_and_decode_with_value() {
        // Arrange & Act
        let encoded = encode(b"key", 0, Some(b"myvalue"), 0, 0, None);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.key_delta, b"key");
        assert_eq!(entry.value, Some(b"myvalue".to_vec()));
    }

    #[test]
    fn should_encode_and_decode_sequence() {
        // Arrange & Act
        let encoded = encode(b"key", 0, None, 12345, 0, None);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.sequence, 12345);
    }

    #[test]
    fn should_encode_and_decode_entry_type_delete() {
        // Arrange & Act
        let encoded = encode(b"key", 0, None, 0, 2, None);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.entry_type, 2);
    }

    #[test]
    fn should_encode_and_decode_with_expiration() {
        // Arrange
        let exp = 1234567890u64;
        
        // Act
        let encoded = encode(b"key", 0, None, 0, 0, Some(exp));
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.expiration, Some(exp));
    }

    #[test]
    fn should_encode_and_decode_shared_prefix() {
        // Arrange & Act
        let encoded = encode(b"suffix", 42, Some(b"val"), 0, 0, None);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.shared_len, 42);
    }

    #[test]
    fn should_return_bytes_consumed() {
        // Arrange
        let encoded = encode(b"key", 0, Some(b"val"), 42, 0, None);
        
        // Act
        let (_entry, consumed) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn should_roundtrip_all_fields() {
        // Arrange
        let key = b"test_key";
        let value = Some(b"test_value" as &[u8]);
        let seq = 999u64;
        let op_type = 1u8;
        let exp = Some(5555u64);
        let shared = 5u32;

        // Act
        let encoded = encode(key, shared, value, seq, op_type, exp);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.key_delta, key);
        assert_eq!(entry.value, value.map(|v| v.to_vec()));
        assert_eq!(entry.sequence, seq);
        assert_eq!(entry.entry_type, op_type);
        assert_eq!(entry.expiration, exp);
        assert_eq!(entry.shared_len, shared);
    }

    #[test]
    fn should_handle_binary_keys() {
        // Arrange
        let binary_key = vec![0u8, 1u8, 255u8, 254u8, 128u8];
        
        // Act
        let encoded = encode(&binary_key, 0, Some(b"val"), 0, 0, None);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.key_delta, binary_key);
    }

    #[test]
    fn should_decode_from_offset_zero() {
        // Arrange
        let encoded = encode(b"key", 0, Some(b"val"), 0, 0, None);
        
        // Act
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.key_delta, b"key");
    }

    #[test]
    fn should_handle_invalid_offset_beyond_data() {
        // Arrange
        let encoded = encode(b"key", 0, Some(b"val"), 0, 0, None);
        
        // Act
        let result = decode(&encoded, encoded.len() + 100);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_produces_same_output_for_same_inputs() {
        // Arrange & Act
        let enc1 = encode(b"key", 5, Some(b"val"), 100, 1, Some(200));
        let enc2 = encode(b"key", 5, Some(b"val"), 100, 1, Some(200));

        // Assert
        assert_eq!(enc1, enc2);
    }

    #[test]
    fn should_create_tlv_entry_from_decode() {
        // Arrange
        let encoded = encode(b"test", 0, Some(b"data"), 42, 1, Some(100));
        
        // Act
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.key_delta, b"test");
        assert_eq!(entry.value, Some(b"data".to_vec()));
        assert_eq!(entry.sequence, 42);
        assert_eq!(entry.entry_type, 1);
        assert_eq!(entry.expiration, Some(100));
        assert!(entry.bytes_consumed > 0);
    }

    #[test]
    fn should_tlv_entry_be_cloneable() {
        // Arrange
        let encoded = encode(b"test", 0, Some(b"data"), 42, 1, Some(100));
        let (entry, _) = decode(&encoded, 0).unwrap();
        
        // Act
        let cloned = entry.clone();

        // Assert
        assert_eq!(entry.key_delta, cloned.key_delta);
        assert_eq!(entry.value, cloned.value);
        assert_eq!(entry.sequence, cloned.sequence);
    }

    #[test]
    fn should_tlv_entry_be_debuggable() {
        // Arrange
        let encoded = encode(b"test", 0, Some(b"data"), 42, 1, Some(100));
        let (entry, _) = decode(&encoded, 0).unwrap();
        
        // Act
        let debug_str = format!("{:?}", entry);

        // Assert
        assert!(debug_str.contains("TlvEntry") || debug_str.contains("key_delta"));
    }
}
