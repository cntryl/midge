//! SST entry encoding/decoding in TLV format

use crate::common::MidgeError;
use crate::common::MidgeResult;
use bytes::{BufMut, Bytes, BytesMut};

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
    fn should_roundtrip_entry_when_encoding_and_decoding_simple() {
        // Arrange
        // Act
        let encoded = encode(b"key", 0, Some(b"value"), 42, 0, None);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.key_delta, b"key");
        assert_eq!(entry.value, Some(b"value".to_vec()));
        assert_eq!(entry.sequence, 42);
        assert_eq!(entry.entry_type, 0);
        assert_eq!(entry.expiration, None);
    }

    #[test]
    fn should_preserve_expiration_when_encoding_and_decoding() {
        // Arrange
        // Act
        let encoded = encode(b"key", 0, Some(b"val"), 10, 0, Some(1234567890));
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.sequence, 10);
        assert_eq!(entry.expiration, Some(1234567890));
    }

    #[test]
    fn should_preserve_tombstone_when_encoding_and_decoding() {
        // Arrange
        // Act
        let encoded = encode(b"key", 0, None, 5, 2, None);
        let (entry, _) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.entry_type, 2);
        assert_eq!(entry.value, None);
    }
}
