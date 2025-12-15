//! Common TLV (Tag-Length-Value) encoding/decoding helpers.
//!
//! Provides primitives for encoding and decoding tagged fields,
//! used by both WAL and SST formats.

use bytes::{BufMut, BytesMut};
use crate::common::MidgeError;
use crate::common::MidgeResult;

/// Encode a varint32 value into the buffer
#[inline(always)]
pub fn encode_varint32(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

#[inline]
/// Encode a varint32 with a tag and length prefix
pub fn encode_varint_with_tag(buf: &mut BytesMut, tag: u8, value: u32) {
    buf.put_u8(tag);
    // Encode the varint, then put its length as length byte
    let mut temp = BytesMut::new();
    encode_varint32(&mut temp, value);
    buf.put_u8(temp.len() as u8);
    buf.put_slice(&temp);
}

#[inline]
/// Encode arbitrary bytes with a tag and length prefix
pub fn encode_bytes_with_tag(buf: &mut BytesMut, tag: u8, data: &[u8]) {
    buf.put_u8(tag);
    encode_varint32(buf, data.len() as u32);
    buf.put_slice(data);
}

/// Encode a u64 with a tag and length prefix
#[inline]
pub fn encode_u64_with_tag(buf: &mut BytesMut, tag: u8, value: u64) {
    buf.put_u8(tag);
    buf.put_u8(8); // 8 bytes for u64
    buf.put_u64(value);
}

/// Encode a u8 with a tag and length prefix
#[inline]
pub fn encode_u8_with_tag(buf: &mut BytesMut, tag: u8, value: u8) {
    buf.put_u8(tag);
    buf.put_u8(1); // 1 byte
    buf.put_u8(value);
}

/// Decode a varint32 from a data slice
#[inline(always)]
pub fn decode_varint32(data: &[u8]) -> MidgeResult<u32> {
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

#[inline]
/// Decode a single TLV field from data
/// Returns (tag, value_data, bytes_consumed)
pub fn decode_tlv_field(data: &[u8]) -> MidgeResult<(u8, &[u8], usize)> {
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
    fn should_encode_decode_varint32() {
        // Arrange
        let mut buf = BytesMut::new();
        let value = 300u32;

        // Act
        encode_varint32(&mut buf, value);
        let decoded = decode_varint32(&buf).unwrap();

        // Assert
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_decode_varint32_small() {
        // Arrange & Act
        let mut buf = BytesMut::new();
        encode_varint32(&mut buf, 42);
        let decoded = decode_varint32(&buf).unwrap();

        // Assert
        assert_eq!(decoded, 42);
    }

    #[test]
    fn should_encode_decode_bytes_with_tag() {
        // Arrange
        let mut buf = BytesMut::new();
        let data = b"hello";

        // Act
        encode_bytes_with_tag(&mut buf, 5, data);
        let (tag, value, consumed) = decode_tlv_field(&buf).unwrap();

        // Assert
        assert_eq!(tag, 5);
        assert_eq!(value, data);
        assert_eq!(consumed, 2 + data.len());
    }

    #[test]
    fn should_encode_decode_u64_with_tag() {
        // Arrange
        let mut buf = BytesMut::new();
        let value = 0x0102030405060708u64;

        // Act
        encode_u64_with_tag(&mut buf, 10, value);
        let (tag, value_data, _) = decode_tlv_field(&buf).unwrap();

        // Assert
        assert_eq!(tag, 10);
        assert_eq!(value_data.len(), 8);
        assert_eq!(
            u64::from_be_bytes(value_data[..].try_into().unwrap()),
            value
        );
    }
}
