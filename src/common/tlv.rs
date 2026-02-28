//! Common TLV (Tag-Length-Value) encoding/decoding helpers.
//!
//! Provides primitives for encoding and decoding tagged fields,
//! used by both WAL and SST formats.

use crate::common::MidgeError;
use crate::common::MidgeResult;
use bytes::{BufMut, BytesMut};

/// Encode a varint32 value into the buffer
#[inline]
pub fn encode_varint32(buf: &mut BytesMut, mut value: u32) {
    while value >= 0x80 {
        buf.put_u8((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

/// Encode a varint32 with a tag and length prefix (single-pass, zero-allocation)
#[inline]
pub fn encode_varint_with_tag(buf: &mut BytesMut, tag: u8, mut value: u32) {
    buf.put_u8(tag);

    // Max 5 bytes for u32 varint
    let mut tmp = [0u8; 5];
    let mut i = 0;

    while value >= 0x80 {
        tmp[i] = ((value & 0x7f) as u8) | 0x80;
        value >>= 7;
        i += 1;
    }
    tmp[i] = value as u8;
    i += 1;

    buf.put_u8(i as u8);
    buf.put_slice(&tmp[..i]);
}

/// Encode arbitrary bytes with a tag and length prefix
#[inline]
pub fn encode_bytes_with_tag(buf: &mut BytesMut, tag: u8, data: &[u8]) -> MidgeResult<()> {
    buf.put_u8(tag);
    let len32 = u32::try_from(data.len())
        .map_err(|_| MidgeError::Internal("TLV value too large: exceeds u32::MAX".to_string()))?;
    encode_varint32(buf, len32);
    buf.put_slice(data);
    Ok(())
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
    if data.is_empty() {
        return Err(MidgeError::Corruption("varint32 incomplete".into()));
    }

    let b0 = data[0];
    if b0 < 0x80 {
        return Ok(b0 as u32);
    }

    let mut result = (b0 & 0x7f) as u32;
    let mut shift = 7;

    for (i, &byte) in data.iter().enumerate().skip(1) {
        if i >= 5 {
            return Err(MidgeError::Corruption("varint32 overflow".into()));
        }

        result |= ((byte & 0x7f) as u32) << shift;
        if byte < 0x80 {
            return Ok(result);
        }
        shift += 7;
    }

    Err(MidgeError::Corruption("varint32 incomplete".into()))
}

#[inline]
fn decode_varint32_with_len(data: &[u8]) -> MidgeResult<(u32, usize)> {
    if data.is_empty() {
        return Err(MidgeError::Corruption("varint32 incomplete".into()));
    }

    let b0 = data[0];
    if b0 < 0x80 {
        return Ok((b0 as u32, 1));
    }

    let mut result = (b0 & 0x7f) as u32;
    let mut shift = 7;

    for (i, &byte) in data.iter().enumerate().skip(1) {
        if i >= 5 {
            return Err(MidgeError::Corruption("varint32 overflow".into()));
        }

        result |= ((byte & 0x7f) as u32) << shift;
        if byte < 0x80 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }

    Err(MidgeError::Corruption("varint32 incomplete".into()))
}

/// Decode a single TLV field from data
/// Returns (tag, value_data, bytes_consumed)
#[inline(always)]
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

    let (len, len_bytes) = decode_varint32_with_len(&data[1..])?;
    let len = len as usize;
    let header_len = 1 + len_bytes;

    if data.len() < header_len + len {
        return Err(MidgeError::Corruption("TLV field data truncated".into()));
    }

    let value = &data[header_len..header_len + len];
    Ok((tag, value, header_len + len))
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
        // Arrange
        let mut buf = BytesMut::new();

        // Act
        encode_varint32(&mut buf, 42);
        let decoded = decode_varint32(&buf).unwrap();

        // Assert
        assert_eq!(decoded, 42);
    }

    #[test]
    fn should_encode_decode_varint_with_tag() {
        // Arrange
        let mut buf = BytesMut::new();
        let value = 300u32;

        // Act
        encode_varint_with_tag(&mut buf, 7, value);
        let (tag, value_data, consumed) = decode_tlv_field(&buf).unwrap();

        // Assert
        assert_eq!(tag, 7);
        let decoded = decode_varint32(value_data).unwrap();
        assert_eq!(decoded, value);
        assert!(consumed > 2); // tag + len + at least 1 byte
    }

    #[test]
    fn should_encode_decode_bytes_with_tag() {
        // Arrange
        let mut buf = BytesMut::new();
        let data = b"hello";

        // Act
        encode_bytes_with_tag(&mut buf, 5, data).unwrap();
        let (tag, value, consumed) = decode_tlv_field(&buf).unwrap();

        // Assert
        assert_eq!(tag, 5);
        assert_eq!(value, data);
        assert_eq!(consumed, 2 + data.len());
    }

    #[test]
    fn should_decode_varint_length_for_bytes() {
        // Arrange
        let mut buf = BytesMut::new();
        let data = vec![0u8; 300];

        // Act
        encode_bytes_with_tag(&mut buf, 9, &data).unwrap();
        let (tag, value, consumed) = decode_tlv_field(&buf).unwrap();

        // Assert
        assert_eq!(tag, 9);
        assert_eq!(value.len(), data.len());
        assert_eq!(value, data.as_slice());
        assert!(consumed > 2 + data.len());
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
