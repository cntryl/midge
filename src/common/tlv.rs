// Copyright (c) Midge Contributors
// SPDX-License-Identifier: Apache-2.0

//! TLV (Type-Length-Value) encoding for extensible serialization.
//!
//! Zero-copy, allocation-free primitives used across WAL and SST encodings.
//!
//! ## Tag format
//! High nibble encodes the wire type, low nibble encodes a small field id:
//! `tag = (wire_type << 4) | (field_id & 0x0F)`.
//! Example: `0x22 => WireType::U32 (2) with field id 2`.

use crate::error::{MidgeError, MidgeResult};

/// Wire types for TLV encoding.
///
/// NOTE: For `WireType::Varint`, the value is encoded *inline* (no length
/// prefix). For `WireType::Bytes`, the value is prefixed by a varint length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireType {
    U8 = 0,
    U16 = 1,
    U32 = 2,
    U64 = 3,
    Varint = 4,
    Bytes = 5,
}

impl WireType {
    #[inline(always)]
    pub fn from_tag(tag: u8) -> Option<Self> {
        match tag >> 4 {
            0 => Some(WireType::U8),
            1 => Some(WireType::U16),
            2 => Some(WireType::U32),
            3 => Some(WireType::U64),
            4 => Some(WireType::Varint),
            5 => Some(WireType::Bytes),
            _ => None,
        }
    }

    /// Returns the high-nibble tag prefix for this wire type.
    #[inline(always)]
    pub const fn to_tag_prefix(self) -> u8 {
        (self as u8) << 4
    }
}

/// Compose a tag from wire type and a 4-bit field id.
#[inline(always)]
pub const fn make_tag(wire: WireType, id: u8) -> u8 {
    wire.to_tag_prefix() | (id & 0x0F)
}

/// Unified TLV tags for all Midge serialization.
///
/// Tag allocation strategy:
/// - Field IDs 1-7:   Common fields (shared by WAL and SST)
/// - Field IDs 8-10:  WAL-specific fields
/// - Field IDs 11-13: SST-specific fields (delta encoding)
/// - Field IDs 14-15: Reserved for future use
///
/// Wire types are encoded in the high nibble, field ID in the low nibble.
pub mod tags {
    // ============================================================================
    // Common fields (1-7) - Used by both WAL and SST formats
    // ============================================================================

    /// Operation type: Put=0, Delete=1, DeleteRange=2
    pub const OPERATION: u8 = 0x01; // U8 | id=1

    /// Column family ID (0 = default)
    pub const CF_ID: u8 = 0x22; // U32 | id=2

    /// Sequence number for versioning
    pub const SEQUENCE: u8 = 0x33; // U64 | id=3

    /// User key (or full key in WAL)
    pub const KEY: u8 = 0x54; // BYTES | id=4

    /// Value data
    pub const VALUE: u8 = 0x55; // BYTES | id=5

    /// Expiration timestamp (milliseconds since epoch)
    pub const EXPIRATION: u8 = 0x36; // U64 | id=6

    /// Entry type for SST: Value=0, Tombstone=1, RangeTombstone=2
    pub const ENTRY_TYPE: u8 = 0x07; // U8 | id=7

    // ============================================================================
    // WAL-specific fields (8-10)
    // ============================================================================

    /// User-provided timestamp (for time-series or TTL extraction)
    pub const USER_TIMESTAMP: u8 = 0x38; // U64 | id=8

    /// Compression type: None=0, Snappy=1, LZ4=2
    pub const COMPRESSION: u8 = 0x09; // U8 | id=9

    /// Compressed value (alternative to VALUE when compressed)
    pub const VALUE_COMPRESSED: u8 = 0x5A; // BYTES | id=10

    // ============================================================================
    // SST-specific fields (11-13) - Delta encoding for space efficiency
    // ============================================================================

    /// Shared prefix length (for delta-encoded keys)
    pub const SHARED_PREFIX_LEN: u8 = 0x4B; // VARINT | id=11

    /// Key delta (non-shared suffix)
    pub const KEY_DELTA: u8 = 0x5C; // BYTES | id=12

    /// End key for range operations (DeleteRange, RangeTombstone)
    pub const RANGE_END: u8 = 0x5D; // BYTES | id=13

    // ============================================================================
    // Reserved for future use (14-15)
    // ============================================================================

    /// Transaction ID for MVCC support
    pub const TRANSACTION_ID: u8 = 0x3E; // U64 | id=14

    // Field ID 15 is reserved for future use
}

// (Removed legacy `wal_tags` and `sst_tags` compatibility modules.)

/// TLV writer for encoding records.
pub struct TlvWriter {
    buffer: Vec<u8>,
}

impl TlvWriter {
    #[inline]
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(128),
        }
    }

    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(cap),
        }
    }

    /// Create a TlvWriter that reuses an existing buffer.
    ///
    /// This avoids allocations when encoding into an existing arena.
    #[inline]
    pub fn with_buffer(buffer: Vec<u8>) -> Self {
        Self { buffer }
    }

    #[inline]
    pub fn new_for_wal_record(key_len: usize, value_len: usize) -> Self {
        // small fixed overhead + key + value
        Self::with_capacity(48 + key_len + value_len)
    }

    #[inline(always)]
    pub fn write_u8(&mut self, tag: u8, value: u8) {
        self.buffer.extend_from_slice(&[tag, value]);
    }

    #[inline(always)]
    pub fn write_u16(&mut self, tag: u8, value: u16) {
        self.buffer.push(tag);
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    #[inline(always)]
    pub fn write_u32(&mut self, tag: u8, value: u32) {
        self.buffer.push(tag);
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    #[inline(always)]
    pub fn write_u64(&mut self, tag: u8, value: u64) {
        self.buffer.push(tag);
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    #[inline(always)]
    pub fn write_varint(&mut self, tag: u8, value: u64) {
        self.buffer.push(tag);
        encode_varint64(&mut self.buffer, value);
    }

    #[inline(always)]
    pub fn write_varint32(&mut self, tag: u8, value: u32) {
        self.buffer.push(tag);
        encode_varint32(&mut self.buffer, value);
    }

    #[inline(always)]
    pub fn write_bytes(&mut self, tag: u8, data: &[u8]) {
        self.buffer.push(tag);
        encode_varint32(&mut self.buffer, data.len() as u32);
        self.buffer.extend_from_slice(data);
    }

    /// Write bytes without length prefix for small fixed-size data (<= 8 bytes).
    ///
    /// This is an optimization for small keys or fixed-size metadata where
    /// the length is known from context. Saves ~2 bytes (varint prefix) and
    /// some encoding overhead.
    ///
    /// # Safety
    /// Reader must know the expected length to decode this field correctly.
    #[inline(always)]
    pub fn write_bytes_inline(&mut self, tag: u8, data: &[u8]) {
        debug_assert!(data.len() <= 8, "inline bytes must be <= 8 bytes");
        self.buffer.push(tag);
        self.buffer.extend_from_slice(data);
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    #[inline(always)]
    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Get the current capacity of the internal buffer.
    ///
    /// Useful for deciding whether to reuse this writer or allocate a new one.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Reset the writer while preserving the allocated capacity.
    ///
    /// This is more efficient than creating a new writer when encoding
    /// multiple records of similar size.
    #[inline]
    pub fn reset(&mut self) {
        self.buffer.clear();
    }
}

impl Default for TlvWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-copy TLV reader.
///
/// Iterator semantics:
/// - Returns `(tag, slice)`
/// - For `WireType::Varint`, `slice` is the **raw varint encoding** (not decoded)
/// - For `WireType::Bytes`, `slice` is the payload (length prefix already consumed)
pub struct TlvReader<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> TlvReader<'a> {
    #[inline]
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, cursor: 0 }
    }

    #[inline]
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// Fallible variant that surfaces corruption explicitly.
    /// Keeps the same item type as the `Iterator` impl, but returns a Result.
    #[inline]
    pub fn try_next(&mut self) -> MidgeResult<Option<(u8, &'a [u8])>> {
        if self.cursor >= self.data.len() {
            return Ok(None);
        }

        let tag = self.data[self.cursor];
        self.cursor += 1;

        let Some(wire_type) = WireType::from_tag(tag) else {
            return Err(MidgeError::Corruption {
                message: "unknown wire type".into(),
            });
        };

        let slice = match wire_type {
            WireType::U8 => self.take(1).ok_or_else(|| MidgeError::Corruption {
                message: "truncated u8".into(),
            })?,
            WireType::U16 => self.take(2).ok_or_else(|| MidgeError::Corruption {
                message: "truncated u16".into(),
            })?,
            WireType::U32 => self.take(4).ok_or_else(|| MidgeError::Corruption {
                message: "truncated u32".into(),
            })?,
            WireType::U64 => self.take(8).ok_or_else(|| MidgeError::Corruption {
                message: "truncated u64".into(),
            })?,
            WireType::Varint => {
                let (_, n) = decode_varint32(&self.data[self.cursor..])?;
                self.take(n).ok_or_else(|| MidgeError::Corruption {
                    message: "truncated varint".into(),
                })?
            }
            WireType::Bytes => {
                let (len, n) = decode_varint32(&self.data[self.cursor..])?;
                self.cursor = self
                    .cursor
                    .checked_add(n)
                    .ok_or_else(|| MidgeError::Corruption {
                        message: "cursor overflow".into(),
                    })?;
                self.take(len as usize)
                    .ok_or_else(|| MidgeError::Corruption {
                        message: "truncated bytes".into(),
                    })?
            }
        };

        Ok(Some((tag, slice)))
    }

    #[inline]
    fn read_next(&mut self) -> Option<(u8, &'a [u8])> {
        // Delegate to try_next() and map errors to None to preserve Iterator behavior.
        self.try_next().unwrap_or_default()
    }

    #[inline(always)]
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.cursor.checked_add(n)?;
        if end > self.data.len() {
            return None;
        }
        let s = &self.data[self.cursor..end];
        self.cursor = end;
        Some(s)
    }

    /// Skips a single field; returns false on corruption or if no more fields.
    #[inline]
    pub fn skip_field(&mut self) -> bool {
        if self.cursor >= self.data.len() {
            return false;
        }
        let tag = self.data[self.cursor];
        self.cursor += 1;

        let Some(wire_type) = WireType::from_tag(tag) else {
            return false;
        };

        match wire_type {
            WireType::U8 => {
                self.cursor = match self.cursor.checked_add(1) {
                    Some(v) => v,
                    None => return false,
                };
            }
            WireType::U16 => {
                self.cursor = match self.cursor.checked_add(2) {
                    Some(v) => v,
                    None => return false,
                };
            }
            WireType::U32 => {
                self.cursor = match self.cursor.checked_add(4) {
                    Some(v) => v,
                    None => return false,
                };
            }
            WireType::U64 => {
                self.cursor = match self.cursor.checked_add(8) {
                    Some(v) => v,
                    None => return false,
                };
            }
            WireType::Varint => {
                if let Ok((_, n)) = decode_varint32(&self.data[self.cursor..]) {
                    self.cursor = match self.cursor.checked_add(n) {
                        Some(v) => v,
                        None => return false,
                    };
                } else {
                    return false;
                }
            }
            WireType::Bytes => {
                if let Ok((len, n)) = decode_varint32(&self.data[self.cursor..]) {
                    let after_len = match self.cursor.checked_add(n) {
                        Some(v) => v,
                        None => return false,
                    };
                    self.cursor = match after_len.checked_add(len as usize) {
                        Some(v) => v,
                        None => return false,
                    };
                } else {
                    return false;
                }
            }
        }

        self.cursor <= self.data.len()
    }

    /// Skip forward until finding a field with the specified tag.
    ///
    /// Returns the field's data slice if found, or None if not found or error.
    /// This is more efficient than iterating when you only need one specific field.
    ///
    /// # Example
    /// ```ignore
    /// let mut reader = TlvReader::new(data);
    /// if let Some(value) = reader.skip_to_tag(VALUE_TAG) {
    ///     // Process value without decoding intermediate fields
    /// }
    /// ```
    #[inline]
    pub fn skip_to_tag(&mut self, target_tag: u8) -> Option<&'a [u8]> {
        while self.cursor < self.data.len() {
            let tag = self.data[self.cursor];
            self.cursor += 1;

            let wire_type = WireType::from_tag(tag)?;

            if tag == target_tag {
                // Found it - decode and return the value
                return match wire_type {
                    WireType::U8 => self.take(1),
                    WireType::U16 => self.take(2),
                    WireType::U32 => self.take(4),
                    WireType::U64 => self.take(8),
                    WireType::Varint => {
                        let (_, n) = decode_varint32(&self.data[self.cursor..]).ok()?;
                        self.take(n)
                    }
                    WireType::Bytes => {
                        let (len, n) = decode_varint32(&self.data[self.cursor..]).ok()?;
                        self.cursor = self.cursor.checked_add(n)?;
                        self.take(len as usize)
                    }
                };
            }

            // Not the target - skip this field
            match wire_type {
                WireType::U8 => {
                    self.cursor = self.cursor.checked_add(1)?;
                }
                WireType::U16 => {
                    self.cursor = self.cursor.checked_add(2)?;
                }
                WireType::U32 => {
                    self.cursor = self.cursor.checked_add(4)?;
                }
                WireType::U64 => {
                    self.cursor = self.cursor.checked_add(8)?;
                }
                WireType::Varint => {
                    let (_, n) = decode_varint32(&self.data[self.cursor..]).ok()?;
                    self.cursor = self.cursor.checked_add(n)?;
                }
                WireType::Bytes => {
                    let (len, n) = decode_varint32(&self.data[self.cursor..]).ok()?;
                    let after_len = self.cursor.checked_add(n)?;
                    self.cursor = after_len.checked_add(len as usize)?;
                }
            }
        }

        None
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        self.cursor = 0;
    }

    #[inline(always)]
    pub fn position(&self) -> usize {
        self.cursor
    }
}

impl<'a> Iterator for TlvReader<'a> {
    type Item = (u8, &'a [u8]);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}

#[inline(always)]
pub fn encode_varint32(buf: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

#[inline(always)]
pub fn encode_varint64(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

#[inline(always)]
pub fn decode_varint32(data: &[u8]) -> MidgeResult<(u32, usize)> {
    let mut res = 0u32;
    let mut shift = 0u32;
    let mut i = 0usize;
    // Max 5 bytes for u32
    while i < 5 {
        if i >= data.len() {
            return Err(MidgeError::Corruption {
                message: "varint32 truncated".into(),
            });
        }
        let b = data[i];
        res |= ((b & 0x7F) as u32) << shift;
        i += 1;
        if b & 0x80 == 0 {
            return Ok((res, i));
        }
        shift += 7;
    }
    Err(MidgeError::Corruption {
        message: "varint32 overflow".into(),
    })
}

#[inline(always)]
pub fn decode_varint64(data: &[u8]) -> MidgeResult<(u64, usize)> {
    // Unrolled decode - avoids loop overhead for common small values
    // Most sequence numbers fit in 1-3 bytes
    if data.is_empty() {
        return Err(MidgeError::Corruption {
            message: "varint64 truncated".into(),
        });
    }

    let b0 = data[0];
    if b0 & 0x80 == 0 {
        return Ok((b0 as u64, 1));
    }

    if data.len() < 2 {
        return Err(MidgeError::Corruption {
            message: "varint64 truncated".into(),
        });
    }
    let b1 = data[1];
    if b1 & 0x80 == 0 {
        let val = ((b0 & 0x7F) as u64) | ((b1 as u64) << 7);
        return Ok((val, 2));
    }

    if data.len() < 3 {
        return Err(MidgeError::Corruption {
            message: "varint64 truncated".into(),
        });
    }
    let b2 = data[2];
    if b2 & 0x80 == 0 {
        let val = ((b0 & 0x7F) as u64) | (((b1 & 0x7F) as u64) << 7) | ((b2 as u64) << 14);
        return Ok((val, 3));
    }

    // Fall back to loop for larger values (rare in practice)
    let mut res = ((b0 & 0x7F) as u64) | (((b1 & 0x7F) as u64) << 7) | (((b2 & 0x7F) as u64) << 14);
    let mut shift = 21u32;
    let mut i = 3usize;

    while i < 10 && i < data.len() {
        let b = data[i];
        res |= ((b & 0x7F) as u64) << shift;
        i += 1;
        if b & 0x80 == 0 {
            return Ok((res, i));
        }
        shift += 7;
    }

    if i >= 10 {
        Err(MidgeError::Corruption {
            message: "varint64 overflow".into(),
        })
    } else {
        Err(MidgeError::Corruption {
            message: "varint64 truncated".into(),
        })
    }
}

#[inline(always)]
pub fn parse_u8(v: &[u8]) -> MidgeResult<u8> {
    if v.len() == 1 {
        Ok(v[0])
    } else {
        Err(MidgeError::Corruption {
            message: "expected 1 byte".into(),
        })
    }
}

#[inline(always)]
pub fn parse_u32(v: &[u8]) -> MidgeResult<u32> {
    v.try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| MidgeError::Corruption {
            message: "expected 4 bytes".into(),
        })
}

#[inline(always)]
pub fn parse_u64(v: &[u8]) -> MidgeResult<u64> {
    v.try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| MidgeError::Corruption {
            message: "expected 8 bytes".into(),
        })
}
#[inline(always)]
pub fn parse_varint32_from_slice(v: &[u8]) -> MidgeResult<u32> {
    Ok(decode_varint32(v)?.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_varint32_given_zero() {
        // Arrange
        let mut buf = Vec::new();

        // Act
        encode_varint32(&mut buf, 0);

        // Assert
        assert_eq!(buf, vec![0]);
    }

    #[test]
    fn should_encode_varint32_given_max_one_byte() {
        // Arrange
        let mut buf = Vec::new();

        // Act
        encode_varint32(&mut buf, 127);

        // Assert
        assert_eq!(buf, vec![127]);
    }

    #[test]
    fn should_encode_varint32_given_min_two_bytes() {
        // Arrange
        let mut buf = Vec::new();

        // Act
        encode_varint32(&mut buf, 128);

        // Assert
        assert_eq!(buf, vec![0x80, 0x01]);
    }

    #[test]
    fn should_encode_varint32_given_max_two_bytes() {
        // Arrange
        let mut buf = Vec::new();

        // Act
        encode_varint32(&mut buf, 16383);

        // Assert
        assert_eq!(buf, vec![0xFF, 0x7F]);
    }

    #[test]
    fn should_encode_varint32_given_large_value() {
        // Arrange
        let mut buf = Vec::new();

        // Act
        encode_varint32(&mut buf, 0xFFFF_FFFF);

        // Assert
        assert_eq!(buf, vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F]);
    }

    #[test]
    fn should_decode_varint32_given_various_values() {
        // Arrange
        // Act
        // Assert
        assert_eq!(decode_varint32(&[0]).unwrap(), (0, 1));
        assert_eq!(decode_varint32(&[127]).unwrap(), (127, 1));
        assert_eq!(decode_varint32(&[0x80, 0x01]).unwrap(), (128, 2));
        assert_eq!(decode_varint32(&[0xFF, 0x7F]).unwrap(), (16383, 2));
        assert_eq!(
            decode_varint32(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]).unwrap(),
            (0xFFFF_FFFF, 5)
        );
    }

    #[test]
    fn should_write_u8_given_tag_and_value() {
        // Arrange
        let mut writer = TlvWriter::new();

        // Act
        writer.write_u8(tags::OPERATION, 42);

        // Assert
        let bytes = writer.as_bytes();
        assert_eq!(bytes, &[tags::OPERATION, 42]);
    }

    #[test]
    fn should_write_u32_given_tag_and_value() {
        // Arrange
        let mut writer = TlvWriter::new();

        // Act
        writer.write_u32(tags::CF_ID, 0x1234_5678);

        // Assert
        let bytes = writer.as_bytes();
        assert_eq!(bytes, &[tags::CF_ID, 0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn should_write_u64_given_tag_and_value() {
        // Arrange
        let mut writer = TlvWriter::new();

        // Act
        writer.write_u64(tags::SEQUENCE, 0x01_02_03_04_05_06_07_08);

        // Assert
        let bytes = writer.as_bytes();
        assert_eq!(
            bytes,
            &[
                tags::SEQUENCE,
                0x01,
                0x02,
                0x03,
                0x04,
                0x05,
                0x06,
                0x07,
                0x08
            ]
        );
    }

    #[test]
    fn should_write_bytes_given_tag_and_data() {
        // Arrange
        let mut writer = TlvWriter::new();

        // Act
        writer.write_bytes(tags::KEY, b"hello");

        // Assert
        let bytes = writer.as_bytes();
        assert_eq!(bytes, &[tags::KEY, 5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn should_read_all_fields_given_complete_record() {
        // Arrange
        let mut writer = TlvWriter::new();
        writer.write_u8(tags::OPERATION, 1);
        writer.write_u32(tags::CF_ID, 0);
        writer.write_u64(tags::SEQUENCE, 100);
        writer.write_bytes(tags::KEY, b"key1");
        writer.write_bytes(tags::VALUE, b"value1");
        let encoded = writer.finish();

        // Act
        let mut reader = TlvReader::new(&encoded);

        // Assert
        let (tag, value) = reader.next().unwrap();
        assert_eq!(tag, tags::OPERATION);
        assert_eq!(parse_u8(value).unwrap(), 1);

        let (tag, value) = reader.next().unwrap();
        assert_eq!(tag, tags::CF_ID);
        assert_eq!(parse_u32(value).unwrap(), 0);

        let (tag, value) = reader.next().unwrap();
        assert_eq!(tag, tags::SEQUENCE);
        assert_eq!(parse_u64(value).unwrap(), 100);

        let (tag, value) = reader.next().unwrap();
        assert_eq!(tag, tags::KEY);
        assert_eq!(value, b"key1");

        let (tag, value) = reader.next().unwrap();
        assert_eq!(tag, tags::VALUE);
        assert_eq!(value, b"value1");

        assert!(reader.next().is_none());
    }

    #[test]
    fn should_skip_fields_when_requested() {
        // Arrange
        let mut writer = TlvWriter::new();
        writer.write_u8(tags::OPERATION, 1);
        writer.write_bytes(tags::KEY, b"key1");
        writer.write_bytes(tags::VALUE, b"value1");
        let encoded = writer.finish();

        // Act
        let mut reader = TlvReader::new(&encoded);
        assert!(reader.skip_field()); // Skip op

        // Assert
        let (tag, value) = reader.next().unwrap();
        assert_eq!(tag, tags::KEY);
        assert_eq!(value, b"key1");

        assert!(reader.skip_field()); // Skip value
        assert!(!reader.skip_field()); // End of data
    }

    #[test]
    fn should_handle_optional_fields_given_missing_value() {
        // Arrange
        let mut writer = TlvWriter::new();
        writer.write_u8(tags::OPERATION, 0);
        writer.write_bytes(tags::KEY, b"key1");
        // No value field (Delete operation)
        writer.write_u64(tags::EXPIRATION, 1_234_567_890_000);
        let encoded = writer.finish();

        // Act
        let reader = TlvReader::new(&encoded);
        let mut op = None;
        let mut key = None;
        let mut value = None;
        let mut expiration = None;

        for (tag, val) in reader {
            match tag {
                tags::OPERATION => op = Some(parse_u8(val).unwrap()),
                tags::KEY => key = Some(val),
                tags::VALUE => value = Some(val),
                tags::EXPIRATION => expiration = Some(parse_u64(val).unwrap()),
                _ => {} // Skip unknown tags
            }
        }

        // Assert
        assert_eq!(op, Some(0));
        assert_eq!(key, Some(&b"key1"[..]));
        assert_eq!(value, None); // Not present
        assert_eq!(expiration, Some(1_234_567_890_000));
    }

    #[test]
    fn should_extract_wire_type_from_tag() {
        // Arrange
        // Act
        // Assert
        assert_eq!(WireType::from_tag(0x01), Some(WireType::U8));
        assert_eq!(WireType::from_tag(0x22), Some(WireType::U32));
        assert_eq!(WireType::from_tag(0x33), Some(WireType::U64));
        assert_eq!(WireType::from_tag(0x54), Some(WireType::Bytes));
    }

    #[test]
    fn should_provide_zero_copy_access_to_bytes() {
        // Arrange
        let data = b"this is test data";
        let mut writer = TlvWriter::new();
        writer.write_bytes(tags::VALUE, data);
        let encoded = writer.finish();

        // Act
        let mut reader = TlvReader::new(&encoded);
        let (tag, value) = reader.next().unwrap();

        // Assert
        assert_eq!(tag, tags::VALUE);
        let (_, n) = decode_varint32(&encoded[1..]).unwrap();
        let encoded_payload_ptr = unsafe { encoded.as_ptr().add(1 + n) };
        assert_eq!(value.as_ptr(), encoded_payload_ptr);
        assert_eq!(value, data);
    }
}
