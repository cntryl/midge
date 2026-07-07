//! SST entry encoding/decoding
//!
//! Packed, zero-copy SST *data block* entry format.
//!
//! This is intentionally NOT TLV.
//! TLV is reserved for block-level metadata.
//!
//! Entry layout (little-endian):
//!
//! [`shared_prefix_len`: u16]
//! [`key_delta_len`:   u16]
//! [`value_len`:       u32]
//! [sequence:        u64]
//! [`entry_type`:      u8]
//! [`key_delta` bytes]
//! [value bytes?]
//!
//! Entry length is fully deterministic from the header.
//! Decode is zero-copy and allocation-free.
//!
//! Version 2 extends the header with:
//! [`expiration_millis`: u64] // `u64::MAX` => no expiration
//!
//! Version 2 also supports an extended key-delta length form. When
//! `key_delta_len == u16::MAX` and `value_len == u32::MAX`, the expiration field is
//! followed by `[extended_key_delta_len: u32][extended_value_len: u32]` before the key bytes.

use crate::common::{MidgeError, MidgeResult};
use bytes::{BufMut, BytesMut};
use std::convert::TryFrom;

/// Restart point interval for block building
///
/// This constant is used by block builders/readers to decide restart sampling
/// of prefix-compressed entries within a block.
pub const RESTART_INTERVAL: usize = 16;

const EXTENDED_KEY_DELTA_LEN_MARKER: u16 = u16::MAX;
const EXTENDED_VALUE_LEN_MARKER: u32 = u32::MAX;
const V2_BASE_HEADER_LEN: usize = 25;
const V2_EXTENDED_LENGTH_LEN: usize = 8;

/// Maximum key-delta length that can be stored directly in the legacy inline field.
pub const MAX_INLINE_ENTRY_KEY_DELTA_LEN: usize = 65_535;

/// Maximum key-delta length representable by the SST v2 extended entry format.
pub const MAX_ENTRY_KEY_DELTA_LEN: usize = u32::MAX as usize;

/// Validate that an SST entry key delta can be represented by the v2 on-disk format.
///
/// # Errors
///
/// Returns `InvalidArgument` when the key delta exceeds the extended `u32` length field used by
/// SST data-block entries. Ordinary writers should not need this helper; the v2 codec handles
/// key deltas larger than the legacy inline field.
pub fn validate_entry_key_delta_len(key_delta: &[u8]) -> MidgeResult<()> {
    if key_delta.len() > MAX_ENTRY_KEY_DELTA_LEN {
        return Err(MidgeError::InvalidArgument(format!(
            "SST entry key delta length {} exceeds format limit {}",
            key_delta.len(),
            MAX_ENTRY_KEY_DELTA_LEN
        )));
    }
    Ok(())
}

/// Entry type for SST entries
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryType {
    Put = 0,
    Insert = 1,
    Delete = 2,
    Merge = 3,
}

impl std::fmt::Display for EntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

impl TryFrom<u8> for EntryType {
    type Error = MidgeError;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(EntryType::Put),
            1 => Ok(EntryType::Insert),
            2 => Ok(EntryType::Delete),
            3 => Ok(EntryType::Merge),
            _ => Err(MidgeError::Corruption(format!("Invalid entry_type: {v}"))),
        }
    }
}

/// Encode a single SST entry into `buf`.
///
/// This appends bytes to the provided buffer (block builder style).
#[inline]
#[must_use]
pub fn encode(
    key_delta: &[u8],
    shared_len: u16,
    value: Option<&[u8]>,
    seq: u64,
    entry_type: EntryType,
) -> Vec<u8> {
    // Safe capacity calculation: avoid panics on overflow by falling back to a
    // small header size when sums overflow on 32-bit platforms.
    let header = 2usize + 2 + 4 + 8 + 1;
    let key_len = key_delta.len();
    let val_len = value.map_or(0, <[u8]>::len);
    let cap = header
        .checked_add(key_len)
        .and_then(|s| s.checked_add(val_len))
        .unwrap_or(header);

    let mut buf = BytesMut::with_capacity(cap);

    encode_into(&mut buf, key_delta, shared_len, value, seq, entry_type);
    buf.to_vec()
}

#[inline]
fn encode_into(
    buf: &mut BytesMut,
    key_delta: &[u8],
    shared_len: u16,
    value: Option<&[u8]>,
    seq: u64,
    entry_type: EntryType,
) {
    let shared = shared_len;
    let key_len = u16::try_from(key_delta.len()).unwrap_or(u16::MAX);
    let val = value.unwrap_or(&[]);
    let val_len = u32::try_from(val.len()).unwrap_or(u32::MAX);

    buf.put_u16_le(shared);
    buf.put_u16_le(key_len);
    buf.put_u32_le(val_len);
    buf.put_u64_le(seq);
    buf.put_u8(entry_type as u8);
    buf.extend_from_slice(key_delta);
    buf.extend_from_slice(val);
}

/// Zero-copy decoded SST entry view
#[derive(Debug, Clone, Copy)]
pub struct EntryView<'a> {
    pub shared_len: u16,
    /// Borrowed slice for key delta
    pub key_delta: &'a [u8],
    /// Absolute offset of `key_delta` in the original buffer
    pub key_offset: usize,
    /// Borrowed slice for value (if present)
    pub value: Option<&'a [u8]>,
    /// Absolute offset of value in the original buffer (if present)
    pub value_offset: Option<usize>,
    pub sequence: u64,
    pub entry_type: EntryType,
    pub expiration: Option<u64>,
    pub bytes_consumed: usize,
}

/// Decode a single entry starting at `offset`.
///
/// This is allocation-free and returns a borrowed view.
///
/// # Errors
///
/// Returns an error if the entry is truncated or malformed.
pub fn decode(data: &[u8], offset: usize) -> MidgeResult<(EntryView<'_>, usize)> {
    decode_with_format(data, offset, crate::sst::types::SST_FORMAT_V1)
}

/// Decode a single entry using the SST format version.
///
/// # Errors
///
/// Returns an error if the entry is truncated, malformed, or the format version is unsupported.
pub fn decode_with_format(
    data: &[u8],
    offset: usize,
    format_version: u32,
) -> MidgeResult<(EntryView<'_>, usize)> {
    match format_version {
        crate::sst::types::SST_FORMAT_V1 => decode_v1(data, offset),
        crate::sst::types::SST_FORMAT_V2 | crate::sst::types::SST_FORMAT_V3 => {
            decode_v2(data, offset)
        }
        other => Err(MidgeError::Corruption(format!(
            "Unsupported SST entry format version: {other}"
        ))),
    }
}

fn decode_v1(data: &[u8], offset: usize) -> MidgeResult<(EntryView<'_>, usize)> {
    if offset >= data.len() {
        return Err(MidgeError::Corruption("Offset beyond data length".into()));
    }

    let mut p = offset;

    if data.len() < p + 17 {
        return Err(MidgeError::Corruption("Truncated SST entry header".into()));
    }

    let shared = u16::from_le_bytes([data[p], data[p + 1]]);
    let key_len = u16::from_le_bytes([data[p + 2], data[p + 3]]) as usize;
    let val_len = u32::from_le_bytes([data[p + 4], data[p + 5], data[p + 6], data[p + 7]]) as usize;
    let seq = u64::from_le_bytes([
        data[p + 8],
        data[p + 9],
        data[p + 10],
        data[p + 11],
        data[p + 12],
        data[p + 13],
        data[p + 14],
        data[p + 15],
    ]);
    let raw_entry_type = data[p + 16];
    let entry_type = EntryType::try_from(raw_entry_type)?;

    p += 17;

    // Validate we have enough bytes for key and value with helpful messages
    let key_end = checked_entry_end(p, key_len, "key")?;
    if data.len() < key_end {
        return Err(MidgeError::Corruption(format!(
            "Not enough data for key: need {}, have {}",
            key_len,
            data.len().saturating_sub(p)
        )));
    }

    let value_end = checked_entry_end(key_end, val_len, "value")?;
    if data.len() < value_end {
        return Err(MidgeError::Corruption(format!(
            "Not enough data for value: need {}, have {}",
            val_len,
            data.len().saturating_sub(key_end)
        )));
    }

    let key_offset = p;
    let key = &data[p..key_end];
    p = key_end;

    let (value_offset, value) = decode_value(data, p, value_end, val_len, entry_type);
    p = value_end;

    let consumed = p - offset;

    Ok((
        EntryView {
            shared_len: shared,
            key_delta: key,
            key_offset,
            value,
            value_offset,
            sequence: seq,
            entry_type,
            expiration: None,
            bytes_consumed: consumed,
        },
        p,
    ))
}

fn decode_v2(data: &[u8], offset: usize) -> MidgeResult<(EntryView<'_>, usize)> {
    if offset >= data.len() {
        return Err(MidgeError::Corruption("Offset beyond data length".into()));
    }

    let mut p = offset;

    if data.len() < p + V2_BASE_HEADER_LEN {
        return Err(MidgeError::Corruption("Truncated SST entry header".into()));
    }

    let shared = u16::from_le_bytes([data[p], data[p + 1]]);
    let raw_key_len = u16::from_le_bytes([data[p + 2], data[p + 3]]);
    let raw_val_len = u32::from_le_bytes([data[p + 4], data[p + 5], data[p + 6], data[p + 7]]);
    let seq = u64::from_le_bytes([
        data[p + 8],
        data[p + 9],
        data[p + 10],
        data[p + 11],
        data[p + 12],
        data[p + 13],
        data[p + 14],
        data[p + 15],
    ]);
    let raw_entry_type = data[p + 16];
    let entry_type = EntryType::try_from(raw_entry_type)?;
    let expiration_raw = u64::from_le_bytes([
        data[p + 17],
        data[p + 18],
        data[p + 19],
        data[p + 20],
        data[p + 21],
        data[p + 22],
        data[p + 23],
        data[p + 24],
    ]);

    p += V2_BASE_HEADER_LEN;

    let (key_len, val_len) = if raw_key_len == EXTENDED_KEY_DELTA_LEN_MARKER
        && raw_val_len == EXTENDED_VALUE_LEN_MARKER
    {
        if data.len() < p + V2_EXTENDED_LENGTH_LEN {
            return Err(MidgeError::Corruption(
                "Truncated SST extended entry header".into(),
            ));
        }
        let extended_key_len = u32::from_le_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
        let extended_val_len =
            u32::from_le_bytes([data[p + 4], data[p + 5], data[p + 6], data[p + 7]]);
        p += V2_EXTENDED_LENGTH_LEN;
        (
            usize::try_from(extended_key_len).unwrap_or(usize::MAX),
            usize::try_from(extended_val_len).unwrap_or(usize::MAX),
        )
    } else {
        (
            usize::from(raw_key_len),
            usize::try_from(raw_val_len).unwrap_or(usize::MAX),
        )
    };

    let key_end = checked_entry_end(p, key_len, "key")?;
    if data.len() < key_end {
        return Err(MidgeError::Corruption(format!(
            "Not enough data for key: need {}, have {}",
            key_len,
            data.len().saturating_sub(p)
        )));
    }

    let value_end = checked_entry_end(key_end, val_len, "value")?;
    if data.len() < value_end {
        return Err(MidgeError::Corruption(format!(
            "Not enough data for value: need {}, have {}",
            val_len,
            data.len().saturating_sub(key_end)
        )));
    }

    let key_offset = p;
    let key = &data[p..key_end];
    p = key_end;

    let (value_offset, value) = decode_value(data, p, value_end, val_len, entry_type);
    p = value_end;

    let consumed = p - offset;
    let expiration = if expiration_raw == u64::MAX {
        None
    } else {
        Some(expiration_raw)
    };

    Ok((
        EntryView {
            shared_len: shared,
            key_delta: key,
            key_offset,
            value,
            value_offset,
            sequence: seq,
            entry_type,
            expiration,
            bytes_consumed: consumed,
        },
        p,
    ))
}

fn checked_entry_end(start: usize, len: usize, label: &str) -> MidgeResult<usize> {
    start
        .checked_add(len)
        .ok_or_else(|| MidgeError::Corruption(format!("SST entry {label} length overflows block")))
}

fn decode_value(
    data: &[u8],
    value_start: usize,
    value_end: usize,
    value_len: usize,
    entry_type: EntryType,
) -> (Option<usize>, Option<&[u8]>) {
    if value_len > 0 || !matches!(entry_type, EntryType::Delete) {
        (Some(value_start), Some(&data[value_start..value_end]))
    } else {
        (None, None)
    }
}

/// Encode a v2 SST entry with persisted expiration metadata.
#[inline]
#[must_use]
pub fn encode_v2(
    key_delta: &[u8],
    shared_len: u16,
    value: Option<&[u8]>,
    seq: u64,
    entry_type: EntryType,
    expiration: Option<u64>,
) -> Vec<u8> {
    let val_len = value.map_or(0, <[u8]>::len);
    let encoded_val_len = u32::try_from(val_len).unwrap_or(u32::MAX);
    let use_extended_lengths = key_delta.len() > MAX_INLINE_ENTRY_KEY_DELTA_LEN
        || (key_delta.len() == MAX_INLINE_ENTRY_KEY_DELTA_LEN
            && encoded_val_len == EXTENDED_VALUE_LEN_MARKER);
    let header = if use_extended_lengths {
        V2_BASE_HEADER_LEN + V2_EXTENDED_LENGTH_LEN
    } else {
        V2_BASE_HEADER_LEN
    };
    let key_len = key_delta.len();
    let cap = header
        .checked_add(key_len)
        .and_then(|s| s.checked_add(val_len))
        .unwrap_or(header);

    let mut buf = BytesMut::with_capacity(cap);
    let val = value.unwrap_or(&[]);

    buf.put_u16_le(shared_len);
    if use_extended_lengths {
        buf.put_u16_le(EXTENDED_KEY_DELTA_LEN_MARKER);
        buf.put_u32_le(EXTENDED_VALUE_LEN_MARKER);
    } else {
        buf.put_u16_le(u16::try_from(key_delta.len()).unwrap_or(EXTENDED_KEY_DELTA_LEN_MARKER));
        buf.put_u32_le(encoded_val_len);
    }
    buf.put_u64_le(seq);
    buf.put_u8(entry_type as u8);
    buf.put_u64_le(expiration.unwrap_or(u64::MAX));
    if use_extended_lengths {
        buf.put_u32_le(u32::try_from(key_delta.len()).unwrap_or(u32::MAX));
        buf.put_u32_le(encoded_val_len);
    }
    buf.extend_from_slice(key_delta);
    buf.extend_from_slice(val);
    buf.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_encode_decode_key_delta() {
        let encoded = encode(b"mykey", 0u16, None, 0, EntryType::Put);
        let (entry, _) = decode(&encoded, 0).unwrap();
        assert_eq!(entry.key_delta, b"mykey");
    }

    #[test]
    fn should_roundtrip_encode_decode_with_value() {
        let encoded = encode(b"key", 0u16, Some(b"myvalue"), 0, EntryType::Put);
        let (entry, _) = decode(&encoded, 0).unwrap();
        assert_eq!(entry.value, Some(b"myvalue".as_slice()));
    }

    #[test]
    fn should_roundtrip_encode_decode_sequence() {
        let encoded = encode(b"key", 0u16, None, 12345, EntryType::Put);
        let (entry, _) = decode(&encoded, 0).unwrap();
        assert_eq!(entry.sequence, 12345);
    }

    #[test]
    fn should_roundtrip_encode_decode_entry_type_delete() {
        let encoded = encode(b"key", 0u16, None, 0, EntryType::Delete);
        let (entry, _) = decode(&encoded, 0).unwrap();
        assert_eq!(entry.entry_type, EntryType::Delete);
    }

    #[test]
    fn should_roundtrip_encode_decode_shared_prefix() {
        let encoded = encode(b"suffix", 42u16, Some(b"val"), 0, EntryType::Put);
        let (entry, _) = decode(&encoded, 0).unwrap();
        assert_eq!(entry.shared_len, 42u16);
    }

    #[test]
    fn should_return_bytes_consumed() {
        // Arrange
        let encoded = encode(b"key", 0u16, Some(b"val"), 42, EntryType::Put);

        // Act
        let (entry, consumed) = decode(&encoded, 0).unwrap();

        // Assert
        assert_eq!(entry.bytes_consumed, encoded.len());
        assert_eq!(consumed, encoded.len());
    }
}
