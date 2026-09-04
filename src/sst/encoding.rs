//! SST entry encoding/decoding
//!
//! Packed, zero-copy SST *data block* entry format.
//!
//! This is intentionally NOT TLV.
//! TLV is reserved for block-level metadata.
//!
//! V4 entry layout (little-endian):
//!
//! [`shared_prefix_len`: u16]
//! [`key_delta_len`:   u16]
//! [`value_len`:       u32]
//! [sequence:        u64]
//! [`entry_type`:      u8]
//! [`expiration_present`: u8] // exactly 0 or 1
//! [`expiration_millis`: u64]
//! [`key_delta` bytes]
//! [value bytes?]
//!
//! Entry length is fully deterministic from the header.
//! Decode is zero-copy and allocation-free.
//!
//! V4 supports an extended key-delta length form. When
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
const V4_BASE_HEADER_LEN: usize = 26;
const V4_EXTENDED_LENGTH_LEN: usize = 8;

/// Maximum key-delta length that can be stored directly in the legacy inline field.
pub const MAX_INLINE_ENTRY_KEY_DELTA_LEN: usize = 65_535;

/// Maximum key-delta length representable by the SST V4 extended entry format.
pub const MAX_ENTRY_KEY_DELTA_LEN: usize = u32::MAX as usize;

/// Validate that an SST entry key delta can be represented by the V4 on-disk format.
///
/// # Errors
///
/// Returns `InvalidArgument` when the key delta exceeds the extended `u32` length field used by
/// SST data-block entries. Ordinary writers should not need this helper; the V4 codec handles
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

/// Validate the worst-case entry size at a block restart, before admission.
/// Prefix compression cannot be relied on: flush and compaction may put any
/// entry first in a block. TTL bytes are already included in the V4 header.
pub(crate) fn validate_entry_size(key_len: usize, value_len: usize) -> MidgeResult<()> {
    let _ = checked_v4_lengths(key_len, value_len)?;
    let header = V4_BASE_HEADER_LEN
        + if key_len > MAX_INLINE_ENTRY_KEY_DELTA_LEN {
            V4_EXTENDED_LENGTH_LEN
        } else {
            0
        };
    let size = header
        .checked_add(key_len)
        .and_then(|size| size.checked_add(value_len));
    if size.is_none_or(|size| size > crate::sst::compression::MAX_DECOMPRESSED_BLOCK_SIZE) {
        return Err(MidgeError::ResourceLimit(format!(
            "SST entry with {key_len} key bytes and {value_len} value bytes exceeds the 64 MiB decoded block limit"
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
    encode_v4(key_delta, shared_len, value, seq, entry_type, None)
        .expect("legacy-sized SST entry must fit the V4 length fields")
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
    decode_with_format(data, offset, crate::sst::types::SST_FORMAT_V4)
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
    if format_version != crate::sst::types::SST_FORMAT_V4 {
        return Err(MidgeError::CompatibilityError(format!(
            "unsupported SST entry format version {format_version}; this build requires V{}",
            crate::sst::types::SST_FORMAT_V4
        )));
    }
    decode_v4(data, offset)
}

fn decode_v4(data: &[u8], offset: usize) -> MidgeResult<(EntryView<'_>, usize)> {
    if offset >= data.len() {
        return Err(MidgeError::Corruption("Offset beyond data length".into()));
    }

    let mut p = offset;

    if data.len() < p + V4_BASE_HEADER_LEN {
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
    let expiration = decode_v4_expiration(data, p)?;

    p += V4_BASE_HEADER_LEN;

    let (key_len, val_len) = if raw_key_len == EXTENDED_KEY_DELTA_LEN_MARKER
        && raw_val_len == EXTENDED_VALUE_LEN_MARKER
    {
        if data.len() < p + V4_EXTENDED_LENGTH_LEN {
            return Err(MidgeError::Corruption(
                "Truncated SST extended entry header".into(),
            ));
        }
        let extended_key_len = u32::from_le_bytes([data[p], data[p + 1], data[p + 2], data[p + 3]]);
        let extended_val_len =
            u32::from_le_bytes([data[p + 4], data[p + 5], data[p + 6], data[p + 7]]);
        p += V4_EXTENDED_LENGTH_LEN;
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

fn decode_v4_expiration(data: &[u8], header_start: usize) -> MidgeResult<Option<u64>> {
    let expiration_present = data[header_start + 17];
    if expiration_present > 1 {
        return Err(MidgeError::Corruption(format!(
            "invalid SST expiration-presence byte {expiration_present}"
        )));
    }
    let expiration_start = header_start + 18;
    let expiration_raw = u64::from_le_bytes(
        data[expiration_start..expiration_start + 8]
            .try_into()
            .expect("V4 header length checked before TTL decode"),
    );
    if expiration_present == 0 && expiration_raw != 0 {
        return Err(MidgeError::Corruption(
            "SST entry without expiration has nonzero expiration bytes".into(),
        ));
    }
    Ok((expiration_present == 1).then_some(expiration_raw))
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

/// Encode a V4 SST entry with explicit persisted expiration presence.
#[inline]
pub fn encode_v4(
    key_delta: &[u8],
    shared_len: u16,
    value: Option<&[u8]>,
    seq: u64,
    entry_type: EntryType,
    expiration: Option<u64>,
) -> MidgeResult<Vec<u8>> {
    let val_len = value.map_or(0, <[u8]>::len);
    let (encoded_key_len, encoded_val_len) = checked_v4_lengths(key_delta.len(), val_len)?;
    let use_extended_lengths = key_delta.len() > MAX_INLINE_ENTRY_KEY_DELTA_LEN
        || (key_delta.len() == MAX_INLINE_ENTRY_KEY_DELTA_LEN
            && encoded_val_len == EXTENDED_VALUE_LEN_MARKER);
    let header = if use_extended_lengths {
        V4_BASE_HEADER_LEN + V4_EXTENDED_LENGTH_LEN
    } else {
        V4_BASE_HEADER_LEN
    };
    let key_len = key_delta.len();
    let cap = header
        .checked_add(key_len)
        .and_then(|s| s.checked_add(val_len))
        .ok_or_else(|| {
            MidgeError::ResourceLimit("encoded SST entry length exceeds address space".to_string())
        })?;

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
    buf.put_u8(u8::from(expiration.is_some()));
    buf.put_u64_le(expiration.unwrap_or(0));
    if use_extended_lengths {
        buf.put_u32_le(encoded_key_len);
        buf.put_u32_le(encoded_val_len);
    }
    buf.extend_from_slice(key_delta);
    buf.extend_from_slice(val);
    Ok(buf.to_vec())
}

fn checked_v4_lengths(key_len: usize, value_len: usize) -> MidgeResult<(u32, u32)> {
    let encoded_key_len = u32::try_from(key_len).map_err(|_| {
        MidgeError::ResourceLimit("SST key delta exceeds the 4 GiB format limit".to_string())
    })?;
    let encoded_value_len = u32::try_from(value_len).map_err(|_| {
        MidgeError::ResourceLimit("SST value exceeds the 4 GiB format limit".to_string())
    })?;
    Ok((encoded_key_len, encoded_value_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_admit_only_entries_within_decoded_limit_including_extended_headers() {
        // Arrange
        let limit = crate::sst::compression::MAX_DECOMPRESSED_BLOCK_SIZE;
        for key_len in [0, 3, 65_535, 65_536] {
            let header = if key_len > 65_535 { 34 } else { 26 };
            let value_len = limit - header - key_len;
            // Act
            let accepted = validate_entry_size(key_len, value_len);
            let rejected = validate_entry_size(key_len, value_len + 1);
            // Assert
            assert!(accepted.is_ok());
            assert!(matches!(rejected, Err(MidgeError::ResourceLimit(_))));
        }
        assert!(validate_entry_size(usize::MAX, 1).is_err());
        assert!(validate_entry_size(1, usize::MAX).is_err());
    }

    #[test]
    fn should_round_trip_maximum_expiration_value_given_explicit_sst_ttl_presence() {
        // Arrange
        let encoded = encode_v4(
            b"ttl-key",
            0,
            Some(b"ttl-value"),
            7,
            EntryType::Put,
            Some(u64::MAX),
        )
        .expect("encode SST entry");

        // Act
        let (decoded, consumed) = decode_with_format(&encoded, 0, crate::sst::types::SST_FORMAT_V4)
            .expect("decode SST entry");

        // Assert
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.expiration, Some(u64::MAX));
    }

    #[test]
    fn should_reject_unknown_expiration_presence_given_v4_entry() {
        // Arrange
        let mut encoded = encode_v4(b"ttl-key", 0, Some(b"value"), 7, EntryType::Put, None)
            .expect("encode SST entry");
        encoded[17] = 2;

        // Act
        let result = decode(&encoded, 0);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

    #[test]
    fn should_reject_noncanonical_expiration_bytes_given_absent_v4_ttl() {
        // Arrange
        let mut encoded = encode_v4(b"ttl-key", 0, Some(b"value"), 7, EntryType::Put, None)
            .expect("encode SST entry");
        encoded[18] = 1;

        // Act
        let result = decode(&encoded, 0);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

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

    #[test]
    fn should_reject_unrepresentable_v4_lengths_before_encoding() {
        // Arrange
        let too_large = usize::try_from(u64::from(u32::MAX) + 1).unwrap_or(usize::MAX);

        // Act
        let key_result = checked_v4_lengths(too_large, 0);
        let value_result = checked_v4_lengths(0, too_large);

        // Assert
        assert!(key_result.is_err());
        assert!(value_result.is_err());
    }
}
