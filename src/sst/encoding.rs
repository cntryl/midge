//! SST entry encoding/decoding
//!
//! Packed, zero-copy SST *data block* entry format.
//!
//! This is intentionally NOT TLV.
//! TLV is reserved for block-level metadata.
//!
//! Entry layout (little-endian):
//!
//! [shared_prefix_len: u16]
//! [key_delta_len:   u16]
//! [value_len:       u32]   // 0 => tombstone / no value
//! [sequence:        u64]
//! [entry_type:      u8]
//! [key_delta bytes]
//! [value bytes?]
//!
//! Entry length is fully deterministic from the header.
//! Decode is zero-copy and allocation-free.

use crate::common::{MidgeError, MidgeResult};
use bytes::{BufMut, BytesMut};

/// Restart point interval for block building
///
/// This constant is used by block builders/readers to decide restart sampling
/// of prefix-compressed entries within a block.
pub const RESTART_INTERVAL: usize = 16;

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
            _ => Err(MidgeError::Corruption(format!("Invalid entry_type: {}", v))),
        }
    }
}

/// Encode a single SST entry into `buf`.
///
/// This appends bytes to the provided buffer (block builder style).
#[inline]
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
    let val_len = value.map_or(0, |v| v.len());
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
    let key_len = key_delta.len() as u16;
    let val = value.unwrap_or(&[]);
    let val_len = val.len() as u32;

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
    /// Absolute offset of key_delta in the original buffer
    pub key_offset: usize,
    /// Borrowed slice for value (if present)
    pub value: Option<&'a [u8]>,
    /// Absolute offset of value in the original buffer (if present)
    pub value_offset: Option<usize>,
    pub sequence: u64,
    pub entry_type: EntryType,
    pub bytes_consumed: usize,
}

/// Decode a single entry starting at `offset`.
///
/// This is allocation-free and returns a borrowed view.
pub fn decode<'a>(data: &'a [u8], offset: usize) -> MidgeResult<(EntryView<'a>, usize)> {
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

    p += 17;

    // Validate we have enough bytes for key and value with helpful messages
    if data.len() < p + key_len {
        return Err(MidgeError::Corruption(format!(
            "Not enough data for key: need {}, have {}",
            key_len,
            data.len().saturating_sub(p)
        )));
    }

    if data.len() < p + key_len + val_len {
        return Err(MidgeError::Corruption(format!(
            "Not enough data for value: need {}, have {}",
            val_len,
            data.len().saturating_sub(p + key_len)
        )));
    }

    let key_offset = p;
    let key = &data[p..p + key_len];
    p += key_len;

    let (value_offset, value) = if val_len > 0 {
        let v_off = p;
        let v = &data[p..p + val_len];
        p += val_len;
        (Some(v_off), Some(v))
    } else {
        (None, None)
    };

    let consumed = p - offset;

    let entry_type = EntryType::try_from(raw_entry_type)?;

    Ok((
        EntryView {
            shared_len: shared,
            key_delta: key,
            key_offset,
            value,
            value_offset,
            sequence: seq,
            entry_type,
            bytes_consumed: consumed,
        },
        p,
    ))
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
