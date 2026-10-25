//! V5 SST entries: canonical unsigned varint lengths, fixed sequence and one
//! flags byte. Expiration occupies eight bytes only when present. The data
//! block's restart trailer is defined in `data_block`.

use crate::common::{MidgeError, MidgeResult};
use crate::types::EntryType;

/// Prefixes remain bounded by the writer's existing u16 key-prefix contract.
pub(crate) const MAX_SHARED_PREFIX_LEN: usize = 65_535;
const EXPIRATION_PRESENT: u8 = 0x80;
const ENTRY_TYPE_MASK: u8 = 0x03;

pub(crate) fn validate_entry_size(key_len: usize, value_len: usize) -> MidgeResult<()> {
    let (key, value) = checked_lengths(key_len, value_len)?;
    // A complete single-entry block includes worst-case TTL and its restart
    // word/count. Admission cannot depend on a helpful compression prefix.
    let header = 1 + varint_len(key) + varint_len(value) + 8 + 1 + 8 + 8;
    if header
        .checked_add(key_len)
        .and_then(|n| n.checked_add(value_len))
        .is_none_or(|size| size > crate::codec::MAX_DECOMPRESSED_BLOCK_SIZE)
    {
        return Err(MidgeError::ResourceLimit(format!(
            "SST entry with {key_len} key bytes and {value_len} value bytes exceeds the 64 MiB decoded block limit"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct EntryView<'a> {
    pub shared_len: u16,
    pub key_delta: &'a [u8],
    pub value: Option<&'a [u8]>,
    pub value_offset: Option<usize>,
    pub sequence: u64,
    pub entry_type: EntryType,
    pub expiration: Option<u64>,
}

#[cfg(any(test, feature = "internal-testing"))]
#[must_use]
/// Encode one non-expiring entry for internal fixtures.
///
/// # Panics
/// Panics if either byte slice exceeds the V5 u32 extent.
pub fn encode(
    key_delta: &[u8],
    shared_len: u16,
    value: Option<&[u8]>,
    seq: u64,
    entry_type: EntryType,
) -> Vec<u8> {
    encode_v5(key_delta, shared_len, value, seq, entry_type, None)
        .expect("SST entry fits V5 fields")
}

/// Encode one entry; restart entries use shared length zero.
///
/// # Errors
/// Returns a resource limit error for an unrepresentable length or extent.
pub fn encode_v5(
    key_delta: &[u8],
    shared_len: u16,
    value: Option<&[u8]>,
    sequence: u64,
    entry_type: EntryType,
    expiration: Option<u64>,
) -> MidgeResult<Vec<u8>> {
    let (key_len, value_len) = checked_lengths(key_delta.len(), value.map_or(0, <[u8]>::len))?;
    let capacity = varint_len(u32::from(shared_len))
        + varint_len(key_len)
        + varint_len(value_len)
        + 8
        + 1
        + expiration.map_or(0, |_| 8);
    let capacity = capacity
        .checked_add(key_delta.len())
        .and_then(|n| n.checked_add(value_len as usize))
        .ok_or_else(|| {
            MidgeError::ResourceLimit("encoded SST entry length exceeds address space".into())
        })?;
    let mut encoded = Vec::with_capacity(capacity);
    put_varint(u32::from(shared_len), &mut encoded);
    put_varint(key_len, &mut encoded);
    put_varint(value_len, &mut encoded);
    encoded.extend_from_slice(&sequence.to_le_bytes());
    encoded.push(
        entry_type as u8
            | if expiration.is_some() {
                EXPIRATION_PRESENT
            } else {
                0
            },
    );
    if let Some(expiration) = expiration {
        encoded.extend_from_slice(&expiration.to_le_bytes());
    }
    encoded.extend_from_slice(key_delta);
    encoded.extend_from_slice(value.unwrap_or_default());
    Ok(encoded)
}

#[cfg(any(test, feature = "internal-testing"))]
/// Decode one current-format entry.
///
/// # Errors
/// Returns an error for an invalid extent, varint, type or flags.
pub fn decode(data: &[u8], offset: usize) -> MidgeResult<(EntryView<'_>, usize)> {
    decode_with_format(data, offset, crate::sst::types::SST_FORMAT_V5)
}

/// Decode one entry without allocation. V4 is an intentional pre-1.0 break.
///
/// # Errors
/// Returns compatibility errors for unsupported formats/types, and corruption
/// for malformed lengths, flags or truncated extents.
pub fn decode_with_format(
    data: &[u8],
    offset: usize,
    format_version: u32,
) -> MidgeResult<(EntryView<'_>, usize)> {
    if format_version != crate::sst::types::SST_FORMAT_V5 {
        return Err(MidgeError::CompatibilityError(format!(
            "unsupported SST entry format version {format_version}; this build requires V5"
        )));
    }
    let mut cursor = offset;
    let shared_len = u16::try_from(take_varint(data, &mut cursor)?)
        .map_err(|_| MidgeError::Corruption("SST shared prefix exceeds u16".into()))?;
    let key_len = take_varint(data, &mut cursor)? as usize;
    let value_len = take_varint(data, &mut cursor)? as usize;
    let sequence = u64::from_le_bytes(
        take(data, &mut cursor, 8)?
            .try_into()
            .expect("exact sequence"),
    );
    let flags = take(data, &mut cursor, 1)?[0];
    if flags & !(EXPIRATION_PRESENT | ENTRY_TYPE_MASK) != 0 {
        return Err(MidgeError::Corruption("invalid SST entry flags".into()));
    }
    let entry_type = EntryType::try_from(flags & ENTRY_TYPE_MASK)?;
    let expiration = if flags & EXPIRATION_PRESENT != 0 {
        Some(u64::from_le_bytes(
            take(data, &mut cursor, 8)?
                .try_into()
                .expect("exact expiration"),
        ))
    } else {
        None
    };
    let key_delta = take(data, &mut cursor, key_len)?;
    let value_offset = cursor;
    let bytes = take(data, &mut cursor, value_len)?;
    if entry_type == EntryType::Delete && !bytes.is_empty() {
        return Err(MidgeError::Corruption(
            "SST tombstone carries value bytes".into(),
        ));
    }
    let present = entry_type != EntryType::Delete;
    Ok((
        EntryView {
            shared_len,
            key_delta,
            value: present.then_some(bytes),
            value_offset: present.then_some(value_offset),
            sequence,
            entry_type,
            expiration,
        },
        cursor,
    ))
}

fn checked_lengths(key: usize, value: usize) -> MidgeResult<(u32, u32)> {
    let key = u32::try_from(key).map_err(|_| {
        MidgeError::ResourceLimit("SST key delta exceeds 4 GiB format limit".into())
    })?;
    let value = u32::try_from(value)
        .map_err(|_| MidgeError::ResourceLimit("SST value exceeds 4 GiB format limit".into()))?;
    Ok((key, value))
}

fn varint_len(mut value: u32) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn put_varint(mut value: u32, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push((value.to_le_bytes()[0] & 0x7f) | 0x80);
        value >>= 7;
    }
    out.push(value.to_le_bytes()[0]);
}

fn take_varint(data: &[u8], cursor: &mut usize) -> MidgeResult<u32> {
    let mut result = 0_u32;
    for index in 0..5 {
        let byte = take(data, cursor, 1)?[0];
        if index == 4 && byte > 0x0f {
            return Err(MidgeError::Corruption("SST varint overflows u32".into()));
        }
        result |= u32::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            if index > 0 && byte == 0 {
                return Err(MidgeError::Corruption("noncanonical SST varint".into()));
            }
            return Ok(result);
        }
    }
    Err(MidgeError::Corruption("unterminated SST varint".into()))
}

fn take<'a>(data: &'a [u8], cursor: &mut usize, length: usize) -> MidgeResult<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| MidgeError::Corruption("SST entry extent overflow".into()))?;
    let slice = data
        .get(*cursor..end)
        .ok_or_else(|| MidgeError::Corruption("truncated SST entry".into()))?;
    *cursor = end;
    Ok(slice)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_compact_entries_when_optional_fields_extend_the_header() {
        // Arrange
        for expiration in [None, Some(0), Some(u64::MAX)] {
            for key_len in [0, 3, 65_536] {
                let key = vec![b'k'; key_len];
                let encoded = encode_v5(
                    &key,
                    42,
                    Some(b"value"),
                    u64::MAX,
                    EntryType::Put,
                    expiration,
                )
                .unwrap();
                // Act
                let (entry, end) = decode(&encoded, 0).unwrap();
                // Assert
                assert_eq!(entry.key_delta, key);
                assert_eq!(entry.shared_len, 42);
                assert_eq!(entry.value, Some(b"value".as_slice()));
                assert_eq!(entry.expiration, expiration);
                assert_eq!(entry.sequence, u64::MAX);
                assert_eq!(end, encoded.len());
            }
        }
    }

    #[test]
    fn should_reject_malformed_compact_entry_headers() {
        // Arrange
        let canonical = encode_v5(b"k", 0, Some(b"v"), 1, EntryType::Put, None).unwrap();
        let mut flags = canonical.clone();
        flags[11] = 0x40;
        let mut expiration = canonical.clone();
        expiration[11] = EXPIRATION_PRESENT;
        // Act
        for data in [
            &[0x80, 0][..],
            &[0xff, 0xff, 0xff, 0xff, 0xff],
            &flags,
            &expiration,
        ] {
            // Assert: every malformed header is rejected by the decoder.
            assert!(decode(data, 0).is_err());
        }
        assert!(decode(&canonical, usize::MAX).is_err());
        assert!(matches!(
            decode_with_format(&canonical, 0, 4),
            Err(MidgeError::CompatibilityError(_))
        ));
    }

    #[test]
    fn should_save_fourteen_header_bytes_when_small_entry_has_no_expiration() {
        // Arrange
        let key = b"k";
        let value = b"v";
        // Act
        let encoded = encode_v5(key, 0, Some(value), 1, EntryType::Put, None).unwrap();
        // Assert: three lengths, sequence and flags = twelve bytes (V4:26).
        assert_eq!(encoded.len(), 12 + 2);
        assert!(validate_entry_size(usize::MAX, 1).is_err());
        assert!(validate_entry_size(1, usize::MAX).is_err());
    }
}
