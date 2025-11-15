//! SST entry encoding/decoding in TLV format
//!
//! This module provides the canonical TLV (Tag-Length-Value) encoding and decoding
//! for SST data block entries. All SST implementations (fs, cloud, mem) use this
//! shared encoding format.

use crate::common::tlv::{parse_varint32_from_slice, tags, TlvReader, TlvWriter};
use crate::error::{MidgeError, MidgeResult};
use bytes::Bytes;

// Type alias for complex iterator result: (key, value, sequence, entry_type, expiration)
pub type TlvEntryResult<'a> = MidgeResult<(Vec<u8>, Option<&'a [u8]>, u64, u8, Option<u64>)>;

/// Parsed TLV entry from SST data block
#[derive(Clone, Debug)]
pub struct TlvEntry<'a> {
    pub shared_len: u32,
    pub key_delta: &'a [u8],
    pub value: Option<&'a [u8]>,
    pub sequence: u64,
    pub entry_type: u8,
    pub expiration: Option<u64>,
    pub bytes_consumed: usize,
}

/// Encode a single SST entry in TLV format.
///
/// This is the canonical encoding used by DataBlockBuilder. The format includes:
/// - SHARED_PREFIX_LEN: Number of bytes shared with previous key (varint)
/// - KEY_DELTA: Suffix of the key after shared prefix (bytes)
/// - VALUE: Entry value, optional for tombstones (bytes)
/// - SEQUENCE: Sequence number, only if !internal_on_disk (u64)
/// - ENTRY_TYPE: Entry type (0=Put, 1=Insert, 2=Delete), only if !internal_on_disk (u8)
/// - EXPIRATION: TTL expiration timestamp, optional (u64)
pub fn encode(
    key_delta: &[u8],
    shared_len: u32,
    value: Option<&[u8]>,
    seq: u64,
    tombstone: bool,
    internal_on_disk: bool,
    expiration: Option<u64>,
) -> Vec<u8> {
    let user_value = value.unwrap_or(&[]);
    let mut tlv = TlvWriter::with_capacity(20 + key_delta.len() + user_value.len());

    // Write shared prefix length (varint)
    tlv.write_varint32(tags::SHARED_PREFIX_LEN, shared_len);

    // Write key delta (bytes)
    tlv.write_bytes(tags::KEY_DELTA, key_delta);

    // Write value (bytes, optional for tombstones)
    if !tombstone || !user_value.is_empty() {
        tlv.write_bytes(tags::VALUE, user_value);
    }

    // Only write sequence and entry_type as separate TLV fields when NOT using internal-on-disk format
    // When internal_on_disk=true, key already contains seq+type encoded in the key bytes
    if !internal_on_disk {
        // Write sequence number (u64)
        tlv.write_u64(tags::SEQUENCE, seq);

        // Write entry type (u8): Put=0, Insert=1, Delete=2
        let entry_type = if tombstone { 2u8 } else { 0u8 };
        tlv.write_u8(tags::ENTRY_TYPE, entry_type);
    }

    // Write expiration timestamp (TTL integration)
    if let Some(exp_millis) = expiration {
        tlv.write_u64(tags::EXPIRATION, exp_millis);
    }

    tlv.finish()
}

/// Parse a single TLV entry from data block
pub fn decode<'a>(data: &'a [u8], offset: usize, limit: usize) -> MidgeResult<TlvEntry<'a>> {
    if offset >= limit {
        return Err(MidgeError::InvalidData("offset >= limit".into()));
    }

    let mut reader = TlvReader::new(&data[offset..limit]);
    let mut shared_len: u32 = 0;
    let mut key_delta: Option<&[u8]> = None;
    let mut value: Option<&[u8]> = None;
    let mut sequence: u64 = 0;
    let mut entry_type: u8 = 0;
    let mut expiration: Option<u64> = None;
    let mut seen_shared_len = false;
    let mut final_cursor = 0;

    loop {
        let cursor_before = reader.cursor();
        let (tag, tag_data) = match reader.next() {
            Some(t) => t,
            None => break,
        };
        match tag {
            tags::SHARED_PREFIX_LEN => {
                // If we've already seen a SHARED_LEN, this is the start of the next entry
                // Use cursor_before to exclude this second SHARED_LEN from bytes_consumed
                if seen_shared_len && key_delta.is_some() {
                    final_cursor = cursor_before;
                    break;
                }
                shared_len = parse_varint32_from_slice(tag_data)?;
                seen_shared_len = true;
            }
            tags::KEY_DELTA => {
                key_delta = Some(tag_data);
            }
            tags::VALUE => {
                value = Some(tag_data);
            }
            tags::SEQUENCE => {
                if tag_data.len() >= 8 {
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
                if tag_data.len() >= 8 {
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
    }

    let delta =
        key_delta.ok_or_else(|| MidgeError::InvalidData("TLV entry missing key_delta".into()))?;

    Ok(TlvEntry {
        shared_len,
        key_delta: delta,
        value,
        sequence,
        entry_type,
        expiration,
        bytes_consumed: if final_cursor > 0 {
            final_cursor
        } else {
            reader.cursor()
        },
    })
}

/// Iterate over TLV entries in a data block, reconstructing full keys
pub struct TlvBlockIterator<'a> {
    data: &'a [u8],
    cursor: usize,
    limit: usize,
    last_key: Vec<u8>,
}

impl<'a> TlvBlockIterator<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        if data.len() < 9 {
            return Self {
                data,
                cursor: 0,
                limit: 0,
                last_key: Vec::new(),
            };
        }

        // Parse restart info
        let n = u32::from_le_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]) as usize;
        let restarts_start = data.len() - 4 - n * 4;
        // Version marker is before restart array
        let limit = restarts_start.saturating_sub(1);

        Self {
            data,
            cursor: 0,
            limit,
            last_key: Vec::new(),
        }
    }
}

impl<'a> Iterator for TlvBlockIterator<'a> {
    type Item = TlvEntryResult<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        eprintln!("DEBUG: TlvBlockIterator next, cursor={}, limit={}", self.cursor, self.limit);
        if self.cursor >= self.limit {
            eprintln!("DEBUG: cursor >= limit, returning None");
            return None;
        }

        eprintln!("DEBUG: calling decode at cursor {}", self.cursor);
        let entry = match decode(self.data, self.cursor, self.limit) {
            Ok(e) => {
                eprintln!("DEBUG: decoded entry: shared_len={}, key_delta_len={}, value_len={}", e.shared_len, e.key_delta.len(), e.value.as_ref().map(|v| v.len()).unwrap_or(0));
                e
            }
            Err(e) => {
                eprintln!("DEBUG: decode error: {:?}", e);
                return Some(Err(e));
            }
        };

        // Reconstruct full key
        let mut key = Vec::with_capacity(entry.shared_len as usize + entry.key_delta.len());
        if entry.shared_len as usize > self.last_key.len() {
            return None; // Invalid shared length
        }
        key.extend_from_slice(&self.last_key[..entry.shared_len as usize]);
        key.extend_from_slice(entry.key_delta);

        self.cursor += entry.bytes_consumed;
        // Reuse the existing buffer if possible to avoid allocation
        self.last_key.clear();
        self.last_key.extend_from_slice(&key);

        Some(Ok((
            key,
            entry.value,
            entry.sequence,
            entry.entry_type,
            entry.expiration,
        )))
    }
}

/// Parse key at a restart point (where shared_len must be 0)
pub fn decode_key_at_offset(data: &[u8], offset: usize, limit: usize) -> MidgeResult<Vec<u8>> {
    if offset >= limit {
        return Err(MidgeError::InvalidData("offset beyond data".into()));
    }

    let reader = TlvReader::new(&data[offset..limit]);
    let mut shared_len: u32 = 0;
    let mut key_delta: Option<&[u8]> = None;

    for (tag, tag_data) in reader {
        match tag {
            tags::SHARED_PREFIX_LEN => {
                shared_len = parse_varint32_from_slice(tag_data)?;
            }
            tags::KEY_DELTA => {
                key_delta = Some(tag_data);
            }
            _ => {
                // Skip other tags
            }
        }
    }

    if shared_len != 0 {
        return Err(MidgeError::InvalidData("non-zero shared at restart".into()));
    }

    let delta =
        key_delta.ok_or_else(|| MidgeError::InvalidData("TLV entry missing key_delta".into()))?;

    Ok(delta.to_vec())
}

/// Linear search through data block entries for a target key
pub fn linear_search_data_block(
    data: &[u8],
    mut cursor: usize,
    limit: usize,
    target_key: &[u8],
    decode_internal: bool,
) -> MidgeResult<Option<Bytes>> {
    let mut last_key: Vec<u8> = Vec::new();

    while cursor < limit {
        // Parse TLV entry using decode
        let entry = match decode(data, cursor, limit) {
            Ok(e) => e,
            Err(_) => break,
        };

        // Reconstruct full key
        let mut key = Vec::with_capacity(entry.shared_len as usize + entry.key_delta.len());
        if entry.shared_len as usize > last_key.len() {
            return Err(MidgeError::InvalidData(format!(
                "shared_len {} exceeds last_key len {}",
                entry.shared_len,
                last_key.len()
            )));
        }
        key.extend_from_slice(&last_key[..entry.shared_len as usize]);
        key.extend_from_slice(entry.key_delta);

        // Move cursor to next entry
        cursor += entry.bytes_consumed;

        // Match against target key
        if decode_internal {
            // Extract user key from internal key
            if let Some((user, _seq, _tomb)) =
                crate::common::internal_key::decode_internal_key(&key)
            {
                last_key = key.clone();
                if user.as_slice() == target_key {
                    return Ok(entry.value.map(Bytes::copy_from_slice));
                }
                if user.as_slice() > target_key {
                    break;
                }
            } else {
                // Fallback: treat as regular key
                last_key = key.clone();
                if key.as_slice() == target_key {
                    return Ok(entry.value.map(Bytes::copy_from_slice));
                }
                if key.as_slice() > target_key {
                    break;
                }
            }
        } else {
            // Direct key comparison
            last_key = key.clone();
            if key.as_slice() == target_key {
                return Ok(entry.value.map(Bytes::copy_from_slice));
            }
            if key.as_slice() > target_key {
                break;
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_basic_entry() {
        // Arrange
        let key_delta = b"test_key";
        let value = b"test_value";

        // Act
        let encoded = encode(key_delta, 0, Some(value), 100, false, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.shared_len, 0);
        assert_eq!(decoded.key_delta, key_delta);
        assert_eq!(decoded.value, Some(value.as_slice()));
        assert_eq!(decoded.sequence, 100);
        assert_eq!(decoded.entry_type, 0);
        assert_eq!(decoded.expiration, None);
    }

    #[test]
    fn should_encode_with_shared_prefix() {
        // Arrange
        let key_delta = b"suffix";
        let shared_len = 5;

        // Act
        let encoded = encode(
            key_delta,
            shared_len,
            Some(b"value"),
            42,
            false,
            false,
            None,
        );
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.shared_len, 5);
        assert_eq!(decoded.key_delta, b"suffix");
    }

    #[test]
    fn should_encode_tombstone_entry() {
        // Arrange
        let key_delta = b"deleted_key";

        // Act
        let encoded = encode(key_delta, 0, None, 200, true, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.value, None);
        assert_eq!(decoded.entry_type, 2); // Delete
    }

    #[test]
    fn should_encode_with_expiration() {
        // Arrange
        let key_delta = b"ttl_key";
        let expiration = Some(1698262800000u64);

        // Act
        let encoded = encode(key_delta, 0, Some(b"value"), 100, false, false, expiration);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.expiration, Some(1698262800000));
    }

    #[test]
    fn should_encode_internal_on_disk_format() {
        // Arrange
        let key_delta = b"internal_key";

        // Act
        let encoded = encode(key_delta, 0, Some(b"value"), 100, false, true, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        // When internal_on_disk=true, sequence and entry_type are NOT written separately
        assert_eq!(decoded.sequence, 0); // Default value
        assert_eq!(decoded.entry_type, 0); // Default value
    }

    #[test]
    fn should_encode_with_all_fields() {
        // Arrange
        let key_delta = b"full_key";
        let value = b"full_value";
        let shared_len = 10;
        let seq = 999;
        let expiration = Some(9999999999u64);

        // Act
        let encoded = encode(
            key_delta,
            shared_len,
            Some(value),
            seq,
            false,
            false,
            expiration,
        );
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.shared_len, 10);
        assert_eq!(decoded.key_delta, key_delta);
        assert_eq!(decoded.value, Some(value.as_slice()));
        assert_eq!(decoded.sequence, 999);
        assert_eq!(decoded.entry_type, 0);
        assert_eq!(decoded.expiration, Some(9999999999));
    }

    #[test]
    fn should_handle_empty_value() {
        // Arrange
        let key_delta = b"key";

        // Act
        let encoded = encode(key_delta, 0, Some(b""), 100, false, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.value, Some(&b""[..]));
    }

    #[test]
    fn should_handle_large_shared_prefix() {
        // Arrange
        let key_delta = b"x";
        let shared_len = 255;

        // Act
        let encoded = encode(key_delta, shared_len, Some(b"val"), 50, false, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.shared_len, 255);
    }

    #[test]
    fn should_return_error_given_offset_beyond_limit() {
        // Arrange
        let data = vec![0u8; 10];

        // Act
        let result = decode(&data, 15, 10);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("offset >= limit"));
    }

    #[test]
    fn should_return_error_given_missing_key_delta() {
        // Arrange
        // Malformed TLV with only SHARED_PREFIX_LEN tag
        let data = vec![
            1, // tag: SHARED_PREFIX_LEN
            1, // length
            0, // value: 0
        ];

        // Act
        let result = decode(&data, 0, data.len());

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("missing key_delta"));
    }

    #[test]
    fn should_iterate_over_multiple_entries() {
        // Arrange
        let mut block_data = Vec::new();

        // Add 3 entries
        let entry1 = encode(b"key1", 0, Some(b"value1"), 1, false, false, None);
        let entry2 = encode(b"ey2", 1, Some(b"value2"), 2, false, false, None); // shared_len=1 (shares 'k')
        let entry3 = encode(b"ey3", 1, Some(b"value3"), 3, false, false, None);

        block_data.extend_from_slice(&entry1);
        block_data.extend_from_slice(&entry2);
        block_data.extend_from_slice(&entry3);

        // Add block footer (version marker + restart array + count)
        block_data.push(3); // version marker
        block_data.extend_from_slice(&0u32.to_le_bytes()); // restart point 0
        block_data.extend_from_slice(&1u32.to_le_bytes()); // 1 restart point

        // Act
        let iterator = TlvBlockIterator::new(&block_data);
        let entries: Vec<_> = iterator.collect();

        // Assert
        assert_eq!(entries.len(), 3);
        assert!(entries[0].is_ok());
        assert!(entries[1].is_ok());
        assert!(entries[2].is_ok());

        let (key1, _, _, _, _) = entries[0].as_ref().unwrap();
        let (key2, _, _, _, _) = entries[1].as_ref().unwrap();
        let (key3, _, _, _, _) = entries[2].as_ref().unwrap();

        assert_eq!(key1, b"key1");
        assert_eq!(key2, b"key2");
        assert_eq!(key3, b"key3");
    }

    #[test]
    fn should_handle_empty_block_iterator() {
        // Arrange
        let data = vec![0u8; 5]; // Too small for valid block

        // Act
        let iterator = TlvBlockIterator::new(&data);
        let entries: Vec<_> = iterator.collect();

        // Assert
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn should_reconstruct_keys_with_shared_prefixes() {
        // Arrange
        let mut block_data = Vec::new();

        // "apple", "application", "apply" - all share "appl"
        let entry1 = encode(b"apple", 0, Some(b"v1"), 1, false, false, None);
        let entry2 = encode(b"ication", 4, Some(b"v2"), 2, false, false, None); // shares "appl"
        let entry3 = encode(b"y", 4, Some(b"v3"), 3, false, false, None); // shares "appl"

        block_data.extend_from_slice(&entry1);
        block_data.extend_from_slice(&entry2);
        block_data.extend_from_slice(&entry3);

        block_data.push(3);
        block_data.extend_from_slice(&0u32.to_le_bytes());
        block_data.extend_from_slice(&1u32.to_le_bytes());

        // Act
        let iterator = TlvBlockIterator::new(&block_data);
        let entries: Vec<_> = iterator.collect();

        // Assert
        assert_eq!(entries.len(), 3);

        let (key1, _, _, _, _) = entries[0].as_ref().unwrap();
        let (key2, _, _, _, _) = entries[1].as_ref().unwrap();
        let (key3, _, _, _, _) = entries[2].as_ref().unwrap();

        assert_eq!(key1, b"apple");
        assert_eq!(key2, b"application");
        assert_eq!(key3, b"apply");
    }

    #[test]
    fn should_parse_key_at_restart_point() {
        // Arrange
        let encoded = encode(b"restart_key", 0, Some(b"value"), 100, false, false, None);

        // Act
        let key = decode_key_at_offset(&encoded, 0, encoded.len()).expect("parse");

        // Assert
        assert_eq!(key, b"restart_key");
    }

    #[test]
    fn should_return_error_given_nonzero_shared_at_restart() {
        // Arrange
        let encoded = encode(b"key", 5, Some(b"value"), 100, false, false, None);

        // Act
        let result = decode_key_at_offset(&encoded, 0, encoded.len());

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-zero shared"));
    }

    #[test]
    fn should_return_error_given_offset_beyond_data() {
        // Arrange
        let data = vec![0u8; 10];

        // Act
        let result = decode_key_at_offset(&data, 20, 10);

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("offset beyond data"));
    }

    #[test]
    fn should_find_target_key_in_linear_search() {
        // Arrange - Build a proper data block
        use crate::sst::format::DataBlockBuilder;
        let mut builder = DataBlockBuilder::new(16);
        builder.add(b"apple", b"v1").unwrap();
        builder.add(b"banana", b"v2").unwrap();
        builder.add(b"cherry", b"v3").unwrap();
        let block_data = builder.finish();

        // Calculate entries_end (before restart array)
        let num_restarts = u32::from_le_bytes([
            block_data[block_data.len() - 4],
            block_data[block_data.len() - 3],
            block_data[block_data.len() - 2],
            block_data[block_data.len() - 1],
        ]) as usize;
        let restarts_start = block_data.len() - 4 - (num_restarts * 4);
        let entries_end = restarts_start - 1; // Skip version marker

        // Act
        let result = linear_search_data_block(&block_data, 0, entries_end, b"banana", false)
            .expect("search");

        // Assert
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Bytes::from("v2"));
    }

    #[test]
    fn should_return_none_when_key_not_found() {
        // Arrange - Build a proper data block
        use crate::sst::format::DataBlockBuilder;
        let mut builder = DataBlockBuilder::new(16);
        builder.add(b"apple", b"v1").unwrap();
        builder.add(b"banana", b"v2").unwrap();
        let block_data = builder.finish();

        // Calculate entries_end
        let num_restarts = u32::from_le_bytes([
            block_data[block_data.len() - 4],
            block_data[block_data.len() - 3],
            block_data[block_data.len() - 2],
            block_data[block_data.len() - 1],
        ]) as usize;
        let restarts_start = block_data.len() - 4 - (num_restarts * 4);
        let entries_end = restarts_start - 1;

        // Act
        let result = linear_search_data_block(&block_data, 0, entries_end, b"cherry", false)
            .expect("search");

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_stop_search_when_key_exceeds_target() {
        // Arrange - Build a proper data block
        use crate::sst::format::DataBlockBuilder;
        let mut builder = DataBlockBuilder::new(16);
        builder.add(b"apple", b"v1").unwrap();
        builder.add(b"banana", b"v2").unwrap();
        builder.add(b"cherry", b"v3").unwrap();
        let block_data = builder.finish();

        // Calculate entries_end
        let num_restarts = u32::from_le_bytes([
            block_data[block_data.len() - 4],
            block_data[block_data.len() - 3],
            block_data[block_data.len() - 2],
            block_data[block_data.len() - 1],
        ]) as usize;
        let restarts_start = block_data.len() - 4 - (num_restarts * 4);
        let entries_end = restarts_start - 1;

        // Act - search for "avocado" which is between "apple" and "banana"
        let result = linear_search_data_block(&block_data, 0, entries_end, b"avocado", false)
            .expect("search");

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_handle_binary_data_in_keys_and_values() {
        // Arrange
        let binary_key = vec![0x00, 0xFF, 0x80, 0x7F, 0xDE, 0xAD];
        let binary_value = vec![0xCA, 0xFE, 0xBA, 0xBE];

        // Act
        let encoded = encode(&binary_key, 0, Some(&binary_value), 100, false, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.key_delta, binary_key.as_slice());
        assert_eq!(decoded.value, Some(binary_value.as_slice()));
    }

    #[test]
    fn should_preserve_sequence_numbers() {
        // Arrange
        let sequences = vec![0u64, 1, 100, 999999, u64::MAX];

        for seq in sequences {
            // Act
            let encoded = encode(b"key", 0, Some(b"value"), seq, false, false, None);
            let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

            // Assert
            assert_eq!(decoded.sequence, seq, "Failed for sequence: {}", seq);
        }
    }

    #[test]
    fn should_encode_put_operation() {
        // Arrange & Act
        let encoded = encode(b"key", 0, Some(b"value"), 100, false, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.entry_type, 0); // Put
    }

    #[test]
    fn should_encode_delete_operation() {
        // Arrange & Act
        let encoded = encode(b"key", 0, None, 100, true, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.entry_type, 2); // Delete
    }

    #[test]
    fn should_handle_tombstone_with_value() {
        // Arrange - tombstone but with non-empty value (edge case)
        let value = b"tombstone_value";

        // Act
        let encoded = encode(b"key", 0, Some(value), 100, true, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.value, Some(value.as_slice()));
        assert_eq!(decoded.entry_type, 2);
    }

    #[test]
    fn should_return_error_given_invalid_shared_length() {
        // Arrange
        let mut block_data = Vec::new();
        let entry1 = encode(b"apple", 0, Some(b"v1"), 1, false, false, None);
        let entry2 = encode(b"key", 100, Some(b"v2"), 2, false, false, None); // Invalid: shared > previous key

        block_data.extend_from_slice(&entry1);
        block_data.extend_from_slice(&entry2);

        // Act
        let result = linear_search_data_block(&block_data, 0, block_data.len(), b"key", false);

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("exceeds last_key len"));
    }

    #[test]
    fn should_handle_large_values() {
        // Arrange
        let large_value = vec![b'X'; 10000];

        // Act
        let encoded = encode(b"key", 0, Some(&large_value), 100, false, false, None);
        let decoded = decode(&encoded, 0, encoded.len()).expect("decode");

        // Assert
        assert_eq!(decoded.value.unwrap().len(), 10000);
        assert_eq!(decoded.value.unwrap(), large_value.as_slice());
    }

    #[test]
    fn should_handle_multiple_restarts_in_iterator() {
        // Arrange
        let mut block_data = Vec::new();

        // Add entries
        let entry1 = encode(b"a", 0, Some(b"v1"), 1, false, false, None);
        let entry2 = encode(b"b", 0, Some(b"v2"), 2, false, false, None); // New restart

        block_data.extend_from_slice(&entry1);
        let restart1_offset = block_data.len();
        block_data.extend_from_slice(&entry2);

        // Add block footer with 2 restart points
        block_data.push(3); // version
        block_data.extend_from_slice(&0u32.to_le_bytes()); // restart 0
        block_data.extend_from_slice(&(restart1_offset as u32).to_le_bytes()); // restart 1
        block_data.extend_from_slice(&2u32.to_le_bytes()); // 2 restart points

        // Act
        let iterator = TlvBlockIterator::new(&block_data);
        let entries: Vec<_> = iterator.collect();

        // Assert
        assert_eq!(entries.len(), 2);
        let (key1, _, _, _, _) = entries[0].as_ref().unwrap();
        let (key2, _, _, _, _) = entries[1].as_ref().unwrap();
        assert_eq!(key1, b"a");
        assert_eq!(key2, b"b");
    }

    #[test]
    fn should_preserve_data_across_encode_decode_cycles() {
        // Arrange
        let key_delta = b"test_key";
        let value = b"test_value";
        let shared_len = 5;
        let seq = 42;
        let expiration = Some(123456789u64);

        // Act - encode/decode 10 times
        let mut current_encoded = encode(
            key_delta,
            shared_len,
            Some(value),
            seq,
            false,
            false,
            expiration,
        );

        for _ in 0..10 {
            let decoded = decode(&current_encoded, 0, current_encoded.len()).expect("decode");
            current_encoded = encode(
                decoded.key_delta,
                decoded.shared_len,
                decoded.value,
                decoded.sequence,
                decoded.entry_type == 2,
                false,
                decoded.expiration,
            );
        }

        let final_decoded = decode(&current_encoded, 0, current_encoded.len()).expect("decode");

        // Assert
        assert_eq!(final_decoded.shared_len, shared_len);
        assert_eq!(final_decoded.key_delta, key_delta);
        assert_eq!(final_decoded.value, Some(value.as_slice()));
        assert_eq!(final_decoded.sequence, seq);
        assert_eq!(final_decoded.expiration, expiration);
    }
}
