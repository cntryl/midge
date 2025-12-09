//! Meta index utilities shared across SST implementations.
//!
//! The meta index is a special index block in SST files that maps metadata keys
//! (like "filter.bloom", "tombstones.range", "format.internal_keys") to BlockHandles
//! or other values.

use crate::error::MidgeResult;
use crate::sst::encoding::decode;
use crate::sst::format::BlockHandle;

/// Search meta index for a specific key and return its BlockHandle.
///
/// This performs a linear search through TLV-encoded entries in the meta index,
/// reconstructing full keys and matching against the target key.
///
/// # Arguments
/// * `data` - The raw meta index block data
/// * `start_offset` - Offset to start searching from
/// * `limit` - Maximum offset to search up to
/// * `target_key` - The key to search for (e.g., b"filter.bloom")
///
/// # Returns
/// * `Ok(Some(handle))` - Found the key and successfully decoded its BlockHandle
/// * `Ok(None)` - Key not found or end of index reached
/// * `Err(_)` - Parsing error
pub fn linear_search_meta_index(
    data: &[u8],
    start_offset: usize,
    limit: usize,
    target_key: &[u8],
) -> MidgeResult<Option<BlockHandle>> {
    let mut cursor = start_offset;
    let mut current_key = Vec::new();

    while cursor < limit {
        let entry = match decode(data, cursor, limit) {
            Ok(e) => e,
            Err(_) => break,
        };

        // Reconstruct full key using prefix compression
        current_key.truncate(entry.shared_len as usize);
        current_key.extend_from_slice(entry.key_delta);

        // Try to decode value as BlockHandle
        if let Some(handle_data) = entry.value {
            match BlockHandle::decode(handle_data) {
                Ok((handle, _)) => {
                    if current_key.as_slice() == target_key {
                        return Ok(Some(handle));
                    }
                }
                Err(_) => {
                    // Skip entries that aren't BlockHandles (e.g., format flags)
                }
            }
        }

        cursor += entry.bytes_consumed;
    }

    Ok(None)
}

/// Check if meta index contains a specific key (without returning its value).
///
/// This is more efficient than `linear_search_meta_index` when you only need
/// to check for presence (e.g., checking for "format.internal_keys" flag).
///
/// # Arguments
/// * `data` - The raw meta index block data
/// * `start_offset` - Offset to start searching from
/// * `limit` - Maximum offset to search up to
/// * `target_key` - The key to search for
///
/// # Returns
/// * `Ok(true)` - Key found
/// * `Ok(false)` - Key not found
/// * `Err(_)` - Parsing error
pub fn meta_index_contains(
    data: &[u8],
    start_offset: usize,
    limit: usize,
    target_key: &[u8],
) -> MidgeResult<bool> {
    let mut cursor = start_offset;
    let mut current_key = Vec::new();

    while cursor < limit {
        let entry = match decode(data, cursor, limit) {
            Ok(e) => e,
            Err(_) => break,
        };

        // Reconstruct full key using prefix compression
        current_key.truncate(entry.shared_len as usize);
        current_key.extend_from_slice(entry.key_delta);

        if current_key.as_slice() == target_key {
            return Ok(true);
        }

        cursor += entry.bytes_consumed;
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::format::DataBlockBuilder;

    #[test]
    fn should_find_key_in_meta_index() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        let handle = BlockHandle {
            offset: 100,
            size: 200,
        };
        builder.add(b"filter.bloom", &handle.encode()).unwrap();
        builder
            .add(
                b"tombstones.range",
                &BlockHandle {
                    offset: 300,
                    size: 50,
                }
                .encode(),
            )
            .unwrap();
        let data = builder.finish();

        // Act
        let result = linear_search_meta_index(&data, 0, data.len(), b"filter.bloom").unwrap();

        // Assert
        assert!(result.is_some());
        let found_handle = result.unwrap();
        assert_eq!(found_handle.offset, 100);
        assert_eq!(found_handle.size, 200);
    }

    #[test]
    fn should_return_none_when_key_not_found() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder
            .add(
                b"filter.bloom",
                &BlockHandle {
                    offset: 100,
                    size: 200,
                }
                .encode(),
            )
            .unwrap();
        let data = builder.finish();

        // Act
        let result = linear_search_meta_index(&data, 0, data.len(), b"nonexistent").unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_detect_key_presence() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"format.internal_keys", b"1").unwrap();
        let data = builder.finish();

        // Act
        let result = meta_index_contains(&data, 0, data.len(), b"format.internal_keys").unwrap();

        // Assert
        assert!(result);
    }

    #[test]
    fn should_return_false_when_key_not_present() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"filter.bloom", b"dummy").unwrap();
        let data = builder.finish();

        // Act
        let result = meta_index_contains(&data, 0, data.len(), b"format.internal_keys").unwrap();

        // Assert
        assert!(!result);
    }
}
