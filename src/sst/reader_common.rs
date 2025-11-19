//! Common utilities for SST readers shared across fs, mem, and cloud implementations.
//!
//! This module extracts duplicated logic from the three reader implementations to:
//! - Reduce code duplication
//! - Simplify maintenance
//! - Ensure consistent behavior across all reader types

use crate::error::MidgeResult;
use crate::sst::bloom::BloomFilter;
use crate::sst::format::{Block, BlockHandle, BlockType, Footer};
use crate::sst::meta_index::{linear_search_meta_index, meta_index_contains};
use crate::sst::range_tombstone::decode_range_tombstones;
use crate::sst::sparse_index::SparseIndex;
use crate::sst::traits::RangeTombstone;

/// Common metadata loaded from an SST file
#[derive(Debug)]
pub struct SstMetadata {
    pub footer: Footer,
    pub sparse_index: SparseIndex,
    pub bloom_filter: Option<BloomFilter>,
    pub range_tombstones: Vec<RangeTombstone>,
    pub use_internal_keys: bool,
}

impl SstMetadata {
    /// Parse SST metadata from raw bytes
    ///
    /// This is the common initialization logic shared by all three reader types.
    /// It reads the footer, index block, and optionally the meta index with bloom
    /// filter and range tombstones.
    pub fn from_bytes(raw: &[u8]) -> MidgeResult<Self> {
        if raw.len() < 48 {
            return Err(crate::error::MidgeError::InvalidData(
                "SST too small".into(),
            ));
        }

        let footer_start = raw.len() - 48;
        let footer = Footer::decode(&raw[footer_start..])?;

        // Read and decode index block
        let idx_off = footer.index_handle.offset as usize;
        let idx_size = footer.index_handle.size as usize;
        let idx_raw = &raw[idx_off..idx_off + idx_size];

        let idx_block = Block::decode(idx_raw, BlockType::Index)?;

        let sparse_index = SparseIndex::decode(&idx_block.data)?;

        // Optionally read meta index, bloom filter, and range tombstones
        let mut bloom_filter: Option<BloomFilter> = None;
        let mut range_tombstones: Vec<RangeTombstone> = Vec::new();
        let mut use_internal = false;

        if footer.meta_index_handle.size > 0 {
            let meta_off = footer.meta_index_handle.offset as usize;
            let meta_size = footer.meta_index_handle.size as usize;
            let meta_raw = &raw[meta_off..meta_off + meta_size];

            let meta_block = Block::decode(meta_raw, BlockType::MetaIndex)?;

            if let Some(bh) = find_bloom_filter_handle(&meta_block.data)? {
                let off = bh.offset as usize;
                let sz = bh.size as usize;
                let bloom_raw = &raw[off..off + sz];
                let bloom_block = Block::decode(bloom_raw, BlockType::Filter)?;
                bloom_filter = Some(BloomFilter::decode_block(&bloom_block.data)?);
            }

            if let Some(bh) = find_range_tombstones_handle(&meta_block.data)? {
                let off = bh.offset as usize;
                let sz = bh.size as usize;
                let tomb_raw = &raw[off..off + sz];
                let tomb_block = Block::decode(tomb_raw, BlockType::Filter)?;
                range_tombstones = decode_range_tombstones(&tomb_block.data)?;
            }

            // Detect internal-key format flag
            use_internal = meta_index_contains(
                &meta_block.data,
                0,
                meta_block.data.len(),
                b"format.internal_keys",
            )?;
        }

        Ok(Self {
            footer,
            sparse_index,
            bloom_filter,
            range_tombstones,
            use_internal_keys: use_internal,
        })
    }
}

/// Find bloom filter block handle in meta index
pub fn find_bloom_filter_handle(meta_index_data: &[u8]) -> MidgeResult<Option<BlockHandle>> {
    linear_search_meta_index(meta_index_data, 0, meta_index_data.len(), b"filter.bloom")
}

/// Find range tombstones block handle in meta index
pub fn find_range_tombstones_handle(meta_index_data: &[u8]) -> MidgeResult<Option<BlockHandle>> {
    linear_search_meta_index(
        meta_index_data,
        0,
        meta_index_data.len(),
        b"tombstones.range",
    )
}

/// Parse and optionally decode internal key at offset
///
/// This function is used by all SST readers to parse a key from a data block
/// and optionally decode it if it's in internal key format.
pub fn parse_key_at_offset(
    data: &[u8],
    offset: usize,
    limit: usize,
    decode_internal: bool,
) -> MidgeResult<Vec<u8>> {
    use crate::sst::encoding::decode_key_at_offset;

    let k = decode_key_at_offset(data, offset, limit)?;
    if decode_internal {
        // Only treat the key as internal when it has a valid entry type suffix.
        if let Some((user, _seq, _entry_type)) =
            crate::common::internal_key::decode_internal_key_typed(&k)
        {
            return Ok(user);
        }
    }
    Ok(k)
}

/// Read and decode data block from raw bytes
///
/// Validates that the decoded block is actually a data block.
/// Used by mem and cloud readers that already have data in memory.
pub fn read_data_block_from_bytes(raw: &[u8]) -> MidgeResult<Block> {
    read_data_block_from_bytes_paranoid(raw, false)
}

/// Read and decode data block with optional paranoid checksum verification
pub fn read_data_block_from_bytes_paranoid(raw: &[u8], paranoid: bool) -> MidgeResult<Block> {
    if let Ok(b) = Block::decode_with_options(raw, BlockType::Data, paranoid) {
        if b.block_type == BlockType::Data {
            return Ok(b);
        }
    }
    Err(crate::error::MidgeError::InvalidData(
        "Unable to decode data block".into(),
    ))
}

/// Search data block with binary search over restart points
///
/// This is the core search algorithm used by mem and cloud readers.
/// Performs binary search over restart points, then linear search within a segment.
pub fn search_data_block(
    data: &[u8],
    target_key: &[u8],
    use_internal_keys: bool,
) -> MidgeResult<Option<bytes::Bytes>> {
    use crate::sst::encoding::linear_search_data_block;

    let len = data.len();
    if len < 8 {
        return Ok(None);
    }

    let num_restarts =
        u32::from_le_bytes([data[len - 4], data[len - 3], data[len - 2], data[len - 1]]) as usize;
    let restarts_start = len - 4 - (num_restarts * 4);
    // TLV format: version byte (1 byte) before restart array
    let version_offset = restarts_start.saturating_sub(1);
    let entries_end = version_offset;

    // Binary search over restarts
    let mut left = 0usize;
    let mut right = num_restarts;
    while left < right {
        let mid = (left + right) / 2;
        let off = u32::from_le_bytes([
            data[restarts_start + mid * 4],
            data[restarts_start + mid * 4 + 1],
            data[restarts_start + mid * 4 + 2],
            data[restarts_start + mid * 4 + 3],
        ]) as usize;

        if let Ok(k) = parse_key_at_offset(data, off, entries_end, use_internal_keys) {
            if k.as_slice() <= target_key {
                left = mid + 1;
            } else {
                right = mid;
            }
        } else {
            break;
        }
    }

    let idx = if left > 0 { left - 1 } else { 0 };
    let off = u32::from_le_bytes([
        data[restarts_start + idx * 4],
        data[restarts_start + idx * 4 + 1],
        data[restarts_start + idx * 4 + 2],
        data[restarts_start + idx * 4 + 3],
    ]) as usize;

    linear_search_data_block(data, off, entries_end, target_key, use_internal_keys)
}

/// Check if a key should be filtered out by bloom filter or range tombstones
///
/// Returns `true` if the key is definitely absent (bloom filter says no or covered by tombstone).
/// This is a common early-out check used by all SST readers.
pub fn should_skip_key(
    bloom_filter: &Option<BloomFilter>,
    range_tombstones: &[RangeTombstone],
    key: &[u8],
    snapshot_seq: u64,
) -> bool {
    use crate::sst::range_tombstone::is_covered_by_range_tombstone;

    // Bloom filter early-out
    if let Some(bf) = bloom_filter {
        if !bf.may_contain(key) {
            return true;
        }
    }

    // Range tombstone check

    is_covered_by_range_tombstone(range_tombstones, key, snapshot_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::codec::CompressionType;
    use crate::sst::format::{DataBlockBuilder, IndexBlockBuilder as MetaIndexBuilder};

    // --- SstMetadata::from_bytes tests ---

    #[test]
    fn should_reject_data_smaller_than_footer() {
        // Arrange
        let data = vec![0u8; 40]; // Less than 48 bytes

        // Act
        let result = SstMetadata::from_bytes(&data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_data_exactly_footer_size_without_index() {
        // Arrange
        let data = vec![0u8; 48]; // Exactly footer size but invalid

        // Act
        let result = SstMetadata::from_bytes(&data);

        // Assert
        assert!(result.is_err()); // Invalid footer data
    }

    #[test]
    fn should_detect_data_too_small_error() {
        // Arrange
        let data = vec![0u8; 10];

        // Act
        let result = SstMetadata::from_bytes(&data);

        // Assert
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("SST too small") || err_msg.contains("InvalidData"));
    }

    // --- find_bloom_filter_handle tests ---

    #[test]
    fn should_return_none_when_bloom_filter_not_in_meta_index() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder
            .add(
                b"some.other.key",
                &BlockHandle {
                    offset: 100,
                    size: 50,
                }
                .encode(),
            )
            .unwrap();
        let meta_data = builder.finish();

        // Act
        let result = find_bloom_filter_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_find_bloom_filter_handle_when_present() {
        // Arrange
        let handle = BlockHandle {
            offset: 1000,
            size: 256,
        };
        let mut builder = MetaIndexBuilder::new();
        builder.add_index_entry(b"filter.bloom", handle).unwrap();
        let meta_data = builder.finish();

        // Act
        let result = find_bloom_filter_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_some());
        let found = result.unwrap();
        assert_eq!(found.offset, 1000);
        assert_eq!(found.size, 256);
    }

    #[test]
    fn should_find_bloom_filter_among_multiple_entries() {
        // Arrange
        let bloom_handle = BlockHandle {
            offset: 500,
            size: 128,
        };
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(
            b"another.key",
            BlockHandle {
                offset: 200,
                size: 75,
            },
        );
        let _ = builder.add_index_entry(b"filter.bloom", bloom_handle);
        let _ = builder.add_index_entry(
            b"some.key",
            BlockHandle {
                offset: 100,
                size: 50,
            },
        );
        let meta_data = builder.finish();

        // Act
        let result = find_bloom_filter_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(result.unwrap().offset, 500);
    }

    #[test]
    fn should_handle_empty_meta_index_for_bloom_filter() {
        // Arrange
        let builder = MetaIndexBuilder::new();
        let meta_data = builder.finish();

        // Act
        let result = find_bloom_filter_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_not_match_partial_bloom_filter_key() {
        // Arrange
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(
            b"filter.bloom.extra",
            BlockHandle {
                offset: 100,
                size: 50,
            },
        );
        let meta_data = builder.finish();

        // Act
        let result = find_bloom_filter_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_none()); // Should not match with extra suffix
    }

    // --- find_range_tombstones_handle tests ---

    #[test]
    fn should_return_none_when_tombstones_not_in_meta_index() {
        // Arrange
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(
            b"filter.bloom",
            BlockHandle {
                offset: 100,
                size: 50,
            },
        );
        let meta_data = builder.finish();

        // Act
        let result = find_range_tombstones_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_find_tombstones_handle_when_present() {
        // Arrange
        let handle = BlockHandle {
            offset: 2000,
            size: 512,
        };
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(b"tombstones.range", handle);
        let meta_data = builder.finish();

        // Act
        let result = find_range_tombstones_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_some());
        let found = result.unwrap();
        assert_eq!(found.offset, 2000);
        assert_eq!(found.size, 512);
    }

    #[test]
    fn should_find_tombstones_among_multiple_entries() {
        // Arrange
        let tomb_handle = BlockHandle {
            offset: 800,
            size: 256,
        };
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(
            b"filter.bloom",
            BlockHandle {
                offset: 100,
                size: 50,
            },
        );
        let _ = builder.add_index_entry(b"tombstones.range", tomb_handle);
        let _ =
            builder.add_index_entry(b"format.internal_keys", BlockHandle { offset: 0, size: 0 });
        let meta_data = builder.finish();

        // Act
        let result = find_range_tombstones_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(result.unwrap().offset, 800);
    }

    #[test]
    fn should_handle_empty_meta_index_for_tombstones() {
        // Arrange
        let builder = MetaIndexBuilder::new();
        let meta_data = builder.finish();

        // Act
        let result = find_range_tombstones_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_distinguish_bloom_tombstone_keys() {
        // Arrange
        let bloom_handle = BlockHandle {
            offset: 100,
            size: 50,
        };
        let tomb_handle = BlockHandle {
            offset: 200,
            size: 75,
        };
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(b"filter.bloom", bloom_handle);
        let _ = builder.add_index_entry(b"tombstones.range", tomb_handle);
        let meta_data = builder.finish();

        // Act
        let bloom_result = find_bloom_filter_handle(&meta_data).unwrap();
        let tomb_result = find_range_tombstones_handle(&meta_data).unwrap();

        // Assert
        assert_eq!(bloom_result.unwrap().offset, 100);
        assert_eq!(tomb_result.unwrap().offset, 200);
    }

    #[test]
    fn should_not_match_partial_tombstone_key() {
        // Arrange
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(
            b"tombstones.range.v2",
            BlockHandle {
                offset: 100,
                size: 50,
            },
        );
        let meta_data = builder.finish();

        // Act
        let result = find_range_tombstones_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_none()); // Should not match with extra suffix
    }

    // --- Integration tests ---

    #[test]
    fn should_handle_meta_index_with_bloom_tombstones() {
        // Arrange
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(
            b"filter.bloom",
            BlockHandle {
                offset: 1000,
                size: 128,
            },
        );
        let _ = builder.add_index_entry(
            b"tombstones.range",
            BlockHandle {
                offset: 2000,
                size: 256,
            },
        );
        let meta_data = builder.finish();

        // Act
        let bloom = find_bloom_filter_handle(&meta_data).unwrap();
        let tomb = find_range_tombstones_handle(&meta_data).unwrap();

        // Assert
        assert!(bloom.is_some());
        assert!(tomb.is_some());
        assert_ne!(bloom.unwrap().offset, tomb.unwrap().offset);
    }

    #[test]
    fn should_preserve_exact_handle_values() {
        // Arrange
        let exact_handle = BlockHandle {
            offset: 0xDEADBEEF,
            size: 0xCAFEBABE,
        };
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(b"filter.bloom", exact_handle);
        let meta_data = builder.finish();

        // Act
        let result = find_bloom_filter_handle(&meta_data).unwrap().unwrap();

        // Assert
        assert_eq!(result.offset, 0xDEADBEEF);
        assert_eq!(result.size, 0xCAFEBABE);
    }

    #[test]
    fn should_handle_zero_offset_size() {
        // Arrange
        let zero_handle = BlockHandle { offset: 0, size: 0 };
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(b"tombstones.range", zero_handle);
        let meta_data = builder.finish();

        // Act
        let result = find_range_tombstones_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(result.unwrap().offset, 0);
        assert_eq!(result.unwrap().size, 0);
    }

    #[test]
    fn should_handle_max_offset_size() {
        // Arrange
        let max_handle = BlockHandle {
            offset: u64::MAX,
            size: u64::MAX,
        };
        let mut builder = MetaIndexBuilder::new();
        let _ = builder.add_index_entry(b"filter.bloom", max_handle);
        let meta_data = builder.finish();

        // Act
        let result = find_bloom_filter_handle(&meta_data).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(result.unwrap().offset, u64::MAX);
        assert_eq!(result.unwrap().size, u64::MAX);
    }

    // --- should_skip_key tests ---

    #[test]
    fn should_not_skip_when_no_bloom_filter_and_no_tombstones() {
        // Arrange
        let bloom_filter = None;
        let range_tombstones = vec![];
        let key = b"test_key";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_skip_when_bloom_filter_says_absent() {
        // Arrange
        let mut bloom_filter = BloomFilter::new(100, 0.01);
        bloom_filter.add(b"different_key");
        let bloom_filter = Some(bloom_filter);
        let range_tombstones = vec![];
        let key = b"test_key";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_skip_when_bloom_filter_says_maybe_present() {
        // Arrange
        let mut bloom_filter = BloomFilter::new(100, 0.01);
        bloom_filter.add(b"test_key");
        let bloom_filter = Some(bloom_filter);
        let range_tombstones = vec![];
        let key = b"test_key";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_skip_when_covered_by_range_tombstone() {
        // Arrange
        let bloom_filter = None;
        let range_tombstones = vec![RangeTombstone {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            seq: 50,
        }];
        let key = b"m";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_skip_when_not_covered_by_range_tombstone() {
        // Arrange
        let bloom_filter = None;
        let range_tombstones = vec![RangeTombstone {
            start: b"a".to_vec(),
            end: b"m".to_vec(),
            seq: 50,
        }];
        let key = b"z";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_not_skip_when_tombstone_seq_greater_than_snapshot() {
        // Arrange
        let bloom_filter = None;
        let range_tombstones = vec![RangeTombstone {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            seq: 150,
        }];
        let key = b"m";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_skip_when_bloom_says_maybe_but_tombstone_covers() {
        // Arrange
        let mut bloom_filter = BloomFilter::new(100, 0.01);
        bloom_filter.add(b"test_key");
        let bloom_filter = Some(bloom_filter);
        let range_tombstones = vec![RangeTombstone {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            seq: 50,
        }];
        let key = b"test_key";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_handle_multiple_range_tombstones() {
        // Arrange
        let bloom_filter = None;
        let range_tombstones = vec![
            RangeTombstone {
                start: b"a".to_vec(),
                end: b"c".to_vec(),
                seq: 50,
            },
            RangeTombstone {
                start: b"m".to_vec(),
                end: b"p".to_vec(),
                seq: 60,
            },
        ];
        let key = b"n";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_handle_empty_key() {
        // Arrange
        let bloom_filter = None;
        let range_tombstones = vec![];
        let key = b"";
        let snapshot_seq = 100;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_handle_max_snapshot_seq() {
        // Arrange
        let bloom_filter = None;
        let range_tombstones = vec![RangeTombstone {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            seq: u64::MAX - 1,
        }];
        let key = b"m";
        let snapshot_seq = u64::MAX;

        // Act
        let result = should_skip_key(&bloom_filter, &range_tombstones, key, snapshot_seq);

        // Assert
        assert!(result);
    }

    // --- parse_key_at_offset tests ---

    #[test]
    fn should_parse_key_without_internal_decoding() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"test_key", b"value").unwrap();
        let data = builder.finish();
        let offset = 0;
        let limit = data.len();

        // Act
        let result = parse_key_at_offset(&data, offset, limit, false).unwrap();

        // Assert
        assert_eq!(result, b"test_key");
    }

    #[test]
    fn should_parse_key_with_internal_decoding_when_valid() {
        // Arrange
        let user_key = b"user_key";
        let seq = 100u64;
        let internal_key = crate::common::internal_key::encode_internal_key(user_key, seq, false);
        let mut builder = DataBlockBuilder::new(1);
        builder.add(&internal_key, b"value").unwrap();
        let data = builder.finish();
        let offset = 0;
        let limit = data.len();

        // Act
        let result = parse_key_at_offset(&data, offset, limit, true).unwrap();

        // Assert
        assert_eq!(result, b"user_key");
    }

    #[test]
    fn should_return_original_key_when_internal_decoding_fails() {
        // Arrange
        let key = b"not_internal_key";
        let mut builder = DataBlockBuilder::new(1);
        builder.add(key, b"value").unwrap();
        let data = builder.finish();
        let offset = 0;
        let limit = data.len();

        // Act
        let result = parse_key_at_offset(&data, offset, limit, true).unwrap();

        // Assert
        assert_eq!(result, b"not_internal_key");
    }

    #[test]
    fn should_handle_internal_key_with_tombstone_flag() {
        // Arrange
        let user_key = b"deleted_key";
        let seq = 50u64;
        let internal_key = crate::common::internal_key::encode_internal_key(user_key, seq, true);
        let mut builder = DataBlockBuilder::new(1);
        builder.add(&internal_key, b"").unwrap();
        let data = builder.finish();
        let offset = 0;
        let limit = data.len();

        // Act
        let result = parse_key_at_offset(&data, offset, limit, true).unwrap();

        // Assert
        assert_eq!(result, b"deleted_key");
    }

    #[test]
    fn should_fail_parsing_key_at_invalid_offset() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"key", b"value").unwrap();
        let data = builder.finish();
        let invalid_offset = data.len() + 100;
        let limit = data.len();

        // Act
        let result = parse_key_at_offset(&data, invalid_offset, limit, false);

        // Assert
        assert!(result.is_err());
    }

    // --- read_data_block_from_bytes tests ---

    #[test]
    fn should_read_valid_data_block() {
        // Arrange
        let mut builder = DataBlockBuilder::new(4);
        builder.add(b"key1", b"value1").unwrap();
        builder.add(b"key2", b"value2").unwrap();
        let data = builder.finish();
        let block = Block::new(data.clone(), BlockType::Data, CompressionType::None)
            .encode()
            .unwrap();

        // Act
        let result = read_data_block_from_bytes(&block).unwrap();

        // Assert
        assert_eq!(result.block_type, BlockType::Data);
        assert_eq!(result.data, data);
    }

    #[test]
    fn should_reject_non_data_block_type() {
        // Arrange - Create a block with invalid restart section
        // (not enough bytes for restart count)
        let invalid_data = vec![b'k', b'e', b'y', 0]; // Only 4 bytes, can't be valid

        // Act
        let result = read_data_block_from_bytes(&invalid_data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_invalid_block_data() {
        // Arrange
        let invalid_data = vec![0u8; 10];

        // Act
        let result = read_data_block_from_bytes(&invalid_data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_empty_block_data() {
        let result = read_data_block_from_bytes(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn should_read_data_block_with_single_entry() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"single_key", b"single_value").unwrap();
        let data = builder.finish();
        let block = Block::new(data, BlockType::Data, CompressionType::None)
            .encode()
            .unwrap();

        // Act
        let result = read_data_block_from_bytes(&block).unwrap();

        // Assert
        assert_eq!(result.block_type, BlockType::Data);
    }

    // --- search_data_block tests ---

    #[test]
    fn should_find_key_in_data_block() {
        // Arrange
        let mut builder = DataBlockBuilder::new(4);
        builder.add(b"apple", b"fruit").unwrap();
        builder.add(b"banana", b"yellow").unwrap();
        builder.add(b"cherry", b"red").unwrap();
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, b"banana", false).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(&result.unwrap()[..], b"yellow");
    }

    #[test]
    fn should_return_none_when_key_not_found() {
        // Arrange
        let mut builder = DataBlockBuilder::new(4);
        builder.add(b"apple", b"fruit").unwrap();
        builder.add(b"cherry", b"red").unwrap();
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, b"banana", false).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_handle_empty_data_block() {
        // Arrange
        let builder = DataBlockBuilder::new(1);
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, b"any_key", false).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_find_first_key_in_block() {
        // Arrange
        let mut builder = DataBlockBuilder::new(4);
        builder.add(b"aaa", b"first").unwrap();
        builder.add(b"bbb", b"second").unwrap();
        builder.add(b"ccc", b"third").unwrap();
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, b"aaa", false).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(&result.unwrap()[..], b"first");
    }

    #[test]
    fn should_find_last_key_in_block() {
        // Arrange
        let mut builder = DataBlockBuilder::new(4);
        builder.add(b"aaa", b"first").unwrap();
        builder.add(b"bbb", b"second").unwrap();
        builder.add(b"zzz", b"last").unwrap();
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, b"zzz", false).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(&result.unwrap()[..], b"last");
    }

    #[test]
    fn should_handle_single_entry_block_when_key_exists() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"only_key", b"only_value").unwrap();
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, b"only_key", false).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(&result.unwrap()[..], b"only_value");
    }

    #[test]
    fn should_handle_single_entry_block_when_key_missing() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"only_key", b"only_value").unwrap();
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, b"different_key", false).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_search_with_internal_keys_enabled() {
        // Arrange
        let user_key = b"user_key";
        let seq = 100u64;
        let internal_key = crate::common::internal_key::encode_internal_key(user_key, seq, false);
        let mut builder = DataBlockBuilder::new(4);
        builder.add(&internal_key, b"internal_value").unwrap();
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, user_key, true).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(&result.unwrap()[..], b"internal_value");
    }

    #[test]
    fn should_handle_data_smaller_than_minimum_size() {
        // Arrange
        let tiny_data = vec![0u8; 5];

        // Act
        let result = search_data_block(&tiny_data, b"key", false).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_handle_multiple_restart_points() {
        // Arrange
        let mut builder = DataBlockBuilder::new(2);
        builder.add(b"key1", b"value1").unwrap();
        builder.add(b"key2", b"value2").unwrap();
        builder.add(b"key3", b"value3").unwrap();
        builder.add(b"key4", b"value4").unwrap();
        let data = builder.finish();

        // Act
        let result = search_data_block(&data, b"key3", false).unwrap();

        // Assert
        assert!(result.is_some());
        assert_eq!(&result.unwrap()[..], b"value3");
    }

    // --- SstMetadata::from_bytes success path tests ---

    #[test]
    fn should_parse_minimal_valid_sst() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"key", b"value").unwrap();
        let data_block = builder.finish();
        let encoded_data = Block::new(data_block, BlockType::Data, CompressionType::None)
            .encode()
            .unwrap();

        let mut idx_builder = crate::sst::format::IndexBlockBuilder::new();
        idx_builder
            .add_index_entry(
                b"key",
                BlockHandle {
                    offset: 0,
                    size: encoded_data.len() as u64,
                },
            )
            .unwrap();
        let index_data = idx_builder.finish();
        let encoded_index = Block::new(index_data, BlockType::Index, CompressionType::None)
            .encode()
            .unwrap();

        let footer = Footer::new(
            BlockHandle {
                offset: encoded_data.len() as u64,
                size: encoded_index.len() as u64,
            },
            BlockHandle { offset: 0, size: 0 },
        );
        let encoded_footer = footer.encode();

        let mut sst_data = Vec::new();
        sst_data.extend_from_slice(&encoded_data);
        sst_data.extend_from_slice(&encoded_index);
        sst_data.extend_from_slice(&encoded_footer);

        // Act
        let result = SstMetadata::from_bytes(&sst_data).unwrap();

        // Assert
        assert_eq!(result.footer.index_handle.offset, encoded_data.len() as u64);
        assert_eq!(result.bloom_filter, None);
        assert!(result.range_tombstones.is_empty());
        assert!(!result.use_internal_keys);
    }

    #[test]
    fn should_parse_sst_with_bloom_filter() {
        // Arrange
        let mut bloom = BloomFilter::new(100, 0.01);
        bloom.add(b"test_key");
        let bloom_data = bloom.encode();
        let encoded_bloom = Block::new(bloom_data, BlockType::Filter, CompressionType::None)
            .encode()
            .unwrap();

        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"key", b"value").unwrap();
        let data_block = builder.finish();
        let encoded_data = Block::new(data_block, BlockType::Data, CompressionType::None)
            .encode()
            .unwrap();

        let mut idx_builder = crate::sst::format::IndexBlockBuilder::new();
        idx_builder
            .add_index_entry(
                b"key",
                BlockHandle {
                    offset: 0,
                    size: encoded_data.len() as u64,
                },
            )
            .unwrap();
        let index_data = idx_builder.finish();
        let encoded_index = Block::new(index_data, BlockType::Index, CompressionType::None)
            .encode()
            .unwrap();

        let mut meta_builder = MetaIndexBuilder::new();
        meta_builder
            .add_index_entry(
                b"filter.bloom",
                BlockHandle {
                    offset: encoded_data.len() as u64,
                    size: encoded_bloom.len() as u64,
                },
            )
            .unwrap();
        let meta_data = meta_builder.finish();
        let encoded_meta = Block::new(meta_data, BlockType::MetaIndex, CompressionType::None)
            .encode()
            .unwrap();

        let footer = Footer::new(
            BlockHandle {
                offset: (encoded_data.len() + encoded_bloom.len()) as u64,
                size: encoded_index.len() as u64,
            },
            BlockHandle {
                offset: (encoded_data.len() + encoded_bloom.len() + encoded_index.len()) as u64,
                size: encoded_meta.len() as u64,
            },
        );
        let encoded_footer = footer.encode();

        let mut sst_data = Vec::new();
        sst_data.extend_from_slice(&encoded_data);
        sst_data.extend_from_slice(&encoded_bloom);
        sst_data.extend_from_slice(&encoded_index);
        sst_data.extend_from_slice(&encoded_meta);
        sst_data.extend_from_slice(&encoded_footer);

        // Act
        let result = SstMetadata::from_bytes(&sst_data).unwrap();

        // Assert
        assert!(result.bloom_filter.is_some());
    }

    #[test]
    fn should_parse_sst_with_range_tombstones() {
        // Arrange
        let tombstones = vec![RangeTombstone {
            start: b"a".to_vec(),
            end: b"z".to_vec(),
            seq: 100,
        }];
        let tomb_data = crate::sst::range_tombstone::encode_range_tombstones(&tombstones).unwrap();
        let encoded_tomb = Block::new(tomb_data, BlockType::Filter, CompressionType::None)
            .encode()
            .unwrap();

        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"key", b"value").unwrap();
        let data_block = builder.finish();
        let encoded_data = Block::new(data_block, BlockType::Data, CompressionType::None)
            .encode()
            .unwrap();

        let mut idx_builder = crate::sst::format::IndexBlockBuilder::new();
        idx_builder
            .add_index_entry(
                b"key",
                BlockHandle {
                    offset: 0,
                    size: encoded_data.len() as u64,
                },
            )
            .unwrap();
        let index_data = idx_builder.finish();
        let encoded_index = Block::new(index_data, BlockType::Index, CompressionType::None)
            .encode()
            .unwrap();

        let mut meta_builder = MetaIndexBuilder::new();
        meta_builder
            .add_index_entry(
                b"tombstones.range",
                BlockHandle {
                    offset: encoded_data.len() as u64,
                    size: encoded_tomb.len() as u64,
                },
            )
            .unwrap();
        let meta_data = meta_builder.finish();
        let encoded_meta = Block::new(meta_data, BlockType::MetaIndex, CompressionType::None)
            .encode()
            .unwrap();

        let footer = Footer::new(
            BlockHandle {
                offset: (encoded_data.len() + encoded_tomb.len()) as u64,
                size: encoded_index.len() as u64,
            },
            BlockHandle {
                offset: (encoded_data.len() + encoded_tomb.len() + encoded_index.len()) as u64,
                size: encoded_meta.len() as u64,
            },
        );
        let encoded_footer = footer.encode();

        let mut sst_data = Vec::new();
        sst_data.extend_from_slice(&encoded_data);
        sst_data.extend_from_slice(&encoded_tomb);
        sst_data.extend_from_slice(&encoded_index);
        sst_data.extend_from_slice(&encoded_meta);
        sst_data.extend_from_slice(&encoded_footer);

        // Act
        let result = SstMetadata::from_bytes(&sst_data).unwrap();

        // Assert
        assert_eq!(result.range_tombstones.len(), 1);
        assert_eq!(result.range_tombstones[0].start, b"a");
        assert_eq!(result.range_tombstones[0].end, b"z");
    }

    #[test]
    fn should_detect_internal_keys_flag() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"key", b"value").unwrap();
        let data_block = builder.finish();
        let encoded_data = Block::new(data_block, BlockType::Data, CompressionType::None)
            .encode()
            .unwrap();

        let mut idx_builder = crate::sst::format::IndexBlockBuilder::new();
        idx_builder
            .add_index_entry(
                b"key",
                BlockHandle {
                    offset: 0,
                    size: encoded_data.len() as u64,
                },
            )
            .unwrap();
        let index_data = idx_builder.finish();
        let encoded_index = Block::new(index_data, BlockType::Index, CompressionType::None)
            .encode()
            .unwrap();

        let mut meta_builder = MetaIndexBuilder::new();
        meta_builder
            .add_index_entry(b"format.internal_keys", BlockHandle { offset: 0, size: 0 })
            .unwrap();
        let meta_data = meta_builder.finish();
        let encoded_meta = Block::new(meta_data, BlockType::MetaIndex, CompressionType::None)
            .encode()
            .unwrap();

        let footer = Footer::new(
            BlockHandle {
                offset: encoded_data.len() as u64,
                size: encoded_index.len() as u64,
            },
            BlockHandle {
                offset: (encoded_data.len() + encoded_index.len()) as u64,
                size: encoded_meta.len() as u64,
            },
        );
        let encoded_footer = footer.encode();

        let mut sst_data = Vec::new();
        sst_data.extend_from_slice(&encoded_data);
        sst_data.extend_from_slice(&encoded_index);
        sst_data.extend_from_slice(&encoded_meta);
        sst_data.extend_from_slice(&encoded_footer);

        // Act
        let result = SstMetadata::from_bytes(&sst_data).unwrap();

        // Assert
        assert!(result.use_internal_keys);
    }

    #[test]
    fn should_handle_empty_meta_index() {
        // Arrange
        let mut builder = DataBlockBuilder::new(1);
        builder.add(b"key", b"value").unwrap();
        let data_block = builder.finish();
        let encoded_data = Block::new(data_block, BlockType::Data, CompressionType::None)
            .encode()
            .unwrap();

        let mut idx_builder = crate::sst::format::IndexBlockBuilder::new();
        idx_builder
            .add_index_entry(
                b"key",
                BlockHandle {
                    offset: 0,
                    size: encoded_data.len() as u64,
                },
            )
            .unwrap();
        let index_data = idx_builder.finish();
        let encoded_index = Block::new(index_data, BlockType::Index, CompressionType::None)
            .encode()
            .unwrap();

        let footer = Footer::new(
            BlockHandle {
                offset: encoded_data.len() as u64,
                size: encoded_index.len() as u64,
            },
            BlockHandle { offset: 0, size: 0 },
        );
        let encoded_footer = footer.encode();

        let mut sst_data = Vec::new();
        sst_data.extend_from_slice(&encoded_data);
        sst_data.extend_from_slice(&encoded_index);
        sst_data.extend_from_slice(&encoded_footer);

        // Act
        let result = SstMetadata::from_bytes(&sst_data).unwrap();

        // Assert
        assert!(result.bloom_filter.is_none());
        assert!(result.range_tombstones.is_empty());
        assert!(!result.use_internal_keys);
    }
}
