//! Block-level bloom filters for fine-grained negative lookups
//!
//! This module implements the second tier of Midge's two-tier bloom architecture:
//! - SST-level bloom (coarse gate) — eliminates most SSTs
//! - Block-level bloom (fine gate) — eliminates useless block reads
//!
//! Block blooms are stored in a dedicated filter block within the SST and checked
//! after sparse index selection but before actual block I/O.

use super::writer::{BloomFilterOps, BloomTestResult};
use super::{BloomReader, BloomWriter};
use crate::common::MidgeResult;

/// Container for block-level bloom filters
///
/// Stores one bloom per data block, with metadata to map block index → bloom offset.
/// Serialized as: [`num_blocks`: u32] [offsets: u32[]] [`bloom_data`: variable]
#[derive(Debug, Clone)]
pub struct BlockBloomFilter {
    /// Number of data blocks
    num_blocks: usize,
    /// Offset of each bloom filter in the serialized data
    offsets: Vec<u32>,
    /// Serialized bloom data (concatenated)
    bloom_data: Vec<u8>,
}

impl BlockBloomFilter {
    /// Create a new block bloom filter container
    #[must_use]
    pub fn new() -> Self {
        Self {
            num_blocks: 0,
            offsets: Vec::new(),
            bloom_data: Vec::new(),
        }
    }

    /// Add a bloom filter for a data block
    pub fn add_block_bloom(&mut self, bloom: &BloomWriter) {
        let serialized = bloom.serialize();
        let offset = u32::try_from(self.bloom_data.len()).unwrap_or(u32::MAX);

        self.offsets.push(offset);
        self.bloom_data.extend_from_slice(&serialized);
        self.num_blocks += 1;
    }

    /// Serialize the entire block bloom structure
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // Write header: [num_blocks: u32]
        result.extend_from_slice(&u32::try_from(self.num_blocks).unwrap_or(u32::MAX).to_le_bytes());

        // Write offsets array: [offset0, offset1, ..., offsetN]
        for &offset in &self.offsets {
            result.extend_from_slice(&offset.to_le_bytes());
        }

        // Write bloom data
        result.extend_from_slice(&self.bloom_data);

        result
    }

    /// Deserialize from bytes
    ///
    /// # Errors
    ///
    /// Returns `Corruption` if the serialized data is truncated or malformed.
    pub fn deserialize(data: &[u8]) -> MidgeResult<Self> {
        if data.len() < 4 {
            return Err(crate::common::MidgeError::Corruption(
                "Block bloom data too short".into(),
            ));
        }

        let num_blocks = usize::try_from(u32::from_le_bytes([data[0], data[1], data[2], data[3]]))
            .unwrap_or(usize::MAX);
        let expected_header_size = 4 + (num_blocks * 4);

        if data.len() < expected_header_size {
            return Err(crate::common::MidgeError::Corruption(
                "Block bloom header truncated".into(),
            ));
        }

        let mut offsets = Vec::with_capacity(num_blocks);
        for i in 0..num_blocks {
            let offset_pos = 4 + (i * 4);
            let offset = u32::from_le_bytes([
                data[offset_pos],
                data[offset_pos + 1],
                data[offset_pos + 2],
                data[offset_pos + 3],
            ]);
            offsets.push(offset);
        }

        let bloom_data = data[expected_header_size..].to_vec();

        Ok(Self {
            num_blocks,
            offsets,
            bloom_data,
        })
    }

    /// Check if a key might be in the specified block (fail-safe on corruption)
    #[must_use]
    pub fn might_contain_in_block(&self, block_idx: usize, key: &[u8]) -> BloomTestResult {
        if block_idx >= self.num_blocks {
            // Fail-safe: return MightBePresent for out-of-bounds index
            return BloomTestResult::MightBePresent;
        }

        // Find the bloom data for this block
        let start_offset = self.offsets[block_idx] as usize;
        let end_offset = if block_idx + 1 < self.num_blocks {
            self.offsets[block_idx + 1] as usize
        } else {
            self.bloom_data.len()
        };

        if start_offset >= self.bloom_data.len() || end_offset > self.bloom_data.len() {
            // Fail-safe: corrupted offset metadata, assume key might be present
            // INVARIANT: Caller should log this corruption for observability
            return BloomTestResult::MightBePresent;
        }

        let bloom_bytes = &self.bloom_data[start_offset..end_offset];

        // Deserialize and check bloom (fail-safe on corruption)
        match BloomReader::deserialize(bloom_bytes) {
            Ok(bloom) => bloom.contains(key),
            Err(_corruption_error) => {
                // Fail-safe: corrupted bloom data (k out of range, size mismatch, etc.)
                // INVARIANT: Error details available at {_corruption_error} for logging
                BloomTestResult::MightBePresent
            }
        }
    }

    /// Get the number of blocks
    #[must_use]
    pub fn num_blocks(&self) -> usize {
        self.num_blocks
    }

    /// Get the size in bytes
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        4 + (self.offsets.len() * 4) + self.bloom_data.len()
    }
}

impl Default for BlockBloomFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_empty_block_bloom() {
        // Arrange

        // Act
        let filter = BlockBloomFilter::new();

        // Assert
        assert_eq!(filter.num_blocks(), 0);
    }

    #[test]
    fn should_add_single_block_bloom() {
        // Arrange
        let mut filter = BlockBloomFilter::new();
        let mut bloom = BloomWriter::with_defaults(100);
        bloom.insert(b"key1");
        bloom.insert(b"key2");

        // Act
        filter.add_block_bloom(&bloom);

        // Assert
        assert_eq!(filter.num_blocks(), 1);
    }

    #[test]
    fn should_add_multiple_block_blooms() {
        // Arrange
        let mut filter = BlockBloomFilter::new();

        // Act
        for i in 0..5 {
            let mut bloom = BloomWriter::with_defaults(50);
            bloom.insert(format!("block{i}_key1").as_bytes());
            bloom.insert(format!("block{i}_key2").as_bytes());
            filter.add_block_bloom(&bloom);
        }

        // Assert
        assert_eq!(filter.num_blocks(), 5);
    }

    #[test]
    fn should_roundtrip_serialize_deserialize() -> MidgeResult<()> {
        // Arrange
        let mut filter = BlockBloomFilter::new();
        for i in 0..3 {
            let mut bloom = BloomWriter::with_defaults(100);
            bloom.insert(format!("key{i}").as_bytes());
            filter.add_block_bloom(&bloom);
        }

        // Act
        let serialized = filter.serialize();
        let deserialized = BlockBloomFilter::deserialize(&serialized)?;

        // Assert
        assert_eq!(deserialized.num_blocks(), 3);
        Ok(())
    }

    #[test]
    fn should_return_might_be_present_for_key_in_own_block() {
        // Arrange
        let mut filter = BlockBloomFilter::new();

        // Block 0: contains "key0"
        let mut bloom0 = BloomWriter::with_defaults(100);
        bloom0.insert(b"key0");
        filter.add_block_bloom(&bloom0);

        // Block 1: contains "key1"
        let mut bloom1 = BloomWriter::with_defaults(100);
        bloom1.insert(b"key1");
        filter.add_block_bloom(&bloom1);

        // Act
        let r0_key0 = filter.might_contain_in_block(0, b"key0");
        let r1_key1 = filter.might_contain_in_block(1, b"key1");

        // Assert
        assert_eq!(r0_key0, BloomTestResult::MightBePresent);
        assert_eq!(r1_key1, BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_return_definitely_not_present_for_key_in_other_block() {
        // Arrange
        let mut filter = BlockBloomFilter::new();

        // Block 0: contains "key0"
        let mut bloom0 = BloomWriter::with_defaults(100);
        bloom0.insert(b"key0");
        filter.add_block_bloom(&bloom0);

        // Block 1: contains "key1"
        let mut bloom1 = BloomWriter::with_defaults(100);
        bloom1.insert(b"key1");
        filter.add_block_bloom(&bloom1);

        // Act
        let r0_key1 = filter.might_contain_in_block(0, b"key1");
        let r1_key0 = filter.might_contain_in_block(1, b"key0");

        // Assert
        assert_eq!(r0_key1, BloomTestResult::DefinitelyNotPresent);
        assert_eq!(r1_key0, BloomTestResult::DefinitelyNotPresent);
    }

    #[test]
    fn should_return_definitely_not_present_for_absent_keys() {
        // Arrange
        let mut filter = BlockBloomFilter::new();
        let mut bloom = BloomWriter::with_defaults(100);
        bloom.insert(b"present_key");
        filter.add_block_bloom(&bloom);

        // Act
        let result = filter.might_contain_in_block(0, b"absent_key");

        // Assert
        assert_eq!(result, BloomTestResult::DefinitelyNotPresent);
    }

    #[test]
    fn should_handle_invalid_block_index_gracefully() {
        // Arrange
        let mut filter = BlockBloomFilter::new();
        let mut bloom = BloomWriter::with_defaults(100);
        bloom.insert(b"key");
        filter.add_block_bloom(&bloom);

        // Act - query beyond valid range
        let result = filter.might_contain_in_block(999, b"key");

        // Assert - should not crash, conservatively return "might be present"
        assert_eq!(result, BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_handle_empty_bloom_data() -> MidgeResult<()> {
        // Arrange
        let filter = BlockBloomFilter::new();

        // Act
        let serialized = filter.serialize();
        let deserialized = BlockBloomFilter::deserialize(&serialized)?;

        // Assert
        assert_eq!(deserialized.num_blocks(), 0);
        Ok(())
    }

    #[test]
    fn should_reject_corrupted_data() {
        // Arrange
        let bad_data = vec![0xFF; 3]; // Too short

        // Act
        let result = BlockBloomFilter::deserialize(&bad_data);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_calculate_size_correctly() {
        // Arrange
        let mut filter = BlockBloomFilter::new();
        for _ in 0..3 {
            let mut bloom = BloomWriter::with_defaults(100);
            bloom.insert(b"test_key");
            filter.add_block_bloom(&bloom);
        }

        // Act
        let reported_size = filter.size_bytes();
        let actual_size = filter.serialize().len();

        // Assert
        assert_eq!(reported_size, actual_size);
    }

    #[test]
    fn should_preserve_bloom_accuracy_after_serialization() -> MidgeResult<()> {
        // Arrange
        let mut filter = BlockBloomFilter::new();
        let mut bloom = BloomWriter::with_defaults(1000);

        // Insert 100 keys
        for i in 0..100 {
            bloom.insert(format!("key{i:04}").as_bytes());
        }
        filter.add_block_bloom(&bloom);

        // Act
        let serialized = filter.serialize();
        let deserialized = BlockBloomFilter::deserialize(&serialized)?;

        // Assert - all inserted keys should be "might be present"
        for i in 0..100 {
            let result = deserialized.might_contain_in_block(0, format!("key{i:04}").as_bytes());
            assert_eq!(result, BloomTestResult::MightBePresent);
        }

        // Assert - most non-inserted keys should be "definitely not present"
        let mut rejections = 0;
        for i in 1000..1100 {
            let result = deserialized.might_contain_in_block(0, format!("key{i:04}").as_bytes());
            if result == BloomTestResult::DefinitelyNotPresent {
                rejections += 1;
            }
        }

        // Expect >90% rejection rate for absent keys
        assert!(rejections > 90);
        Ok(())
    }

    #[test]
    fn should_handle_corrupted_bloom_in_block() {
        // Arrange - Create valid structure then corrupt internal bloom
        let mut filter = BlockBloomFilter::new();
        let mut bloom = BloomWriter::with_defaults(100);
        bloom.insert(b"test_key");
        filter.add_block_bloom(&bloom);

        // Serialize and corrupt the inner bloom data
        let mut serialized = filter.serialize();

        // Find the bloom data section and corrupt k byte
        // Format: \[num_blocks: u32\]\[offsets...\]\[bloom_data...\]
        // num_blocks = 1, so header is 4 + 4 = 8 bytes
        // Bloom data starts at offset 8
        // Inner bloom format: \[num_bits\]\[key_count\]\[k\]\[bits...\]
        // So k is at position 8 + 8 = 16
        if serialized.len() > 16 {
            serialized[16] = 99; // Set k to invalid value
        }

        // Act - deserialize and query
        let result = BlockBloomFilter::deserialize(&serialized);
        assert!(result.is_ok());
        let corrupted_filter = result.unwrap();

        // Assert - query should not panic, should conservatively return MightBePresent
        let query_result = corrupted_filter.might_contain_in_block(0, b"any_key");
        assert_eq!(query_result, BloomTestResult::MightBePresent);
    }

    #[test]
    fn should_handle_offset_corruption_gracefully() {
        // Arrange - Create structure then corrupt offset metadata
        let mut filter = BlockBloomFilter::new();
        let mut bloom = BloomWriter::with_defaults(100);
        bloom.insert(b"test_key");
        filter.add_block_bloom(&bloom);

        // Serialize and corrupt the offset
        let mut serialized = filter.serialize();

        // Format: [num_blocks: u32][offset0: u32][bloom_data...]
        // offset0 is at bytes 4-8
        // Set offset to point beyond bloom_data
        if serialized.len() > 8 {
            serialized[4] = 255;
            serialized[5] = 255;
            serialized[6] = 255;
            serialized[7] = 255;
        }

        // Act - deserialize and query
        let result = BlockBloomFilter::deserialize(&serialized);
        assert!(result.is_ok());
        let corrupted_filter = result.unwrap();

        // Assert - query should not panic, should conservatively return MightBePresent
        let query_result = corrupted_filter.might_contain_in_block(0, b"any_key");
        assert_eq!(query_result, BloomTestResult::MightBePresent);
    }
}
