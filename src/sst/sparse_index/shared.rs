//! Shared sparse index types

use crate::sst::types::BlockHandle;

/// A single entry in the sparse index (sampled key position)
///
/// Maps a key to the block range containing it.
#[derive(Clone, Debug)]
pub struct IndexEntry {
    /// The sampled key (not all keys, sparse sample)
    pub key: Vec<u8>,
    /// Block containing this key
    pub block_handle: BlockHandle,
    /// Block index in the SST
    pub block_index: usize,
}

impl IndexEntry {
    /// Create a new index entry
    pub fn new(key: Vec<u8>, block_handle: BlockHandle, block_index: usize) -> Self {
        Self {
            key,
            block_handle,
            block_index,
        }
    }

    /// Get the size of this entry in bytes (for serialization estimates)
    pub fn size_bytes(&self) -> usize {
        4 + self.key.len() + 16 + 8 // size + key + handle(offset+size) + index
    }
}

/// Block range information from index lookup
#[derive(Clone, Copy, Debug)]
pub struct BlockRange {
    /// First block index to check (lower bound)
    pub start_block: usize,
    /// Last block index to check (upper bound, inclusive)
    pub end_block: usize,
}

impl BlockRange {
    /// Create a new block range
    pub fn new(start_block: usize, end_block: usize) -> Self {
        Self {
            start_block,
            end_block,
        }
    }

    /// Number of blocks in this range
    pub fn block_count(&self) -> usize {
        self.end_block - self.start_block + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_index_entry() {
        // Arrange
        let key = b"test_key".to_vec();
        let handle = BlockHandle::new(100, 500);

        // Act
        let entry = IndexEntry::new(key.clone(), handle, 5);

        // Assert
        assert_eq!(entry.key, key);
        assert_eq!(entry.block_handle.offset, 100);
        assert_eq!(entry.block_handle.size, 500);
        assert_eq!(entry.block_index, 5);
    }

    #[test]
    fn should_calculate_entry_size_bytes() {
        // Arrange
        let entry = IndexEntry::new(b"key".to_vec(), BlockHandle::new(0, 100), 0);

        // Act
        let size = entry.size_bytes();

        // Assert - 4 (len) + 3 (key) + 16 (handle) + 8 (index) = 31
        assert_eq!(size, 4 + 3 + 16 + 8);
    }

    #[test]
    fn should_handle_empty_key_in_entry() {
        // Arrange
        let entry = IndexEntry::new(b"".to_vec(), BlockHandle::new(0, 100), 0);

        // Act
        let size = entry.size_bytes();

        // Assert
        assert_eq!(size, 4 + 16 + 8);
    }

    #[test]
    fn should_handle_large_key_in_entry() {
        // Arrange
        let large_key = b"x".repeat(1000).to_vec();
        let entry = IndexEntry::new(large_key.clone(), BlockHandle::new(0, 100), 0);

        // Act
        let size = entry.size_bytes();

        // Assert
        assert_eq!(size, 4 + 1000 + 16 + 8);
    }

    #[test]
    fn should_clone_index_entry() {
        // Arrange
        let original = IndexEntry::new(b"key".to_vec(), BlockHandle::new(100, 200), 5);

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.key, original.key);
        assert_eq!(cloned.block_handle.offset, original.block_handle.offset);
        assert_eq!(cloned.block_index, original.block_index);
    }

    #[test]
    fn should_create_block_range() {
        // Arrange
        let start = 0;
        let end = 10;

        // Act
        let range = BlockRange::new(start, end);

        // Assert
        assert_eq!(range.start_block, 0);
        assert_eq!(range.end_block, 10);
    }

    #[test]
    fn should_calculate_block_count() {
        // Arrange
        let range = BlockRange::new(5, 15);

        // Act
        let count = range.block_count();

        // Assert
        assert_eq!(count, 11); // 15 - 5 + 1
    }

    #[test]
    fn should_handle_single_block_range() {
        // Arrange
        let range = BlockRange::new(5, 5);

        // Act
        let count = range.block_count();

        // Assert
        assert_eq!(count, 1);
    }

    #[test]
    fn should_handle_zero_block_range() {
        // Arrange
        let range = BlockRange::new(0, 0);

        // Act
        let count = range.block_count();

        // Assert
        assert_eq!(count, 1);
    }

    #[test]
    fn should_clone_block_range() {
        // Arrange
        let original = BlockRange::new(5, 15);

        // Act
        let cloned = original;

        // Assert
        assert_eq!(cloned.start_block, original.start_block);
        assert_eq!(cloned.end_block, original.end_block);
    }

    #[test]
    fn should_handle_large_block_range() {
        // Arrange
        let range = BlockRange::new(0, 1000000);

        // Act
        let count = range.block_count();

        // Assert
        assert_eq!(count, 1000001);
    }

    #[test]
    fn should_debug_format_index_entry() {
        // Arrange
        let entry = IndexEntry::new(b"key".to_vec(), BlockHandle::new(100, 200), 5);

        // Act
        let formatted = format!("{:?}", entry);

        // Assert
        assert!(formatted.contains("IndexEntry"));
    }

    #[test]
    fn should_debug_format_block_range() {
        // Arrange
        let range = BlockRange::new(5, 15);

        // Act
        let formatted = format!("{:?}", range);

        // Assert
        assert!(formatted.contains("BlockRange"));
    }
}
