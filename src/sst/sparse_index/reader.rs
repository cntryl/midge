//! Sparse index reader - fast binary search for block range lookup

use super::shared::{BlockRange, IndexEntry};
use crate::common::MidgeResult;

/// Sparse index reader
///
/// Binary searches sampled keys to find the block range for any key.
/// Reduces block I/O by narrowing search range.
pub struct SparseIndexReader {
    /// Sorted sampled entries
    entries: Vec<IndexEntry>,
}

impl SparseIndexReader {
    /// Create a reader from index entries
    pub fn new(entries: Vec<IndexEntry>) -> MidgeResult<Self> {
        // Ensure entries are sorted by key
        let mut sorted = entries;
        sorted.sort_by(|a, b| a.key.cmp(&b.key));

        Ok(Self { entries: sorted })
    }

    /// Find the block range containing a key
    ///
    /// Returns the range of blocks to search (start_block to end_block inclusive).
    /// For keys larger than all index entries, returns (last_block, usize::MAX).
    /// Caller is responsible for bounding end_block to the actual SST block count.
    pub fn find_block_range(&self, key: &[u8]) -> BlockRange {
        if self.entries.is_empty() {
            // No index, search all blocks (caller must provide actual count)
            return BlockRange::new(0, usize::MAX);
        }

        // Binary search for the rightmost entry where key >= entry.key
        let mut left = 0;
        let mut right = self.entries.len();

        while left < right {
            let mid = (left + right) / 2;
            if self.entries[mid].key.as_slice() <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // left is now the index of first entry where key < entry.key
        // So our key should be in the block of entry at (left - 1)
        if left == 0 {
            // Key is smaller than all entries, start from block 0
            BlockRange::new(0, self.entries[0].block_index)
        } else {
            // Key is between entries[left-1] and entries[left]
            let start_block = self.entries[left - 1].block_index;
            let end_block = if left < self.entries.len() {
                self.entries[left].block_index.saturating_sub(1)
            } else {
                // Key is larger than all entries, extend to unknown max
                // Caller must clamp this to actual block count
                usize::MAX
            };
            BlockRange::new(start_block, end_block)
        }
    }

    /// Get number of sampled entries
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::types::BlockHandle;

    #[test]
    fn should_find_block_range_for_key() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_100".to_vec(), handle, 0),
            IndexEntry::new(b"key_200".to_vec(), handle, 5),
            IndexEntry::new(b"key_300".to_vec(), handle, 10),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_150");

        // Assert - key_150 should be in block range starting from 0
        assert_eq!(range.start_block, 0);
        assert!(range.end_block < 5);
    }

    #[test]
    fn should_find_range_for_key_between_entries() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_100".to_vec(), handle, 0),
            IndexEntry::new(b"key_300".to_vec(), handle, 10),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_250");

        // Assert
        assert_eq!(range.start_block, 0);
    }

    #[test]
    fn should_handle_key_smaller_than_all_entries() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_500".to_vec(), handle, 5),
            IndexEntry::new(b"key_600".to_vec(), handle, 10),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_100");

        // Assert - should start from block 0
        assert_eq!(range.start_block, 0);
    }

    #[test]
    fn should_handle_key_larger_than_all_entries() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_100".to_vec(), handle, 0),
            IndexEntry::new(b"key_200".to_vec(), handle, 5),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_999");

        // Assert - should extend to end
        assert_eq!(range.start_block, 5);
    }

    #[test]
    fn should_handle_empty_index() {
        // Arrange
        let reader = SparseIndexReader::new(vec![]).unwrap();

        // Act
        let range = reader.find_block_range(b"any_key");

        // Assert
        assert_eq!(range.start_block, 0);
    }

    #[test]
    fn should_return_correct_block_counts() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_100".to_vec(), handle, 0),
            IndexEntry::new(b"key_200".to_vec(), handle, 5),
            IndexEntry::new(b"key_300".to_vec(), handle, 10),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_150");

        // Assert
        assert_eq!(range.block_count(), range.end_block - range.start_block + 1);
    }

    #[test]
    fn should_sort_unsorted_entries() {
        // Arrange - entries intentionally out of order
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_300".to_vec(), handle, 2),
            IndexEntry::new(b"key_100".to_vec(), handle, 0),
            IndexEntry::new(b"key_200".to_vec(), handle, 1),
        ];

        // Act
        let reader = SparseIndexReader::new(entries).unwrap();
        let range = reader.find_block_range(b"key_150");

        // Assert - should still find correct range
        assert_eq!(range.start_block, 0);
    }

    #[test]
    fn should_count_entries() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_100".to_vec(), handle, 0),
            IndexEntry::new(b"key_200".to_vec(), handle, 5),
            IndexEntry::new(b"key_300".to_vec(), handle, 10),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let count = reader.entry_count();

        // Assert
        assert_eq!(count, 3);
    }

    #[test]
    fn should_identify_empty_index() {
        // Arrange
        let reader = SparseIndexReader::new(vec![]).unwrap();

        // Act
        let is_empty = reader.is_empty();

        // Assert
        assert!(is_empty);
    }

    #[test]
    fn should_identify_non_empty_index() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![IndexEntry::new(b"key".to_vec(), handle, 0)];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let is_empty = reader.is_empty();

        // Assert
        assert!(!is_empty);
    }

    #[test]
    fn should_find_exact_match() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_100".to_vec(), handle, 0),
            IndexEntry::new(b"key_200".to_vec(), handle, 5),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_100");

        // Assert
        assert_eq!(range.start_block, 0);
    }

    #[test]
    fn should_find_range_for_single_entry() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![IndexEntry::new(b"key_100".to_vec(), handle, 5)];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act - search for key smaller than the single entry
        let range = reader.find_block_range(b"key_050");

        // Assert - key before all entries should start from block 0 to first entry's block
        assert_eq!(range.start_block, 0);
        assert_eq!(range.end_block, 5);
    }

    #[test]
    fn should_handle_large_index() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let mut entries = Vec::new();
        for i in 0..1000 {
            entries.push(IndexEntry::new(
                format!("key_{:04}", i * 10).into_bytes(),
                handle,
                i,
            ));
        }
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_4567");

        // Assert
        assert!(range.start_block < 500);
    }

    #[test]
    fn should_handle_binary_keys_in_search() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(vec![0u8, 0, 0], handle, 0),
            IndexEntry::new(vec![255u8, 255, 255], handle, 10),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(&[128u8, 128, 128]);

        // Assert
        assert_eq!(range.start_block, 0);
    }

    #[test]
    fn should_narrow_search_range_effectively() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let mut entries = Vec::new();
        for i in (0..100).step_by(10) {
            entries.push(IndexEntry::new(
                format!("key_{:04}", i).into_bytes(),
                handle,
                i,
            ));
        }
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_0055");

        // Assert - should narrow range to reasonable bounds
        assert!(range.block_count() <= 100);
    }

    #[test]
    fn should_handle_duplicate_keys_in_index() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key".to_vec(), handle, 0),
            IndexEntry::new(b"key".to_vec(), handle, 5),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key");

        // Assert - should still find a valid range
        assert!(range.start_block <= range.end_block || range.end_block == usize::MAX);
    }

    #[test]
    fn should_handle_boundary_search_before_first_entry() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"bbb".to_vec(), handle, 5),
            IndexEntry::new(b"ccc".to_vec(), handle, 10),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"aaa");

        // Assert
        assert_eq!(range.start_block, 0);
    }

    #[test]
    fn should_handle_boundary_search_after_last_entry() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"aaa".to_vec(), handle, 0),
            IndexEntry::new(b"bbb".to_vec(), handle, 5),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"zzz");

        // Assert
        assert_eq!(range.start_block, 5);
        assert_eq!(range.end_block, usize::MAX);
    }

    #[test]
    fn should_handle_search_between_high_blocks() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let entries = vec![
            IndexEntry::new(b"key_100".to_vec(), handle, 100),
            IndexEntry::new(b"key_200".to_vec(), handle, 200),
        ];
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_150");

        // Assert
        assert_eq!(range.start_block, 100);
    }

    #[test]
    fn should_find_correct_range_with_many_entries() {
        // Arrange
        let handle = BlockHandle::new(0, 100);
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push(IndexEntry::new(
                format!("key_{:03}", i).into_bytes(),
                handle,
                i,
            ));
        }
        let reader = SparseIndexReader::new(entries).unwrap();

        // Act
        let range = reader.find_block_range(b"key_050");

        // Assert
        assert_eq!(range.start_block, 50);
    }
}
