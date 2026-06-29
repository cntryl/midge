//! Sparse index writer - samples keys during SST creation

use super::shared::IndexEntry;
use crate::sst::types::BlockHandle;

/// Sparse index writer
///
/// Samples every Nth key to build a fast lookup index.
/// Default: sample every 16 keys.
pub struct SparseIndexWriter {
    /// Sampled index entries
    entries: Vec<IndexEntry>,
    /// Sample rate (1 in every N keys)
    sample_rate: usize,
    /// Total keys seen so far
    key_count: usize,
    /// Current block index
    current_block: usize,
}

impl SparseIndexWriter {
    /// Create a new sparse index writer with default sample rate (16)
    #[must_use]
    pub fn new() -> Self {
        Self::with_sample_rate(16)
    }

    /// Create a new sparse index writer with custom sample rate
    #[must_use]
    pub fn with_sample_rate(sample_rate: usize) -> Self {
        Self {
            entries: Vec::new(),
            sample_rate: sample_rate.max(1),
            key_count: 0,
            current_block: 0,
        }
    }

    /// Record a key (add to index if it's a sample point)
    pub fn record_key(&mut self, key: Vec<u8>, block_handle: BlockHandle) {
        // Sample every Nth key
        if self.key_count.is_multiple_of(self.sample_rate) {
            self.entries
                .push(IndexEntry::new(key, block_handle, self.current_block));
        }
        self.key_count += 1;
    }

    /// Note that we're moving to a new block
    pub fn next_block(&mut self) {
        self.current_block += 1;
    }

    /// Finish writing and return the index entries
    #[must_use]
    pub fn finish(self) -> Vec<IndexEntry> {
        self.entries
    }

    /// Get current number of sampled entries
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Get total keys seen
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    /// Estimate serialized size in bytes
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        4 + self
            .entries
            .iter()
            .map(super::shared::IndexEntry::size_bytes)
            .sum::<usize>()
    }
}

impl Default for SparseIndexWriter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_sample_every_nth_key() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(4);
        let handle = BlockHandle::new(0, 100);

        // Act
        for i in 0..12 {
            writer.record_key(format!("key_{i:03}").into_bytes(), handle);
        }
        let entries = writer.finish();

        // Assert - should sample at indices 0, 4, 8
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, b"key_000".to_vec());
        assert_eq!(entries[1].key, b"key_004".to_vec());
        assert_eq!(entries[2].key, b"key_008".to_vec());
    }

    #[test]
    fn should_track_block_transitions() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(2);
        let handle = BlockHandle::new(0, 100);

        // Act
        writer.record_key(b"key_0".to_vec(), handle);
        writer.record_key(b"key_1".to_vec(), handle);
        writer.next_block();
        writer.record_key(b"key_2".to_vec(), handle);
        writer.record_key(b"key_3".to_vec(), handle);
        let entries = writer.finish();

        // Assert - should sample at indices 0 and 2 (sample_rate=2)
        // key_0 is at block 0, key_2 is at block 1
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].block_index, 0);
        assert_eq!(entries[1].block_index, 1);
    }

    #[test]
    fn should_estimate_serialization_size() {
        // Arrange
        let mut writer = SparseIndexWriter::new();
        let handle = BlockHandle::new(0, 100);

        // Act
        for i in 0..32 {
            writer.record_key(format!("key_{i:03}").into_bytes(), handle);
        }

        // Assert - should estimate reasonable size
        let size = writer.size_bytes();
        assert!(size > 0);
        assert!(size < 5000); // Rough upper bound
    }

    #[test]
    fn should_track_key_count() {
        // Arrange
        let mut writer = SparseIndexWriter::new();
        let handle = BlockHandle::new(0, 100);

        // Act
        for i in 0..100 {
            writer.record_key(format!("key_{i:03}").into_bytes(), handle);
        }

        // Assert
        assert_eq!(writer.key_count(), 100);
        assert!(writer.entry_count() > 0);
        assert!(writer.entry_count() < 100);
    }

    #[test]
    fn should_create_with_default_sample_rate() {
        // Arrange

        // Act
        let writer = SparseIndexWriter::new();

        // Assert
        assert_eq!(writer.sample_rate, 16);
        assert_eq!(writer.entry_count(), 0);
    }

    #[test]
    fn should_create_with_custom_sample_rate() {
        // Arrange
        let sample_rate = 8;

        // Act
        let writer = SparseIndexWriter::with_sample_rate(sample_rate);

        // Assert
        assert_eq!(writer.sample_rate, 8);
    }

    #[test]
    fn should_enforce_minimum_sample_rate() {
        // Arrange
        let sample_rate = 0;

        // Act
        let writer = SparseIndexWriter::with_sample_rate(sample_rate);

        // Assert - should enforce minimum of 1
        assert_eq!(writer.sample_rate, 1);
    }

    #[test]
    fn should_sample_with_sample_rate_one() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(1);
        let handle = BlockHandle::new(0, 100);

        // Act
        for i in 0..5 {
            writer.record_key(format!("key_{i}").into_bytes(), handle);
        }
        let entries = writer.finish();

        // Assert - should sample every key when rate is 1
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn should_handle_empty_writer() {
        // Arrange
        let writer = SparseIndexWriter::new();

        // Act
        let entries = writer.finish();

        // Assert
        assert_eq!(entries.len(), 0);
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn should_calculate_correct_size_bytes() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(1);
        let handle = BlockHandle::new(0, 100);

        // Act
        writer.record_key(b"key1".to_vec(), handle);
        writer.record_key(b"key2".to_vec(), handle);
        let size_before_finish = writer.size_bytes();
        let entries = writer.finish();

        // Assert - size should match sum of entry sizes
        let expected = 4 + entries
            .iter()
            .map(super::super::shared::IndexEntry::size_bytes)
            .sum::<usize>();
        assert_eq!(size_before_finish, expected);
    }

    #[test]
    fn should_increment_key_count_for_each_key() {
        // Arrange
        let mut writer = SparseIndexWriter::new();
        let handle = BlockHandle::new(0, 100);

        // Act
        for i in 0..50 {
            assert_eq!(writer.key_count(), i);
            writer.record_key(format!("key_{i}").into_bytes(), handle);
        }

        // Assert
        assert_eq!(writer.key_count(), 50);
    }

    #[test]
    fn should_track_multiple_block_transitions() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(3);
        let handle = BlockHandle::new(0, 100);

        // Act
        writer.record_key(b"k0".to_vec(), handle); // index 0, sampled, block 0
        writer.record_key(b"k1".to_vec(), handle); // index 1
        writer.record_key(b"k2".to_vec(), handle); // index 2
        writer.next_block(); // block 1
        writer.record_key(b"k3".to_vec(), handle); // index 3, sampled, block 1
        writer.record_key(b"k4".to_vec(), handle); // index 4
        writer.record_key(b"k5".to_vec(), handle); // index 5
        writer.next_block(); // block 2
        writer.record_key(b"k6".to_vec(), handle); // index 6, sampled, block 2
        let entries = writer.finish();

        // Assert
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].block_index, 0);
        assert_eq!(entries[1].block_index, 1);
        assert_eq!(entries[2].block_index, 2);
    }

    #[test]
    fn should_create_via_default() {
        // Arrange

        // Act
        let writer = SparseIndexWriter::default();

        // Assert
        assert_eq!(writer.sample_rate, 16);
        assert_eq!(writer.key_count(), 0);
    }

    #[test]
    fn should_handle_large_sample_rate() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(1000);
        let handle = BlockHandle::new(0, 100);

        // Act
        for i in 0..500 {
            writer.record_key(format!("key_{i}").into_bytes(), handle);
        }
        let entries = writer.finish();

        // Assert - should have very few entries with large sample rate
        assert!(entries.len() <= 1);
    }

    #[test]
    fn should_preserve_block_handle_in_entries() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(1);
        let handle1 = BlockHandle::new(0, 100);
        let handle2 = BlockHandle::new(200, 150);

        // Act
        writer.record_key(b"key1".to_vec(), handle1);
        writer.record_key(b"key2".to_vec(), handle2);
        let entries = writer.finish();

        // Assert
        assert_eq!(entries[0].block_handle.offset, 0);
        assert_eq!(entries[0].block_handle.size, 100);
        assert_eq!(entries[1].block_handle.offset, 200);
        assert_eq!(entries[1].block_handle.size, 150);
    }

    #[test]
    fn should_sample_correctly_with_large_dataset() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(16);
        let handle = BlockHandle::new(0, 100);

        // Act
        for i in 0..1000 {
            writer.record_key(format!("key_{i:04}").into_bytes(), handle);
        }
        let entries = writer.finish();

        // Assert - 1000 keys with rate 16 should give ~62-63 entries
        assert!(entries.len() >= 60 && entries.len() <= 65);
    }

    #[test]
    fn should_handle_binary_keys() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(2);
        let handle = BlockHandle::new(0, 100);

        // Act
        writer.record_key(vec![0u8, 1, 2, 3, 255], handle);
        writer.record_key(vec![10u8, 11, 12], handle);
        let entries = writer.finish();

        // Assert
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, vec![0u8, 1, 2, 3, 255]);
    }

    #[test]
    fn should_maintain_entry_order() {
        // Arrange
        let mut writer = SparseIndexWriter::with_sample_rate(2);
        let handle = BlockHandle::new(0, 100);

        // Act
        for i in 0..8 {
            writer.record_key(format!("key_{i}").into_bytes(), handle);
        }
        let entries = writer.finish();

        // Assert - entries should be in insertion order
        assert_eq!(entries[0].key, b"key_0".to_vec());
        assert_eq!(entries[1].key, b"key_2".to_vec());
        assert_eq!(entries[2].key, b"key_4".to_vec());
        assert_eq!(entries[3].key, b"key_6".to_vec());
    }
}
