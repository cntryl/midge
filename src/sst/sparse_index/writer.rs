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
    pub fn new() -> Self {
        Self::with_sample_rate(16)
    }

    /// Create a new sparse index writer with custom sample rate
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
        if self.key_count % self.sample_rate == 0 {
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
    pub fn finish(self) -> Vec<IndexEntry> {
        self.entries
    }

    /// Get current number of sampled entries
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Get total keys seen
    pub fn key_count(&self) -> usize {
        self.key_count
    }

    /// Estimate serialized size in bytes
    pub fn size_bytes(&self) -> usize {
        4 + self.entries.iter().map(|e| e.size_bytes()).sum::<usize>()
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
            writer.record_key(format!("key_{:03}", i).into_bytes(), handle.clone());
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
        writer.record_key(b"key_0".to_vec(), handle.clone());
        writer.record_key(b"key_1".to_vec(), handle.clone());
        writer.next_block();
        writer.record_key(b"key_2".to_vec(), handle.clone());
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
            writer.record_key(format!("key_{:03}", i).into_bytes(), handle.clone());
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
            writer.record_key(format!("key_{:03}", i).into_bytes(), handle.clone());
        }

        // Assert
        assert_eq!(writer.key_count(), 100);
        assert!(writer.entry_count() > 0);
        assert!(writer.entry_count() < 100);
    }
}
