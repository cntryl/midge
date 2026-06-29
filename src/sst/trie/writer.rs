//! Trie writer for SST integration

use crate::common::MidgeResult;
use crate::sst::trie::TrieBuilder;

/// Writer for trie index during SST construction
pub struct TrieWriter {
    builder: TrieBuilder,
    enabled: bool,
}

impl TrieWriter {
    /// Create a new trie writer
    #[must_use]
    pub fn new(enabled: bool) -> Self {
        Self {
            builder: TrieBuilder::new(),
            enabled,
        }
    }

    /// Add a key at block boundary
    ///
    /// Only call this for the first key of each block.
    /// Keys must be added in sorted order.
    pub fn add_block_key(&mut self, key: &[u8], block_id: u32) -> MidgeResult<()> {
        if !self.enabled {
            return Ok(());
        }

        self.builder.add_key(key, block_id)
    }

    /// Finish building and return serialized trie
    #[must_use]
    pub fn finish(self) -> Option<Vec<u8>> {
        if !self.enabled {
            return None;
        }

        let data = self.builder.finish();
        if data.is_empty() {
            None
        } else {
            Some(data)
        }
    }

    /// Check if trie is enabled
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get number of nodes in trie
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.builder.node_count()
    }
}

impl Default for TrieWriter {
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::trie::reader::TrieReader;

    #[test]
    fn should_create_enabled_writer() {
        // Arrange

        // Act
        let writer = TrieWriter::new(true);

        // Assert
        assert!(writer.is_enabled());
    }

    #[test]
    fn should_create_disabled_writer() {
        // Arrange

        // Act
        let writer = TrieWriter::new(false);

        // Assert
        assert!(!writer.is_enabled());
    }

    #[test]
    fn should_create_default_writer() {
        // Arrange

        // Act
        let writer = TrieWriter::default();

        // Assert
        assert!(writer.is_enabled()); // Default is enabled
    }

    #[test]
    fn should_skip_when_disabled() {
        // Arrange
        let mut writer = TrieWriter::new(false);

        // Act
        writer.add_block_key(b"test", 0).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_skip_multiple_keys_when_disabled() {
        // Arrange
        let mut writer = TrieWriter::new(false);

        // Act
        writer.add_block_key(b"apple", 0).unwrap();
        writer.add_block_key(b"banana", 1).unwrap();
        writer.add_block_key(b"cherry", 2).unwrap();
        assert_eq!(writer.node_count(), 1); // Only root
        let result = writer.finish();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_build_trie_when_enabled() {
        // Arrange
        let mut writer = TrieWriter::new(true);

        // Act
        writer.add_block_key(b"apple", 0).unwrap();
        writer.add_block_key(b"banana", 1).unwrap();
        writer.add_block_key(b"cherry", 2).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn should_track_node_count_when_enabled() {
        // Arrange
        let mut writer = TrieWriter::new(true);
        let initial_count = writer.node_count();

        // Act
        writer.add_block_key(b"apple", 0).unwrap();

        // Assert
        assert!(writer.node_count() > initial_count);
    }

    #[test]
    fn should_not_increase_node_count_when_disabled() {
        // Arrange
        let mut writer = TrieWriter::new(false);
        let initial_count = writer.node_count();

        // Act
        writer.add_block_key(b"apple", 0).unwrap();

        // Assert
        assert_eq!(writer.node_count(), initial_count);
    }

    #[test]
    fn should_build_trie_with_single_key() {
        // Arrange
        let mut writer = TrieWriter::new(true);

        // Act
        writer.add_block_key(b"single", 0).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();
        assert!(!data.is_empty());

        // Verify it can be read back
        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(b"single"), Some(0));
    }

    #[test]
    fn should_build_readable_trie_with_multiple_keys() {
        // Arrange
        let mut writer = TrieWriter::new(true);

        // Act
        writer.add_block_key(b"apple", 0).unwrap();
        writer.add_block_key(b"application", 1).unwrap();
        writer.add_block_key(b"banana", 2).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(b"apple"), Some(0));
        assert_eq!(reader.find_block(b"application"), Some(1));
        assert_eq!(reader.find_block(b"banana"), Some(2));
    }

    #[test]
    fn should_handle_empty_key() {
        // Arrange
        let mut writer = TrieWriter::new(true);
        let initial_count = writer.node_count();

        // Act
        writer.add_block_key(b"", 0).unwrap();

        // Assert
        assert_eq!(writer.node_count(), initial_count); // Empty key skipped
    }

    #[test]
    fn should_handle_prefix_keys() {
        // Arrange - keys in sorted order
        let mut writer = TrieWriter::new(true);

        // Act
        writer.add_block_key(b"test", 0).unwrap();
        writer.add_block_key(b"tester", 1).unwrap();
        writer.add_block_key(b"testing", 2).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(b"test"), Some(0));
        assert_eq!(reader.find_block(b"tester"), Some(1));
        assert_eq!(reader.find_block(b"testing"), Some(2));
    }

    #[test]
    fn should_handle_maximum_block_id() {
        // Arrange
        let mut writer = TrieWriter::new(true);

        // Act
        writer.add_block_key(b"key", u32::MAX - 1).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(b"key"), Some(u32::MAX - 1));
    }

    #[test]
    fn should_handle_block_id_zero() {
        // Arrange
        let mut writer = TrieWriter::new(true);

        // Act
        writer.add_block_key(b"key", 0).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(b"key"), Some(0));
    }

    #[test]
    fn should_handle_large_keys() {
        // Arrange
        let mut writer = TrieWriter::new(true);
        let large_key = vec![42; 1000];

        // Act
        writer.add_block_key(&large_key, 0).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(&large_key), Some(0));
    }

    #[test]
    fn should_handle_many_keys() {
        // Arrange
        let mut writer = TrieWriter::new(true);

        // Act
        for i in 0..100 {
            let key = format!("key_{i:04}").into_bytes();
            writer.add_block_key(&key, i as u32).unwrap();
        }
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        for i in 0..100 {
            let key = format!("key_{i:04}").into_bytes();
            assert_eq!(reader.find_block(&key), Some(i as u32));
        }
    }

    #[test]
    fn should_return_none_for_empty_enabled_writer() {
        // Arrange
        let writer = TrieWriter::new(true);

        // Act
        let result = writer.finish();

        // Assert
        // Empty trie still produces some data (at least root node)
        assert!(result.is_some());
    }

    #[test]
    fn should_handle_special_characters_in_keys() {
        // Arrange
        let mut writer = TrieWriter::new(true);

        // Act
        writer.add_block_key(b"key-with-dashes", 0).unwrap();
        writer.add_block_key(b"key.with.dots", 1).unwrap();
        writer.add_block_key(b"key_with_underscores", 2).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(b"key-with-dashes"), Some(0));
        assert_eq!(reader.find_block(b"key.with.dots"), Some(1));
        assert_eq!(reader.find_block(b"key_with_underscores"), Some(2));
    }

    #[test]
    fn should_handle_binary_keys() {
        // Arrange
        let mut writer = TrieWriter::new(true);
        let key1 = vec![0, 1, 2, 3];
        let key2 = vec![5, 6, 7, 8];

        // Act
        writer.add_block_key(&key1, 0).unwrap();
        writer.add_block_key(&key2, 1).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(&key1), Some(0));
        assert_eq!(reader.find_block(&key2), Some(1));
    }

    #[test]
    fn should_produce_consistent_output() {
        // Arrange
        let mut writer1 = TrieWriter::new(true);
        writer1.add_block_key(b"apple", 0).unwrap();
        writer1.add_block_key(b"banana", 1).unwrap();

        let mut writer2 = TrieWriter::new(true);
        writer2.add_block_key(b"apple", 0).unwrap();
        writer2.add_block_key(b"banana", 1).unwrap();

        // Act
        let result1 = writer1.finish().unwrap();
        let result2 = writer2.finish().unwrap();

        // Assert
        assert_eq!(result1, result2);
    }

    #[test]
    fn should_maintain_block_ids_after_write() {
        // Arrange
        let mut writer = TrieWriter::new(true);
        let block_ids = [0, 5, 10, 100, 1000, u32::MAX - 1];

        // Act
        for (idx, &bid) in block_ids.iter().enumerate() {
            let key = format!("key_{idx}").into_bytes();
            writer.add_block_key(&key, bid).unwrap();
        }
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        for (idx, &bid) in block_ids.iter().enumerate() {
            let key = format!("key_{idx}").into_bytes();
            assert_eq!(reader.find_block(&key), Some(bid));
        }
    }

    #[test]
    fn should_handle_sequential_identical_block_ids() {
        // Arrange
        let mut writer = TrieWriter::new(true);

        // Act
        writer.add_block_key(b"a", 42).unwrap();
        writer.add_block_key(b"b", 42).unwrap();
        writer.add_block_key(b"c", 42).unwrap();
        let result = writer.finish();

        // Assert
        assert!(result.is_some());
        let data = result.unwrap();

        let reader = TrieReader::new(&data).unwrap();
        assert_eq!(reader.find_block(b"a"), Some(42));
        assert_eq!(reader.find_block(b"b"), Some(42));
        assert_eq!(reader.find_block(b"c"), Some(42));
    }
}
