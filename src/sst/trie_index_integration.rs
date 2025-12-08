//! Integration layer for optional trie index in SST files.
//!
//! This module provides helpers to wire the trie index into existing SST writers
//! when the `new_sst_index` flag is enabled. It maintains backward compatibility
//! by storing the trie as an optional meta-index entry.

use crate::error::MidgeResult;
use crate::sst::meta_index;
use crate::sst::trie_index::{TrieIndex, TrieIndexBuilder};
use bytes::Bytes;

/// Constant key for trie index in meta_index
pub const TRIE_INDEX_KEY: &[u8] = b"index.trie";

/// Wrapper for optional trie index support during SST writing
pub struct OptionalTrieIndexWriter {
    /// Only Some if new_sst_index flag is enabled
    builder: Option<TrieIndexBuilder>,
}

impl OptionalTrieIndexWriter {
    /// Create a new optional trie writer
    pub fn new(build_trie: bool) -> Self {
        Self {
            builder: if build_trie {
                Some(TrieIndexBuilder::new())
            } else {
                None
            },
        }
    }

    /// Record a data block in the trie (if enabled)
    pub fn add_block(&mut self, min_key: &[u8], max_key: &[u8]) {
        if let Some(builder) = &mut self.builder {
            builder.add_block(min_key, max_key);
        }
    }

    /// Finish and return encoded trie if enabled, or empty bytes if not
    pub fn finish(&self) -> Bytes {
        self.builder
            .as_ref()
            .map(|b| b.finish())
            .unwrap_or_default()
    }

    /// Check if trie index was built
    pub fn is_enabled(&self) -> bool {
        self.builder.is_some()
    }
}

/// Wrapper for optional trie index support during SST reading
pub struct OptionalTrieIndexReader {
    /// Only Some if trie index was found in meta_index
    index: Option<TrieIndex>,
}

impl OptionalTrieIndexReader {
    /// Create from meta_index data (returns Ok with None if trie not found)
    pub fn from_meta_index(meta_index_data: &[u8]) -> MidgeResult<Self> {
        match meta_index::linear_search_meta_index(
            meta_index_data,
            0,
            meta_index_data.len(),
            TRIE_INDEX_KEY,
        ) {
            Ok(Some(_handle)) => {
                // Found trie index block handle, but we'd need the actual block data to decode it
                // This is a placeholder - in real implementation, would read block from SST
                Ok(Self { index: None })
            }
            Ok(None) => {
                // Trie index not present - that's fine (old SST or trie disabled)
                Ok(Self { index: None })
            }
            Err(e) => Err(e),
        }
    }

    /// Decode trie from block data
    pub fn from_block_data(data: &[u8]) -> MidgeResult<Self> {
        match TrieIndex::decode(data) {
            Ok(index) => Ok(Self { index: Some(index) }),
            Err(e) => Err(e),
        }
    }

    /// Find candidate blocks for a key (falls back to empty if trie not available)
    pub fn find_candidate_blocks(&self, key: &[u8]) -> Vec<u32> {
        self.index
            .as_ref()
            .map(|idx| idx.find_candidate_blocks(key))
            .unwrap_or_default()
    }

    /// Find candidate blocks for a range (falls back to empty if trie not available)
    pub fn find_blocks_in_range(&self, start: &[u8], end: &[u8]) -> Vec<u32> {
        self.index
            .as_ref()
            .map(|idx| idx.find_blocks_in_range(start, end))
            .unwrap_or_default()
    }

    /// Check if trie index is available
    pub fn is_available(&self) -> bool {
        self.index.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_optional_writer_disabled() {
        let writer = OptionalTrieIndexWriter::new(false);
        assert!(!writer.is_enabled());
        assert!(writer.finish().is_empty());
    }

    #[test]
    fn should_create_optional_writer_enabled() {
        let writer = OptionalTrieIndexWriter::new(true);
        assert!(writer.is_enabled());
    }

    #[test]
    fn should_add_blocks_when_enabled() {
        // Arrange
        let mut writer = OptionalTrieIndexWriter::new(true);

        // Act
        writer.add_block(b"key_001", b"key_010");
        writer.add_block(b"key_020", b"key_030");
        let encoded = writer.finish();

        // Assert
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_ignore_blocks_when_disabled() {
        // Arrange
        let mut writer = OptionalTrieIndexWriter::new(false);

        // Act
        writer.add_block(b"key_001", b"key_010");
        writer.add_block(b"key_020", b"key_030");
        let encoded = writer.finish();

        // Assert
        assert!(encoded.is_empty());
    }

    #[test]
    fn should_create_optional_reader() {
        // Arrange
        // Act
        let reader = OptionalTrieIndexReader { index: None };

        // Assert
        assert!(!reader.is_available());
    }

    #[test]
    fn should_find_no_blocks_when_unavailable() {
        // Arrange
        let reader = OptionalTrieIndexReader { index: None };

        // Act
        let blocks = reader.find_candidate_blocks(b"any_key");

        // Assert
        assert_eq!(blocks.len(), 0);
    }
}
