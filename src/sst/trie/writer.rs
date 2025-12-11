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
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get number of nodes in trie
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

    #[test]
    fn should_create_disabled_writer() {
        let writer = TrieWriter::new(false);
        assert!(!writer.is_enabled());
    }

    #[test]
    fn should_create_enabled_writer() {
        let writer = TrieWriter::new(true);
        assert!(writer.is_enabled());
    }

    #[test]
    fn should_skip_when_disabled() {
        let mut writer = TrieWriter::new(false);
        writer.add_block_key(b"test", 0).unwrap();

        let result = writer.finish();
        assert!(result.is_none());
    }

    #[test]
    fn should_build_trie_when_enabled() {
        let mut writer = TrieWriter::new(true);
        writer.add_block_key(b"apple", 0).unwrap();
        writer.add_block_key(b"banana", 1).unwrap();
        writer.add_block_key(b"cherry", 2).unwrap();

        let result = writer.finish();
        assert!(result.is_some());
        assert!(result.unwrap().len() > 0);
    }
}
