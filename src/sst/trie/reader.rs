//! Trie reader for SST lookups

use crate::common::{MidgeError, MidgeResult};
use crate::sst::trie::node::TrieNode;
use crate::sst::trie::{encoding, lcp};

/// Reader for trie index lookups
pub struct TrieReader {
    /// Flat array of nodes
    nodes: Vec<TrieNode>,

    /// Root node index (always 0)
    root_index: usize,
}

impl TrieReader {
    /// Create a new trie reader from serialized data
    pub fn new(data: &[u8]) -> MidgeResult<Self> {
        let nodes = encoding::decode_trie(data)?;

        if nodes.is_empty() {
            return Err(MidgeError::Corruption("Empty trie".into()));
        }

        Ok(Self {
            nodes,
            root_index: 0,
        })
    }

    /// Find block ID for exact key lookup
    ///
    /// Returns None if key not found in trie.
    pub fn find_block(&self, key: &[u8]) -> Option<u32> {
        if key.is_empty() {
            return None;
        }

        let mut current_index = self.root_index;
        let mut matched_len = 0;

        while matched_len < key.len() {
            let remaining = &key[matched_len..];
            let node = &self.nodes[current_index];

            // Try to find matching child
            if let Some(edge) = node.find_child(remaining[0]) {
                let child_index = edge.child_index as usize;
                if child_index >= self.nodes.len() {
                    return None; // Invalid index
                }

                let child = &self.nodes[child_index];

                // Check if remaining key matches child's key_delta
                let child_match_len = lcp(&child.key_delta, remaining);

                if child_match_len == child.key_delta.len() {
                    // Full match of this node's key
                    matched_len += child_match_len;
                    current_index = child_index;
                } else {
                    // Partial match, key not in trie
                    return None;
                }
            } else {
                // No matching child
                return None;
            }
        }

        // Found exact match
        self.nodes[current_index].block_id
    }

    /// Find block IDs for prefix range
    ///
    /// Returns all block IDs that contain keys with the given prefix.
    pub fn find_prefix_range(&self, prefix: &[u8]) -> Vec<u32> {
        let mut result = Vec::new();

        if prefix.is_empty() {
            // Empty prefix matches everything, collect all leaf blocks
            self.collect_all_leaves(&mut result);
            return result;
        }

        // Navigate to prefix node
        let mut current_index = self.root_index;
        let mut matched_len = 0;

        while matched_len < prefix.len() {
            let remaining = &prefix[matched_len..];
            let node = &self.nodes[current_index];

            if let Some(edge) = node.find_child(remaining[0]) {
                let child_index = edge.child_index as usize;
                if child_index >= self.nodes.len() {
                    return result; // Invalid index
                }

                let child = &self.nodes[child_index];
                let child_match_len = lcp(&child.key_delta, remaining);

                if child_match_len == child.key_delta.len() {
                    // Full match, continue
                    matched_len += child_match_len;
                    current_index = child_index;
                } else if child_match_len == remaining.len() {
                    // Prefix ends in middle of this node's key
                    // Collect all descendants
                    self.collect_subtree(child_index, &mut result);
                    return result;
                } else {
                    // Partial match, no keys with this prefix
                    return result;
                }
            } else {
                // No matching child
                return result;
            }
        }

        // Found prefix node, collect all descendants
        self.collect_subtree(current_index, &mut result);
        result
    }

    fn collect_subtree(&self, node_index: usize, result: &mut Vec<u32>) {
        if node_index >= self.nodes.len() {
            return;
        }

        let node = &self.nodes[node_index];

        // Add this node's block if it's a leaf
        if let Some(block_id) = node.block_id {
            result.push(block_id);
        }

        // Recursively collect children
        for edge in &node.children {
            self.collect_subtree(edge.child_index as usize, result);
        }
    }

    fn collect_all_leaves(&self, result: &mut Vec<u32>) {
        for node in &self.nodes {
            if let Some(block_id) = node.block_id {
                result.push(block_id);
            }
        }
    }

    /// Seek to next key >= target
    ///
    /// Returns block ID containing the next key.
    pub fn seek_next(&self, key: &[u8]) -> Option<u32> {
        // For now, use exact match
        // TODO: Implement proper seek logic
        self.find_block(key)
    }

    /// Get number of nodes in trie
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::trie::TrieBuilder;

    fn build_test_trie() -> Vec<u8> {
        let mut builder = TrieBuilder::new();
        builder.add_key(b"apple", 0).unwrap();
        builder.add_key(b"application", 1).unwrap();
        builder.add_key(b"banana", 2).unwrap();
        builder.add_key(b"cherry", 3).unwrap();
        builder.finish()
    }

    fn build_large_test_trie() -> Vec<u8> {
        let mut builder = TrieBuilder::new();
        for i in 0..100 {
            let key = format!("key_{:04}", i).into_bytes();
            builder.add_key(&key, i as u32).unwrap();
        }
        builder.finish()
    }

    #[test]
    fn should_create_reader_from_valid_trie() {
        // Arrange
        let data = build_test_trie();

        // Act
        let reader = TrieReader::new(&data);

        // Assert
        assert!(reader.is_ok());
    }

    #[test]
    fn should_reject_empty_trie_data() {
        // Arrange
        // Act
        let reader = TrieReader::new(&[]);

        // Assert
        assert!(reader.is_err());
    }

    #[test]
    fn should_find_exact_keys() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"apple"), Some(0));
        assert_eq!(reader.find_block(b"application"), Some(1));
        assert_eq!(reader.find_block(b"banana"), Some(2));
        assert_eq!(reader.find_block(b"cherry"), Some(3));
    }

    #[test]
    fn should_return_none_for_missing_keys() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"apricot"), None);
        assert_eq!(reader.find_block(b"zoo"), None);
        assert_eq!(reader.find_block(b""), None);
        assert_eq!(reader.find_block(b"app"), None);
    }

    #[test]
    fn should_return_none_for_partial_matches() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"app"), None);
        assert_eq!(reader.find_block(b"appl"), None);
        assert_eq!(reader.find_block(b"ban"), None);
    }

    #[test]
    fn should_find_first_key() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"apple"), Some(0));
    }

    #[test]
    fn should_find_last_key() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"cherry"), Some(3));
    }

    #[test]
    fn should_find_block_in_large_trie() {
        // Arrange
        let data = build_large_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"key_0000"), Some(0));
        assert_eq!(reader.find_block(b"key_0050"), Some(50));
        assert_eq!(reader.find_block(b"key_0099"), Some(99));
    }

    #[test]
    fn should_find_prefix_range_for_single_key() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let blocks = reader.find_prefix_range(b"apple");

        // Assert
        assert_eq!(blocks.len(), 1);
        assert!(blocks.contains(&0));
    }

    #[test]
    fn should_find_prefix_range_for_multiple_keys() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let blocks = reader.find_prefix_range(b"app");

        // Assert
        assert!(blocks.contains(&0)); // apple
        assert!(blocks.contains(&1)); // application
        assert!(!blocks.contains(&2)); // banana
        assert!(!blocks.contains(&3)); // cherry
    }

    #[test]
    fn should_find_single_prefix_match() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let ban_blocks = reader.find_prefix_range(b"ban");

        // Assert
        assert_eq!(ban_blocks, vec![2]); // banana
    }

    #[test]
    fn should_handle_empty_prefix() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let all_blocks = reader.find_prefix_range(b"");

        // Assert
        assert!(all_blocks.len() >= 4); // All blocks
        assert!(all_blocks.contains(&0));
        assert!(all_blocks.contains(&1));
        assert!(all_blocks.contains(&2));
        assert!(all_blocks.contains(&3));
    }

    #[test]
    fn should_return_empty_for_no_prefix_match() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let blocks = reader.find_prefix_range(b"xyz");

        // Assert
        assert_eq!(blocks.len(), 0);
    }

    #[test]
    fn should_return_empty_for_partial_non_matching_prefix() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let blocks = reader.find_prefix_range(b"zz");

        // Assert
        assert_eq!(blocks.len(), 0);
    }

    #[test]
    fn should_find_prefix_in_large_trie() {
        // Arrange
        let data = build_large_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let blocks = reader.find_prefix_range(b"key_00");

        // Assert
        assert!(blocks.contains(&0));
        assert!(blocks.contains(&9));
    }

    #[test]
    fn should_seek_next_with_find_block() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.seek_next(b"apple"), Some(0));
        assert_eq!(reader.seek_next(b"banana"), Some(2));
    }

    #[test]
    fn should_seek_next_missing_key() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.seek_next(b"missing"), None);
    }

    #[test]
    fn should_get_node_count() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let count = reader.node_count();

        // Assert
        assert!(count > 0);
    }

    #[test]
    fn should_get_node_count_for_large_trie() {
        // Arrange
        let data = build_large_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let count = reader.node_count();

        // Assert
        assert!(count >= 100);
    }

    #[test]
    fn should_find_exact_match_in_prefix_range() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let blocks = reader.find_prefix_range(b"cherry");

        // Assert
        assert!(blocks.contains(&3));
    }

    #[test]
    fn should_find_all_keys_with_common_prefix() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let app_blocks = reader.find_prefix_range(b"a");

        // Assert
        assert!(app_blocks.contains(&0)); // apple
        assert!(app_blocks.contains(&1)); // application
        assert!(!app_blocks.contains(&2)); // banana
    }

    #[test]
    fn should_handle_single_character_prefix() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let b_blocks = reader.find_prefix_range(b"b");

        // Assert
        assert!(b_blocks.contains(&2)); // banana
        assert!(!b_blocks.contains(&0)); // apple
    }

    #[test]
    fn should_handle_prefix_longer_than_key() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act
        let blocks = reader.find_prefix_range(b"applezzzz");

        // Assert
        assert_eq!(blocks.len(), 0);
    }

    #[test]
    fn should_maintain_block_id_values() {
        // Arrange
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"apple"), Some(0));
        assert_eq!(reader.find_block(b"application"), Some(1));
        assert_eq!(reader.find_block(b"banana"), Some(2));
        assert_eq!(reader.find_block(b"cherry"), Some(3));
    }

    #[test]
    fn should_find_keys_with_special_characters() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"key-with-dash", 0).unwrap();
        builder.add_key(b"key.with.dot", 1).unwrap();
        builder.add_key(b"key_with_underscore", 2).unwrap();
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"key-with-dash"), Some(0));
        assert_eq!(reader.find_block(b"key.with.dot"), Some(1));
        assert_eq!(reader.find_block(b"key_with_underscore"), Some(2));
    }

    #[test]
    fn should_find_binary_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(&[0, 1, 2], 0).unwrap();
        builder.add_key(&[0, 1, 3], 1).unwrap();
        builder.add_key(&[5, 6, 7], 2).unwrap();
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(&[0, 1, 2]), Some(0));
        assert_eq!(reader.find_block(&[0, 1, 3]), Some(1));
        assert_eq!(reader.find_block(&[5, 6, 7]), Some(2));
    }

    #[test]
    fn should_find_maximum_block_ids() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"key1", u32::MAX - 1).unwrap();
        builder.add_key(b"key2", 0).unwrap();
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"key1"), Some(u32::MAX - 1));
        assert_eq!(reader.find_block(b"key2"), Some(0));
    }

    #[test]
    fn should_handle_deeply_nested_trie() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"a", 0).unwrap();
        builder.add_key(b"ab", 1).unwrap();
        builder.add_key(b"abc", 2).unwrap();
        builder.add_key(b"abcd", 3).unwrap();
        builder.add_key(b"abcde", 4).unwrap();
        let data = builder.finish();
        let reader = TrieReader::new(&data).unwrap();

        // Act

        // Assert
        assert_eq!(reader.find_block(b"a"), Some(0));
        assert_eq!(reader.find_block(b"ab"), Some(1));
        assert_eq!(reader.find_block(b"abc"), Some(2));
        assert_eq!(reader.find_block(b"abcd"), Some(3));
        assert_eq!(reader.find_block(b"abcde"), Some(4));
    }
}
