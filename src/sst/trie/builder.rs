//! Trie builder for constructing trie during SST writing

use crate::common::MidgeResult;
use crate::sst::trie::node::{TrieEdge, TrieNode};
use crate::sst::trie::{encoding, lcp};

/// Builder for constructing a trie index during SST writing
pub struct TrieBuilder {
    /// Flat array of nodes (avoids recursion)
    nodes: Vec<TrieNode>,

    /// Last key added (for prefix comparison)
    last_key: Vec<u8>,

    /// Root node index (always 0)
    root_index: usize,
}

impl TrieBuilder {
    /// Create a new trie builder
    #[must_use]
    pub fn new() -> Self {
        // Create root node with empty key
        let root = TrieNode::new(0, Vec::new(), None);

        Self {
            nodes: vec![root],
            last_key: Vec::new(),
            root_index: 0,
        }
    }

    /// Add a key to the trie
    ///
    /// Keys MUST be added in sorted order (enforced by SST writer).
    /// Only add keys at block boundaries (first key of each block).
    pub fn add_key(&mut self, key: &[u8], block_id: u32) -> MidgeResult<()> {
        if key.is_empty() {
            return Ok(()); // Skip empty keys
        }

        // Safety: enforce sorted order to prevent corrupted trie structure
        if !self.last_key.is_empty() && key < self.last_key.as_slice() {
            return Err(crate::common::MidgeError::InvalidArgument(format!(
                "Trie keys must be in sorted order: {:?} > {:?}",
                String::from_utf8_lossy(&self.last_key),
                String::from_utf8_lossy(key)
            )));
        }

        // Safety: block_id u32::MAX would overflow in encoding (causes ambiguity with None)
        if block_id == u32::MAX {
            return Err(crate::common::MidgeError::InvalidArgument(format!(
                "Trie block_id={} reserved for internal use",
                u32::MAX
            )));
        }

        // Insert key into trie
        self.insert_key(key, block_id);

        // Update last key
        self.last_key = key.to_vec();

        Ok(())
    }

    fn insert_key(&mut self, key: &[u8], block_id: u32) {
        let mut current_index = self.root_index;
        let mut matched_len = 0;

        // Navigate to insertion point
        while matched_len < key.len() {
            let remaining = &key[matched_len..];
            let node = &self.nodes[current_index];

            // Try to find matching child
            if let Some(edge) = node.find_child(remaining[0]) {
                let child_index = edge.child_index as usize;
                let child = &self.nodes[child_index];

                // Calculate how much of child's key_delta matches
                let child_match_len = lcp(&child.key_delta, remaining);

                if child_match_len == child.key_delta.len() {
                    // Full match, continue down this path
                    matched_len += child_match_len;
                    current_index = child_index;
                } else {
                    // Partial match, need to split the child node
                    self.split_node(child_index, child_match_len, remaining, block_id);
                    return;
                }
            } else {
                // No matching child, create new leaf
                let new_node =
                    TrieNode::new(matched_len as u16, remaining.to_vec(), Some(block_id));
                let new_index = self.nodes.len() as u32;
                self.nodes.push(new_node);

                // Add edge from current node to new node
                let edge = TrieEdge::new(remaining[0], new_index);
                self.nodes[current_index].add_child(edge);
                return;
            }
        }

        // Key fully matched existing path, update block_id
        self.nodes[current_index].block_id = Some(block_id);
    }

    fn split_node(&mut self, node_index: usize, split_pos: usize, remaining: &[u8], block_id: u32) {
        // Clone the node data we need (to avoid borrow checker issues)
        let old_key_delta = self.nodes[node_index].key_delta.clone();
        let old_block_id = self.nodes[node_index].block_id;
        let old_children = self.nodes[node_index].children.clone();

        // Create new intermediate node (becomes the parent)
        let common_part = old_key_delta[..split_pos].to_vec();
        let mut intermediate = TrieNode::new(self.nodes[node_index].prefix_len, common_part, None);

        // Create node for remainder of old key
        let old_suffix = old_key_delta[split_pos..].to_vec();
        let old_remainder = TrieNode {
            prefix_len: split_pos as u16,
            key_delta: old_suffix.clone(),
            block_id: old_block_id,
            children: old_children,
        };
        let old_remainder_index = self.nodes.len() as u32;
        self.nodes.push(old_remainder);
        intermediate.add_child(TrieEdge::new(old_suffix[0], old_remainder_index));

        // Create node for new key
        let new_suffix = remaining[split_pos..].to_vec();
        if new_suffix.is_empty() {
            // New key ends at split point
            intermediate.block_id = Some(block_id);
        } else {
            let new_node = TrieNode::new(split_pos as u16, new_suffix.clone(), Some(block_id));
            let new_node_index = self.nodes.len() as u32;
            self.nodes.push(new_node);
            intermediate.add_child(TrieEdge::new(new_suffix[0], new_node_index));
        }

        // Replace original node with intermediate
        self.nodes[node_index] = intermediate;
    }

    /// Finish building and return serialized trie
    #[must_use]
    pub fn finish(self) -> Vec<u8> {
        encoding::encode_trie(&self.nodes)
    }

    /// Get number of nodes in trie
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl Default for TrieBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_new_builder() {
        // Arrange

        // Act
        let builder = TrieBuilder::new();

        // Assert
        assert_eq!(builder.node_count(), 1); // Root node
    }

    #[test]
    fn should_create_default_builder() {
        // Arrange

        // Act
        let builder = TrieBuilder::default();

        // Assert
        assert_eq!(builder.node_count(), 1);
    }

    #[test]
    fn should_add_single_key() {
        // Arrange
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"apple", 0).unwrap();

        // Assert
        assert!(builder.node_count() >= 2);
    }

    #[test]
    fn should_add_multiple_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"apple", 0).unwrap();
        builder.add_key(b"banana", 1).unwrap();
        builder.add_key(b"cherry", 2).unwrap();

        // Assert
        assert!(builder.node_count() >= 3);
    }

    #[test]
    fn should_skip_empty_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();
        let initial_count = builder.node_count();

        // Act
        builder.add_key(b"", 0).unwrap();

        // Assert
        assert_eq!(builder.node_count(), initial_count);
    }

    #[test]
    fn should_handle_prefix_keys() {
        // Arrange - keys in sorted order
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"test", 0).unwrap();
        builder.add_key(b"tester", 1).unwrap();
        builder.add_key(b"testing", 2).unwrap();

        // Assert
        let data = builder.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn should_handle_single_character_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"a", 0).unwrap();
        builder.add_key(b"b", 1).unwrap();
        builder.add_key(b"c", 2).unwrap();

        // Assert
        assert!(builder.node_count() >= 3);
    }

    #[test]
    fn should_handle_identical_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"same", 0).unwrap();
        builder.add_key(b"same", 1).unwrap();
        builder.add_key(b"same", 2).unwrap();

        // Assert
        let data = builder.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn should_handle_overlapping_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"abc", 0).unwrap();
        builder.add_key(b"abcd", 1).unwrap();
        builder.add_key(b"abcde", 2).unwrap();

        // Assert
        assert!(builder.node_count() >= 3);
    }

    #[test]
    fn should_handle_different_prefixes() {
        // Arrange - sorted order: apple < application < banana
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"apple", 0).unwrap();
        builder.add_key(b"application", 1).unwrap();
        builder.add_key(b"banana", 2).unwrap();

        // Assert
        assert!(builder.node_count() >= 3);
    }

    #[test]
    fn should_handle_large_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();
        let large_key = vec![42; 1000];

        // Act
        builder.add_key(&large_key, 0).unwrap();

        // Assert
        assert!(builder.node_count() >= 2);
    }

    #[test]
    fn should_handle_many_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();

        // Act
        for i in 0..100 {
            let key = format!("key_{i:04}").into_bytes();
            builder.add_key(&key, i as u32).unwrap();
        }

        // Assert
        assert!(builder.node_count() >= 10);
    }

    #[test]
    fn should_finish_empty_builder() {
        // Arrange
        let builder = TrieBuilder::new();

        // Act
        let data = builder.finish();

        // Assert
        assert!(!data.is_empty()); // Root node encoded
    }

    #[test]
    fn should_finish_with_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"apple", 0).unwrap();
        builder.add_key(b"banana", 1).unwrap();

        // Act
        let data = builder.finish();

        // Assert
        assert!(!data.is_empty());
    }

    #[test]
    fn should_reject_reversed_keys() {
        // Arrange
        let mut builder = TrieBuilder::new();
        let result1 = builder.add_key(b"zebra", 0);
        assert!(result1.is_ok());

        // Act
        let result2 = builder.add_key(b"apple", 1);

        // Assert
        // should reject unsorted key
        assert!(result2.is_err());
        assert!(result2.unwrap_err().to_string().contains("sorted order"));
    }

    #[test]
    fn should_handle_special_characters() {
        // Arrange
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"key-with-dashes", 0).unwrap();
        builder.add_key(b"key.with.dots", 1).unwrap();
        builder.add_key(b"key/with/slashes", 2).unwrap();
        builder.add_key(b"key_with_underscores", 3).unwrap();

        // Assert
        assert!(builder.node_count() >= 4);
    }

    #[test]
    fn should_handle_binary_keys() {
        // Arrange - keys must be in sorted order (binary comparison)
        let mut builder = TrieBuilder::new();
        let binary_key1 = vec![0, 1, 2, 3, 4];
        let binary_key2 = vec![0, 1, 2, 3, 5];
        let binary_key3 = vec![5, 6, 7, 8, 9];

        // Act
        builder.add_key(&binary_key1, 0).unwrap();
        builder.add_key(&binary_key2, 1).unwrap();
        builder.add_key(&binary_key3, 2).unwrap();

        // Assert
        assert!(builder.node_count() >= 3);
    }

    #[test]
    fn should_handle_maximum_block_ids() {
        // Arrange
        let mut builder = TrieBuilder::new();
        builder.add_key(b"key1", u32::MAX - 2).unwrap();
        builder.add_key(b"key2", 0).unwrap();
        builder.add_key(b"key3", u32::MAX - 1).unwrap();

        // Act
        let result4 = builder.add_key(b"key4", u32::MAX);

        // Assert
        assert!(builder.node_count() >= 3);
        // should reject u32::MAX (reserved for encoding)
        assert!(result4.is_err());
        assert!(result4.unwrap_err().to_string().contains("reserved"));
    }

    #[test]
    fn should_track_node_count() {
        // Arrange
        let mut builder = TrieBuilder::new();
        let initial = builder.node_count();

        // Act
        builder.add_key(b"apple", 0).unwrap();
        let after_first = builder.node_count();
        builder.add_key(b"banana", 1).unwrap();
        let after_second = builder.node_count();

        // Assert
        assert!(after_first > initial);
        assert!(after_second >= after_first);
    }

    #[test]
    fn should_handle_utf8_like_sequences() {
        // Arrange - UTF-8-like byte sequences in sorted order
        let mut builder = TrieBuilder::new();

        // Act
        builder.add_key(b"aaa", 0).unwrap();
        builder.add_key(b"aab", 1).unwrap();
        builder.add_key(b"abc", 2).unwrap();

        // Assert
        assert!(builder.node_count() >= 3);
    }

    #[test]
    fn should_finish_produces_deterministic_output() {
        // Arrange
        let mut builder1 = TrieBuilder::new();
        builder1.add_key(b"apple", 0).unwrap();
        builder1.add_key(b"banana", 1).unwrap();

        let mut builder2 = TrieBuilder::new();
        builder2.add_key(b"apple", 0).unwrap();
        builder2.add_key(b"banana", 1).unwrap();

        // Act
        let data1 = builder1.finish();
        let data2 = builder2.finish();

        // Assert
        assert_eq!(data1, data2);
    }
}
