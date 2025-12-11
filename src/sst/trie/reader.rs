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

    #[test]
    fn should_find_exact_keys() {
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        assert_eq!(reader.find_block(b"apple"), Some(0));
        assert_eq!(reader.find_block(b"application"), Some(1));
        assert_eq!(reader.find_block(b"banana"), Some(2));
        assert_eq!(reader.find_block(b"cherry"), Some(3));
    }

    #[test]
    fn should_return_none_for_missing_keys() {
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        assert_eq!(reader.find_block(b"apricot"), None);
        assert_eq!(reader.find_block(b"zoo"), None);
        assert_eq!(reader.find_block(b""), None);
    }

    #[test]
    fn should_find_prefix_ranges() {
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        let app_blocks = reader.find_prefix_range(b"app");
        assert!(app_blocks.contains(&0)); // apple
        assert!(app_blocks.contains(&1)); // application

        let ban_blocks = reader.find_prefix_range(b"ban");
        assert_eq!(ban_blocks, vec![2]); // banana
    }

    #[test]
    fn should_handle_empty_prefix() {
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        let all_blocks = reader.find_prefix_range(b"");
        assert!(all_blocks.len() >= 4); // All blocks
    }

    #[test]
    fn should_return_empty_for_no_prefix_match() {
        let data = build_test_trie();
        let reader = TrieReader::new(&data).unwrap();

        let blocks = reader.find_prefix_range(b"xyz");
        assert_eq!(blocks.len(), 0);
    }
}
