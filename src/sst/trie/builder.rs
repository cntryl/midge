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

        // Calculate prefix overlap with last key
        let _common_len = if self.last_key.is_empty() {
            0
        } else {
            lcp(&self.last_key, key)
        };

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
        if !new_suffix.is_empty() {
            let new_node = TrieNode::new(split_pos as u16, new_suffix.clone(), Some(block_id));
            let new_node_index = self.nodes.len() as u32;
            self.nodes.push(new_node);
            intermediate.add_child(TrieEdge::new(new_suffix[0], new_node_index));
        } else {
            // New key ends at split point
            intermediate.block_id = Some(block_id);
        }

        // Replace original node with intermediate
        self.nodes[node_index] = intermediate;
    }

    /// Finish building and return serialized trie
    pub fn finish(self) -> Vec<u8> {
        encoding::encode_trie(&self.nodes)
    }

    /// Get number of nodes in trie
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
    fn should_build_simple_trie() {
        let mut builder = TrieBuilder::new();
        builder.add_key(b"apple", 0).unwrap();
        builder.add_key(b"banana", 1).unwrap();
        builder.add_key(b"cherry", 2).unwrap();

        assert!(builder.node_count() >= 3); // At least one node per key
    }

    #[test]
    fn should_handle_prefix_keys() {
        let mut builder = TrieBuilder::new();
        builder.add_key(b"test", 0).unwrap();
        builder.add_key(b"testing", 1).unwrap();
        builder.add_key(b"tester", 2).unwrap();

        let data = builder.finish();
        assert!(data.len() > 0);
    }

    #[test]
    fn should_skip_empty_keys() {
        let mut builder = TrieBuilder::new();
        builder.add_key(b"", 0).unwrap();
        builder.add_key(b"key", 1).unwrap();

        assert!(builder.node_count() >= 1);
    }
}
