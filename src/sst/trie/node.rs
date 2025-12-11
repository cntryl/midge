//! Trie node structures

/// Trie node representing a prefix-compressed edge
#[derive(Debug, Clone)]
pub struct TrieNode {
    /// Length of prefix shared with parent
    pub prefix_len: u16,

    /// Remaining suffix for this edge
    pub key_delta: Vec<u8>,

    /// Block ID if this node maps to a block (leaf nodes)
    pub block_id: Option<u32>,

    /// Children sorted by first byte of key_delta
    pub children: Vec<TrieEdge>,
}

impl TrieNode {
    /// Create a new trie node
    pub fn new(prefix_len: u16, key_delta: Vec<u8>, block_id: Option<u32>) -> Self {
        Self {
            prefix_len,
            key_delta,
            block_id,
            children: Vec::new(),
        }
    }

    /// Add a child edge to this node
    pub fn add_child(&mut self, edge: TrieEdge) {
        // Insert in sorted order by first byte
        let insert_pos = self
            .children
            .binary_search_by_key(&edge.first_byte, |e| e.first_byte)
            .unwrap_or_else(|pos| pos);
        self.children.insert(insert_pos, edge);
    }

    /// Find child by first byte
    pub fn find_child(&self, byte: u8) -> Option<&TrieEdge> {
        self.children
            .binary_search_by_key(&byte, |e| e.first_byte)
            .ok()
            .map(|idx| &self.children[idx])
    }

    /// Check if this is a leaf node (maps to a block)
    pub fn is_leaf(&self) -> bool {
        self.block_id.is_some()
    }
}

/// Edge connecting parent node to child node
#[derive(Debug, Clone)]
pub struct TrieEdge {
    /// First byte of child's key_delta (for binary search)
    pub first_byte: u8,

    /// Index of child node in flat node array
    pub child_index: u32,
}

impl TrieEdge {
    /// Create a new trie edge
    pub fn new(first_byte: u8, child_index: u32) -> Self {
        Self {
            first_byte,
            child_index,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_node() {
        let node = TrieNode::new(0, b"test".to_vec(), Some(42));
        assert_eq!(node.prefix_len, 0);
        assert_eq!(node.key_delta, b"test");
        assert_eq!(node.block_id, Some(42));
        assert!(node.is_leaf());
    }

    #[test]
    fn should_add_children_in_sorted_order() {
        let mut node = TrieNode::new(0, b"root".to_vec(), None);

        node.add_child(TrieEdge::new(b'c', 2));
        node.add_child(TrieEdge::new(b'a', 0));
        node.add_child(TrieEdge::new(b'b', 1));

        assert_eq!(node.children.len(), 3);
        assert_eq!(node.children[0].first_byte, b'a');
        assert_eq!(node.children[1].first_byte, b'b');
        assert_eq!(node.children[2].first_byte, b'c');
    }

    #[test]
    fn should_find_child_by_byte() {
        let mut node = TrieNode::new(0, b"root".to_vec(), None);
        node.add_child(TrieEdge::new(b'a', 0));
        node.add_child(TrieEdge::new(b'b', 1));

        assert!(node.find_child(b'a').is_some());
        assert!(node.find_child(b'b').is_some());
        assert!(node.find_child(b'c').is_none());
    }
}
