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

    /// Children sorted by first byte of `key_delta`
    pub children: Vec<TrieEdge>,
}

impl TrieNode {
    /// Create a new trie node
    #[must_use]
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
        // A node can have at most one outgoing edge for a byte. Replacing the
        // target preserves deterministic lookup and prevents an ambiguous
        // serialized trie if a builder retries an edge.
        match self
            .children
            .binary_search_by_key(&edge.first_byte, |existing| existing.first_byte)
        {
            Ok(index) => self.children[index] = edge,
            Err(index) => self.children.insert(index, edge),
        }
    }

    /// Find child by first byte
    #[must_use]
    pub fn find_child(&self, byte: u8) -> Option<&TrieEdge> {
        self.children
            .binary_search_by_key(&byte, |e| e.first_byte)
            .ok()
            .map(|idx| &self.children[idx])
    }

    /// Check if this is a leaf node (maps to a block)
    #[must_use]
    pub fn is_leaf(&self) -> bool {
        self.block_id.is_some()
    }
}

/// Edge connecting parent node to child node
#[derive(Debug, Clone)]
pub struct TrieEdge {
    /// First byte of child's `key_delta` (for binary search)
    pub first_byte: u8,

    /// Index of child node in flat node array
    pub child_index: u32,
}

impl TrieEdge {
    /// Create a new trie edge
    #[must_use]
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
    fn should_add_single_child() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);
        let edge = TrieEdge::new(b'a', 1);

        // Act
        node.add_child(edge);

        // Assert
        assert_eq!(node.children.len(), 1);
        assert_eq!(node.children[0].first_byte, b'a');
        assert_eq!(node.children[0].child_index, 1);
    }

    #[test]
    fn should_add_children_in_sorted_order() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);

        // Act
        node.add_child(TrieEdge::new(b'c', 2));
        node.add_child(TrieEdge::new(b'a', 0));
        node.add_child(TrieEdge::new(b'b', 1));

        // Assert
        assert_eq!(node.children.len(), 3);
        assert_eq!(node.children[0].first_byte, b'a');
        assert_eq!(node.children[1].first_byte, b'b');
        assert_eq!(node.children[2].first_byte, b'c');
    }

    #[test]
    fn should_replace_duplicate_child_edge_when_byte_already_exists() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);

        // Act
        node.add_child(TrieEdge::new(b'z', 0));
        node.add_child(TrieEdge::new(b'a', 1));
        node.add_child(TrieEdge::new(b'z', 2)); // Duplicate byte, different index
        node.add_child(TrieEdge::new(b'm', 3));

        // Assert
        assert_eq!(node.children.len(), 3);
        assert_eq!(node.children[0].first_byte, b'a');
        assert_eq!(node.children[0].child_index, 1);
        assert_eq!(node.children[1].first_byte, b'm');
        assert_eq!(node.children[1].child_index, 3);
        assert_eq!(node.children[2].first_byte, b'z');
        assert_eq!(node.children[2].child_index, 2);
    }

    #[test]
    fn should_add_many_children_in_order() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);
        let bytes = b"zyxwvutsrqponmlkjihgfedcba";

        // Act
        for (i, &b) in bytes.iter().enumerate() {
            node.add_child(TrieEdge::new(b, u32::try_from(i).unwrap_or(u32::MAX)));
        }

        // Assert
        assert_eq!(node.children.len(), 26);
        for i in 0..26 {
            assert_eq!(
                node.children[i].first_byte,
                b"abcdefghijklmnopqrstuvwxyz"[i]
            );
        }
    }

    #[test]
    fn should_find_child_by_byte() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);
        node.add_child(TrieEdge::new(b'a', 0));
        node.add_child(TrieEdge::new(b'b', 1));
        node.add_child(TrieEdge::new(b'c', 2));

        // Act
        let a = node.find_child(b'a').map(|e| e.child_index);
        let b = node.find_child(b'b').map(|e| e.child_index);
        let c = node.find_child(b'c').map(|e| e.child_index);

        // Assert
        assert_eq!(a, Some(0));
        assert_eq!(b, Some(1));
        assert_eq!(c, Some(2));
    }

    #[test]
    fn should_return_none_for_missing_child() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);
        node.add_child(TrieEdge::new(b'a', 0));
        node.add_child(TrieEdge::new(b'b', 1));

        // Act
        let z = node.find_child(b'z');
        let c = node.find_child(b'c');
        let zero = node.find_child(0);

        // Assert
        assert!(z.is_none());
        assert!(c.is_none());
        assert!(zero.is_none());
    }

    #[test]
    fn should_find_first_last_children() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);
        node.add_child(TrieEdge::new(0, 0)); // Minimum byte value
        node.add_child(TrieEdge::new(255, 1)); // Maximum byte value

        // Act
        let first = node.find_child(0);
        let last = node.find_child(255);

        // Assert
        assert!(first.is_some());
        assert!(last.is_some());
    }

    #[test]
    fn should_identify_leaf_correctly() {
        // Arrange
        let leaf_node = TrieNode::new(0, b"leaf".to_vec(), Some(42));
        let internal_node = TrieNode::new(0, b"internal".to_vec(), None);

        // Act
        let leaf_is_leaf = leaf_node.is_leaf();
        let internal_is_leaf = internal_node.is_leaf();

        // Assert
        assert!(leaf_is_leaf);
        assert!(!internal_is_leaf);
    }

    #[test]
    fn should_identify_leaf_with_block_id_zero() {
        // Arrange
        let prefix_len = 0;
        let key_delta = b"key".to_vec();
        let block_id = Some(0);

        // Act
        let node = TrieNode::new(prefix_len, key_delta, block_id);

        // Assert
        assert!(node.is_leaf()); // Some(0) is still a leaf
    }
}
