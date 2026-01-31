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
        // Arrange
        let prefix_len = 0;
        let key_delta = b"test".to_vec();
        let block_id = Some(42);

        // Act
        let node = TrieNode::new(prefix_len, key_delta, block_id);

        // Assert
        assert_eq!(node.prefix_len, 0);
        assert_eq!(node.key_delta, b"test");
        assert_eq!(node.block_id, Some(42));
        assert!(node.is_leaf());
    }

    #[test]
    fn should_create_root_node() {
        // Arrange
        let prefix_len = 0;
        let key_delta = Vec::new();
        let block_id = None;

        // Act
        let node = TrieNode::new(prefix_len, key_delta, block_id);

        // Assert
        assert_eq!(node.prefix_len, 0);
        assert!(node.key_delta.is_empty());
        assert!(node.block_id.is_none());
        assert!(!node.is_leaf());
    }

    #[test]
    fn should_create_internal_node_with_prefix() {
        // Arrange
        let prefix_len = 5;
        let key_delta = b"delta".to_vec();
        let block_id = None;

        // Act
        let node = TrieNode::new(prefix_len, key_delta, block_id);

        // Assert
        assert_eq!(node.prefix_len, 5);
        assert_eq!(node.key_delta, b"delta");
        assert!(node.block_id.is_none());
        assert!(!node.is_leaf());
    }

    #[test]
    fn should_create_leaf_node_with_block_id() {
        // Arrange
        let prefix_len = 10;
        let key_delta = b"suffix".to_vec();
        let block_id = Some(999);

        // Act
        let node = TrieNode::new(prefix_len, key_delta, block_id);

        // Assert
        assert_eq!(node.prefix_len, 10);
        assert_eq!(node.key_delta, b"suffix");
        assert_eq!(node.block_id, Some(999));
        assert!(node.is_leaf());
    }

    #[test]
    fn should_create_node_with_large_block_id() {
        // Arrange
        let prefix_len = 0;
        let key_delta = b"key".to_vec();
        let block_id = Some(u32::MAX);

        // Act
        let node = TrieNode::new(prefix_len, key_delta, block_id);

        // Assert
        assert_eq!(node.block_id, Some(u32::MAX));
        assert!(node.is_leaf());
    }

    #[test]
    fn should_create_node_with_large_key_delta() {
        // Arrange
        let large_key: Vec<u8> = vec![42; 1000];

        // Act
        let node = TrieNode::new(0, large_key.clone(), Some(1));

        // Assert
        assert_eq!(node.key_delta.len(), 1000);
        assert_eq!(node.key_delta, large_key);
    }

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
    fn should_maintain_sort_order_with_duplicates() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);

        // Act
        node.add_child(TrieEdge::new(b'z', 0));
        node.add_child(TrieEdge::new(b'a', 1));
        node.add_child(TrieEdge::new(b'z', 2)); // Duplicate byte, different index
        node.add_child(TrieEdge::new(b'm', 3));

        // Assert - should be sorted by first_byte, with duplicates in insertion order within same byte
        assert_eq!(node.children.len(), 4);
        assert_eq!(node.children[0].first_byte, b'a');
        assert_eq!(node.children[0].child_index, 1);
        assert_eq!(node.children[1].first_byte, b'm');
        assert_eq!(node.children[1].child_index, 3);
        assert_eq!(node.children[2].first_byte, b'z');
        // Both z entries should be present
        assert_eq!(node.children[3].first_byte, b'z');
    }

    #[test]
    fn should_add_many_children_in_order() {
        // Arrange
        let mut node = TrieNode::new(0, b"root".to_vec(), None);
        let bytes = b"zyxwvutsrqponmlkjihgfedcba";

        // Act
        for (i, &b) in bytes.iter().enumerate() {
            node.add_child(TrieEdge::new(b, i as u32));
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

    #[test]
    fn should_clone_node_correctly() {
        // Arrange
        let mut original = TrieNode::new(5, b"test".to_vec(), Some(42));
        original.add_child(TrieEdge::new(b'a', 10));

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.prefix_len, original.prefix_len);
        assert_eq!(cloned.key_delta, original.key_delta);
        assert_eq!(cloned.block_id, original.block_id);
        assert_eq!(cloned.children.len(), original.children.len());
    }

    #[test]
    fn should_debug_format_node() {
        // Arrange
        let node = TrieNode::new(5, b"test".to_vec(), Some(42));

        // Act
        let debug_str = format!("{:?}", node);

        // Assert
        assert!(debug_str.contains("TrieNode"));
        assert!(debug_str.contains("prefix_len"));
    }

    #[test]
    fn should_create_edge_with_valid_indices() {
        // Arrange
        let first_byte_min = b'x';
        let child_index_min = 0;
        let first_byte_max = b'y';
        let child_index_max = u32::MAX;

        // Act
        let edge = TrieEdge::new(first_byte_min, child_index_min);
        let edge_max = TrieEdge::new(first_byte_max, child_index_max);

        // Assert
        assert_eq!(edge.first_byte, b'x');
        assert_eq!(edge.child_index, 0);
        assert_eq!(edge_max.first_byte, b'y');
        assert_eq!(edge_max.child_index, u32::MAX);
    }

    #[test]
    fn should_clone_edge() {
        // Arrange
        let original = TrieEdge::new(b'a', 42);

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.first_byte, original.first_byte);
        assert_eq!(cloned.child_index, original.child_index);
    }

    #[test]
    fn should_debug_format_edge() {
        // Arrange
        let edge = TrieEdge::new(b'a', 10);

        // Act
        let debug_str = format!("{:?}", edge);

        // Assert
        assert!(debug_str.contains("TrieEdge"));
        assert!(debug_str.contains("first_byte"));
    }
}
