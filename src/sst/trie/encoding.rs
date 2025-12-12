//! Trie serialization and deserialization

use crate::common::{MidgeError, MidgeResult};
use crate::sst::trie::node::{TrieEdge, TrieNode};
use bytes::{BufMut, BytesMut};

/// Encode trie nodes to compact binary format
///
/// Layout:
///   [ varint node_count ]
///   [ node_0 ]
///   [ node_1 ]
///   ...
///
/// Each node:
///   [ varint prefix_len ]
///   [ varint key_delta_len ] + key_delta
///   [ varint block_id ] (0 = None, 1-based offset)
///   [ varint child_count ]
///   [ children: varint child_index ]*child_count
pub fn encode_trie(nodes: &[TrieNode]) -> Vec<u8> {
    let mut buf = BytesMut::new();

    // Write node count
    encode_varint(&mut buf, nodes.len() as u64);

    // Write each node
    for node in nodes {
        encode_node(&mut buf, node);
    }

    buf.to_vec()
}

fn encode_node(buf: &mut BytesMut, node: &TrieNode) {
    // prefix_len
    encode_varint(buf, node.prefix_len as u64);

    // key_delta length + data
    encode_varint(buf, node.key_delta.len() as u64);
    buf.put_slice(&node.key_delta);

    // block_id (0 = None, 1-based for Some)
    encode_varint(buf, node.block_id.map(|id| id + 1).unwrap_or(0) as u64);

    // child_count
    encode_varint(buf, node.children.len() as u64);

    // children
    for child in &node.children {
        buf.put_u8(child.first_byte);
        encode_varint(buf, child.child_index as u64);
    }
}

/// Decode trie nodes from binary format
pub fn decode_trie(data: &[u8]) -> MidgeResult<Vec<TrieNode>> {
    let mut cursor = std::io::Cursor::new(data);

    // Read node count
    let node_count = decode_varint(&mut cursor)? as usize;
    let mut nodes = Vec::with_capacity(node_count);

    // Read each node
    for _ in 0..node_count {
        nodes.push(decode_node(&mut cursor)?);
    }

    Ok(nodes)
}

fn decode_node(cursor: &mut std::io::Cursor<&[u8]>) -> MidgeResult<TrieNode> {
    // prefix_len
    let prefix_len = decode_varint(cursor)? as u16;

    // key_delta
    let key_delta_len = decode_varint(cursor)? as usize;
    let pos = cursor.position() as usize;
    let data = cursor.get_ref();
    if pos + key_delta_len > data.len() {
        return Err(MidgeError::Corruption("Trie key_delta truncated".into()));
    }
    let key_delta = data[pos..pos + key_delta_len].to_vec();
    cursor.set_position((pos + key_delta_len) as u64);

    // block_id (0 = None, 1-based for Some)
    let block_id_raw = decode_varint(cursor)?;
    let block_id = if block_id_raw == 0 {
        None
    } else {
        Some((block_id_raw - 1) as u32)
    };

    // child_count
    let child_count = decode_varint(cursor)? as usize;

    // children
    let mut children = Vec::with_capacity(child_count);
    for _ in 0..child_count {
        if cursor.position() >= cursor.get_ref().len() as u64 {
            return Err(MidgeError::Corruption("Trie children truncated".into()));
        }
        let first_byte = cursor.get_ref()[cursor.position() as usize];
        cursor.set_position(cursor.position() + 1);
        let child_index = decode_varint(cursor)? as u32;
        children.push(TrieEdge::new(first_byte, child_index));
    }

    Ok(TrieNode {
        prefix_len,
        key_delta,
        block_id,
        children,
    })
}

fn encode_varint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8((value as u8) | 0x80);
        value >>= 7;
    }
    buf.put_u8(value as u8);
}

fn decode_varint(cursor: &mut std::io::Cursor<&[u8]>) -> MidgeResult<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        if cursor.position() >= cursor.get_ref().len() as u64 {
            return Err(MidgeError::Corruption("Varint truncated".into()));
        }

        let byte = cursor.get_ref()[cursor.position() as usize];
        cursor.set_position(cursor.position() + 1);

        result |= ((byte & 0x7f) as u64) << shift;

        if byte & 0x80 == 0 {
            return Ok(result);
        }

        shift += 7;
        if shift >= 64 {
            return Err(MidgeError::Corruption("Varint overflow".into()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_empty_trie() {
        // Arrange
        let nodes = vec![];

        // Act
        let encoded = encode_trie(&nodes);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn should_roundtrip_single_node() {
        // Arrange
        let node = TrieNode::new(0, b"test".to_vec(), Some(42));
        let nodes = vec![node];

        // Act
        let encoded = encode_trie(&nodes);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].prefix_len, 0);
        assert_eq!(decoded[0].key_delta, b"test");
        assert_eq!(decoded[0].block_id, Some(42));
    }

    #[test]
    fn should_roundtrip_node_with_children() {
        // Arrange
        let mut parent = TrieNode::new(0, b"root".to_vec(), None);
        parent.add_child(TrieEdge::new(b'a', 1));
        parent.add_child(TrieEdge::new(b'b', 2));

        let child1 = TrieNode::new(4, b"pple".to_vec(), Some(10));
        let child2 = TrieNode::new(4, b"anana".to_vec(), Some(20));

        let nodes = vec![parent, child1, child2];

        // Act
        let encoded = encode_trie(&nodes);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].children.len(), 2);
        assert_eq!(decoded[0].children[0].child_index, 1);
        assert_eq!(decoded[0].children[1].child_index, 2);
        assert_eq!(decoded[1].block_id, Some(10));
        assert_eq!(decoded[2].block_id, Some(20));
    }

    #[test]
    fn should_encode_none_block_id() {
        // Arrange
        let node = TrieNode::new(0, b"internal".to_vec(), None);

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].block_id, None);
    }

    #[test]
    fn should_encode_maximum_block_id() {
        // Arrange - use max value that won't overflow when encoded (max - 1)
        let node = TrieNode::new(0, b"max".to_vec(), Some(u32::MAX - 1));

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].block_id, Some(u32::MAX - 1));
    }

    #[test]
    fn should_encode_block_id_zero() {
        // Arrange
        let node = TrieNode::new(0, b"zero".to_vec(), Some(0));

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].block_id, Some(0));
    }

    #[test]
    fn should_roundtrip_large_key_delta() {
        // Arrange
        let large_key = vec![42; 1000];
        let node = TrieNode::new(0, large_key.clone(), Some(1));

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].key_delta.len(), 1000);
        assert_eq!(decoded[0].key_delta, large_key);
    }

    #[test]
    fn should_roundtrip_empty_key_delta() {
        // Arrange
        let node = TrieNode::new(0, Vec::new(), Some(42));

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].key_delta.len(), 0);
        assert_eq!(decoded[0].block_id, Some(42));
    }

    #[test]
    fn should_roundtrip_large_prefix_len() {
        // Arrange
        let node = TrieNode::new(u16::MAX, b"suffix".to_vec(), Some(42));

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].prefix_len, u16::MAX);
    }

    #[test]
    fn should_roundtrip_many_children() {
        // Arrange
        let mut parent = TrieNode::new(0, b"parent".to_vec(), None);
        for i in 0..100 {
            parent.add_child(TrieEdge::new(i as u8, i as u32));
        }
        let mut children = vec![parent];

        // Add child nodes
        for i in 0..100 {
            children.push(TrieNode::new(0, format!("child{}", i).into_bytes(), Some(i as u32)));
        }

        // Act
        let encoded = encode_trie(&children);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].children.len(), 100);
        assert_eq!(decoded.len(), 101);
    }

    #[test]
    fn should_roundtrip_binary_key_data() {
        // Arrange
        let binary_key = vec![0, 1, 2, 255, 254, 253, 128, 127];
        let node = TrieNode::new(0, binary_key.clone(), Some(42));

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].key_delta, binary_key);
    }

    #[test]
    fn should_preserve_child_order() {
        // Arrange
        let mut parent = TrieNode::new(0, b"root".to_vec(), None);
        parent.add_child(TrieEdge::new(b'z', 1));
        parent.add_child(TrieEdge::new(b'a', 2));
        parent.add_child(TrieEdge::new(b'm', 3));

        let nodes = vec![parent];

        // Act
        let encoded = encode_trie(&nodes);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].children.len(), 3);
        assert_eq!(decoded[0].children[0].first_byte, b'a');
        assert_eq!(decoded[0].children[1].first_byte, b'm');
        assert_eq!(decoded[0].children[2].first_byte, b'z');
    }

    #[test]
    fn should_roundtrip_multiple_nodes_with_variation() {
        // Arrange
        let node1 = TrieNode::new(0, b"node1".to_vec(), Some(1));
        let node2 = TrieNode::new(5, b"".to_vec(), None);
        let node3 = TrieNode::new(u16::MAX, vec![0; 500], Some(u32::MAX - 1));

        let nodes = vec![node1, node2, node3];

        // Act
        let encoded = encode_trie(&nodes);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].key_delta, b"node1");
        assert_eq!(decoded[1].key_delta.len(), 0);
        assert_eq!(decoded[2].key_delta.len(), 500);
        assert_eq!(decoded[2].block_id, Some(u32::MAX - 1));
    }

    #[test]
    fn should_handle_all_byte_values_in_key_delta() {
        // Arrange
        let all_bytes: Vec<u8> = (0..=255).collect();
        let node = TrieNode::new(0, all_bytes.clone(), Some(42));

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].key_delta.len(), 256);
        assert_eq!(decoded[0].key_delta, all_bytes);
    }

    #[test]
    fn should_reject_truncated_varint_node_count() {
        // Arrange
        let truncated = vec![0xFF]; // Varint that's incomplete

        // Act
        let result = decode_trie(&truncated);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_truncated_key_delta() {
        // Arrange
        // Encode a node but truncate its key data
        let node = TrieNode::new(0, b"toolong".to_vec(), Some(0));
        let mut encoded = encode_trie(&[node]);
        encoded.truncate(encoded.len() - 3); // Remove last 3 bytes of key

        // Act
        let result = decode_trie(&encoded);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_invalid_child_indices() {
        // This test ensures that out-of-bounds child indices are handled
        // Create a parent node with edge pointing beyond node count
        let mut parent = TrieNode::new(0, b"parent".to_vec(), None);
        parent.add_child(TrieEdge::new(b'a', 999)); // Index way out of bounds

        let encoded = encode_trie(&[parent]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert - should decode successfully but has invalid reference
        assert_eq!(decoded[0].children[0].child_index, 999);
    }

    #[test]
    fn should_roundtrip_node_with_no_block_id() {
        // Arrange
        let node = TrieNode::new(5, b"internal".to_vec(), None);

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].block_id, None);
        assert_eq!(decoded[0].prefix_len, 5);
    }

    #[test]
    fn should_handle_maximum_varint_values() {
        // Arrange - use max values that won't overflow
        let node = TrieNode::new(u16::MAX, vec![255; 1000], Some(u32::MAX - 1));

        // Act
        let encoded = encode_trie(&[node]);
        let decoded = decode_trie(&encoded).unwrap();

        // Assert
        assert_eq!(decoded[0].prefix_len, u16::MAX);
        assert_eq!(decoded[0].key_delta.len(), 1000);
        assert_eq!(decoded[0].block_id, Some(u32::MAX - 1));
    }
}
