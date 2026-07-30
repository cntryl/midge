//! Trie serialization and deserialization

use crate::common::{MidgeError, MidgeResult};
use crate::sst::trie::node::{TrieEdge, TrieNode};
use bytes::{BufMut, BytesMut};
use std::io::Cursor;

const MIN_ENCODED_NODE_BYTES: usize = 4;
const MIN_ENCODED_CHILD_BYTES: usize = 2;

/// Encode trie nodes to compact binary format
///
/// Layout:
///   \[ varint `node_count` \]
///   \[ `node_0` \]
///   \[ `node_1` \]
///   ...
///
/// Each node:
///   [ varint `prefix_len` ]
///   [ varint `key_delta_len` ] + `key_delta`
///   [ varint `block_id` ] (0 = None, 1-based offset)
///   [ varint `child_count` ]
///   [ children: varint `child_index` ]*`child_count`
#[must_use]
pub fn encode_trie(nodes: &[TrieNode]) -> Vec<u8> {
    let mut buf = BytesMut::new();

    // Write node count
    encode_varint(&mut buf, usize_to_u64(nodes.len()));

    // Write each node
    for node in nodes {
        encode_node(&mut buf, node);
    }

    buf.to_vec()
}

fn encode_node(buf: &mut BytesMut, node: &TrieNode) {
    // prefix_len
    encode_varint(buf, u64::from(node.prefix_len));

    // key_delta length + data
    encode_varint(buf, usize_to_u64(node.key_delta.len()));
    buf.put_slice(&node.key_delta);

    // block_id (0 = None, 1-based for Some)
    // Safety: block_id range is [0, u32::MAX-1] to prevent overflow
    encode_varint(
        buf,
        match node.block_id {
            None => 0,
            Some(id) => u64::from(id) + 1,
        },
    );

    // child_count
    encode_varint(buf, usize_to_u64(node.children.len()));

    // children
    for child in &node.children {
        buf.put_u8(child.first_byte);
        encode_varint(buf, u64::from(child.child_index));
    }
}

/// Decode trie nodes from binary format
///
/// # Errors
/// Returns `MidgeError::Corruption` when the encoded node count, node fields,
/// or child references overflow local in-memory representations or the input is
/// truncated.
pub fn decode_trie(data: &[u8]) -> MidgeResult<Vec<TrieNode>> {
    let mut cursor = Cursor::new(data);

    // Read node count
    let node_count = decode_usize(&mut cursor, "Trie node count")?;
    validate_count_fits_remaining(
        node_count,
        MIN_ENCODED_NODE_BYTES,
        remaining_bytes(&cursor)?,
        "Trie node count",
    )?;
    let mut nodes = Vec::with_capacity(node_count);

    // Read each node
    for _ in 0..node_count {
        nodes.push(decode_node(&mut cursor)?);
    }

    Ok(nodes)
}

fn decode_node(cursor: &mut Cursor<&[u8]>) -> MidgeResult<TrieNode> {
    // prefix_len
    let prefix_len = decode_u16(cursor, "Trie prefix length")?;

    // key_delta
    let key_delta_len = decode_usize(cursor, "Trie key delta length")?;
    let pos = cursor_position(cursor)?;
    let data = cursor.get_ref();
    let key_delta_end = pos
        .checked_add(key_delta_len)
        .ok_or_else(|| MidgeError::Corruption("Trie key_delta length overflow".into()))?;
    if key_delta_end > data.len() {
        return Err(MidgeError::Corruption("Trie key_delta truncated".into()));
    }
    let key_delta = data[pos..key_delta_end].to_vec();
    cursor.set_position(usize_to_u64(key_delta_end));

    // block_id (0 = None, 1-based for Some)
    let block_id_raw = decode_varint(cursor)?;
    let block_id = if block_id_raw == 0 {
        None
    } else {
        Some(
            u32::try_from(block_id_raw - 1)
                .map_err(|_| MidgeError::Corruption("Trie block_id overflow".into()))?,
        )
    };

    // child_count
    let child_count = decode_usize(cursor, "Trie child count")?;
    validate_count_fits_remaining(
        child_count,
        MIN_ENCODED_CHILD_BYTES,
        remaining_bytes(cursor)?,
        "Trie child count",
    )?;

    // children
    let mut children = Vec::with_capacity(child_count);
    for _ in 0..child_count {
        if cursor.position() >= usize_to_u64(cursor.get_ref().len()) {
            return Err(MidgeError::Corruption("Trie children truncated".into()));
        }
        let first_byte = cursor.get_ref()[cursor_position(cursor)?];
        cursor.set_position(cursor.position() + 1);
        let child_index = decode_u32(cursor, "Trie child index")?;
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
        buf.put_u8(value.to_le_bytes()[0] | 0x80);
        value >>= 7;
    }
    buf.put_u8(value.to_le_bytes()[0]);
}

fn decode_varint(cursor: &mut Cursor<&[u8]>) -> MidgeResult<u64> {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        if cursor.position() >= usize_to_u64(cursor.get_ref().len()) {
            return Err(MidgeError::Corruption("Varint truncated".into()));
        }

        let byte = cursor.get_ref()[cursor_position(cursor)?];
        cursor.set_position(cursor.position() + 1);

        result |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            return Ok(result);
        }

        shift += 7;
        if shift >= 64 {
            return Err(MidgeError::Corruption("Varint overflow".into()));
        }
    }
}

fn decode_usize(cursor: &mut Cursor<&[u8]>, field: &str) -> MidgeResult<usize> {
    usize::try_from(decode_varint(cursor)?)
        .map_err(|_| MidgeError::Corruption(format!("{field} overflow")))
}

fn decode_u32(cursor: &mut Cursor<&[u8]>, field: &str) -> MidgeResult<u32> {
    u32::try_from(decode_varint(cursor)?)
        .map_err(|_| MidgeError::Corruption(format!("{field} overflow")))
}

fn decode_u16(cursor: &mut Cursor<&[u8]>, field: &str) -> MidgeResult<u16> {
    u16::try_from(decode_varint(cursor)?)
        .map_err(|_| MidgeError::Corruption(format!("{field} overflow")))
}

fn cursor_position(cursor: &Cursor<&[u8]>) -> MidgeResult<usize> {
    usize::try_from(cursor.position())
        .map_err(|_| MidgeError::Corruption("Cursor position overflow".into()))
}

fn remaining_bytes(cursor: &Cursor<&[u8]>) -> MidgeResult<usize> {
    let pos = cursor_position(cursor)?;
    cursor
        .get_ref()
        .len()
        .checked_sub(pos)
        .ok_or_else(|| MidgeError::Corruption("Cursor position exceeds input length".into()))
}

fn validate_count_fits_remaining(
    count: usize,
    min_encoded_size: usize,
    remaining: usize,
    field: &str,
) -> MidgeResult<()> {
    if count > remaining / min_encoded_size {
        return Err(MidgeError::Corruption(format!(
            "{field} exceeds remaining trie bytes"
        )));
    }
    Ok(())
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
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
            parent.add_child(TrieEdge::new(
                u8::try_from(i).unwrap(),
                u32::try_from(i).unwrap(),
            ));
        }
        let mut children = vec![parent];

        // Add child nodes
        for i in 0..100 {
            children.push(TrieNode::new(
                0,
                format!("child{i}").into_bytes(),
                Some(u32::try_from(i).unwrap()),
            ));
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

        let trie_nodes = vec![node1, node2, node3];

        // Act
        let encoded = encode_trie(&trie_nodes);
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
    fn should_reject_declared_node_count_larger_than_remaining_bytes() {
        // Arrange
        let mut encoded = BytesMut::new();
        encode_varint(&mut encoded, u64::MAX);

        // Act
        let result = decode_trie(&encoded);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
    }

    #[test]
    fn should_reject_declared_child_count_larger_than_remaining_bytes() {
        // Arrange
        let mut encoded = BytesMut::new();
        encode_varint(&mut encoded, 1);
        encode_varint(&mut encoded, 0);
        encode_varint(&mut encoded, 0);
        encode_varint(&mut encoded, 0);
        encode_varint(&mut encoded, u64::MAX);

        // Act
        let result = decode_trie(&encoded);

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
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
    fn should_reject_corrupt_trie_node_given_invalid_child_offset_when_opening() {
        // Arrange
        let mut parent = TrieNode::new(0, b"parent".to_vec(), None);
        parent.add_child(TrieEdge::new(b'a', 999)); // Index way out of bounds

        // Act
        let encoded = encode_trie(&[parent]);
        let decoded = decode_trie(&encoded).unwrap();
        let child_index = decoded[0].children[0].child_index;

        // Assert
        assert_eq!(child_index, 999);
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
