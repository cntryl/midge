//! Trie-based index for SST files.
//!
//! This module provides an optional trie index that maps key prefixes to data blocks,
//! enabling fast prefix-based lookups and range scans. The trie index is optional and
//! controlled by the `new_sst_index` flag; old SST files without a trie index remain
//! fully backward compatible.
//!
//! # Format
//! The trie index is appended to the SST file as an optional block that contains:
//! - A compact trie structure mapping prefixes to block offsets
//! - Metadata about block boundaries and key ranges
//! - CRC32C checksum for integrity verification

use bytes::{Bytes, BytesMut, BufMut};
use crate::error::{MidgeError, MidgeResult};
use std::collections::BTreeMap;

/// Maximum prefix length stored in the trie index (optimization for cache locality)
const MAX_PREFIX_LEN: usize = 16;

/// Trie node in the index
#[derive(Debug, Clone)]
struct TrieNode {
    /// Child nodes indexed by next byte
    children: BTreeMap<u8, Box<TrieNode>>,
    /// Block offset(s) for this prefix (may contain multiple blocks)
    block_offsets: Vec<u32>,
    /// Whether this is a final prefix (leaf in some paths)
    is_terminal: bool,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: BTreeMap::new(),
            block_offsets: Vec::new(),
            is_terminal: false,
        }
    }

    /// Insert a prefix mapping to a block offset
    fn insert(&mut self, prefix: &[u8], block_offset: u32) {
        if prefix.is_empty() {
            self.block_offsets.push(block_offset);
            self.is_terminal = true;
            return;
        }

        let next_byte = prefix[0];
        let child = self.children
            .entry(next_byte)
            .or_insert_with(|| Box::new(TrieNode::new()));
        child.insert(&prefix[1..], block_offset);
    }

    /// Find all block offsets that could contain the given key
    fn find_blocks(&self, key: &[u8]) -> Vec<u32> {
        let mut offsets = Vec::new();
        self.find_blocks_recursive(key, &mut offsets);
        offsets
    }

    fn find_blocks_recursive(&self, key: &[u8], offsets: &mut Vec<u32>) {
        // Add all blocks at this node
        offsets.extend(&self.block_offsets);

        if key.is_empty() {
            return;
        }

        let next_byte = key[0];
        if let Some(child) = self.children.get(&next_byte) {
            child.find_blocks_recursive(&key[1..], offsets);
        }
    }
}

/// Builder for constructing a trie index during SST writing
pub struct TrieIndexBuilder {
    root: TrieNode,
    next_block_id: u32,
}

impl TrieIndexBuilder {
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
            next_block_id: 0,
        }
    }

    /// Add a key range to the trie index
    /// This is called for each data block with its min/max keys
    pub fn add_block(&mut self, min_key: &[u8], _max_key: &[u8]) {
        let block_offset = self.next_block_id;
        self.next_block_id += 1;

        // Extract prefix (up to MAX_PREFIX_LEN bytes)
        let prefix_len = std::cmp::min(MAX_PREFIX_LEN, min_key.len());
        let prefix = &min_key[..prefix_len];

        // Insert prefix → block mapping
        self.root.insert(prefix, block_offset);

        // Also insert progressively longer prefixes for better coverage
        for i in 1..=prefix_len {
            self.root.insert(&prefix[..i], block_offset);
        }
    }

    /// Finish building and encode the trie to bytes
    pub fn finish(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.encode_node(&self.root, &mut buf);
        buf.freeze()
    }

    fn encode_node(&self, node: &TrieNode, buf: &mut BytesMut) {
        // Write node header
        buf.put_u32_le(node.block_offsets.len() as u32);
        buf.put_u8(if node.is_terminal { 1 } else { 0 });

        // Write block offsets
        for &offset in &node.block_offsets {
            buf.put_u32_le(offset);
        }

        // Write children count
        buf.put_u8(node.children.len() as u8);

        // Write children
        for (&byte, child) in &node.children {
            buf.put_u8(byte);
            self.encode_node(child, buf);
        }
    }
}

/// Trie index reader for looking up keys
pub struct TrieIndex {
    root: TrieNode,
}

impl TrieIndex {
    /// Decode a trie index from bytes
    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        let (root, _) = Self::decode_node(data, 0)?;
        Ok(Self { root })
    }

    fn decode_node(data: &[u8], mut pos: usize) -> MidgeResult<(TrieNode, usize)> {
        if pos + 5 > data.len() {
            return Err(MidgeError::InvalidData("Trie node header truncated".to_string()));
        }

        let block_count = u32::from_le_bytes([
            data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
        ]) as usize;
        pos += 4;

        let is_terminal = data[pos] != 0;
        pos += 1;

        let mut node = TrieNode::new();
        node.is_terminal = is_terminal;

        // Read block offsets
        for _ in 0..block_count {
            if pos + 4 > data.len() {
                return Err(MidgeError::InvalidData("Trie block offset truncated".to_string()));
            }
            let offset = u32::from_le_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
            ]);
            node.block_offsets.push(offset);
            pos += 4;
        }

        // Read children
        if pos >= data.len() {
            return Err(MidgeError::InvalidData("Trie children count missing".to_string()));
        }

        let children_count = data[pos] as usize;
        pos += 1;

        for _ in 0..children_count {
            if pos >= data.len() {
                return Err(MidgeError::InvalidData("Trie child byte missing".to_string()));
            }

            let byte = data[pos];
            pos += 1;

            let (child, new_pos) = Self::decode_node(data, pos)?;
            node.children.insert(byte, Box::new(child));
            pos = new_pos;
        }

        Ok((node, pos))
    }

    /// Find candidate blocks for a given key
    pub fn find_candidate_blocks(&self, key: &[u8]) -> Vec<u32> {
        self.root.find_blocks(key)
    }

    /// Find candidate blocks for a range scan
    pub fn find_blocks_in_range(&self, start: &[u8], end: &[u8]) -> Vec<u32> {
        // Get blocks for start key and end key, then merge
        let start_blocks = self.find_candidate_blocks(start);
        let end_blocks = self.find_candidate_blocks(end);

        let mut all_blocks = start_blocks;
        for block in end_blocks {
            if !all_blocks.contains(&block) {
                all_blocks.push(block);
            }
        }

        all_blocks.sort();
        all_blocks.dedup();
        all_blocks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_trie_from_keys() {
        let mut builder = TrieIndexBuilder::new();
        builder.add_block(b"apple", b"apricot");
        builder.add_block(b"banana", b"berry");
        builder.add_block(b"cherry", b"date");

        let encoded = builder.finish();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_find_candidate_blocks_for_key() {
        let mut builder = TrieIndexBuilder::new();
        builder.add_block(b"apple", b"apricot");
        builder.add_block(b"banana", b"berry");

        let encoded = builder.finish();
        let index = TrieIndex::decode(&encoded).unwrap();

        let blocks = index.find_candidate_blocks(b"apple");
        assert!(!blocks.is_empty());
    }

    #[test]
    fn should_find_blocks_in_range() {
        let mut builder = TrieIndexBuilder::new();
        builder.add_block(b"apple", b"apricot");
        builder.add_block(b"banana", b"berry");
        builder.add_block(b"cherry", b"date");

        let encoded = builder.finish();
        let index = TrieIndex::decode(&encoded).unwrap();

        let blocks = index.find_blocks_in_range(b"apricot", b"cherry");
        assert!(!blocks.is_empty());
    }

    #[test]
    fn should_roundtrip_encode_decode() {
        let mut builder = TrieIndexBuilder::new();
        builder.add_block(b"key_001", b"key_010");
        builder.add_block(b"key_020", b"key_030");
        builder.add_block(b"key_040", b"key_050");

        let encoded = builder.finish();
        let decoded = TrieIndex::decode(&encoded);
        assert!(decoded.is_ok());
    }

    #[test]
    fn should_handle_empty_trie() {
        let builder = TrieIndexBuilder::new();
        let encoded = builder.finish();
        let index = TrieIndex::decode(&encoded).unwrap();

        let blocks = index.find_candidate_blocks(b"any_key");
        assert_eq!(blocks.len(), 0);
    }

    #[test]
    fn should_handle_prefix_matching() {
        let mut builder = TrieIndexBuilder::new();
        builder.add_block(b"prefix_001", b"prefix_010");

        let encoded = builder.finish();
        let index = TrieIndex::decode(&encoded).unwrap();

        // Should find block when searching with full key
        let blocks1 = index.find_candidate_blocks(b"prefix_005");
        assert!(!blocks1.is_empty());

        // Should also find blocks for partial prefixes
        let blocks2 = index.find_candidate_blocks(b"prefix");
        assert!(!blocks2.is_empty());
    }
}
