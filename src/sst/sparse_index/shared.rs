//! Shared sparse index types

use crate::sst::types::BlockHandle;

/// A single entry in the sparse index (sampled key position)
///
/// Maps a key to the block range containing it.
#[derive(Clone, Debug)]
pub struct IndexEntry {
    /// The sampled key (not all keys, sparse sample)
    pub key: Vec<u8>,
    /// Block containing this key
    pub block_handle: BlockHandle,
    /// Block index in the SST
    pub block_index: usize,
}

impl IndexEntry {
    /// Create a new index entry
    pub fn new(key: Vec<u8>, block_handle: BlockHandle, block_index: usize) -> Self {
        Self {
            key,
            block_handle,
            block_index,
        }
    }

    /// Get the size of this entry in bytes (for serialization estimates)
    pub fn size_bytes(&self) -> usize {
        4 + self.key.len() + 16 + 8 // size + key + handle(offset+size) + index
    }
}

/// Block range information from index lookup
#[derive(Clone, Copy, Debug)]
pub struct BlockRange {
    /// First block index to check (lower bound)
    pub start_block: usize,
    /// Last block index to check (upper bound, inclusive)
    pub end_block: usize,
}

impl BlockRange {
    /// Create a new block range
    pub fn new(start_block: usize, end_block: usize) -> Self {
        Self {
            start_block,
            end_block,
        }
    }

    /// Number of blocks in this range
    pub fn block_count(&self) -> usize {
        self.end_block - self.start_block + 1
    }
}

