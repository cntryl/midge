//! Cache key type identifying a block in the cache
//!
//! A cache key uniquely identifies a block by combining:
//! - SST file ID (u64)
//! - Block offset within the SST (u64)

use std::hash::{Hash, Hasher};

/// Unique identifier for a cached block
///
/// Combines SST ID and block offset to uniquely identify a block within the database.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CacheKey {
    /// SST file ID
    pub sst_id: u64,
    /// Block offset in bytes within the SST file
    pub block_offset: u64,
}

impl CacheKey {
    /// Create a new cache key for a block
    pub fn new(sst_id: u64, block_offset: u64) -> Self {
        Self { sst_id, block_offset }
    }

    /// Get the shard index for this key (0..num_shards)
    pub fn shard_index(&self, num_shards: usize) -> usize {
        // Use XOR combination of both fields for better distribution
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        (hasher.finish() as usize) % num_shards
    }
}
