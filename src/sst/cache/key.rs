//! Cache key type identifying a block in the cache
//!
//! A cache key uniquely identifies a block by combining:
//! - SST file ID (u64)
//! - Block offset within the SST (u64)
//! - Block type (Index, Data, Filter)
//!
//! Block types enable differentiated caching policies:
//! - Index blocks: Always admitted, protected from eviction
//! - Filter blocks: Always admitted, protected from eviction
//! - Data blocks: Admission control, evictable

use std::hash::{Hash, Hasher};
use std::convert::TryFrom;

/// Type of cached block
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum BlockType {
    /// Index block (sparse index, block index)
    Index,
    /// Data block (KV pairs)
    Data,
    /// Filter block (bloom filter)
    Filter,
}

/// Unique identifier for a cached block
///
/// Combines SST ID, block offset, and block type to uniquely identify a block
/// and enable type-aware caching policies.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct CacheKey {
    /// SST file ID
    pub sst_id: u64,
    /// Block offset in bytes within the SST file
    pub block_offset: u64,
    /// Type of block (determines caching policy)
    pub block_type: BlockType,
}

impl CacheKey {
    /// Create a new cache key for a block
    #[must_use]
    pub fn new(sst_id: u64, block_offset: u64, block_type: BlockType) -> Self {
        Self {
            sst_id,
            block_offset,
            block_type,
        }
    }

    /// Create a cache key for a data block
    #[must_use]
    pub fn for_data(sst_id: u64, block_offset: u64) -> Self {
        Self::new(sst_id, block_offset, BlockType::Data)
    }

    /// Create a cache key for an index block
    #[must_use]
    pub fn for_index(sst_id: u64, block_offset: u64) -> Self {
        Self::new(sst_id, block_offset, BlockType::Index)
    }

    /// Create a cache key for a filter block
    #[must_use]
    pub fn for_filter(sst_id: u64, block_offset: u64) -> Self {
        Self::new(sst_id, block_offset, BlockType::Filter)
    }

    /// Get the shard index for this key (`0..num_shards`)
    #[must_use]
    pub fn shard_index(&self, num_shards: usize) -> usize {
        // Use XOR combination of both fields for better distribution
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        usize::try_from(hasher.finish()).unwrap_or(0) % num_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_cache_key_with_values() {
        // Arrange

        // Act
        let key = CacheKey::for_data(42, 1024);

        // Assert
        assert_eq!(key.sst_id, 42);
        assert_eq!(key.block_offset, 1024);
        assert_eq!(key.block_type, BlockType::Data);
    }

    #[test]
    fn should_create_data_block_key() {
        // Arrange

        // Act
        let key = CacheKey::for_data(1, 100);

        // Assert
        assert_eq!(key.block_type, BlockType::Data);
    }

    #[test]
    fn should_create_index_block_key() {
        // Arrange

        // Act
        let key = CacheKey::for_index(1, 100);

        // Assert
        assert_eq!(key.block_type, BlockType::Index);
    }

    #[test]
    fn should_create_filter_block_key() {
        // Arrange

        // Act
        let key = CacheKey::for_filter(1, 100);

        // Assert
        assert_eq!(key.block_type, BlockType::Filter);
    }

    #[test]
    fn should_distinguish_keys_with_different_sst_ids() {
        // Arrange
        let key1 = CacheKey::for_data(1, 100);
        let key2 = CacheKey::for_data(2, 100);

        // Act

        // Assert
        assert_ne!(key1, key2);
    }

    #[test]
    fn should_distinguish_keys_with_different_offsets() {
        // Arrange
        let key1 = CacheKey::for_data(1, 100);
        let key2 = CacheKey::for_data(1, 200);

        // Act

        // Assert
        assert_ne!(key1, key2);
    }

    #[test]
    fn should_distinguish_keys_with_different_block_types() {
        // Arrange
        let key1 = CacheKey::for_data(1, 100);
        let key2 = CacheKey::for_index(1, 100);

        // Act

        // Assert
        assert_ne!(key1, key2);
    }

    #[test]
    fn should_recognize_identical_keys() {
        // Arrange
        let key1 = CacheKey::for_data(1, 100);
        let key2 = CacheKey::for_data(1, 100);

        // Act

        // Assert
        assert_eq!(key1, key2);
    }

    #[test]
    fn should_compute_consistent_shard_index() {
        // Arrange
        let key = CacheKey::for_data(1, 100);
        let num_shards = 16;

        // Act
        let index1 = key.shard_index(num_shards);
        let index2 = key.shard_index(num_shards);

        // Assert
        assert_eq!(index1, index2);
        assert!(index1 < num_shards);
    }

    #[test]
    fn should_distribute_keys_across_shards() {
        // Arrange
        let num_shards = 16;
        let mut seen_shards = std::collections::HashSet::new();

        // Act
        for i in 0..100 {
            let key = CacheKey::for_data(i, 0);
            let shard_idx = key.shard_index(num_shards);
            seen_shards.insert(shard_idx);
        }

        // Assert (with 100 keys and 16 shards, we should see multiple shards)
        assert!(
            seen_shards.len() > 1,
            "Keys should distribute across multiple shards"
        );
    }

    #[test]
    fn should_handle_shard_index_with_different_shard_counts() {
        // Arrange
        let key = CacheKey::for_data(42, 1024);

        // Act
        let index_4 = key.shard_index(4);
        let index_16 = key.shard_index(16);
        let index_32 = key.shard_index(32);

        // Assert
        assert!(index_4 < 4);
        assert!(index_16 < 16);
        assert!(index_32 < 32);
    }

    #[test]
    fn should_hash_consistently() {
        // Arrange
        let key = CacheKey::for_data(1, 100);

        // Act
        let mut hasher1 = std::collections::hash_map::DefaultHasher::new();
        let mut hasher2 = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher1);
        key.hash(&mut hasher2);

        // Assert
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn should_be_copyable() {
        // Arrange
        let key1 = CacheKey::for_data(1, 100);

        // Act
        let key2 = key1;
        let key3 = key1;

        // Assert (Copy trait allows this)
        assert_eq!(key1, key2);
        assert_eq!(key2, key3);
    }

    #[test]
    fn should_handle_max_u64_values() {
        // Arrange

        // Act
        let key = CacheKey::for_data(u64::MAX, u64::MAX);

        // Assert
        assert_eq!(key.sst_id, u64::MAX);
        assert_eq!(key.block_offset, u64::MAX);
        let _ = key.shard_index(16); // Should not panic
    }

    #[test]
    fn should_handle_zero_values() {
        // Arrange

        // Act
        let key = CacheKey::for_data(0, 0);

        // Assert
        assert_eq!(key.sst_id, 0);
        assert_eq!(key.block_offset, 0);
        let shard_idx = key.shard_index(16);
        assert!(shard_idx < 16);
    }
}
