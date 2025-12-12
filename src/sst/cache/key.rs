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
        Self {
            sst_id,
            block_offset,
        }
    }

    /// Get the shard index for this key (0..num_shards)
    pub fn shard_index(&self, num_shards: usize) -> usize {
        // Use XOR combination of both fields for better distribution
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        (hasher.finish() as usize) % num_shards
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_cache_key_with_values() {
        // Arrange & Act
        let key = CacheKey::new(42, 1024);

        // Assert
        assert_eq!(key.sst_id, 42);
        assert_eq!(key.block_offset, 1024);
    }

    #[test]
    fn should_distinguish_keys_with_different_sst_ids() {
        // Arrange
        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(2, 100);

        // Assert
        assert_ne!(key1, key2);
    }

    #[test]
    fn should_distinguish_keys_with_different_offsets() {
        // Arrange
        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(1, 200);

        // Assert
        assert_ne!(key1, key2);
    }

    #[test]
    fn should_recognize_identical_keys() {
        // Arrange
        let key1 = CacheKey::new(1, 100);
        let key2 = CacheKey::new(1, 100);

        // Assert
        assert_eq!(key1, key2);
    }

    #[test]
    fn should_compute_consistent_shard_index() {
        // Arrange
        let key = CacheKey::new(1, 100);
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
            let key = CacheKey::new(i, 0);
            let shard_idx = key.shard_index(num_shards);
            seen_shards.insert(shard_idx);
        }

        // Assert (with 100 keys and 16 shards, we should see multiple shards)
        assert!(seen_shards.len() > 1, "Keys should distribute across multiple shards");
    }

    #[test]
    fn should_handle_shard_index_with_different_shard_counts() {
        // Arrange
        let key = CacheKey::new(42, 1024);

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
        let key = CacheKey::new(1, 100);

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
        let key1 = CacheKey::new(1, 100);

        // Act
        let key2 = key1;
        let key3 = key1;

        // Assert (Copy trait allows this)
        assert_eq!(key1, key2);
        assert_eq!(key2, key3);
    }

    #[test]
    fn should_handle_max_u64_values() {
        // Arrange & Act
        let key = CacheKey::new(u64::MAX, u64::MAX);

        // Assert
        assert_eq!(key.sst_id, u64::MAX);
        assert_eq!(key.block_offset, u64::MAX);
        let _ = key.shard_index(16); // Should not panic
    }

    #[test]
    fn should_handle_zero_values() {
        // Arrange & Act
        let key = CacheKey::new(0, 0);

        // Assert
        assert_eq!(key.sst_id, 0);
        assert_eq!(key.block_offset, 0);
        let shard_idx = key.shard_index(16);
        assert!(shard_idx < 16);
    }
}
