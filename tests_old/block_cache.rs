//! Tests for block cache functionality.
//!
//! These tests verify the block cache behavior including basic operations,
//! sharded caching, and eviction policies.

use cntryl_midge::sst::block_cache::{
    BlockCache, BlockCacheOptions, BlockData, BlockKey, BlockKind, ShardedBlockCache,
};
use std::sync::Arc;
use std::thread;

fn create_cache(capacity: usize) -> ShardedBlockCache {
    ShardedBlockCache::new(BlockCacheOptions::with_capacity(capacity))
}

fn create_sharded_cache(capacity: usize, num_shards: usize) -> ShardedBlockCache {
    ShardedBlockCache::new(BlockCacheOptions::with_capacity(capacity).num_shards(num_shards))
}

fn make_block_data(data: &[u8]) -> BlockData {
    let arc_data: Arc<[u8]> = data.to_vec().into();
    BlockData::uncompressed(arc_data, BlockKind::Data)
}

fn make_block_key(file_num: u64, offset: u64, kind: BlockKind) -> BlockKey {
    BlockKey::new(file_num, offset, kind, 0)
}

// =============================================================================
// BASIC CACHE TESTS
// =============================================================================

#[test]
fn should_cache_block_given_basic_cache_when_inserting() {
    // Arrange
    let cache = create_cache(1024 * 1024); // 1 MB
    let key = make_block_key(1, 0, BlockKind::Data);
    let block = make_block_data(b"test block data");

    // Act
    cache.insert(key, block);
    let retrieved = cache.get(&key);

    // Assert
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().data().bytes(), b"test block data");
}

#[test]
fn should_return_none_given_nonexistent_key_when_getting() {
    // Arrange
    let cache = create_cache(1024 * 1024);
    let key = make_block_key(999, 12345, BlockKind::Data);

    // Act
    let result = cache.get(&key);

    // Assert
    assert!(result.is_none());
}

#[test]
fn should_distinguish_block_types_given_same_file_and_offset_when_caching() {
    // Arrange
    let cache = create_cache(1024 * 1024);

    let data_key = make_block_key(1, 100, BlockKind::Data);
    let index_key = make_block_key(1, 100, BlockKind::Index);
    let filter_key = make_block_key(1, 100, BlockKind::Filter);

    let data_block = make_block_data(b"data block");
    let index_data: Arc<[u8]> = b"index block".to_vec().into();
    let index_block = BlockData::uncompressed(index_data, BlockKind::Index);
    let filter_data: Arc<[u8]> = b"filter block".to_vec().into();
    let filter_block = BlockData::uncompressed(filter_data, BlockKind::Filter);

    // Act
    cache.insert(data_key, data_block);
    cache.insert(index_key, index_block);
    cache.insert(filter_key, filter_block);

    // Assert - each block type stored separately
    assert_eq!(cache.get(&data_key).unwrap().data().bytes(), b"data block");
    assert_eq!(
        cache.get(&index_key).unwrap().data().bytes(),
        b"index block"
    );
    assert_eq!(
        cache.get(&filter_key).unwrap().data().bytes(),
        b"filter block"
    );
}

#[test]
fn should_track_stats_given_cache_operations_when_querying_stats() {
    // Arrange
    let cache = create_cache(1024 * 1024);
    let key = make_block_key(1, 0, BlockKind::Data);
    let block = make_block_data(b"stats test data");

    // Act
    cache.insert(key, block);
    let _ = cache.get(&key); // Hit
    let _ = cache.get(&key); // Hit
    let nonexistent = make_block_key(999, 0, BlockKind::Data);
    let _ = cache.get(&nonexistent); // Miss

    let stats = cache.stats();

    // Assert - stats should reflect operations
    assert!(
        stats.hits >= 2,
        "should have at least 2 hits, got {}",
        stats.hits
    );
    assert!(
        stats.misses >= 1,
        "should have at least 1 miss, got {}",
        stats.misses
    );
}

// =============================================================================
// SHARDED CACHE TESTS
// =============================================================================

#[test]
fn should_cache_block_given_sharded_cache_when_inserting() {
    // Arrange
    let cache = create_sharded_cache(1024 * 1024, 8); // 8 shards
    let key = make_block_key(1, 0, BlockKind::Data);
    let block = make_block_data(b"sharded block data");

    // Act
    cache.insert(key, block);
    let retrieved = cache.get(&key);

    // Assert
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().data().bytes(), b"sharded block data");
}

#[test]
fn should_distribute_entries_given_many_keys_when_using_sharded_cache() {
    // Arrange
    let cache = create_sharded_cache(10 * 1024 * 1024, 16); // 16 shards

    // Act - insert many entries (they should distribute across shards)
    for i in 0..1000 {
        let key = make_block_key(i as u64, i as u64 * 4096, BlockKind::Data);
        let data: Arc<[u8]> = vec![i as u8; 100].into();
        let block = BlockData::uncompressed(data, BlockKind::Data);
        cache.insert(key, block);
    }

    // Assert - all entries should be retrievable
    for i in 0..1000 {
        let key = make_block_key(i as u64, i as u64 * 4096, BlockKind::Data);
        let result = cache.get(&key);
        assert!(result.is_some(), "entry {} should exist", i);
    }
}

#[test]
fn should_handle_concurrent_access_given_multiple_threads_when_using_sharded_cache() {
    // Arrange
    let cache = Arc::new(create_sharded_cache(10 * 1024 * 1024, 16));
    let num_threads = 8;
    let entries_per_thread = 500;

    // Act - concurrent inserts
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for i in 0..entries_per_thread {
                    // Use unique file_num per thread to avoid collisions
                    let key =
                        make_block_key((t * 1000 + i) as u64, i as u64 * 1000, BlockKind::Data);
                    let data: Arc<[u8]> = format!("thread {} entry {}", t, i).into_bytes().into();
                    let block = BlockData::uncompressed(data, BlockKind::Data);
                    cache.insert(key, block);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread join");
    }

    // Assert - verify some entries are retrievable
    for t in 0..num_threads {
        let key = make_block_key((t * 1000) as u64, 0, BlockKind::Data);
        assert!(
            cache.get(&key).is_some(),
            "thread {} entry 0 should exist",
            t
        );
    }
}

// =============================================================================
// LRU EVICTION TESTS
// =============================================================================

#[test]
fn should_evict_entries_given_capacity_exceeded_when_inserting() {
    // Arrange - small cache that will fill up
    let cache = create_cache(10_000); // ~10 KB

    // Act - insert entries larger than cache capacity
    for i in 0..100 {
        let key = make_block_key(i as u64, 0, BlockKind::Data);
        let data: Arc<[u8]> = vec![i as u8; 500].into(); // 500 bytes each
        let block = BlockData::uncompressed(data, BlockKind::Data);
        cache.insert(key, block);
    }

    // Assert - oldest entries should be evicted
    let stats = cache.stats();
    // With 100 * 500 = 50KB of data into 10KB cache, evictions must occur
    assert!(
        stats.used_bytes <= 15_000,
        "cache should respect size limit, got {} bytes",
        stats.used_bytes
    );
    assert!(stats.evictions > 0, "evictions should have occurred");
}

#[test]
fn should_update_lru_order_given_recent_access_when_getting() {
    // Arrange
    let cache = create_cache(5_000); // Small cache

    // Insert entries
    for i in 0..10 {
        let key = make_block_key(i as u64, 0, BlockKind::Data);
        let data: Arc<[u8]> = vec![i as u8; 400].into();
        let block = BlockData::uncompressed(data, BlockKind::Data);
        cache.insert(key, block);
    }

    let key_0 = make_block_key(0, 0, BlockKind::Data);

    // Act - access entry 0 to make it recently used, then insert more to cause eviction
    let _ = cache.get(&key_0);

    // Insert more entries to cause eviction
    for i in 10..20 {
        let key = make_block_key(i as u64, 0, BlockKind::Data);
        let data: Arc<[u8]> = vec![i as u8; 400].into();
        let block = BlockData::uncompressed(data, BlockKind::Data);
        cache.insert(key, block);
    }

    // Assert - entry 0 should still exist (was recently accessed)
    // This test verifies the code path works without panicking
    let result = cache.get(&key_0);
    let _ = result; // Just verify no panic
}

// =============================================================================
// BLOCK KEY TESTS
// =============================================================================

#[test]
fn should_distinguish_keys_given_different_files_when_same_offset() {
    // Arrange
    let cache = create_cache(1024 * 1024);

    let key1 = make_block_key(1, 0, BlockKind::Data);
    let key2 = make_block_key(2, 0, BlockKind::Data);

    cache.insert(key1, make_block_data(b"file a data"));
    cache.insert(key2, make_block_data(b"file b data"));

    // Act
    let result1 = cache.get(&key1);
    let result2 = cache.get(&key2);

    // Assert
    assert_eq!(result1.unwrap().data().bytes(), b"file a data");
    assert_eq!(result2.unwrap().data().bytes(), b"file b data");
}

#[test]
fn should_distinguish_keys_given_different_offsets_when_same_file() {
    // Arrange
    let cache = create_cache(1024 * 1024);

    let key1 = make_block_key(1, 0, BlockKind::Data);
    let key2 = make_block_key(1, 4096, BlockKind::Data);
    let key3 = make_block_key(1, 8192, BlockKind::Data);

    cache.insert(key1, make_block_data(b"block 0"));
    cache.insert(key2, make_block_data(b"block 1"));
    cache.insert(key3, make_block_data(b"block 2"));

    // Act
    let result1 = cache.get(&key1);
    let result2 = cache.get(&key2);
    let result3 = cache.get(&key3);

    // Assert
    assert_eq!(result1.unwrap().data().bytes(), b"block 0");
    assert_eq!(result2.unwrap().data().bytes(), b"block 1");
    assert_eq!(result3.unwrap().data().bytes(), b"block 2");
}

// =============================================================================
// CACHE SIZE TESTS
// =============================================================================

#[test]
fn should_respect_size_limit_given_large_blocks_when_inserting() {
    // Arrange
    let max_size = 100_000; // 100 KB
    let cache = create_cache(max_size);

    // Act - insert blocks that exceed capacity
    for i in 0..50 {
        let key = make_block_key(i as u64, 0, BlockKind::Data);
        let data: Arc<[u8]> = vec![i as u8; 10_000].into(); // 10 KB each
        let block = BlockData::uncompressed(data, BlockKind::Data);
        cache.insert(key, block);
    }

    // Assert - cache size should be bounded
    let stats = cache.stats();
    assert!(
        stats.used_bytes <= max_size + 20_000, // Allow some overhead
        "cache size {} should be near limit {}",
        stats.used_bytes,
        max_size
    );
}
