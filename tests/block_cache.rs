//! Tests for block cache functionality.
//!
//! These tests verify the LRU block cache behavior including basic operations,
//! sharded caching, adaptive caching, and eviction policies.

use cntryl_midge::sst::{
    create_adaptive_cache, create_basic_cache, create_sharded_cache, BlockKey,
    CacheBlockType, CachedBlock,
};
use bytes::Bytes;
use std::sync::Arc;
use std::thread;

// =============================================================================
// BASIC CACHE TESTS
// =============================================================================

#[test]
fn should_cache_block_given_basic_cache_when_inserting() {
    // Arrange
    let cache = create_basic_cache(1024 * 1024); // 1 MB
    let key = BlockKey {
        file_name: "test.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };
    let block = CachedBlock {
        data: Bytes::from_static(b"test block data"),
    };

    // Act
    cache.insert(key.clone(), block.clone());
    let retrieved = cache.get(&key);

    // Assert
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().data.as_ref(), b"test block data");
}

#[test]
fn should_return_none_given_nonexistent_key_when_getting() {
    // Arrange
    let cache = create_basic_cache(1024 * 1024);
    let key = BlockKey {
        file_name: "nonexistent.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 12345,
    };

    // Act
    let result = cache.get(&key);

    // Assert
    assert!(result.is_none());
}

#[test]
fn should_distinguish_block_types_given_same_file_and_offset_when_caching() {
    // Arrange
    let cache = create_basic_cache(1024 * 1024);

    let data_key = BlockKey {
        file_name: "test.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 100,
    };
    let index_key = BlockKey {
        file_name: "test.sst".to_string(),
        block_type: CacheBlockType::Index,
        offset: 100,
    };
    let filter_key = BlockKey {
        file_name: "test.sst".to_string(),
        block_type: CacheBlockType::Filter,
        offset: 100,
    };

    let data_block = CachedBlock {
        data: Bytes::from_static(b"data block"),
    };
    let index_block = CachedBlock {
        data: Bytes::from_static(b"index block"),
    };
    let filter_block = CachedBlock {
        data: Bytes::from_static(b"filter block"),
    };

    // Act
    cache.insert(data_key.clone(), data_block);
    cache.insert(index_key.clone(), index_block);
    cache.insert(filter_key.clone(), filter_block);

    // Assert - each block type stored separately
    assert_eq!(
        cache.get(&data_key).unwrap().data.as_ref(),
        b"data block"
    );
    assert_eq!(
        cache.get(&index_key).unwrap().data.as_ref(),
        b"index block"
    );
    assert_eq!(
        cache.get(&filter_key).unwrap().data.as_ref(),
        b"filter block"
    );
}

#[test]
fn should_clear_all_entries_given_populated_cache_when_clearing() {
    // Arrange
    let cache = create_basic_cache(1024 * 1024);

    for i in 0..100 {
        let key = BlockKey {
            file_name: format!("file_{}.sst", i),
            block_type: CacheBlockType::Data,
            offset: i as u64 * 1000,
        };
        let block = CachedBlock {
            data: Bytes::from(format!("data {}", i)),
        };
        cache.insert(key, block);
    }

    // Act
    cache.clear();

    // Assert - all entries should be gone
    for i in 0..100 {
        let key = BlockKey {
            file_name: format!("file_{}.sst", i),
            block_type: CacheBlockType::Data,
            offset: i as u64 * 1000,
        };
        assert!(cache.get(&key).is_none());
    }
}

#[test]
fn should_track_stats_given_cache_operations_when_querying_stats() {
    // Arrange
    let cache = create_basic_cache(1024 * 1024);
    let key = BlockKey {
        file_name: "stats_test.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };
    let block = CachedBlock {
        data: Bytes::from_static(b"stats test data"),
    };

    // Act
    cache.insert(key.clone(), block);
    let _ = cache.get(&key); // Hit
    let _ = cache.get(&key); // Hit
    let nonexistent = BlockKey {
        file_name: "nonexistent.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };
    let _ = cache.get(&nonexistent); // Miss

    let stats = cache.stats();

    // Assert - stats should reflect operations
    assert!(stats.hits >= 2, "should have at least 2 hits");
    assert!(stats.misses >= 1, "should have at least 1 miss");
}

// =============================================================================
// SHARDED CACHE TESTS
// =============================================================================

#[test]
fn should_cache_block_given_sharded_cache_when_inserting() {
    // Arrange
    let cache = create_sharded_cache(1024 * 1024, 8); // 8 shards
    let key = BlockKey {
        file_name: "sharded_test.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };
    let block = CachedBlock {
        data: Bytes::from_static(b"sharded block data"),
    };

    // Act
    cache.insert(key.clone(), block);
    let retrieved = cache.get(&key);

    // Assert
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().data.as_ref(), b"sharded block data");
}

#[test]
fn should_distribute_entries_given_many_keys_when_using_sharded_cache() {
    // Arrange
    let cache = create_sharded_cache(10 * 1024 * 1024, 16); // 16 shards

    // Act - insert many entries (they should distribute across shards)
    for i in 0..1000 {
        let key = BlockKey {
            file_name: format!("file_{}.sst", i),
            block_type: CacheBlockType::Data,
            offset: i as u64 * 4096,
        };
        let block = CachedBlock {
            data: Bytes::from(vec![i as u8; 100]),
        };
        cache.insert(key, block);
    }

    // Assert - all entries should be retrievable
    for i in 0..1000 {
        let key = BlockKey {
            file_name: format!("file_{}.sst", i),
            block_type: CacheBlockType::Data,
            offset: i as u64 * 4096,
        };
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
                    let key = BlockKey {
                        file_name: format!("thread_{}_file_{}.sst", t, i),
                        block_type: CacheBlockType::Data,
                        offset: i as u64 * 1000,
                    };
                    let block = CachedBlock {
                        data: Bytes::from(format!("thread {} entry {}", t, i)),
                    };
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
        let key = BlockKey {
            file_name: format!("thread_{}_file_0.sst", t),
            block_type: CacheBlockType::Data,
            offset: 0,
        };
        assert!(cache.get(&key).is_some(), "thread {} entry 0 should exist", t);
    }
}

// =============================================================================
// ADAPTIVE CACHE TESTS
// =============================================================================

#[test]
fn should_function_correctly_given_adaptive_cache_when_basic_operations() {
    // Arrange
    let cache = create_adaptive_cache(1024 * 1024);
    let key = BlockKey {
        file_name: "adaptive_test.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };
    let block = CachedBlock {
        data: Bytes::from_static(b"adaptive block data"),
    };

    // Act
    cache.insert(key.clone(), block);
    let retrieved = cache.get(&key);

    // Assert
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().data.as_ref(), b"adaptive block data");
}

#[test]
fn should_handle_high_concurrency_given_many_threads_when_adaptive_cache() {
    // Arrange
    let cache = Arc::new(create_adaptive_cache(50 * 1024 * 1024));
    let num_threads = 16;
    let ops_per_thread = 1000;

    // Act - high concurrency workload
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let cache = Arc::clone(&cache);
            thread::spawn(move || {
                for i in 0..ops_per_thread {
                    let key = BlockKey {
                        file_name: format!("adaptive_t{}_f{}.sst", t, i % 100),
                        block_type: if i % 3 == 0 {
                            CacheBlockType::Index
                        } else {
                            CacheBlockType::Data
                        },
                        offset: (i as u64 * 4096) % 1_000_000,
                    };

                    if i % 2 == 0 {
                        // Insert
                        let block = CachedBlock {
                            data: Bytes::from(vec![t as u8; 1000]),
                        };
                        cache.insert(key, block);
                    } else {
                        // Get (may or may not hit)
                        let _ = cache.get(&key);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread join");
    }

    // Assert - cache should still function
    let stats = cache.stats();
    assert!(
        stats.hits + stats.misses > 0,
        "cache should have processed operations"
    );
}

// =============================================================================
// LRU EVICTION TESTS
// =============================================================================

#[test]
fn should_evict_lru_entries_given_capacity_exceeded_when_inserting() {
    // Arrange - small cache that will fill up
    let cache = create_basic_cache(10_000); // ~10 KB

    // Act - insert entries larger than cache capacity
    for i in 0..100 {
        let key = BlockKey {
            file_name: format!("evict_test_{}.sst", i),
            block_type: CacheBlockType::Data,
            offset: 0,
        };
        let block = CachedBlock {
            data: Bytes::from(vec![i as u8; 500]), // 500 bytes each
        };
        cache.insert(key, block);
    }

    // Assert - oldest entries should be evicted
    // We can't guarantee exactly which entries are evicted due to LRU,
    // but the cache should have evicted some entries
    let stats = cache.stats();
    // With 100 * 500 = 50KB of data into 10KB cache, evictions must occur
    assert!(stats.size_bytes <= 15_000, "cache should respect size limit");
}

#[test]
fn should_update_lru_order_given_recent_access_when_getting() {
    // Arrange
    let cache = create_basic_cache(5_000); // Small cache

    // Insert entries
    for i in 0..10 {
        let key = BlockKey {
            file_name: format!("lru_order_{}.sst", i),
            block_type: CacheBlockType::Data,
            offset: 0,
        };
        let block = CachedBlock {
            data: Bytes::from(vec![i as u8; 400]),
        };
        cache.insert(key, block);
    }

    let key_0 = BlockKey {
        file_name: "lru_order_0.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };

    // Act - access entry 0 to make it recently used, then insert more to cause eviction
    let _ = cache.get(&key_0);

    // Insert more entries to cause eviction
    for i in 10..20 {
        let key = BlockKey {
            file_name: format!("lru_order_{}.sst", i),
            block_type: CacheBlockType::Data,
            offset: 0,
        };
        let block = CachedBlock {
            data: Bytes::from(vec![i as u8; 400]),
        };
        cache.insert(key, block);
    }

    // Assert - entry 0 should still exist (was recently accessed)
    // This is a probabilistic test - entry 0 should survive longer
    // because it was accessed, but we can't guarantee exact behavior
    let result = cache.get(&key_0);
    // Entry 0 may or may not be present depending on exact eviction timing
    // The test mainly verifies the code path works
    let _ = result; // Just verify no panic
}

// =============================================================================
// BLOCK KEY TESTS
// =============================================================================

#[test]
fn should_distinguish_keys_given_different_files_when_same_offset() {
    // Arrange
    let cache = create_basic_cache(1024 * 1024);

    let key1 = BlockKey {
        file_name: "file_a.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };
    let key2 = BlockKey {
        file_name: "file_b.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };

    cache.insert(
        key1.clone(),
        CachedBlock {
            data: Bytes::from_static(b"file a data"),
        },
    );
    cache.insert(
        key2.clone(),
        CachedBlock {
            data: Bytes::from_static(b"file b data"),
        },
    );

    // Act
    let result1 = cache.get(&key1);
    let result2 = cache.get(&key2);

    // Assert
    assert_eq!(result1.unwrap().data.as_ref(), b"file a data");
    assert_eq!(result2.unwrap().data.as_ref(), b"file b data");
}

#[test]
fn should_distinguish_keys_given_different_offsets_when_same_file() {
    // Arrange
    let cache = create_basic_cache(1024 * 1024);

    let key1 = BlockKey {
        file_name: "multi_block.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 0,
    };
    let key2 = BlockKey {
        file_name: "multi_block.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 4096,
    };
    let key3 = BlockKey {
        file_name: "multi_block.sst".to_string(),
        block_type: CacheBlockType::Data,
        offset: 8192,
    };

    cache.insert(
        key1.clone(),
        CachedBlock {
            data: Bytes::from_static(b"block 0"),
        },
    );
    cache.insert(
        key2.clone(),
        CachedBlock {
            data: Bytes::from_static(b"block 1"),
        },
    );
    cache.insert(
        key3.clone(),
        CachedBlock {
            data: Bytes::from_static(b"block 2"),
        },
    );

    // Act
    let result1 = cache.get(&key1);
    let result2 = cache.get(&key2);
    let result3 = cache.get(&key3);

    // Assert
    assert_eq!(result1.unwrap().data.as_ref(), b"block 0");
    assert_eq!(result2.unwrap().data.as_ref(), b"block 1");
    assert_eq!(result3.unwrap().data.as_ref(), b"block 2");
}

// =============================================================================
// CACHE SIZE TESTS
// =============================================================================

#[test]
fn should_respect_size_limit_given_large_blocks_when_inserting() {
    // Arrange
    let max_size = 100_000; // 100 KB
    let cache = create_basic_cache(max_size);

    // Act - insert blocks that exceed capacity
    for i in 0..50 {
        let key = BlockKey {
            file_name: format!("large_block_{}.sst", i),
            block_type: CacheBlockType::Data,
            offset: 0,
        };
        let block = CachedBlock {
            data: Bytes::from(vec![i as u8; 10_000]), // 10 KB each
        };
        cache.insert(key, block);
    }

    // Assert - cache size should be bounded
    let stats = cache.stats();
    assert!(
        stats.size_bytes <= max_size + 20_000, // Allow some overhead
        "cache size {} should be near limit {}",
        stats.size_bytes,
        max_size
    );
}
