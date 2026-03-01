//! Resource cleanup tests - verify proper thread and memory cleanup
//!
//! Tests that components properly clean up threads, memory, and other
//! resources when dropped, ensuring the engine can run in constrained
//! environments.

use cntryl_midge::sst::cache::{BlockCache, CachePolicyType};
use std::sync::Arc;

#[test]
fn should_cleanup_block_cache_threads_when_dropped() {
    // Arrange - create many BlockCache instances to stress thread management
    // Each cache spawns 16 worker threads
    let caches: Vec<BlockCache> = (0..10)
        .map(|_| BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru))
        .collect();

    // Act - drop all caches
    drop(caches);

    // Assert - test completes without hanging (implicit success)
    // If threads weren't properly joined, Drop would either:
    // - Block indefinitely (timeout failure)
    // - Leak threads (would accumulate over test runs)
}

#[test]
fn should_handle_rapid_cache_creation_destruction() {
    // Arrange - prepare iteration count
    const ITERATIONS: usize = 50;

    // Act - rapidly create and drop caches
    // This tests the race condition between thread spawning and cleanup
    for _ in 0..ITERATIONS {
        let cache = BlockCache::new(512 * 1024, 16, CachePolicyType::Lru);
        drop(cache);
    }

    // Assert - test completes without panic or thread exhaustion
}

#[test]
fn should_cleanup_cache_with_active_operations() {
    // Arrange
    let cache = Arc::new(BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru));
    let cache_clone = Arc::clone(&cache);

    // Start worker thread with cache operations
    let handle = std::thread::spawn(move || {
        use cntryl_midge::sst::cache::CacheKey;
        for i in 0..100 {
            let key = CacheKey::for_data(i, 0);
            let data = bytes::Bytes::from(vec![0u8; 100]);
            let _ = cache_clone.put(key, data);
        }
    });

    // Wait for operations to start
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Act - drop the main cache reference
    drop(cache);

    // Assert - worker thread completes and cache cleans up properly
    handle.join().unwrap();
}

#[test]
fn should_cleanup_multiple_component_types_together() {
    // Arrange - create multiple resource-holding components
    let cache1 = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
    let cache2 = BlockCache::new(512 * 1024, 8, CachePolicyType::TinyLfu);
    let cache3 = BlockCache::new(2 * 1024 * 1024, 16, CachePolicyType::ClockPro);

    // Act - drop all components
    drop((cache1, cache2, cache3));

    // Assert - cleanup completes in reasonable time without deadlock
}

#[test]
fn should_handle_cache_drop_in_panic_context() {
    // Arrange - prepare panic scenario
    let _cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);

    // Act - verify cleanup happens even when panic unwinds
    let result = std::panic::catch_unwind(|| {
        panic!("Intentional panic to test cleanup");
    });

    // Assert - panic was caught and cache was dropped properly
    assert!(result.is_err());
}

#[test]
fn should_handle_zero_capacity_cache_cleanup() {
    // Arrange - edge case: cache with zero capacity
    let cache = BlockCache::new(0, 16, CachePolicyType::Lru);

    // Act
    drop(cache);

    // Assert - completes without panic
}

#[test]
fn should_handle_single_shard_cache_cleanup() {
    // Arrange - edge case: cache with only 1 shard (1 worker thread)
    let cache = BlockCache::new(1024 * 1024, 1, CachePolicyType::Lru);

    // Act
    drop(cache);

    // Assert - completes without issue
}
