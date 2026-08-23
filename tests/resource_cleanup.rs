//! Resource cleanup tests - verify proper memory and handle cleanup
//!
//! Tests that components properly clean up memory and other resources when
//! dropped, ensuring the engine can run in constrained environments.

use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use std::sync::Arc;

#[test]
fn should_cleanup_block_cache_resources_when_dropped() {
    // Arrange - create many BlockCache instances and populate each so there
    // is real state for cleanup to discard.
    let caches: Vec<BlockCache> = (0..10)
        .map(|shard_seed| {
            let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
            for i in 0..20 {
                let key = CacheKey::for_data(shard_seed, i);
                let data = bytes::Bytes::from(vec![0u8; 128]);
                assert!(cache.put(key, &data), "put should succeed under capacity");
            }
            cache
        })
        .collect();

    // Sanity check: every cache actually holds the entries we just inserted.
    for cache in &caches {
        assert_eq!(cache.len(), 20);
        assert!(cache.size_bytes() > 0);
    }

    // Act - drop all caches
    drop(caches);
    // Assert (implicit) - Drop for BlockCache/CacheShard runs without panicking
    // or hanging even while every shard holds populated entries.
}

#[test]
fn should_cleanup_cache_with_active_operations() {
    // Arrange
    let cache = Arc::new(BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru));
    let cache_clone = Arc::clone(&cache);
    // Keep a second surviving handle so we can inspect cache state after the
    // original reference is dropped mid-operation.
    let cache_check = Arc::clone(&cache);

    // Start a thread with cache operations
    let handle = std::thread::spawn(move || {
        let mut successes = 0usize;
        for i in 0..100 {
            let key = CacheKey::for_data(i, 0);
            let data = bytes::Bytes::from(vec![0u8; 100]);
            if cache_clone.put(key, &data) {
                successes += 1;
            }
        }
        successes
    });

    // Wait for operations to start
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Act - drop the main cache reference while the worker is still writing
    drop(cache);

    // Assert - all inserts succeeded and are visible through the surviving
    // Arc handle, proving the shared cache state survived the concurrent drop.
    let successes = handle.join().expect("worker thread should not panic");
    assert_eq!(
        successes, 100,
        "all puts should have succeeded under capacity"
    );
    assert_eq!(
        cache_check.len(),
        successes,
        "surviving cache handle should see exactly the entries the worker inserted"
    );
}

#[test]
fn should_cleanup_multiple_component_types_together() {
    // Arrange - create multiple caches, one per eviction policy
    let cache1 = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
    let cache2 = BlockCache::new(512 * 1024, 8, CachePolicyType::TinyLfu);
    let cache3 = BlockCache::new(2 * 1024 * 1024, 16, CachePolicyType::ClockPro);

    // Act - exercise each cache so we know it actually holds live state
    // before the components are dropped together.
    let key = CacheKey::for_data(1, 0);
    let data = bytes::Bytes::from(vec![1u8; 64]);
    assert!(cache1.put(key, &data));
    assert!(cache2.put(key, &data));
    assert!(cache3.put(key, &data));

    assert_eq!(
        cache1.get(&key).map(|v| (*v.data).clone()),
        Some(data.clone())
    );
    assert_eq!(
        cache2.get(&key).map(|v| (*v.data).clone()),
        Some(data.clone())
    );
    assert_eq!(
        cache3.get(&key).map(|v| (*v.data).clone()),
        Some(data.clone())
    );

    // Assert - drop all three populated components together without deadlock
    drop((cache1, cache2, cache3));
}

#[test]
fn should_handle_zero_capacity_cache_cleanup() {
    // Arrange - edge case: cache with zero capacity
    let cache = BlockCache::new(0, 16, CachePolicyType::Lru);
    let key = CacheKey::for_data(1, 0);
    let data = bytes::Bytes::from(vec![0u8; 16]);

    // Act
    let accepted = cache.put(key, &data);

    // Assert - a non-empty value can never fit in a zero-capacity cache, so
    // put must be rejected and the cache must stay empty.
    assert!(
        !accepted,
        "put should be rejected when it cannot fit under zero capacity"
    );
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());

    drop(cache);
}

#[test]
fn should_handle_single_shard_cache_cleanup() {
    // Arrange - edge case: cache with only 1 shard (1 worker thread)
    let cache = BlockCache::new(1024 * 1024, 1, CachePolicyType::Lru);
    assert_eq!(cache.num_shards(), 1);

    // Act - insert entries that would land in different shards under a
    // multi-shard cache, to confirm single-shard routing still round-trips.
    for i in 0..10 {
        let key = CacheKey::for_data(i, 0);
        let byte = u8::try_from(i).expect("test index fits in u8");
        let data = bytes::Bytes::from(vec![byte; 32]);
        assert!(cache.put(key, &data));
    }

    // Assert - all entries are present and readable back
    for i in 0..10 {
        let key = CacheKey::for_data(i, 0);
        let byte = u8::try_from(i).expect("test index fits in u8");
        let expected = bytes::Bytes::from(vec![byte; 32]);
        assert_eq!(cache.get(&key).map(|v| (*v.data).clone()), Some(expected));
    }
    assert_eq!(cache.len(), 10);

    drop(cache);
}
