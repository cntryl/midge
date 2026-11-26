use bytes::Bytes;
use cntryl_midge_table::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use std::sync::Arc;

#[test]
fn should_cleanup_block_cache_resources_when_dropped() {
    let caches: Vec<BlockCache> = (0..10)
        .map(|shard_seed| {
            let cache = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
            for i in 0..20 {
                assert!(cache.put(
                    CacheKey::for_data(shard_seed, i),
                    &Bytes::from(vec![0; 128])
                ));
            }
            assert_eq!(cache.len(), 20);
            assert!(cache.size_bytes() > 0);
            cache
        })
        .collect();
    drop(caches);
}

#[test]
fn should_preserve_shared_cache_entries_when_another_owner_drops() {
    let cache = Arc::new(BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru));
    let cache_check = Arc::clone(&cache);
    let worker_cache = Arc::clone(&cache);
    let handle = std::thread::spawn(move || {
        (0..100)
            .filter(|i| worker_cache.put(CacheKey::for_data(*i, 0), &Bytes::from(vec![0; 100])))
            .count()
    });
    drop(cache);
    let successes = handle.join().expect("worker thread should not panic");
    assert_eq!(successes, 100);
    assert_eq!(cache_check.len(), successes);
}

#[test]
fn should_cleanup_multiple_component_types_together() {
    let cache1 = BlockCache::new(1024 * 1024, 16, CachePolicyType::Lru);
    let cache2 = BlockCache::new(512 * 1024, 8, CachePolicyType::TinyLfu);
    let cache3 = BlockCache::new(2 * 1024 * 1024, 16, CachePolicyType::ClockPro);
    let key = CacheKey::for_data(1, 0);
    let data = Bytes::from(vec![1; 64]);
    assert!(cache1.put(key, &data));
    assert!(cache2.put(key, &data));
    assert!(cache3.put(key, &data));
    assert_eq!(cache1.get(&key).map(|v| v.data), Some(data.clone()));
    assert_eq!(cache2.get(&key).map(|v| v.data), Some(data.clone()));
    assert_eq!(cache3.get(&key).map(|v| v.data), Some(data));
}

#[test]
fn should_handle_zero_capacity_cache_cleanup() {
    let cache = BlockCache::new(0, 16, CachePolicyType::Lru);
    assert!(!cache.put(CacheKey::for_data(1, 0), &Bytes::from(vec![0; 16])));
    assert!(cache.is_empty());
}

#[test]
fn should_handle_single_shard_cache_cleanup() {
    let cache = BlockCache::new(1024 * 1024, 1, CachePolicyType::Lru);
    assert_eq!(cache.num_shards(), 1);
    for i in 0..10 {
        let byte = u8::try_from(i).expect("test index fits in u8");
        assert!(cache.put(CacheKey::for_data(i, 0), &Bytes::from(vec![byte; 32])));
    }
    for i in 0..10 {
        let byte = u8::try_from(i).expect("test index fits in u8");
        assert_eq!(
            cache.get(&CacheKey::for_data(i, 0)).map(|v| v.data),
            Some(Bytes::from(vec![byte; 32]))
        );
    }
    assert_eq!(cache.len(), 10);
}
