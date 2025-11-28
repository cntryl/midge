//! Tier 1 — Hot Path Block Cache Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical block cache hot paths:
//! - Hot cache lookups and insertions
//! - Hit ratio calculations
//! - Hot tier cache lock-free fast path

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::block_cache::{
    create_basic_cache, create_hot_cache, BlockKey, BlockType, CachedBlock,
};
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn make_block_data(size: usize) -> Bytes {
    Bytes::from(vec![0xAB; size])
}

/// Benchmark block cache get operations on hot data
fn bench_block_cache_get_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_get_hot");
    group.measurement_time(std::time::Duration::from_millis(200));

    let cache = create_basic_cache(10 * 1024 * 1024); // 10MB cache
                                                      // Pre-populate with hot data
    for i in 0..1000 {
        let key = BlockKey {
            file_name: "test.sst".to_string(),
            block_type: BlockType::Data,
            offset: i,
        };
        let block = CachedBlock {
            data: make_block_data(4096),
        };
        cache.insert(key, block);
    }

    let hot_key = BlockKey {
        file_name: "test.sst".to_string(),
        block_type: BlockType::Data,
        offset: 42,
    };

    group.bench_function("get_hot_4k_block", |b| {
        b.iter(|| {
            let result = cache.get(&hot_key);
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark block cache insert operations
fn bench_block_cache_insert_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_insert_hot");
    group.measurement_time(std::time::Duration::from_millis(200));

    group.bench_function("insert_4k_block", |b| {
        b.iter_batched(
            || {
                let cache = create_basic_cache(10 * 1024 * 1024);
                // Pre-populate to simulate hot cache
                for i in 0..100 {
                    let key = BlockKey {
                        file_name: "test.sst".to_string(),
                        block_type: BlockType::Data,
                        offset: i,
                    };
                    let block = CachedBlock {
                        data: make_block_data(4096),
                    };
                    cache.insert(key, block);
                }
                cache
            },
            |cache| {
                let key = BlockKey {
                    file_name: "test.sst".to_string(),
                    block_type: BlockType::Data,
                    offset: 1000,
                };
                let block = CachedBlock {
                    data: make_block_data(4096),
                };
                cache.insert(key, block);
                black_box(&cache);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark hit ratio calculation (simplified)
fn bench_block_cache_hit_ratio_fast(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_hit_ratio_fast");
    group.measurement_time(std::time::Duration::from_millis(200));

    let cache = create_basic_cache(10 * 1024 * 1024);
    // Pre-populate
    for i in 0..100 {
        let key = BlockKey {
            file_name: "test.sst".to_string(),
            block_type: BlockType::Data,
            offset: i,
        };
        let block = CachedBlock {
            data: make_block_data(1024),
        };
        cache.insert(key, block);
    }

    // Precompute keys for accesses (all hits for simplicity)
    let access_keys: Vec<BlockKey> = (0..100)
        .map(|i| BlockKey {
            file_name: "test.sst".to_string(),
            block_type: BlockType::Data,
            offset: i,
        })
        .collect();

    group.bench_function("hit_ratio_calc_100_accesses", |b| {
        b.iter(|| {
            let mut hits = 0;
            for key in &access_keys {
                if cache.get(key).is_some() {
                    hits += 1;
                }
            }
            let ratio = hits as f64 / access_keys.len() as f64;
            black_box(ratio);
        })
    });

    group.finish();
}

/// Benchmark comparing basic cache vs hot tier cache for hot data lookups.
///
/// This demonstrates the benefit of the lock-free hot tier for frequently
/// accessed blocks.
fn bench_hot_tier_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_hot_tier_comparison");
    group.measurement_time(std::time::Duration::from_millis(200));

    // Setup: create both cache types with same data
    let basic_cache = create_basic_cache(10 * 1024 * 1024);
    let hot_cache = create_hot_cache(10 * 1024 * 1024);

    // Pre-populate with data - use static file name to avoid String allocation in hot path
    static FILE_NAME: &str = "test.sst";
    for i in 0..1000 {
        let key = BlockKey {
            file_name: FILE_NAME.to_string(),
            block_type: BlockType::Data,
            offset: i,
        };
        let block = CachedBlock {
            data: make_block_data(4096),
        };
        basic_cache.insert(key.clone(), block.clone());
        hot_cache.insert(key, block);
    }

    // Warm up the hot tier by accessing all keys once
    for i in 0..1000 {
        let key = BlockKey {
            file_name: FILE_NAME.to_string(),
            block_type: BlockType::Data,
            offset: i,
        };
        let _ = hot_cache.get(&key);
    }

    // Precompute the hot key (avoid allocation in benchmark loop)
    let hot_key = BlockKey {
        file_name: FILE_NAME.to_string(),
        block_type: BlockType::Data,
        offset: 42,
    };

    group.bench_function("basic_cache_get_hot", |b| {
        b.iter(|| {
            let result = basic_cache.get(&hot_key);
            black_box(result);
        })
    });

    group.bench_function("hot_tier_cache_get_hot", |b| {
        b.iter(|| {
            let result = hot_cache.get(&hot_key);
            black_box(result);
        })
    });

    // Also benchmark a batch of lookups to amortize overhead
    let hot_keys: Vec<BlockKey> = (0..100)
        .map(|i| BlockKey {
            file_name: FILE_NAME.to_string(),
            block_type: BlockType::Data,
            offset: i,
        })
        .collect();

    group.bench_function("basic_cache_get_100_hot", |b| {
        b.iter(|| {
            for key in &hot_keys {
                black_box(basic_cache.get(key));
            }
        })
    });

    group.bench_function("hot_tier_cache_get_100_hot", |b| {
        b.iter(|| {
            for key in &hot_keys {
                black_box(hot_cache.get(key));
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_block_cache_hot;
    config = criterion_config();
    targets = bench_block_cache_get_hot, bench_block_cache_insert_hot, bench_block_cache_hit_ratio_fast, bench_hot_tier_comparison
}
criterion_main!(tier1_hotpath_block_cache_hot);
