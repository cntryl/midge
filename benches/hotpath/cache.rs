//! Tier 1 — Hot Path Cache Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers block cache operations (critical read path)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::{
    create_basic_cache, BlockKey, CacheBlockType, CachedBlock,
};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn make_block_key(file_num: usize, offset: u64) -> BlockKey {
    BlockKey {
        file_name: format!("sst_{:06}.sst", file_num),
        block_type: CacheBlockType::Data,
        offset,
    }
}

fn make_cached_block(size: usize) -> CachedBlock {
    let data = vec![0u8; size];
    CachedBlock {
        data: Bytes::from(data),
    }
}

/// Benchmark cache insert operations
fn bench_cache_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_insert");

    let cache_size = 10 * 1024 * 1024; // 10 MB
    let block_size = 4 * 1024; // 4 KB

    for &num_blocks in &[100, 1_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            &num_blocks,
            |b, &n| {
                b.iter_batched(
                    || create_basic_cache(cache_size),
                    |cache| {
                        for i in 0..n {
                            let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
                            let block = make_cached_block(block_size);
                            cache.insert(key, block);
                            black_box(());
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark cache get operations (hot path for every read)
fn bench_cache_get_hit(c: &mut Criterion) {
    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Pre-populate cache
    let cache = create_basic_cache(cache_size);
    for i in 0..num_blocks {
        let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
        let block = make_cached_block(block_size);
        cache.insert(key, block);
    }

    c.bench_function("hotpath_cache_get_hit", |b| {
        b.iter(|| {
            let mut count = 0;
            for i in 0..num_blocks {
                let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
                if cache.get(&key).is_some() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });
}

/// Benchmark cache get operations on missing keys (cache misses)
fn bench_cache_get_miss(c: &mut Criterion) {
    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Pre-populate cache with keys 0..1000
    let cache = create_basic_cache(cache_size);
    for i in 0..num_blocks {
        let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
        let block = make_cached_block(block_size);
        cache.insert(key, block);
    }

    c.bench_function("hotpath_cache_get_miss", |b| {
        b.iter(|| {
            // Query keys that don't exist (1000..2000)
            let mut count = 0;
            for i in num_blocks..num_blocks * 2 {
                let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
                if cache.get(&key).is_some() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });
}

/// Benchmark cache eviction under memory pressure
fn bench_cache_eviction(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_eviction");

    // Small cache to trigger eviction (2 MB, holds ~512 4KB blocks)
    let cache_size = 2 * 1024 * 1024;
    let block_size = 4 * 1024;

    // Insert more blocks than cache can hold
    group.bench_function("evict_under_pressure", |b| {
        b.iter(|| {
            let cache = create_basic_cache(cache_size);
            // Try to insert 1000 blocks when only ~512 fit
            for i in 0..1000 {
                let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
                let block = make_cached_block(block_size);
                cache.insert(key, block);
            }
            black_box(cache);
        })
    });

    group.finish();
}

/// Benchmark concurrent cache access pattern (multiple threads, same keys)
fn bench_cache_concurrent_access(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let mut group = c.benchmark_group("hotpath_cache_concurrent");

    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Pre-populate cache once
    let cache = create_basic_cache(cache_size);
    for i in 0..num_blocks {
        let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
        let block = make_cached_block(block_size);
        cache.insert(key, block);
    }

    for &num_threads in &[2, 4, 8, 16, 32] {
        group.bench_function(format!("{}_threads", num_threads), |b| {
            b.iter(|| {
                let mut handles = vec![];
                for _ in 0..num_threads {
                    let cache_clone = Arc::clone(&cache);
                    let handle = thread::spawn(move || {
                        let mut count = 0;
                        for i in 0..num_blocks {
                            let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
                            if cache_clone.get(&key).is_some() {
                                count += 1;
                            }
                        }
                        black_box(count)
                    });
                    handles.push(handle);
                }

                let mut total = 0;
                for handle in handles {
                    total += handle.join().unwrap();
                }
                black_box(total);
            })
        });
    }

    group.finish();
}

criterion_group! {
    name = hotpath_cache;
    config = criterion_config();
    targets = bench_cache_insert, bench_cache_get_hit, bench_cache_get_miss, bench_cache_eviction, bench_cache_concurrent_access
}
criterion_main!(hotpath_cache);
