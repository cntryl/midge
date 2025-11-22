//! Tier 1 — Hot Path Cache Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers block cache operations (critical read path)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::{create_basic_cache, BlockKey, CacheBlockType, CachedBlock};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
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

fn precompute_keys_and_blocks(
    num_blocks: usize,
    block_size: usize,
) -> (Vec<BlockKey>, Vec<CachedBlock>) {
    let mut keys = Vec::with_capacity(num_blocks);
    let mut blocks = Vec::with_capacity(num_blocks);
    for i in 0..num_blocks {
        let key = make_block_key(i / 100, (i % 100) as u64 * block_size as u64);
        let block = make_cached_block(block_size);
        keys.push(key);
        blocks.push(block);
    }
    (keys, blocks)
}

/// Benchmark cache insert operations
fn bench_cache_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_insert");
    group.sampling_mode(SamplingMode::Flat);

    let cache_size = 10 * 1024 * 1024; // 10 MB
    let block_size = 4 * 1024; // 4 KB

    for &num_blocks in &[100, 1_000] {
        // Precompute keys and blocks outside the loop
        let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);
        group.throughput(Throughput::Elements(num_blocks as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            &num_blocks,
            |b, &n| {
                b.iter_batched(
                    || create_basic_cache(cache_size),
                    |cache| {
                        for i in 0..n {
                            cache.insert(keys[i].clone(), blocks[i].clone());
                        }
                        black_box(());
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
    let mut group = c.benchmark_group("hotpath_cache_get_hit");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Precompute keys and blocks
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Pre-populate cache
    let cache = create_basic_cache(cache_size);
    for i in 0..num_blocks {
        cache.insert(keys[i].clone(), blocks[i].clone());
    }

    group.bench_function("get_hit", |b| {
        b.iter(|| {
            let mut count = 0;
            for key in keys.iter().take(num_blocks) {
                if cache.get(key).is_some() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });

    group.finish();
}

/// Benchmark cache get operations on missing keys (cache misses)
fn bench_cache_get_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_get_miss");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Precompute keys and blocks for population
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Pre-populate cache with keys 0..1000
    let cache = create_basic_cache(cache_size);
    for i in 0..num_blocks {
        cache.insert(keys[i].clone(), blocks[i].clone());
    }

    // Precompute miss keys (1000..2000)
    let (miss_keys, _) = precompute_keys_and_blocks(num_blocks, block_size);

    group.bench_function("get_miss", |b| {
        b.iter(|| {
            let mut count = 0;
            for key in &miss_keys {
                if cache.get(key).is_some() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });

    group.finish();
}

/// Benchmark cache eviction under memory pressure
fn bench_cache_eviction(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_cache_eviction");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    // Small cache to trigger eviction (2 MB, holds ~512 4KB blocks)
    let cache_size = 2 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Precompute keys and blocks
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Insert more blocks than cache can hold
    group.bench_function("evict_under_pressure", |b| {
        b.iter(|| {
            let cache = create_basic_cache(cache_size);
            // Try to insert 1000 blocks when only ~512 fit
            for i in 0..num_blocks {
                cache.insert(keys[i].clone(), blocks[i].clone());
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
    group.sampling_mode(SamplingMode::Flat);

    let cache_size = 10 * 1024 * 1024;
    let block_size = 4 * 1024;
    let num_blocks = 1000;

    // Precompute keys and blocks
    let (keys, blocks) = precompute_keys_and_blocks(num_blocks, block_size);

    // Pre-populate cache once
    let cache = Arc::new(create_basic_cache(cache_size));
    for i in 0..num_blocks {
        cache.insert(keys[i].clone(), blocks[i].clone());
    }

    for &num_threads in &[2, 4, 8, 16, 32] {
        group.throughput(Throughput::Elements((num_threads * num_blocks) as u64));

        group.bench_function(format!("{}_threads", num_threads), |b| {
            b.iter(|| {
                let mut handles = vec![];
                for _ in 0..num_threads {
                    let cache_clone = Arc::clone(&cache);
                    let keys_clone = keys.clone();
                    let handle = thread::spawn(move || {
                        let mut count = 0;
                        for key in &keys_clone {
                            if cache_clone.get(key).is_some() {
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
