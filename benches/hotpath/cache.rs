//! Tier 1 — Hot Path Cache Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers block cache operations (critical read path)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::{BlockCache, BlockKey, CacheBlockType, CachedBlock};
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
                    || BlockCache::new(cache_size),
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
    let cache = BlockCache::new(cache_size);
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

criterion_group! {
    name = hotpath_cache;
    config = criterion_config();
    targets = bench_cache_insert, bench_cache_get_hit
}
criterion_main!(hotpath_cache);
