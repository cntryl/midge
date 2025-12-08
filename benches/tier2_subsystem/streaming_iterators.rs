//! Tier 2 — Streaming iterator subsystem benchmarks
//!
//! Covers:
//! - IndexTable sequential predictor hit rate on sequential access
//! - Fence-pointer based range intersection checks
//!
//! Target runtime: ~1-2s total; all data precomputed outside hot loop.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::format::BlockHandle;
use cntryl_midge::sst::{
    block_meta::BlockMeta, block_meta::IndexTable, fast_negative_filter::FastNegativeFilter,
    sequential_access_optimizer::SequentialAccessOptimizer,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};

const BLOCK_COUNT: usize = 256;

fn build_index_table() -> IndexTable {
    let metas: Vec<BlockMeta> = (0..BLOCK_COUNT)
        .map(|i| {
            let min = Bytes::from(format!("key_{:06}", i * 10));
            let max = Bytes::from(format!("key_{:06}", i * 10 + 9));
            BlockMeta::new(min, max, BlockHandle::new(i as u64 * 4096, 1024))
        })
        .collect();

    let mut table = IndexTable::new(metas);
    // Attach sequential optimizer for predictor path
    table.set_sequential_optimizer(SequentialAccessOptimizer::new());
    // Optional: attach a fast negative filter with all bits set (neutral)
    let mut fast_filter = FastNegativeFilter::new();
    for i in 0..BLOCK_COUNT {
        fast_filter.set_block(i);
    }
    table.set_fast_negative_filter(fast_filter);
    table
}

fn bench_index_table_sequential_predictor(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_index_table_sequential_predictor");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(BLOCK_COUNT as u64));

    let table = build_index_table();
    let lookup_keys: Vec<Bytes> = (0..BLOCK_COUNT)
        .map(|i| Bytes::from(format!("key_{:06}", i * 10 + 5)))
        .collect();

    group.bench_function("sequential_scan_find_block", |b| {
        b.iter(|| {
            let mut found = 0usize;
            for key in &lookup_keys {
                if table.find_block(black_box(key.as_ref())).is_some() {
                    found += 1;
                }
            }
            black_box(found)
        })
    });

    group.finish();
}

fn bench_index_table_fence_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_index_table_fence_range");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let table = build_index_table();
    // Precompute a range that intersects ~10 blocks
    let range_start = Bytes::from("key_050000");
    let range_end = Bytes::from("key_050120");

    group.bench_function("find_blocks_in_range", |b| {
        b.iter(|| {
            let blocks = table.find_blocks_in_range(
                black_box(range_start.as_ref()),
                black_box(range_end.as_ref()),
            );
            black_box(blocks.len())
        })
    });

    group.finish();
}

criterion_group!(
    name = streaming_iterators_subsystem;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_index_table_sequential_predictor, bench_index_table_fence_range
);
criterion_main!(streaming_iterators_subsystem);
