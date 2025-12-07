//! Tier 2 — Tombstone Index Subsystem Benchmarks
//!
//! **Target Runtime:** < 3 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers tombstone index operations:
//! - Point lookup (find blocks containing a key)
//! - Range scan (find blocks overlapping a range)
//! - Deletion check (might_be_deleted query)
//! - Index build time from tombstone blocks

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use cntryl_midge::sst::{TombstoneIndex, TombstoneIndexBuilder};
use cntryl_midge::sst::format::BlockHandle;
use cntryl_midge::sst::traits::RangeTombstone;
use std::hint::black_box;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Pre-allocated tombstones to avoid allocation in benchmarks
fn create_tombstone(start: Vec<u8>, end: Vec<u8>, seq: u64) -> RangeTombstone {
    RangeTombstone { start, end, seq }
}

/// Build tombstone index (precomputed for benchmarks)
fn build_tombstone_index(num_blocks: usize, tombstones_per_block: usize) -> TombstoneIndex {
    let mut builder = TombstoneIndexBuilder::new();
    
    for block_idx in 0..num_blocks {
        let mut tombstones = Vec::new();
        for tomb_idx in 0..tombstones_per_block {
            let start = format!("key_{:06}_{:03}_start", block_idx, tomb_idx).into_bytes();
            let end = format!("key_{:06}_{:03}_end", block_idx, tomb_idx).into_bytes();
            tombstones.push(create_tombstone(start, end, (block_idx * 1000 + tomb_idx) as u64));
        }
        builder.add_block(&tombstones, BlockHandle::new((block_idx as u64) * 4096, 4096));
    }
    
    builder.finish()
}

// ─── Point Lookup Benchmarks ─────────────────────────────────────────────────

/// Benchmark point lookup: find blocks containing a specific key
fn tombstone_index_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("tombstone_index/point_lookup");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    for num_blocks in [10, 100, 500].iter() {
        // Precompute index outside benchmark loop
        let index = build_tombstone_index(*num_blocks, 10);
        
        // Precompute search keys to avoid allocation in hot path
        let search_keys: Vec<Vec<u8>> = (0..1000)
            .map(|i| format!("key_{:06}_005_middle", i % num_blocks).into_bytes())
            .collect();
        
        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            num_blocks,
            |b, _| {
                b.iter(|| {
                    let mut hits = 0;
                    for key in &search_keys {
                        let blocks: Vec<_> = index.find_blocks_for_key(black_box(key)).collect();
                        hits += blocks.len();
                    }
                    black_box(hits)
                })
            },
        );
    }
    group.finish();
}

/// Benchmark range scan: find blocks overlapping a key range
fn tombstone_index_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("tombstone_index/range_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    for num_blocks in [10, 100, 500].iter() {
        // Precompute index outside benchmark loop
        let index = build_tombstone_index(*num_blocks, 10);
        
        // Precompute range keys to avoid allocation in hot path
        let ranges: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| {
                let start = format!("key_{:06}_000_start", i * 5).into_bytes();
                let end = format!("key_{:06}_009_end", (i * 5) + 10).into_bytes();
                (start, end)
            })
            .collect();
        
        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            num_blocks,
            |b, _| {
                b.iter(|| {
                    let mut total_blocks = 0;
                    for (start, end) in &ranges {
                        let blocks: Vec<_> = index.find_blocks_in_range(black_box(start), black_box(end)).collect();
                        total_blocks += blocks.len();
                    }
                    black_box(total_blocks)
                })
            },
        );
    }
    group.finish();
}

/// Benchmark deletion check: might_be_deleted query
fn tombstone_index_might_be_deleted(c: &mut Criterion) {
    let mut group = c.benchmark_group("tombstone_index/might_be_deleted");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10000));

    for num_blocks in [10, 100, 500].iter() {
        // Precompute index outside benchmark loop
        let index = build_tombstone_index(*num_blocks, 10);
        
        // Precompute query keys to avoid allocation in hot path
        let query_keys: Vec<Vec<u8>> = (0..10000)
            .map(|i| format!("key_{:06}_005_middle", i % (num_blocks * 2)).into_bytes())
            .collect();
        
        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            num_blocks,
            |b, _| {
                b.iter(|| {
                    let mut deleted_count = 0;
                    for key in &query_keys {
                        if index.might_be_deleted(black_box(key)) {
                            deleted_count += 1;
                        }
                    }
                    black_box(deleted_count)
                })
            },
        );
    }
    group.finish();
}

/// Benchmark index build: construct tombstone index from blocks
fn tombstone_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("tombstone_index/build");
    group.sampling_mode(SamplingMode::Flat);

    for num_blocks in [10, 100, 500].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_blocks),
            num_blocks,
            |b, n| {
                b.iter(|| {
                    black_box(build_tombstone_index(*n, 10))
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = 
        tombstone_index_point_lookup,
        tombstone_index_range_scan,
        tombstone_index_might_be_deleted,
        tombstone_index_build
);
criterion_main!(benches);
