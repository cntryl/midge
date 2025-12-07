//! Tier 2 — Iterator throughput with sequential optimizer
//!
//! Measures end-to-end iterator throughput improvement for 1000-block sequential scans
//! and reports predictor hit ratio.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::{block_meta::BlockMeta, block_meta::IndexTable, sequential_access_optimizer::SequentialAccessOptimizer};
use cntryl_midge::sst::format::BlockHandle;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};

const BLOCK_COUNT: usize = 1024;

fn build_table_with_optimizer() -> IndexTable {
    let metas: Vec<BlockMeta> = (0..BLOCK_COUNT)
        .map(|i| {
            let min = Bytes::from(format!("key_{:08}", i * 100));
            let max = Bytes::from(format!("key_{:08}", i * 100 + 99));
            BlockMeta::new(min, max, BlockHandle::new(i as u64 * 4096, 1024))
        })
        .collect();

    let mut table = IndexTable::new(metas);
    table.set_sequential_optimizer(SequentialAccessOptimizer::new());
    table
}

fn bench_sequential_scan_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_iterator_sequential_throughput");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(BLOCK_COUNT as u64));

    let table = build_table_with_optimizer();
    
    // Sequential keys
    let keys: Vec<Bytes> = (0..BLOCK_COUNT)
        .map(|i| Bytes::from(format!("key_{:08}", i * 100 + 50)))
        .collect();

    group.bench_function("sequential_1024_blocks", |b| {
        b.iter(|| {
            let mut found = 0usize;
            for key in &keys {
                if table.find_block(black_box(key.as_ref())).is_some() {
                    found += 1;
                }
            }
            black_box(found)
        })
    });

    group.finish();
}

fn bench_predictor_hit_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_predictor_hit_ratio");
    group.sampling_mode(SamplingMode::Flat);
    
    let table = build_table_with_optimizer();
    let keys: Vec<Bytes> = (0..BLOCK_COUNT)
        .map(|i| Bytes::from(format!("key_{:08}", i * 100 + 50)))
        .collect();

    group.bench_function("measure_hit_ratio", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for key in &keys {
                if table.find_block(black_box(key.as_ref())).is_some() {
                    hits += 1;
                }
            }
            
            // Check optimizer metrics
            if let Some(opt_cell) = table.sequential_optimizer() {
                if let Ok(opt) = opt_cell.try_borrow() {
                    let ratio = opt.predictor_hit_ratio();
                    // Should be >85% for sequential access
                    assert!(ratio > 0.85 || hits == 0, "Predictor ratio too low: {}", ratio);
                }
            }
            
            black_box(hits)
        })
    });

    group.finish();
}

criterion_group!(
    name = streaming_iterator_throughput;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_sequential_scan_throughput, bench_predictor_hit_ratio
);
criterion_main!(streaming_iterator_throughput);
