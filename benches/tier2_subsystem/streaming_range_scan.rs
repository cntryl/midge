//! Tier 2 — Range scan efficiency with fence pointers and fast filters
//!
//! Measures block touches for a 90% negative range mix and reports skip ratio.
//! Target runtime: ~1-2s, Flat sampling.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::{block_meta::BlockMeta, block_meta::IndexTable, fast_negative_filter::FastNegativeFilter};
use cntryl_midge::sst::format::BlockHandle;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};

const BLOCK_COUNT: usize = 256;

fn build_index_table_with_filters() -> IndexTable {
    let metas: Vec<BlockMeta> = (0..BLOCK_COUNT)
        .map(|i| {
            let min = Bytes::from(format!("key_{:06}", i * 100));
            let max = Bytes::from(format!("key_{:06}", i * 100 + 99));
            BlockMeta::new(min, max, BlockHandle::new(i as u64 * 4096, 1024))
        })
        .collect();

    let mut table = IndexTable::new(metas);
    let mut fast_filter = FastNegativeFilter::new();
    for i in 0..BLOCK_COUNT {
        fast_filter.set_block(i);
    }
    table.set_fast_negative_filter(fast_filter);
    table
}

fn bench_range_scan_negative_mix(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_range_scan_negative_mix");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let table = build_index_table_with_filters();
    // 90% negative windows: ranges that mostly miss blocks
    let ranges: Vec<(Bytes, Bytes)> = (0..100)
        .map(|i| {
            let start = i * 250; // stride large to skip most blocks
            let end = start + 50;
            (
                Bytes::from(format!("key_{:06}", start)),
                Bytes::from(format!("key_{:06}", end)),
            )
        })
        .collect();

    group.bench_function("range_scan_90pct_negative", |b| {
        b.iter(|| {
            let mut touched = 0usize;
            for (s, e) in &ranges {
                let blocks = table.find_blocks_in_range(black_box(s.as_ref()), black_box(e.as_ref()));
                touched += blocks.len();
            }
            black_box(touched)
        })
    });

    group.finish();
}

criterion_group!(
    name = streaming_range_scan_subsystem;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_range_scan_negative_mix
);
criterion_main!(streaming_range_scan_subsystem);
