//! Tier 1 — Sparse Index Hot Path Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers sparse index hot paths:
//! - Binary search for block range lookup
//! - Hit at beginning, middle, end of index
//! - Edge cases (before first, after last)

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::sst::sparse_index::{IndexEntry, SparseIndexReader};
use cntryl_midge::sst::types::BlockHandle;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier1;
use std::hint::black_box;

const SPARSE_INDEX_LOOKUP_BATCH_SIZE_DEFAULT: usize = 256;
const SPARSE_INDEX_LOOKUP_BATCH_SIZE_LARGE: usize = 1024;

/// Benchmark sparse index binary search for exact key match
fn bench_sparse_index_find_block(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sparse_index_find_block");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Build sparse index with 100 sampled entries (simulates 100-block SST)
    let entries: Vec<IndexEntry> = (0..100)
        .map(|i| {
            let key = format!("key_{:010}", i * 100);
            let block_handle = BlockHandle::new(i as u64 * 4096, 4096);
            IndexEntry::new(key.into_bytes(), block_handle, i)
        })
        .collect();

    let reader = SparseIndexReader::new(entries).unwrap();

    // Precompute keys for different scenarios
    let key_beginning = b"key_0000000050"; // Before first sampled key
    let key_middle = b"key_0000005050"; // Middle of index
    let key_end = b"key_0000009950"; // Near end
    let key_after = b"key_0000099999"; // After all sampled keys

    group.bench_function("find_beginning", |b| {
        b.iter(|| {
            let range = reader.find_block_range(black_box(key_beginning));
            black_box(range);
        });
    });

    group.bench_function("find_middle", |b| {
        b.iter(|| {
            let range = reader.find_block_range(black_box(key_middle));
            black_box(range);
        });
    });

    group.bench_function("find_end", |b| {
        b.iter(|| {
            let range = reader.find_block_range(black_box(key_end));
            black_box(range);
        });
    });

    group.bench_function("find_after_last", |b| {
        b.iter(|| {
            let range = reader.find_block_range(black_box(key_after));
            black_box(range);
        });
    });

    group.finish();
}

/// Benchmark sparse index lookup with different index sizes
fn bench_sparse_index_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sparse_index_sizes");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_millis(600));
    for &size in &[10, 100, 1000] {
        let lookup_batch_size = if size >= 1000 {
            SPARSE_INDEX_LOOKUP_BATCH_SIZE_LARGE
        } else {
            SPARSE_INDEX_LOOKUP_BATCH_SIZE_DEFAULT
        };
        group.throughput(Throughput::Elements(lookup_batch_size as u64));
        let entries: Vec<IndexEntry> = (0..size)
            .map(|i| {
                let key = format!("key_{:010}", i * 100);
                let block_handle = BlockHandle::new(i as u64 * 4096, 4096);
                IndexEntry::new(key.into_bytes(), block_handle, i)
            })
            .collect();

        let reader = SparseIndexReader::new(entries).unwrap();
        let lookup_key = format!("key_{:010}", (size / 2) * 100);

        group.bench_function(format!("{size}_entries"), |b| {
            b.iter(|| {
                let mut found = 0usize;
                for _ in 0..lookup_batch_size {
                    let range = reader.find_block_range(black_box(lookup_key.as_bytes()));
                    found = found.wrapping_add(range.start_block);
                }
                black_box(found);
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_sparse_index;
    config = criterion_config_for_tier1();
    targets =
        bench_sparse_index_find_block,
        bench_sparse_index_sizes
}
criterion_main!(tier1_hotpath_sparse_index);
