//! Tier 1 — Sparse Index Hot Path Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers sparse index hot paths:
//! - Binary search for block range lookup
//! - Hit at beginning, middle, end of index
//! - Edge cases (before first, after last)

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::sst::sparse_index::{IndexEntry, SparseIndexReader};
use cntryl_midge::sst::types::BlockHandle;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const SPARSE_INDEX_FIND_BATCH_SIZE: usize = 1_048_576;
const SPARSE_INDEX_LOOKUP_BATCH_SIZE_DEFAULT: usize = 65_536;
const SPARSE_INDEX_LOOKUP_BATCH_SIZE_LARGE: usize = 16_384;
const SPARSE_INDEX_FIND_KEY_WINDOW: usize = 64;

cntryl_stress::stress_allocator!();

fn build_sparse_index(size: usize) -> SparseIndexReader {
    let entries: Vec<IndexEntry> = (0..size)
        .map(|i| {
            let key = format!("key_{:010}", i * 100);
            let block_handle = BlockHandle::new(i as u64 * 4096, 4096);
            IndexEntry::new(key.into_bytes(), block_handle, i)
        })
        .collect();
    SparseIndexReader::new(entries).unwrap()
}

fn run_find_block(ctx: &mut StressContext, scenario: &'static str, key_base: usize) {
    let reader = build_sparse_index(100);
    let keys: Vec<Vec<u8>> = (0..SPARSE_INDEX_FIND_KEY_WINDOW)
        .map(|i| format!("key_{:010}", key_base + i).into_bytes())
        .collect();
    let mut key_index = 0usize;
    ctx.parameter("scenario", scenario);
    ctx.parameter("entries", 100);
    ctx.parameter("find_batch_size", SPARSE_INDEX_FIND_BATCH_SIZE);
    ctx.parameter("key_window", SPARSE_INDEX_FIND_KEY_WINDOW);
    ctx.parameter("logical_unit", "sparse_index_probe");

    stress_config::measure_hot_path_batch(
        ctx,
        scenario,
        SPARSE_INDEX_FIND_BATCH_SIZE as u64,
        || {
            let mut start_blocks = 0usize;
            for _ in 0..SPARSE_INDEX_FIND_BATCH_SIZE {
                let key = &keys[key_index % SPARSE_INDEX_FIND_KEY_WINDOW];
                key_index = key_index.wrapping_add(1);
                let range = reader.find_block_range(black_box(key.as_slice()));
                start_blocks = start_blocks.wrapping_add(range.start_block);
            }
            black_box(start_blocks);
        },
    );
}

#[stress(
    tier = 1,
    metadata(
        component = "sparse_index",
        scenario = "find_block_at_beginning",
        trust_class = "diagnostic",
        validated_micro = "true"
    )
)]
fn find_beginning(ctx: &mut StressContext) {
    run_find_block(ctx, "find_beginning", 50);
}

#[stress(
    tier = 1,
    metadata(
        component = "sparse_index",
        scenario = "find_block_at_middle",
        trust_class = "diagnostic",
        validated_micro = "true"
    )
)]
fn find_middle(ctx: &mut StressContext) {
    run_find_block(ctx, "find_middle", 5050);
}

#[stress(
    tier = 1,
    metadata(
        component = "sparse_index",
        scenario = "find_block_at_end",
        trust_class = "diagnostic",
        validated_micro = "true"
    )
)]
fn find_end(ctx: &mut StressContext) {
    run_find_block(ctx, "find_end", 9950);
}

#[stress(
    tier = 1,
    metadata(
        component = "sparse_index",
        scenario = "find_key_after_last",
        trust_class = "diagnostic",
        validated_micro = "true"
    )
)]
fn find_after_last(ctx: &mut StressContext) {
    run_find_block(ctx, "find_after_last", 99_999);
}

fn run_index_size(ctx: &mut StressContext, scenario: &'static str, size: usize) {
    const SIZE_10_LOOKUP_REPEAT_PER_SAMPLE: usize = 5;
    const SIZE_10_SAMPLE_COUNT: usize = 5;

    let lookup_batch_size = if size >= 1000 {
        SPARSE_INDEX_LOOKUP_BATCH_SIZE_LARGE
    } else {
        SPARSE_INDEX_LOOKUP_BATCH_SIZE_DEFAULT
    };
    let repeat = if size == 10 {
        SIZE_10_LOOKUP_REPEAT_PER_SAMPLE
    } else {
        1
    };
    let logical_ops = lookup_batch_size * repeat;
    let reader = build_sparse_index(size);
    let lookup_key = format!("key_{:010}", (size / 2) * 100);

    ctx.parameter("entries", size);
    ctx.parameter("lookup_batch_size", logical_ops);
    ctx.parameter("logical_unit", "sparse_index_probe");

    if size == 10 {
        ctx.benchmark(scenario)
            .samples(SIZE_10_SAMPLE_COUNT)
            .measure_batch(logical_ops as u64, || {
                let mut found = 0usize;
                for _ in 0..repeat {
                    for _ in 0..lookup_batch_size {
                        let range = reader.find_block_range(black_box(lookup_key.as_bytes()));
                        found = found.wrapping_add(range.start_block);
                    }
                }
                black_box(found);
            });
    } else {
        stress_config::measure_hot_path_batch(ctx, scenario, logical_ops as u64, || {
            let mut found = 0usize;
            for _ in 0..logical_ops {
                let range = reader.find_block_range(black_box(lookup_key.as_bytes()));
                found = found.wrapping_add(range.start_block);
            }
            black_box(found);
        });
    }
}

#[stress(
    tier = 1,
    metadata(
        component = "sparse_index",
        scenario = "size_10_entries",
        trust_class = "diagnostic",
        validated_micro = "true"
    )
)]
fn size_10_entries(ctx: &mut StressContext) {
    run_index_size(ctx, "size_10_entries", 10);
}

#[stress(
    tier = 1,
    metadata(component = "sparse_index", scenario = "size_100_entries")
)]
fn size_100_entries(ctx: &mut StressContext) {
    run_index_size(ctx, "size_100_entries", 100);
}

#[stress(
    tier = 1,
    metadata(component = "sparse_index", scenario = "size_1000_entries")
)]
fn size_1000_entries(ctx: &mut StressContext) {
    run_index_size(ctx, "size_1000_entries", 1000);
}

stress_main!();
