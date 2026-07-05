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
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

const SPARSE_INDEX_LOOKUP_BATCH_SIZE_DEFAULT: usize = 256;
const SPARSE_INDEX_LOOKUP_BATCH_SIZE_LARGE: usize = 1024;

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

fn run_find_block(ctx: &mut StressContext, scenario: &'static str, key: &'static [u8]) {
    let reader = build_sparse_index(100);
    ctx.parameter("scenario", scenario);
    ctx.parameter("entries", 100);
    ctx.measure_micro(|| {
        let range = reader.find_block_range(black_box(key));
        black_box(range);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "sparse_index", scenario = "find_beginning")
)]
fn find_beginning(ctx: &mut StressContext) {
    run_find_block(ctx, "find_beginning", b"key_0000000050");
}

#[stress_test(
    tier = 1,
    metadata(component = "sparse_index", scenario = "find_middle")
)]
fn find_middle(ctx: &mut StressContext) {
    run_find_block(ctx, "find_middle", b"key_0000005050");
}

#[stress_test(tier = 1, metadata(component = "sparse_index", scenario = "find_end"))]
fn find_end(ctx: &mut StressContext) {
    run_find_block(ctx, "find_end", b"key_0000009950");
}

#[stress_test(
    tier = 1,
    metadata(component = "sparse_index", scenario = "find_after_last")
)]
fn find_after_last(ctx: &mut StressContext) {
    run_find_block(ctx, "find_after_last", b"key_0000099999");
}

fn run_index_size(ctx: &mut StressContext, size: usize) {
    let lookup_batch_size = if size >= 1000 {
        SPARSE_INDEX_LOOKUP_BATCH_SIZE_LARGE
    } else {
        SPARSE_INDEX_LOOKUP_BATCH_SIZE_DEFAULT
    };
    let reader = build_sparse_index(size);
    let lookup_key = format!("key_{:010}", (size / 2) * 100);
    ctx.parameter("entries", size);
    ctx.parameter("lookup_batch_size", lookup_batch_size);

    stress_config::measure_micro_batch(ctx, lookup_batch_size as u64, || {
        let mut found = 0usize;
        for _ in 0..lookup_batch_size {
            let range = reader.find_block_range(black_box(lookup_key.as_bytes()));
            found = found.wrapping_add(range.start_block);
        }
        black_box(found);
    });
}

#[stress_test(tier = 1, metadata(component = "sparse_index", scenario = "size_10"))]
fn size_10_entries(ctx: &mut StressContext) {
    run_index_size(ctx, 10);
}

#[stress_test(tier = 1, metadata(component = "sparse_index", scenario = "size_100"))]
fn size_100_entries(ctx: &mut StressContext) {
    run_index_size(ctx, 100);
}

#[stress_test(tier = 1, metadata(component = "sparse_index", scenario = "size_1000"))]
fn size_1000_entries(ctx: &mut StressContext) {
    run_index_size(ctx, 1000);
}

stress_main!();
