//! Tier 2 — SST Point Read with Bloom Filter
//!
//! **Target Runtime:** 2-5 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! **Purpose**: Validates bloom filter effectiveness by measuring block reads avoided.
//! Tests SST point lookups with bloom enabled vs disabled to quantify bloom value.
//!
//! **Tier-2 Compliance**:
//! - Subsystem interaction: Bloom filter → Sparse index → Block lookup
//! - System metrics: Blocks read, bloom checks, false positives
//! - Realistic patterns: 90% misses (realistic key-not-found workload)

use cntryl_midge::sst::bloom::{writer::BloomFilterOps, BloomWriter};
use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::sst::sparse_index::{IndexEntry, SparseIndexReader};
use cntryl_midge::sst::types::BlockHandle;
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

// ─── Test Data ───────────────────────────────────────────────────────────────

/// Represents an SST with 100 blocks, 100 keys per block = 10,000 keys
const BLOCKS_PER_SST: usize = 100;
const KEYS_PER_BLOCK: usize = 100;
const TOTAL_KEYS: usize = BLOCKS_PER_SST * KEYS_PER_BLOCK;
const COMPARISON_REPEATS: usize = 4;
const COMPARISON_REPEAT_OPS: u64 = 4;

/// Pre-computed SST structure for benchmarking
struct MockSst {
    bloom: cntryl_midge::sst::bloom::BloomReader,
    sparse_index: SparseIndexReader,
    cache: BlockCache,
    sst_id: u64,
}

/// Pre-generate all keys for SST (deterministic, no allocations in benchmark)
fn precompute_sst_keys() -> Vec<Bytes> {
    (0..TOTAL_KEYS)
        .map(|i| Bytes::from(format!("user:tenant:key:{i:010}")))
        .collect()
}

/// Pre-generate query keys: 10% hits (present in SST), 90% misses (absent)
fn precompute_query_keys(seed: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let hits: Vec<Bytes> = (0..1_000)
        .map(|i| {
            // Pick keys that exist in SST (use deterministic pattern)
            let idx = (i * 7 + seed) % TOTAL_KEYS;
            Bytes::from(format!("user:tenant:key:{idx:010}"))
        })
        .collect();

    let misses: Vec<Bytes> = (0..9_000)
        .map(|i| {
            // Keys that don't exist in SST (offset by large number)
            Bytes::from(format!("user:tenant:key:{:010}", TOTAL_KEYS + i + seed))
        })
        .collect();

    (hits, misses)
}

/// Build mock SST with bloom filter and sparse index
fn build_mock_sst(sst_id: u64) -> MockSst {
    let keys = precompute_sst_keys();

    // Build bloom filter
    let mut bloom_builder = BloomWriter::with_defaults(TOTAL_KEYS);
    for key in &keys {
        bloom_builder.insert(key);
    }
    let bloom = bloom_builder.finish();

    // Build sparse index (sample every 100 keys = 1 per block)
    let entries: Vec<IndexEntry> = keys
        .iter()
        .step_by(KEYS_PER_BLOCK)
        .enumerate()
        .map(|(block_idx, key)| {
            let offset = (block_idx * 4096) as u64;
            IndexEntry::new(key.to_vec(), BlockHandle::new(offset, 4096), block_idx)
        })
        .collect();
    let sparse_index = SparseIndexReader::new(entries).unwrap();

    // Create cache (10MB = can hold ~25 blocks of 4KB each)
    let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);

    // Populate cache with some blocks (simulate warm cache)
    let block_data = Bytes::from_static(&[0xAB; 4096]);
    for block_idx in 0_u64..25 {
        let key = CacheKey::for_data(sst_id, block_idx * 4096);
        cache.put(key, &block_data);
    }

    MockSst {
        bloom,
        sparse_index,
        cache,
        sst_id,
    }
}

// ─── Point Read with Bloom Enabled ───────────────────────────────────────────

#[stress(
    tier = 2,
    metadata(component = "sst_point_read", scenario = "bloom_enabled")
)]
fn bloom_enabled(ctx: &mut StressContext) {
    let sst = build_mock_sst(1);
    let (hits, misses) = precompute_query_keys(42);
    ctx.parameter("queries", 10_000);
    ctx.parameter("hit_ratio_pct", 10);
    ctx.parameter("bloom", "enabled");

    let _completed = ctx.measure_batch("bloom_enabled", 10_000, || {
        let mut bloom_checks = 0u32;
        let mut bloom_rejects = 0u32;
        let mut blocks_read = 0u32;
        let mut cache_hits = 0u32;

        for key in &hits {
            bloom_checks += 1;
            if sst.bloom.contains(key).might_be_present() {
                let range = sst.sparse_index.find_block_range(key);
                for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                    let cache_key = CacheKey::for_data(sst.sst_id, (block_idx * 4096) as u64);
                    if sst.cache.get(&cache_key).is_some() {
                        cache_hits += 1;
                    } else {
                        blocks_read += 1;
                    }
                }
            } else {
                bloom_rejects += 1;
            }
        }

        for key in &misses {
            bloom_checks += 1;
            if sst.bloom.contains(key).might_be_present() {
                let range = sst.sparse_index.find_block_range(key);
                for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                    let cache_key = CacheKey::for_data(sst.sst_id, (block_idx * 4096) as u64);
                    if sst.cache.get(&cache_key).is_some() {
                        cache_hits += 1;
                    } else {
                        blocks_read += 1;
                    }
                }
            } else {
                bloom_rejects += 1;
            }
        }

        black_box((bloom_checks, bloom_rejects, blocks_read, cache_hits));
    });
}

#[stress(
    tier = 2,
    metadata(component = "sst_point_read", scenario = "bloom_disabled")
)]
fn bloom_disabled(ctx: &mut StressContext) {
    let sst = build_mock_sst(2);
    let (hits, misses) = precompute_query_keys(42);
    ctx.parameter("queries", 10_000);
    ctx.parameter("hit_ratio_pct", 10);
    ctx.parameter("bloom", "disabled");

    let _completed = ctx.measure_batch("bloom_disabled", 10_000, || {
        let mut blocks_read = 0u32;
        let mut cache_hits = 0u32;

        for key in &hits {
            let range = sst.sparse_index.find_block_range(key);
            for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                let cache_key = CacheKey::for_data(sst.sst_id, (block_idx * 4096) as u64);
                if sst.cache.get(&cache_key).is_some() {
                    cache_hits += 1;
                } else {
                    blocks_read += 1;
                }
            }
        }

        for key in &misses {
            let range = sst.sparse_index.find_block_range(key);
            for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                let cache_key = CacheKey::for_data(sst.sst_id, (block_idx * 4096) as u64);
                if sst.cache.get(&cache_key).is_some() {
                    cache_hits += 1;
                } else {
                    blocks_read += 1;
                }
            }
        }

        black_box((blocks_read, cache_hits));
    });
}

fn run_comparison(ctx: &mut StressContext, scenario: &'static str, mode: &'static str) {
    let sst = build_mock_sst(3);
    let (hits, misses) = precompute_query_keys(42);
    let query_keys: Vec<Bytes> = hits.iter().chain(misses.iter()).cloned().collect();
    ctx.parameter("comparison_mode", mode);
    ctx.parameter("queries", query_keys.len());
    ctx.parameter("comparison_repeats", COMPARISON_REPEATS);

    let _completed = ctx.measure_batch(
        scenario,
        (query_keys.len() as u64) * COMPARISON_REPEAT_OPS,
        || {
            if mode == "with_bloom" {
                let mut bloom_rejects = 0u32;
                let mut blocks_read = 0u32;

                for _ in 0..COMPARISON_REPEATS {
                    for key in &query_keys {
                        if sst.bloom.contains(key).might_be_present() {
                            let range = sst.sparse_index.find_block_range(key);
                            for block_idx in
                                range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1)
                            {
                                let cache_key =
                                    CacheKey::for_data(sst.sst_id, (block_idx * 4096) as u64);
                                if sst.cache.get(&cache_key).is_none() {
                                    blocks_read += 1;
                                }
                            }
                        } else {
                            bloom_rejects += 1;
                        }
                    }
                }
                black_box((bloom_rejects, blocks_read));
            } else {
                let mut blocks_read = 0u32;

                for _ in 0..COMPARISON_REPEATS {
                    for key in &query_keys {
                        let range = sst.sparse_index.find_block_range(key);
                        for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1)
                        {
                            let cache_key =
                                CacheKey::for_data(sst.sst_id, (block_idx * 4096) as u64);
                            if sst.cache.get(&cache_key).is_none() {
                                blocks_read += 1;
                            }
                        }
                    }
                }
                black_box(blocks_read);
            }
        },
    );
}

#[stress(
    tier = 2,
    metadata(component = "sst_point_read", scenario = "comparison_with_bloom")
)]
fn comparison_with_bloom(ctx: &mut StressContext) {
    run_comparison(ctx, "comparison_with_bloom", "with_bloom");
}

#[stress(
    tier = 2,
    metadata(component = "sst_point_read", scenario = "comparison_without_bloom")
)]
fn comparison_without_bloom(ctx: &mut StressContext) {
    run_comparison(ctx, "comparison_without_bloom", "without_bloom");
}

stress_main!();
