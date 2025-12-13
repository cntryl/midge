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

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::sst::bloom::{writer::BloomFilterOps, BloomWriter};
use cntryl_midge::sst::cache::{BlockCache, CacheKey, CachePolicyType};
use cntryl_midge::sst::sparse_index::{IndexEntry, SparseIndexReader};
use cntryl_midge::sst::types::BlockHandle;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ─── Test Data ───────────────────────────────────────────────────────────────

/// Represents an SST with 100 blocks, 100 keys per block = 10,000 keys
const BLOCKS_PER_SST: usize = 100;
const KEYS_PER_BLOCK: usize = 100;
const TOTAL_KEYS: usize = BLOCKS_PER_SST * KEYS_PER_BLOCK;

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
        .map(|i| Bytes::from(format!("user:tenant:key:{:010}", i)))
        .collect()
}

/// Pre-generate query keys: 10% hits (present in SST), 90% misses (absent)
fn precompute_query_keys(seed: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let hits: Vec<Bytes> = (0..1_000)
        .map(|i| {
            // Pick keys that exist in SST (use deterministic pattern)
            let idx = (i * 7 + seed) % TOTAL_KEYS;
            Bytes::from(format!("user:tenant:key:{:010}", idx))
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
            IndexEntry::new(
                key.to_vec(),
                BlockHandle::new(offset, 4096),
                block_idx,
            )
        })
        .collect();
    let sparse_index = SparseIndexReader::new(entries).unwrap();

    // Create cache (10MB = can hold ~25 blocks of 4KB each)
    let cache = BlockCache::new(10 * 1024 * 1024, 16, CachePolicyType::Lru);

    // Populate cache with some blocks (simulate warm cache)
    let block_data = Bytes::from_static(&[0xAB; 4096]);
    for block_idx in 0..25 {
        let key = CacheKey::new(sst_id, (block_idx * 4096) as u64);
        cache.put(key, block_data.clone());
    }

    MockSst {
        bloom,
        sparse_index,
        cache,
        sst_id,
    }
}

// ─── Point Read with Bloom Enabled ───────────────────────────────────────────

/// Benchmark point reads WITH bloom filter
fn bench_point_read_bloom_enabled(c: &mut Criterion) {
    let sst = build_mock_sst(1);
    let (hits, misses) = precompute_query_keys(42);

    let mut group = c.benchmark_group("sst_point_read_bloom_enabled");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("10k_queries_10pct_hit", |b| {
        b.iter(|| {
            let mut bloom_checks = 0u32;
            let mut bloom_rejects = 0u32;
            let mut blocks_read = 0u32;
            let mut cache_hits = 0u32;

            // Query hits (10%)
            for key in &hits {
                bloom_checks += 1;
                if sst.bloom.contains(key).might_be_present() {
                    // Bloom says maybe present, check sparse index
                    let range = sst.sparse_index.find_block_range(key);
                    for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                        let cache_key = CacheKey::new(sst.sst_id, (block_idx * 4096) as u64);
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

            // Query misses (90%)
            for key in &misses {
                bloom_checks += 1;
                if sst.bloom.contains(key).might_be_present() {
                    // False positive - must check sparse index
                    let range = sst.sparse_index.find_block_range(key);
                    for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                        let cache_key = CacheKey::new(sst.sst_id, (block_idx * 4096) as u64);
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

            black_box((bloom_checks, bloom_rejects, blocks_read, cache_hits))
        })
    });

    group.finish();
}

// ─── Point Read with Bloom Disabled ──────────────────────────────────────────

/// Benchmark point reads WITHOUT bloom filter (always check sparse index)
fn bench_point_read_bloom_disabled(c: &mut Criterion) {
    let sst = build_mock_sst(2);
    let (hits, misses) = precompute_query_keys(42);

    let mut group = c.benchmark_group("sst_point_read_bloom_disabled");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    group.bench_function("10k_queries_10pct_hit", |b| {
        b.iter(|| {
            let mut blocks_read = 0u32;
            let mut cache_hits = 0u32;

            // Query hits (10%)
            for key in &hits {
                let range = sst.sparse_index.find_block_range(key);
                for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                    let cache_key = CacheKey::new(sst.sst_id, (block_idx * 4096) as u64);
                    if sst.cache.get(&cache_key).is_some() {
                        cache_hits += 1;
                    } else {
                        blocks_read += 1;
                    }
                }
            }

            // Query misses (90%)
            for key in &misses {
                let range = sst.sparse_index.find_block_range(key);
                for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                    let cache_key = CacheKey::new(sst.sst_id, (block_idx * 4096) as u64);
                    if sst.cache.get(&cache_key).is_some() {
                        cache_hits += 1;
                    } else {
                        blocks_read += 1;
                    }
                }
            }

            black_box((blocks_read, cache_hits))
        })
    });

    group.finish();
}

// ─── Comparison Benchmark ────────────────────────────────────────────────────

/// Direct comparison: bloom vs no-bloom on same workload
fn bench_point_read_bloom_comparison(c: &mut Criterion) {
    let sst = build_mock_sst(3);
    let (hits, misses) = precompute_query_keys(42);

    let mut group = c.benchmark_group("sst_point_read_comparison");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));

    for &mode in &["with_bloom", "without_bloom"] {
        group.bench_with_input(BenchmarkId::from_parameter(mode), &mode, |b, &mode| {
            if mode == "with_bloom" {
                b.iter(|| {
                    let mut bloom_rejects = 0u32;
                    let mut blocks_read = 0u32;

                    for key in hits.iter().chain(misses.iter()) {
                        if sst.bloom.contains(key).might_be_present() {
                            let range = sst.sparse_index.find_block_range(key);
                            for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                                let cache_key = CacheKey::new(sst.sst_id, (block_idx * 4096) as u64);
                                if sst.cache.get(&cache_key).is_none() {
                                    blocks_read += 1;
                                }
                            }
                        } else {
                            bloom_rejects += 1;
                        }
                    }
                    black_box((bloom_rejects, blocks_read))
                })
            } else {
                b.iter(|| {
                    let mut blocks_read = 0u32;

                    for key in hits.iter().chain(misses.iter()) {
                        let range = sst.sparse_index.find_block_range(key);
                        for block_idx in range.start_block..=range.end_block.min(BLOCKS_PER_SST - 1) {
                            let cache_key = CacheKey::new(sst.sst_id, (block_idx * 4096) as u64);
                            if sst.cache.get(&cache_key).is_none() {
                                blocks_read += 1;
                            }
                        }
                    }
                    black_box(blocks_read)
                })
            }
        });
    }

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier2_subsystem_sst_point_read_bloom;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets =
        bench_point_read_bloom_enabled,
        bench_point_read_bloom_disabled,
        bench_point_read_bloom_comparison
}
criterion_main!(tier2_subsystem_sst_point_read_bloom);
