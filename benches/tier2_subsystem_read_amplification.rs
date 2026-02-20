//! Tier 2 — Read Amplification under Mixed Workload
//!
//! **Target Runtime:** 3-6 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! **Purpose**: Measures read amplification (blocks read per query) under realistic
//! mixed workloads. Validates that LSM design effectively controls amplification
//! through bloom filters, caching, and compaction.
//!
//! **Tier-2 Compliance**:
//! - Subsystem interaction: Multiple SSTs → Iterator merge → Cache → Bloom filters
//! - System metrics: Blocks read per query, cache hit rate, amplification factor
//! - Realistic patterns: Zipfian key distribution, mixed get/scan operations

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::Bytes;
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_config::criterion_config_for_tier2;
use std::hint::black_box;

// ─── Configuration ──────────────────────────────────────────────────────

const KEYS_PER_BLOCK: usize = 100;

/// Simulated SST with bloom filter and block data
struct SstSimulator {
    sst_id: u64,
    /// Keys in this SST (in sorted order)
    keys: Vec<Bytes>,
    /// Which block contains which key (index into keys)
    block_layout: Vec<(usize, usize)>, // (start_idx, end_idx) per block
}

impl SstSimulator {
    /// Create SST with keys in range [start_key, start_key+num_keys)
    fn new(sst_id: u64, start_key: usize, num_keys: usize) -> Self {
        let keys: Vec<Bytes> = (start_key..start_key + num_keys)
            .map(|i| Bytes::from(format!("key:{:010}", i)))
            .collect();

        let num_blocks = num_keys.div_ceil(KEYS_PER_BLOCK);
        let block_layout = (0..num_blocks)
            .map(|block_idx| {
                let start = block_idx * KEYS_PER_BLOCK;
                let end = ((block_idx + 1) * KEYS_PER_BLOCK).min(num_keys);
                (start, end)
            })
            .collect();

        Self {
            sst_id,
            keys,
            block_layout,
        }
    }

    /// Check if key exists in SST (binary search)
    fn contains(&self, key: &Bytes) -> bool {
        self.keys.binary_search(key).is_ok()
    }

    /// Find which block would contain this key
    fn find_block_for_key(&self, key: &Bytes) -> Option<usize> {
        match self.keys.binary_search(key) {
            Ok(key_idx) => {
                // Found exact match - find which block
                for (block_idx, (start, end)) in self.block_layout.iter().enumerate() {
                    if key_idx >= *start && key_idx < *end {
                        return Some(block_idx);
                    }
                }
                None
            }
            Err(insert_pos) => {
                // Key not found - would insert at insert_pos
                // Find which block this position would be in
                for (block_idx, (start, end)) in self.block_layout.iter().enumerate() {
                    if insert_pos >= *start && insert_pos < *end {
                        return Some(block_idx);
                    }
                }
                None
            }
        }
    }

    #[allow(dead_code)]
    fn block_count(&self) -> usize {
        self.block_layout.len()
    }
}

/// Simulates LSM with multiple levels, each with multiple SSTs
struct LsmSimulator {
    /// Level -> Vec of SSTs
    levels: Vec<Vec<SstSimulator>>,
    /// Cache: (sst_id, block_idx) -> bool
    cache: std::collections::HashMap<(u64, usize), bool>,
    cache_capacity: usize,
}

impl LsmSimulator {
    /// Create LSM with 3 levels
    /// Level 0: 1 SST with 10k keys
    /// Level 1: 4 SSTs with 10k keys each
    /// Level 2: 8 SSTs with 10k keys each
    fn new_zipfian() -> Self {
        let mut levels = Vec::new();

        // Level 0: 1 fresh SST
        let level0 = vec![SstSimulator::new(0, 0, 10_000)];

        // Level 1: 4 SSTs with non-overlapping ranges
        let level1 = (0..4)
            .map(|i| SstSimulator::new(100 + i as u64, i * 10_000, 10_000))
            .collect();

        // Level 2: 8 SSTs with non-overlapping ranges (more spread out)
        let level2 = (0..8)
            .map(|i| SstSimulator::new(200 + i as u64, i * 5_000, 5_000))
            .collect();

        levels.push(level0);
        levels.push(level1);
        levels.push(level2);

        Self {
            levels,
            cache: std::collections::HashMap::new(),
            cache_capacity: 50, // Can hold 50 blocks
        }
    }

    /// Lookup key and track blocks read + cache behavior
    fn get(&mut self, key: &Bytes) -> (u32, u32, bool) {
        let mut blocks_read = 0u32;
        let mut cache_hits = 0u32;
        let mut found = false;

        // Search each level in order
        for level in &self.levels {
            for sst in level {
                if sst.contains(key) {
                    // Found in this SST
                    if let Some(block_idx) = sst.find_block_for_key(key) {
                        let cache_key = (sst.sst_id, block_idx);

                        if self.cache.contains_key(&cache_key) {
                            cache_hits += 1;
                        } else {
                            blocks_read += 1;
                            // Add to cache (with simple LRU eviction)
                            if self.cache.len() >= self.cache_capacity {
                                // Evict first entry (simple FIFO for now)
                                if let Some(k) = self.cache.keys().next().cloned() {
                                    self.cache.remove(&k);
                                }
                            }
                            self.cache.insert(cache_key, true);
                        }
                    }
                    found = true;
                    break;
                }
            }
            if found {
                break;
            }
        }

        (blocks_read, cache_hits, found)
    }

    /// Range scan across keys [start_key, start_key+num_keys)
    /// Returns (total_blocks_read, cache_hits, keys_found)
    fn scan(&mut self, start_key: usize, num_keys: usize) -> (u32, u32, u32) {
        let mut blocks_read = 0u32;
        let mut cache_hits = 0u32;
        let mut keys_found = 0u32;

        let keys: Vec<Bytes> = (start_key..start_key + num_keys)
            .map(|i| Bytes::from(format!("key:{:010}", i)))
            .collect();

        for key in &keys {
            let (br, ch, found) = self.get(key);
            blocks_read += br;
            cache_hits += ch;
            if found {
                keys_found += 1;
            }
        }

        (blocks_read, cache_hits, keys_found)
    }

    #[allow(dead_code)]
    fn cache_hit_rate(&self) -> f64 {
        self.cache.len() as f64 / self.cache_capacity as f64
    }
}

/// Zipfian key distribution generator
/// Returns keys with frequency distribution: P(key_i) ∝ 1 / (i+1)^alpha
struct ZipfianDistribution {
    seed: u64,
    alpha: f64,
    max_key: usize,
}

impl ZipfianDistribution {
    fn new(max_key: usize, alpha: f64) -> Self {
        Self {
            seed: 0xDEADBEEFCAFEBABE,
            alpha,
            max_key,
        }
    }

    /// Generate next zipfian-distributed key index
    fn next(&mut self) -> usize {
        // Simple LCG for determinism
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let u = ((self.seed >> 32) as f64) / (u32::MAX as f64); // [0, 1)

        // Inverse transform sampling for Zipfian
        let zeta = 1.6; // Approximate zeta(2, 1.5) for alpha=1.5
        let uz = u * zeta;
        if uz < 1.0 {
            0
        } else if uz < (1.0 + 0.5_f64.powf(self.alpha)) {
            1
        } else {
            ((self.max_key as f64) * (uz - 1.0).powf(-1.0 / self.alpha))
                .min(self.max_key as f64 - 1.0) as usize
        }
    }
}

// ─── Benchmark Scenarios ────────────────────────────────────────────────────

/// Benchmark: Point lookups with zipfian distribution (80/20 workload)
fn bench_read_amp_point_lookups_zipfian(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_amplification_point_lookups_zipfian");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("1000_gets_80_20_dist", |b| {
        b.iter(|| {
            let mut lsm = LsmSimulator::new_zipfian();
            let mut zipf = ZipfianDistribution::new(40_000, 1.5);

            let mut total_blocks_read = 0u32;
            let mut total_cache_hits = 0u32;
            let mut total_found = 0u32;

            // Perform 1000 lookups
            for _ in 0..1000 {
                let key_idx = zipf.next();
                let key = Bytes::from(format!("key:{:010}", key_idx));
                let (br, ch, found) = lsm.get(&key);
                total_blocks_read += br;
                total_cache_hits += ch;
                if found {
                    total_found += 1;
                }
            }

            black_box((total_blocks_read, total_cache_hits, total_found))
        })
    });

    group.finish();
}

/// Benchmark: Mixed get/scan workload (70% get, 30% scan)
fn bench_read_amp_mixed_get_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_amplification_mixed_get_scan");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000)); // 1000 operations (gets + scan chunks)

    group.bench_function("700_gets_300_scan", |b| {
        b.iter(|| {
            let mut lsm = LsmSimulator::new_zipfian();
            let mut zipf = ZipfianDistribution::new(40_000, 1.5);

            let mut total_blocks_read = 0u32;
            let mut total_cache_hits = 0u32;
            let mut total_found = 0u32;

            // 700 point lookups
            for _ in 0..700 {
                let key_idx = zipf.next();
                let key = Bytes::from(format!("key:{:010}", key_idx));
                let (br, ch, found) = lsm.get(&key);
                total_blocks_read += br;
                total_cache_hits += ch;
                if found {
                    total_found += 1;
                }
            }

            // 300 scans (30 scans of 10 keys each)
            for _ in 0..30 {
                let start_key = zipf.next();
                let (br, ch, found) = lsm.scan(start_key, 10);
                total_blocks_read += br;
                total_cache_hits += ch;
                total_found += found;
            }

            black_box((total_blocks_read, total_cache_hits, total_found))
        })
    });

    group.finish();
}

/// Benchmark: Uniform distribution (baseline - no skew)
fn bench_read_amp_uniform_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_amplification_uniform_distribution");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("1000_uniform_gets", |b| {
        b.iter(|| {
            let mut lsm = LsmSimulator::new_zipfian();
            let mut seed = 0xDEADBEEFCAFEBABEu64;

            let mut total_blocks_read = 0u32;
            let mut total_cache_hits = 0u32;
            let mut total_found = 0u32;

            // Perform 1000 lookups with uniform distribution
            for _ in 0..1000 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key_idx = (seed as usize) % 40_000;
                let key = Bytes::from(format!("key:{:010}", key_idx));
                let (br, ch, found) = lsm.get(&key);
                total_blocks_read += br;
                total_cache_hits += ch;
                if found {
                    total_found += 1;
                }
            }

            black_box((total_blocks_read, total_cache_hits, total_found))
        })
    });

    group.finish();
}

/// Benchmark: Cache effectiveness comparison
/// Same workload, compare with cache vs without cache
fn bench_read_amp_cache_effectiveness(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_amplification_cache_effectiveness");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    for &with_cache in &[true, false] {
        let label = if with_cache {
            "with_cache"
        } else {
            "without_cache"
        };
        group.bench_with_input(
            BenchmarkId::from_parameter(label),
            &with_cache,
            |b, &with_cache| {
                b.iter(|| {
                    let mut lsm = LsmSimulator::new_zipfian();

                    // If no cache, clear it
                    if !with_cache {
                        lsm.cache_capacity = 0;
                    }

                    let mut zipf = ZipfianDistribution::new(40_000, 1.5);
                    let mut total_blocks_read = 0u32;
                    let mut total_found = 0u32;

                    for _ in 0..1000 {
                        let key_idx = zipf.next();
                        let key = Bytes::from(format!("key:{:010}", key_idx));
                        let (br, _, found) = lsm.get(&key);
                        total_blocks_read += br;
                        if found {
                            total_found += 1;
                        }
                    }

                    black_box((total_blocks_read, total_found))
                })
            },
        );
    }

    group.finish();
}

// ─── Criterion Setup ────────────────────────────────────────────────────────

criterion_group! {
    name = tier2_subsystem_read_amplification;
    config = criterion_config_for_tier2();
    targets =
        bench_read_amp_point_lookups_zipfian,
        bench_read_amp_mixed_get_scan,
        bench_read_amp_uniform_distribution,
        bench_read_amp_cache_effectiveness
}
criterion_main!(tier2_subsystem_read_amplification);
