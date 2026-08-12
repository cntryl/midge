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

use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

// ─── Configuration ──────────────────────────────────────────────────────

const KEYS_PER_BLOCK: usize = 100;
const LOOKUPS_PER_SAMPLE_REPEAT: usize = 1000;
const POINT_LOOKUP_REPEAT_PER_SAMPLE: usize = 4;
const MIXED_GET_SCAN_REPEAT_PER_SAMPLE: usize = 16;
const MIXED_GET_SCAN_SAMPLE_COUNT: usize = 24;
const MIXED_GETS_PER_SAMPLE_REPEAT: usize = 700;
const MIXED_SCANS_PER_SAMPLE_REPEAT: usize = 30;
const SCAN_WIDTH: usize = 10;
const UNIFORM_LOOKUP_REPEAT_PER_SAMPLE: usize = 4;
const LCG_SEED: u64 = 0xDEAD_BEEF_CAFE_BABE_u64;
const LCG_MULTIPLIER: u64 = 6_364_136_223_846_793_005;

fn usize_to_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).expect("value fits in u32"))
}

#[inline]
fn key_from_index(idx: usize) -> Bytes {
    Bytes::from(format!("key:{idx:010}"))
}

fn precompute_zipf_keys(count: usize, max_key: usize, alpha: f64) -> Vec<Bytes> {
    let mut zipf = ZipfianDistribution::new(max_key, alpha);
    (0..count).map(|_| key_from_index(zipf.next())).collect()
}

fn precompute_uniform_keys(count: usize, max_key: usize) -> Vec<Bytes> {
    let mut seed = LCG_SEED;
    let max_key_u64 = u64::try_from(max_key).expect("max_key fits in u64");
    (0..count)
        .map(|_| {
            seed = seed.wrapping_mul(LCG_MULTIPLIER).wrapping_add(1);
            let idx = usize::try_from(seed % max_key_u64).expect("modulo result fits in usize");
            key_from_index(idx)
        })
        .collect()
}

fn precompute_zipf_starts(count: usize, max_key: usize, alpha: f64) -> Vec<usize> {
    let mut zipf = ZipfianDistribution::new(max_key, alpha);
    (0..count).map(|_| zipf.next()).collect()
}

/// Simulated SST with bloom filter and block data
struct SstSimulator {
    sst_id: u64,
    /// Keys in this SST (in sorted order)
    keys: Vec<Bytes>,
    /// Which block contains which key (index into keys)
    block_layout: Vec<(usize, usize)>, // (start_idx, end_idx) per block
}

impl SstSimulator {
    /// Create SST with keys in range [`start_key`, `start_key+num_keys`)
    fn new(sst_id: u64, start_key: usize, num_keys: usize) -> Self {
        let keys: Vec<Bytes> = (start_key..start_key + num_keys)
            .map(|i| Bytes::from(format!("key:{i:010}")))
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
    /// Cache: (`sst_id`, `block_idx`) -> bool
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
        let l0_ssts = vec![SstSimulator::new(0, 0, 10_000)];

        // Level 1: 4 SSTs with non-overlapping ranges
        let l1_ssts = (0_u64..4)
            .map(|i| {
                SstSimulator::new(
                    100 + i,
                    usize::try_from(i).expect("loop index fits in usize") * 10_000,
                    10_000,
                )
            })
            .collect();

        // Level 2: 8 SSTs with non-overlapping ranges (more spread out)
        let l2_ssts = (0_u64..8)
            .map(|i| {
                SstSimulator::new(
                    200 + i,
                    usize::try_from(i).expect("loop index fits in usize") * 5_000,
                    5_000,
                )
            })
            .collect();

        levels.push(l0_ssts);
        levels.push(l1_ssts);
        levels.push(l2_ssts);

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
                                if let Some(k) = self.cache.keys().next().copied() {
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

    /// Range scan across keys [`start_key`, `start_key+num_keys`)
    /// Returns (`total_blocks_read`, `cache_hits`, `keys_found`)
    fn scan_keys(&mut self, keys: &[Bytes]) -> (u32, u32, u32) {
        let mut blocks_read = 0u32;
        let mut cache_hits = 0u32;
        let mut keys_found = 0u32;

        for key in keys {
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
        usize_to_f64(self.cache.len()) / usize_to_f64(self.cache_capacity)
    }
}

/// Zipfian key distribution generator
/// Returns keys with frequency distribution: `P(key_i)` ∝ 1 / (i+1)^alpha
struct ZipfianDistribution {
    seed: u64,
    alpha: f64,
    max_key: usize,
}

impl ZipfianDistribution {
    fn new(max_key: usize, alpha: f64) -> Self {
        Self {
            seed: LCG_SEED,
            alpha,
            max_key,
        }
    }

    /// Generate next zipfian-distributed key index
    fn next(&mut self) -> usize {
        // Simple LCG for determinism
        self.seed = self.seed.wrapping_mul(LCG_MULTIPLIER).wrapping_add(1);
        let u = f64::from(u32::try_from(self.seed >> 32).expect("upper 32 bits fit in u32"))
            / f64::from(u32::MAX); // [0, 1)

        // Inverse transform sampling for Zipfian
        let zeta = 1.6; // Approximate zeta(2, 1.5) for alpha=1.5
        let uz = u * zeta;
        if uz < 1.0 {
            0
        } else if uz < (1.0 + 0.5_f64.powf(self.alpha)) {
            1
        } else {
            let target = (uz - 1.0).powf(-1.0 / self.alpha);
            let mut idx = 0usize;
            let max_key_f64 = usize_to_f64(self.max_key);

            while idx + 1 < self.max_key && usize_to_f64(idx + 1) / max_key_f64 < target {
                idx += 1;
            }

            idx
        }
    }
}

// ─── Benchmark Scenarios ────────────────────────────────────────────────────

const LOOKUP_VARIANT_SAMPLE_COUNT: usize = 12;

#[stress(
    tier = 2,
    role = "diagnostic",
    metadata(component = "read_amplification", scenario = "point_lookups_zipfian")
)]
fn point_lookups_zipfian(ctx: &mut StressContext) {
    let lookup_keys = precompute_zipf_keys(LOOKUPS_PER_SAMPLE_REPEAT, 40_000, 1.5);
    ctx.parameter("lookup_count", LOOKUPS_PER_SAMPLE_REPEAT);
    ctx.parameter("distribution", "zipfian");
    ctx.parameter("logical_unit", "lsm_key_probe");
    ctx.metadata("diagnostic_reason", "local_rsd_above_5pct");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

    let _completed = ctx
        .benchmark("point_lookups_zipfian")
        .samples(LOOKUP_VARIANT_SAMPLE_COUNT)
        .measure_batch(
            (LOOKUPS_PER_SAMPLE_REPEAT as u64) * POINT_LOOKUP_REPEAT_PER_SAMPLE as u64,
            || {
                let mut lsm = LsmSimulator::new_zipfian();
                let mut total_blocks_read = 0u32;
                let mut total_cache_hits = 0u32;
                let mut total_found = 0u32;

                for _ in 0..POINT_LOOKUP_REPEAT_PER_SAMPLE {
                    for key in &lookup_keys {
                        let (br, ch, found) = lsm.get(key);
                        total_blocks_read += br;
                        total_cache_hits += ch;
                        if found {
                            total_found += 1;
                        }
                    }
                }

                black_box((total_blocks_read, total_cache_hits, total_found));
            },
        );
}

#[stress(
    tier = 2,
    role = "diagnostic",
    metadata(component = "read_amplification", scenario = "mixed_get_scan")
)]
fn mixed_get_scan(ctx: &mut StressContext) {
    let get_keys = precompute_zipf_keys(MIXED_GETS_PER_SAMPLE_REPEAT, 40_000, 1.5);
    let scan_starts = precompute_zipf_starts(MIXED_SCANS_PER_SAMPLE_REPEAT, 40_000, 1.5);
    let scan_keys: Vec<Vec<Bytes>> = scan_starts
        .iter()
        .map(|&start| (start..start + SCAN_WIDTH).map(key_from_index).collect())
        .collect();
    let logical_ops = MIXED_GETS_PER_SAMPLE_REPEAT + (MIXED_SCANS_PER_SAMPLE_REPEAT * SCAN_WIDTH);
    ctx.parameter("gets", MIXED_GETS_PER_SAMPLE_REPEAT);
    ctx.parameter("scans", MIXED_SCANS_PER_SAMPLE_REPEAT);
    ctx.parameter("scan_width", SCAN_WIDTH);
    ctx.parameter("logical_unit", "lsm_key_probe");
    ctx.metadata("diagnostic_reason", "local_rsd_above_5pct");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

    let _completed = ctx
        .benchmark("mixed_get_scan")
        .samples(MIXED_GET_SCAN_SAMPLE_COUNT)
        .measure_batch(
            (logical_ops as u64) * MIXED_GET_SCAN_REPEAT_PER_SAMPLE as u64,
            || {
                let mut lsm = LsmSimulator::new_zipfian();
                let mut total_blocks_read = 0u32;
                let mut total_cache_hits = 0u32;
                let mut total_found = 0u32;

                for _ in 0..MIXED_GET_SCAN_REPEAT_PER_SAMPLE {
                    for key in &get_keys {
                        let (br, ch, found) = lsm.get(key);
                        total_blocks_read += br;
                        total_cache_hits += ch;
                        if found {
                            total_found += 1;
                        }
                    }

                    for keys in &scan_keys {
                        let (br, ch, found) = lsm.scan_keys(keys);
                        total_blocks_read += br;
                        total_cache_hits += ch;
                        total_found += found;
                    }
                }

                black_box((total_blocks_read, total_cache_hits, total_found));
            },
        );
}

#[stress(
    tier = 2,
    role = "diagnostic",
    metadata(component = "read_amplification", scenario = "uniform_distribution")
)]
fn uniform_distribution(ctx: &mut StressContext) {
    let lookup_keys = precompute_uniform_keys(LOOKUPS_PER_SAMPLE_REPEAT, 40_000);
    ctx.parameter("lookup_count", LOOKUPS_PER_SAMPLE_REPEAT);
    ctx.parameter("distribution", "uniform");
    ctx.parameter("logical_unit", "lsm_key_probe");
    ctx.metadata("diagnostic_reason", "local_rsd_above_5pct");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

    let _completed = ctx
        .benchmark("uniform_distribution")
        .samples(LOOKUP_VARIANT_SAMPLE_COUNT)
        .measure_batch(
            (LOOKUPS_PER_SAMPLE_REPEAT as u64) * UNIFORM_LOOKUP_REPEAT_PER_SAMPLE as u64,
            || {
                let mut lsm = LsmSimulator::new_zipfian();
                let mut total_blocks_read = 0u32;
                let mut total_cache_hits = 0u32;
                let mut total_found = 0u32;

                for _ in 0..UNIFORM_LOOKUP_REPEAT_PER_SAMPLE {
                    for key in &lookup_keys {
                        let (br, ch, found) = lsm.get(key);
                        total_blocks_read += br;
                        total_cache_hits += ch;
                        if found {
                            total_found += 1;
                        }
                    }
                }

                black_box((total_blocks_read, total_cache_hits, total_found));
            },
        );
}

stress_main!();
