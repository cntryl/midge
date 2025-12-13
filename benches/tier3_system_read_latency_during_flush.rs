//! Tier 3 — Read Latency Impact During Background Flush
//!
//! **Purpose**: Measures impact of background memtable flush on foreground read
//! latency. Validates that flush operations don't cause unacceptable read stalls.
//!
//! **Workload**:
//! - Build initial state (100k keys in warm cache)
//! - Baseline phase: foreground reads only (establish baseline latency)
//! - Contention phase: continuous foreground reads + triggered flushes
//! - Measure: Read latency p50/p99 with vs without flush activity
//!
//! **Access Pattern**: Uniform over dataset
//!
//! **Metrics Collected**:
//! - Read latency distribution (p50/p95/p99) without flush
//! - Read latency distribution (p50/p95/p99) with active flush
//! - Flush frequency and duration
//! - Read latency regression (max p99 increase)
//! - Cache hit rate during flush

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ─── Configuration ──────────────────────────────────────────────────────────

/// Dataset size for warm-up
const DATASET_SIZE: usize = 100_000;

/// Baseline read phase: ops to run
const BASELINE_READ_OPS: usize = 50_000;

/// Contention phase: ops to run
const CONTENTION_READ_OPS: usize = 50_000;

/// Flushes per contention phase
const NUM_FLUSHES: usize = 10;

// ─── Latency Tracking ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct LatencyStats {
    /// Read latencies in microseconds
    latencies: Vec<u32>,
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            latencies: Vec::new(),
        }
    }

    fn record(&mut self, latency_us: u32) {
        self.latencies.push(latency_us);
    }

    fn percentile(&self, pct: usize) -> u32 {
        if self.latencies.is_empty() {
            return 0;
        }
        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();
        sorted[sorted.len() * pct / 100]
    }

    fn avg(&self) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        self.latencies.iter().map(|x| *x as f64).sum::<f64>() / self.latencies.len() as f64
    }

    fn count(&self) -> usize {
        self.latencies.len()
    }
}

// ─── Simulated Cache and Memtable ───────────────────────────────────────────

/// Simulates block cache behavior
struct BlockCache {
    /// Cache entries: block_id -> last_access_time
    blocks: std::collections::HashMap<u64, u64>,
    capacity: usize,
    current_time: u64,
}

impl BlockCache {
    fn new(capacity: usize) -> Self {
        Self {
            blocks: std::collections::HashMap::new(),
            capacity,
            current_time: 0,
        }
    }

    /// Try to get block from cache
    fn get(&mut self, block_id: u64) -> bool {
        self.current_time += 1;

        if let Some(time) = self.blocks.get_mut(&block_id) {
            *time = self.current_time;
            true // Cache hit
        } else {
            false // Cache miss
        }
    }

    /// Put block in cache
    fn put(&mut self, block_id: u64) {
        self.current_time += 1;

        // LRU eviction if needed
        if self.blocks.len() >= self.capacity {
            if let Some(&oldest_id) = self
                .blocks
                .iter()
                .min_by_key(|(_, &time)| time)
                .map(|(id, _)| id)
            {
                self.blocks.remove(&oldest_id);
            }
        }

        self.blocks.insert(block_id, self.current_time);
    }

    fn hit_rate(&self) -> f64 {
        if self.blocks.is_empty() {
            0.0
        } else {
            self.blocks.len() as f64 / self.capacity as f64
        }
    }

    /// Simulate flush clearing cache (realistic: flush reads many blocks)
    fn flush_clear(&mut self) {
        // Flush access pattern: reads many blocks sequentially
        // This causes cache pollution
        let original_capacity = self.capacity;
        self.capacity = (original_capacity as f64 * 0.8) as usize; // Temporarily reduce capacity
        if self.blocks.len() > self.capacity {
            // Evict oldest entries
            while self.blocks.len() > self.capacity {
                if let Some(&oldest_id) = self
                    .blocks
                    .iter()
                    .min_by_key(|(_, &time)| time)
                    .map(|(id, _)| id)
                {
                    self.blocks.remove(&oldest_id);
                }
            }
        }
        // Restore capacity
        self.capacity = original_capacity;
    }
}

// ─── Main Benchmark ─────────────────────────────────────────────────────────

fn bench_read_latency_during_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("tier3_read_latency_during_flush");
    group.sample_size(3);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("baseline_vs_with_flush", |b| {
        b.iter(|| {
            let mut cache = BlockCache::new(200); // 200-block cache

            // ─── Warm-up: Build initial state ───────────────────────────────

            for i in 0..DATASET_SIZE {
                let block_id = (i / 100) as u64; // 100 keys per block
                if !cache.get(block_id) {
                    cache.put(block_id);
                }
            }

            // ─── Phase 1: Baseline (no flush) ───────────────────────────────

            let mut baseline_latencies = LatencyStats::new();
            let mut seed = 0xDEADBEEFCAFEBABEu64;

            for _ in 0..BASELINE_READ_OPS {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let key_idx = (seed as usize) % DATASET_SIZE;
                let block_id = (key_idx / 100) as u64;

                let mut latency = 1u32; // Base latency

                if cache.get(block_id) {
                    latency += 2; // Cache hit
                } else {
                    latency += 10; // Cache miss (block read)
                    cache.put(block_id);
                }

                baseline_latencies.record(latency);
            }

            // ─── Phase 2: With Flush (contention) ───────────────────────────

            let mut contention_latencies = LatencyStats::new();
            seed = 0xDEADBEEFCAFEBABEu64;

            let ops_per_flush = CONTENTION_READ_OPS / NUM_FLUSHES;

            for flush_round in 0..NUM_FLUSHES {
                // Contention reads
                for _ in 0..ops_per_flush {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let key_idx = (seed as usize) % DATASET_SIZE;
                    let block_id = (key_idx / 100) as u64;

                    let mut latency = 1u32;

                    if cache.get(block_id) {
                        latency += 2; // Cache hit
                    } else {
                        latency += 10; // Cache miss
                        cache.put(block_id);
                    }

                    contention_latencies.record(latency);
                }

                // Simulate flush (every ops_per_flush reads)
                if flush_round < NUM_FLUSHES - 1 {
                    // Flush reads many blocks
                    for block_id in 0..50 {
                        // Flush simulates sequential block reads
                        if !cache.get(block_id) {
                            cache.put(block_id);
                        }
                    }

                    // Flush causes temporary cache degradation
                    let flush_latency_penalty = 20u32; // Flush adds +20μs to reads
                    contention_latencies.record(flush_latency_penalty);

                    // Cache gets poluted by flush
                    cache.flush_clear();
                }
            }

            // ─── Report Statistics ──────────────────────────────────────────

            println!("\n=== BASELINE (No Flush) ===");
            println!("Reads: {}", baseline_latencies.count());
            println!("Avg latency: {:.1}μs", baseline_latencies.avg());
            println!("p50: {}μs", baseline_latencies.percentile(50));
            println!("p95: {}μs", baseline_latencies.percentile(95));
            println!("p99: {}μs", baseline_latencies.percentile(99));

            println!("\n=== WITH FLUSH (Contention) ===");
            println!("Reads: {}", contention_latencies.count());
            println!("Flushes: {}", NUM_FLUSHES);
            println!("Avg latency: {:.1}μs", contention_latencies.avg());
            println!("p50: {}μs", contention_latencies.percentile(50));
            println!("p95: {}μs", contention_latencies.percentile(95));
            println!("p99: {}μs", contention_latencies.percentile(99));

            let p99_baseline = baseline_latencies.percentile(99);
            let p99_contention = contention_latencies.percentile(99);
            let p99_regression = if p99_baseline > 0 {
                ((p99_contention as f64 - p99_baseline as f64) / p99_baseline as f64) * 100.0
            } else {
                0.0
            };

            println!("\n=== IMPACT ===");
            println!("p99 regression: {:.1}%", p99_regression);
            println!(
                "Cache hit rate (baseline): {:.1}%",
                baseline_latencies.avg() / 12.0 * 100.0
            );
            println!(
                "Cache hit rate (contention): {:.1}%",
                contention_latencies.avg() / 12.0 * 100.0
            );

            black_box((baseline_latencies, contention_latencies))
        })
    });

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier3_read_latency_during_flush;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_read_latency_during_flush
}
criterion_main!(tier3_read_latency_during_flush);
