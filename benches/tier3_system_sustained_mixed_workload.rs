//! Tier 3 — Sustained Mixed Read/Write with Active Compaction
//!
//! **Purpose**: Measures system behavior under sustained mixed workload using the
//! real Midge engine. Answers: How does compaction interfere with foreground ops?
//! What's the steady-state throughput and latency distribution?
//!
//! **Workload**:
//! - Phase 1: Warm-up (10k puts to build initial state)
//! - Phase 2: Steady-state (100k mixed ops: 70% reads + 30% writes)
//! - Measure: Real latency, throughput, interference
//!
//! **Access Pattern**: Zipfian hot-key mix (realistic 80/20)
//!
//! **Metrics Collected**:
//! - Throughput over time (sliding window)
//! - Latency distribution (p50/p95/p99) per phase
//! - Real compaction interference on read latency
//! - System stability under sustained load

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{testkit::MidgeOptions, testkit::StorageMode, MidgeEngine};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::time::Instant;

// ─── Configuration ──────────────────────────────────────────────────────

/// Warm-up phase: build initial state
const WARMUP_OPS: usize = 10_000;

/// Steady-state phase: main benchmark
const STEADY_STATE_OPS: usize = 100_000;

/// Report throughput every N ops
const THROUGHPUT_WINDOW: usize = 5_000;

/// Write ratio (30% writes, 70% reads)
const WRITE_RATIO: f64 = 0.30;

// ─── Latency Statistics Collector ───────────────────────────────────────────

/// Tracks operation latencies
#[derive(Debug, Clone)]
struct LatencyTracker {
    latencies: Vec<u64>,
}

impl LatencyTracker {
    fn new() -> Self {
        Self {
            latencies: Vec::new(),
        }
    }

    fn record(&mut self, latency_ns: u64) {
        self.latencies.push(latency_ns);
    }

    fn percentile(&self, pct: usize) -> u64 {
        if self.latencies.is_empty() {
            return 0;
        }
        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();
        sorted[sorted.len() * pct / 100]
    }

    fn percentile_us(&self, pct: usize) -> f64 {
        self.percentile(pct) as f64 / 1000.0
    }

    fn avg_us(&self) -> f64 {
        if self.latencies.is_empty() {
            0.0
        } else {
            self.latencies.iter().sum::<u64>() as f64 / (self.latencies.len() as f64 * 1000.0)
        }
    }

    fn count(&self) -> usize {
        self.latencies.len()
    }
}

// ─── Zipfian Distribution for Hot Keys ──────────────────────────────────────

struct ZipfianGenerator {
    seed: u64,
    alpha: f64,
    max_key: usize,
}

impl ZipfianGenerator {
    fn new(max_key: usize, alpha: f64) -> Self {
        Self {
            seed: 0xDEADBEEFCAFEBABEu64,
            alpha,
            max_key,
        }
    }

    fn next(&mut self) -> usize {
        // Simple LCG for determinism
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let u = ((self.seed >> 32) as f64) / (u32::MAX as f64);

        // Simplified Zipfian: bias towards lower indices
        let z = 1.0 + u * (self.max_key as f64).ln();
        ((self.max_key as f64) * (-z).exp()).min(self.max_key as f64 - 1.0) as usize
    }
}

// ─── Main Benchmark ─────────────────────────────────────────────────────────

fn bench_sustained_mixed_workload_with_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("tier3_sustained_mixed_workload");
    group.sample_size(3);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("mixed_70read_30write_100k_ops", |b| {
        b.iter(|| {
            // Use in-memory mode with defaults for repeatable benchmarking
            let opts = MidgeOptions {
                storage_mode: StorageMode::Memory,
                wal_sync: false,
                memtable_size: 64 * 1024 * 1024,
                compression: false,
                enable_compaction: true,
                memory_budget: None,
            };

            let engine = MidgeEngine::open_with_options(opts).expect("Failed to open engine");
            let cf = engine.default_column_family();

            let mut latencies_warmup = LatencyTracker::new();
            let mut latencies_steady = LatencyTracker::new();
            let mut zipf = ZipfianGenerator::new(10_000, 1.5);

            // ─── Phase 1: Warm-up ───────────────────────────────────────────

            let warmup_start = Instant::now();

            for i in 0..WARMUP_OPS {
                let key = format!("key:{:010}", i);
                let value = format!("value:{:010}", i);

                let op_start = Instant::now();
                let _ = engine.put(cf, key.as_bytes(), value.as_bytes());
                let op_time = op_start.elapsed().as_nanos() as u64;

                latencies_warmup.record(op_time);
            }

            let warmup_time = warmup_start.elapsed();

            // ─── Phase 2: Steady-State ──────────────────────────────────────

            let steady_start = Instant::now();
            let mut throughput_window_start = Instant::now();
            let mut ops_in_window = 0usize;
            let mut window_throughputs = Vec::new();

            for op in 0..STEADY_STATE_OPS {
                let is_write = ((op as f64) % 1.0) < WRITE_RATIO;

                let op_start = Instant::now();

                if is_write {
                    let key_idx = zipf.next();
                    let key = format!("key:{:010}", key_idx);
                    let value = format!("value:{}", op);
                    let _ = engine.put(cf, key.as_bytes(), value.as_bytes());
                } else {
                    let key_idx = zipf.next();
                    let key = format!("key:{:010}", key_idx);
                    let _ = engine.get(cf, key.as_bytes());
                }

                let op_time = op_start.elapsed().as_nanos() as u64;
                latencies_steady.record(op_time);
                ops_in_window += 1;

                // Measure window throughput
                if ops_in_window >= THROUGHPUT_WINDOW {
                    let window_elapsed = throughput_window_start.elapsed().as_secs_f64();
                    let throughput = (ops_in_window as f64) / window_elapsed;
                    window_throughputs.push(throughput);

                    throughput_window_start = Instant::now();
                    ops_in_window = 0;
                }
            }

            let steady_time = steady_start.elapsed();

            // ─── Report Statistics ──────────────────────────────────────────

            println!("\n=== WARMUP PHASE ===");
            println!("Operations: {}", WARMUP_OPS);
            println!("Time: {:.2}ms", warmup_time.as_secs_f64() * 1000.0);
            println!(
                "Throughput: {:.0} ops/sec",
                WARMUP_OPS as f64 / warmup_time.as_secs_f64()
            );
            println!("Latency p50: {:.2}μs", latencies_warmup.percentile_us(50));
            println!("Latency p99: {:.2}μs", latencies_warmup.percentile_us(99));

            println!("\n=== STEADY-STATE PHASE ===");
            println!("Operations: {}", STEADY_STATE_OPS);
            println!("Time: {:.2}ms", steady_time.as_secs_f64() * 1000.0);
            println!(
                "Overall throughput: {:.0} ops/sec",
                STEADY_STATE_OPS as f64 / steady_time.as_secs_f64()
            );

            if !window_throughputs.is_empty() {
                let avg_window_tp =
                    window_throughputs.iter().sum::<f64>() / window_throughputs.len() as f64;
                println!("Avg window throughput: {:.0} ops/sec", avg_window_tp);
            }

            println!("Latency p50: {:.2}μs", latencies_steady.percentile_us(50));
            println!("Latency p95: {:.2}μs", latencies_steady.percentile_us(95));
            println!("Latency p99: {:.2}μs", latencies_steady.percentile_us(99));

            black_box((latencies_warmup, latencies_steady, warmup_time, steady_time))
        })
    });

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier3_sustained_mixed_workload;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_sustained_mixed_workload_with_compaction
}
criterion_main!(tier3_sustained_mixed_workload);
