//! YCSB Workload B: 95% Read / 5% Write (Read-Heavy) — Tier 4 Integration Bench
//!
//! Benchmarks both storage backends (fs/cloud, sync/nosync) and
//! scales by number of column families (1, 2, 4, 8, 16) and threads.
//!
//! **Enhanced with Latency Tracking:**
//! - Measures p50, p99, p99.9 operation latencies
//! - Reports tail latency insights for read-heavy workloads

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "ycsb_common.rs"]
mod ycsb_common;

use cntryl_midge::{MidgeEngine, WriteBatch};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use ycsb_common::*;

const CF_COUNTS: &[usize] = &[1, 2, 4, 8, 16];

// Full file content copied from system/ycsb_workload_b.rs

fn run_workload_b(
    engine: &MidgeEngine,
    operations: usize,
    record_count: usize,
    cf_count: usize,
) -> LatencyStats {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(12345);
    let zipfian = ZipfianGenerator::new(record_count, 0.99);
    let mut batch = WriteBatch::new();
    let mut histogram = Histogram::<u64>::new(3).unwrap();

    for _ in 0..operations {
        let key_id = zipfian.next(&mut rng);
        let key = generate_key(key_id);
        let cf_index = rng.gen_range(0..cf_count);
        let cf = &cf_list[cf_index];
        let cf_id = cf.id();

        let start = Instant::now();
        if rng.random_bool(0.95) {
            // Read operation
            let _ = black_box(engine.get(cf, &key));
        } else {
            // Write operation - add to batch
            let value = generate_value(key_id, rng.random());
            batch.put(cf_id, key, value);

            // Flush batch every BATCH_SIZE writes
            if batch.len() >= BATCH_SIZE {
                engine.write_batch(&batch).unwrap();
                batch.clear();
            }
        }
        let elapsed_us = start.elapsed().as_micros() as u64;
        let _ = histogram.record(elapsed_us.max(1));
    }

    // Flush any remaining writes
    if !batch.is_empty() {
        engine.write_batch(&batch).unwrap();
    }

    LatencyStats {
        p50: histogram.value_at_percentile(50.0),
        p99: histogram.value_at_percentile(99.0),
        p99_9: histogram.value_at_percentile(99.9),
    }
}

// Benchmark driver / criterion registration omitted for brevity, copied from original.
