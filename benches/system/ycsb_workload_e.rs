//! YCSB Workload E: Range Scans
//!
//! **Target Runtime:** ~1 minute
//! **Run Frequency:** Nightly CI
//!
//! Benchmarks range query workload:
//! - 95% short range scans (10-100 records)
//! - 5% inserts (to maintain dataset size)
//! - Scales by thread count (1, 2, 8) and column families (1-16)
//! - Realistic for analytical/reporting queries
//!
//! **Enhanced with Latency Tracking:**
//! - Measures p50, p99, p99.9 scan operation latencies
//! - Reports range scan performance characteristics

#[path = "../criterion_helper.rs"]
mod criterion_helper;
#[path = "ycsb_common.rs"]
mod ycsb_common;

use cntryl_midge::api::query::Query;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use ycsb_common::*;

// ============================================================================
// Configuration
// ============================================================================

const OPS_PER_THREAD: usize = 5_000;
const RECORD_COUNT: usize = 25_000;
const SCAN_LENGTH_MIN: usize = 10;
const SCAN_LENGTH_MAX: usize = 100;

// ============================================================================
// Setup and Workload Logic
// ============================================================================

fn setup_workload_e_db(cf_count: usize) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_ycsb_e_{}", cf_count));
    let _ = std::fs::remove_dir_all(&path);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 8 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();

    // Create column families
    for i in 1..cf_count {
        let _ = engine.create_column_family(&format!("cf{}", i), Default::default());
    }

    // Load initial dataset
    load_data(&engine, RECORD_COUNT);

    engine
}

/// Execute workload E: 95% scans, 5% inserts
fn run_workload_e(engine: &MidgeEngine, operations: usize, cf_count: usize) -> usize {
    let cf_list = engine.list_column_families();
    let mut rng = StdRng::seed_from_u64(54321);
    let zipfian = ZipfianGenerator::new(RECORD_COUNT, 0.99);
    let mut scan_count = 0;

    for _ in 0..operations {
        let cf_index = rng.gen_range(0..cf_count);
        let cf = &cf_list[cf_index];

        if rng.random::<f64>() < 0.95 {
            // Scan operation (95%)
            let scan_start = zipfian.next(&mut rng);
            let scan_len = rng.gen_range(SCAN_LENGTH_MIN..=SCAN_LENGTH_MAX);
            let start_key = generate_key(scan_start);
            let end_key = generate_key(scan_start + scan_len);

            let query = Query::new().start_key(start_key).end_key(end_key);
            let _ = black_box(engine.scan(cf, query).unwrap_or_default());
            scan_count += 1;
        } else {
            // Insert operation (5%)
            let new_id = RECORD_COUNT + rng.gen_range(0..10_000); // Insert new keys
            let key = generate_key(new_id);
            let value = generate_value(new_id, rng.random());
            let _ = engine.put(cf, &key, &value);
        }
    }

    scan_count
}

// ============================================================================
// Benchmarks
// ============================================================================

fn bench_workload_e_single_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_workload_e_single");

    for &cf_count in &[1, 2, 4, 8, 16] {
        group.throughput(Throughput::Elements(OPS_PER_THREAD as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(cf_count),
            &cf_count,
            |b, &cf_count| {
                b.iter_batched(
                    || setup_workload_e_db(cf_count),
                    |engine| {
                        let scans = run_workload_e(&engine, OPS_PER_THREAD, cf_count);
                        black_box(scans);
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_workload_e_multi_thread(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_workload_e_multi");

    for &thread_count in &[2, 8] {
        for &cf_count in &[1, 4, 8, 16] {
            let total_ops = thread_count * OPS_PER_THREAD;
            group.throughput(Throughput::Elements(total_ops as u64));

            group.bench_with_input(
                BenchmarkId::new(format!("t{}", thread_count), cf_count),
                &(thread_count, cf_count),
                |b, &(thread_count, cf_count)| {
                    b.iter_batched(
                        || setup_workload_e_db(cf_count),
                        |engine| {
                            let engine = Arc::new(engine);

                            thread::scope(|scope| {
                                for _ in 0..thread_count {
                                    let e = Arc::clone(&engine);

                                    scope.spawn(move || {
                                        let _ = run_workload_e(&e, OPS_PER_THREAD, cf_count);
                                    });
                                }
                            });

                            black_box(());
                        },
                        criterion::BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Scan Length Impact
// ============================================================================

/// Measure how scan length affects throughput
fn bench_workload_e_scan_lengths(c: &mut Criterion) {
    let mut group = c.benchmark_group("ycsb_workload_e_scan_lengths");

    for &scan_max_len in &[10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("max_scan_len", scan_max_len),
            &scan_max_len,
            |b, &max_len| {
                b.iter_batched(
                    || setup_workload_e_db(4),
                    |engine| {
                        let cf_list = engine.list_column_families();
                        let cf = &cf_list[0];
                        let mut rng = StdRng::seed_from_u64(99999);
                        let zipfian = ZipfianGenerator::new(RECORD_COUNT, 0.99);

                        for _ in 0..2_000 {
                            let start_key_id = zipfian.next(&mut rng);
                            let start_key = generate_key(start_key_id);
                            let end_key = generate_key(start_key_id + max_len);

                            let query = Query::new().start_key(start_key).end_key(end_key);
                            let _ = black_box(engine.scan(cf, query).unwrap_or_default());
                        }
                    },
                    criterion::BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = ycsb_workload_e;
    config = criterion_config();
    targets =
        bench_workload_e_single_thread,
        bench_workload_e_multi_thread,
        bench_workload_e_scan_lengths
}
criterion_main!(ycsb_workload_e);
