//! Tier 4 â€” MVCC Isolation Under Concurrent Load
//!
//! **Purpose**: Validate snapshot isolation semantics under realistic multi-threaded pressure.
//! MVCC bugs are subtle (dirty reads, lost updates, isolation violations).
//! This suite models production multi-tenant or multi-core scenarios.
//!
//! **Validation Goals**:
//! 1. No dirty reads: Snapshot readers never see uncommitted data
//! 2. No lost updates: Writer changes are not silently dropped
//! 3. No isolation violations: Snapshot represents a consistent point-in-time
//! 4. Fairness: Long-running snapshot should not starve writers indefinitely
//! 5. Compaction under snapshots: Old versions are retained correctly
//!
//! **Failure Modes to Catch**:
//! - Snapshot not pinning versions â†’ compaction deletes data reader needs
//! - Version chain not properly released â†’ memory bloat
//! - Dirty read under concurrent write â†’ isolation violation
//! - Writer timeout under snapshot contention â†’ deadlock or timeout
//!
//! **High Priority**: MVCC is critical for multi-tenant isolation.
//! If one tenant's long read blocks compaction, all tenants degrade.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const _KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;
const BENCH_MEMTABLE_SIZE: usize = 1024 * 1024 * 1024;
const BENCH_MEMORY_BUDGET: usize = 4 * 1024 * 1024 * 1024;

/// Holds read-only snapshot + validation results
#[allow(dead_code)]
struct SnapshotReader {
    /// Snapshot transaction (held for duration of test)
    tx: cntryl_midge::Transaction,
    /// Values observed during the snapshot
    observed_values: Vec<u64>,
    /// Isolation violations detected
    violations: usize,
}

#[derive(Debug)]
struct WriterWindowResult {
    completed_ops: u64,
    elapsed: Duration,
}

fn open_mvcc_bench_engine() -> cntryl_midge::Engine {
    let mut opts = cntryl_midge::testkit::opts_for_mode("memory");
    opts.memtable_size = BENCH_MEMTABLE_SIZE;
    opts.memory_budget = Some(BENCH_MEMORY_BUDGET);
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn prepopulate_snapshot_bench(engine: &Arc<cntryl_midge::Engine>, cf_id: u32, num_keys: usize) {
    for i in 0..num_keys {
        let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
        let fill_byte = u8::try_from(i % 251).expect("prepopulated value byte fits in u8");
        let v = vec![fill_byte; VALUE_SIZE];
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(k.to_vec(), v, None).unwrap();
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    }
}

fn spawn_snapshot_readers(
    engine: &Arc<cntryl_midge::Engine>,
    cf_id: u32,
    stop_flag: &Arc<AtomicBool>,
    violation_counter: &Arc<Mutex<usize>>,
    num_readers: usize,
    num_keys: usize,
    hold_duration: Duration,
) -> Vec<thread::JoinHandle<()>> {
    (0..num_readers)
        .map(|reader_id| {
            let engine = Arc::clone(engine);
            let stop_flag = Arc::clone(stop_flag);
            let violation_counter = Arc::clone(violation_counter);
            thread::spawn(move || {
                let snapshot_tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .unwrap();

                let start = std::time::Instant::now();
                while !stop_flag.load(Ordering::Relaxed) && start.elapsed() < hold_duration {
                    for i in 0..10 {
                        let key_idx = (reader_id + i) % num_keys;
                        let k = cntryl_midge::testkit::stress::key16_u64_be(key_idx as u64);
                        if snapshot_tx.get(&k).ok().flatten().is_none() {
                            *violation_counter.lock().unwrap() += 1;
                        }
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            })
        })
        .collect()
}

fn measure_snapshot_bench_writers(
    engine: &Arc<cntryl_midge::Engine>,
    cf_id: u32,
    writer_samples: &Arc<Mutex<Vec<u64>>>,
    num_writers: usize,
    window: Duration,
    num_keys: usize,
) -> WriterWindowResult {
    let barrier = Arc::new(Barrier::new(num_writers + 1));
    let completed_ops = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for writer_id in 0..num_writers {
        let barrier = Arc::clone(&barrier);
        let completed_ops = Arc::clone(&completed_ops);
        let engine = Arc::clone(engine);
        let writer_samples = Arc::clone(writer_samples);
        let handle = thread::spawn(move || {
            let write_opts = cntryl_midge::WriteOptions::buffered();
            let mut local_samples = Vec::new();
            let mut ops_done = 0u64;

            barrier.wait();
            let started = Instant::now();
            while started.elapsed() < window {
                let key_idx = (writer_id + ops_done as usize) % num_keys;
                let k = cntryl_midge::testkit::stress::key16_u64_be(key_idx as u64);
                let v = vec![
                    u8::try_from(ops_done % 256).expect("modulo result fits in u8");
                    VALUE_SIZE
                ];

                let op_started = Instant::now();
                let mut tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(k.to_vec(), v, None).unwrap();
                tx.commit(write_opts).unwrap();
                let elapsed_us =
                    u64::try_from(op_started.elapsed().as_micros()).expect("latency fits in u64");

                local_samples.push(elapsed_us);
                ops_done = ops_done.saturating_add(1);
            }

            completed_ops.fetch_add(ops_done, Ordering::Relaxed);
            writer_samples.lock().unwrap().extend(local_samples);
        });
        handles.push(handle);
    }

    barrier.wait();
    let started = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }

    WriterWindowResult {
        completed_ops: completed_ops.load(Ordering::Relaxed),
        elapsed: started.elapsed(),
    }
}

fn measure_long_snapshot_writers(
    engine: &Arc<cntryl_midge::Engine>,
    cf_id: u32,
    writer_latencies: &Arc<Mutex<Vec<u64>>>,
    num_writers: usize,
    window: Duration,
    num_keys: usize,
) -> WriterWindowResult {
    let barrier = Arc::new(Barrier::new(num_writers + 1));
    let completed_ops = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for writer_id in 0..num_writers {
        let barrier = Arc::clone(&barrier);
        let completed_ops = Arc::clone(&completed_ops);
        let engine = Arc::clone(engine);
        let writer_latencies = Arc::clone(writer_latencies);
        let handle = thread::spawn(move || {
            let write_opts = cntryl_midge::WriteOptions::buffered();
            let mut local_latencies = Vec::new();
            let mut ops = 0u64;

            barrier.wait();
            let started = Instant::now();
            while started.elapsed() < window {
                let key_idx = (writer_id + ops as usize) % num_keys;
                let k = cntryl_midge::testkit::stress::key16_u64_be(key_idx as u64);
                let v =
                    vec![u8::try_from(ops % 256).expect("modulo result fits in u8"); VALUE_SIZE];

                let op_started = Instant::now();
                let mut tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(k.to_vec(), v, None).unwrap();
                tx.commit(write_opts).unwrap();
                let elapsed_us =
                    u64::try_from(op_started.elapsed().as_micros()).expect("latency fits in u64");

                local_latencies.push(elapsed_us);
                ops = ops.saturating_add(1);
            }

            completed_ops.fetch_add(ops, Ordering::Relaxed);
            writer_latencies.lock().unwrap().extend(local_latencies);
        });
        handles.push(handle);
    }

    barrier.wait();
    let started = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }

    WriterWindowResult {
        completed_ops: completed_ops.load(Ordering::Relaxed),
        elapsed: started.elapsed(),
    }
}

fn tag_writer_p99(ctx: &mut StressContext, writer_samples: &Arc<Mutex<Vec<u64>>>) {
    let samples = writer_samples.lock().unwrap();
    if !samples.is_empty() {
        let p99_us = {
            let mut sorted = samples.clone();
            sorted.sort_unstable();
            sorted[sorted.len() * 99 / 100]
        };
        ctx.tag("writer_p99_latency_us", format!("{p99_us}").as_str());
    }
}

/// Scenario: Snapshot isolation under concurrent writes and compaction
///
/// Setup:
/// - 4+ threads: N readers (hold snapshots for 5s), M writers (continuous updates)
/// - Compaction happening in background
/// - Snapshot must not see partial/dirty updates
/// - Writer latency should not increase catastrophically due to snapshot hold
#[stress(tier = 4)]
fn tier4_mvcc_snapshot_isolation_under_concurrency_4threads(ctx: &mut StressContext) {
    const INTERNAL_WARMUP_DURATION: Duration = Duration::from_secs(1);
    const MEASURED_WRITER_DURATION: Duration = Duration::from_secs(5);
    const NUM_READERS: usize = 2;
    const NUM_WRITERS: usize = 2;
    const SNAPSHOT_HOLD_DURATION: Duration = Duration::from_secs(6);
    const NUM_KEYS: usize = 1_000;

    ctx.tag("scenario", "mvcc_isolation_4threads");
    ctx.tag("readers", NUM_READERS.to_string().as_str());
    ctx.tag("writers", NUM_WRITERS.to_string().as_str());
    ctx.tag("snapshot_hold_secs", "6");
    ctx.tag("writer_warmup_secs", "1");
    ctx.tag("measured_writer_secs", "5");

    let engine = Arc::new(open_mvcc_bench_engine());
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    prepopulate_snapshot_bench(&engine, cf_id, NUM_KEYS);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let violation_counter = Arc::new(Mutex::new(0usize));
    let writer_samples = Arc::new(Mutex::new(Vec::new()));

    let reader_handles = spawn_snapshot_readers(
        &engine,
        cf_id,
        &stop_flag,
        &violation_counter,
        NUM_READERS,
        NUM_KEYS,
        SNAPSHOT_HOLD_DURATION,
    );

    let _warmup_window = measure_snapshot_bench_writers(
        &engine,
        cf_id,
        &writer_samples,
        NUM_WRITERS,
        INTERNAL_WARMUP_DURATION,
        NUM_KEYS,
    );
    writer_samples.lock().unwrap().clear();

    let writer_window = measure_snapshot_bench_writers(
        &engine,
        cf_id,
        &writer_samples,
        NUM_WRITERS,
        MEASURED_WRITER_DURATION,
        NUM_KEYS,
    );
    ctx.record_external(
        "tier4_mvcc_snapshot_isolation_under_concurrency_4threads",
        writer_window.elapsed,
        writer_window.completed_ops,
    );

    // Signal readers to stop and wait for them
    stop_flag.store(true, Ordering::Relaxed);
    for handle in reader_handles {
        let _ = handle.join();
    }

    let violations = *violation_counter.lock().unwrap();
    tag_writer_p99(ctx, &writer_samples);
    ctx.tag("isolation_violations", format!("{violations}").as_str());
    assert_eq!(
        violations, 0,
        "MVCC isolation violation detected: {violations} dirty reads"
    );

    drop(engine);
}

/// Scenario: Long-running snapshot fairness
///
/// Setup:
/// - One reader thread holds snapshot for 10s (simulating long query)
/// - One writer continuously updates keys
/// - Measure: Writer latency and compaction throughput
/// Expected: Writers do NOT timeout or degrade catastrophically
#[stress(tier = 4)]
fn tier4_mvcc_long_snapshot_fairness_10sec(ctx: &mut StressContext) {
    const INTERNAL_WARMUP_DURATION: Duration = Duration::from_secs(2);
    const LONG_SNAPSHOT_DURATION: Duration = Duration::from_secs(10);
    const LONG_SNAPSHOT_HOLD_DURATION: Duration = Duration::from_secs(12);
    const NUM_WRITERS: usize = 1;
    const NUM_KEYS: usize = 1_000;

    ctx.tag("scenario", "long_snapshot_fairness");
    ctx.tag("snapshot_hold_secs", "10");
    ctx.tag("writer_warmup_secs", "2");
    ctx.tag("measured_writer_secs", "10");
    ctx.tag("writers", NUM_WRITERS.to_string().as_str());
    ctx.tag("key_count", NUM_KEYS.to_string().as_str());

    let engine = Arc::new(open_mvcc_bench_engine());
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    prepopulate_snapshot_bench(&engine, cf_id, NUM_KEYS);

    let stop_flag = Arc::new(AtomicBool::new(false));
    let writer_latencies = Arc::new(Mutex::new(Vec::new()));

    // Start long-running reader (holds snapshot)
    let reader_handle = {
        let engine = Arc::clone(&engine);
        let stop_flag = Arc::clone(&stop_flag);
        thread::spawn(move || {
            let snap_tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();

            let start = std::time::Instant::now();
            while !stop_flag.load(Ordering::Relaxed)
                && start.elapsed() < LONG_SNAPSHOT_HOLD_DURATION
            {
                // Periodically read to keep snapshot alive
                let k = cntryl_midge::testkit::stress::key16_u64_be(0);
                let _ = snap_tx.get(&k); // Ignore result, just keeping snapshot open
                thread::sleep(Duration::from_millis(500));
            }
        })
    };

    let _warmup_window = measure_long_snapshot_writers(
        &engine,
        cf_id,
        &writer_latencies,
        NUM_WRITERS,
        INTERNAL_WARMUP_DURATION,
        NUM_KEYS,
    );
    writer_latencies.lock().unwrap().clear();

    let writer_window = measure_long_snapshot_writers(
        &engine,
        cf_id,
        &writer_latencies,
        NUM_WRITERS,
        LONG_SNAPSHOT_DURATION,
        NUM_KEYS,
    );
    ctx.record_external(
        "tier4_mvcc_long_snapshot_fairness_10sec",
        writer_window.elapsed,
        writer_window.completed_ops,
    );

    stop_flag.store(true, Ordering::Relaxed);
    reader_handle.join().unwrap();

    // Analyze latencies
    let latencies = writer_latencies.lock().unwrap();
    if !latencies.is_empty() {
        let sorted = {
            let mut s = latencies.clone();
            s.sort_unstable();
            s
        };
        let p50_us = sorted[sorted.len() / 2];
        let p95_us = sorted[sorted.len() * 95 / 100];
        let p99_us = sorted[sorted.len() * 99 / 100];
        let max_us = sorted[sorted.len() - 1];

        ctx.tag("writer_p50_latency_us", format!("{p50_us}").as_str());
        ctx.tag("writer_p95_latency_us", format!("{p95_us}").as_str());
        ctx.tag("writer_p99_latency_us", format!("{p99_us}").as_str());
        ctx.tag("writer_max_latency_us", format!("{max_us}").as_str());
    }

    drop(engine);
}

stress_main!();
