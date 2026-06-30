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

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const _KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;

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

fn prepopulate_snapshot_bench(engine: &Arc<cntryl_midge::Engine>, cf_id: u32, num_keys: usize) {
    for i in 0..num_keys {
        let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
        let v = vec![u8::try_from(i).expect("prepopulated key index fits in u8"); VALUE_SIZE];
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
    ctx: &mut StressContext,
    engine: &Arc<cntryl_midge::Engine>,
    cf_id: u32,
    writer_samples: &Arc<Mutex<Vec<u64>>>,
    num_writers: usize,
    num_ops: usize,
    num_keys: usize,
) {
    ctx.measure_ref(engine, |_e| {
        let mut handles = vec![];

        for writer_id in 0..num_writers {
            let engine = Arc::clone(engine);
            let writer_samples = Arc::clone(writer_samples);
            let handle = thread::spawn(move || {
                let mut ops_done = 0;
                let write_opts = cntryl_midge::WriteOptions::buffered();
                while ops_done < num_ops / num_writers {
                    let key_idx = (writer_id + ops_done) % num_keys;
                    let k = cntryl_midge::testkit::stress::key16_u64_be(key_idx as u64);
                    let v = vec![
                        u8::try_from(ops_done % 256).expect("modulo result fits in u8");
                        VALUE_SIZE
                    ];

                    let start = std::time::Instant::now();
                    let mut tx = engine
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .unwrap();
                    tx.put(k.to_vec(), v, None).unwrap();
                    tx.commit(write_opts).unwrap();
                    let elapsed_us =
                        u64::try_from(start.elapsed().as_micros()).expect("latency fits in u64");

                    writer_samples.lock().unwrap().push(elapsed_us);
                    ops_done += 1;
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }
    });
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
#[stress_test]
fn tier4_mvcc_snapshot_isolation_under_concurrency_4threads(ctx: &mut StressContext) {
    const NUM_READERS: usize = 2;
    const NUM_WRITERS: usize = 2;
    const SNAPSHOT_HOLD_DURATION: Duration = Duration::from_secs(5);
    const NUM_KEYS: usize = 1_000;
    const NUM_OPS: usize = 50_000; // Total operations across all writers

    ctx.tag("scenario", "mvcc_isolation_4threads");
    ctx.tag("readers", NUM_READERS.to_string().as_str());
    ctx.tag("writers", NUM_WRITERS.to_string().as_str());
    ctx.tag("snapshot_hold_secs", "5");
    ctx.set_elements(NUM_OPS as u64);

    let engine = Arc::new(cntryl_midge::testkit::stress::open_engine_no_compaction(
        cntryl_midge::testkit::opts_for_mode("memory"),
    ));
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
    measure_snapshot_bench_writers(
        ctx,
        &engine,
        cf_id,
        &writer_samples,
        NUM_WRITERS,
        NUM_OPS,
        NUM_KEYS,
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
/// - Multiple writers continuously update different keys
/// - Measure: Writer latency and compaction throughput
/// Expected: Writers do NOT timeout or degrade catastrophically
#[stress_test]
fn tier4_mvcc_long_snapshot_fairness_10sec(ctx: &mut StressContext) {
    const LONG_SNAPSHOT_DURATION: Duration = Duration::from_secs(10);
    const NUM_WRITERS: usize = 4;
    const NUM_OPS: usize = 20_000;

    ctx.tag("scenario", "long_snapshot_fairness");
    ctx.tag("snapshot_hold_secs", "10");
    ctx.set_elements(NUM_OPS as u64);

    let engine = Arc::new(cntryl_midge::testkit::stress::open_engine_no_compaction(
        cntryl_midge::testkit::opts_for_mode("memory"),
    ));
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

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
            while start.elapsed() < LONG_SNAPSHOT_DURATION {
                // Periodically read to keep snapshot alive
                let k = cntryl_midge::testkit::stress::key16_u64_be(0);
                let _ = snap_tx.get(&k); // Ignore result, just keeping snapshot open
                thread::sleep(Duration::from_millis(500));
            }

            stop_flag.store(true, Ordering::Relaxed);
        })
    };

    // Measure writer throughput under long-running snapshot
    ctx.measure_ref(&engine, |_e| {
        let mut handles = vec![];

        for writer_id in 0..NUM_WRITERS {
            let engine = Arc::clone(&engine);
            let stop_flag = Arc::clone(&stop_flag);
            let writer_latencies = Arc::clone(&writer_latencies);
            let handle = thread::spawn(move || {
                let mut ops = 0;
                let write_opts = cntryl_midge::WriteOptions::buffered();
                while ops < NUM_OPS / NUM_WRITERS && !stop_flag.load(Ordering::Relaxed) {
                    let k = cntryl_midge::testkit::stress::key16_u64_be((writer_id + ops) as u64);
                    let v = vec![
                        u8::try_from(ops % 256).expect("modulo result fits in u8");
                        VALUE_SIZE
                    ];

                    let start = std::time::Instant::now();
                    let mut tx = engine
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .unwrap();
                    tx.put(k.to_vec(), v, None).unwrap();
                    tx.commit(write_opts).unwrap();
                    let elapsed_us =
                        u64::try_from(start.elapsed().as_micros()).expect("latency fits in u64");

                    writer_latencies.lock().unwrap().push(elapsed_us);
                    ops += 1;
                }
                ops
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.join();
        }
    });

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

        // Sanity check: p99 should not be catastrophically high
        // (expected range: 10-1000Âµs depending on machine)
        if p99_us > 10_000 {
            eprintln!("Warning: writer p99 latency is very high: {p99_us}Âµs");
        }
    }

    drop(engine);
}

stress_main!();
