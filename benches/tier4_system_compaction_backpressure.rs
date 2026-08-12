//! Tier 4 - Compaction write backpressure
//!
//! Measures sustained local write throughput while background compaction is enabled.
//! The reported throughput is write throughput under compaction pressure, not raw bytes compacted.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::{MidgeEngine, MidgeError, WriteOptions};
use cntryl_stress::{stress, stress_main, LogicalUnit, OperationOutcome, StressContext};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use stress_config::{ycsb, MidgeOptions};

const VALUE_SIZE: usize = 512;
const WRITE_BATCH_SIZE: usize = 64;
const COMPACTION_MEMTABLE_SIZE_BYTES: usize = 512 * 1024;

#[derive(Default, Clone, Copy)]
struct PhaseResult {
    elapsed: Duration,
    writes: u64,
    transactions: u64,
}

fn make_value(fill: u8) -> [u8; VALUE_SIZE] {
    [fill; VALUE_SIZE]
}

const WRITERS: usize = 1;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(8);

fn open_compaction_engine(mut opts: MidgeOptions) -> MidgeEngine {
    opts.enable_compaction = true;
    opts.memtable_size = COMPACTION_MEMTABLE_SIZE_BYTES;
    MidgeEngine::open(opts.to_open_options()).expect("open compaction backpressure engine")
}

fn run_write_phase(
    engine: &Arc<MidgeEngine>,
    cf_id: cntryl_midge::ColumnFamilyId,
    write_options: fn() -> WriteOptions,
    next_key: &Arc<AtomicU64>,
    writers: usize,
    duration: Duration,
    count: bool,
) -> PhaseResult {
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);

    for _ in 0..writers {
        let engine = Arc::clone(engine);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let next_key = Arc::clone(next_key);

        handles.push(thread::spawn(move || {
            barrier.wait();

            let mut local_writes = 0u64;
            let mut local_transactions = 0u64;

            while !stop.load(Ordering::Acquire) {
                let mut tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin compaction pressure transaction");

                for _ in 0..WRITE_BATCH_SIZE {
                    let seq = next_key.fetch_add(1, Ordering::Relaxed);
                    let key = ycsb::make_key(seq);
                    let value = make_value((seq & 0xFF) as u8);
                    tx.put(key.to_vec(), value.to_vec(), None)
                        .expect("put compaction pressure value");
                }

                match tx.commit(write_options()) {
                    Ok(()) => {
                        if count {
                            local_writes = local_writes.wrapping_add(WRITE_BATCH_SIZE as u64);
                            local_transactions = local_transactions.wrapping_add(1);
                        }
                    }
                    Err(MidgeError::WriteStall(_)) => {
                        let _ = engine
                            .wait_for_write_stall_clear(cf_id, Duration::from_millis(500))
                            .expect("wait for compaction pressure stall");
                    }
                    Err(error) => panic!("commit compaction pressure transaction: {error}"),
                }
            }

            PhaseResult {
                writes: local_writes,
                transactions: local_transactions,
                ..PhaseResult::default()
            }
        }));
    }

    barrier.wait();
    let started_at = Instant::now();
    thread::sleep(duration);
    let elapsed = started_at.elapsed();
    stop.store(true, Ordering::Release);

    let mut out = PhaseResult {
        elapsed,
        ..PhaseResult::default()
    };
    for handle in handles {
        let result = handle
            .join()
            .expect("compaction pressure worker should finish");
        out.writes = out.writes.wrapping_add(result.writes);
        out.transactions = out.transactions.wrapping_add(result.transactions);
    }

    out
}

fn run_compaction_backpressure_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mode: &'static str,
    write_options: fn() -> WriteOptions,
) {
    stress_config::init_benchmark_telemetry().expect("initialize benchmark telemetry");

    let opts = stress_config::opts_for_mode(mode);
    let engine = Arc::new(open_compaction_engine(opts));
    let cf = engine
        .create_column_family("cf1")
        .expect("create compaction pressure column family");
    let cf_id = cf.id();
    let next_key = Arc::new(AtomicU64::new(0));
    let _warmup = run_write_phase(
        &engine,
        cf_id,
        write_options,
        &next_key,
        WRITERS,
        WARMUP,
        false,
    );

    let start = ycsb::capture_runtime_perf_snapshot(&engine);
    let measured = run_write_phase(
        &engine,
        cf_id,
        write_options,
        &next_key,
        WRITERS,
        MEASURED,
        true,
    );
    let perf = ycsb::runtime_perf_report(&engine, start);

    ctx.parameter("storage_profile", mode);
    ctx.parameter("writers", WRITERS);
    ctx.parameter("write_batch_size", WRITE_BATCH_SIZE);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("memtable_size_bytes", COMPACTION_MEMTABLE_SIZE_BYTES);
    ctx.parameter("logical_bytes_per_operation", ycsb::KEY_SIZE + VALUE_SIZE);
    ctx.parameter("warmup_secs", WARMUP.as_secs_f64());
    ctx.parameter("measured_secs", MEASURED.as_secs_f64());
    assert_eq!(
        measured.writes,
        measured
            .transactions
            .saturating_mul(WRITE_BATCH_SIZE as u64),
        "each completed transaction must account for one full write batch"
    );

    ctx.record_external_outcome(
        scenario,
        measured.elapsed,
        LogicalUnit::new("write"),
        OperationOutcome::success(measured.writes),
    );
    ycsb::record_runtime_correctness(ctx, &perf);
}

#[stress(tier = 4)]
fn tier4_compaction_write_backpressure_local(ctx: &mut StressContext) {
    run_compaction_backpressure_case(
        ctx,
        "tier4_compaction_write_backpressure_local",
        "local",
        WriteOptions::buffered,
    );
}

stress_main!();
