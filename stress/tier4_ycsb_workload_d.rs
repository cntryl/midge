//! Tier 4 — YCSB Workload D (Read latest)
//!
//! Workload D: 95% reads, 5% inserts; reads bias toward the most recent keys.

use cntryl_stress::{stress_main, stress_test, StressContext};

use std::cell::Cell;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::MidgeOptions;

const INITIAL_KEYS: usize = 100_000;
const WARMUP: Duration = Duration::from_secs(10);
const MEASURED: Duration = Duration::from_secs(30);

fn run_workload_d(ctx: &mut StressContext, opts: MidgeOptions) {
    // Load phase (populate initial dataset)
    let engine = ycsb::open_tier4_engine(opts);
    let cf = engine.default_column_family();
    ycsb::load_initial_dataset(&engine, &cf, INITIAL_KEYS);

    // Warm-up phase (unmeasured)
    {
        let mut rng = ycsb::XorShift64::new(0xD0D0_DA11_5678_9ABC);
        let mut next_key_id = INITIAL_KEYS as u64;

        let _ = ycsb::run_for_duration(WARMUP, |i| {
            if (rng.next_u64() % 100) < 95 {
                // Read-latest bias: strongly prefer the newest ~10% of keys.
                let recent_window = (next_key_id / 10).max(1);
                let pick = if (rng.next_u64() % 100) < 90 {
                    next_key_id - 1 - (rng.next_u64() % recent_window)
                } else {
                    rng.next_u64() % next_key_id
                };

                let k = ycsb::make_key(pick);
                let _ = engine.get(&cf, &k[..]).expect("warmup get");
            } else {
                let k = ycsb::make_key(next_key_id);
                let v = ycsb::make_value((i % 251) as u8);
                engine.put(&cf, &k[..], &v[..]).expect("warmup insert");
                next_key_id += 1;
            }

            (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
        });
    }

    // Measured phase (steady-state only; duration-based)
    // Expected: stable reads with periodic drops as inserts/flushes occur.
    let ops_done = Cell::new(0u64);
    let bytes_done = Cell::new(0u64);

    ctx.measure_ref(&engine, |e| {
        let mut rng = ycsb::XorShift64::new(0xD0D0_EA5E_5678_9ABC);
        let mut next_key_id = INITIAL_KEYS as u64;

        let (ops, bytes) = ycsb::run_for_duration(MEASURED, |i| {
            if (rng.next_u64() % 100) < 95 {
                let recent_window = (next_key_id / 10).max(1);
                let pick = if (rng.next_u64() % 100) < 90 {
                    next_key_id - 1 - (rng.next_u64() % recent_window)
                } else {
                    rng.next_u64() % next_key_id
                };

                let k = ycsb::make_key(pick);
                let _ = e.get(&cf, &k[..]).expect("measured get");
            } else {
                let k = ycsb::make_key(next_key_id);
                let v = ycsb::make_value((i % 251) as u8);
                e.put(&cf, &k[..], &v[..]).expect("measured insert");
                next_key_id += 1;
            }

            (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
        });

        ops_done.set(ops);
        bytes_done.set(bytes);
    });

    ctx.set_elements(ops_done.get());
    ctx.set_bytes(bytes_done.get());
}

#[stress_test]
fn tier4_ycsb_d_mem(ctx: &mut StressContext) {
    // Expected: read-latest dominates; inserts cause occasional stalls.
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_d(ctx, opts);
}

#[stress_test]
fn tier4_ycsb_d_local(ctx: &mut StressContext) {
    // Expected: lower throughput than mem; background flush/compaction visible.
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_d(ctx, opts);
}

#[stress_test]
fn tier4_ycsb_d_cloud(ctx: &mut StressContext) {
    // Expected: lowest throughput; tail latency higher.
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_d(ctx, opts);
}

stress_main!();
