//! Tier 4 — YCSB Workload C (Read only)
//!
//! Workload C: 100% reads.

use cntryl_stress::{stress_main, stress_test, StressContext};

use std::cell::Cell;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::testkit::zipfian::ZipfianGenerator;
use cntryl_midge::MidgeOptions;

const INITIAL_KEYS: usize = 100_000;
const WARMUP: Duration = Duration::from_secs(10);
const MEASURED: Duration = Duration::from_secs(30);

const ZIPFIAN_THETA: f64 = 0.99;

fn run_workload_c(ctx: &mut StressContext, opts: MidgeOptions) {
    // Load phase (populate initial dataset)
    let engine = ycsb::open_tier4_engine(opts);
    let cf = engine.default_column_family();
    ycsb::load_initial_dataset(&engine, &cf, INITIAL_KEYS);

    // Warm-up phase (unmeasured)
    {
        let mut rng = ycsb::XorShift64::new(0xC0C0_CA11_5678_9ABC);
        let zipf = ZipfianGenerator::new(INITIAL_KEYS, ZIPFIAN_THETA);
        let _ = ycsb::run_for_duration(WARMUP, |_i| {
            let key_idx = zipf.next_from_u64(&mut || rng.next_u64()) as u64;
            let k = ycsb::make_key(key_idx);
            let _ = engine.get(&cf, &k[..]).expect("warmup get");
            (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
        });
    }

    // Measured phase (steady-state only; duration-based)
    // Expected: high read throughput; latency stable if cache is warm.
    let ops_done = Cell::new(0u64);
    let bytes_done = Cell::new(0u64);

    ctx.measure_ref(&engine, |e| {
        let mut rng = ycsb::XorShift64::new(0xC0C0_EA5E_5678_9ABC);
        let zipf = ZipfianGenerator::new(INITIAL_KEYS, ZIPFIAN_THETA);

        let (ops, bytes) = ycsb::run_for_duration(MEASURED, |_i| {
            let key_idx = zipf.next_from_u64(&mut || rng.next_u64()) as u64;
            let k = ycsb::make_key(key_idx);
            let _ = e.get(&cf, &k[..]).expect("measured get");
            (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
        });

        ops_done.set(ops);
        bytes_done.set(bytes);
    });

    ctx.set_elements(ops_done.get());
    ctx.set_bytes(bytes_done.get());
}

#[stress_test]
fn tier4_ycsb_c_mem(ctx: &mut StressContext) {
    // Expected: very high read throughput; stable latency once warmed.
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts);
}

#[stress_test]
fn tier4_ycsb_c_local(ctx: &mut StressContext) {
    // Expected: high read throughput; sensitive to cache sizing.
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts);
}

#[stress_test]
fn tier4_ycsb_c_cloud(ctx: &mut StressContext) {
    // Expected: lower throughput; higher tail latency.
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts);
}

stress_main!();
