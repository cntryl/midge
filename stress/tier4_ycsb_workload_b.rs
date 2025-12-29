//! Tier 4 — YCSB Workload B (Read mostly)
//!
//! Workload B: 95% reads, 5% updates.

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

fn run_workload_b(ctx: &mut StressContext, opts: MidgeOptions) {
    // Load phase (populate initial dataset)
    let engine = ycsb::open_tier4_engine(opts);
    let cf = engine.default_column_family();
    ycsb::load_initial_dataset(&engine, &cf, INITIAL_KEYS);

    // Warm-up phase (unmeasured)
    {
        let mut rng = ycsb::XorShift64::new(0xB0B0_BA11_5678_9ABC);
        let zipf = ZipfianGenerator::new(INITIAL_KEYS, ZIPFIAN_THETA);
        let _ = ycsb::run_for_duration(WARMUP, |i| {
            let key_idx = zipf.next_from_u64(&mut || rng.next_u64()) as u64;
            let k = ycsb::make_key(key_idx);

            if (rng.next_u64() % 100) < 95 {
                let _ = engine.get(&cf, &k[..]).expect("warmup get");
            } else {
                let v = ycsb::make_value((i % 251) as u8);
                engine.put(&cf, &k[..], &v[..]).expect("warmup put");
            }

            (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
        });
    }

    // Measured phase (steady-state only; duration-based)
    // Expected: read-mostly throughput; updates introduce occasional stalls.
    let ops_done = Cell::new(0u64);
    let bytes_done = Cell::new(0u64);

    ctx.measure_ref(&engine, |e| {
        let mut rng = ycsb::XorShift64::new(0xB0B0_EA5E_5678_9ABC);
        let zipf = ZipfianGenerator::new(INITIAL_KEYS, ZIPFIAN_THETA);

        let (ops, bytes) = ycsb::run_for_duration(MEASURED, |i| {
            let key_idx = zipf.next_from_u64(&mut || rng.next_u64()) as u64;
            let k = ycsb::make_key(key_idx);

            if (rng.next_u64() % 100) < 95 {
                let _ = e.get(&cf, &k[..]).expect("measured get");
            } else {
                let v = ycsb::make_value((i % 251) as u8);
                e.put(&cf, &k[..], &v[..]).expect("measured put");
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
fn tier4_ycsb_b_mem(ctx: &mut StressContext) {
    // Expected: read-mostly; high throughput; stable latency.
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_b(ctx, opts);
}

#[stress_test]
fn tier4_ycsb_b_local(ctx: &mut StressContext) {
    // Expected: high throughput with occasional stalls on updates/flush.
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_b(ctx, opts);
}

#[stress_test]
fn tier4_ycsb_b_cloud(ctx: &mut StressContext) {
    // Expected: lower throughput; higher tail latency due to cloud durability.
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_b(ctx, opts);
}

stress_main!();
