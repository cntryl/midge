//! Tier 4 — YCSB Workload E (Scan heavy)
//!
//! Workload E: 95% scans, 5% inserts.

use cntryl_stress::{stress_main, stress_test, StressContext};

use std::cell::Cell;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::MidgeOptions;

const INITIAL_KEYS: usize = 100_000;
const WARMUP: Duration = Duration::from_secs(10);
const MEASURED: Duration = Duration::from_secs(30);

const SCAN_LEN: u64 = 64;

fn run_workload_e(ctx: &mut StressContext, opts: MidgeOptions) {
    // Load phase (populate initial dataset)
    let engine = ycsb::open_tier4_engine(opts);
    let cf = engine.default_column_family();
    ycsb::load_initial_dataset(&engine, &cf, INITIAL_KEYS);

    // Warm-up phase (unmeasured)
    {
        let mut rng = ycsb::XorShift64::new(0xE0E0_EA11_5678_9ABC);
        let mut next_key_id = INITIAL_KEYS as u64;

        let _ = ycsb::run_for_duration(WARMUP, |i| {
            if (rng.next_u64() % 100) < 95 {
                let max_start = next_key_id.saturating_sub(SCAN_LEN + 1).max(1);
                let start_id = rng.next_u64() % max_start;
                let start = ycsb::make_key(start_id);
                let end = ycsb::make_key(start_id.saturating_add(SCAN_LEN));

                let scanned = engine
                    .range(&cf, &start[..], &end[..])
                    .expect("warmup range")
                    .len() as u64;

                scanned * (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
            } else {
                let k = ycsb::make_key(next_key_id);
                let v = ycsb::make_value((i % 251) as u8);
                engine.put(&cf, &k[..], &v[..]).expect("warmup insert");
                next_key_id += 1;
                (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
            }
        });
    }

    // Measured phase (steady-state only; duration-based)
    // Expected: scan-heavy throughput; latency is sensitive to compaction.
    let ops_done = Cell::new(0u64);
    let bytes_done = Cell::new(0u64);

    ctx.measure_ref(&engine, |e| {
        let mut rng = ycsb::XorShift64::new(0xE0E0_EA5E_5678_9ABC);
        let mut next_key_id = INITIAL_KEYS as u64;

        let (ops, bytes) = ycsb::run_for_duration(MEASURED, |i| {
            if (rng.next_u64() % 100) < 95 {
                let max_start = next_key_id.saturating_sub(SCAN_LEN + 1).max(1);
                let start_id = rng.next_u64() % max_start;
                let start = ycsb::make_key(start_id);
                let end = ycsb::make_key(start_id.saturating_add(SCAN_LEN));

                let scanned = e
                    .range(&cf, &start[..], &end[..])
                    .expect("measured range")
                    .len() as u64;

                scanned * (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
            } else {
                let k = ycsb::make_key(next_key_id);
                let v = ycsb::make_value((i % 251) as u8);
                e.put(&cf, &k[..], &v[..]).expect("measured insert");
                next_key_id += 1;
                (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64
            }
        });

        ops_done.set(ops);
        bytes_done.set(bytes);
    });

    ctx.set_elements(ops_done.get());
    ctx.set_bytes(bytes_done.get());
}

#[stress_test]
fn tier4_ycsb_e_mem(ctx: &mut StressContext) {
    // Expected: scan-heavy; throughput stable, latency proportional to scan length.
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_e(ctx, opts);
}

#[stress_test]
fn tier4_ycsb_e_local(ctx: &mut StressContext) {
    // Expected: scan-heavy; sensitive to compaction and iterator efficiency.
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_e(ctx, opts);
}

#[stress_test]
fn tier4_ycsb_e_cloud(ctx: &mut StressContext) {
    // Expected: slowest scans; higher variance from storage backpressure.
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_e(ctx, opts);
}

stress_main!();
