//! Tier 3 — Compaction scenarios (stress harness)
//!
//! This file intentionally avoids Criterion.
//! Each scenario is a **single-shot** stress test with an explicit name.

use cntryl_stress::{stress_test, StressContext};

use cntryl_midge::{MidgeEngine, MidgeOptions};

const KEY_SIZE: usize = 16;
const DEFAULT_VALUE_SIZE: usize = 100;

fn precompute_kv(num_keys: usize, value_size: usize) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        let mut k = [0u8; KEY_SIZE];
        k[..8].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(k);
        values.push(vec![(i % 256) as u8; value_size]);
    }

    (keys, values)
}

fn setup_engine(mut opts: MidgeOptions) -> MidgeEngine {
    // Compaction scenarios should not run background compaction.
    opts.enable_compaction = false;
    MidgeEngine::open_with_options(opts).unwrap()
}

fn run_flush_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize, value_size: usize) {
    let (keys, values) = precompute_kv(num_keys, value_size);

    ctx.set_elements(num_keys as u64);
    ctx.set_bytes((num_keys * (KEY_SIZE + value_size)) as u64);

    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured)
    for (k, v) in keys.iter().zip(values.iter()) {
        engine.put(&cf, &k[..], v).unwrap();
    }

    // Measure exactly one flush
    ctx.time_ref(&engine, |e| {
        e.flush().expect("flush failed");
    });

    drop(engine);
}

fn run_compact_all_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    num_keys: usize,
    value_size: usize,
) {
    let (keys, values) = precompute_kv(num_keys, value_size);

    ctx.set_elements(num_keys as u64);
    ctx.set_bytes((num_keys * (KEY_SIZE + value_size)) as u64);

    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured): create multiple SSTs.
    let batches = 4usize;
    let chunk = (num_keys / batches).max(1);

    for i in 0..batches {
        let start = i * chunk;
        let end = if i + 1 == batches {
            num_keys
        } else {
            ((i + 1) * chunk).min(num_keys)
        };
        for idx in start..end {
            engine.put(&cf, &keys[idx][..], values[idx].as_slice()).unwrap();
        }
        engine.flush().unwrap();
    }

    // Measure exactly one full compaction.
    ctx.time_ref(&engine, |e| {
        e.compact_all().expect("compact_all failed");
    });

    drop(engine);
}

fn run_incremental_compact_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    num_keys_per_batch: usize,
    num_batches: usize,
    value_size: usize,
) {
    let total_keys = num_keys_per_batch * num_batches;
    let (base_keys, base_values) = precompute_kv(total_keys, value_size);

    ctx.set_elements(total_keys as u64);
    ctx.set_bytes((total_keys * (KEY_SIZE + value_size)) as u64);

    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup: create multiple L0 files with overlapping keyspace.
    for batch in 0..num_batches {
        let start = batch * num_keys_per_batch;
        let end = start + num_keys_per_batch;

        for idx in start..end {
            let mut k = base_keys[idx];
            // Introduce overlap across batches.
            k[0] = (batch % 10) as u8;
            engine.put(&cf, &k[..], base_values[idx].as_slice()).unwrap();
        }
        engine.flush().unwrap();
    }

    ctx.time_ref(&engine, |e| {
        e.compact_all().expect("compact_all failed");
    });

    drop(engine);
}

// ---------------------------------------------------------------------------
// Stress tests (explicit, one datapoint per test)
// ---------------------------------------------------------------------------

#[stress_test]
fn tier3_compaction_flush_local_disk_5k_100b() {
    let mut ctx = StressContext::new("tier3_compaction_flush_local_disk_5k_100b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_flush_case(&mut ctx, opts, 5_000, DEFAULT_VALUE_SIZE);
    ctx.finish();
}

#[stress_test]
fn tier3_compaction_flush_local_disk_20k_100b() {
    let mut ctx = StressContext::new("tier3_compaction_flush_local_disk_20k_100b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_flush_case(&mut ctx, opts, 20_000, DEFAULT_VALUE_SIZE);
    ctx.finish();
}

#[stress_test]
fn tier3_compaction_compact_all_local_disk_10k_100b() {
    let mut ctx = StressContext::new("tier3_compaction_compact_all_local_disk_10k_100b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_compact_all_case(&mut ctx, opts, 10_000, DEFAULT_VALUE_SIZE);
    ctx.finish();
}

#[stress_test]
fn tier3_compaction_compact_all_local_disk_15k_100b() {
    let mut ctx = StressContext::new("tier3_compaction_compact_all_local_disk_15k_100b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_compact_all_case(&mut ctx, opts, 15_000, DEFAULT_VALUE_SIZE);
    ctx.finish();
}

#[stress_test]
fn tier3_compaction_flush_tp_local_disk_5k_64b() {
    let mut ctx = StressContext::new("tier3_compaction_flush_tp_local_disk_5k_64b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_flush_case(&mut ctx, opts, 5_000, 64);
    ctx.finish();
}

#[stress_test]
fn tier3_compaction_flush_tp_local_disk_5k_256b() {
    let mut ctx = StressContext::new("tier3_compaction_flush_tp_local_disk_5k_256b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_flush_case(&mut ctx, opts, 5_000, 256);
    ctx.finish();
}

#[stress_test]
fn tier3_compaction_flush_tp_local_disk_5k_1024b() {
    let mut ctx = StressContext::new("tier3_compaction_flush_tp_local_disk_5k_1024b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_flush_case(&mut ctx, opts, 5_000, 1024);
    ctx.finish();
}

#[stress_test]
fn tier3_compaction_flush_tp_local_disk_5k_4096b() {
    let mut ctx = StressContext::new("tier3_compaction_flush_tp_local_disk_5k_4096b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_flush_case(&mut ctx, opts, 5_000, 4096);
    ctx.finish();
}

#[stress_test]
fn tier3_compaction_incremental_compact_local_disk_4x2000_100b() {
    let mut ctx = StressContext::new("tier3_compaction_incremental_compact_local_disk_4x2000_100b");
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_incremental_compact_case(&mut ctx, opts, 2_000, 4, DEFAULT_VALUE_SIZE);
    ctx.finish();
}
