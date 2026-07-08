//! Tier 3 — Engine primitives
//!
//! Measures: cost of repeated put/get/commit primitives
//! NOT: bulk operations, batch throughput, or volume scaling
//!
//! **Measurement Notes:**
//! - Memory mode: reads from in-memory skiplist (memtable)
//! - Local mode: reads from flushed SST via block cache
//! - Cloud mode: reads from cloud-backed SST via block cache
//!
//! Different storage modes may show different latencies because they exercise
//! different code paths. This is expected and informative, not a bug.
//! Memory mode hits memtable, while local/cloud modes hit the block cache
//! after the setup flush.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use stress_config::MidgeOptions;

const VALUE_SIZE: usize = 128;
const PUT_BATCH_SIZE: usize = 64;
const GET_BATCH_SIZE: usize = 128;
const GET_KEY_COUNT: usize = 4096;

fn run_single_put_case(ctx: &mut StressContext, scenario: &'static str, opts: MidgeOptions) {
    ctx.parameter("put_batch_size", PUT_BATCH_SIZE);
    ctx.parameter("logical_batch_size", PUT_BATCH_SIZE);
    ctx.parameter("logical_unit", "engine_put_commit");
    ctx.parameter("operation_surface", "engine_put_commit");
    ctx.parameter("begin_tx_included", "true");
    ctx.parameter("memtable_size_bytes", opts.memtable_size);
    match scenario {
        "tier3_engine_put_mem" => stress_config::mark_local_rsd_diagnostic(ctx),
        "tier3_engine_put_local" => {
            stress_config::mark_capped_probe(ctx, "local_commit_duration_window_fixed_batch");
        }
        _ => {}
    }
    let write_opts = stress_config::measured_write_options(&opts);

    let engine = stress_config::bench_stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    let keys: Vec<[u8; 16]> = (0..4096)
        .map(stress_config::bench_stress::key16_u64_be)
        .collect();
    let v = vec![1u8; VALUE_SIZE];
    let mut key_index = 0usize;

    // Measure repeated logical put/commit calls per framework iteration.
    let _ = ctx.measure_batch(scenario, PUT_BATCH_SIZE as u64, || {
        for _ in 0..PUT_BATCH_SIZE {
            let k = keys[key_index % keys.len()];
            key_index = key_index.wrapping_add(1);
            let e = &engine;
            let mut tx = e
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.put(k.to_vec(), v.clone(), None).unwrap();
            tx.commit(write_opts).unwrap();
        }
    });

    drop(engine);
}

fn run_single_get_case(ctx: &mut StressContext, scenario: &'static str, opts: MidgeOptions) {
    ctx.parameter("logical_batch_size", GET_BATCH_SIZE);
    ctx.parameter("logical_unit", "engine_point_read");
    ctx.parameter("operation_surface", "engine_get");
    ctx.parameter("begin_tx_included", "true");
    ctx.parameter("rotating_key_count", GET_KEY_COUNT);
    match scenario {
        "tier3_engine_get_mem" => {
            stress_config::mark_capped_probe(ctx, "memory_point_read_duration_plateau");
        }
        "tier3_engine_get_local" => {
            stress_config::mark_capped_probe(ctx, "local_point_read_duration_plateau");
        }
        _ => {}
    }

    let engine = stress_config::bench_stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Setup (not measured): write rotating read keys.
    let keys: Vec<[u8; 16]> = (0..GET_KEY_COUNT)
        .map(|index| stress_config::bench_stress::key16_u64_be(index as u64))
        .collect();
    {
        let v = vec![1u8; VALUE_SIZE];
        for chunk in keys.chunks(PUT_BATCH_SIZE) {
            let mut tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            for key in chunk {
                tx.put(key.to_vec(), v.clone(), None).unwrap();
            }
            tx.commit(cntryl_midge::WriteOptions::best_effort())
                .unwrap();
        }
        engine.flush_cf(&cf).unwrap(); // Ensure durability before measurement
    }

    let mut key_index = 0usize;

    let _ = ctx.measure_batch(scenario, GET_BATCH_SIZE as u64, || {
        for _ in 0..GET_BATCH_SIZE {
            let key = keys[key_index % keys.len()];
            key_index = key_index.wrapping_add(1);
            let tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin");
            let _ = tx.get(&key[..]).unwrap();
        }
    });

    drop(engine);
}

// MOVED TO TIER 4: batch throughput testing belongs in tier4_system_engine.rs
// This was a Tier 3 violation: loop inside measured body violates Rule 3.

// ---------------------------------------------------------------------------
// Stress tests
// ---------------------------------------------------------------------------

#[stress(tier = 3)]
fn tier3_engine_put_mem(ctx: &mut StressContext) {
    let opts = stress_config::write_coordination_opts_for_mode("memory");
    run_single_put_case(ctx, "tier3_engine_put_mem", opts);
}

#[stress(tier = 3)]
fn tier3_engine_put_local(ctx: &mut StressContext) {
    let opts = stress_config::write_coordination_opts_for_mode("local");
    run_single_put_case(ctx, "tier3_engine_put_local", opts);
}

#[stress(tier = 3)]
fn tier3_engine_put_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_single_put_case(ctx, "tier3_engine_put_cloud", opts);
}

#[stress(tier = 3)]
fn tier3_engine_get_mem(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("memory");
    run_single_get_case(ctx, "tier3_engine_get_mem", opts);
}

#[stress(tier = 3)]
fn tier3_engine_get_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_single_get_case(ctx, "tier3_engine_get_local", opts);
}

#[stress(tier = 3)]
fn tier3_engine_get_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_single_get_case(ctx, "tier3_engine_get_cloud", opts);
}

stress_main!();
