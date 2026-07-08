//! Tier 3 — SST primitives
//!
//! Measures: cost of point seek, iterator construction, first advance
//! NOT: full scans, iteration, payload processing

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use cntryl_midge::MidgeEngine;
use stress_config::MidgeOptions;

const KEY_SIZE: usize = stress_config::bench_stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;
const SST_POINT_SEEK_BATCH_SIZE: usize = 1;
const SST_RANGE_SEEK_BATCH_SIZE: usize = 64;
const SST_POINT_SEEK_SAMPLE_COUNT: usize = 12;
const SST_POINT_SEEK_WARMUP_SAMPLES: usize = 4;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    stress_config::bench_stress::open_engine_no_compaction(opts)
}

fn precompute_keys(num: usize) -> Vec<[u8; KEY_SIZE]> {
    stress_config::bench_stress::precompute_keys16_u64_be(num)
}

fn run_sst_point_seek_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: MidgeOptions,
    num_keys: usize,
) {
    ctx.parameter("logical_batch_size", SST_POINT_SEEK_BATCH_SIZE);
    ctx.parameter("logical_unit", "sst_point_seek");
    ctx.parameter("operation_surface", "sst_point_seek");
    ctx.parameter("begin_tx_included", "false");
    ctx.parameter("rotating_key_count", num_keys);

    let write_opts = stress_config::measured_write_options(&opts);
    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup outside measurement: create an SST
    let keys = precompute_keys(num_keys);
    let cf_id = cf.id();
    let total = keys.len();
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for (i, k) in keys[start..end].iter().enumerate() {
            let idx = start + i;
            let v = vec![u8::try_from(idx % 251).expect("value byte fits in u8"); VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");
    let mut key_index = num_keys / 2;

    let _ = ctx
        .benchmark(scenario)
        .samples(SST_POINT_SEEK_SAMPLE_COUNT)
        .warmup(SST_POINT_SEEK_WARMUP_SAMPLES)
        .measure_batch(SST_POINT_SEEK_BATCH_SIZE as u64, || {
            for _ in 0..SST_POINT_SEEK_BATCH_SIZE {
                let key = keys[key_index % keys.len()];
                key_index = key_index.wrapping_add(1);
                let _ = tx.get(&key[..]).unwrap();
            }
        });

    drop(engine);
}

fn run_sst_range_seek_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: MidgeOptions,
    num_keys: usize,
) {
    ctx.parameter("logical_batch_size", SST_RANGE_SEEK_BATCH_SIZE);
    ctx.parameter("logical_unit", "sst_range_seek");
    ctx.parameter("operation_surface", "sst_range_seek_first_row");
    ctx.parameter("begin_tx_included", "false");
    ctx.parameter("rotating_key_count", num_keys);

    let write_opts = stress_config::measured_write_options(&opts);
    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup outside measurement: create an SST
    let keys = precompute_keys(num_keys);
    let cf_id = cf.id();
    let total = keys.len();
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for (i, k) in keys[start..end].iter().enumerate() {
            let idx = start + i;
            let v = vec![u8::try_from(idx % 251).expect("value byte fits in u8"); VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");
    let mut key_index = 0usize;

    let _ = ctx.measure_batch(scenario, SST_RANGE_SEEK_BATCH_SIZE as u64, || {
        for _ in 0..SST_RANGE_SEEK_BATCH_SIZE {
            let start_index = key_index % (keys.len() - 33);
            let start = keys[start_index];
            let end = keys[start_index + 32];
            key_index = key_index.wrapping_add(1);
            let query = cntryl_midge::Query::new()
                .start_key(cntryl_midge::Bytes::copy_from_slice(&start[..]))
                .end_key(cntryl_midge::Bytes::copy_from_slice(&end[..]));
            let mut it = tx.scan(&query).expect("scan failed");
            let _ = it.next();
        }
    });

    drop(engine);
}

#[stress(tier = 3)]
fn tier3_sst_point_seek_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_sst_point_seek_case(ctx, "tier3_sst_point_seek_local", opts, 5_000);
}

#[stress(tier = 3)]
fn tier3_sst_point_seek_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_sst_point_seek_case(ctx, "tier3_sst_point_seek_cloud", opts, 5_000);
}

#[stress(tier = 3)]
fn tier3_sst_range_seek_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_sst_range_seek_case(ctx, "tier3_sst_range_seek_local", opts, 10_000);
}

stress_main!();
