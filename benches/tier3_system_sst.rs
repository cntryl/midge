//! Tier 3 — SST primitives
//!
//! Measures: cost of point seek, iterator construction, first advance
//! NOT: full scans, iteration, payload processing

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};

use cntryl_midge::MidgeEngine;
use stress_config::MidgeOptions;

const KEY_SIZE: usize = stress_config::bench_stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;
const SST_POINT_SEEK_BATCH_SIZE: usize = 1;
const SST_RANGE_SEEK_BATCH_SIZE: usize = 64;
const SST_FIXTURE_MEMTABLE_SIZE_BYTES: usize = 4 * 1024 * 1024;
const SST_POINT_SEEK_SAMPLE_COUNT: usize = 12;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    stress_config::bench_stress::open_engine_no_compaction(opts)
}

fn precompute_keys(num: usize) -> Vec<[u8; KEY_SIZE]> {
    stress_config::bench_stress::precompute_keys16_u64_be(num)
}

fn run_sst_point_seek_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mut opts: MidgeOptions,
    num_keys: usize,
) {
    // Build one deliberate SST at the explicit flush boundary. The generic
    // local profile's 64 KiB memtable is smaller than this fixture and can
    // trigger write stalls or background flushes during setup.
    opts.memtable_size = opts.memtable_size.max(SST_FIXTURE_MEMTABLE_SIZE_BYTES);

    ctx.parameter("logical_batch_size", SST_POINT_SEEK_BATCH_SIZE);
    ctx.parameter("logical_unit", "sst_point_seek");
    ctx.parameter("operation_surface", "sst_point_seek");
    ctx.parameter("begin_tx_included", "false");
    ctx.parameter("rotating_key_count", num_keys);
    ctx.parameter("fixture_memtable_size_bytes", opts.memtable_size);
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

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
    let read_path_before = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let mut validation_failures = 0_u64;

    let _ = ctx
        .benchmark(scenario)
        .samples(SST_POINT_SEEK_SAMPLE_COUNT)
        .measure_batch(SST_POINT_SEEK_BATCH_SIZE as u64, || {
            for _ in 0..SST_POINT_SEEK_BATCH_SIZE {
                let key = keys[key_index % keys.len()];
                key_index = key_index.wrapping_add(1);
                let expected = vec![
                    u8::try_from(key_index.wrapping_sub(1) % keys.len() % 251)
                        .expect("value byte fits in u8");
                    VALUE_SIZE
                ];
                match tx.get(&key[..]) {
                    Ok(Some(value)) if value.as_ref() == expected.as_slice() => {}
                    _ => validation_failures += 1,
                }
            }
        });

    let read_path_after = engine.read_path_diagnostics_snapshot_for_benchmarks();
    assert_eq!(
        validation_failures, 0,
        "measured SST point reads must validate"
    );
    assert!(
        read_path_after.candidate_sst_files_checked > read_path_before.candidate_sst_files_checked
            && read_path_after.candidate_blocks_checked > read_path_before.candidate_blocks_checked,
        "SST point row must exercise candidate SST and block work"
    );

    // Engine shutdown waits for active transaction guards. Release the
    // read snapshot before dropping the engine so this benchmark can finish.
    drop(tx);
    drop(engine);
}

fn run_sst_range_seek_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mut opts: MidgeOptions,
    num_keys: usize,
) {
    // Keep setup in one memtable until the explicit fixture flush below.
    opts.memtable_size = opts.memtable_size.max(SST_FIXTURE_MEMTABLE_SIZE_BYTES);

    ctx.parameter("logical_batch_size", SST_RANGE_SEEK_BATCH_SIZE);
    ctx.parameter("logical_unit", "sst_range_seek");
    ctx.parameter("operation_surface", "sst_range_seek_first_row");
    ctx.parameter("begin_tx_included", "false");
    ctx.parameter("rotating_key_count", num_keys);
    ctx.parameter("fixture_memtable_size_bytes", opts.memtable_size);
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

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
    let read_path_before = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let mut validation_failures = 0_u64;

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
            let expected_value =
                vec![u8::try_from(start_index % 251).expect("value byte fits"); VALUE_SIZE];
            match it.next() {
                Some(Ok((key, value)))
                    if key.as_ref() == start.as_slice()
                        && value.as_ref() == expected_value.as_slice() => {}
                _ => validation_failures += 1,
            }
        }
    });

    let read_path_after = engine.read_path_diagnostics_snapshot_for_benchmarks();
    assert_eq!(
        validation_failures, 0,
        "measured SST range reads must validate"
    );
    assert!(
        read_path_after.candidate_sst_files_checked > read_path_before.candidate_sst_files_checked
            && read_path_after.candidate_blocks_checked > read_path_before.candidate_blocks_checked,
        "SST range row must exercise candidate SST and block work"
    );

    // Engine shutdown waits for active transaction guards. Release the
    // read snapshot before dropping the engine so this benchmark can finish.
    drop(tx);
    drop(engine);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_sst_point_seek_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_sst_point_seek_case(ctx, "tier3_sst_point_seek_local", opts, 5_000);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_sst_point_seek_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_sst_point_seek_case(ctx, "tier3_sst_point_seek_cloud", opts, 5_000);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_sst_range_seek_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_sst_range_seek_case(ctx, "tier3_sst_range_seek_local", opts, 10_000);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_sst_range_seek_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_sst_range_seek_case(ctx, "tier3_sst_range_seek_cloud", opts, 10_000);
}

stress_main!();
