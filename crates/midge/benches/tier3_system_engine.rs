//! Tier 3 — Engine primitives
//!
//! Measures: cost of repeated engine read primitives
//! NOT: bulk operations, write throughput, or volume scaling
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

use stress_config::MidgeOptions;

const PUT_BATCH_SIZE: usize = 64;
const GET_BATCH_SIZE: usize = 1;
const GET_KEY_COUNT: usize = 4096;
const ENGINE_GET_MEMTABLE_SIZE_BYTES: usize = 2 * 1024 * 1024;
const ENGINE_GET_SAMPLE_COUNT: usize = 12;

fn run_single_get_case(ctx: &mut StressContext, scenario: &'static str, mut opts: MidgeOptions) {
    // Keep the setup batch in one active memtable so the explicit flush below
    // produces the single-SST read fixture this benchmark intends to measure.
    // The generic local benchmark profile uses a deliberately tiny memtable,
    // which can otherwise stall before setup reaches the explicit flush.
    opts.memtable_size = opts.memtable_size.max(ENGINE_GET_MEMTABLE_SIZE_BYTES);

    ctx.parameter("logical_batch_size", GET_BATCH_SIZE);
    ctx.parameter("logical_unit", "engine_point_read");
    ctx.parameter("operation_surface", "engine_get");
    ctx.parameter("begin_tx_included", "true");
    ctx.parameter("rotating_key_count", GET_KEY_COUNT);
    ctx.parameter("fixture_memtable_size_bytes", opts.memtable_size);

    let engine = stress_config::bench_stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Setup (not measured): write rotating read keys.
    let keys: Vec<[u8; 16]> = (0..GET_KEY_COUNT)
        .map(|index| stress_config::bench_stress::key16_u64_be(index as u64))
        .collect();
    {
        let v = vec![1u8; 128];
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

    let read_path_before = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let expected = vec![1u8; 128];
    let mut key_index = 0usize;
    let mut validation_failures = 0_u64;

    let _ = ctx
        .benchmark(scenario)
        .samples(ENGINE_GET_SAMPLE_COUNT)
        .measure_batch(GET_BATCH_SIZE as u64, || {
            for _ in 0..GET_BATCH_SIZE {
                let key = keys[key_index % keys.len()];
                key_index = key_index.wrapping_add(1);
                let tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin");
                match tx.get(&key[..]) {
                    Ok(Some(value)) if value.as_ref() == expected.as_slice() => {}
                    _ => validation_failures += 1,
                }
            }
        });

    let read_path_after = engine.read_path_diagnostics_snapshot_for_benchmarks();
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");
    ctx.parameter("local_gate_rsd_limit_pct", 5);
    assert_eq!(
        validation_failures, 0,
        "measured engine reads must validate"
    );
    assert!(
        read_path_after.read_only_begin_tx_count > read_path_before.read_only_begin_tx_count
            && read_path_after.candidate_sst_files_checked
                > read_path_before.candidate_sst_files_checked
            && read_path_after.candidate_blocks_checked > read_path_before.candidate_blocks_checked,
        "engine point-read row must exercise read-only transactions, candidate SSTs, and blocks"
    );

    drop(engine);
}

// MOVED TO TIER 4: batch throughput testing belongs in tier4_system_engine.rs
// This was a Tier 3 violation: loop inside measured body violates Rule 3.

// ---------------------------------------------------------------------------
// Stress tests
// ---------------------------------------------------------------------------

#[stress(tier = 3, role = "diagnostic")]
fn tier3_engine_get_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_single_get_case(ctx, "tier3_engine_get_local", opts);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_engine_get_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_single_get_case(ctx, "tier3_engine_get_cloud", opts);
}

stress_main!();
