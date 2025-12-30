//! Tier 3 — Flush / (future) compaction system guardrails (cntryl-stress)
//!
//! Subsystem: flush orchestration (engine/runtime boundary).
//! Storage semantics: tests run for durable backends (local/cloud) explicitly.
//!
//! Current implementation note:
//! - `MidgeEngine::compact_all()` is currently a stub that calls `flush()`.
//!   These tests therefore measure flush behavior today.
//! - Memory mode also does not implement compaction (no on-disk LSM levels).
//!
//! What is *not* tested here:
//! - Steady-state / long-running workload behavior (Tier-4)
//! - Cache warmup effects, sampling, or throughput tuning
//! - Durability / ack policy correctness (see Tier-3 durability suite)
//!
//! Each scenario is a single-shot system question with setup strictly outside
//! the measured section. No Criterion.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{MidgeEngine, MidgeOptions};

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const DEFAULT_VALUE_SIZE: usize = 100;

// Tier-3 should test system shape, not volume.
// Expected: typically <500ms on local disk.
const DEFAULT_COMPACTION_KEYS: usize = 1_000;

fn precompute_kv(num_keys: usize, value_size: usize) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    cntryl_midge::testkit::stress::precompute_kv16_u64_be(num_keys, value_size, u8::MAX)
}

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn run_flush_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize, value_size: usize) {
    let (keys, values) = precompute_kv(num_keys, value_size);

    ctx.set_elements(num_keys as u64);
    ctx.set_bytes((num_keys * (KEY_SIZE + value_size)) as u64);

    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured)
    for (k, v) in keys.iter().zip(values.iter()) {
        engine.put(cf, &k[..], v).expect("setup put");
    }

    // Measure exactly one flush
    ctx.measure_ref(&engine, |e| e.flush().expect("flush failed"));

    drop(engine);
}

fn run_compact_all_many_sst_case(
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

    // Setup (not measured): create multiple flush outputs.
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
            engine
                .put(cf, &keys[idx][..], values[idx].as_slice())
                .expect("setup put");
        }
        engine.flush().expect("setup flush");
    }

    // Measure exactly one `compact_all()` call (currently a flush proxy).
    ctx.measure_ref(&engine, |e| e.compact_all().expect("compact_all failed"));

    drop(engine);
}

fn run_many_overlapping_l0_files_case(
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
            engine
                .put(cf, &k[..], base_values[idx].as_slice())
                .expect("setup put");
        }
        engine.flush().expect("setup flush");
    }

    ctx.measure_ref(&engine, |e| e.compact_all().expect("compact_all failed"));

    drop(engine);
}

fn run_overlap_pressure_compact_case(
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

    // Setup: create many overlapping flush outputs by repeatedly writing the same keyspace.
    for batch in 0..num_batches {
        for idx in 0..num_keys_per_batch {
            // Reuse the same key range each batch to maximize overlap.
            let k = base_keys[idx];
            engine
                .put(cf, &k[..], base_values[batch * num_keys_per_batch + idx].as_slice())
                .expect("setup put");
        }
        engine.flush().expect("setup flush");
    }

    // Measure `compact_all()` under overlap pressure (currently a flush proxy).
    ctx.measure_ref(&engine, |e| e.compact_all().expect("compact_all failed"));

    drop(engine);
}

// ---------------------------------------------------------------------------
// Stress tests (explicit, one datapoint per test)
// ---------------------------------------------------------------------------

#[stress_test]
fn tier3_compaction_flush_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_flush_case(ctx, opts, DEFAULT_COMPACTION_KEYS, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier3_compaction_flush_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_flush_case(ctx, opts, DEFAULT_COMPACTION_KEYS, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier3_compaction_compact_all_many_sst_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_compact_all_many_sst_case(ctx, opts, DEFAULT_COMPACTION_KEYS, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier3_compaction_compact_all_many_sst_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_compact_all_many_sst_case(ctx, opts, DEFAULT_COMPACTION_KEYS, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier3_compaction_many_overlapping_l0_files_local(ctx: &mut StressContext) {
    // Pathological overlap patterns are intentionally expensive; gate behind feature.
    // Enable via: `cargo stress -v --features tier3-heavy`
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_many_overlapping_l0_files_case(ctx, opts, 250, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier3_compaction_many_overlapping_l0_files_cloud(ctx: &mut StressContext) {
    // Enable via: `cargo stress -v --features tier3-heavy`
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_many_overlapping_l0_files_case(ctx, opts, 250, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier3_compaction_overlap_pressure_local(ctx: &mut StressContext) {
    // Enable via: `cargo stress -v --features tier3-heavy`
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_overlap_pressure_compact_case(ctx, opts, 250, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier3_compaction_overlap_pressure_cloud(ctx: &mut StressContext) {
    // Enable via: `cargo stress -v --features tier3-heavy`
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_overlap_pressure_compact_case(ctx, opts, 250, 4, DEFAULT_VALUE_SIZE);
}

stress_main!();
