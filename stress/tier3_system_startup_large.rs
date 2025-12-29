//! Tier 3 — Startup large manifest scenarios (stress harness)
//!
//! This file intentionally avoids Criterion.
//! Each scenario is a **single-shot** stress test with an explicit name.

use cntryl_stress::{stress_test, StressContext};

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    create_seed_dir, precompute_kv, reopen_engine_at_path, run_single_shot_open_from_seed,
    setup_engine_at_path, BenchEngineConfig, BenchStorageMode, BYTES_PER_OP, VALUE_SIZE,
};

fn build_seed_many_ssts(
    seed_prefix: &str,
    cfg: &BenchEngineConfig,
    num_keys: usize,
    flush_every: usize,
) -> std::path::PathBuf {
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    create_seed_dir(seed_prefix, |p| {
        let engine = setup_engine_at_path(p, cfg);
        let cf = engine.default_column_family();

        for i in 0..num_keys {
            engine.put(cf, &keys[i], &vals[i]).expect("put failed");
            if flush_every > 0 && i % flush_every == (flush_every - 1) {
                engine.flush().expect("flush failed");
            }
        }
        engine.flush().expect("final flush failed");
        drop(engine);
    })
}

fn run_startup_large_manifest_case(mode: BenchStorageMode, num_keys: usize, flush_every: usize) {
    let mut ctx = StressContext::new("tier3_startup_large_manifest");
    ctx.set_elements(num_keys as u64);
    ctx.set_bytes((num_keys as u64) * BYTES_PER_OP);

    // Small memtable => more SSTs.
    let cfg = BenchEngineConfig {
        storage_mode: mode,
        enable_compaction: false,
        memtable_size: Some(64 * 1024),
        ..Default::default()
    };

    let seed_prefix = format!(
        "tier3_startup_large_manifest_seed_{}_flush{}_{:?}",
        num_keys, flush_every, mode
    );
    let seed_path = build_seed_many_ssts(&seed_prefix, &cfg, num_keys, flush_every);

    let d = run_single_shot_open_from_seed(&seed_path, &cfg, |p, cfg| {
        // Measure open time only.
        reopen_engine_at_path(p, cfg)
    });

    ctx.record(d);
    ctx.finish();
}

#[stress_test]
fn tier3_startup_large_manifest_local_disk_5k_flush100() {
    run_startup_large_manifest_case(BenchStorageMode::LocalDisk, 5_000, 100);
}
