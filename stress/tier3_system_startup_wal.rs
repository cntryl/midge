//! Tier 3 — Startup WAL replay scenarios (stress harness)
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

fn build_seed_wal_only(seed_prefix: &str, cfg: &BenchEngineConfig, num_ops: usize) -> std::path::PathBuf {
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    create_seed_dir(seed_prefix, |p| {
        let engine = setup_engine_at_path(p, cfg);
        let cf = engine.default_column_family();
        for i in 0..num_ops {
            engine.put(cf, &keys[i], &vals[i]).expect("put failed");
        }
        // Intentionally do NOT flush: keep data only in WAL.
        drop(engine);
    })
}

fn run_startup_from_wal_case(mode: BenchStorageMode, num_ops: usize) {
    let mut ctx = StressContext::new("tier3_startup_wal_replay");
    ctx.set_elements(num_ops as u64);
    ctx.set_bytes((num_ops as u64) * BYTES_PER_OP);

    // Large memtable = no auto flush, keep data only in WAL.
    let cfg = BenchEngineConfig {
        storage_mode: mode,
        enable_compaction: false,
        memtable_size: Some(100 * 1024 * 1024),
        ..Default::default()
    };

    let seed_prefix = format!("tier3_startup_wal_seed_{}_{}", num_ops, mode.as_str());
    let seed_path = build_seed_wal_only(&seed_prefix, &cfg, num_ops);

    let d = run_single_shot_open_from_seed(&seed_path, &cfg, |p, cfg| {
        let engine = reopen_engine_at_path(p, cfg);
        let cf = engine.default_column_family();
        // Validate recovery touched at least one record.
        let (keys, _) = precompute_kv(num_ops, VALUE_SIZE);
        let _ = engine.get(cf, &keys[num_ops / 2]).expect("get failed");
        engine
    });

    ctx.record(d);
    ctx.finish();
}

#[stress_test]
fn tier3_startup_wal_replay_local_disk_50k() {
    run_startup_from_wal_case(BenchStorageMode::LocalDisk, 50_000);
}
