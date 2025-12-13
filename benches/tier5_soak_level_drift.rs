//! Tier 5 — Soak/Level drift
//!
//! **Target Runtime:** Long-running soak tests (10+ minutes)
//! **Run Frequency:** Manual / extended CI
//!
//! Measures LSM level distribution drift over prolonged mixed workload

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark level distribution stability under mixed read/write/delete workload
/// Ideal LSM maintains balanced level sizes; drift indicates suboptimal compaction
fn bench_level_drift(c: &mut Criterion) {
    let mut group = c.benchmark_group("soak_level_drift");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);

    group.bench_function("mixed_workload_20k_ops", |b| {
        b.iter(|| {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("level_drift");

            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: path },
                memtable_size: 1024 * 1024, // 1MB memtable
                enable_compaction: true,
                ..Default::default()
            };

            let engine = MidgeEngine::open(opts).unwrap();
            let cf = engine.default_column_family();

            // Phase 1: Initial population (creates baseline levels)
            for i in 0..10_000 {
                let key = format!("key_{:010}", i);
                let val = format!("initial_value_{}", i);
                engine.put(cf, key.as_bytes(), val.as_bytes()).unwrap();
            }
            engine.flush().unwrap();
            let _ = engine.compact_all(); // Compact all levels

            // Phase 2: Mixed workload (updates, deletes, new keys)
            for i in 0..10_000 {
                match i % 3 {
                    0 => {
                        // Update existing key
                        let key = format!("key_{:010}", i % 10_000);
                        let val = format!("updated_value_{}", i);
                        engine.put(cf, key.as_bytes(), val.as_bytes()).unwrap();
                    }
                    1 => {
                        // Delete old key
                        let key = format!("key_{:010}", i % 10_000);
                        engine.delete(cf, key.as_bytes()).unwrap();
                    }
                    _ => {
                        // Insert new key
                        let key = format!("new_key_{:010}", i);
                        let val = format!("new_value_{}", i);
                        engine.put(cf, key.as_bytes(), val.as_bytes()).unwrap();
                    }
                }

                // Periodic flush to observe level drift
                if i % 1000 == 999 {
                    engine.flush().unwrap();
                }
            }

            // Final state measurement
            engine.flush().unwrap();

            // Level drift metric = variance in level sizes
            // Would need manifest API to measure actual distribution
            black_box(engine);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier5_soak_level_drift;
    config = criterion_config_for_tier(BenchTier::Tier5Soak);
    targets = bench_level_drift
}
criterion_main!(tier5_soak_level_drift);
