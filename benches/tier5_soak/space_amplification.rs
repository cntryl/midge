//! Tier 5 — Soak/Space Amplification Bench
//!
//! **Target Runtime:** Long-running soak tests (10+ minutes)
//! **Run Frequency:** Manual / extended CI
//!
//! Measures space amplification: (total disk space) / (live data size)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use criterion_helper::criterion_config;
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark space amplification under update-heavy workload
/// Space amplification = total_disk_space / logical_data_size
/// Ideal: ~1.0x, Acceptable: <3.0x, Poor: >5.0x
fn bench_space_amplification(c: &mut Criterion) {
    let mut group = c.benchmark_group("soak_space_amplification");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);

    group.bench_function("update_heavy_15k_ops", |b| {
        b.iter(|| {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("space_amp");
            
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
                memtable_size: 1024 * 1024,
                enable_compaction: true,
                ..Default::default()
            };
            
            let engine = MidgeEngine::open(opts).unwrap();
            let cf = engine.default_column_family();
            
            // Phase 1: Initial dataset (5k keys, 256 bytes each = ~1.25MB logical)
            let value_size = 256;
            for i in 0..5_000 {
                let key = format!("key_{:010}", i);
                let val = vec![b'a'; value_size];
                engine.put(&cf, key.as_bytes(), &val).unwrap();
            }
            engine.flush().unwrap();
            
            // Measure initial disk usage
            let _initial_size = estimate_disk_usage(&path);
            
            // Phase 2: Repeated updates (creates obsolete versions)
            // Update same 5k keys multiple times
            for round in 0..2 {
                for i in 0..5_000 {
                    let key = format!("key_{:010}", i);
                    let val = vec![b'b' + round as u8; value_size];
                    engine.put(&cf, key.as_bytes(), &val).unwrap();
                }
                engine.flush().unwrap();
            }
            
            // Measure space before compaction
            let before_compact = estimate_disk_usage(&path);
            
            // Trigger compaction to remove obsolete versions
            let _ = engine.compact_level(&cf, 0);
            engine.flush().unwrap();
            
            // Measure space after compaction
            let after_compact = estimate_disk_usage(&path);
            
            // Space amplification = actual_size / logical_size
            let logical_size = 5_000 * value_size; // 1.25MB
            let amplification_before = before_compact as f64 / logical_size as f64;
            let amplification_after = after_compact as f64 / logical_size as f64;
            
            black_box((amplification_before, amplification_after));
        })
    });

    group.finish();
}

/// Estimate total disk usage of database directory
fn estimate_disk_usage(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    total += metadata.len();
                }
            }
        }
    }
    total
}

criterion_group! {
    name = space_amplification_group;
    config = criterion_config();
    targets = bench_space_amplification
}
criterion_main!(space_amplification_group);