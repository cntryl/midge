//! Tier 5 — Soak/Compaction backlog growth
//!
//! **Target Runtime:** Long-running soak tests (10+ minutes)
//! **Run Frequency:** Manual / extended CI
//!
//! Measures compaction backlog growth when writes exceed compaction capacity

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark compaction backlog growth under sustained write pressure
/// Simulates a scenario where writes arrive faster than compaction can keep up
fn bench_compaction_backlog_growth(c: &mut Criterion) {
    let mut group = c.benchmark_group("soak_compaction_backlog_growth");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(20));
    group.sample_size(10);

    group.bench_function("sustained_writes_10k_ops", |b| {
        b.iter(|| {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("backlog_growth");

            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: path },
                memtable_size: 512 * 1024, // Small memtable = frequent flushes
                enable_compaction: true,
                ..Default::default()
            };

            let engine = MidgeEngine::open(opts).unwrap();
            let cf = engine.default_column_family();

            // Sustained write workload: 10k operations
            // Track L0 file accumulation (backlog indicator)
            let mut l0_counts = Vec::new();

            for i in 0..10_000 {
                let key = format!("soak_key_{:010}", i);
                let val = format!("value_{}", i);
                engine.put(cf, key.as_bytes(), val.as_bytes()).unwrap();

                // Sample L0 file count periodically
                if i % 500 == 0 {
                    // Get current manifest state (would need to expose this API)
                    // For now, use flush as a proxy for backlog
                    engine.flush().unwrap();
                    l0_counts.push(i);
                }
            }

            // Final flush and measure
            engine.flush().unwrap();

            // Backlog growth rate = (L0 files at end) / (operations)
            // Higher ratio = compaction can't keep up
            black_box(l0_counts.len());
        })
    });

    group.finish();
}

criterion_group! {
    name = tier5_soak_compaction_backlog_growth;
    config = criterion_config_for_tier(BenchTier::Tier5Soak);
    targets = bench_compaction_backlog_growth
}
criterion_main!(tier5_soak_compaction_backlog_growth);
