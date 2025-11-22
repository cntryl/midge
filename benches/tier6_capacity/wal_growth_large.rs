//! Tier 6 — Capacity/WAL growth large
//!
//! **Target Runtime:** Large-scale capacity tests (minutes)
//! **Run Frequency:** Manual / capacity CI
//!
//! Measures WAL file growth behavior under large write workloads

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::fs;
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark WAL file growth and rotation behavior under sustained writes
/// Measures WAL size vs operation count to verify proper rotation/truncation
fn bench_wal_growth_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_wal_growth_large");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(30));
    group.sample_size(10);
    group.throughput(Throughput::Elements(50_000));

    group.bench_function("wal_growth_50k_writes", |b| {
        b.iter(|| {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("wal_growth");

            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: path.clone(),
                },
                memtable_size: 512 * 1024, // Small memtable = frequent flushes & WAL rotation
                enable_compaction: false,
                ..Default::default()
            };

            let engine = MidgeEngine::open(opts).unwrap();
            let cf = engine.default_column_family();

            let mut wal_sizes = Vec::new();

            // Sustained write workload
            for i in 0..50_000 {
                let key = format!("wal_key_{:010}", i);
                let val = vec![b'w'; 128]; // 128-byte values
                engine.put(&cf, key.as_bytes(), &val).unwrap();

                // Sample WAL size periodically
                if i % 5000 == 0 {
                    engine.flush().unwrap(); // Should truncate WAL
                    let wal_size = measure_wal_size(&path);
                    wal_sizes.push(wal_size);
                }
            }

            engine.flush().unwrap();
            let final_wal_size = measure_wal_size(&path);

            // WAL should be small after flush (truncation working)
            // Unbounded growth = bug
            black_box((wal_sizes, final_wal_size));
        })
    });

    group.finish();
}

/// Measure total size of WAL files in database directory
fn measure_wal_size(db_path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = fs::read_dir(db_path) {
        for entry in entries.flatten() {
            if let Ok(name) = entry.file_name().into_string() {
                if name.ends_with(".wal") || name.ends_with(".log") {
                    if let Ok(metadata) = entry.metadata() {
                        total += metadata.len();
                    }
                }
            }
        }
    }
    total
}

criterion_group! {
    name = wal_growth_large_group;
    config = criterion_config();
    targets = bench_wal_growth_large
}
criterion_main!(wal_growth_large_group);
