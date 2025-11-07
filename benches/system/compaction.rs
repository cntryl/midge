//! Tier 3 — System Benchmarks: Compaction
//!
//! **Target Runtime:** 1-5 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers full compaction workflows (flush, merge, write amplification)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;

use midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::hint::black_box;

fn setup_db(name: &str, num_keys: usize) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_system_compaction_{}", name));
    let _ = std::fs::remove_dir_all(&path);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();

    for i in 0..num_keys {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_data_padding", i);
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
    }

    engine
}

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush");

    for &num_keys in &[10_000, 50_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            &num_keys,
            |b, &size| {
                b.iter_batched(
                    || setup_db(&format!("flush_{}", size), size),
                    |engine| {
                        engine.flush().unwrap();
                        black_box(());
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_compact_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_compact");

    for &num_keys in &[50_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            &num_keys,
            |b, &size| {
                b.iter_batched(
                    || {
                        let engine = setup_db(&format!("compact_{}", size), size);
                        engine.flush().unwrap();
                        engine
                    },
                    |engine| {
                        engine.compact_all().unwrap();
                        black_box(());
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = system_compaction;
    config = criterion_config();
    targets = bench_flush, bench_compact_all
}
criterion_main!(system_compaction);
