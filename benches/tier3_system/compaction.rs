//! Tier 3 — System Benchmarks: Compaction
//!
//! **Target Runtime:** 1-5 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers full compaction workflows (flush, merge, write amplification)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::criterion_config;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::hint::black_box;

/// Pre-generate keys and values to avoid format! overhead during benchmark setup
fn generate_kv(num_keys: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        // Fixed-size keys using direct byte manipulation (no format! allocations)
        let mut key = vec![0u8; 14];
        key[..4].copy_from_slice(b"key_");
        // Write i as 10-digit decimal directly
        let mut n = i;
        for j in (4..14).rev() {
            key[j] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        keys.push(key);

        // Fixed-size value with padding
        let mut value = vec![0u8; 30];
        value[..6].copy_from_slice(b"value_");
        let mut n = i;
        for j in (6..16).rev() {
            value[j] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        value[16..].copy_from_slice(b"_data_padding_");
        values.push(value);
    }

    (keys, values)
}

fn setup_db_with_data(name: &str, keys: &[Vec<u8>], values: &[Vec<u8>]) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_system_compaction_{}", name));
    let _ = std::fs::remove_dir_all(&path);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();

    for (key, value) in keys.iter().zip(values.iter()) {
        engine.put(&cf, key, value).unwrap();
    }

    engine
}

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush");
    group.sampling_mode(SamplingMode::Flat);

    for &num_keys in &[10_000, 50_000] {
        // Pre-generate data outside the benchmark loop
        let (keys, values) = generate_kv(num_keys);

        group.throughput(Throughput::Elements(num_keys as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            &num_keys,
            |b, &size| {
                b.iter_batched(
                    || setup_db_with_data(&format!("flush_{}", size), &keys, &values),
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
    group.sampling_mode(SamplingMode::Flat);

    for &num_keys in &[50_000, 100_000] {
        // Pre-generate data outside the benchmark loop
        let (keys, values) = generate_kv(num_keys);

        group.throughput(Throughput::Elements(num_keys as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            &num_keys,
            |b, &size| {
                b.iter_batched(
                    || {
                        let engine = setup_db_with_data(&format!("compact_{}", size), &keys, &values);
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
    name = tier3_system_compaction;
    config = criterion_config();
    targets = bench_flush, bench_compact_all
}
criterion_main!(tier3_system_compaction);
