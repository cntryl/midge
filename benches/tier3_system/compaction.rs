//! Tier 3 — System Benchmarks: Compaction
//!
//! **Target Runtime:** 1-5 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers full compaction workflows (flush, merge, write amplification).
//!
//! ## Benchmarks
//!
//! - `system_flush`: Measures memtable-to-SST flush latency
//! - `system_compact`: Measures full compaction (compact_all) latency
//! - `system_compact_level`: Measures single-level compaction latency
//! - `system_flush_throughput`: Measures flush bytes/sec with varying value sizes

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key size in bytes (fixed for consistent measurements)
const KEY_SIZE: usize = 16;
/// Default value size in bytes
const DEFAULT_VALUE_SIZE: usize = 100;

/// Pre-generate keys and values with configurable value size.
/// Uses fixed-size keys with lexicographic ordering for deterministic behavior.
fn generate_kv(num_keys: usize, value_size: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        // Fixed-size keys: "k" + 15-digit zero-padded number for lexicographic order
        let mut key = vec![b'k'; KEY_SIZE];
        let mut n = i;
        for j in (1..KEY_SIZE).rev() {
            key[j] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        keys.push(key);

        // Fixed-size value filled with deterministic pattern
        let mut value = vec![0u8; value_size];
        // First 8 bytes: index as big-endian for verification
        if value_size >= 8 {
            value[..8].copy_from_slice(&(i as u64).to_be_bytes());
        }
        // Fill rest with repeating pattern based on index
        let pattern = (i % 256) as u8;
        for byte in value.iter_mut().skip(8) {
            *byte = pattern;
        }
        values.push(value);
    }

    (keys, values)
}

/// Generate a unique path for benchmark database to avoid cross-iteration interference
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_{}_{}_{}_{}", prefix, pid, counter, 
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ))
}

/// Wrapper to ensure cleanup on drop
struct BenchDb {
    engine: MidgeEngine,
    path: PathBuf,
}

impl BenchDb {
    fn new(prefix: &str, keys: &[Vec<u8>], values: &[Vec<u8>], enable_compaction: bool) -> Self {
        let path = unique_bench_path(prefix);
        let _ = std::fs::remove_dir_all(&path);

        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
            // Use smaller memtable to trigger more frequent flushes
            memtable_size: 4 * 1024 * 1024,
            enable_compaction,
            ..Default::default()
        };

        let engine = MidgeEngine::open(opts).expect("failed to open engine");
        let cf = engine.default_column_family();

        for (key, value) in keys.iter().zip(values.iter()) {
            engine.put(&cf, key, value).expect("failed to put");
        }

        Self { engine, path }
    }

    /// Create a DB with data already flushed to SST (for compaction benchmarks)
    fn with_flushed_data(prefix: &str, keys: &[Vec<u8>], values: &[Vec<u8>]) -> Self {
        let db = Self::new(prefix, keys, values, false);
        db.engine.flush().expect("failed to flush");
        db
    }
}

impl Drop for BenchDb {
    fn drop(&mut self) {
        // Best-effort cleanup - don't panic in drop
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush");
    group.sampling_mode(SamplingMode::Flat);

    for &num_keys in &[10_000, 50_000] {
        // Pre-generate data outside the benchmark loop
        let (keys, values) = generate_kv(num_keys, DEFAULT_VALUE_SIZE);
        let total_bytes = num_keys * (KEY_SIZE + DEFAULT_VALUE_SIZE);

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}keys", num_keys)),
            &num_keys,
            |b, &_size| {
                b.iter_batched(
                    || BenchDb::new("flush", &keys, &values, false),
                    |db| {
                        db.engine.flush().expect("flush failed");
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
        let (keys, values) = generate_kv(num_keys, DEFAULT_VALUE_SIZE);
        let total_bytes = num_keys * (KEY_SIZE + DEFAULT_VALUE_SIZE);

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}keys", num_keys)),
            &num_keys,
            |b, &_size| {
                b.iter_batched(
                    || BenchDb::with_flushed_data("compact", &keys, &values),
                    |db| {
                        db.engine.compact_all().expect("compact_all failed");
                        black_box(());
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark flush throughput with varying value sizes.
/// This helps identify if larger values are processed efficiently.
fn bench_flush_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush_throughput");
    group.sampling_mode(SamplingMode::Flat);

    // Fixed key count, varying value sizes
    let num_keys = 20_000;
    
    for &value_size in &[64, 256, 1024, 4096] {
        let (keys, values) = generate_kv(num_keys, value_size);
        let total_bytes = num_keys * (KEY_SIZE + value_size);

        group.throughput(Throughput::Bytes(total_bytes as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}B_values", value_size)),
            &value_size,
            |b, &_vs| {
                b.iter_batched(
                    || BenchDb::new("flush_tp", &keys, &values, false),
                    |db| {
                        db.engine.flush().expect("flush failed");
                        black_box(());
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark incremental compaction with multiple flushes.
/// Simulates real-world scenario where multiple L0 files need compaction.
fn bench_incremental_compact(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_incremental_compact");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys_per_batch = 10_000;
    let num_batches = 5;
    
    // Generate multiple batches of overlapping keys
    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();
    
    for batch in 0..num_batches {
        let (keys, values) = generate_kv(num_keys_per_batch, DEFAULT_VALUE_SIZE);
        // Interleave batches by adjusting key prefix
        for (mut k, v) in keys.into_iter().zip(values.into_iter()) {
            k[0] = b'a' + (batch as u8);
            all_keys.push(k);
            all_values.push(v);
        }
    }
    
    let total_keys = num_keys_per_batch * num_batches;
    let total_bytes = total_keys * (KEY_SIZE + DEFAULT_VALUE_SIZE);

    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.bench_function(
        BenchmarkId::from_parameter(format!("{}batches_x_{}keys", num_batches, num_keys_per_batch)),
        |b| {
            b.iter_batched(
                || {
                    // Create DB and write data in batches, flushing each batch
                    let path = unique_bench_path("incr_compact");
                    let _ = std::fs::remove_dir_all(&path);

                    let opts = MidgeOptions {
                        storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
                        memtable_size: 2 * 1024 * 1024, // Smaller memtable to force more flushes
                        enable_compaction: false,
                        ..Default::default()
                    };

                    let engine = MidgeEngine::open(opts).expect("failed to open");
                    let cf = engine.default_column_family();

                    // Write and flush in batches to create multiple L0 files
                    for batch_idx in 0..num_batches {
                        let start = batch_idx * num_keys_per_batch;
                        let end = start + num_keys_per_batch;
                        for idx in start..end {
                            engine.put(&cf, &all_keys[idx], &all_values[idx]).expect("put failed");
                        }
                        engine.flush().expect("flush failed");
                    }
                    
                    (engine, path)
                },
                |(engine, path)| {
                    // Benchmark compact_all which merges all L0 files
                    engine.compact_all().expect("compact_all failed");
                    let _ = std::fs::remove_dir_all(&path);
                    black_box(());
                },
                BatchSize::LargeInput,
            )
        },
    );

    group.finish();
}

criterion_group! {
    name = tier3_system_compaction;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_flush, bench_compact_all, bench_flush_throughput, bench_incremental_compact
}
criterion_main!(tier3_system_compaction);
