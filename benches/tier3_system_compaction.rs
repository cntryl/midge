//! Tier 3 — System Benchmarks: Compaction
//!
//! **Target Runtime:** <3 minutes total
//! **Run Frequency:** Nightly / release builds
//!
//! Covers full compaction workflows (flush, merge, write amplification).
//!
//! ## Benchmarks
//!
//! - `system_flush`: Measures memtable-to-SST flush latency
//! - `system_compact`: Measures full compaction (compact_all) latency
//! - `system_flush_throughput`: Measures flush bytes/sec with varying value sizes
//! - `system_incremental_compact`: Multiple L0 files compaction
//! - `system_flush_concurrent`: Measures flush latency under concurrent writes
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since compaction requires persistence
//! - Optimized for benchmark accuracy: precomputed KV using Bytes, no allocations in hot loop
//! - All data precomputed outside measurement loop
//! - Returns engine from closure to prevent Drop during timing

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    setup_engine, setup_engine_at_path, unique_bench_path, BenchEngineConfig, BenchStorageMode,
    DURABLE_STORAGE_MODES,
};

use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::sync::Arc;

/// Key size in bytes (fixed for consistent measurements)
const KEY_SIZE: usize = 16;
/// Default value size in bytes
const DEFAULT_VALUE_SIZE: usize = 100;

/// Benchmark name constants to avoid repeated string allocations
const BENCH_FLUSH: &str = "flush";
const BENCH_COMPACT: &str = "compact";
const BENCH_FLUSH_TP: &str = "flush_tp";
const BENCH_INCR_COMPACT: &str = "incr_compact";
const BENCH_FLUSH_CONCURRENT: &str = "flush_concurrent";

/// Pre-generate immutable keys and values with configurable value size.
/// Keys: "k" + 15-digit zero-padded number (16 bytes total)
/// Values: index in first 8 bytes + pattern fill
/// Returns Bytes for zero-copy sharing across iterations.
#[inline]
fn generate_kv(num_keys: usize, value_size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        // Fixed-size keys: "k" + 15-digit zero-padded number
        let mut key = vec![b'k'; KEY_SIZE];
        let mut n = i;
        for j in (1..KEY_SIZE).rev() {
            key[j] = b'0' + (n % 10) as u8;
            n /= 10;
        }
        keys.push(Bytes::from(key));

        // Fixed-size value with index + pattern
        let mut value = vec![0u8; value_size];
        if value_size >= 8 {
            value[..8].copy_from_slice(&(i as u64).to_be_bytes());
        }
        let pattern = (i % 256) as u8;
        for byte in value.iter_mut().skip(8) {
            *byte = pattern;
        }
        values.push(Bytes::from(value));
    }

    (keys, values)
}

/// Precomputed key-value data for benchmarks.
/// Stored as immutable Bytes for zero-copy sharing.
struct PrecomputedKV {
    keys: Vec<Bytes>,
    values: Vec<Bytes>,
}

impl PrecomputedKV {
    fn new(num_keys: usize, value_size: usize) -> Self {
        let (keys, values) = generate_kv(num_keys, value_size);
        Self { keys, values }
    }

    /// Generate batched KV data with overlapping key ranges for realistic compaction.
    /// Uses secondary byte variation to create overlap between batches.
    fn new_batched(num_keys_per_batch: usize, num_batches: usize, value_size: usize) -> Self {
        let total_keys = num_keys_per_batch * num_batches;
        let mut keys = Vec::with_capacity(total_keys);
        let mut values = Vec::with_capacity(total_keys);

        for batch in 0..num_batches {
            let (batch_keys, batch_values) = generate_kv(num_keys_per_batch, value_size);
            for (k, v) in batch_keys.into_iter().zip(batch_values.into_iter()) {
                // Modify secondary byte to create overlapping ranges across batches
                // This produces more realistic compaction pressure than disjoint prefixes
                let mut key_bytes = k.to_vec();
                key_bytes[1] = b'0' + (batch % 10) as u8;
                keys.push(Bytes::from(key_bytes));
                values.push(v);
            }
        }

        Self { keys, values }
    }

    fn len(&self) -> usize {
        self.keys.len()
    }
}

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush");
    group.sampling_mode(SamplingMode::Flat);

    // Reduced key counts to keep each iteration under ~2s
    for &num_keys in &[5_000, 20_000] {
        // Precompute KV once per key count, reused across all modes
        let kv = PrecomputedKV::new(num_keys, DEFAULT_VALUE_SIZE);
        let total_bytes: u64 = (num_keys as u64) * (KEY_SIZE as u64 + DEFAULT_VALUE_SIZE as u64);

        group.throughput(Throughput::Bytes(total_bytes));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}keys/{}", num_keys, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new(BENCH_FLUSH, &bench_name),
                &(num_keys, mode),
                |b, &(_size, mode)| {
                    let kv_ref = &kv;

                    b.iter_batched(
                        || {
                            use std::time::Instant;

                            // Use a larger batch config for benches to reduce fsync frequency during setup
                            let mut cfg = BenchEngineConfig {
                                storage_mode: mode,
                                enable_compaction: false,
                                ..Default::default()
                            };
                            // Increase batch bytes threshold for bench (1MB) and keep short delay
                            cfg = cfg.with_wal_batch_config(cntryl_midge::wal::policy::BatchConfig { max_delay_ms: 200, max_bytes: 1024 * 1024 });
                            let engine = setup_engine(BENCH_FLUSH, &cfg);
                            let _cf = engine.default_column_family();

                            // Use WriteBatch to submit all puts in a single round-trip (much faster)
                            use cntryl_midge::engine::api::WriteBatch;

                            let mut batch = WriteBatch::new();
                            for (k, v) in kv_ref.keys.iter().zip(kv_ref.values.iter()) {
                                batch.put(k.clone(), v.clone());
                            }

                            let start = Instant::now();
                            engine.write_batch(&batch).expect("write_batch failed");
                            let total = start.elapsed();
                            eprintln!(
                                "[bench setup] mode={} write_batch of {} ops took {:?} (avg {:?}/op)",
                                mode.as_str(),
                                kv_ref.len(),
                                total,
                                total / (kv_ref.len() as u32)
                            );

                            // Optionally: a small sanity check of chunked writes to see per-chunk cost
                            // (this uses pre-sized chunks above; we've measured them below already)

                            // Submit write_batch in larger chunks (1k ops) to avoid per-op fsync overhead
                            if kv_ref.len() >= 1000 {
                                let chunk = 1000usize;
                                let mut i = 0usize;
                                while i < kv_ref.len() {
                                    let end = (i + chunk).min(kv_ref.len());
                                    let mut bchunk = WriteBatch::new();
                                    for (k, v) in kv_ref.keys[i..end].iter().zip(&kv_ref.values[i..end]) {
                                        bchunk.put(k.clone(), v.clone());
                                    }
                                    let start_chunk = Instant::now();
                                    engine.write_batch(&bchunk).expect("write_batch failed");
                                    let now = Instant::now();
                                    eprintln!(
                                        "[bench setup] mode={} chunked write_batch {}..{} took {:?}",
                                        mode.as_str(),
                                        i + 1,
                                        end,
                                        now.duration_since(start_chunk)
                                    );
                                    i = end;
                                }
                            }

                            engine
                        },
                        |engine| {
                            engine.flush().expect("flush failed");
                            engine
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_compact_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_compact");
    group.sampling_mode(SamplingMode::Flat);

    // Reduced key counts for faster runs; LocalDisk-only for larger
    for &num_keys in &[10_000, 15_000] {
        // Precompute KV once per key count, reused across all modes
        let kv = PrecomputedKV::new(num_keys, DEFAULT_VALUE_SIZE);
        let total_bytes: u64 = (num_keys as u64) * (KEY_SIZE as u64 + DEFAULT_VALUE_SIZE as u64);

        group.throughput(Throughput::Bytes(total_bytes));

        for mode in DURABLE_STORAGE_MODES {
            // LocalDisk only for larger workload to avoid cloud overhead
            if num_keys > 10_000 && !matches!(mode, BenchStorageMode::LocalDisk) {
                continue;
            }
            let bench_name = format!("{}keys/{}", num_keys, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new(BENCH_COMPACT, &bench_name),
                &(num_keys, mode),
                |b, &(_size, mode)| {
                    let kv_ref = &kv;

                    b.iter_batched(
                        || {
                            let engine = setup_engine(
                                BENCH_COMPACT,
                                &BenchEngineConfig {
                                    storage_mode: mode,
                                    enable_compaction: true,
                                    ..Default::default()
                                },
                            );
                            let cf = engine.default_column_family();
                            for (k, v) in kv_ref.keys.iter().zip(kv_ref.values.iter()) {
                                engine.put(cf, k, v).expect("put failed");
                            }
                            engine.flush().expect("flush failed");
                            engine
                        },
                        |engine| {
                            engine.compact_all().expect("compact_all failed");
                            engine
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_flush_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush_throughput");
    group.sampling_mode(SamplingMode::Flat);

    // Reduced to keep iterations fast while still measuring throughput accurately
    let num_keys = 5_000;

    for &value_size in &[64, 256, 1024, 4096] {
        // Precompute KV once per value size, reused across all modes
        let kv = PrecomputedKV::new(num_keys, value_size);
        let total_bytes: u64 = (num_keys as u64) * (KEY_SIZE as u64 + value_size as u64);

        group.throughput(Throughput::Bytes(total_bytes));

        for mode in DURABLE_STORAGE_MODES {
            let bench_name = format!("{}B_values/{}", value_size, mode.as_str());
            group.bench_with_input(
                BenchmarkId::new(BENCH_FLUSH_TP, &bench_name),
                &(value_size, mode),
                |b, &(_vs, mode)| {
                    let kv_ref = &kv;

                    b.iter_batched(
                        || {
                            let engine = setup_engine(
                                BENCH_FLUSH_TP,
                                &BenchEngineConfig {
                                    storage_mode: mode,
                                    enable_compaction: false,
                                    ..Default::default()
                                },
                            );
                            let cf = engine.default_column_family();
                            for (k, v) in kv_ref.keys.iter().zip(kv_ref.values.iter()) {
                                engine.put(cf, k, v).expect("put failed");
                            }
                            engine
                        },
                        |engine| {
                            engine.flush().expect("flush failed");
                            engine
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_incremental_compact(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_incremental_compact");
    group.sampling_mode(SamplingMode::Flat);

    // Reduced to keep iterations under ~2s while still testing multi-batch compaction
    let num_keys_per_batch = 2_000;
    let num_batches = 4;

    // Generate batched KV with overlapping key ranges for realistic compaction
    let kv = PrecomputedKV::new_batched(num_keys_per_batch, num_batches, DEFAULT_VALUE_SIZE);
    let total_bytes: u64 = (kv.len() as u64) * (KEY_SIZE as u64 + DEFAULT_VALUE_SIZE as u64);

    group.throughput(Throughput::Bytes(total_bytes));

    for mode in DURABLE_STORAGE_MODES {
        let bench_name = format!(
            "{}batches_x_{}keys/{}",
            num_batches,
            num_keys_per_batch,
            mode.as_str()
        );
        group.bench_with_input(
            BenchmarkId::new(BENCH_INCR_COMPACT, &bench_name),
            &mode,
            |b, &mode| {
                let kv_ref = &kv;

                b.iter_batched(
                    || {
                        let path = unique_bench_path(BENCH_INCR_COMPACT);
                        let _ = std::fs::remove_dir_all(&path);

                        let config = BenchEngineConfig {
                            storage_mode: mode,
                            enable_compaction: true,
                            ..Default::default()
                        };
                        let engine = setup_engine_at_path(&path, &config);
                        let cf = engine.default_column_family();

                        // Write and flush in batches to create multiple L0 files
                        for batch_idx in 0..num_batches {
                            let start = batch_idx * num_keys_per_batch;
                            let end = start + num_keys_per_batch;
                            for idx in start..end {
                                engine
                                    .put(cf, &kv_ref.keys[idx], &kv_ref.values[idx])
                                    .expect("put failed");
                            }
                            engine.flush().expect("flush failed");
                        }

                        engine
                    },
                    |engine| {
                        engine.compact_all().expect("compact_all failed");
                        engine
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark flush under concurrent write load.
/// Measures flush latency when other threads are actively writing.
fn bench_flush_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_flush_concurrent");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 5_000;
    let concurrent_writers = 2;
    let writes_per_writer = 500;

    // Precompute data
    let kv = PrecomputedKV::new(num_keys, DEFAULT_VALUE_SIZE);
    let writer_keys: Vec<Vec<Bytes>> = (0..concurrent_writers)
        .map(|w| {
            let base = num_keys + w * writes_per_writer;
            (base..(base + writes_per_writer))
                .map(|i| {
                    let mut key = vec![b'w'; KEY_SIZE];
                    let mut n = i;
                    for j in (1..KEY_SIZE).rev() {
                        key[j] = b'0' + (n % 10) as u8;
                        n /= 10;
                    }
                    Bytes::from(key)
                })
                .collect()
        })
        .collect();
    let writer_value = Bytes::from(vec![b'v'; DEFAULT_VALUE_SIZE]);
    let total_bytes: u64 = (num_keys as u64) * (KEY_SIZE as u64 + DEFAULT_VALUE_SIZE as u64);

    group.throughput(Throughput::Bytes(total_bytes));

    // Only test disk mode for concurrent scenarios to keep runtime reasonable
    let mode = BenchStorageMode::LocalDisk;
    group.bench_with_input(
        BenchmarkId::new(BENCH_FLUSH_CONCURRENT, "concurrent_2writers"),
        &mode,
        |b, &mode| {
            let kv_ref = &kv;
            let writer_keys_ref = &writer_keys;
            let writer_value_ref = &writer_value;

            b.iter_batched(
                || {
                    let engine = Arc::new(setup_engine(
                        BENCH_FLUSH_CONCURRENT,
                        &BenchEngineConfig {
                            storage_mode: mode,
                            enable_compaction: false,
                            ..Default::default()
                        },
                    ));
                    let cf = engine.default_column_family();
                    for (k, v) in kv_ref.keys.iter().zip(kv_ref.values.iter()) {
                        engine.put(cf, k, v).expect("put failed");
                    }
                    engine
                },
                |engine| {
                    let cf = engine.default_column_family();
                    // Spawn concurrent writers while flushing
                    std::thread::scope(|s| {
                        // Writer threads
                        for w_keys in writer_keys_ref {
                            let engine_ref = &engine;
                            let cf_ref = &cf;
                            let val = writer_value_ref;
                            s.spawn(move || {
                                for k in w_keys {
                                    let _ = engine_ref.put(cf_ref, k, val);
                                }
                            });
                        }
                        // Flush in main thread (timed operation)
                        engine.flush().expect("flush failed");
                        black_box(());
                    });
                    engine
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
    targets = bench_flush, bench_compact_all, bench_flush_throughput, bench_incremental_compact, bench_flush_concurrent
}
criterion_main!(tier3_system_compaction);
