//! Tier 3 — Advanced Engine Feature Benchmarks
//!
//! **Target Runtime:** ~30–60 seconds
//! **Run Frequency:** Nightly CI
//!
//! Covers advanced engine features with full engine setup:
//! - TTL expiration operations
//! - Column family scaling
//! - Large value handling (64KB → 1MB)
//! - Delete-heavy workloads
//!
//! ## Design Notes
//!
//! - Returns engine from timed closure to avoid Drop during measurement
//! - Throughput measured in bytes where applicable
//! - SamplingMode::Flat for system-level stability
//! - Benchmarks run against all storage modes (Memory, LocalDisk, CloudBacked)

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    make_key, make_value_fixed, precompute_kv, setup_engine_with_mode, ALL_STORAGE_MODES,
    BYTES_PER_OP, KEY_SIZE, VALUE_SIZE,
};

use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ============================================================================
// TTL Operations
// ============================================================================

fn bench_ttl(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_advanced/ttl");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 500usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let ttl_secs = 1200u64;

    // PUT with TTL --------------------------------------------------------

    let bytes_total = (num_ops as u64) * BYTES_PER_OP;
    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("put_with_ttl", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || setup_engine_with_mode("ttl_put", mode),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..num_ops {
                            engine
                                .put_with_ttl(&cf, &keys[i], &vals[i], ttl_secs)
                                .unwrap();
                        }
                        engine
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    // READ after TTL insert ----------------------------------------------

    let read_indices: Vec<usize> = (0..num_ops).step_by(4).collect();
    let read_count = read_indices.len();

    group.throughput(Throughput::Bytes((read_count as u64) * BYTES_PER_OP));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("read_after_insert", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_with_mode("ttl_read", mode);
                        let cf = engine.default_column_family();
                        for i in 0..num_ops {
                            engine
                                .put_with_ttl(&cf, &keys[i], &vals[i], ttl_secs)
                                .unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for &i in &read_indices {
                            black_box(engine.get(&cf, &keys[i]).unwrap());
                        }
                        engine
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Column Family Scaling
// ============================================================================

fn bench_column_family_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_advanced/column_families");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 1_000usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    for mode in ALL_STORAGE_MODES {
        for &cf_count in &[1, 4, 8, 16] {
            group.throughput(Throughput::Bytes(bytes_total));

            group.bench_with_input(
                BenchmarkId::new(format!("{}_cfs", cf_count), mode.as_str()),
                &(mode, cf_count),
                |b, &(mode, cf_count)| {
                    b.iter_batched(
                        || {
                            let engine = setup_engine_with_mode(
                                &format!("cf_scale_{}_{}", mode.as_str(), cf_count),
                                mode,
                            );
                            for i in 1..cf_count {
                                engine
                                    .create_column_family(&format!("cf{}", i))
                                    .unwrap();
                            }
                            engine
                        },
                        |(engine)| {
                            let cf_list = engine.list_column_families().unwrap_or_default();
                            for i in 0..num_ops {
                                let cf = &cf_list[i % cf_count];
                                engine.put(cf, &keys[i], &vals[i]).unwrap();
                            }
                            engine
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Large Value Handling
// ============================================================================

fn bench_large_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_advanced/large_values");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 10usize;

    for mode in ALL_STORAGE_MODES {
        for &value_size in &[64 * 1024, 512 * 1024, 1024 * 1024] {
            // Precompute large buffers
            let keys: Vec<Bytes> = (0..num_ops).map(make_key).collect();
            let vals: Vec<Bytes> = (0..num_ops).map(|_| make_value_fixed(value_size)).collect();

            let bytes_total = (num_ops as u64) * (KEY_SIZE as u64 + value_size as u64);

            // PUT ----------------------------------------------------------

            group.throughput(Throughput::Bytes(bytes_total));
            group.bench_with_input(
                BenchmarkId::new(format!("put_{}b", value_size), mode.as_str()),
                &mode,
                |b, &mode| {
                    b.iter_batched(
                        || {
                            setup_engine_with_mode(
                                &format!("large_put_{}_{}", mode.as_str(), value_size),
                                mode,
                            )
                        },
                        |engine| {
                            let cf = engine.default_column_family();
                            for i in 0..num_ops {
                                engine.put(&cf, &keys[i], &vals[i]).unwrap();
                            }
                            engine
                        },
                        BatchSize::SmallInput,
                    )
                },
            );

            // GET ----------------------------------------------------------

            group.throughput(Throughput::Bytes(bytes_total));
            group.bench_with_input(
                BenchmarkId::new(format!("get_{}b", value_size), mode.as_str()),
                &mode,
                |b, &mode| {
                    b.iter_batched(
                        || {
                            let engine = setup_engine_with_mode(
                                &format!("large_get_{}_{}", mode.as_str(), value_size),
                                mode,
                            );
                            let cf = engine.default_column_family();
                            for i in 0..num_ops {
                                engine.put(&cf, &keys[i], &vals[i]).unwrap();
                            }
                            engine
                        },
                        |engine| {
                            let cf = engine.default_column_family();
                            for k in &keys {
                                black_box(engine.get(&cf, k).unwrap());
                            }
                            engine
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

// ============================================================================
// Delete-Heavy Workload
// ============================================================================

fn bench_delete_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_advanced/delete_heavy");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 2_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    // 50% delete ----------------------------------------------------------

    let delete_50_indices: Vec<usize> = (0..num_keys).step_by(2).collect();
    let delete_50_count = delete_50_indices.len();

    group.throughput(Throughput::Bytes(
        (delete_50_count as u64) * KEY_SIZE as u64,
    ));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("delete_50pct", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_with_mode("delete_heavy_50", mode);
                        let cf = engine.default_column_family();
                        for i in 0..num_keys {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for &i in &delete_50_indices {
                            engine.delete(&cf, &keys[i]).unwrap();
                        }
                        engine
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    // 90% delete ----------------------------------------------------------

    let delete_90_count = (num_keys * 9) / 10;

    group.throughput(Throughput::Bytes(
        (delete_90_count as u64) * KEY_SIZE as u64,
    ));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("delete_90pct", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_with_mode("delete_heavy_90", mode);
                        let cf = engine.default_column_family();
                        for i in 0..num_keys {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for key in keys.iter().take(delete_90_count) {
                            engine.delete(&cf, key).unwrap();
                        }
                        engine
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Criterion Entry
// ============================================================================

criterion_group! {
    name = tier3_system_engine_advanced;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_ttl,
        bench_column_family_scaling,
        bench_large_values,
        bench_delete_heavy
}
criterion_main!(tier3_system_engine_advanced);
