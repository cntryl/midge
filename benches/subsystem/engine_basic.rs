//! Tier 1-2 — Basic Engine Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers fundamental engine operations:
//! - CRUD operations (put/get/delete, random vs sequential)
//! - Write strategies (sync modes, batch sizes)
//! - Memory mode performance

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::hint::black_box;

fn setup_db(name: &str, enable_wal_sync: bool) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_subsystem_basic_{}", name));
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: enable_wal_sync,
        ..Default::default()
    };
    MidgeEngine::open(opts).unwrap()
}

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}
fn make_value(i: usize, base: usize) -> Bytes {
    // introduce slight variance in size
    let size = base + (i % 50);
    Bytes::from(vec![b'x'; size])
}

// ============================================================================
// CRUD Operations
// ============================================================================

fn bench_put_variants(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_put_variants");

    for &op_count in &[100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("sequential", op_count),
            &op_count,
            |b, &n| {
                b.iter_batched(
                    || setup_db("sequential", false),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..n {
                            engine
                                .put(&cf, &make_key(i), &make_value(i, 80))
                                .unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("random", op_count),
            &op_count,
            |b, &n| {
                b.iter_batched(
                    || {
                        let mut rng = StdRng::seed_from_u64(42);
                        let mut indices: Vec<usize> = (0..n).collect();
                        indices.shuffle(&mut rng);
                        (setup_db("random", false), indices)
                    },
                    |(engine, indices)| {
                        let cf = engine.default_column_family();
                        for i in indices {
                            engine
                                .put(&cf, &make_key(i), &make_value(i, 80))
                                .unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// GET Operations
// ============================================================================

fn bench_get_hit_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_get");

    group.bench_function("hit_mixed", |b| {
        let engine = setup_db("get_hit", false);
        let cf = engine.default_column_family();
        for i in 0..1000 {
            engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
        }

        b.iter(|| {
            for i in (0..1000).step_by(4) {
                let _ = engine.get(&cf, &make_key(i)).unwrap();
            }
        })
    });

    group.bench_function("miss_random", |b| {
        let engine = setup_db("get_miss", false);
        let cf = engine.default_column_family();

        b.iter(|| {
            for i in 1000..1100 {
                let _ = engine.get(&cf, &make_key(i)).unwrap();
            }
        })
    });

    group.finish();
}

// ============================================================================
// DELETE Operations
// ============================================================================

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_delete");

    group.bench_function("delete_existing", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("delete", false);
                let cf = engine.default_column_family();
                for i in 0..1000 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..1000 {
                    engine.delete(&cf, &make_key(i)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Write Modes
// ============================================================================

fn bench_write_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_write_modes");

    group.bench_function("nosync_batched", |b| {
        b.iter_batched(
            || setup_db("nosync", false),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..500 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sync_every_write", |b| {
        b.iter_batched(
            || setup_db("sync", true),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..500 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("small_batch_write", |b| {
        b.iter_batched(
            || setup_db("batch", false),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..500 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Memory Mode
// ============================================================================

fn bench_memory_mode(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memory_mode");

    group.bench_function("read_write_mix", |b| {
        b.iter_batched(
            || setup_db("memory_mode", false),
            |engine| {
                let cf = engine.default_column_family();

                // writes
                for i in 0..100 {
                    engine.put(&cf, &make_key(i), &make_value(i, 200)).unwrap();
                }

                // reads
                for i in (0..100).step_by(2) {
                    let _ = engine.get(&cf, &make_key(i)).unwrap();
                }

                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = subsystem_engine_basic;
    config = criterion_config();
    targets =
        bench_put_variants,
        bench_get_hit_miss,
        bench_delete,
        bench_write_modes,
        bench_memory_mode
}
criterion_main!(subsystem_engine_basic);
