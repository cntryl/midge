//! Tier 2 — Subsystem Engine Benchmarks
//!
//! **Target Runtime:** < 5 seconds total
//! **Run Frequency:** Daily CI
//!
//! Covers integrated engine subsystems:
//! - CRUD operations (put/get/delete, random vs sequential)
//! - Write strategies (sync modes, batch sizes)
//! - Memory mode performance
//! - Read latency distribution
//! - TTL expiration

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use rand::seq::IndexedRandom;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::hint::black_box;
use std::time::Duration;

fn setup_db(name: &str, enable_wal_sync: bool) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_subsystem_engine_{}", name));
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

    for &value_size in &[100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("sequential", value_size),
            &value_size,
            |b, &sz| {
                b.iter_batched(
                    || setup_db(&format!("put_seq_{}", sz), false),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..1_000 {
                            engine.put(&cf, &make_key(i), &make_value(i, sz)).unwrap();
                        }
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("random", value_size),
            &value_size,
            |b, &sz| {
                b.iter_batched(
                    || setup_db(&format!("put_rand_{}", sz), false),
                    |engine| {
                        let cf = engine.default_column_family();
                        let mut keys: Vec<_> = (0..1_000).collect();
                        let mut rng = StdRng::seed_from_u64(42);
                        keys.shuffle(&mut rng);
                        for i in keys {
                            engine.put(&cf, &make_key(i), &make_value(i, sz)).unwrap();
                        }
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

fn bench_get_hit_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_get");
    let engine = setup_db("get", false);
    let cf = engine.default_column_family();

    for i in 0..5_000 {
        engine.put(&cf, &make_key(i), &make_value(i, 800)).unwrap();
    }

    group.bench_function("hit_mixed", |b| {
        b.iter(|| {
            for i in (0..5_000).step_by(5) {
                let _ = engine.get(&cf, &make_key(i)).unwrap();
            }
        })
    });

    group.bench_function("miss_random", |b| {
        let mut rng = StdRng::seed_from_u64(99);
        let misses: Vec<_> = (10_000..11_000).collect();
        b.iter(|| {
            for i in misses.choose_multiple(&mut rng, 500) {
                let _ = engine.get(&cf, &make_key(*i)).unwrap();
            }
        })
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_delete");
    group.bench_function("delete_existing", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("delete", false);
                let cf = engine.default_column_family();
                for i in 0..1_000 {
                    engine.put(&cf, &make_key(i), &make_value(i, 120)).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                for i in (0..1_000).step_by(2) {
                    engine.delete(&cf, &make_key(i)).unwrap();
                }
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

// ============================================================================
// Write Strategies
// ============================================================================

fn bench_write_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_write_modes");

    group.bench_function("nosync_batched", |b| {
        b.iter_batched(
            || setup_db("nosync_batch", false),
            |engine| {
                let cf = engine.default_column_family();
                for batch in 0..10 {
                    for i in 0..100 {
                        engine
                            .put(&cf, &make_key(batch * 100 + i), &make_value(i, 100))
                            .unwrap();
                    }
                }
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sync_every_write", |b| {
        b.iter_batched(
            || setup_db("sync", true),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..100 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("small_batch_write", |b| {
        b.iter_batched(
            || setup_db("small_batch", false),
            |engine| {
                let cf = engine.default_column_family();
                for batch in 0..50 {
                    for i in 0..20 {
                        engine
                            .put(&cf, &make_key(batch * 20 + i), &make_value(i, 200))
                            .unwrap();
                    }
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
            || {
                let opts = MidgeOptions {
                    storage_mode: StorageMode::Memory,
                    memtable_size: 4 * 1024 * 1024,
                    enable_compaction: false,
                    ..Default::default()
                };
                MidgeEngine::open(opts).unwrap()
            },
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..1_000 {
                    engine.put(&cf, &make_key(i), &make_value(i, 150)).unwrap();
                }
                for i in (0..1_000).step_by(3) {
                    let _ = engine.get(&cf, &make_key(i)).unwrap();
                }
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// TTL Operations
// ============================================================================

fn bench_ttl(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_ttl");

    group.bench_function("put_with_ttl", |b| {
        b.iter_batched(
            || setup_db("ttl", false),
            |engine| {
                let cf = engine.default_column_family();
                let ttl = Duration::from_secs(1200);
                for i in 0..500 {
                    engine
                        .put_with_ttl(&cf, &make_key(i), &make_value(i, 80), ttl.as_secs())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("ttl_read_after_insert", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("ttl_read", false);
                let cf = engine.default_column_family();
                let ttl = Duration::from_secs(1200);
                for i in 0..500 {
                    engine
                        .put_with_ttl(&cf, &make_key(i), &make_value(i, 100), ttl.as_secs())
                        .unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                for i in (0..500).step_by(4) {
                    let _ = engine.get(&cf, &make_key(i)).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = subsystem_engine;
    config = criterion_config();
    targets =
        bench_put_variants,
        bench_get_hit_miss,
        bench_delete,
        bench_write_modes,
        bench_memory_mode,
        bench_ttl
}
criterion_main!(subsystem_engine);
