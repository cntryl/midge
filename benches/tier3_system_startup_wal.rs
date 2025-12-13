//! Tier 3 — Startup WAL replay bench
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers engine startup with WAL replay
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since WAL replay requires persistence

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    precompute_kv, setup_engine_at_path, unique_bench_path, BenchEngineConfig, BYTES_PER_OP,
    DURABLE_STORAGE_MODES, VALUE_SIZE,
};

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Benchmark name constant
const BENCH_WAL_REPLAY: &str = "wal_replay";

/// Benchmark engine startup with WAL replay (50k operations)
fn bench_engine_startup_from_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_from_wal");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 50_000usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("replay_50k_wal_ops", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;

                b.iter_batched(
                    || {
                        let path = unique_bench_path(BENCH_WAL_REPLAY);
                        let _ = std::fs::remove_dir_all(&path);

                        // Large memtable = no auto flush, keep data only in WAL
                        let config = BenchEngineConfig {
                            storage_mode: mode,
                            enable_compaction: false,
                            memtable_size: 100 * 1024 * 1024,
                            ..Default::default()
                        };

                        // Create WAL with 50k operations WITHOUT flushing
                        {
                            let engine = setup_engine_at_path(&path, &config);
                            let cf = engine.default_column_family();

                            for i in 0..num_ops {
                                engine
                                    .put(&cf, &keys_ref[i], &vals_ref[i])
                                    .expect("put failed");
                            }
                            // DO NOT flush - keep data only in WAL
                        }

                        (path, config)
                    },
                    |(path, config)| {
                        // Measure startup time (WAL replay into memtable)
                        let engine = setup_engine_at_path(&path, &config);

                        // Verify data was recovered from WAL
                        let cf = engine.default_column_family();
                        let key = black_box(&keys_ref[25_000]);
                        black_box(engine.get(&cf, key).expect("get failed"));

                        engine
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_startup_wal;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_engine_startup_from_wal
}
criterion_main!(tier3_system_startup_wal);
