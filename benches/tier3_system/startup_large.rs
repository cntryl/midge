//! Tier 3 — Startup large dataset bench
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers engine startup with large manifest (many SST files)
//!
//! ## Design Notes
//!
//! - Uses DURABLE_STORAGE_MODES since startup with SSTs requires persistence

#[path = "../criterion_helper.rs"]
mod criterion_helper;

mod bench_common;

use bench_common::{
    precompute_kv, setup_engine_at_path, unique_bench_path, BenchEngineConfig, BYTES_PER_OP,
    DURABLE_STORAGE_MODES, VALUE_SIZE,
};

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};

/// Benchmark name constant
const BENCH_LARGE_MANIFEST: &str = "large_manifest";

/// Benchmark engine startup with large manifest (simulated via many flushes)
fn bench_engine_startup_100k_sst_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_large_manifest");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 5_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    let bytes_total = (num_keys as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in DURABLE_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("startup_with_many_ssts", mode.as_str()),
            &mode,
            |b, &mode| {
                let keys_ref = &keys;
                let vals_ref = &vals;

                b.iter_batched(
                    || {
                        let path = unique_bench_path(BENCH_LARGE_MANIFEST);
                        let _ = std::fs::remove_dir_all(&path);

                        // Small memtable = more SSTs
                        let config = BenchEngineConfig {
                            storage_mode: mode,
                            enable_compaction: false,
                            memtable_size: 64 * 1024,
                            ..Default::default()
                        };

                        // Create database and populate with many small flushes
                        {
                            let engine = setup_engine_at_path(&path, &config);
                            let cf = engine.default_column_family();

                            // Write keys with periodic flushes to create ~50 SST files
                            for i in 0..num_keys {
                                engine
                                    .put(&cf, &keys_ref[i], &vals_ref[i])
                                    .expect("put failed");

                                if i % 100 == 99 {
                                    engine.flush().expect("flush failed");
                                }
                            }
                            engine.flush().expect("final flush failed");
                        }

                        (path, config)
                    },
                    |(path, config)| {
                        // Measure startup time (manifest loading + recovery)
                        setup_engine_at_path(&path, &config)
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_startup_large;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_engine_startup_100k_sst_files
}
criterion_main!(tier3_system_startup_large);
