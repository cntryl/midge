// This file was moved to `stress/pruned/tier3_system_isolation_mvcc.rs`.
// It contains several stress scenarios. Keep single-threaded baselines in Tier-1/Tier-2
// where appropriate and move heavy stress scenarios to the stress harness.

// Original content preserved at `stress/pruned/tier3_system_isolation_mvcc.rs` for migration.

#[allow(unused)]
const _TIER3_GUARD: () = {
    // Tier-3 benches must use bench_common::tier3 APIs and `tier3_bench!`/`tier3_bench_restore!`.
};

#[path = "./criterion_helper.rs"]
mod criterion_helper;

#[path = "./tier3_system_bench_common.rs"]
mod bench_common;

use bench_common::{
    precompute_kv, setup_engine_with_mode, ALL_STORAGE_MODES, BYTES_PER_OP, VALUE_SIZE,
};

use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ============================================================================
// 1. Single-Threaded Baseline
// ============================================================================

fn bench_single_thread_baseline(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_baseline_single_thread");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 1_000usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("baseline_seq_puts", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || setup_engine_with_mode("baseline_seq", mode),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..num_ops {
                            engine.put(cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    // Get benchmark - reads step_by(5) = 200 reads
    let read_count = num_ops / 5;
    group.throughput(Throughput::Bytes((read_count as u64) * BYTES_PER_OP));

    for mode in ALL_STORAGE_MODES {
        group.bench_with_input(
            BenchmarkId::new("baseline_random_gets_hit", mode.as_str()),
            &mode,
            |b, &mode| {
                b.iter_batched(
                    || {
                        let engine = setup_engine_with_mode("baseline_get", mode);
                        let cf = engine.default_column_family();
                        for i in 0..num_ops {
                            engine.put(cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine
                    },
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in (0..num_ops).step_by(5) {
                            black_box(engine.get(cf, &keys[i]).unwrap());
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

// bench_contention_breakdown was pruned — moved to `stress/pruned/tier3_system_isolation_mvcc.rs`.
// See stress/pruned file for the full scenario implementation.
// (Pruned content removed from this stub so the bench compiles cleanly.)

// group.finish(); (content intentionally removed)

// bench_snapshot_stress was pruned — moved to `stress/pruned/tier3_system_isolation_mvcc.rs`.
// See stress/pruned file for the full scenario implementation.
// bench_transaction_isolation was pruned — moved to `stress/pruned/tier3_system_isolation_mvcc.rs`.
// See stress/pruned file for the full scenario implementation.

criterion_group! {
    name = tier3_system_isolation_mvcc;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_single_thread_baseline
}
criterion_main!(tier3_system_isolation_mvcc);
