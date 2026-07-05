//! Tier 2 — Memtable Rotate Benchmarks
//!
//! Measures fill + drain cycle cost for small and large memtables.

use cntryl_midge::sst::SkipListMemtable;
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

fn make_kv_pairs(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            (
                format!("key_{i:010}").into_bytes(),
                format!("value_{i:010}").into_bytes(),
            )
        })
        .collect()
}

fn run_memtable_rotate(ctx: &mut StressContext, count: usize) {
    let kv_pairs = make_kv_pairs(count);
    ctx.parameter("entry_count", count);

    let _completed = ctx.measure_counted(|| {
        let memtable = SkipListMemtable::new();
        for (key, value) in &kv_pairs {
            memtable
                .put_with_exp(key.clone(), value.clone(), None)
                .unwrap();
        }
        black_box(memtable.iter_all(u64::MAX));
        count as u64
    });
}

#[stress_test(tier = 2, metadata(component = "memtable", scenario = "rotate_small"))]
fn rotate_small(ctx: &mut StressContext) {
    run_memtable_rotate(ctx, 100);
}

#[stress_test(tier = 2, metadata(component = "memtable", scenario = "rotate_large"))]
fn rotate_large(ctx: &mut StressContext) {
    run_memtable_rotate(ctx, 10_000);
}

stress_main!();
