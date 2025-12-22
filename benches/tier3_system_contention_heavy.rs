// This bench was pruned and moved to `stress/pruned/tier3_system_contention_heavy.rs`.
// The heavy scenarios are available under `stress/pruned/` and should be run via the stress harness.

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_contention_heavy_stub(_: &mut Criterion) {
    // Stubbed – heavy content moved to stress/pruned/
}

criterion_group!(benches, bench_contention_heavy_stub);
criterion_main!(benches);
