// This bench was pruned and moved to `stress/pruned/tier3_system_read_latency_during_flush.rs`.
// The heavy scenario is available under `stress/pruned/` and should be run via the stress harness.

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_read_latency_during_flush(_: &mut Criterion) {
    // Stubbed: full scenario moved to stress/pruned/
}

criterion_group!(benches, bench_read_latency_during_flush);
criterion_main!(benches);
