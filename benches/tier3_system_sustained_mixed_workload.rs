// This workload was pruned and moved to `stress/pruned/tier3_system_sustained_mixed_workload.rs`.
// The heavy scenario is available under `stress/pruned/` and should be run via the stress harness.

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_sustained_mixed_workload_with_compaction(_: &mut Criterion) {}

criterion_group!(benches, bench_sustained_mixed_workload_with_compaction);
criterion_main!(benches);
