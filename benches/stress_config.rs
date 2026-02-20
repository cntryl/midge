/// Stress configuration for Fitz tier3 and tier4 benchmarks
///
/// Tier 3: System-level (domain + plumbing, single family/concurrent access patterns)
/// Tier 4: Integration-level (full TCP/WS to domain, complete pipeline)
///
/// **Environment variables**
/// - `BENCH_RUNS`: Number of measurement runs per stress test (default: 5). Use lower (e.g. 3) for CI.
/// - `BENCH_WARMUP`: Number of warmup runs before measurement (default: 1).
///
/// **set_elements(N)** in each `#[stress_test]`: N must match the logical number of operations
/// inside `ctx.measure(|| { ... })` so that throughput (elements/time) reported by
/// `scripts/benchmark_summary.py` is meaningful and comparable across scenarios.
#[allow(dead_code)]
pub struct BenchConfig {
    pub runs: usize,
    pub warmup: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        let runs = std::env::var("BENCH_RUNS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);

        let warmup = std::env::var("BENCH_WARMUP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);

        BenchConfig { runs, warmup }
    }
}
