/// Stress configuration for Tier 3 and Tier 4 benchmarks
///
/// Tier 3: System-level (domain + plumbing, single family/concurrent access patterns)
/// Tier 4: Integration-level (full TCP/WS to domain, complete pipeline)
///
/// **Environment variables**
/// - `BENCH_RUNS`: Number of measurement runs per stress test (default: 1, CI: 3). Increase to 5 for regression detection.
/// - `BENCH_WARMUP`: Number of warmup runs before measurement (default: 1).
///
/// **`set_elements(N)`** in each `#[stress_test]`: N must match the logical number of operations
/// inside `ctx.measure(|| { ... })` so that throughput (elements/time) reported by
/// `cntryl-tools summarize-benchmarks` is meaningful and comparable across scenarios.
///
/// **Signal Discipline (High Priority)**
///
/// Current issue: Stress tests run only 1-3 samples, making regression detection weak.
/// Failure modes:
/// - 10% variance run-to-run is noise → easy to miss real regressions
/// - No latency percentiles tracked → can't detect p99/tail latency regressions
/// - Throughput-only metrics miss isolation violations or fairness issues
///
/// Recommended improvements:
/// 1. Increase `BENCH_RUNS` to 5 for local development, 3+ for CI
/// 2. Track latency distributions: p50, p95, p99, max (not just throughput)
/// 3. Set explicit thresholds:
///    - Hotpath (ns-µs): Flag if >5% regression
///    - Subsystem (µs-ms): Flag if >8-10% regression
///    - Stress (ms+): Flag if >15% throughput regression OR >20% p99 latency regression, sustained across runs
/// 4. For MVCC/concurrent tests, measure fairness: writer latency under snapshot contention
/// 5. For cloud tests, measure recovery correctness (not just throughput)
///
/// See: `docs/development/performance-targets.md` for current guardrail policy.
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

// ═════════════════════════════════════════════════════════════════════════════
// Future: Latency Distribution Tracking
// ═════════════════════════════════════════════════════════════════════════════
//
// The next phase should add structured latency tracking to stress tests.
//
// Example (pseudo-code for future implementation):
//
// ```rust
// use hdrhistogram::Histogram;
//
// pub struct LatencyTracker {
//     histogram: Histogram<u64>,
// }
//
// impl LatencyTracker {
//     pub fn new() -> Self {
//         LatencyTracker {
//             histogram: Histogram::new(7).unwrap(), // up to 10^7 (10ms)
//         }
//     }
//
//     pub fn record(&mut self, latency_us: u64) {
//         let _ = self.histogram.record(latency_us);
//     }
//
//     pub fn percentile(&self, p: f64) -> u64 {
//         self.histogram.value_at_percentile(p)
//     }
//
//     pub fn to_json(&self) -> serde_json::Value {
//         json!({
//             "p50_us": self.percentile(50.0),
//             "p95_us": self.percentile(95.0),
//             "p99_us": self.percentile(99.0),
//             "max_us": self.histogram.max(),
//         })
//     }
// }
// ```
//
// Usage in stress tests:
// ```rust
// let mut tracker = LatencyTracker::new();
// for i in 0..num_ops {
//     let start = std::time::Instant::now();
//     // ... operation ...
//     let latency_us = start.elapsed().as_micros() as u64;
//     tracker.record(latency_us);
// }
//
// ctx.tag("latency_p99_us", &tracker.percentile(99.0).to_string());
// ```
//
// This would give operators visibility into:
// - Long-tail latencies (p99, max)
// - Fairness under contention (writer p99 under snapshot load)
// - Isolation violations affecting latency distribution
