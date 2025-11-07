/// Criterion configuration helper that respects CRITERION_FULL environment variable
///
/// Usage in benchmarks:
/// ```
/// criterion_group!(name = my_bench; config = criterion_config(); targets = bench_fn);
/// ```
use criterion::Criterion;
use std::time::Duration;

pub fn criterion_config() -> Criterion {
    let is_full_mode = std::env::var("CRITERION_FULL")
        .map(|v| v == "1" || v.to_lowercase() == "true")
        .unwrap_or(false);

    if is_full_mode {
        // Full mode: high-fidelity profiling
        Criterion::default()
            .sample_size(100)
            .measurement_time(Duration::from_secs(5))
            .warm_up_time(Duration::from_secs(2))
            .confidence_level(0.99)
            .significance_level(0.01)
            .noise_threshold(0.005)
            .nresamples(100_000)
            .without_plots()
    } else {
        // Quick mode (default): fast CI runs
        Criterion::default()
            .sample_size(10)
            .measurement_time(Duration::from_secs(2))
            .warm_up_time(Duration::from_millis(500))
            .confidence_level(0.95)
            .significance_level(0.05)
            .noise_threshold(0.02)
            .nresamples(50_000)
            .without_plots()
    }
}
