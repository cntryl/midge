/// Criterion configuration helper that respects CRITERION_FULL environment variable
///
/// Usage in benchmarks:
/// ```
/// criterion_group!(name = my_bench; config = criterion_config(); targets = bench_fn);
/// ```
use criterion::Criterion;
use std::time::Duration;

pub fn criterion_config() -> Criterion {
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
