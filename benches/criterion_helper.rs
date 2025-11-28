/// Criterion configuration helper tuned for fast, reasonably stable runs.
///
/// Usage in benchmarks:
/// ```
/// criterion_group!(name = my_bench; config = criterion_config(); targets = bench_fn);
/// ```
use criterion::Criterion;
use std::time::Duration;

pub fn criterion_config() -> Criterion {
    // Single, solid default: quick enough for local/CI use, but
    // with enough samples to give stable-ish numbers for hotpaths.
    Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_millis(700))
        .warm_up_time(Duration::from_millis(300))
        .confidence_level(0.95)
        .significance_level(0.05)
        .noise_threshold(0.02)
        .nresamples(50_000)
        .without_plots()
}
