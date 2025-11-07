Perfect — here’s a concise, **practical, reusable “Benchmark Playbook”**.
It keeps the professional tone and structure of your original doc but focuses on _actionable performance engineering_, not just Criterion boilerplate.

# **Benchmark Playbook**

**Version:** 2.0
**Last Updated:** October 30, 2025

## **Purpose**

This playbook defines how to design, execute, and maintain meaningful benchmarks for any performance-critical project.
It emphasizes **real-world insight**, **reproducibility**, and **actionable data** — not just micro-numbers.

## **Core Principles**

1. **Benchmarks must drive decisions.**
   Every benchmark should help confirm or reject a hypothesis (“does batching improve throughput?”).

2. **Fast feedback matters.**
   Quick, approximate results are more useful day-to-day than long, perfect runs you never execute.

3. **Reproducibility over realism.**
   Use fixed inputs, stable environments, and deterministic seeds.

4. **Focus on what users feel.**
   Throughput, latency, and resource use matter more than synthetic metrics like iteration counts.

5. **Actionable results only.**
   Every run should produce a number that can be compared, trended, or gated in CI.

## **Benchmark Tiers**

| Tier                        | Purpose                                                          | Target Runtime | Frequency                |
| --------------------------- | ---------------------------------------------------------------- | -------------- | ------------------------ |
| **Tier 1 — Hot Path Micro** | Single functions or primitives (hashing, insert, append).        | < 1 s          | On every PR              |
| **Tier 2 — Subsystem**      | Integrated modules (cache + IO + serialization).                 | < 5 s          | Daily CI                 |
| **Tier 3 — System / Soak**  | Full workflows including persistence, concurrency, and recovery. | 1–5 min        | Nightly / release builds |

Each benchmark should declare its tier in code or docs.

## **Design Checklist**

- **Question:** What are we learning?
- **Metric:** ops/sec, GB/s, µs latency, or amplification ratio.
- **Baseline:** Reference version or previous commit.
- **Threshold:** ± 5 % change triggers review.
- **Repeatability:** Fixed seed and input set.
- **Output:** Numeric summary + optional histogram.

## **Environment Requirements**

To ensure comparability:

- **Hardware:** Record CPU model, core count, memory, disk/NVMe.
- **Software:** Compiler version, build flags, allocator, OS kernel.
- **Isolation:** Disable turbo scaling, background jobs, and network noise.
- **Location:** Run in tmpfs or RAM disk when testing logic only.

Document these in `BENCH_ENV.md`.

## **Directory Layout**

```
benches/
├── hotpath.rs        # Core primitives
├── subsystem.rs      # Combined components
├── system.rs         # Full workflow
└── utils/            # Shared setup/helpers
```

Each file should own a clear subsystem or flow.

## **Benchmark Patterns**

### **Throughput (bulk ops)**

```rust
b.iter(|| {
    for record in &records {
        black_box(target.insert(record));
    }
});
```

### **Latency (single op)**

```rust
b.iter(|| {
    black_box(target.query(key));
});
```

### **Scaling (input-size sweep)**

```rust
for size in [1_000, 10_000, 100_000] {
    group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &s| { … });
}
```

Use `black_box()` to prevent optimization and set up data outside the loop.

## **Configuration Profiles**

| Profile    | Sample Size | Measurement | Use Case        |
| ---------- | ----------- | ----------- | --------------- |
| **Quick**  | 10          | 1 s         | Local iteration |
| **Perf**   | 50          | 10 s        | CI or profiling |
| **Stress** | 100         | 30 s        | Release or soak |

Example:

```rust
Criterion::default()
    .sample_size(10)
    .measurement_time(Duration::from_secs(1));
```

## **CI Integration**

- **Fast mode:** run Tier 1–2 with quick config.
- **Perf mode:** Tier 3 gated behind `--features perf`.
- **Regression gate:**

  ```bash
  cargo bench -- --save-baseline main
  cargo bench -- --baseline main --fail-threshold 0.05
  ```

- Fail CI if throughput ↓ > 5 % or latency ↑ > 10 %.

## **Result Reporting**

- Export raw Criterion results (`target/criterion`) to CI artifacts.
- Optionally push summaries to Prometheus, Influx, or CSV for trending.
- Track regressions across commits — visualize percent deltas, not absolute numbers.

## **Best Practices**

✅ **Do**

- Pre-allocate all inputs outside the measurement loop.
- Use deterministic data patterns.
- Benchmark steady-state performance only.
- Add brief doc comments describing intent (“measures insert throughput under contention”).

❌ **Don’t**

- Include correctness checks — that’s what tests are for.
- Mix I/O randomness unless that’s the point.
- Benchmark rare code paths.
- Run benchmarks with debug builds.

## **Performance Targets (Generic)**

| Metric                | Competitive                   | Best-in-Class |
| --------------------- | ----------------------------- | ------------- |
| Throughput (ops/sec)  | Within 80 % of peer libraries | ≥ 95 %        |
| Latency p99           | < 200 µs                      | < 100 µs      |
| CPU Utilization       | < 80 % per core               | < 60 %        |
| Variance between runs | < 5 %                         | < 2 %         |

Adjust numbers per project domain.

## **Maintenance**

- Review results weekly; update baselines monthly.
- Delete stale or redundant benchmarks.
- Add a short changelog entry when benchmark behavior changes.

## **Template Summary**

```rust
fn bench_insert(c: &mut Criterion) {
    let data = setup_data();
    c.bench_function("insert_1k", |b| {
        b.iter(|| black_box(target_insert(&data)));
    });
}
criterion_group!(benches, bench_insert);
criterion_main!(benches);
```

**This playbook is designed to be cloned, edited, and reused** across any Rust or systems project that values measurable, reproducible performance.
It favors clarity, actionability, and integration over exhaustive boilerplate.
