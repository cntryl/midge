//! Tier 1 — Hot Path Index Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical index structures:
//! - Bloom filter (build, query, encode/decode)
//! - Sparse index (binary search lookups)
//! - False positive rate variance

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::sst::BloomFilter;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;

use std::hint::black_box;

fn make_keys(prefix: &str, n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{}{:08}", prefix, i).into_bytes())
        .collect()
}

/// Benchmark bloom filter add operations (hot path during SST build).
///
/// Separates allocation from insertion to measure pure add() throughput.
fn bench_bloom_build(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_bloom_build");
    g.sampling_mode(SamplingMode::Flat);

    for &n in &[1_000, 10_000] {
        let keys = make_keys("k", n);
        g.throughput(Throughput::Elements(n as u64));

        // Pre-allocate filter outside benchmark loop - we're measuring add() throughput
        g.bench_function(format!("{}_keys", n), |b| {
            b.iter_batched(
                || BloomFilter::new(n, 0.01),
                |mut f| {
                    for k in keys.iter() {
                        f.add(black_box(k));
                    }
                    black_box(f)
                },
                BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

/// Benchmark bloom filter queries (hot path for every read)
fn bench_bloom_query(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_bloom_query");
    g.sampling_mode(SamplingMode::Flat);

    let n = 10_000;
    let present = make_keys("p", n);
    let absent = make_keys("q", n);
    let mut f = BloomFilter::new(n, 0.01);
    for k in &present {
        f.add(k);
    }

    g.throughput(Throughput::Elements(n as u64));
    g.bench_function("present", |b| {
        b.iter(|| {
            let mut cnt = 0;
            for k in &present {
                if f.may_contain(k) {
                    cnt += 1;
                }
            }
            black_box(cnt)
        })
    });

    g.throughput(Throughput::Elements(n as u64));
    g.bench_function("absent", |b| {
        b.iter(|| {
            let mut cnt = 0;
            for k in &absent {
                if f.may_contain(k) {
                    cnt += 1;
                }
            }
            black_box(cnt)
        })
    });

    g.finish();
}

/// Benchmark false positive rate variance across different target FP rates
fn bench_bloom_false_positive_rates(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_bloom_fp_variance");
    g.sampling_mode(SamplingMode::Flat);

    let n = 10_000;
    let present = make_keys("p", n);
    let absent = make_keys("q", 10_000); // More absents to get accurate FP rate

    // Test different false positive targets
    for &fp_rate in &[0.001, 0.01, 0.05] {
        let mut f = BloomFilter::new(n, fp_rate);
        for k in &present {
            f.add(k);
        }

        g.throughput(Throughput::Elements(10_000));
        g.bench_function(format!("fp_rate_{}", (fp_rate * 1000.0) as u32), |b| {
            b.iter(|| {
                let mut false_positives = 0;
                for k in &absent {
                    if f.may_contain(k) {
                        false_positives += 1;
                    }
                }
                black_box(false_positives)
            })
        });
    }

    g.finish();
}

criterion_group! {
    name = tier1_hotpath_index;
    config = criterion_config();
    targets = bench_bloom_build, bench_bloom_query, bench_bloom_false_positive_rates
}
criterion_main!(tier1_hotpath_index);
