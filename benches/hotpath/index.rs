//! Tier 1 — Hot Path Index Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical index structures:
//! - Bloom filter (build, query, encode/decode)
//! - Sparse index (binary search lookups)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use criterion_helper::criterion_config;

use std::hint::black_box;

fn make_keys(prefix: &str, n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("{}{:08}", prefix, i).into_bytes())
        .collect()
}

/// Benchmark bloom filter construction
fn bench_bloom_build(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_bloom_build");

    for &n in &[1_000, 10_000] {
        let keys = make_keys("k", n);
        g.bench_function(format!("{}_keys", n), |b| {
            b.iter_batched(
                || (midge::bloom::BloomFilter::new(n, 0.01), &keys),
                |(mut f, keys)| {
                    for k in keys.iter() {
                        f.add(k);
                    }
                    black_box(f);
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

    let n = 10_000;
    let present = make_keys("p", n);
    let absent = make_keys("q", n);
    let mut f = midge::bloom::BloomFilter::new(n, 0.01);
    for k in &present {
        f.add(k);
    }

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

criterion_group! {
    name = hotpath_index;
    config = criterion_config();
    targets = bench_bloom_build, bench_bloom_query
}
criterion_main!(hotpath_index);
