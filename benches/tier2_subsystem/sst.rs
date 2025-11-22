//! Tier 2 — Subsystem SST Benchmarks
//!
//! Target Runtime: 3–8 seconds
//! Frequency: On-demand or nightly
//!
//! Covers realistic SST writer + block iteration + compression behavior.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::common::codec::CompressionType;
use cntryl_midge::sst::encoding::TlvBlockIterator;
use cntryl_midge::sst::format::DataBlockBuilder;
use cntryl_midge::sst::mem::SstMemWriter;
use std::hint::black_box;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_kv(n: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        let mut k = [0u8; 27];
        k[..19].copy_from_slice(b"user:tenant:entity:");
        k[19..27].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(Bytes::copy_from_slice(&k));
        vals.push(Bytes::from_static(b"value_payload_000000000000000"));
    }
    (keys, vals)
}

fn build_test_block(n: usize) -> Bytes {
    let (keys, vals) = make_kv(n);
    let mut builder = DataBlockBuilder::new(16);

    for i in 0..n {
        builder.add(&keys[i], &vals[i]).unwrap();
    }

    builder.finish()
}

// ---------------------------------------------------------------------------
// 1. Full Block Iteration (scan all entries)
// ---------------------------------------------------------------------------

fn bench_sst_iterator_full(c: &mut Criterion) {
    use criterion::SamplingMode;
    let mut g = c.benchmark_group("subsystem_sst_iterator");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[100usize, 1_000, 10_000] {
        let block = build_test_block(entries);
        g.throughput(Throughput::Elements(entries as u64));

        g.bench_with_input(BenchmarkId::from_parameter(entries), &block, |b, block| {
            b.iter(|| {
                let iter = TlvBlockIterator::new(block.as_ref());
                black_box(iter.count());
            });
        });
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 2. Full Block Decode (correct raw TLV decode)
// ---------------------------------------------------------------------------

fn bench_sst_full_decode(c: &mut Criterion) {
    use criterion::SamplingMode;
    let mut g = c.benchmark_group("subsystem_sst_full_decode");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[100usize, 1_000, 10_000] {
        let block = build_test_block(entries);
        g.throughput(Throughput::Elements(entries as u64));

        g.bench_with_input(BenchmarkId::from_parameter(entries), &block, |b, block| {
            b.iter(|| {
                let iter = TlvBlockIterator::new(block.as_ref());
                for result in iter {
                    black_box(result.unwrap());
                }
            });
        });
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 3. SST Writer Scale (100, 1k, 10k entries)
// ---------------------------------------------------------------------------

fn bench_sst_writer_scale(c: &mut Criterion) {
    use criterion::SamplingMode;
    let mut g = c.benchmark_group("subsystem_sst_writer_scale");
    g.sampling_mode(SamplingMode::Flat);

    for &entries in &[100usize, 1_000, 10_000] {
        let (keys, vals) = make_kv(entries);

        g.throughput(Throughput::Elements(entries as u64));

        g.bench_with_input(
            BenchmarkId::from_parameter(entries),
            &(keys, vals, entries),
            |b, (keys, vals, n)| {
                b.iter(|| {
                    let mut writer = SstMemWriter::new(CompressionType::None, 4096);

                    for i in 0..*n {
                        writer.add(&keys[i], &vals[i]).unwrap();
                    }

                    black_box(writer.finish_bytes().unwrap());
                });
            },
        );
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 4. Writer Compression Comparison (None vs LZ4)
// ---------------------------------------------------------------------------

fn bench_sst_writer_compression(c: &mut Criterion) {
    use criterion::SamplingMode;
    let mut g = c.benchmark_group("subsystem_sst_writer_compression");
    g.sampling_mode(SamplingMode::Flat);

    let (keys, vals) = make_kv(1_000);

    for &comp in &[CompressionType::None, CompressionType::Lz4] {
        let name = match comp {
            CompressionType::None => "none",
            CompressionType::Lz4 => "lz4",
            _ => continue,
        };

        g.bench_with_input(BenchmarkId::from_parameter(name), &comp, |b, &c_type| {
            b.iter(|| {
                let mut writer = SstMemWriter::new(c_type, 4096);

                for i in 0..1_000 {
                    writer.add(&keys[i], &vals[i]).unwrap();
                }

                black_box(writer.finish_bytes().unwrap())
            });
        });
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// Criterion Registration
// ---------------------------------------------------------------------------

criterion_group! {
    name = subsystem_sst;
    config = criterion_config();
    targets =
        bench_sst_iterator_full,
        bench_sst_full_decode,
        bench_sst_writer_scale,
        bench_sst_writer_compression
}

criterion_main!(subsystem_sst);
