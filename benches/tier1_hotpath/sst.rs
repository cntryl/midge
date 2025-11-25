//! Tier 1 — REAL Hot Path SST Benchmarks
//!
//! Target: < 500ms total runtime
//! Frequency: Every PR (CI gate)
//!
//! Hotpath = operations that occur on most Get/Put cycles:
//! - encode small delta keys
//! - decode a single entry
//! - iterator.next() on an already-built block
//! - tiny 10-entry writer (memtable flush microstep)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::common::codec::CompressionType;
use cntryl_midge::sst::encoding::{decode, encode, TlvBlockIterator};
use cntryl_midge::sst::format::DataBlockBuilder;
use cntryl_midge::sst::mem::SstMemWriter;
use std::hint::black_box;

// ---------------------------------------------------------------------------
// Shared prefix helper (allocation-free)
// ---------------------------------------------------------------------------
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// HOTPATH 1: Encode single entry
// ---------------------------------------------------------------------------
fn bench_encode(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_sst_encode");
    g.sampling_mode(SamplingMode::Flat);
    g.throughput(Throughput::Elements(1));

    let prev = b"user:1000:settings";
    let key = b"user:1000:profile";
    let shared = shared_prefix_len(prev, key);
    let delta = &key[shared..];

    let value = b"value_data";

    g.bench_function("encode_small", |b| {
        b.iter(|| {
            black_box(encode(
                delta,
                shared as u32,
                Some(value),
                1,
                0,
                false,
                None,
            ));
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// HOTPATH 2: Decode single entry
// ---------------------------------------------------------------------------
fn bench_decode(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_sst_decode");
    g.sampling_mode(SamplingMode::Flat);
    g.throughput(Throughput::Elements(1));

    let prev = b"user:1000:settings";
    let key = b"user:1000:profile";
    let shared = shared_prefix_len(prev, key);
    let delta = &key[shared..];

    let encoded = encode(delta, shared as u32, Some(b"value"), 1, 0, false, None);

    g.bench_function("decode_small", |b| {
        b.iter(|| {
            black_box(decode(&encoded, 0, encoded.len()).unwrap());
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// HOTPATH 3: Iterator.next() (NOT a full scan)
// ---------------------------------------------------------------------------
fn bench_iterator_step(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_sst_iter_step");
    g.sampling_mode(SamplingMode::Flat);
    g.throughput(Throughput::Elements(1));

    // Pre-build 10-entry block outside benchmarking
    let mut builder = DataBlockBuilder::new(16);
    for i in 0..10 {
        let key = format!("user:small:{:03}", i);
        let val = format!("v{}", i);
        builder.add(key.as_bytes(), val.as_bytes()).unwrap();
    }
    let block = builder.finish();

    g.bench_function("iterator_single_step", |b| {
        b.iter(|| {
            let mut it = TlvBlockIterator::new(block.as_ref());
            black_box(it.next()); // only step once
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// HOTPATH 4: Roundtrip (encode → decode 1 entry)
// ---------------------------------------------------------------------------
fn bench_roundtrip(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_sst_roundtrip");
    g.sampling_mode(SamplingMode::Flat);
    g.throughput(Throughput::Elements(1));

    let prev = b"user:1000:settings";
    let key = b"user:1000:profile";
    let shared = shared_prefix_len(prev, key);
    let delta = &key[shared..];

    let value = b"value_data";

    g.bench_function("roundtrip_small", |b| {
        b.iter(|| {
            let encoded = encode(delta, shared as u32, Some(value), 1, 0, false, None);
            let _ = decode(&encoded, 0, encoded.len()).unwrap();
            black_box(encoded);
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// HOTPATH 5: Tiny Writer (10 entries)
// ---------------------------------------------------------------------------
fn bench_writer_tiny(c: &mut Criterion) {
    let mut g = c.benchmark_group("hotpath_sst_writer_tiny");
    g.sampling_mode(SamplingMode::Flat);
    g.throughput(Throughput::Elements(10));

    // Precompute key/value slices
    let keys: Vec<Bytes> = (0..10)
        .map(|i| Bytes::from(format!("key_{:03}", i)))
        .collect();
    let vals: Vec<Bytes> = (0..10).map(|i| Bytes::from(format!("v{:03}", i))).collect();

    g.bench_function("writer_10_entries", |b| {
        b.iter(|| {
            let mut w = SstMemWriter::new(CompressionType::None, 4096);
            for i in 0..10 {
                w.add(&keys[i], &vals[i]).unwrap();
            }
            black_box(w.finish_bytes().unwrap());
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// Criterion registration
// ---------------------------------------------------------------------------
criterion_group! {
    name = hotpath_sst;
    config = criterion_config();
    targets =
        bench_encode,
        bench_decode,
        bench_iterator_step,
        bench_roundtrip,
        bench_writer_tiny
}
criterion_main!(hotpath_sst);
