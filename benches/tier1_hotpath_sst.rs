//! Tier 1 — SST Encoding Hot Path Benchmarks
//!
//! Target: < 500ms total runtime
//! Frequency: Every PR (CI gate)
//!
//! Hotpath = operations that occur on most Get/Put cycles:
//! - encode single SST entry (TLV format)
//! - decode single SST entry
//! - roundtrip encode→decode

#[path = "./criterion_config.rs"]
mod criterion_config;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier1;

use cntryl_midge::sst::encoding::{decode, encode, EntryType};
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
    let shared_len = shared_prefix_len(prev, key);
    let shared = u16::try_from(shared_len).expect("shared prefix length fits in u16");
    let delta = &key[shared_len..];

    let value = b"value_data";

    g.bench_function("encode_small", |b| {
        b.iter(|| {
            black_box(encode(delta, shared, Some(value), 1, EntryType::Put));
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
    let shared_len = shared_prefix_len(prev, key);
    let shared = u16::try_from(shared_len).expect("shared prefix length fits in u16");
    let delta = &key[shared_len..];

    let encoded = encode(delta, shared, Some(b"value"), 1, EntryType::Put);

    g.bench_function("decode_small", |b| {
        b.iter(|| {
            black_box(decode(&encoded, 0).unwrap());
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
    let shared_len = shared_prefix_len(prev, key);
    let shared = u16::try_from(shared_len).expect("shared prefix length fits in u16");
    let delta = &key[shared_len..];

    let value = b"value_data";

    g.bench_function("roundtrip_small", |b| {
        b.iter(|| {
            let encoded = encode(delta, shared, Some(value), 1, EntryType::Put);
            let _ = decode(&encoded, 0).unwrap();
            black_box(encoded);
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// Criterion registration
// ---------------------------------------------------------------------------
criterion_group! {
    name = tier1_hotpath_sst;
    config = criterion_config_for_tier1();
    targets =
        bench_encode,
        bench_decode,
        bench_roundtrip
}
criterion_main!(tier1_hotpath_sst);
