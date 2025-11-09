//! Tier 1 — Hot Path SST Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers SST encoding/decoding, iteration, and writer performance across
//! various entry counts and configurations.
//!
//! # Organization
//! 1. **Encoding** - SST entry encoding with different prefix lengths
//! 2. **Decoding** - Single SST entry parsing performance
//! 3. **Iteration** - TlvBlockIterator over multiple entries
//! 4. **Round-trip** - Full encode -> decode cycles
//! 5. **Writer Small Scale** - 100 entries
//! 6. **Writer Medium Scale** - 1K entries
//! 7. **Writer Large Scale** - 10K entries
//! 8. **Writer Compression** - None vs LZ4 comparison
//! 9. **Writer Internal Keys** - With/without internal key metadata

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::codec::CompressionType;
use cntryl_midge::sst::encoding::{decode, encode, TlvBlockIterator};
use cntryl_midge::sst::format::DataBlockBuilder;
use cntryl_midge::sst::mem::SstMemWriter;
use std::hint::black_box;

// ============================================================================
// Helper Functions
// ============================================================================

/// Calculate shared prefix length between two byte slices
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ============================================================================
// Section 1: Encoding Benchmarks
// ============================================================================

/// Benchmark SST entry encoding with different shared prefix lengths
fn bench_sst_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_encode");

    let test_cases = vec![
        (
            "no_prefix",
            b"completely_different_key".as_slice(),
            b"prev_key_with_nothing_shared".as_slice(),
        ),
        (
            "short_prefix",
            b"user:1000:profile".as_slice(),
            b"user:1000:settings".as_slice(),
        ),
        (
            "long_prefix",
            b"namespace:subdomain:entity:attribute:12345".as_slice(),
            b"namespace:subdomain:entity:attribute:12346".as_slice(),
        ),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, key, prev_key) in test_cases {
        group.bench_function(name, |b| {
            let value = b"value_data";
            let shared_len = shared_prefix_len(prev_key, key);
            let key_delta = &key[shared_len..];
            b.iter(|| {
                black_box(encode(
                    key_delta,
                    shared_len as u32,
                    Some(value),
                    1,     // seq
                    false, // tombstone
                    false, // internal_on_disk
                    None,  // expiration
                ))
            });
        });
    }

    group.finish();
}

// ============================================================================
// Section 2: Decoding Benchmarks
// ============================================================================

/// Benchmark single SST entry decoding
fn bench_sst_parse_entry(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_parse");

    // Pre-encode entries with different prefix lengths
    let test_cases = vec![
        ("no_prefix", {
            let key = b"completely_different_key";
            let prev_key = b"prev_key";
            let shared_len = shared_prefix_len(prev_key, key);
            let key_delta = &key[shared_len..];
            let encoded = encode(
                key_delta,
                shared_len as u32,
                Some(b"value"),
                1,
                false,
                false,
                None,
            );
            (encoded, prev_key.len())
        }),
        ("short_prefix", {
            let key = b"user:1000:profile";
            let prev_key = b"user:1000:settings";
            let shared_len = shared_prefix_len(prev_key, key);
            let key_delta = &key[shared_len..];
            let encoded = encode(
                key_delta,
                shared_len as u32,
                Some(b"value"),
                1,
                false,
                false,
                None,
            );
            (encoded, prev_key.len())
        }),
        ("long_prefix", {
            let key = b"namespace:subdomain:entity:attribute:12345";
            let prev_key = b"namespace:subdomain:entity:attribute:12346";
            let shared_len = shared_prefix_len(prev_key, key);
            let key_delta = &key[shared_len..];
            let encoded = encode(
                key_delta,
                shared_len as u32,
                Some(b"value"),
                1,
                false,
                false,
                None,
            );
            (encoded, prev_key.len())
        }),
    ];

    group.throughput(Throughput::Elements(1));

    for (name, (encoded, _prev_key_len)) in test_cases {
        group.bench_function(name, |b| {
            b.iter(|| black_box(decode(&encoded, 0, encoded.len()).unwrap()));
        });
    }

    group.finish();
}

// ============================================================================
// Section 3: Iterator Benchmarks
// ============================================================================

/// Benchmark TlvBlockIterator over multiple entries
fn bench_sst_block_iterator(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_iterator");

    // Build a realistic block with restart points
    for num_entries in [10, 50] {
        let mut builder = DataBlockBuilder::new(16); // restart_interval = 16

        // Add entries with shared prefixes (realistic pattern)
        for i in 0..num_entries {
            let key = format!("user:namespace:{:08}", i);
            let value = format!("value_{}", i);
            builder.add(key.as_bytes(), value.as_bytes()).unwrap();
        }

        let block_data = builder.finish();

        group.throughput(Throughput::Elements(num_entries as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_entries),
            &block_data,
            |b, block: &Bytes| {
                b.iter(|| {
                    let iter = TlvBlockIterator::new(block.as_ref());
                    let count = iter.count();
                    black_box(count)
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Section 4: Round-trip Benchmarks
// ============================================================================

/// Benchmark full encode -> decode round-trip
fn bench_sst_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_roundtrip");

    let key = b"user:1000:profile";
    let value = b"value_data";
    let prev_key = b"user:1000:settings";
    let shared_len = shared_prefix_len(prev_key, key);
    let key_delta = &key[shared_len..];

    group.throughput(Throughput::Elements(1));
    group.bench_function("short_prefix", |b| {
        b.iter(|| {
            let encoded = encode(
                key_delta,
                shared_len as u32,
                Some(value),
                1,
                false,
                false,
                None,
            );
            // Just verify we can parse it - don't return references
            decode(&encoded, 0, encoded.len())
                .map(|e| e.shared_len)
                .unwrap();
            black_box(encoded)
        });
    });

    group.finish();
}

// ============================================================================
// Section 5: Writer Small Scale (100 entries)
// ============================================================================

fn bench_sst_writer_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_writer_small");
    group.throughput(Throughput::Elements(100));

    group.bench_function("100_entries", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);

            for i in 0..100 {
                let key = format!("key_{:05}", i);
                let value = format!("value_{:05}", i);
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }

            black_box(writer.finish_bytes().unwrap())
        });
    });

    group.finish();
}

// ============================================================================
// Section 6: Writer Medium Scale (1K entries)
// ============================================================================

fn bench_sst_writer_medium(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_writer_medium");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("1k_entries", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);

            for i in 0..1000 {
                let key = format!("user:{:04}:profile", i); // Zero-padded for correct sorting
                let value = format!("{{\"name\":\"User{}\",\"age\":{}}}", i, 20 + (i % 50));
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }

            black_box(writer.finish_bytes().unwrap())
        });
    });

    group.finish();
}

// ============================================================================
// Section 7: Writer Large Scale (10K entries)
// ============================================================================

fn bench_sst_writer_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_writer_large");
    group.throughput(Throughput::Elements(10000));
    group.sample_size(20); // Reduce sample size for faster benchmarking

    group.bench_function("10k_entries", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new(CompressionType::None, 4096);

            for i in 0..10000 {
                let key = format!("namespace:entity:id:{:08}", i);
                let value = format!(
                    "{{\"data\":\"payload_{}\",\"timestamp\":{}}}",
                    i,
                    1700000000 + i
                );
                writer.add(key.as_bytes(), value.as_bytes()).unwrap();
            }

            black_box(writer.finish_bytes().unwrap())
        });
    });

    group.finish();
}

// ============================================================================
// Section 8: Writer Compression (None vs LZ4)
// ============================================================================

fn bench_sst_writer_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_writer_compression");
    group.throughput(Throughput::Elements(1000));

    for compression in [CompressionType::None, CompressionType::Lz4] {
        let name = match compression {
            CompressionType::None => "none",
            CompressionType::Lz4 => "lz4",
            _ => "other",
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &compression,
            |b, &comp| {
                b.iter(|| {
                    let mut writer = SstMemWriter::new(comp, 4096);

                    for i in 0..1000 {
                        let key = format!("key_{:06}", i);
                        let value = b"x".repeat(100); // Compressible data
                        writer.add(key.as_bytes(), &value).unwrap();
                    }

                    black_box(writer.finish_bytes().unwrap())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Section 9: Writer Internal Keys (with/without metadata)
// ============================================================================

fn bench_sst_writer_internal_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_sst_writer_internal_keys");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("with_internal_keys", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new_with_internal(CompressionType::None, 4096, true);

            for i in 0..1000 {
                let key = format!("key_{:06}", i);
                writer
                    .add_with_meta(key.as_bytes(), Some(b"value"), i as u64, false, None)
                    .unwrap();
            }

            black_box(writer.finish_bytes().unwrap())
        });
    });

    group.bench_function("without_internal_keys", |b| {
        b.iter(|| {
            let mut writer = SstMemWriter::new_with_internal(CompressionType::None, 4096, false);

            for i in 0..1000 {
                let key = format!("key_{:06}", i);
                writer
                    .add_with_meta(key.as_bytes(), Some(b"value"), i as u64, false, None)
                    .unwrap();
            }

            black_box(writer.finish_bytes().unwrap())
        });
    });

    group.finish();
}

// ============================================================================
// Criterion Registration
// ============================================================================

criterion_group! {
    name = hotpath_sst;
    config = criterion_config();
    targets =
        bench_sst_encode,
        bench_sst_parse_entry,
        bench_sst_block_iterator,
        bench_sst_roundtrip,
        bench_sst_writer_small,
        bench_sst_writer_medium,
        bench_sst_writer_large,
        bench_sst_writer_compression,
        bench_sst_writer_internal_keys
}
criterion_main!(hotpath_sst);
