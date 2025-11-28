//! Tier 1 — Hot Path TLV Encoding/Decoding Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers TLV (Tag-Length-Value) primitives used throughout WAL/SST formats:
//! - Varint encoding/decoding (lengths, sequences)
//! - TlvWriter operations (building records)
//! - TlvReader operations (parsing records)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;

use cntryl_midge::common::tlv::{
    decode_varint32, decode_varint64, encode_varint32, encode_varint64, tags, TlvReader, TlvWriter,
};
use std::hint::black_box;

// ============================================================================
// Varint32 Encoding Benchmarks
// ============================================================================

fn bench_varint32_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint32_encode");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Small values (< 128) - single byte encoding, common for key lengths
    group.bench_function("small_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint32(&mut buf, black_box(64));
            black_box(&buf);
        });
    });

    // Medium values (128-16383) - two byte encoding
    group.bench_function("medium_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint32(&mut buf, black_box(1024));
            black_box(&buf);
        });
    });

    // Large values (> 16383) - multi-byte encoding
    group.bench_function("large_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint32(&mut buf, black_box(1_000_000));
            black_box(&buf);
        });
    });

    group.finish();
}

// ============================================================================
// Varint64 Encoding Benchmarks
// ============================================================================

fn bench_varint64_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint64_encode");
    group.throughput(Throughput::Elements(1));

    // Small values (< 128) - single byte encoding, common for sequence numbers
    group.bench_function("small_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint64(&mut buf, black_box(100));
            black_box(&buf);
        });
    });

    // Medium values
    group.bench_function("medium_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint64(&mut buf, black_box(10000));
            black_box(&buf);
        });
    });

    // Large values (realistic sequence numbers)
    group.bench_function("large_value", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            encode_varint64(&mut buf, black_box(1_000_000_000));
            black_box(&buf);
        });
    });

    group.finish();
}

// ============================================================================
// Varint32 Decoding Benchmarks
// ============================================================================

fn bench_varint32_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint32_decode");
    group.throughput(Throughput::Elements(1));

    // Pre-encode test values
    let mut small = Vec::new();
    encode_varint32(&mut small, 64);

    let mut medium = Vec::new();
    encode_varint32(&mut medium, 1024);

    let mut large = Vec::new();
    encode_varint32(&mut large, 1_000_000);

    group.bench_function("small_value", |b| {
        b.iter(|| black_box(decode_varint32(&small).unwrap()));
    });

    group.bench_function("medium_value", |b| {
        b.iter(|| black_box(decode_varint32(&medium).unwrap()));
    });

    group.bench_function("large_value", |b| {
        b.iter(|| black_box(decode_varint32(&large).unwrap()));
    });

    group.finish();
}

// ============================================================================
// Varint64 Decoding Benchmarks
// ============================================================================

fn bench_varint64_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_varint64_decode");
    group.throughput(Throughput::Elements(1));

    // Pre-encode test values
    let mut small = Vec::new();
    encode_varint64(&mut small, 100);

    let mut medium = Vec::new();
    encode_varint64(&mut medium, 10000);

    let mut large = Vec::new();
    encode_varint64(&mut large, 1_000_000_000);

    group.bench_function("small_value", |b| {
        b.iter(|| black_box(decode_varint64(&small).unwrap()));
    });

    group.bench_function("medium_value", |b| {
        b.iter(|| black_box(decode_varint64(&medium).unwrap()));
    });

    group.bench_function("large_value", |b| {
        b.iter(|| black_box(decode_varint64(&large).unwrap()));
    });

    group.finish();
}

// ============================================================================
// TlvWriter Benchmarks
// ============================================================================

fn bench_tlv_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_writer");
    group.throughput(Throughput::Elements(1));

    // Write a simple record with primitive types
    group.bench_function("write_primitives", |b| {
        b.iter(|| {
            let mut writer = TlvWriter::new();
            writer.write_u8(tags::OPERATION, black_box(1));
            writer.write_u32(tags::CF_ID, black_box(0));
            writer.write_u64(tags::SEQUENCE, black_box(12345));
            black_box(writer.finish());
        });
    });

    // Write with small bytes field (typical key)
    group.bench_function("write_small_bytes", |b| {
        let key = b"user:12345";
        b.iter(|| {
            let mut writer = TlvWriter::new();
            writer.write_u8(tags::OPERATION, 1);
            writer.write_bytes(tags::KEY, black_box(key));
            black_box(writer.finish());
        });
    });

    // Write with medium bytes field (typical value)
    group.bench_function("write_medium_bytes", |b| {
        let value = vec![0u8; 256];
        b.iter(|| {
            let mut writer = TlvWriter::new();
            writer.write_bytes(tags::VALUE, black_box(&value));
            black_box(writer.finish());
        });
    });

    // Write complete record (realistic WAL-like structure)
    group.bench_function("write_complete_record", |b| {
        let key = b"user:12345";
        let value = vec![0u8; 256];
        b.iter(|| {
            let mut writer = TlvWriter::with_capacity(320);
            writer.write_u8(tags::OPERATION, black_box(0));
            writer.write_u32(tags::CF_ID, black_box(0));
            writer.write_u64(tags::SEQUENCE, black_box(12345));
            writer.write_bytes(tags::KEY, black_box(key));
            writer.write_bytes(tags::VALUE, black_box(&value));
            black_box(writer.finish());
        });
    });

    // Write with reuse (pre-allocated capacity)
    group.bench_function("write_with_reuse", |b| {
        let mut writer = TlvWriter::with_capacity(128);
        let key = b"key";
        b.iter(|| {
            writer.clear();
            writer.write_u8(tags::OPERATION, black_box(1));
            writer.write_bytes(tags::KEY, black_box(key));
            black_box(writer.as_bytes());
        });
    });

    group.finish();
}

// ============================================================================
// TlvReader Benchmarks
// ============================================================================

fn bench_tlv_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_reader");
    group.throughput(Throughput::Elements(1));

    // Pre-encode test records
    let mut writer = TlvWriter::new();
    writer.write_u8(tags::OPERATION, 1);
    writer.write_u32(tags::CF_ID, 0);
    writer.write_u64(tags::SEQUENCE, 12345);
    let simple_record = writer.finish();

    let mut writer = TlvWriter::new();
    writer.write_u8(tags::OPERATION, 0);
    writer.write_u32(tags::CF_ID, 0);
    writer.write_u64(tags::SEQUENCE, 12345);
    writer.write_bytes(tags::KEY, b"user:12345");
    writer.write_bytes(tags::VALUE, &vec![0u8; 256]);
    let complete_record = writer.finish();

    // Parse simple record (3 fields)
    group.bench_function("read_simple_record", |b| {
        b.iter(|| {
            let reader = TlvReader::new(black_box(&simple_record));
            for (tag, _value) in reader {
                black_box(tag);
            }
        });
    });

    // Parse complete record (5 fields with bytes)
    group.bench_function("read_complete_record", |b| {
        b.iter(|| {
            let reader = TlvReader::new(black_box(&complete_record));
            for (tag, _value) in reader {
                black_box(tag);
            }
        });
    });

    // Parse with field extraction (realistic usage)
    group.bench_function("read_and_extract_fields", |b| {
        b.iter(|| {
            let reader = TlvReader::new(black_box(&complete_record));
            let mut op = 0u8;
            let mut seq = 0u64;
            let mut key: &[u8] = &[];

            for (tag, value) in reader {
                match tag {
                    tags::OPERATION => op = value[0],
                    tags::SEQUENCE => {
                        seq = u64::from_be_bytes([
                            value[0], value[1], value[2], value[3], value[4], value[5], value[6],
                            value[7],
                        ]);
                    }
                    tags::KEY => key = value,
                    _ => {}
                }
            }

            black_box((op, seq, key));
        });
    });

    group.finish();
}

// ============================================================================
// Round-trip Benchmarks
// ============================================================================

fn bench_tlv_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_tlv_roundtrip");
    group.throughput(Throughput::Elements(1));

    let key = b"user:12345";
    let value = vec![0u8; 256];

    // Encode + decode simple record
    group.bench_function("simple_record", |b| {
        b.iter(|| {
            // Encode
            let mut writer = TlvWriter::new();
            writer.write_u8(tags::OPERATION, black_box(0));
            writer.write_u64(tags::SEQUENCE, black_box(12345));
            let encoded = writer.finish();

            // Decode
            let reader = TlvReader::new(&encoded);
            for (tag, _value) in reader {
                black_box(tag);
            }
        });
    });

    // Encode + decode complete record
    group.bench_function("complete_record", |b| {
        b.iter(|| {
            // Encode
            let mut writer = TlvWriter::with_capacity(320);
            writer.write_u8(tags::OPERATION, black_box(0));
            writer.write_u32(tags::CF_ID, black_box(0));
            writer.write_u64(tags::SEQUENCE, black_box(12345));
            writer.write_bytes(tags::KEY, black_box(key));
            writer.write_bytes(tags::VALUE, black_box(&value));
            let encoded = writer.finish();

            // Decode
            let reader = TlvReader::new(&encoded);
            for (tag, _value) in reader {
                black_box(tag);
            }
        });
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_tlv;
    config = criterion_config();
    targets =
        bench_varint32_encode,
        bench_varint64_encode,
        bench_varint32_decode,
        bench_varint64_decode,
        bench_tlv_writer,
        bench_tlv_reader,
        bench_tlv_roundtrip
}
criterion_main!(tier1_hotpath_tlv);
