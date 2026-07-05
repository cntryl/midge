//! Tier 1 — Hot Path WAL Encoding Benchmarks
//!
//! Covers WAL record TLV serialization, deserialization, and round trips.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::wal::encoding::{decode, encode};
use cntryl_midge::wal::{WalOpKind, WalRecord};
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

const WAL_ENCODE_BATCH_SIZE_DEFAULT: usize = 128;
const WAL_ENCODE_BATCH_SIZE_SMALL: usize = 512;
const WAL_DECODE_BATCH_SIZE_DEFAULT: usize = 256;
const WAL_DECODE_BATCH_SIZE_DELETE: usize = 512;
const WAL_DECODE_BATCH_SIZE_MEDIUM: usize = 512;
const WAL_ROUNDTRIP_BATCH_SIZE_DEFAULT: usize = 256;
const WAL_ROUNDTRIP_BATCH_SIZE_MEDIUM: usize = 128;

cntryl_stress::stress_allocator!();

fn small_put_record() -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(b"key"),
        Some(Bytes::from_static(b"value")),
        1,
        1,
    )
}

fn medium_put_record() -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        Bytes::from_static(&[0u8; 64]),
        Some(Bytes::from_static(&[0u8; 256])),
        1,
        1,
    )
}

fn delete_record() -> WalRecord {
    WalRecord::new(
        WalOpKind::Delete,
        Bytes::from_static(b"deleted_key"),
        None,
        1,
        1,
    )
}

fn run_encode_record(ctx: &mut StressContext, scenario: &'static str, record: &WalRecord) {
    let encode_batch_size = if scenario == "small_put" {
        WAL_ENCODE_BATCH_SIZE_SMALL
    } else {
        WAL_ENCODE_BATCH_SIZE_DEFAULT
    };
    ctx.parameter("scenario", scenario);
    ctx.parameter("encode_batch_size", encode_batch_size);

    stress_config::measure_micro_batch(ctx, encode_batch_size as u64, || {
        let mut encoded = 0usize;
        for _ in 0..encode_batch_size {
            let out = encode(record).unwrap();
            encoded = encoded.wrapping_add(out.len());
        }
        black_box(encoded);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "encode_small_put")
)]
fn encode_small_put(ctx: &mut StressContext) {
    run_encode_record(ctx, "small_put", &small_put_record());
}

#[stress_test(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "encode_medium_put")
)]
fn encode_medium_put(ctx: &mut StressContext) {
    run_encode_record(ctx, "medium_put", &medium_put_record());
}

#[stress_test(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "encode_delete")
)]
fn encode_delete(ctx: &mut StressContext) {
    run_encode_record(ctx, "delete", &delete_record());
}

fn run_decode_record(ctx: &mut StressContext, scenario: &'static str, encoded: &Bytes) {
    let decode_batch_size = if scenario == "medium_put" {
        WAL_DECODE_BATCH_SIZE_MEDIUM
    } else if scenario == "delete" {
        WAL_DECODE_BATCH_SIZE_DELETE
    } else {
        WAL_DECODE_BATCH_SIZE_DEFAULT
    };
    ctx.parameter("scenario", scenario);
    ctx.parameter("decode_batch_size", decode_batch_size);

    stress_config::measure_micro_batch(ctx, decode_batch_size as u64, || {
        let mut decoded = 0usize;
        for _ in 0..decode_batch_size {
            let record = decode(encoded.clone()).unwrap();
            decoded += usize::from(record.seq >= 1);
        }
        black_box(decoded);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "decode_small_put")
)]
fn decode_small_put(ctx: &mut StressContext) {
    run_decode_record(ctx, "small_put", &encode(&small_put_record()).unwrap());
}

#[stress_test(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "decode_medium_put")
)]
fn decode_medium_put(ctx: &mut StressContext) {
    run_decode_record(ctx, "medium_put", &encode(&medium_put_record()).unwrap());
}

#[stress_test(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "decode_delete")
)]
fn decode_delete(ctx: &mut StressContext) {
    run_decode_record(ctx, "delete", &encode(&delete_record()).unwrap());
}

fn run_roundtrip(ctx: &mut StressContext, scenario: &'static str, record: &WalRecord) {
    let batch_size = if scenario == "medium" {
        WAL_ROUNDTRIP_BATCH_SIZE_MEDIUM
    } else {
        WAL_ROUNDTRIP_BATCH_SIZE_DEFAULT
    };
    ctx.parameter("scenario", scenario);
    ctx.parameter("roundtrip_batch_size", batch_size);

    stress_config::measure_micro_batch(ctx, batch_size as u64, || {
        let mut decoded = 0usize;
        for _ in 0..batch_size {
            let encoded = encode(record).unwrap();
            let record = decode(encoded).unwrap();
            decoded = decoded.wrapping_add(record.key.len());
            decoded = decoded.wrapping_add(record.value.as_ref().map_or(0, Bytes::len));
        }
        black_box(decoded);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "roundtrip_small")
)]
fn roundtrip_small(ctx: &mut StressContext) {
    run_roundtrip(ctx, "small", &small_put_record());
}

#[stress_test(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "roundtrip_medium")
)]
fn roundtrip_medium(ctx: &mut StressContext) {
    run_roundtrip(ctx, "medium", &medium_put_record());
}

stress_main!();
