//! Tier 1 — Hot Path WAL Encoding Benchmarks
//!
//! Covers WAL record TLV serialization, deserialization, and round trips.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::wal::encoding::{decode_view, encode, encode_into};
use cntryl_midge::wal::{WalOpKind, WalRecord};
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const WAL_ENCODE_BATCH_SIZE_DEFAULT: usize = 4096;
const WAL_ENCODE_BATCH_SIZE_SMALL: usize = 4096;
const WAL_DECODE_BATCH_SIZE_DEFAULT: usize = 4096;
const WAL_DECODE_BATCH_SIZE_DELETE: usize = 4096;
const WAL_DECODE_BATCH_SIZE_MEDIUM: usize = 2048;
const WAL_ROUNDTRIP_BATCH_SIZE_DEFAULT: usize = 4096;
const WAL_ROUNDTRIP_BATCH_SIZE_MEDIUM: usize = 1024;

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
    let mut buf = Vec::with_capacity(encode(record).unwrap().len());

    let measurement_name = format!("encode_{scenario}");
    stress_config::measure_hot_path_batch(ctx, measurement_name, encode_batch_size as u64, || {
        let mut encoded = 0usize;
        for _ in 0..encode_batch_size {
            buf.clear();
            encode_into(record, &mut buf).unwrap();
            encoded = encoded.wrapping_add(buf.len());
        }
        black_box(encoded);
    });
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "encode_small_put")
)]
fn encode_small_put(ctx: &mut StressContext) {
    run_encode_record(ctx, "small_put", &small_put_record());
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "encode_medium_put")
)]
fn encode_medium_put(ctx: &mut StressContext) {
    run_encode_record(ctx, "medium_put", &medium_put_record());
}

#[stress(
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

    let measurement_name = format!("decode_{scenario}");
    stress_config::measure_hot_path_batch(ctx, measurement_name, decode_batch_size as u64, || {
        let mut decoded = 0usize;
        for _ in 0..decode_batch_size {
            let record = decode_view(black_box(encoded.as_ref())).unwrap();
            decoded += usize::from(record.seq >= 1);
        }
        black_box(decoded);
    });
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "decode_small_put")
)]
fn decode_small_put(ctx: &mut StressContext) {
    run_decode_record(ctx, "small_put", &encode(&small_put_record()).unwrap());
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "decode_medium_put")
)]
fn decode_medium_put(ctx: &mut StressContext) {
    run_decode_record(ctx, "medium_put", &encode(&medium_put_record()).unwrap());
}

#[stress(
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
    let mut buf = Vec::with_capacity(encode(record).unwrap().len());

    let measurement_name = format!("roundtrip_{scenario}");
    stress_config::measure_hot_path_batch(ctx, measurement_name, batch_size as u64, || {
        let mut decoded = 0usize;
        for _ in 0..batch_size {
            buf.clear();
            encode_into(record, &mut buf).unwrap();
            let record = decode_view(black_box(&buf)).unwrap();
            decoded = decoded.wrapping_add(record.key.len());
            decoded = decoded.wrapping_add(record.value.map_or(0, <[u8]>::len));
        }
        black_box(decoded);
    });
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "roundtrip_small")
)]
fn roundtrip_small(ctx: &mut StressContext) {
    run_roundtrip(ctx, "small", &small_put_record());
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "roundtrip_medium")
)]
fn roundtrip_medium(ctx: &mut StressContext) {
    run_roundtrip(ctx, "medium", &medium_put_record());
}

stress_main!();
