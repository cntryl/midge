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
const WAL_DECODE_BATCH_SIZE_DEFAULT: usize = 65_536;
const WAL_DECODE_BATCH_SIZE_DELETE: usize = 65_536;
const WAL_DECODE_BATCH_SIZE_MEDIUM: usize = 65_536;
const WAL_ROUNDTRIP_BATCH_SIZE_DEFAULT: usize = 262_144;
const WAL_ROUNDTRIP_BATCH_SIZE_MEDIUM: usize = 65_536;
const WAL_RECORDS_PER_LOGICAL_OPERATION: usize = 32;

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark count fits in u64")
}

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

fn variable_small_put_record(index: usize) -> WalRecord {
    WalRecord::new(
        WalOpKind::Put,
        Bytes::from(format!("key_{index:010}")),
        Some(Bytes::from(format!("value_{index:010}"))),
        usize_to_u64(index).wrapping_add(1),
        1,
    )
}

fn variable_medium_put_record(index: usize) -> WalRecord {
    let mut key = vec![0u8; 64];
    let mut value = vec![0u8; 256];
    let index_bytes = usize_to_u64(index).to_be_bytes();
    key[56..64].copy_from_slice(&index_bytes);
    value[248..256].copy_from_slice(&index_bytes);
    WalRecord::new(
        WalOpKind::Put,
        Bytes::from(key),
        Some(Bytes::from(value)),
        usize_to_u64(index).wrapping_add(1),
        1,
    )
}

fn variable_delete_record(index: usize) -> WalRecord {
    WalRecord::new(
        WalOpKind::Delete,
        Bytes::from(format!("deleted_key_{index:010}")),
        None,
        usize_to_u64(index).wrapping_add(1),
        1,
    )
}

fn variable_record(scenario: &'static str, index: usize) -> WalRecord {
    match scenario {
        "small_put" => variable_small_put_record(index),
        "medium_put" => variable_medium_put_record(index),
        "delete" => variable_delete_record(index),
        _ => unreachable!("unknown WAL decode scenario"),
    }
}

fn wal_decode_logical_operation_count(batch_size: usize) -> u64 {
    usize_to_u64(batch_size / WAL_RECORDS_PER_LOGICAL_OPERATION)
}

fn run_encode_record(ctx: &mut StressContext, scenario: &'static str, record: &WalRecord) {
    let encode_batch_size = if scenario == "small_put" {
        WAL_ENCODE_BATCH_SIZE_SMALL
    } else {
        WAL_ENCODE_BATCH_SIZE_DEFAULT
    };
    let records: Vec<WalRecord> = (0..encode_batch_size)
        .map(|index| variable_record(scenario, index))
        .collect();
    ctx.parameter("scenario", scenario);
    ctx.parameter("encode_batch_size", encode_batch_size);
    ctx.parameter("logical_unit", "wal_record");
    let mut buf = Vec::with_capacity(encode(record).unwrap().len());

    let measurement_name = format!("encode_{scenario}");
    stress_config::measure_hot_path_batch(ctx, measurement_name, encode_batch_size as u64, || {
        let mut encoded = 0usize;
        for record in &records {
            buf.clear();
            encode_into(record, &mut buf).unwrap();
            encoded = encoded.wrapping_add(buf.len());
        }
        black_box(encoded);
    });
}

#[stress(
    tier = 1,
    metadata(
        component = "wal_encoding",
        scenario = "encode_small_put",
        trust_class = "diagnostic",
        validated_micro = "true"
    )
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
    metadata(
        component = "wal_encoding",
        scenario = "encode_delete",
        trust_class = "diagnostic",
        validated_micro = "true"
    )
)]
fn encode_delete(ctx: &mut StressContext) {
    run_encode_record(ctx, "delete", &delete_record());
}

fn run_decode_record(ctx: &mut StressContext, scenario: &'static str) {
    let decode_batch_size = if scenario == "medium_put" {
        WAL_DECODE_BATCH_SIZE_MEDIUM
    } else if scenario == "delete" {
        WAL_DECODE_BATCH_SIZE_DELETE
    } else {
        WAL_DECODE_BATCH_SIZE_DEFAULT
    };
    let encoded_records: Vec<Bytes> = (0..decode_batch_size)
        .map(|i| encode(&variable_record(scenario, i)).unwrap())
        .collect();
    ctx.parameter("scenario", scenario);
    ctx.parameter("decode_batch_size", decode_batch_size);
    ctx.parameter(
        "records_per_logical_operation",
        WAL_RECORDS_PER_LOGICAL_OPERATION,
    );
    ctx.parameter("logical_unit", "wal_decode_batch");

    let measurement_name = format!("decode_{scenario}");
    stress_config::measure_hot_path_batch(
        ctx,
        measurement_name,
        wal_decode_logical_operation_count(decode_batch_size),
        || {
            let mut decoded = 0usize;
            for encoded in &encoded_records {
                let record = decode_view(black_box(encoded.as_ref())).unwrap();
                decoded = decoded.wrapping_add(record.key.len());
                decoded = decoded.wrapping_add(record.value.map_or(0, <[u8]>::len));
                decoded = decoded.wrapping_add(
                    usize::try_from(record.seq).expect("benchmark sequence fits in usize"),
                );
            }
            black_box(decoded);
        },
    );
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "decode_small_put")
)]
fn decode_small_put(ctx: &mut StressContext) {
    run_decode_record(ctx, "small_put");
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "decode_medium_put")
)]
fn decode_medium_put(ctx: &mut StressContext) {
    run_decode_record(ctx, "medium_put");
}

#[stress(
    tier = 1,
    metadata(component = "wal_encoding", scenario = "decode_delete")
)]
fn decode_delete(ctx: &mut StressContext) {
    run_decode_record(ctx, "delete");
}

fn run_roundtrip(ctx: &mut StressContext, scenario: &'static str, record: &WalRecord) {
    let batch_size = if scenario == "medium" {
        WAL_ROUNDTRIP_BATCH_SIZE_MEDIUM
    } else {
        WAL_ROUNDTRIP_BATCH_SIZE_DEFAULT
    };
    ctx.parameter("scenario", scenario);
    ctx.parameter("roundtrip_batch_size", batch_size);
    ctx.parameter("logical_unit", "wal_record");
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
