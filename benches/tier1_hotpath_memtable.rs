//! Tier 1 — Memtable Hot Path Benchmarks
//!
//! Covers insert, lookup, delete, and size accounting on the in-memory memtable.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::sst::{Memtable, SkipListMemtable};
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const LOOKUP_HIT_BATCH_SIZE: usize = 1_048_576;
const LOOKUP_HIT_BATCH_OPS: u64 = 1_048_576;
const LOOKUP_MISS_BATCH_SIZE: usize = 1_048_576;
const LOOKUP_MISS_BATCH_OPS: u64 = 1_048_576;
const PUT_SINGLE_BATCH_SIZE: usize = 65_536;
const PUT_SINGLE_BATCH_OPS: u64 = 65_536;
const PUT_BATCH_ROUNDS: usize = 128;
const ROTATING_WRITE_KEYS: usize = 65_536;
const DELETE_BATCH_SIZE: usize = 65_536;
const DELETE_KEY_COUNT: usize = 1000;
const SIZE_BYTES_BATCH_SIZE: usize = 1_048_576;
const SIZE_BYTES_PER_LOGICAL_OPERATION: usize = 1024;

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("benchmark count fits in u64")
}

#[inline]
fn make_key(i: usize) -> Vec<u8> {
    format!("key_{i:010}").into_bytes()
}

fn make_value(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

fn make_value_indexed(i: usize) -> Vec<u8> {
    format!("value_{i}").into_bytes()
}

fn warmed_memtable(value: &[u8]) -> SkipListMemtable {
    let memtable = SkipListMemtable::new();
    for i in 0..100 {
        let _ = memtable.put(make_key(i), value.to_vec());
    }
    memtable
}

fn run_put_single(ctx: &mut StressContext, scenario: &'static str, value_size: usize) {
    let value = make_value(value_size);
    let keys: Vec<Vec<u8>> = (100..100 + ROTATING_WRITE_KEYS).map(make_key).collect();
    let memtable = warmed_memtable(&value);
    let mut key_index = 0usize;
    ctx.parameter("scenario", scenario);
    ctx.parameter("value_size", value_size);
    ctx.parameter("batch_size", PUT_SINGLE_BATCH_SIZE);
    stress_config::mark_validated_micro(ctx, "memtable_put");

    ctx.benchmark(scenario)
        .measure_batch(PUT_SINGLE_BATCH_OPS, || {
            for _ in 0..PUT_SINGLE_BATCH_SIZE {
                let idx = key_index % keys.len();
                key_index = key_index.wrapping_add(1);
                let _ = memtable.put(black_box(keys[idx].clone()), black_box(value.clone()));
            }
        });
}

#[stress(
    tier = 1,
    metadata(component = "memtable", scenario = "put_single_64b")
)]
fn put_single_64b(ctx: &mut StressContext) {
    run_put_single(ctx, "put_single_64b", 64);
}

#[stress(tier = 1, metadata(component = "memtable", scenario = "put_batch_100"))]
fn put_batch_100(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(128);
    ctx.parameter("batch_size", keys.len());
    ctx.parameter("rounds", PUT_BATCH_ROUNDS);
    ctx.parameter("value_size", value.len());
    ctx.parameter("logical_unit", "memtable_put_batch");
    ctx.parameter("items_per_logical_operation", keys.len());

    stress_config::measure_hot_path_batch(
        ctx,
        "put_batch_100",
        usize_to_u64(PUT_BATCH_ROUNDS),
        || {
            let mut inserted = 0usize;
            for _ in 0..PUT_BATCH_ROUNDS {
                let memtable = SkipListMemtable::new();
                for key in &keys {
                    let _ = memtable.put(black_box(key.clone()), black_box(value.clone()));
                    inserted = inserted.wrapping_add(1);
                }
                black_box(memtable);
            }
            black_box(inserted);
        },
    );
}

#[stress(tier = 1, metadata(component = "memtable", scenario = "get_hit"))]
fn get_hit(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..1000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..1000).map(make_value_indexed).collect();
    let memtable = SkipListMemtable::new();
    for i in 0..1000 {
        let _ = memtable.put(keys[i].clone(), values[i].clone());
    }
    let hit_keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    ctx.parameter("lookup_batch_size", LOOKUP_HIT_BATCH_SIZE);
    ctx.parameter("lookup_key_count", hit_keys.len());
    stress_config::mark_validated_micro(ctx, "memtable_get_hit");

    ctx.benchmark("get_hit")
        .measure_batch(LOOKUP_HIT_BATCH_OPS, || {
            let mut hits = 0usize;
            for i in 0..LOOKUP_HIT_BATCH_SIZE {
                let hit_key = hit_keys[i % hit_keys.len()];
                if matches!(
                    memtable
                        .get_key_state_at_with_time(black_box(hit_key), u64::MAX, 0)
                        .unwrap(),
                    cntryl_midge::sst::types::KeyState::Value(..)
                ) {
                    hits += 1;
                }
            }
            black_box(hits);
        });
}

#[stress(
    tier = 1,
    role = "diagnostic",
    metadata(component = "memtable", scenario = "get_miss")
)]
fn get_miss(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..1000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..1000).map(make_value_indexed).collect();
    let memtable = SkipListMemtable::new();
    for i in 0..1000 {
        let _ = memtable.put(keys[i].clone(), values[i].clone());
    }
    let miss_keys: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("key_{i:010}_missing").into_bytes())
        .collect();
    ctx.parameter("lookup_batch_size", LOOKUP_MISS_BATCH_SIZE);
    ctx.parameter("lookup_key_count", miss_keys.len());
    stress_config::mark_validated_micro(ctx, "memtable_get_miss");
    stress_config::mark_local_rsd_diagnostic(ctx);

    ctx.benchmark("get_miss")
        .measure_batch(LOOKUP_MISS_BATCH_OPS, || {
            let mut misses = 0usize;
            for i in 0..LOOKUP_MISS_BATCH_SIZE {
                let miss_key = &miss_keys[i % miss_keys.len()];
                if matches!(
                    memtable
                        .get_key_state_at_with_time(black_box(miss_key.as_slice()), u64::MAX, 0,)
                        .unwrap(),
                    cntryl_midge::sst::types::KeyState::Absent
                ) {
                    misses += 1;
                }
            }
            black_box(misses);
        });
}

#[stress(tier = 1, metadata(component = "memtable", scenario = "delete"))]
fn delete(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..DELETE_KEY_COUNT).map(make_key).collect();
    let value = make_value(128);
    let memtable = SkipListMemtable::new();
    for key in &keys {
        let _ = memtable.put(key.clone(), value.clone());
    }
    let mut key_index = 0usize;
    ctx.parameter("key_count", keys.len());
    ctx.parameter("batch_size", DELETE_BATCH_SIZE);
    ctx.parameter("logical_unit", "memtable_delete");

    stress_config::measure_hot_path_batch(ctx, "delete", usize_to_u64(DELETE_BATCH_SIZE), || {
        let mut deleted = 0usize;
        for _ in 0..DELETE_BATCH_SIZE {
            let idx = key_index % keys.len();
            key_index = key_index.wrapping_add(1);
            let _ = memtable.delete(black_box(keys[idx].clone()));
            deleted = deleted.wrapping_add(1);
        }
        black_box(deleted);
    });
}

#[stress(
    tier = 1,
    metadata(
        component = "memtable",
        scenario = "size_bytes",
        validated_micro = "true"
    )
)]
fn size_bytes(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(1024);
    let memtable = SkipListMemtable::new();
    for key in &keys {
        let _ = memtable.put(key.clone(), value.clone());
    }
    ctx.parameter("key_count", keys.len());
    ctx.parameter("value_size", value.len());
    ctx.parameter("batch_size", SIZE_BYTES_BATCH_SIZE);
    ctx.parameter(
        "size_reads_per_logical_operation",
        SIZE_BYTES_PER_LOGICAL_OPERATION,
    );
    ctx.parameter("logical_unit", "memtable_size_bytes_batch");

    stress_config::measure_hot_path_batch(
        ctx,
        "size_bytes",
        usize_to_u64(SIZE_BYTES_BATCH_SIZE / SIZE_BYTES_PER_LOGICAL_OPERATION),
        || {
            let mut total = 0usize;
            for _ in 0..SIZE_BYTES_BATCH_SIZE {
                total = total.wrapping_add(memtable.size_bytes());
            }
            black_box(total);
        },
    );
}

stress_main!();
