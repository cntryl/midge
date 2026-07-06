//! Tier 1 — Memtable Hot Path Benchmarks
//!
//! Covers insert, lookup, delete, and size accounting on the in-memory memtable.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::sst::{Memtable, SkipListMemtable};
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const LOOKUP_HIT_BATCH_SIZE: usize = 32_768;
const LOOKUP_HIT_BATCH_OPS: u64 = 32_768;
const LOOKUP_MISS_BATCH_SIZE: usize = 262_144;
const LOOKUP_MISS_BATCH_OPS: u64 = 262_144;
const PUT_SINGLE_BATCH_SIZE: usize = 1024;
const PUT_SINGLE_BATCH_OPS: u64 = 1024;
const ROTATING_WRITE_KEYS: usize = 4096;
const SIZE_BYTES_BATCH_SIZE: usize = 1024;
const SIZE_BYTES_BATCH_OPS: u64 = 1024;

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

    stress_config::measure_hot_path_batch(ctx, scenario, PUT_SINGLE_BATCH_OPS, || {
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
    ctx.parameter("value_size", value.len());

    stress_config::measure_hot_path_batch(ctx, "put_batch_100", keys.len() as u64, || {
        let memtable = SkipListMemtable::new();
        for key in &keys {
            let _ = memtable.put(black_box(key.clone()), black_box(value.clone()));
        }
        black_box(memtable);
    });
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

    stress_config::measure_hot_path_batch(ctx, "get_hit", LOOKUP_HIT_BATCH_OPS, || {
        let mut hits = 0usize;
        for i in 0..LOOKUP_HIT_BATCH_SIZE {
            let hit_key = hit_keys[i % hit_keys.len()];
            if memtable.get(black_box(hit_key)).unwrap().is_some() {
                hits += 1;
            }
        }
        black_box(hits);
    });
}

#[stress(tier = 1, metadata(component = "memtable", scenario = "get_miss"))]
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

    stress_config::measure_hot_path_batch(ctx, "get_miss", LOOKUP_MISS_BATCH_OPS, || {
        let mut misses = 0usize;
        for i in 0..LOOKUP_MISS_BATCH_SIZE {
            let miss_key = &miss_keys[i % miss_keys.len()];
            if memtable
                .get(black_box(miss_key.as_slice()))
                .unwrap()
                .is_none()
            {
                misses += 1;
            }
        }
        black_box(misses);
    });
}

#[stress(tier = 1, metadata(component = "memtable", scenario = "delete"))]
fn delete(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(128);
    ctx.parameter("key_count", keys.len());

    ctx.measure("delete", || {
        let memtable = SkipListMemtable::new();
        for key in &keys {
            let _ = memtable.put(key.clone(), value.clone());
        }
        let _ = memtable.delete(black_box(keys[50].clone()));
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

    stress_config::measure_hot_path_batch(ctx, "size_bytes", SIZE_BYTES_BATCH_OPS, || {
        let mut total = 0usize;
        for _ in 0..SIZE_BYTES_BATCH_SIZE {
            total = total.wrapping_add(memtable.size_bytes());
        }
        black_box(total);
    });
}

stress_main!();
