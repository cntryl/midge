//! Tier 1 — Memtable Hot Path Benchmarks
//!
//! Covers insert, lookup, delete, and size accounting on the in-memory memtable.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::sst::{Memtable, SkipListMemtable};
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};
use std::sync::atomic::{AtomicUsize, Ordering};

const LOOKUP_BATCH_SIZE: usize = 8192;
const LOOKUP_BATCH_OPS: u64 = 8192;
const PUT_SINGLE_BATCH_SIZE: usize = 1024;
const PUT_SINGLE_BATCH_OPS: u64 = 1024;
const PUT_COUNTED_BATCH_SIZE: usize = 4096;
const PUT_COUNTED_BATCH_OPS: u64 = 4096;
const PUT_COUNTED_LARGE_BATCH_SIZE: usize = 65_536;
const PUT_COUNTED_LARGE_BATCH_OPS: u64 = 65_536;
const ROTATING_WRITE_KEYS: usize = 4096;
const SIZE_BYTES_BATCH_SIZE: usize = 1024;
const SIZE_BYTES_BATCH_OPS: u64 = 1024;

cntryl_stress::stress_allocator!();

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
    let counter = AtomicUsize::new(0);
    ctx.parameter("scenario", scenario);
    ctx.parameter("value_size", value_size);
    ctx.parameter("batch_size", PUT_SINGLE_BATCH_SIZE);

    stress_config::measure_micro_batch(ctx, PUT_SINGLE_BATCH_OPS, || {
        for _ in 0..PUT_SINGLE_BATCH_SIZE {
            let idx = counter.fetch_add(1, Ordering::Relaxed) % keys.len();
            let _ = memtable.put(black_box(keys[idx].clone()), black_box(value.clone()));
        }
    });
}

fn run_put_counted(ctx: &mut StressContext, scenario: &'static str, value_size: usize) {
    let value = make_value(value_size);
    let keys: Vec<Vec<u8>> = (100..100 + ROTATING_WRITE_KEYS).map(make_key).collect();
    let memtable = warmed_memtable(&value);
    let (batch_size, batch_ops) = if value_size >= 4096 {
        (PUT_COUNTED_LARGE_BATCH_SIZE, PUT_COUNTED_LARGE_BATCH_OPS)
    } else {
        (PUT_COUNTED_BATCH_SIZE, PUT_COUNTED_BATCH_OPS)
    };
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..batch_size)
        .map(|i| (keys[i % keys.len()].clone(), value.clone()))
        .collect();
    ctx.parameter("scenario", scenario);
    ctx.parameter("value_size", value_size);
    ctx.parameter("batch_size", batch_size);

    let _completed = ctx.measure_counted(|| {
        for (key, value) in pairs {
            let _ = memtable.put(black_box(key), black_box(value));
        }
        batch_ops
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "memtable", scenario = "put_single_64b")
)]
fn put_single_64b(ctx: &mut StressContext) {
    run_put_single(ctx, "64b_value", 64);
}

#[stress_test(
    tier = 2,
    metadata(component = "memtable", scenario = "put_single_1kb")
)]
fn put_single_1kb(ctx: &mut StressContext) {
    run_put_counted(ctx, "1kb_value", 1024);
}

#[stress_test(
    tier = 2,
    metadata(component = "memtable", scenario = "put_single_4kb")
)]
fn put_single_4kb(ctx: &mut StressContext) {
    run_put_counted(ctx, "4kb_value", 4096);
}

#[stress_test(tier = 1, metadata(component = "memtable", scenario = "put_batch_100"))]
fn put_batch_100(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(128);
    ctx.parameter("batch_size", keys.len());
    ctx.parameter("value_size", value.len());

    stress_config::measure_micro_batch(ctx, keys.len() as u64, || {
        let memtable = SkipListMemtable::new();
        for key in &keys {
            let _ = memtable.put(black_box(key.clone()), black_box(value.clone()));
        }
        black_box(memtable);
    });
}

#[stress_test(tier = 1, metadata(component = "memtable", scenario = "get_hit"))]
fn get_hit(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..1000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..1000).map(make_value_indexed).collect();
    let memtable = SkipListMemtable::new();
    for i in 0..1000 {
        let _ = memtable.put(keys[i].clone(), values[i].clone());
    }
    let hit_keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
    ctx.parameter("lookup_batch_size", LOOKUP_BATCH_SIZE);
    ctx.parameter("lookup_key_count", hit_keys.len());

    stress_config::measure_micro_batch(ctx, LOOKUP_BATCH_OPS, || {
        let mut hits = 0usize;
        for i in 0..LOOKUP_BATCH_SIZE {
            let hit_key = hit_keys[i % hit_keys.len()];
            if memtable.get(black_box(hit_key)).unwrap().is_some() {
                hits += 1;
            }
        }
        black_box(hits);
    });
}

#[stress_test(tier = 1, metadata(component = "memtable", scenario = "get_miss"))]
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
    ctx.parameter("lookup_batch_size", LOOKUP_BATCH_SIZE);
    ctx.parameter("lookup_key_count", miss_keys.len());

    stress_config::measure_micro_batch(ctx, LOOKUP_BATCH_OPS, || {
        let mut misses = 0usize;
        for i in 0..LOOKUP_BATCH_SIZE {
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

#[stress_test(tier = 1, metadata(component = "memtable", scenario = "delete"))]
fn delete(ctx: &mut StressContext) {
    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(128);
    ctx.parameter("key_count", keys.len());

    ctx.measure_micro(|| {
        let memtable = SkipListMemtable::new();
        for key in &keys {
            let _ = memtable.put(key.clone(), value.clone());
        }
        let _ = memtable.delete(black_box(keys[50].clone()));
    });
}

#[stress_test(
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

    stress_config::measure_micro_batch(ctx, SIZE_BYTES_BATCH_OPS, || {
        let mut total = 0usize;
        for _ in 0..SIZE_BYTES_BATCH_SIZE {
            total = total.wrapping_add(memtable.size_bytes());
        }
        black_box(total);
    });
}

stress_main!();
