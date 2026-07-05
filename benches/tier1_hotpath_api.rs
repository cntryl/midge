//! Tier 1 — Hot Path API Benchmarks
//!
//! Covers memory-only public API write and read hot paths.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::Bytes;
use cntryl_midge::{
    testkit::{MidgeOptions, StorageMode},
    MidgeEngine,
};
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const BATCH_PUT_ROUNDS: usize = 16;
const SINGLE_GET_BATCH_SIZE: usize = 2048;
const SINGLE_PUT_BATCH_SIZE: usize = 512;

fn make_fixed_kv(size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(size);
    let mut vals = Vec::with_capacity(size);
    for i in 0..size {
        let mut key = [0u8; 16];
        key[8..16].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(Bytes::copy_from_slice(&key));
        let mut val = [0u8; 32];
        val[24..32].copy_from_slice(&(i as u64).to_be_bytes());
        vals.push(Bytes::copy_from_slice(&val));
    }
    (keys, vals)
}

fn setup_db() -> MidgeEngine {
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        wal_sync: false,
        wal_batch_config: None,
        memtable_size: 1024 * 1024 * 1024,
        compression: false,
        enable_compaction: false,
        memory_budget: Some(4 * 1024 * 1024 * 1024),
        cloud_runtime_policy_overrides: None,
        simulated_cloud_overrides: None,
    };

    MidgeEngine::open_with_options(&opts).unwrap()
}

fn run_batch_put(ctx: &mut StressContext, scenario: &'static str, batch_size: usize) {
    let engine = setup_db();
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::buffered();
    let (keys, vals) = make_fixed_kv(batch_size * BATCH_PUT_ROUNDS);
    ctx.parameter("batch_size", batch_size);
    ctx.parameter("rounds", BATCH_PUT_ROUNDS);
    ctx.parameter("storage_profile", "memory");

    stress_config::measure_hot_path_batch(
        ctx,
        scenario,
        (batch_size * BATCH_PUT_ROUNDS) as u64,
        || {
            for round in 0..BATCH_PUT_ROUNDS {
                let offset = round * batch_size;
                let mut tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                for i in 0..batch_size {
                    let idx = offset + i;
                    tx.put(keys[idx].to_vec(), vals[idx].to_vec(), None)
                        .unwrap();
                }
                tx.commit(write_opts).unwrap();
            }
            black_box(());
        },
    );

    let _ = engine.flush_cf(&cf);
}

#[stress(tier = 1, metadata(component = "api", scenario = "batch_put_100"))]
fn batch_put_100(ctx: &mut StressContext) {
    run_batch_put(ctx, "batch_put_100", 100);
}

#[stress(tier = 1, metadata(component = "api", scenario = "batch_put_1000"))]
fn batch_put_1000(ctx: &mut StressContext) {
    run_batch_put(ctx, "batch_put_1000", 1_000);
}

fn setup_get_fixture() -> (
    MidgeEngine,
    cntryl_midge::ColumnFamilyId,
    Vec<Bytes>,
    Vec<Bytes>,
) {
    let engine = setup_db();
    let cf = engine.create_column_family("cf1").unwrap();
    let num_keys = 1_000;
    let (keys, vals) = make_fixed_kv(num_keys);
    for i in 0..num_keys {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(keys[i].to_vec(), vals[i].to_vec(), None).unwrap();
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    }
    (engine, cf.id(), keys, vals)
}

#[stress(
    tier = 1,
    metadata(component = "api", scenario = "single_get_hit_memtable")
)]
fn single_get_hit_memtable(ctx: &mut StressContext) {
    let (engine, cf_id, keys, _vals) = setup_get_fixture();
    let mut key_index = 0usize;
    ctx.parameter("batch_size", SINGLE_GET_BATCH_SIZE);
    ctx.parameter("key_count", keys.len());

    stress_config::measure_hot_path_batch(
        ctx,
        "single_get_hit_memtable",
        SINGLE_GET_BATCH_SIZE as u64,
        || {
            let mut hits = 0usize;
            for _ in 0..SINGLE_GET_BATCH_SIZE {
                let idx = key_index % keys.len();
                key_index = key_index.wrapping_add(1);
                let tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin");
                if tx.get(black_box(&keys[idx])).unwrap().is_some() {
                    hits += 1;
                }
            }
            black_box(hits);
        },
    );
}

#[stress(tier = 1, metadata(component = "api", scenario = "single_get_miss"))]
fn single_get_miss(ctx: &mut StressContext) {
    let (engine, cf_id, keys, _vals) = setup_get_fixture();
    let num_keys = keys.len();
    let miss_keys: Vec<Bytes> = (0..num_keys)
        .map(|i| {
            let mut key = [0u8; 16];
            key[8..16].copy_from_slice(&((i + num_keys * 2) as u64).to_be_bytes());
            Bytes::copy_from_slice(&key)
        })
        .collect();
    let mut key_index = 0usize;
    ctx.parameter("batch_size", SINGLE_GET_BATCH_SIZE);
    ctx.parameter("key_count", num_keys);

    stress_config::measure_hot_path_batch(
        ctx,
        "single_get_miss",
        SINGLE_GET_BATCH_SIZE as u64,
        || {
            let mut misses = 0usize;
            for _ in 0..SINGLE_GET_BATCH_SIZE {
                let idx = key_index % num_keys;
                key_index = key_index.wrapping_add(1);
                let tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin");
                if tx.get(black_box(&miss_keys[idx])).unwrap().is_none() {
                    misses += 1;
                }
            }
            black_box(misses);
        },
    );
}

#[stress(tier = 1, metadata(component = "api", scenario = "single_put"))]
fn single_put(ctx: &mut StressContext) {
    let engine = setup_db();
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    let num_ops = 65_536;
    let (keys, vals) = make_fixed_kv(num_ops);
    let mut key_index = 0usize;
    ctx.parameter("batch_size", SINGLE_PUT_BATCH_SIZE);
    ctx.parameter("key_count", num_ops);

    stress_config::measure_hot_path_batch(ctx, "single_put", SINGLE_PUT_BATCH_SIZE as u64, || {
        for _ in 0..SINGLE_PUT_BATCH_SIZE {
            let idx = key_index % num_ops;
            key_index = key_index.wrapping_add(1);
            let mut tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.put(keys[idx].to_vec(), vals[idx].to_vec(), None)
                .unwrap();
            tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        }
        black_box(key_index);
    });
}

stress_main!();
