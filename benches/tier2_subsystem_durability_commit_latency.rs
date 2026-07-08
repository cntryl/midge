//! Tier 2 - Durability commit latency
//!
//! Measures fixed-operation latency for one put plus one synced commit on durable local storage.
//! This target owns the direct durability cost for a single committed transaction.
//! Tier 4 owns sustained write throughput under durability and compaction pressure.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::{Engine, TransactionMode, WriteOptions};
use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};
use std::time::Instant;

const VALUE_SIZE: usize = 128;
const TRANSACTIONS_PER_SAMPLE: usize = 512;
const FIXTURE_KEYS: usize = TRANSACTIONS_PER_SAMPLE;

fn run_transaction_batch(
    engine: &Engine,
    cf_id: cntryl_midge::ColumnFamilyId,
    fixture: &[(Vec<u8>, Vec<u8>)],
    write_options: fn() -> WriteOptions,
) -> u64 {
    for (key, value) in fixture {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin durability latency transaction");
        tx.put(key.clone(), value.clone(), None)
            .expect("put durability latency value");
        tx.commit(write_options())
            .expect("commit durability latency transaction");
    }

    fixture.len() as u64
}

fn run_commit_latency_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    commit_mode: &'static str,
    write_options: fn() -> WriteOptions,
) {
    let mut opts = stress_config::write_coordination_opts_for_mode("local");
    opts.enable_compaction = false;

    ctx.parameter("logical_batch_size", TRANSACTIONS_PER_SAMPLE);
    ctx.parameter("logical_unit", "transaction");
    ctx.parameter("storage_profile", "local");
    ctx.parameter("commit_mode", commit_mode);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("memtable_size_bytes", opts.memtable_size);
    ctx.parameter("operation_surface", "single_put_single_commit");

    let fixture: Vec<(Vec<u8>, Vec<u8>)> = (0..FIXTURE_KEYS)
        .map(|i| {
            let key = stress_config::bench_stress::key16_u64_be(i as u64).to_vec();
            let fill = u8::try_from(i % 251).expect("value byte fits in u8");
            let value = vec![fill; VALUE_SIZE];
            (key, value)
        })
        .collect();

    let engine = Engine::open(opts.to_open_options()).expect("open durability latency engine");
    let cf = engine
        .create_column_family("cf1")
        .expect("create durability latency column family");
    let cf_id = cf.id();

    let started_at = Instant::now();
    let completed = run_transaction_batch(&engine, cf_id, &fixture, write_options);
    ctx.record_external(scenario, started_at.elapsed(), completed);
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local(ctx: &mut StressContext) {
    run_commit_latency_case(
        ctx,
        "tier2_durability_commit_sync_local",
        "sync",
        WriteOptions::sync,
    );
}

stress_main!();
