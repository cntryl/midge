//! Tier 2 - Durability commit latency
//!
//! Measures fixed-work strict commit throughput and latency on durable local storage.
//! The 1-writer row owns the direct durability floor. The 16- and 64-writer rows
//! expose physical WAL append/fsync sharing across independent transactions.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::{ColumnFamilyId, Engine, RuntimeMetricsSnapshot, TransactionMode, WriteOptions};
use cntryl_stress::{stress, stress_main, StressContext};
use hdrhistogram::Histogram;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const VALUE_SIZE: usize = 128;
const TRANSACTIONS_PER_SAMPLE: usize = 512;

struct StrictCommitSample {
    elapsed: Duration,
    completed: u64,
    commit_p50_us: u64,
    commit_p99_us: u64,
    wal_appends: u64,
    physical_fsyncs: u64,
}

fn metrics_delta(start: &RuntimeMetricsSnapshot, end: &RuntimeMetricsSnapshot) -> (u64, u64) {
    (
        end.wal_append_count.saturating_sub(start.wal_append_count),
        end.wal_fsync_count.saturating_sub(start.wal_fsync_count),
    )
}

fn u64_to_f64(value: u64) -> f64 {
    let upper = u32::try_from(value >> 32).expect("upper half fits in u32");
    let lower = u32::try_from(value & u64::from(u32::MAX)).expect("lower half fits in u32");
    f64::from(upper) * 4_294_967_296.0 + f64::from(lower)
}

fn writer_workload(
    engine: &Engine,
    cf_id: ColumnFamilyId,
    writer: usize,
    transactions: usize,
    barrier: &Barrier,
) -> Vec<u64> {
    let mut commit_latencies_us = Vec::with_capacity(transactions);
    barrier.wait();
    for index in 0..transactions {
        let ordinal = writer * transactions + index;
        let key = stress_config::bench_stress::key16_u64_be(
            u64::try_from(ordinal).expect("transaction ordinal fits in u64"),
        )
        .to_vec();
        let value = vec![u8::try_from(ordinal % 251).expect("value byte fits in u8"); VALUE_SIZE];
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin durability latency transaction");
        tx.put(key, value, None)
            .expect("put durability latency value");
        let commit_started_at = Instant::now();
        tx.commit(WriteOptions::sync())
            .expect("commit durability latency transaction");
        commit_latencies_us
            .push(u64::try_from(commit_started_at.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    commit_latencies_us
}

fn execute_strict_commit_case(writers: usize) -> StrictCommitSample {
    assert!(TRANSACTIONS_PER_SAMPLE.is_multiple_of(writers));
    let mut opts = stress_config::write_coordination_opts_for_mode("local");
    opts.enable_compaction = false;
    stress_config::init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let engine =
        Arc::new(Engine::open(opts.to_open_options()).expect("open durability latency engine"));
    let cf = engine
        .create_column_family("cf1")
        .expect("create durability latency column family");
    let cf_id = cf.id();
    let start_metrics = engine
        .get_runtime_metrics()
        .expect("capture starting durability metrics");
    let transactions_per_writer = TRANSACTIONS_PER_SAMPLE / writers;
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            writer_workload(&engine, cf_id, writer, transactions_per_writer, &barrier)
        }));
    }

    let started_at = Instant::now();
    barrier.wait();
    let mut histogram = Histogram::<u64>::new(3).expect("create strict commit histogram");
    for handle in handles {
        for latency_us in handle.join().expect("join strict commit writer") {
            histogram
                .record(latency_us)
                .expect("record strict commit latency");
        }
    }
    let elapsed = started_at.elapsed();
    let end_metrics = engine
        .get_runtime_metrics()
        .expect("capture ending durability metrics");
    let (wal_appends, physical_fsyncs) = metrics_delta(&start_metrics, &end_metrics);
    let mut engine = Arc::try_unwrap(engine).unwrap_or_else(|_| {
        panic!("all strict commit benchmark engine references must be released")
    });
    engine
        .shutdown(Duration::from_secs(10))
        .expect("shut down strict commit benchmark engine");

    StrictCommitSample {
        elapsed,
        completed: u64::try_from(TRANSACTIONS_PER_SAMPLE).expect("transaction count fits in u64"),
        commit_p50_us: histogram.value_at_quantile(0.50),
        commit_p99_us: histogram.value_at_quantile(0.99),
        wal_appends,
        physical_fsyncs,
    }
}

fn run_strict_commit_case(ctx: &mut StressContext, scenario: &'static str, writers: usize) {
    let sample = execute_strict_commit_case(writers);
    ctx.parameter("logical_batch_size", TRANSACTIONS_PER_SAMPLE);
    ctx.parameter("logical_unit", "transaction");
    ctx.parameter("storage_profile", "local");
    ctx.parameter("commit_mode", "sync");
    ctx.parameter("writers", writers);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("operation_surface", "single_put_single_commit");
    ctx.parameter("commit_p50_us", sample.commit_p50_us);
    ctx.parameter("commit_p99_us", sample.commit_p99_us);
    ctx.parameter("wal_appends", sample.wal_appends);
    ctx.parameter("physical_fsyncs", sample.physical_fsyncs);
    ctx.parameter(
        "commits_per_fsync",
        format!(
            "{:.2}",
            u64_to_f64(sample.completed) / u64_to_f64(sample.physical_fsyncs.max(1))
        ),
    );
    ctx.record_external(scenario, sample.elapsed, sample.completed);
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local(ctx: &mut StressContext) {
    run_strict_commit_case(ctx, "tier2_durability_commit_sync_local", 1);
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local_16_writers(ctx: &mut StressContext) {
    run_strict_commit_case(ctx, "tier2_durability_commit_sync_local_16_writers", 16);
}

#[stress(tier = 2)]
fn tier2_durability_commit_sync_local_64_writers(ctx: &mut StressContext) {
    run_strict_commit_case(ctx, "tier2_durability_commit_sync_local_64_writers", 64);
}

stress_main!();
