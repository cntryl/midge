//! Tier 4 - Complete local Strict group commit
//!
//! Fixed-work concurrent strict commits followed by multiple flushes,
//! compaction, clean shutdown, reopen, point/scan digest verification, and
//! final SST footprint capture.

use cntryl_midge::{Engine, OpenOptions, Query, RecoveryPolicy, TransactionMode, WriteOptions};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

const WRITERS: usize = 16;
const FLUSH_WAVES: usize = 8;
const TRANSACTIONS_PER_WRITER_PER_WAVE: usize = 256;
const VALUE_SIZE: usize = 256;
const MEMTABLE_SIZE: usize = 128 * 1024;
const TOTAL_TRANSACTIONS: usize = WRITERS * FLUSH_WAVES * TRANSACTIONS_PER_WRITER_PER_WAVE;

struct SystemOutcome {
    strict_ingest: Duration,
    total: Duration,
    completed: u64,
    wal_appends: u64,
    physical_fsyncs: u64,
    final_sst_count: usize,
    final_sst_bytes: u64,
}

fn record(ordinal: usize) -> (Vec<u8>, Vec<u8>) {
    let key = format!("strict-system:{ordinal:08}").into_bytes();
    let value = (0..VALUE_SIZE)
        .map(|index| {
            u8::try_from((ordinal.wrapping_mul(31) + index.wrapping_mul(17)) % 251)
                .expect("value byte fits in u8")
        })
        .collect();
    (key, value)
}

fn update_digest(digest: &mut crc32fast::Hasher, key: &[u8], value: &[u8]) {
    digest.update(
        &u64::try_from(key.len())
            .expect("key length fits in u64")
            .to_be_bytes(),
    );
    digest.update(key);
    digest.update(
        &u64::try_from(value.len())
            .expect("value length fits in u64")
            .to_be_bytes(),
    );
    digest.update(value);
}

fn expected_digest() -> u32 {
    let mut digest = crc32fast::Hasher::new();
    for ordinal in 0..TOTAL_TRANSACTIONS {
        let (key, value) = record(ordinal);
        update_digest(&mut digest, &key, &value);
    }
    digest.finalize()
}

fn run_wave(engine: &Arc<Engine>, cf_id: cntryl_midge::ColumnFamilyId, wave: usize) {
    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let mut handles = Vec::with_capacity(WRITERS);
    for writer in 0..WRITERS {
        let engine = Arc::clone(engine);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            for index in 0..TRANSACTIONS_PER_WRITER_PER_WAVE {
                let ordinal = wave * WRITERS * TRANSACTIONS_PER_WRITER_PER_WAVE
                    + writer * TRANSACTIONS_PER_WRITER_PER_WAVE
                    + index;
                let (key, value) = record(ordinal);
                loop {
                    let mut transaction = engine
                        .begin_tx(cf_id, TransactionMode::ReadWrite)
                        .expect("begin strict system transaction");
                    transaction
                        .put(key.clone(), value.clone(), None)
                        .expect("stage strict system value");
                    match transaction.commit(WriteOptions::sync()) {
                        Ok(()) => break,
                        Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                            assert!(
                                engine
                                    .wait_for_write_stall_clear(cf_id, Duration::from_secs(5))
                                    .expect("wait for strict system write stall"),
                                "strict system write stall did not clear"
                            );
                        }
                        Err(error) => panic!("commit strict system transaction: {error}"),
                    }
                }
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("join strict system writer");
    }
}

fn sst_footprint(path: &Path) -> (usize, u64) {
    fs::read_dir(path.join("sst"))
        .expect("read final SST directory")
        .filter_map(|entry| {
            let entry = entry.expect("read final SST entry");
            (entry
                .file_type()
                .expect("read final SST file type")
                .is_file()
                && entry.path().extension().is_some_and(|ext| ext == "sst"))
            .then(|| entry.metadata().expect("read final SST metadata").len())
        })
        .fold((0, 0), |(count, bytes), size| {
            (count + 1, bytes.saturating_add(size))
        })
}

fn verify_reopened(engine: &Engine, expected: u32) {
    let cf = engine
        .get_column_family("default")
        .expect("reopened default column family");
    let point_read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin point verification transaction");
    let mut point_digest = crc32fast::Hasher::new();
    for ordinal in 0..TOTAL_TRANSACTIONS {
        let (key, expected_value) = record(ordinal);
        let value = point_read
            .get(&key)
            .expect("read strict system point")
            .expect("strict system point must exist");
        assert_eq!(value.as_ref(), expected_value.as_slice());
        update_digest(&mut point_digest, &key, &value);
    }
    assert_eq!(point_digest.finalize(), expected);
    drop(point_read);

    let scan_read = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("begin scan verification transaction");
    let mut scan_digest = crc32fast::Hasher::new();
    let mut rows = 0usize;
    for row in scan_read
        .scan(&Query::new())
        .expect("scan reopened strict system database")
    {
        let (key, value) = row.expect("read strict system scan row");
        update_digest(&mut scan_digest, &key, &value);
        rows += 1;
    }
    assert_eq!(rows, TOTAL_TRANSACTIONS);
    assert_eq!(scan_digest.finalize(), expected);
}

fn execute_system_workload() -> SystemOutcome {
    let temp = tempfile::tempdir().expect("create strict system benchmark database");
    let options = OpenOptions::local(temp.path())
        .recovery_policy(RecoveryPolicy::Strict)
        .background_compaction(false)
        .with_memtable_size_limit(MEMTABLE_SIZE)
        .with_memtable_flush_threshold(MEMTABLE_SIZE)
        .build()
        .expect("build strict system benchmark options");
    stress_config::init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let engine =
        Arc::new(Engine::open(options.clone()).expect("open strict system benchmark database"));
    let cf = engine
        .get_column_family("default")
        .expect("default column family");
    let start_metrics = engine
        .get_runtime_metrics()
        .expect("capture starting strict system metrics");
    let total_started_at = Instant::now();
    let ingest_started_at = Instant::now();
    for wave in 0..FLUSH_WAVES {
        run_wave(&engine, cf.id(), wave);
        engine
            .flush_cf(&cf)
            .expect("flush strict system benchmark wave");
    }
    let strict_ingest = ingest_started_at.elapsed();
    engine
        .compact_all()
        .expect("complete strict system benchmark compaction");
    engine
        .compact_all()
        .expect("complete any compaction exposed by the first pass");
    let end_metrics = engine
        .get_runtime_metrics()
        .expect("capture ending strict system metrics");
    let wal_appends = end_metrics
        .wal_append_count
        .saturating_sub(start_metrics.wal_append_count);
    let physical_fsyncs = end_metrics
        .wal_fsync_count
        .saturating_sub(start_metrics.wal_fsync_count);
    let mut engine =
        Arc::try_unwrap(engine).unwrap_or_else(|_| panic!("all strict system writers must exit"));
    engine
        .shutdown(Duration::from_secs(30))
        .expect("cleanly shut down strict system benchmark database");

    let expected = expected_digest();
    let mut reopened =
        Engine::open(options).expect("reopen strict system benchmark database after compaction");
    verify_reopened(&reopened, expected);
    reopened
        .shutdown(Duration::from_secs(30))
        .expect("cleanly shut down reopened strict system database");
    let total = total_started_at.elapsed();
    let (final_sst_count, final_sst_bytes) = sst_footprint(temp.path());

    SystemOutcome {
        strict_ingest,
        total,
        completed: u64::try_from(TOTAL_TRANSACTIONS)
            .expect("strict system transaction count fits in u64"),
        wal_appends,
        physical_fsyncs,
        final_sst_count,
        final_sst_bytes,
    }
}

#[stress(
    tier = 4,
    metadata(
        component = "strict_group_commit",
        scenario = "complete_local_system",
        measurement_shape = "fixed_workload"
    )
)]
#[allow(clippy::cast_precision_loss)]
fn tier4_complete_local_strict_group_commit(ctx: &mut StressContext) {
    let outcome = execute_system_workload();
    ctx.parameter("writers", WRITERS);
    ctx.parameter("flush_count", FLUSH_WAVES);
    ctx.parameter("transactions", TOTAL_TRANSACTIONS);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("memtable_size_bytes", MEMTABLE_SIZE);
    assert!(
        outcome.wal_appends > 0,
        "strict commits must append to the WAL"
    );
    assert!(
        outcome.physical_fsyncs > 0,
        "strict commits must issue physical fsyncs"
    );
    assert!(
        outcome.final_sst_count > 0 && outcome.final_sst_bytes > 0,
        "compacted strict workload must leave a non-empty SST footprint"
    );
    ctx.record_external_outcome(
        "tier4_complete_local_strict_group_commit_total",
        outcome.total,
        LogicalUnit::new("transaction"),
        OperationOutcome::success(outcome.completed),
    );
    ctx.record_observation(
        "ingest_ns",
        outcome.strict_ingest.as_nanos() as f64,
        ObservationUnit::Nanoseconds,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "wal_appends",
        outcome.wal_appends as f64,
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "physical_fsyncs",
        outcome.physical_fsyncs as f64,
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "commits_per_fsync",
        outcome.completed as f64 / outcome.physical_fsyncs as f64,
        ObservationUnit::Ratio,
        ObservationDirection::HigherIsBetter,
    );
    ctx.record_observation(
        "final_sst_count",
        outcome.final_sst_count as f64,
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "final_sst_bytes",
        outcome.final_sst_bytes as f64,
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );
}

#[path = "./stress_config.rs"]
mod stress_config;

stress_main!();
