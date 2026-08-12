//! Tier 4 - Engine Compression Policy Comparison
//!
//! Fixed-work local workloads compare the actual policies derived from
//! `Goal::{Latency, Throughput, Economy}`. Each workload performs four
//! explicit flushes, a completed compaction, and a clean shutdown. Phase
//! timings and the final SST footprint are recorded separately.

use cntryl_midge::{
    Engine, Goal, OpenOptions, RecoveryPolicy, TransactionMode, WorkloadProfile, WriteOptions,
};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

const FLUSHES: usize = 16;
const RECORDS_PER_FLUSH: usize = 512;
const VALUE_SIZE: usize = 16 * 1024;
const LOGICAL_BYTES: usize = FLUSHES * RECORDS_PER_FLUSH * VALUE_SIZE;

#[derive(Clone, Copy)]
enum RecordShape {
    Structured,
    Mixed,
}

impl RecordShape {
    const fn name(self) -> &'static str {
        match self {
            Self::Structured => "structured",
            Self::Mixed => "mixed",
        }
    }
}

struct WorkloadOutcome {
    ingest: Duration,
    flush_compaction: Duration,
    total: Duration,
    final_sst_bytes: u64,
}

fn lcg_bytes(size: usize, seed: u32) -> Vec<u8> {
    let mut state = seed;
    (0..size)
        .map(|_| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            u8::try_from(state >> 24).expect("shifted LCG byte fits in u8")
        })
        .collect()
}

fn structured_value(size: usize, ordinal: usize) -> Vec<u8> {
    let pattern = format!(
        "account={ordinal:04}|region={}|status=active|segment={}|",
        ["east", "west", "north", "south"][ordinal % 4],
        ["consumer", "business", "public"][ordinal % 3],
    );
    pattern
        .as_bytes()
        .iter()
        .copied()
        .cycle()
        .take(size)
        .collect()
}

fn record_value(shape: RecordShape, ordinal: usize) -> Vec<u8> {
    match shape {
        RecordShape::Structured => structured_value(VALUE_SIZE, ordinal),
        RecordShape::Mixed => {
            let structured = structured_value(VALUE_SIZE, ordinal);
            let random = lcg_bytes(
                VALUE_SIZE,
                0xc0ff_ee31 ^ u32::try_from(ordinal).expect("ordinal fits in u32"),
            );
            structured
                .into_iter()
                .zip(random)
                .enumerate()
                .map(|(index, (structured_byte, random_byte))| {
                    if index.is_multiple_of(4) {
                        random_byte
                    } else {
                        structured_byte
                    }
                })
                .collect()
        }
    }
}

fn records(shape: RecordShape) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..FLUSHES * RECORDS_PER_FLUSH)
        .map(|ordinal| {
            (
                format!("policy:{:04}:{ordinal:08}", shape.name()).into_bytes(),
                record_value(shape, ordinal),
            )
        })
        .collect()
}

fn final_sst_bytes(path: &Path) -> u64 {
    fs::read_dir(path.join("sst"))
        .expect("read final SST directory")
        .map(|entry| {
            let entry = entry.expect("read final SST entry");
            if entry
                .file_type()
                .expect("read final SST file type")
                .is_file()
                && entry.path().extension().is_some_and(|ext| ext == "sst")
            {
                entry.metadata().expect("read final SST metadata").len()
            } else {
                0
            }
        })
        .sum()
}

fn goal_name(goal: Goal) -> &'static str {
    match goal {
        Goal::Latency => "latency_lz4",
        Goal::Throughput => "throughput_adaptive",
        Goal::Economy => "economy_zstd9",
    }
}

fn execute_workload(shape: RecordShape, goal: Goal) -> WorkloadOutcome {
    let temp = tempfile::tempdir().expect("create compression-policy benchmark database");
    let records = records(shape);
    let total_started = Instant::now();
    let mut engine = Engine::open(
        OpenOptions::local(temp.path())
            .goal(goal)
            .workload(WorkloadProfile::WriteHeavy)
            .recovery_policy(RecoveryPolicy::Strict)
            .background_compaction(false)
            .build()
            .expect("build compression-policy benchmark options"),
    )
    .expect("open compression-policy benchmark database");
    let column_family = engine
        .create_column_family("compression-policy")
        .expect("create compression-policy benchmark column family");

    let mut ingest = Duration::ZERO;
    let mut flush_compaction = Duration::ZERO;
    for batch in records.chunks_exact(RECORDS_PER_FLUSH) {
        let ingest_started = Instant::now();
        let mut transaction = engine
            .begin_tx(column_family.id(), TransactionMode::ReadWrite)
            .expect("begin compression-policy benchmark transaction");
        for (key, value) in batch {
            transaction
                .put(key.clone(), value.clone(), None)
                .expect("write compression-policy benchmark record");
        }
        transaction
            .commit(WriteOptions::buffered())
            .expect("commit compression-policy benchmark records");
        ingest += ingest_started.elapsed();

        let flush_started = Instant::now();
        engine
            .flush_cf(&column_family)
            .expect("flush compression-policy benchmark batch");
        flush_compaction += flush_started.elapsed();
    }

    let compaction_started = Instant::now();
    engine
        .compact_all()
        .expect("complete compression-policy benchmark compaction");
    flush_compaction += compaction_started.elapsed();
    engine
        .shutdown(Duration::from_secs(30))
        .expect("cleanly shut down compression-policy benchmark database");
    let total = total_started.elapsed();

    WorkloadOutcome {
        ingest,
        flush_compaction,
        total,
        final_sst_bytes: final_sst_bytes(temp.path()),
    }
}

#[allow(clippy::cast_precision_loss)]
fn run_workload(ctx: &mut StressContext, shape: RecordShape, goal: Goal) {
    let outcome = execute_workload(shape, goal);
    let completed = u64::try_from(LOGICAL_BYTES).expect("logical bytes fit in u64");
    ctx.parameter("record_shape", shape.name());
    ctx.parameter("engine_goal_policy", goal_name(goal));
    ctx.parameter("record_count", FLUSHES * RECORDS_PER_FLUSH);
    ctx.parameter("record_value_bytes", VALUE_SIZE);
    ctx.parameter("flush_count", FLUSHES);
    ctx.parameter("logical_bytes", LOGICAL_BYTES);
    assert!(
        outcome.final_sst_bytes > 0,
        "compression workload must leave a non-empty SST footprint"
    );
    ctx.record_external_outcome(
        format!("engine_policy_{}_total_{}", goal_name(goal), shape.name()),
        outcome.total,
        LogicalUnit::new("record_value_byte"),
        OperationOutcome::success(completed),
    );
    ctx.record_observation(
        "ingest_ns",
        outcome.ingest.as_nanos() as f64,
        ObservationUnit::Nanoseconds,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "flush_compaction_ns",
        outcome.flush_compaction.as_nanos() as f64,
        ObservationUnit::Nanoseconds,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "final_sst_bytes",
        outcome.final_sst_bytes as f64,
        ObservationUnit::Bytes,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "compression_ratio",
        completed as f64 / outcome.final_sst_bytes as f64,
        ObservationUnit::Ratio,
        ObservationDirection::HigherIsBetter,
    );
}

macro_rules! engine_policy_case {
    ($fn_name:ident, $goal:ident, $shape:ident) => {
        #[stress(
            tier = 4,
            metadata(
                component = "engine_compression_policy",
                scenario = "flushes_and_compaction",
                measurement_shape = "fixed_workload"
            )
        )]
        fn $fn_name(ctx: &mut StressContext) {
            run_workload(ctx, RecordShape::$shape, Goal::$goal);
        }
    };
}

engine_policy_case!(latency_policy_structured, Latency, Structured);
engine_policy_case!(latency_policy_mixed, Latency, Mixed);
engine_policy_case!(throughput_policy_structured, Throughput, Structured);
engine_policy_case!(throughput_policy_mixed, Throughput, Mixed);
engine_policy_case!(economy_policy_structured, Economy, Structured);
engine_policy_case!(economy_policy_mixed, Economy, Mixed);

stress_main!();
