//! Tier 4 - Engine Compression Policy Comparison
//!
//! Fixed-work local workloads compare the actual policies derived from
//! `Goal::{Latency, Throughput, Economy}`. Each workload performs four
//! explicit flushes, a completed compaction, and a clean shutdown. Phase
//! timings and the final SST footprint are recorded separately.

use cntryl_midge::{
    Engine, Goal, OpenOptions, RecoveryPolicy, TransactionMode, WorkloadProfile, WriteOptions,
};
use cntryl_stress::{stress, stress_main, StressContext};
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

const FLUSHES: usize = 4;
const RECORDS_PER_FLUSH: usize = 128;
const VALUE_SIZE: usize = 16 * 1024;
const LOGICAL_BYTES: usize = FLUSHES * RECORDS_PER_FLUSH * VALUE_SIZE;

#[derive(Clone, Copy)]
enum RecordShape {
    Repeated,
    Structured,
    Mixed,
    PrefixRandomTail,
    LowCardinality,
}

impl RecordShape {
    const fn name(self) -> &'static str {
        match self {
            Self::Repeated => "repeated",
            Self::Structured => "structured",
            Self::Mixed => "mixed",
            Self::PrefixRandomTail => "prefix_random_tail",
            Self::LowCardinality => "low_cardinality",
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
        RecordShape::Repeated => {
            let byte = b'A' + u8::try_from(ordinal % 23).expect("ordinal fits in u8");
            vec![byte; VALUE_SIZE]
        }
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
        RecordShape::PrefixRandomTail => {
            let prefix_len = VALUE_SIZE * 3 / 4;
            let mut value = structured_value(prefix_len, ordinal);
            value.extend(lcg_bytes(
                VALUE_SIZE - prefix_len,
                0x51a7_1e5d ^ u32::try_from(ordinal).expect("ordinal fits in u32"),
            ));
            value
        }
        RecordShape::LowCardinality => {
            let symbols = [
                b'A' + u8::try_from(ordinal % 13).expect("ordinal fits in u8"),
                b'a' + u8::try_from(ordinal % 17).expect("ordinal fits in u8"),
                b'0' + u8::try_from(ordinal % 10).expect("ordinal fits in u8"),
                b'|',
            ];
            lcg_bytes(
                VALUE_SIZE,
                0xa11c_e5ed ^ u32::try_from(ordinal).expect("ordinal fits in u32"),
            )
            .into_iter()
            .map(|byte| symbols[usize::from(byte) % symbols.len()])
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

fn run_workload(ctx: &mut StressContext, shape: RecordShape, goal: Goal) {
    let outcome = execute_workload(shape, goal);
    let completed = u64::try_from(LOGICAL_BYTES).expect("logical bytes fit in u64");
    ctx.parameter("record_shape", shape.name());
    ctx.parameter("engine_goal_policy", goal_name(goal));
    ctx.parameter("record_count", FLUSHES * RECORDS_PER_FLUSH);
    ctx.parameter("record_value_bytes", VALUE_SIZE);
    ctx.parameter("flush_count", FLUSHES);
    ctx.parameter("compaction_completed", true);
    ctx.parameter("clean_shutdown_completed", true);
    ctx.parameter("logical_bytes", LOGICAL_BYTES);
    ctx.parameter("logical_unit", "record_value_byte");
    ctx.parameter("ingest_ns", outcome.ingest.as_nanos());
    ctx.parameter("flush_compaction_ns", outcome.flush_compaction.as_nanos());
    ctx.parameter("total_ns", outcome.total.as_nanos());
    ctx.parameter("final_sst_bytes", outcome.final_sst_bytes);
    ctx.record_external(
        format!("engine_policy_{}_ingest_{}", goal_name(goal), shape.name()),
        outcome.ingest,
        completed,
    );
    ctx.record_external(
        format!(
            "engine_policy_{}_flush_compaction_{}",
            goal_name(goal),
            shape.name()
        ),
        outcome.flush_compaction,
        completed,
    );
    ctx.record_external(
        format!("engine_policy_{}_total_{}", goal_name(goal), shape.name()),
        outcome.total,
        completed,
    );
}

macro_rules! engine_policy_case {
    ($fn_name:ident, $goal:ident, $shape:ident) => {
        #[stress(
            tier = 4,
            metadata(
                component = "engine_compression_policy",
                scenario = "four_flushes_and_compaction"
            )
        )]
        fn $fn_name(ctx: &mut StressContext) {
            run_workload(ctx, RecordShape::$shape, Goal::$goal);
        }
    };
}

engine_policy_case!(latency_policy_repeated, Latency, Repeated);
engine_policy_case!(latency_policy_structured, Latency, Structured);
engine_policy_case!(latency_policy_mixed, Latency, Mixed);
engine_policy_case!(latency_policy_prefix_random_tail, Latency, PrefixRandomTail);
engine_policy_case!(latency_policy_low_cardinality, Latency, LowCardinality);
engine_policy_case!(throughput_policy_repeated, Throughput, Repeated);
engine_policy_case!(throughput_policy_structured, Throughput, Structured);
engine_policy_case!(throughput_policy_mixed, Throughput, Mixed);
engine_policy_case!(
    throughput_policy_prefix_random_tail,
    Throughput,
    PrefixRandomTail
);
engine_policy_case!(
    throughput_policy_low_cardinality,
    Throughput,
    LowCardinality
);
engine_policy_case!(economy_policy_repeated, Economy, Repeated);
engine_policy_case!(economy_policy_structured, Economy, Structured);
engine_policy_case!(economy_policy_mixed, Economy, Mixed);
engine_policy_case!(economy_policy_prefix_random_tail, Economy, PrefixRandomTail);
engine_policy_case!(economy_policy_low_cardinality, Economy, LowCardinality);

stress_main!();
