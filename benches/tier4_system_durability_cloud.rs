//! Tier 4 - Cloud durability semantics scenarios (stress harness)
//!
//! Cloud runs are dominated by network/object-store latency and are inherently
//! slower/less deterministic than local-only durability. Keeping these in Tier 4
//! avoids making Tier 3 runs long-running.

#[path = "./stress_config.rs"]
mod stress_config;

use std::time::{Duration, Instant};

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use cntryl_midge::testkit::{ycsb, MidgeOptions};
use cntryl_midge::WriteOptions;
use hdrhistogram::Histogram;

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 128;
const CLOUD_ASYNC_MIN_COMMIT_P99_US: u64 = 5_000;

#[derive(Clone, Copy, Debug)]
enum CloudDurabilityGuardrail {
    BufferedAsync { local_buffered_p99_us: u64 },
    SyncSeal,
    StrictAck,
}

#[derive(Clone, Copy, Debug, Default)]
struct CommitLatencyStats {
    samples: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

impl CommitLatencyStats {
    fn from_histogram(histogram: &Histogram<u64>) -> Self {
        let samples = histogram.len();
        if samples == 0 {
            return Self::default();
        }

        Self {
            samples,
            p50_us: histogram.value_at_quantile(0.50),
            p95_us: histogram.value_at_quantile(0.95),
            p99_us: histogram.value_at_quantile(0.99),
            max_us: histogram.max(),
        }
    }
}

fn duration_to_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn run_commit_latency_workload(
    engine: &cntryl_midge::Engine,
    cf_id: cntryl_midge::ColumnFamilyId,
    num_ops: usize,
    write_opts: WriteOptions,
) -> Histogram<u64> {
    let mut commit_latency_us = Histogram::<u64>::new(3).expect("create commit latency histogram");

    for i in 0..num_ops {
        let k = cntryl_midge::testkit::stress::key16_u64_be(
            u64::try_from(i).expect("operation index fits in u64"),
        );
        let value_byte = u8::try_from(i % 251).expect("value byte fits in u8");
        let v = vec![value_byte; VALUE_SIZE];
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(k.to_vec(), v, None).unwrap();

        let started_at = Instant::now();
        tx.commit(write_opts).unwrap();
        commit_latency_us
            .record(duration_to_micros(started_at.elapsed()))
            .expect("record commit latency");
    }

    commit_latency_us
}

fn measure_unreported_local_buffered_p99(num_ops: usize) -> u64 {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let latency = run_commit_latency_workload(&engine, cf.id(), num_ops, WriteOptions::buffered());
    CommitLatencyStats::from_histogram(&latency).p99_us
}

fn tag_commit_latency(ctx: &mut StressContext, stats: CommitLatencyStats) {
    ctx.tag("commit_samples", stats.samples.to_string());
    ctx.tag("commit_p50_us", stats.p50_us.to_string());
    ctx.tag("commit_p95_us", stats.p95_us.to_string());
    ctx.tag("commit_p99_us", stats.p99_us.to_string());
    ctx.tag("commit_max_us", stats.max_us.to_string());
}

fn tag_runtime_cloud_health(ctx: &mut StressContext, report: ycsb::RuntimePerfReport) {
    for (name, value) in report.tags() {
        ctx.tag(name, value.to_string());
    }

    let cloud_lag = report
        .end_wal_local_durable_seq
        .saturating_sub(report.end_wal_cloud_durable_seq);
    ctx.tag(
        "cloud_async_wal_uploads_failed",
        report.cloud_async_wal_uploads_failed.to_string(),
    );
    ctx.tag(
        "write_stalls_cloud",
        report.write_stalls_cloud_total.to_string(),
    );
    ctx.tag("wal_cloud_durable_lag_end", cloud_lag.to_string());
    ctx.tag(
        "pending_cloud_uploads_end",
        report.end_pending_cloud_uploads.to_string(),
    );
}

fn assert_cloud_guardrails(
    guardrail: CloudDurabilityGuardrail,
    stats: CommitLatencyStats,
    report: ycsb::RuntimePerfReport,
) {
    assert_eq!(
        report.cloud_async_wal_uploads_failed, 0,
        "cloud WAL uploads failed during durability benchmark"
    );

    match guardrail {
        CloudDurabilityGuardrail::BufferedAsync {
            local_buffered_p99_us,
        } => {
            assert_eq!(
                report.write_stalls_cloud_total, 0,
                "default async cloud durability benchmark should not cloud-stall writes"
            );
            let max_allowed_p99 = local_buffered_p99_us
                .saturating_mul(2)
                .max(CLOUD_ASYNC_MIN_COMMIT_P99_US);
            assert!(
                stats.p99_us <= max_allowed_p99,
                "cloud async commit p99 exceeded guardrail: cloud={}us local={}us allowed={}us",
                stats.p99_us,
                local_buffered_p99_us,
                max_allowed_p99
            );
        }
        CloudDurabilityGuardrail::SyncSeal => {}
        CloudDurabilityGuardrail::StrictAck => {
            let cloud_lag = report
                .end_wal_local_durable_seq
                .saturating_sub(report.end_wal_cloud_durable_seq);
            assert_eq!(
                cloud_lag, 0,
                "strict cloud durability benchmark ended with WAL cloud durable lag"
            );
        }
    }
}

fn run_durability_puts_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    num_ops: usize,
    mode_name: &str,
    write_opts: WriteOptions,
    guardrail: CloudDurabilityGuardrail,
) {
    ctx.tag("durability_mode", mode_name);
    ctx.set_elements(u64::try_from(num_ops).expect("operation count fits in u64"));
    ctx.set_bytes(u64::try_from(num_ops * (KEY_SIZE + VALUE_SIZE)).expect("byte count fits"));

    if let CloudDurabilityGuardrail::BufferedAsync {
        local_buffered_p99_us,
    } = guardrail
    {
        ctx.tag(
            "local_buffered_commit_p99_us",
            local_buffered_p99_us.to_string(),
        );
    }

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    let perf_start = ycsb::capture_runtime_perf_snapshot(&engine);

    let latency = ctx.measure_ref(&engine, |e| {
        run_commit_latency_workload(e, cf_id, num_ops, write_opts)
    });

    let stats = CommitLatencyStats::from_histogram(&latency);
    assert_eq!(
        stats.samples,
        u64::try_from(num_ops).expect("operation count fits in u64")
    );
    tag_commit_latency(ctx, stats);

    let report = ycsb::runtime_perf_report(&engine, perf_start);
    tag_runtime_cloud_health(ctx, report);
    assert_cloud_guardrails(guardrail, stats, report);

    // Not timed.
    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");
    assert!(tx.get(&[0u8; KEY_SIZE]).is_ok());

    drop(engine);
}

#[stress_test]
fn tier4_durability_async_cloud_1000(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    let local_buffered_p99_us = measure_unreported_local_buffered_p99(1_000);
    run_durability_puts_case(
        ctx,
        opts,
        1_000,
        "cloud_buffered_async",
        WriteOptions::buffered(),
        CloudDurabilityGuardrail::BufferedAsync {
            local_buffered_p99_us,
        },
    );
}

#[stress_test]
fn tier4_durability_sync_seal_cloud_250(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_durability_puts_case(
        ctx,
        opts,
        250,
        "cloud_sync_seal",
        WriteOptions::sync(),
        CloudDurabilityGuardrail::SyncSeal,
    );
}

#[stress_test]
fn tier4_durability_cloud_strict_100(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_durability_puts_case(
        ctx,
        opts,
        100,
        "cloud_strict_ack",
        WriteOptions::cloud_strict(),
        CloudDurabilityGuardrail::StrictAck,
    );
}

stress_main!();
