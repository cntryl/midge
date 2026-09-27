//! Tier 2 — Engine read amplification on a deterministic flushed-SST layout.
//!
//! Each sample opens a local Engine with three overlapping L0 SSTs. Fixture
//! construction and metric snapshots are outside the timed read workload.

#[path = "./bench_support/read_amp.rs"]
mod read_amp;

use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};
use read_amp::{
    metrics_delta, run_workload, ReadAmpFixture, ReadMetricsDelta, ReadWorkload,
    ReadWorkloadResult, KEYS_PER_SST, SCAN_WIDTH,
};
use std::time::Instant;

fn as_f64(value: u64) -> f64 {
    f64::from(u32::try_from(value).expect("benchmark counter fits in u32"))
}

fn record_observations(
    ctx: &mut StressContext,
    result: &ReadWorkloadResult,
    delta: &ReadMetricsDelta,
    bloom_checks: u64,
    bloom_rejects: u64,
    candidate_sst_files_checked: u64,
) {
    for (name, value) in [
        ("point_reads", result.point_reads),
        ("point_hits", result.point_hits),
        ("point_misses", result.point_misses),
        ("scans", result.scans),
        ("scan_rows", result.scan_rows),
        ("ssts_touched", delta.ssts_touched),
        ("l0_ssts_touched", delta.l0_ssts_touched),
        ("blocks_read", delta.blocks_read),
        ("cache_hits", delta.cache_hits),
        ("cache_misses", delta.cache_misses),
        ("bloom_checks", bloom_checks),
        ("bloom_rejects", bloom_rejects),
        ("candidate_sst_files_checked", candidate_sst_files_checked),
    ] {
        ctx.record_observation(
            name,
            as_f64(value),
            ObservationUnit::Count,
            ObservationDirection::Informational,
        );
    }
    for (name, value) in [
        (
            "ssts_touched_per_point_read",
            as_f64(delta.ssts_touched) / as_f64(delta.reads),
        ),
        (
            "blocks_read_per_point_read",
            as_f64(delta.blocks_read) / as_f64(delta.reads),
        ),
        (
            "block_cache_hit_rate",
            as_f64(delta.cache_hits) / as_f64(delta.cache_hits + delta.cache_misses),
        ),
    ] {
        ctx.record_observation(
            name,
            value,
            ObservationUnit::Ratio,
            ObservationDirection::Informational,
        );
    }
}

fn run_case(ctx: &mut StressContext, scenario: &'static str, point_reads: usize, scans: usize) {
    let fixture = ReadAmpFixture::new();
    let workload = ReadWorkload::new(point_reads, scans);
    ctx.parameter("storage_profile", "local");
    ctx.parameter("sst_layout", "three_overlapping_l0_ssts");
    ctx.parameter("fixture_keys_per_sst", KEYS_PER_SST);
    ctx.parameter("point_reads", point_reads);
    ctx.parameter("scans", scans);
    ctx.parameter("scan_width", SCAN_WIDTH);
    ctx.parameter("logical_unit", "read_query");
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");

    let read_before = fixture
        .engine
        .metrics()
        .get_read_amp_metrics()
        .expect("capture read amplification baseline");
    let runtime_before = fixture
        .engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture cache baseline");
    let path_before = fixture
        .engine
        .read_path_diagnostics_snapshot_for_benchmarks();
    let started_at = Instant::now();
    let result = run_workload(&fixture, &workload);
    let elapsed = started_at.elapsed();
    let read_after = fixture
        .engine
        .metrics()
        .get_read_amp_metrics()
        .expect("capture read amplification result");
    let runtime_after = fixture
        .engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture cache result");
    let path_after = fixture
        .engine
        .read_path_diagnostics_snapshot_for_benchmarks();
    let delta = metrics_delta(&read_before, &read_after, &runtime_before, &runtime_after);

    assert_eq!(result.point_reads, workload.expected_point_reads());
    assert_eq!(result.scans, workload.expected_scans());
    assert_eq!(result.point_hits + result.point_misses, result.point_reads);
    assert_eq!(result.point_hits, result.point_reads * 3 / 4);
    assert_eq!(result.point_misses, result.point_reads / 4);
    assert_eq!(
        result.scan_rows,
        result.scans * u64::try_from(SCAN_WIDTH).expect("scan width fits in u64")
    );
    assert_eq!(delta.reads, result.point_reads);
    assert!(delta.ssts_touched > 0 && delta.blocks_read > 0);
    assert!(delta.cache_hits > 0 && delta.cache_misses > 0);
    assert!(path_after.bloom_checks > path_before.bloom_checks);
    assert!(path_after.bloom_rejects > path_before.bloom_rejects);

    ctx.record_external_outcome(
        scenario,
        elapsed,
        LogicalUnit::new("read_query"),
        OperationOutcome::success(result.point_reads + result.scans),
    );
    record_observations(
        ctx,
        &result,
        &delta,
        path_after.bloom_checks - path_before.bloom_checks,
        path_after.bloom_rejects - path_before.bloom_rejects,
        path_after.candidate_sst_files_checked - path_before.candidate_sst_files_checked,
    );
}

#[stress(
    tier = 2,
    role = "diagnostic",
    metadata(component = "engine_read_amplification", scenario = "point_hit_miss")
)]
fn point_hit_miss(ctx: &mut StressContext) {
    run_case(ctx, "point_hit_miss", 32_768, 0);
}

#[stress(
    tier = 2,
    role = "diagnostic",
    metadata(
        component = "engine_read_amplification",
        scenario = "mixed_point_short_scan"
    )
)]
fn mixed_point_short_scan(ctx: &mut StressContext) {
    run_case(ctx, "mixed_point_short_scan", 32_768, 1_024);
}

stress_main!();
