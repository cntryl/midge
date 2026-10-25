//! Simulator gate rows share the lag workload with the Sqrzl diagnostics.
#[path = "bench_support/hybrid_lag.rs"]
mod hybrid_lag;
use cntryl_stress::{stress, stress_main, StressContext};
use hybrid_lag::{run_case, CommitMode};

#[stress(tier = 2)]
fn tier2_hybrid_cloud_async_commit_vs_durable_lag_1_client(ctx: &mut StressContext) {
    run_case(
        ctx,
        "tier2_hybrid_cloud_async_commit_vs_durable_lag_1_client",
        CommitMode::CloudAsync,
    );
}

#[stress(tier = 2)]
fn tier2_hybrid_cloud_strict_commit_vs_durable_lag_1_client(ctx: &mut StressContext) {
    run_case(
        ctx,
        "tier2_hybrid_cloud_strict_commit_vs_durable_lag_1_client",
        CommitMode::CloudStrict,
    );
}

#[stress(tier = 2)]
fn tier2_hybrid_local_sync_commit_floor_1_client(ctx: &mut StressContext) {
    run_case(
        ctx,
        "tier2_hybrid_local_sync_commit_floor_1_client",
        CommitMode::LocalSync,
    );
}

stress_main!();
