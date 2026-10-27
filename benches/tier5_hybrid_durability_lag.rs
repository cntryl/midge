//! Sqrzl diagnostic counterparts of the Tier 2 simulator lag rows.
#[path = "bench_support/hybrid_lag.rs"]
mod hybrid_lag;
#[path = "bench_support/stress_scenarios.rs"]
#[allow(dead_code)]
mod stress_scenarios;
use cntryl_stress::{stress, stress_main, StressContext};
use hybrid_lag::CommitMode;

fn provider_case(ctx: &mut StressContext, scenario: &'static str, mode: CommitMode) {
    provider_case_with_writers(ctx, scenario, mode, 1);
}

fn provider_case_with_writers(
    ctx: &mut StressContext,
    scenario: &'static str,
    mode: CommitMode,
    writers: usize,
) {
    let cache = tempfile::tempdir().expect("provider lag cache");
    let options = if mode == CommitMode::LocalSync {
        hybrid_lag::local_floor_options()
    } else {
        let namespace = stress_scenarios::unique_namespace(scenario);
        stress_scenarios::ensure_namespace("s3", &namespace)
            .expect("Sqrzl unavailable or namespace creation failed; diagnostic is inconclusive");
        hybrid_lag::sqrzl_options(cache.path(), &namespace)
    };
    hybrid_lag::run_provider_case_with_writers(ctx, scenario, mode, options, writers);
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_sqrzl_cloud_async_commit_vs_durable_lag_1_client(ctx: &mut StressContext) {
    provider_case(
        ctx,
        "tier5_sqrzl_cloud_async_commit_vs_durable_lag_1_client",
        CommitMode::CloudAsync,
    );
}
#[stress(tier = 5, role = "diagnostic")]
fn tier5_sqrzl_cloud_strict_commit_vs_durable_lag_1_client(ctx: &mut StressContext) {
    provider_case(
        ctx,
        "tier5_sqrzl_cloud_strict_commit_vs_durable_lag_1_client",
        CommitMode::CloudStrict,
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_sqrzl_cloud_strict_commit_vs_durable_lag_4_clients(ctx: &mut StressContext) {
    provider_case_with_writers(
        ctx,
        "tier5_sqrzl_cloud_strict_commit_vs_durable_lag_4_clients",
        CommitMode::CloudStrict,
        4,
    );
}
#[stress(tier = 5, role = "diagnostic")]
fn tier5_sqrzl_local_sync_commit_floor_1_client(ctx: &mut StressContext) {
    provider_case(
        ctx,
        "tier5_sqrzl_local_sync_commit_floor_1_client",
        CommitMode::LocalSync,
    );
}
stress_main!();
