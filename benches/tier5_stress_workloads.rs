//! Tier 5 concurrency sweeps for the former Midge Destroyer workloads.

#[path = "bench_support/stress_scenarios.rs"]
mod stress_scenarios;

use cntryl_stress::{stress, stress_main, StressContext};
use stress_scenarios::{run_case, WorkloadCase};

const WRITE_CLIENTS: [usize; 5] = [1, 2, 4, 8, 16];
const READ_CLIENTS: [usize; 4] = [1, 4, 16, 32];
const BALANCED_CLIENTS: [usize; 4] = [1, 4, 8, 16];
const SMALL_TX_CLIENTS: [usize; 4] = [1, 4, 16, 32];
const MIXED_TX_CLIENTS: [usize; 3] = [1, 4, 8];
const CLOUD_CLIENTS: [usize; 3] = [1, 2, 4];

#[stress(tier = 5, role = "diagnostic")]
fn tier5_write_heavy_local(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "write-heavy",
            backend: "local",
            tier: 5,
            workload: "write-heavy",
            stages: &WRITE_CLIENTS,
        },
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_read_heavy_local(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "read-heavy",
            backend: "local",
            tier: 5,
            workload: "read-heavy",
            stages: &READ_CLIENTS,
        },
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_balanced_local(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "balanced",
            backend: "local",
            tier: 5,
            workload: "balanced",
            stages: &BALANCED_CLIENTS,
        },
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_many_small_transactions_local(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "many-small-transactions",
            backend: "local",
            tier: 5,
            workload: "many-small-transactions",
            stages: &SMALL_TX_CLIENTS,
        },
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_mixed_transaction_sizes_local(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "mixed-transaction-sizes",
            backend: "local",
            tier: 5,
            workload: "mixed-transaction-sizes",
            stages: &MIXED_TX_CLIENTS,
        },
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_write_pressure_s3(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "write-pressure-s3",
            backend: "s3",
            tier: 5,
            workload: "sqrzl-write-pressure",
            stages: &CLOUD_CLIENTS,
        },
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_write_pressure_azure(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "write-pressure-azure",
            backend: "azure",
            tier: 5,
            workload: "sqrzl-write-pressure",
            stages: &CLOUD_CLIENTS,
        },
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_write_pressure_gcs_xml(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "write-pressure-gcs-xml",
            backend: "gcs-xml",
            tier: 5,
            workload: "sqrzl-write-pressure",
            stages: &CLOUD_CLIENTS,
        },
    );
}

#[stress(tier = 5, role = "diagnostic")]
fn tier5_write_pressure_gcs_json(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "write-pressure-gcs-json",
            backend: "gcs-json",
            tier: 5,
            workload: "sqrzl-write-pressure",
            stages: &CLOUD_CLIENTS,
        },
    );
}

stress_main!();
