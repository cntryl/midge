//! Tier 6 one-hour composite workload soaks.

#[path = "bench_support/stress_scenarios.rs"]
mod stress_scenarios;

use cntryl_stress::{stress, stress_main, StressContext};
use stress_scenarios::{run_case, WorkloadCase};

const SOAK_CLIENTS: [usize; 1] = [8];

#[stress(tier = 6, role = "diagnostic")]
fn tier6_mixed_workload_local(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "mixed-workload-soak-local",
            backend: "local",
            tier: 6,
            workload: "mixed-workload-soak",
            stages: &SOAK_CLIENTS,
        },
    );
}

#[stress(tier = 6, role = "diagnostic")]
fn tier6_mixed_workload_s3(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "mixed-workload-soak-s3",
            backend: "s3",
            tier: 6,
            workload: "mixed-workload-soak",
            stages: &SOAK_CLIENTS,
        },
    );
}

#[stress(tier = 6, role = "diagnostic")]
fn tier6_mixed_workload_azure(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "mixed-workload-soak-azure",
            backend: "azure",
            tier: 6,
            workload: "mixed-workload-soak",
            stages: &SOAK_CLIENTS,
        },
    );
}

#[stress(tier = 6, role = "diagnostic")]
fn tier6_mixed_workload_gcs_xml(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "mixed-workload-soak-gcs-xml",
            backend: "gcs-xml",
            tier: 6,
            workload: "mixed-workload-soak",
            stages: &SOAK_CLIENTS,
        },
    );
}

#[stress(tier = 6, role = "diagnostic")]
fn tier6_mixed_workload_gcs_json(ctx: &mut StressContext) {
    run_case(
        ctx,
        WorkloadCase {
            scenario: "mixed-workload-soak-gcs-json",
            backend: "gcs-json",
            tier: 6,
            workload: "mixed-workload-soak",
            stages: &SOAK_CLIENTS,
        },
    );
}

stress_main!();
