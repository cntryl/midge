//! Tier 3 — Engine Reopen Cost (single primitive operation)
//!
//! Measures: latency of opening a clean engine (no recovery needed)
//! NOT: recovery workflows, replay costs, or state-dependent reopen (Tier 4)
//!
//! Not meaningful for pure memory; only local and cloud.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::MidgeOptions;

fn setup_engine(opts: MidgeOptions) -> cntryl_midge::MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn run_reopen_clean_case(ctx: &mut StressContext, opts: MidgeOptions) {
    // Setup (not measured): create initial metadata, then close.
    {
        let engine = setup_engine(opts.clone());
        drop(engine);
    }

    ctx.set_elements(100); // expensive (disk I/O per iteration)

    // Measure ONLY one reopen call of a clean engine
    ctx.measure(|| {
        let engine = setup_engine(opts.clone());
        drop(engine);
    });
}

// TIER 4 ONLY: reopen after flush/compaction
// Moved to tier4_system_recovery_throughput.rs
//
// Reason: Recovery after lifecycle events (flush, compaction) depends on:
// - Manifest state
// - WAL content
// - SST file structure
// These are SYSTEM BEHAVIOR patterns, not primitive costs. Tier 4 measures
// multi-stage recovery, replay throughput, and degradation under complex states.

#[stress_test]
fn tier3_recovery_reopen_clean_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_reopen_clean_case(ctx, opts);
}

#[stress_test]
fn tier3_recovery_reopen_clean_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_reopen_clean_case(ctx, opts);
}

stress_main!();
