//! Exercise Midge's actual stress worker loop with deterministic clocks.
#![cfg(feature = "stress-soak")]

// This test target imports shared bench support; the full workload entry point
// and provider setup helpers are intentionally unused by these focused tests.
#[allow(dead_code)]
#[path = "../benches/bench_support/stress_scenarios.rs"]
mod stress_scenarios;
