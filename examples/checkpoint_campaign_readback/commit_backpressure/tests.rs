//! Constructed reader contracts only; no real ACK, provider or waiter execution.

use super::{check_native_no_progress, qualify};
use crate::checkpoint_campaign_readback::CELLS;
use serde_json::{json, Value};

fn counters(successful: u64, stalls: u64, timeouts: u64, wait_stalls: u64, elapsed: u64) -> Value {
    json!({"attempts":successful+stalls,"successful_commits":successful,"write_stalls":stalls,
        "wait_calls":stalls+timeouts+wait_stalls,"wait_timeouts":timeouts,
        "wait_write_stalls":wait_stalls,"wait_elapsed_ns":elapsed,
        "last_stall":if stalls==0 {Value::Null} else {json!("L0 admission backpressure")}})
}

fn records() -> (Value, Value, Value) {
    let before = counters(26, 2, 1, 0, 2_000_000_000);
    let after = counters(256, 5, 3, 1, 5_000_000_000);
    let status = json!({"commit_backpressure":after,
        "commit_backpressure_policy":{"schema_version":"midge-checkpoint-commit-policy.v1",
        "retry_error":"write_stall_only","retry_budget_ns":30_000_000_000_u64,
        "wait_slice_ns":1_000_000_000_u64,"cell_budget_ns":900_000_000_000_u64,
        "required_no_progress_timeout_ns":60_000_000_000_u64}});
    (
        json!({"commit_backpressure_before":before,"commit_backpressure_after":after}),
        status,
        json!({"measured_elapsed_ns":6_000_000_000_u64}),
    )
}

#[test]
fn should_retain_stall_diagnostics_when_constructed_strict_successes_match_fixed_cycles() {
    // Arrange: these are cumulative records with real-execution claims deliberately absent.
    let (observations, status, window) = records();
    // Act.
    let result = qualify(&observations, &status, &window, CELLS[0]).unwrap();
    // Assert: waiter results never add an ACK or publication denominator.
    assert_eq!(result["measured"]["successful_commits"], 230);
    assert_eq!(result["measured"]["write_stalls"], 3);
    assert_eq!(result["wait_clear_is_ack"], false);
    assert_eq!(result["executed_wait_body_verified_from_json"], false);
}

#[test]
fn should_reject_missing_or_altered_attempts_when_constructed_backpressure_endpoint_is_checked() {
    // Arrange.
    let (mut observations, mut status, window) = records();
    observations["commit_backpressure_after"]
        .as_object_mut()
        .unwrap()
        .remove("attempts");
    // Act/Assert: a missing field is not an implicit zero.
    assert!(qualify(&observations, &status, &window, CELLS[0]).is_err());
    // Arrange: retain every field but fabricate one additional attempt.
    observations["commit_backpressure_after"]["attempts"] = json!(262);
    status["commit_backpressure"] = observations["commit_backpressure_after"].clone();
    // Act/Assert.
    assert!(qualify(&observations, &status, &window, CELLS[0]).is_err());
}

#[test]
fn should_reject_warmup_counter_reset_when_constructed_measured_endpoint_appears_complete() {
    // Arrange: after is plausible but before improperly discarded all warmup successes.
    let (mut observations, status, window) = records();
    observations["commit_backpressure_before"] = counters(0, 0, 0, 0, 0);
    // Act/Assert.
    assert!(qualify(&observations, &status, &window, CELLS[0]).is_err());
}

#[test]
fn should_reject_waiter_clear_as_ack_when_constructed_commit_counters_are_incomplete() {
    // Arrange: the waiter permission was incorrectly used in place of a strict commit.
    let (mut observations, mut status, window) = records();
    observations["commit_backpressure_after"] = counters(255, 6, 3, 1, 5_000_000_000);
    status["commit_backpressure"] = observations["commit_backpressure_after"].clone();
    // Act/Assert: fixed cell requires 256 genuine strict successes, not 256 cleared waits.
    assert!(qualify(&observations, &status, &window, CELLS[0]).is_err());
}

#[test]
fn should_reject_excluded_wait_time_when_constructed_measured_clock_is_shorter_than_wait_delta() {
    // Arrange: recorded wait delta is 3 seconds, but supplied actual window is shorter.
    let (observations, status, mut window) = records();
    window["measured_elapsed_ns"] = json!(2_999_999_999_u64);
    // Act/Assert: this coarse check cannot prove each executed wait body.
    assert!(qualify(&observations, &status, &window, CELLS[0]).is_err());
}

#[test]
fn should_reject_changed_budget_or_native_watchdog_when_constructed_policy_is_checked() {
    // Arrange.
    let (observations, mut status, window) = records();
    status["commit_backpressure_policy"]["retry_budget_ns"] = json!(30_000_000_001_u64);
    // Act/Assert.
    assert!(qualify(&observations, &status, &window, CELLS[0]).is_err());
    assert!(
        check_native_no_progress("No-progress timeout: 61s (cli --no-progress-timeout-secs)")
            .is_err()
    );
    assert!(check_native_no_progress(
        "No-progress timeout: 60s (env STRESS_NO_PROGRESS_TIMEOUT_SECS)"
    )
    .is_ok());
}

#[test]
fn should_reject_waiter_calls_when_constructed_cumulative_endpoint_has_no_commit_rejections() {
    // Arrange: no commit was rejected, but a waiter-route rejection is fabricated.
    // These are JSON contracts; no executed waiter or real ACK is claimed.
    let (mut observations, mut status, window) = records();
    observations["commit_backpressure_before"] = counters(26, 0, 0, 0, 0);
    observations["commit_backpressure_after"] = counters(256, 0, 0, 1, 0);
    status["commit_backpressure"] = observations["commit_backpressure_after"].clone();
    // Act.
    let result = qualify(&observations, &status, &window, CELLS[0]);
    // Assert: the helper cannot enter any waiter without a rejected commit.
    assert!(result.is_err());
}

#[test]
fn should_reject_waiter_calls_when_constructed_measured_delta_has_no_new_commit_rejections() {
    // Arrange: both cumulative endpoints are coherent, but the measured delta
    // adds a waiter timeout without a new commit rejection after completed warmup.
    let (mut observations, mut status, window) = records();
    observations["commit_backpressure_before"] = counters(26, 2, 1, 0, 2_000_000_000);
    observations["commit_backpressure_after"] = counters(256, 2, 2, 0, 3_000_000_000);
    status["commit_backpressure"] = observations["commit_backpressure_after"].clone();
    // Act.
    let result = qualify(&observations, &status, &window, CELLS[0]);
    // Assert: no waiter is retained across this completed-cycle measurement boundary.
    assert!(result.is_err());
}
