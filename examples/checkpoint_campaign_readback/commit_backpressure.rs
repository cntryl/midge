//! Independent cumulative-counter contracts; JSON cannot prove an executed waiter or ACK.

use super::{number, require, Cell, Result};
use serde_json::{json, Value};

const COUNTERS: [&str; 7] = [
    "attempts",
    "successful_commits",
    "write_stalls",
    "wait_calls",
    "wait_timeouts",
    "wait_write_stalls",
    "wait_elapsed_ns",
];

fn completed_boundary(value: &Value, successes: u64) -> Result<()> {
    for field in COUNTERS {
        number(value, field)?;
    }
    let committed = number(value, "successful_commits")?;
    let stalls = number(value, "write_stalls")?;
    require(
        committed == successes && committed.checked_add(stalls) == Some(number(value, "attempts")?),
        "backpressure attempts/strict-success arithmetic differs",
    )?;
    let wait_errors = number(value, "wait_timeouts")?
        .checked_add(number(value, "wait_write_stalls")?)
        .ok_or("backpressure waiter counter overflow")?;
    let waits = number(value, "wait_calls")?;
    require(
        stalls.checked_add(wait_errors) == Some(waits),
        "backpressure waiter clears differ from rejected commit attempts",
    )?;
    require(
        stalls > 0 || (waits == 0 && number(value, "wait_elapsed_ns")? == 0),
        "backpressure waiter work exists without a rejected commit",
    )?;
    let last = value
        .get("last_stall")
        .ok_or("missing backpressure last_stall")?;
    require(
        if stalls == 0 {
            last.is_null()
        } else {
            last.as_str().is_some_and(|detail| !detail.is_empty())
        },
        "backpressure last-stall diagnostic contradicts counters",
    )
}

fn policy(value: &Value) -> Result<()> {
    require(
        value["schema_version"] == "midge-checkpoint-commit-policy.v1"
            && value["retry_error"] == "write_stall_only",
        "backpressure policy schema/classification differs",
    )?;
    let retry = number(value, "retry_budget_ns")?;
    let slice = number(value, "wait_slice_ns")?;
    require(
        retry > 0
            && retry <= 30_000_000_000
            && slice > 0
            && slice <= 1_000_000_000
            && slice <= retry,
        "backpressure retry/wait allowance exceeds declared bounds",
    )?;
    require(
        number(value, "cell_budget_ns")? == 900_000_000_000
            && number(value, "required_no_progress_timeout_ns")? == 60_000_000_000,
        "backpressure original cell/watchdog budget changed",
    )
}

fn delta(after: &Value, before: &Value) -> Result<Value> {
    let mut result = serde_json::Map::new();
    for field in COUNTERS {
        let difference = number(after, field)?
            .checked_sub(number(before, field)?)
            .ok_or("backpressure cumulative counter decreased")?;
        result.insert(field.into(), json!(difference));
    }
    Ok(Value::Object(result))
}

/// Keep recorded stalls diagnostic. Only strict commit success is an ACK.
pub fn qualify(observations: &Value, status: &Value, window: &Value, cell: Cell) -> Result<Value> {
    policy(&status["commit_backpressure_policy"])?;
    let before = &observations["commit_backpressure_before"];
    let after = &observations["commit_backpressure_after"];
    completed_boundary(before, cell.warmup())?;
    completed_boundary(after, cell.cycles)?;
    require(
        status["commit_backpressure"] == *after,
        "backpressure final status differs from cumulative measured endpoint",
    )?;
    let measured = delta(after, before)?;
    // A completed warmup cycle cannot leave a waiter crossing this boundary.
    require(
        number(&measured, "write_stalls")? > 0
            || (number(&measured, "wait_calls")? == 0
                && number(&measured, "wait_elapsed_ns")? == 0),
        "backpressure measured waiter work exists without a new rejected commit",
    )?;
    require(
        number(&measured, "successful_commits")? == cell.measured(),
        "backpressure measured strict-success delta differs",
    )?;
    let elapsed = number(window, "measured_elapsed_ns")?;
    require(
        elapsed > 0
            && elapsed <= 900_000_000_000
            && number(&measured, "wait_elapsed_ns")? <= elapsed,
        "backpressure recorded waits exceed actual measured ingestion clock",
    )?;
    Ok(json!({"before":before,"after":after,"measured":measured,
        "policy":status["commit_backpressure_policy"],"wait_clear_is_ack":false,
        "executed_wait_body_verified_from_json":false}))
}

/// Bind the required watchdog to the captured native `--print-config` output.
pub fn check_native_no_progress(config: &str) -> Result<()> {
    let lines = config
        .lines()
        .filter_map(|line| line.strip_prefix("No-progress timeout: "))
        .collect::<Vec<_>>();
    require(
        lines.len() == 1 && lines[0].split_whitespace().next() == Some("60s"),
        "captured native no-progress timeout differs from original 60 seconds",
    )
}

#[cfg(test)]
mod tests;
