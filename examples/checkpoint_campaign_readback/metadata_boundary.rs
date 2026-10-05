//! Endpoint-record consistency only; counters cannot prove executed work or a clock.

use super::{array, number, require, Result};
use serde_json::{json, Value};

fn policy(value: &Value) -> Result<(u64, u64)> {
    require(
        value["schema_version"] == "midge-checkpoint-boundary-policy.v1"
            && value["metadata_scope"] == "persistent_only"
            && value["sample_order"] == "runtime_metrics_then_metadata_snapshot"
            && value["zero_persistent_active_required"] == true
            && value["progress_advanced"] == false
            && value["before_clock_scope"] == "warmup_wait_outside_measured_final_query_inside"
            && value["after_clock_scope"] == "end_inside_measured",
        "metadata boundary policy/order/clock scope differs",
    )?;
    let budget = number(value, "boundary_budget_ns")?;
    let pause = number(value, "pause_slice_ns")?;
    require(
        budget > 0
            && budget <= 30_000_000_000
            && pause > 0
            && pause <= 1_000_000
            && pause <= budget
            && number(value, "cell_budget_ns")? == 900_000_000_000,
        "metadata boundary allowance/pause/original cell budget differs",
    )?;
    Ok((budget, pause))
}

fn persistent_active(snapshot: &Value) -> Result<u64> {
    let buckets = array(snapshot, "buckets")?;
    require(
        buckets.len() == 18,
        "metadata boundary snapshot buckets missing",
    )?;
    let persistent = buckets
        .iter()
        .filter(|bucket| bucket["medium"] == "persistent")
        .collect::<Vec<_>>();
    require(
        persistent.len() == 9,
        "metadata boundary persistent buckets missing",
    )?;
    persistent.into_iter().try_fold(0_u64, |total, bucket| {
        total
            .checked_add(number(bucket, "active_operations")?)
            .ok_or_else(|| "metadata boundary active counter overflow".into())
    })
}

fn observation(value: &Value, snapshot: &Value, budget: u64, pause_slice: u64) -> Result<()> {
    let samples = number(value, "samples")?;
    let active = number(value, "active_samples")?;
    let calls = number(value, "pause_calls")?;
    require(
        active.checked_add(1) == Some(samples) && calls == active,
        "metadata boundary sample/busy/pause arithmetic differs",
    )?;
    let requested = number(value, "pause_requested_ns")?;
    let maximum = number(value, "max_pause_requested_ns")?;
    let observed_wait = number(value, "paused_ns")?;
    let elapsed = number(value, "elapsed_ns")?;
    require(
        elapsed < budget && observed_wait <= elapsed && maximum <= pause_slice,
        "metadata boundary elapsed/pause exceeds captured policy bounds",
    )?;
    require(
        if calls == 0 {
            requested == 0 && maximum == 0 && observed_wait == 0
        } else {
            maximum > 0
                && requested >= maximum
                && u128::from(requested) >= u128::from(calls)
                && u128::from(requested) <= u128::from(calls) * u128::from(maximum)
        },
        "metadata boundary requested-pause accounting differs",
    )?;
    require(
        value["complete"] == true
            && number(value, "final_persistent_active")? == 0
            && persistent_active(snapshot)? == 0,
        "metadata boundary did not retain an idle Persistent snapshot",
    )
}

/// Warmup rejected candidates are excluded; every end capture is measured.
pub fn qualify(
    ingestion: &Value,
    status: &Value,
    window: &Value,
    before_snapshot: &Value,
    after_snapshot: &Value,
) -> Result<Value> {
    let declaration = &status["metadata_boundary_policy"];
    let (budget, pause) = policy(declaration)?;
    let before = &ingestion["metadata_boundary_before"];
    let after = &ingestion["metadata_boundary_after"];
    require(
        status["metadata_boundary_before"] == *before
            && status["metadata_boundary_after"] == *after,
        "metadata boundary status/ingestion capture differs",
    )?;
    require(
        number(before_snapshot, "owner_id")? > 0
            && before_snapshot["owner_id"] == after_snapshot["owner_id"],
        "metadata boundary chosen owner changed",
    )?;
    observation(before, before_snapshot, budget, pause)?;
    observation(after, after_snapshot, budget, pause)?;
    require(
        number(after, "elapsed_ns")? <= number(window, "measured_elapsed_ns")?,
        "metadata end capture was excluded from the measured ingestion clock",
    )?;
    Ok(json!({"policy":declaration,"before":before,"after":after,
        "selected_owner_id":before_snapshot["owner_id"],
        "executed_work_or_clock_verified_from_json":false,"global_runtime_barrier":false}))
}
