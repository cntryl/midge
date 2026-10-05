//! Independent JSON shape/delta and exact rational accounting gate.

use super::{array, load, number, require, Cell, Result};
use serde_json::{json, Value};
use std::path::Path;

const ORIGINS: [&str; 9] = [
    "ordinary_local_flush",
    "cloud_flush",
    "recovery",
    "bootstrap",
    "ddl",
    "compaction_before_gc",
    "administration",
    "shutdown",
    "unclassified",
];
const COUNTERS: [&str; 19] = [
    "operation_attempts",
    "operation_failures",
    "abandoned_operations",
    "snapshot_durable_bytes",
    "journal_durable_bytes",
    "snapshot_durable_count",
    "checkpoint_complete_count",
    "checkpoint_attempts",
    "checkpoint_elapsed_ns",
    "journal_append_attempts",
    "journal_elapsed_ns",
    "publication_attempts",
    "publication_failures",
    "publication_attempt_elapsed_ns",
    "flush_committed_count",
    "flush_committed_sst_bytes",
    "flush_full_publication_elapsed_ns",
    "compaction_committed_count",
    "compaction_committed_sst_bytes",
];

fn u64_array(value: &Value, expected: usize) -> Result<Vec<u64>> {
    let values = value.as_array().ok_or("numeric array missing")?;
    require(
        values.len() == expected,
        "numeric array size differs from fixed accounting schema",
    )?;
    values
        .iter()
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| "invalid nonnegative integer counter".into())
        })
        .collect()
}

fn shape(snapshot: &Value) -> Result<()> {
    require(
        number(snapshot, "owner_id")? > 0,
        "missing accounting owner",
    )?;
    require(
        snapshot["overflow"] == false
            && number(snapshot, "late_operation_writes")? == 0
            && number(snapshot, "incomplete_observations")? == 0,
        "overflow/escaped/incomplete accounting",
    )?;
    let buckets = array(snapshot, "buckets")?;
    require(
        buckets.len() == 18,
        "accounting must have exactly18 origin/medium buckets",
    )?;
    for (index, bucket) in buckets.iter().enumerate() {
        require(
            bucket["origin"] == ORIGINS[index / 2]
                && bucket["medium"]
                    == if index % 2 == 0 {
                        "persistent"
                    } else {
                        "memory_only"
                    },
            "accounting bucket identity/order differs",
        )?;
        number(bucket, "active_operations")?;
        for field in COUNTERS {
            number(&bucket["counters"], field)?;
        }
        u64_array(&bucket["counters"]["issued_bytes"], 3)?;
        u64_array(&bucket["counters"]["returned_write_bytes"], 3)?;
        u64_array(&bucket["checkpoint_latency"]["counts"], 80)?;
        u64_array(&bucket["full_publication_latency"]["counts"], 80)?;
        u64_array(&bucket["committed_sst_size_log2"], 64)?;
    }
    Ok(())
}

fn subtract(after: &Value, before: &Value) -> Result<Value> {
    match (after, before) {
        (Value::Number(after), Value::Number(before)) => {
            let result = after
                .as_u64()
                .zip(before.as_u64())
                .and_then(|(after, before)| after.checked_sub(before));
            result
                .map(Value::from)
                .ok_or_else(|| "accounting counter decreased or wrong type".into())
        }
        (Value::Array(after), Value::Array(before)) if after.len() == before.len() => after
            .iter()
            .zip(before)
            .map(|(after, before)| subtract(after, before))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        (Value::Object(after), Value::Object(before)) if after.len() == before.len() => {
            let mut result = serde_json::Map::new();
            for (key, value) in after {
                result.insert(
                    key.clone(),
                    subtract(value, before.get(key).ok_or("counter keys differ")?)?,
                );
            }
            Ok(Value::Object(result))
        }
        _ => Err("counter topology/type differs".into()),
    }
}

pub fn delta(after: &Value, before: &Value) -> Result<Value> {
    shape(after)?;
    shape(before)?;
    require(
        after["owner_id"] == before["owner_id"],
        "accounting owners differ",
    )?;
    let mut result = after.clone();
    for index in 0..18 {
        for field in [
            "counters",
            "checkpoint_latency",
            "full_publication_latency",
            "committed_sst_size_log2",
        ] {
            result["buckets"][index][field] = subtract(
                &after["buckets"][index][field],
                &before["buckets"][index][field],
            )?;
        }
    }
    Ok(result)
}

fn sum(values: &Value, size: usize) -> Result<u128> {
    Ok(u64_array(values, size)?.into_iter().map(u128::from).sum())
}

pub fn p95(counts: &Value) -> Result<Option<(u64, u64)>> {
    let counts = u64_array(counts, 80)?;
    let total: u128 = counts.iter().map(|value| u128::from(*value)).sum();
    if total == 0 {
        return Ok(None);
    }
    let rank = (total * 95).div_ceil(100);
    let mut seen = 0_u128;
    for (index, count) in counts.iter().enumerate() {
        seen += u128::from(*count);
        if seen >= rank {
            let upper = |index: usize| {
                if index < 64 {
                    (index as u64 + 1) * 250_000 - 1
                } else {
                    (16_000_000_u64 << (index - 64 + 1)) - 1
                }
            };
            return Ok(Some((
                if index == 0 { 0 } else { upper(index - 1) + 1 },
                upper(index),
            )));
        }
    }
    Err("impossible histogram quantile".into())
}

/// Persistence integrity for a measured snapshot or either final owner.
/// This inspects later forced costs without adding them to measured ratios.
pub fn persistent_integrity(snapshot: &Value) -> Result<()> {
    shape(snapshot)?;
    for bucket in array(snapshot, "buckets")? {
        if bucket["medium"] != "persistent" {
            continue;
        }
        let values = &bucket["counters"];
        require(
            number(bucket, "active_operations")? == 0,
            "persistent metadata remains active at the boundary",
        )?;
        require(
            number(values, "operation_failures")? == 0
                && number(values, "abandoned_operations")? == 0,
            "persistent metadata operation failed/abandoned",
        )?;
        let checkpoints = number(values, "checkpoint_attempts")?;
        require(
            number(values, "checkpoint_complete_count")? == checkpoints
                && number(values, "snapshot_durable_count")? == checkpoints,
            "persistent checkpoint durability/completion differs",
        )?;
        require(
            sum(&bucket["checkpoint_latency"]["counts"], 80)? == u128::from(checkpoints),
            "persistent checkpoint histogram missing attempt",
        )?;
        let flushes = u128::from(number(values, "flush_committed_count")?);
        require(
            sum(&bucket["full_publication_latency"]["counts"], 80)? == flushes,
            "persistent full-publication histogram incomplete",
        )?;
        require(
            sum(&bucket["committed_sst_size_log2"], 64)? == flushes,
            "persistent SST size distribution incomplete",
        )?;
    }
    Ok(())
}

pub fn evaluate(delta: &Value, expected_flushes: u64, compactions: u64) -> Result<Value> {
    shape(delta)?;
    require(compactions > 0, "no genuine measured compaction completion")?;
    persistent_integrity(delta)?;
    for bucket in array(delta, "buckets")? {
        if bucket["origin"] == "unclassified" {
            require(
                number(&bucket["counters"], "operation_attempts")? == 0
                    && number(&bucket["counters"], "flush_committed_count")? == 0,
                "unclassified measured metadata cost",
            )?;
        }
    }
    let bucket = &delta["buckets"][0];
    let counters = &bucket["counters"];
    require(
        number(counters, "flush_committed_count")? == expected_flushes,
        "ordinary committed flush count differs",
    )?;
    require(
        sum(&bucket["committed_sst_size_log2"], 64)? == u128::from(expected_flushes),
        "SST size distribution incomplete",
    )?;
    require(
        sum(&bucket["full_publication_latency"]["counts"], 80)? == u128::from(expected_flushes),
        "full-publication histogram incomplete",
    )?;
    let sst = number(counters, "flush_committed_sst_bytes")?;
    let publication_ns = number(counters, "flush_full_publication_elapsed_ns")?;
    require(sst > 0 && publication_ns > 0, "zero denominator")?;
    let snapshot_bytes = u64_array(&counters["issued_bytes"], 3)?[0];
    let checkpoint_ns = number(counters, "checkpoint_elapsed_ns")?;
    let bounds = p95(&bucket["checkpoint_latency"]["counts"])?;
    let snapshot_miss = u128::from(snapshot_bytes) * 100 >= u128::from(sst) * 5;
    let checkpoint_miss = u128::from(checkpoint_ns) * 100 >= u128::from(publication_ns) * 20
        && bounds.is_some_and(|(lower, _)| lower >= 5_000_000);
    Ok(
        json!({"snapshot_miss":snapshot_miss,"checkpoint_miss":checkpoint_miss,"miss":snapshot_miss||checkpoint_miss,
        "checkpoint_p95_lower_ns":bounds.map(|value| value.0),"checkpoint_p95_upper_ns":bounds.map(|value| value.1),
        "snapshot_issued_bytes":snapshot_bytes,"ordinary_committed_sst_bytes":sst,
        "checkpoint_elapsed_ns":checkpoint_ns,"full_publication_elapsed_ns":publication_ns}),
    )
}

pub fn qualify(directory: &Path, cell: Cell) -> Result<Value> {
    let before = load(&directory.join("accounting-before.json"))?;
    let after = load(&directory.join("accounting-after.json"))?;
    let window = load(&directory.join("accounting-window.json"))?;
    let computed = delta(&after, &before)?;
    require(
        window["delta"] == computed,
        "window delta differs from independent cumulative subtraction",
    )?;
    require(
        window["finalized_after_shutdown"] == true,
        "window is provisional",
    )?;
    require(
        number(&window, "measured_acknowledged_rows")? == cell.measured() * cell.rows,
        "measured acknowledged rows differ",
    )?;
    require(
        number(&window, "measured_elapsed_ns")? > 0,
        "measured interval absent",
    )?;
    for bucket in array(&before, "buckets")? {
        if bucket["medium"] == "persistent" {
            require(
                number(bucket, "active_operations")? == 0,
                "metadata crosses warmup boundary",
            )?;
        }
    }
    let settled = load(&directory.join("accounting-original-final.json"))?;
    persistent_integrity(&settled)?;
    require(
        settled["owner_id"] == computed["owner_id"],
        "final original owner identity changed",
    )?;
    delta(&settled, &after)?;
    require(
        array(&settled, "buckets")?
            .iter()
            .all(|bucket| bucket["active_operations"] == 0),
        "post-shutdown metadata remains active",
    )?;
    let runtime_before = load(&directory.join("runtime-before.json"))?;
    let runtime_after = load(&directory.join("runtime-after.json"))?;
    let completed = number(&runtime_after, "compactions_run")?
        .checked_sub(number(&runtime_before, "compactions_run")?)
        .ok_or("runtime compaction counter decreased")?;
    require(
        number(&window, "completed_compactions")? == completed,
        "measured compaction delta differs",
    )?;
    let gate = evaluate(&computed, cell.measured(), completed)?;
    require(
        window["gate"]["valid"] == true && array(&window["gate"], "invalid_reasons")?.is_empty(),
        "producer gate invalid",
    )?;
    for field in [
        "snapshot_miss",
        "checkpoint_miss",
        "miss",
        "checkpoint_p95_lower_ns",
        "checkpoint_p95_upper_ns",
    ] {
        require(
            window["gate"][field] == gate[field],
            "producer ratio/quantile gate differs from independent computation",
        )?;
    }
    let reopened = load(&directory.join("accounting-reopened-after-shutdown.json"))?;
    persistent_integrity(&reopened)?;
    require(
        number(&reopened, "owner_id")? != number(&settled, "owner_id")?,
        "reopen reused owner",
    )?;
    require(
        array(&reopened, "buckets")?
            .iter()
            .all(|bucket| bucket["active_operations"] == 0),
        "reopened owner remains active after shutdown",
    )?;
    Ok(
        json!({"gate":gate,"measured_elapsed_ns":window["measured_elapsed_ns"],"completed_compactions":completed,
        "owner_id":computed["owner_id"],"device_write_amplification_measured":false}),
    )
}
