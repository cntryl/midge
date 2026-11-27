//! Canonical receipt binding mirrors the existing finalizer, with fixed-cell gates.

use super::{array, json_files, load, number, require, string, Cell, Identity, Result, SUITE};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::Path;

pub fn selected_workload(command: &Value) -> Option<String> {
    let args = command.as_array()?;
    let args = args.iter().map(Value::as_str).collect::<Option<Vec<_>>>()?;
    let mut selected = Vec::new();
    for (index, arg) in args.iter().enumerate() {
        if matches!(*arg, "--workload" | "--filter") {
            selected.push(*args.get(index + 1)?);
        } else if let Some(value) = arg
            .strip_prefix("--workload=")
            .or_else(|| arg.strip_prefix("--filter="))
        {
            selected.push(value);
        }
    }
    (selected.len() == 1).then(|| selected[0].to_owned())
}

pub fn stem_pid(stem: &str) -> Option<u64> {
    let pieces = stem.split('-').collect::<Vec<_>>();
    if pieces.len() != 3
        || pieces
            .iter()
            .any(|piece| piece.is_empty() || !piece.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }
    pieces[1].parse().ok()
}

pub fn valid_receipt(value: &Value) -> bool {
    if !value.is_object() || !value.get("metadata").is_none_or(Value::is_object) {
        return false;
    }
    if value["metadata"]
        .get("reporter_errors")
        .is_some_and(|item| !item.is_string())
    {
        return false;
    }
    for key in ["benchmark_specs", "summaries", "samples"] {
        let Some(rows) = value[key].as_array() else {
            return false;
        };
        for row in rows {
            if !row.is_object() || !row.get("metadata").is_none_or(Value::is_object) {
                return false;
            }
            for field in ["benchmark_error", "failure_kind"] {
                if row["metadata"]
                    .get(field)
                    .is_some_and(|item| !item.is_string())
                {
                    return false;
                }
            }
            if key == "summaries" {
                if !row.get("correctness").is_none_or(Value::is_object) {
                    return false;
                }
                if row["correctness"]
                    .get("passed")
                    .is_some_and(|item| !item.is_boolean())
                {
                    return false;
                }
            }
            if key == "samples" {
                if let Some(value) = row.get("counters") {
                    let Some(counters) = value.as_object() else {
                        return false;
                    };
                    if counters.values().any(|value| value.as_u64().is_none()) {
                        return false;
                    }
                }
            }
        }
    }
    true
}

pub fn match_receipt(
    root: &Path,
    cell: Cell,
    identity: &Identity,
    pid: u64,
) -> Result<(Value, String)> {
    let mut matches = Vec::new();
    for path in json_files(root)? {
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if stem_pid(stem) != Some(pid) {
            continue;
        }
        let Ok(receipt) = load(&path) else {
            continue;
        };
        if valid_receipt(&receipt)
            && receipt["schema_version"] == "cntryl-stress.v2"
            && receipt["suite"] == SUITE
            && receipt["started_at"] == stem
            && receipt["environment"]["git_commit"] == identity.sha
            && selected_workload(&receipt["environment"]["command_line"]).as_deref()
                == Some(cell.workload)
        {
            matches.push((receipt, path.display().to_string()));
        }
    }
    require(
        matches.len() == 1,
        "canonical native receipt missing or ambiguous for exact PID/suite/SHA/selector",
    )?;
    Ok(matches.remove(0))
}

pub fn failure_evidence(receipt: &Value) -> Value {
    let mut messages = BTreeSet::new();
    let mut kinds = BTreeSet::new();
    let mut correctness = Vec::new();
    for field in ["benchmark_specs", "summaries"] {
        if let Some(rows) = receipt[field].as_array() {
            for row in rows {
                if let Some(message) = row["metadata"]["benchmark_error"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                {
                    messages.insert(message.to_owned());
                }
                if let Some(kind) = row["metadata"]["failure_kind"]
                    .as_str()
                    .filter(|value| !value.is_empty())
                {
                    kinds.insert(kind.to_owned());
                }
                if field == "summaries" {
                    correctness.push(row["correctness"].clone());
                }
            }
        }
    }
    let counters = receipt["samples"]
        .as_array()
        .map(|samples| {
            samples
                .iter()
                .map(|sample| sample["counters"].clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let failed_counters = counters.iter().any(|counter| {
        [
            "failures",
            "timeouts",
            "duplicates",
            "dropped",
            "validation_errors",
        ]
        .into_iter()
        .any(|field| counter[field].as_u64().is_some_and(|value| value > 0))
    });
    let correctness_failed =
        correctness.iter().any(|value| value["passed"] == false) || failed_counters;
    let reporter_error = receipt["metadata"]["reporter_errors"].clone();
    let failed = !messages.is_empty()
        || !kinds.is_empty()
        || correctness_failed
        || !reporter_error.is_null();
    json!({"failed":failed,"failure_kind":if kinds.len() == 1 {kinds.first().cloned()} else {None},
        "benchmark_errors":messages,"failure_kinds":kinds,"correctness_failed":correctness_failed,
        "correctness":correctness,"sample_counters":counters,"reporter_error":reporter_error})
}

fn check_counters(value: &Value, completed: u64) -> Result<()> {
    require(
        number(value, "attempted")? == completed && number(value, "completed")? == completed,
        "native acknowledged-row counters differ from the fixed measured cell",
    )?;
    for field in [
        "failures",
        "timeouts",
        "duplicates",
        "dropped",
        "validation_errors",
    ] {
        require(
            number(value, field)? == 0,
            "native correctness counter failed",
        )?;
    }
    Ok(())
}

pub fn qualify(
    receipt: &Value,
    identity: &Identity,
    cell: Cell,
    repeat: u64,
    elapsed_ns: u64,
) -> Result<Value> {
    require(valid_receipt(receipt), "malformed native receipt")?;
    require(
        receipt["metadata"]["run_id"] == identity.native_run_id(cell, repeat),
        "native campaign run ID mismatch",
    )?;
    require(
        receipt["metadata"].get("reporter_errors").is_none(),
        "native reporter error",
    )?;
    require(
        receipt["environment"]["build_profile"] == "release",
        "native binary is not release-built",
    )?;
    let profile = &receipt["environment"]["profile_config"];
    require(
        profile["profile"] == "smoke"
            && profile["measured_samples"] == 1
            && profile["warmup_samples"] == 0
            && profile["cooldown_samples"] == 0,
        "native sampling differs from the one fixed-cell preregistration",
    )?;
    let specs = array(receipt, "benchmark_specs")?;
    let summaries = array(receipt, "summaries")?;
    let samples = array(receipt, "samples")?;
    require(
        specs.len() == 1 && summaries.len() == 1 && samples.len() == 1,
        "fixed cell must have exactly one native external row/sample",
    )?;
    let (spec, summary, sample) = (&specs[0], &summaries[0], &samples[0]);
    require(
        spec["name"] == cell.workload && summary["name"] == cell.workload,
        "native row is not the selected fixed cell",
    )?;
    require(
        spec["id"] == summary["benchmark_id"] && spec["id"] == sample["benchmark_id"],
        "native row IDs differ",
    )?;
    require(
        spec["tier"] == 4
            && summary["tier"] == 4
            && spec["intent"] == "external"
            && summary["intent"] == "external"
            && sample["intent"] == "external"
            && sample["phase"] == "measured",
        "native row topology differs",
    )?;
    require(
        summary["measured_samples"] == 1
            && summary["warmup_samples"] == 0
            && summary["cooldown_samples"] == 0,
        "native summary sampling differs",
    )?;
    require(
        number(sample, "operations_attempted")? == cell.measured() * cell.rows
            && number(sample, "operations_completed")? == cell.measured() * cell.rows,
        "native operation totals differ from the fixed measured cell",
    )?;
    require(
        summary["correctness"]["passed"] == true,
        "native correctness failed",
    )?;
    require(
        array(&summary["correctness"], "errors")?.is_empty(),
        "native correctness errors present",
    )?;
    check_counters(
        &summary["correctness"]["counters"],
        cell.measured() * cell.rows,
    )?;
    check_counters(&sample["counters"], cell.measured() * cell.rows)?;
    require(
        number(sample, "elapsed_ns")? == elapsed_ns,
        "native elapsed differs from measured window",
    )?;
    for row in [spec, summary] {
        require(
            row["metadata"].get("benchmark_error").is_none()
                && row["metadata"].get("failure_kind").is_none(),
            "native typed benchmark failure present",
        )?;
    }
    Ok(
        json!({"started_at": string(receipt,"started_at")?, "correctness": summary["correctness"],
        "quality": summary["quality"], "trust_class": summary["trust_class"],
        "diagnostics": summary["diagnostics"], "metadata": receipt["metadata"]}),
    )
}
