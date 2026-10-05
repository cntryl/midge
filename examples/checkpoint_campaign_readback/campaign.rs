//! Always retain the nine preregistered attempts; invalid evidence never counts as a miss.

use super::{
    accounting, array, json_files, load, native, number, provenance, require, string, Cell,
    Identity, Result, CELLS, TARGET,
};
use serde_json::{json, Value};
use std::path::Path;

fn job<'a>(jobs: &'a Value, cell: Cell, repeat: u64, identity: &Identity) -> Result<&'a Value> {
    let matches = array(jobs, "jobs")?
        .iter()
        .filter(|job| job["name"] == cell.job_name(repeat))
        .collect::<Vec<_>>();
    require(
        matches.len() == 1,
        "hosted matrix job missing/ambiguous; a skipped attempt is invalid",
    )?;
    let job = matches[0];
    require(
        number(job, "id")? > 0 && job["status"] == "completed" && job["conclusion"] == "success",
        "hosted attempt did not complete successfully",
    )?;
    require(
        number(job, "run_id")? == identity.run_id
            && number(job, "run_attempt")? == identity.run_attempt
            && job["head_sha"] == identity.sha,
        "hosted job belongs to another run/attempt/source",
    )?;
    let steps = array(job, "steps")?
        .iter()
        .filter(|step| step["name"] == "Run one native fixed workload in fresh process")
        .collect::<Vec<_>>();
    require(
        steps.len() == 1
            && steps[0]["status"] == "completed"
            && steps[0]["conclusion"] == "success",
        "actual workload step missing/failed/skipped",
    )?;
    Ok(job)
}

fn build(root: &Path, identity: &Identity) -> Result<Value> {
    let directory = root.join("artifacts").join(identity.build_artifact());
    let manifest = load(&directory.join("build-manifest.json"))?;
    require(
        manifest["schema_version"] == "midge-checkpoint-build.v1"
            && manifest["sha"] == identity.sha
            && string(&manifest, "run_id")? == identity.run_id.to_string()
            && string(&manifest, "run_attempt")? == identity.run_attempt.to_string(),
        "build identity differs",
    )?;
    require(
        super::is_hash(string(&manifest, "tree")?, 40)
            && super::is_hash(string(&manifest, "cargo_lock_sha256")?, 64),
        "build tree/lock identity absent",
    )?;
    require(
        manifest["profile"] == "bench/release"
            && manifest["binary"] == TARGET
            && array(&manifest, "features")? == [Value::from("checkpoint-bench")],
        "build target/profile/features differ",
    )?;
    require(
        provenance::hash_file(&directory.join(TARGET))? == string(&manifest, "executable_sha256")?,
        "release executable differs from build manifest",
    )?;
    let compiler = load(&directory.join("compiler-artifact.json"))?;
    require(
        compiler["reason"] == "compiler-artifact"
            && compiler["target"]["name"] == TARGET
            && array(&compiler["target"], "kind")?.contains(&Value::from("bench"))
            && compiler["profile"]["opt_level"] == "3"
            && compiler["profile"]["debug_assertions"] == false
            && array(&compiler, "features")?.contains(&Value::from("internal-testing"))
            && array(&compiler, "features")?.contains(&Value::from("checkpoint-bench")),
        "compiler artifact not the release bench",
    )?;
    require(
        manifest["reader"] == "checkpoint_campaign_readback"
            && array(&manifest, "reader_features")? == [Value::from("internal-testing")]
            && provenance::hash_file(&directory.join("checkpoint_campaign_readback"))?
                == string(&manifest, "reader_executable_sha256")?,
        "Rust reader executable differs from build manifest",
    )?;
    let reader = load(&directory.join("reader-compiler-artifact.json"))?;
    require(
        reader["reason"] == "compiler-artifact"
            && reader["target"]["name"] == "checkpoint_campaign_readback"
            && array(&reader["target"], "kind")?.contains(&Value::from("example"))
            && reader["profile"]["opt_level"] == "3"
            && reader["profile"]["debug_assertions"] == false
            && array(&reader, "features")?.contains(&Value::from("internal-testing")),
        "compiler artifact not the release Rust reader",
    )?;
    Ok(manifest)
}

fn status(
    directory: &Path,
    cell: Cell,
    repeat: u64,
    identity: &Identity,
) -> Result<(Value, std::path::PathBuf)> {
    let repeat_string = repeat.to_string();
    let mut selected = Vec::new();
    for path in json_files(&directory.join("midge"))? {
        if path
            .file_name()
            .is_none_or(|name| name != "workload-status.json")
        {
            continue;
        }
        let Ok(value) = load(&path) else {
            continue;
        };
        if value["benchmark_workload"] == cell.workload
            && value["git_commit"] == identity.sha
            && value["cell"] == cell.id
            && value["repeat"].as_str() == Some(repeat_string.as_str())
            && value["process_id"].as_u64().is_some()
        {
            selected.push((value, path));
        }
    }
    require(
        selected.len() == 1,
        "workload status missing/ambiguous for source/cell/repeat/PID",
    )?;
    Ok(selected.remove(0))
}

fn check_status(status: &Value, cell: Cell) -> Result<()> {
    require(
        status["schema_version"] == "midge-checkpoint.v1"
            && status["status"] == "passed"
            && status["phase"] == "complete"
            && status["terminal_error"].is_null()
            && status["verified"] == true
            && status["reopened_verified"] == true
            && status["flush_in_flight"] == false,
        "fixed-cell work/verification/reopen incomplete or failed",
    )?;
    for (key, expected) in [
        ("total_cycles", cell.cycles),
        ("warmup_cycles", cell.warmup()),
        ("measured_cycles", cell.measured()),
        ("rows_per_cycle", cell.rows),
        ("value_bytes", 1024),
        ("families", cell.families),
        ("completed_cycles", cell.cycles),
        ("acknowledged_rows", cell.cycles * cell.rows),
    ] {
        require(
            number(status, key)? == expected,
            "fixed-cell construction counters differ",
        )?;
    }
    require(
        status["stage_index"].is_null(),
        "unexpected staged workload status",
    )?;
    let finalized = &status["finalization"];
    require(
        finalized["source"] == "cntryl-stress"
            && finalized["receipt_selection"] == "matched"
            && finalized["step_outcome"] == "success"
            && finalized["failed"] == false
            && finalized["correctness_failed"] == false
            && finalized["failure_kind"].is_null()
            && finalized["failure_source"].is_null(),
        "canonical finalization did not pass",
    )
}

fn check_job_manifest(
    directory: &Path,
    cell: Cell,
    repeat: u64,
    identity: &Identity,
    build: &Value,
) -> Result<()> {
    let repeat_string = repeat.to_string();
    let manifest = load(&directory.join("job-manifest.json"))?;
    require(
        manifest["schema_version"] == "midge-checkpoint-attempt.v1"
            && manifest["sha"] == identity.sha
            && string(&manifest, "run_id")? == identity.run_id.to_string()
            && string(&manifest, "run_attempt")? == identity.run_attempt.to_string()
            && manifest["cell"] == cell.id
            && manifest["repeat"].as_str() == Some(repeat_string.as_str())
            && manifest["workload"] == cell.workload
            && manifest["job"] == "checkpoint-measure"
            && manifest["device_write_amplification_measured"] == false,
        "hosted companion identity differs",
    )?;
    require(
        !string(&manifest, "runner_image")?.is_empty(),
        "runner image not captured",
    )?;
    require(
        load(&directory.join("build-manifest.json"))? == *build,
        "attempt build copy differs from canonical build",
    )?;
    let selection = load(&directory.join("selection.json"))?;
    let selected = array(&selection, "selected")?;
    require(
        selection["workload"] == cell.workload
            && array(&selection, "registered")?.len() == 3
            && selected.len() == 1
            && selected[0]["function_name"] == cell.workload
            && selected[0]["tier"] == 4
            && selected[0]["ignored"] == false,
        "exact native selection not independently captured",
    )?;
    require(
        load(&directory.join("exit-outcome.json"))?["workload_step_outcome"] == "success",
        "authoritative workload outcome differs",
    )?;
    let finalization = load(&directory.join("finalization.json"))?;
    require(
        array(&finalization, "consistency_errors")?.is_empty()
            && array(&finalization, "finalized")?.len() == 1
            && finalization["matching_receipts"] == 1
            && finalization["step_outcome"] == "success",
        "artifact finalizer summary did not pass",
    )
}

fn observations(directory: &Path, cell: Cell) -> Result<()> {
    let options = load(&directory.join("resolved-options.json"))?;
    for (field, value) in [
        ("memory_budget_bytes", 512 * 1024 * 1024),
        ("memtable_size_limit", 8 * 1024 * 1024),
        ("memtable_flush_threshold", 8 * 1024 * 1024),
        ("transaction_memory_pool_bytes", 32 * 1024 * 1024),
    ] {
        require(
            number(&options, field)? == value,
            "resolved fixed engine options differ",
        )?;
    }
    require(
        options["background_compaction"] == true
            && number(&options, "target_sst_size")? > 0
            && number(&options, "l0_compaction_trigger")? > 0,
        "resolved compaction options missing",
    )?;
    let observations = load(&directory.join("ingestion-observations.json"))?;
    let latencies = array(&observations, "flush_latencies_ns")?;
    require(
        latencies.len() as u64 == cell.measured()
            && latencies
                .iter()
                .all(|latency| latency.as_u64().is_some_and(|ns| ns > 0)),
        "actual measured flush latency evidence incomplete",
    )?;
    let pressure = array(&observations, "pressure_samples")?;
    require(
        !pressure.is_empty() && pressure.len() <= 5,
        "bounded runtime pressure samples missing",
    )
}

fn attempt(
    root: &Path,
    identity: &Identity,
    cell: Cell,
    repeat: u64,
    jobs: &Value,
    compilation: &Result<Value>,
    binding: &Result<provenance::Download>,
) -> Value {
    let artifact_name = identity.artifact(cell, repeat);
    let directory = root.join("artifacts").join(&artifact_name);
    let mut row = json!({"cell":cell.id,"repeat":repeat,"workload":cell.workload,"artifact_name":artifact_name,
        "valid":false,"miss":null,"invalid_reasons":[],"failure_kind":null,"provenance_bound":binding.is_ok()});
    let mut invalid = Vec::new();
    if let Err(error) = binding {
        invalid.push(format!("download binding: {error}"));
    }
    match job(jobs, cell, repeat, identity) {
        Ok(job) => {
            row["hosted_job_id"] = job["id"].clone();
            row["hosted_job_conclusion"] = job["conclusion"].clone();
        }
        Err(error) => invalid.push(error),
    }
    let canonical = match compilation {
        Ok(manifest) => Some(manifest),
        Err(error) => {
            invalid.push(format!("build: {error}"));
            None
        }
    };
    if let Some(manifest) = canonical {
        if let Err(error) = check_job_manifest(&directory, cell, repeat, identity, manifest) {
            invalid.push(error);
        }
    }
    match status(&directory, cell, repeat, identity) {
        Ok((status, path)) => {
            row["observed_status"] = status.clone();
            if let Err(error) = check_status(&status, cell) {
                invalid.push(error);
            }
            let internal = path.parent().expect("a file has a parent");
            match accounting::qualify(internal, cell) {
                Ok(accounting) => row["accounting"] = accounting,
                Err(error) => invalid.push(format!("accounting: {error}")),
            }
            if let Err(error) = observations(internal, cell) {
                invalid.push(error);
            }
            match native::match_receipt(
                &directory.join("native"),
                cell,
                identity,
                number(&status, "process_id").unwrap_or(0),
            ) {
                Ok((receipt, path)) => {
                    row["native_receipt"] = Value::from(path);
                    row["observed_native_failure_metadata"] = receipt["metadata"].clone();
                    row["native_failure"] = native::failure_evidence(&receipt);
                    row["failure_kind"] = row["native_failure"]["failure_kind"].clone();
                    if status["finalization"]["receipt_run_id"] != receipt["started_at"]
                        || status["finalization"]["sha"] != identity.sha
                        || status["finalization"]["benchmark_workload"] != cell.workload
                    {
                        invalid.push(
                            "finalizer bound a different native receipt/source/workload".into(),
                        );
                    }
                    let elapsed = row["accounting"]["measured_elapsed_ns"]
                        .as_u64()
                        .unwrap_or(0);
                    match native::qualify(&receipt, identity, cell, repeat, elapsed) {
                        Ok(native) => row["native"] = native,
                        Err(error) => invalid.push(format!("native: {error}")),
                    }
                }
                Err(error) => invalid.push(error),
            }
        }
        Err(error) => invalid.push(format!("status: {error}; actual workload cause unknown")),
    }
    if invalid.is_empty() {
        row["valid"] = Value::Bool(true);
        row["miss"] = row["accounting"]["gate"]["miss"].clone();
    }
    row["invalid_reasons"] = json!(invalid);
    row
}

pub fn evaluate(root: &Path, identity: &Identity) -> Value {
    let binding = provenance::verify(root, identity);
    let hosted = provenance::hosted(root, identity);
    let mut errors = Vec::new();
    let (run, jobs) = match hosted {
        Ok((run, jobs, _)) => (run, jobs),
        Err(error) => {
            errors.push(error);
            (Value::Null, json!({"jobs":[]}))
        }
    };
    if let Err(error) = &binding {
        errors.push(error.clone());
    }
    if run["status"] != "completed" || run["conclusion"] != "success" {
        errors.push("workflow has not completed successfully; campaign cannot qualify".into());
    }
    let built = build(root, identity);
    let mut rows = Vec::new();
    for cell in CELLS {
        for repeat in 1..=3 {
            rows.push(attempt(
                root, identity, cell, repeat, &jobs, &built, &binding,
            ));
        }
    }
    let complete = errors.is_empty() && rows.iter().all(|row| row["valid"] == true);
    let mut cells = Vec::new();
    for cell in CELLS {
        let misses = rows
            .iter()
            .filter(|row| row["cell"] == cell.id && row["valid"] == true && row["miss"] == true)
            .count();
        let valid = rows
            .iter()
            .filter(|row| row["cell"] == cell.id && row["valid"] == true)
            .count();
        cells.push(
            json!({"cell":cell.id,"valid_attempts":valid,"observed_valid_misses":misses,
            "qualifies":complete && valid == 3 && misses >= 2}),
        );
    }
    let policy_permitted = complete && cells.iter().any(|cell| cell["qualifies"] == true);
    json!({"schema_version":"midge715-campaign-readback.v1","identity":identity,"complete":complete,
        "qualification_errors":errors,"planned_attempts":9,"rows":rows,"cells":cells,
        "conditional_policy_measurement_predicate_met":policy_permitted,
        "cadence_change_accepted":false,"device_write_amplification_measured":false,
        "coverage":"issued engine-Fs metadata payload and per-logical-flush publication wall time; pre-state startup writes excluded",
        "note":"native statistical diagnostics are preserved; hosted metadata is caller-supplied captured REST evidence, no live request made"})
}

/// A dedicated real A/r1 construction run proves transport without qualifying
/// the nine-attempt measurement campaign or using a miss to authorize cadence.
pub fn evaluate_construction_smoke(root: &Path, identity: &Identity) -> Value {
    let mut report = evaluate(root, identity);
    let mut errors = report["qualification_errors"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let rows = report["rows"].as_array().cloned().unwrap_or_default();
    if rows.len() != 9
        || rows
            .first()
            .is_none_or(|row| row["cell"] != "A" || row["repeat"] != 1 || row["valid"] != true)
        || rows.iter().skip(1).any(|row| row["valid"] != false)
    {
        errors.push(json!("construction smoke requires one valid real A/r1 row and eight explicit invalid unexecuted rows"));
    }
    match provenance::hosted(root, identity) {
        Ok((_, jobs, artifacts)) => {
            let expected = [identity.build_artifact(), identity.artifact(CELLS[0], 1)];
            if artifacts.len() != 2
                || artifacts
                    .iter()
                    .any(|artifact| !expected.contains(&artifact.name))
            {
                errors.push(json!(
                    "construction smoke must retain exactly the real build and A/r1 artifacts"
                ));
            }
            for cell in CELLS {
                for repeat in 1..=3 {
                    if (cell.id != "A" || repeat != 1)
                        && array(&jobs, "jobs").is_ok_and(|jobs| {
                            jobs.iter().any(|job| job["name"] == cell.job_name(repeat))
                        })
                    {
                        errors.push(json!(
                            "construction smoke cannot substitute an attempted campaign row"
                        ));
                    }
                }
            }
        }
        Err(error) => errors.push(json!(error)),
    }
    report["mode"] = json!("construction_smoke");
    report["construction_transport_valid"] = json!(errors.is_empty());
    report["construction_errors"] = json!(errors);
    report["complete"] = json!(false);
    report["conditional_policy_measurement_predicate_met"] = json!(false);
    report["cadence_change_accepted"] = json!(false);
    if let Some(cells) = report["cells"].as_array_mut() {
        for cell in cells {
            cell["qualifies"] = json!(false);
        }
    }
    report
}
