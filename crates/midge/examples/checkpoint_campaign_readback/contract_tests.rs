//! Constructed-contract tests only. No hosted/provider/checkpoint execution is claimed.

use super::{accounting, campaign, native, provenance, Identity, CELLS, SUITE};
use serde_json::{json, Value};
use std::path::Path;

fn identity() -> Identity {
    Identity {
        run_id: 42,
        run_attempt: 1,
        sha: "a".repeat(40),
    }
}

fn write(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn hosted(root: &Path, identity: &Identity) {
    std::fs::create_dir(root.join("hosted")).unwrap();
    write(
        &root.join("hosted/run.json"),
        &json!({"id":identity.run_id,"run_attempt":identity.run_attempt,
        "head_sha":identity.sha,"status":"completed","conclusion":"failure"}),
    );
    write(
        &root.join("hosted/jobs.json"),
        &json!({"total_count":0,"jobs":[]}),
    );
    write(
        &root.join("hosted/artifacts.json"),
        &json!({"total_count":0,"artifacts":[]}),
    );
}

#[test]
fn should_retain_all_nine_planned_rows_when_campaign_evidence_is_missing() {
    // Arrange: no uploaded files or native completion exist.
    let directory = tempfile::tempdir().unwrap();
    // Act: missing evidence is reported, not silently omitted.
    let report = campaign::evaluate(directory.path(), &identity());
    // Assert: the full matrix remains visible and cannot qualify.
    assert_eq!(report["rows"].as_array().unwrap().len(), 9);
    assert!(report["rows"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["valid"] == false && row["miss"].is_null()));
    assert_eq!(
        report["conditional_policy_measurement_predicate_met"],
        false
    );
    assert_eq!(report["cadence_change_accepted"], false);
}

#[test]
fn should_refuse_download_binding_when_destination_already_contains_unbound_files() {
    // Arrange: old bytes must not acquire a fresh run's provenance.
    let directory = tempfile::tempdir().unwrap();
    hosted(directory.path(), &identity());
    std::fs::create_dir(directory.path().join("artifacts")).unwrap();
    let old = directory.path().join("artifacts/old-file");
    std::fs::write(&old, b"preserve old evidence").unwrap();
    // Act.
    let result = provenance::prepare(directory.path(), &identity());
    // Assert.
    assert!(result.is_err());
    assert!(!directory.path().join("download-provenance.json").exists());
    assert_eq!(std::fs::read(old).unwrap(), b"preserve old evidence");
}

#[test]
fn should_preserve_originating_provenance_when_readback_requests_another_run() {
    // Arrange: a fresh pending receipt is bound before any download.
    let directory = tempfile::tempdir().unwrap();
    hosted(directory.path(), &identity());
    provenance::prepare(directory.path(), &identity()).unwrap();
    let path = directory.path().join("download-provenance.json");
    let original = std::fs::read(&path).unwrap();
    let mut other = identity();
    other.run_id += 1;
    // Act.
    let result = provenance::verify(directory.path(), &other);
    // Assert: rejection never replaces the old source receipt.
    assert!(result.unwrap_err().contains("another run"));
    assert_eq!(std::fs::read(path).unwrap(), original);
}

#[test]
fn should_refuse_incomplete_pagination_when_hosted_artifact_capture_omits_rows() {
    // Arrange: the REST footer declares one record absent from the capture.
    let directory = tempfile::tempdir().unwrap();
    hosted(directory.path(), &identity());
    write(
        &directory.path().join("hosted/artifacts.json"),
        &json!({"total_count":1,"artifacts":[]}),
    );
    // Act.
    let result = provenance::prepare(directory.path(), &identity());
    // Assert.
    assert!(result.unwrap_err().contains("fully paginated"));
}

#[test]
fn should_reject_hosted_capture_drift_without_replacing_pending_download_evidence() {
    // Arrange: capture a local receipt of synthetic REST contract data.
    let directory = tempfile::tempdir().unwrap();
    hosted(directory.path(), &identity());
    provenance::prepare(directory.path(), &identity()).unwrap();
    let path = directory.path().join("download-provenance.json");
    let original = std::fs::read(&path).unwrap();
    write(
        &directory.path().join("hosted/jobs.json"),
        &json!({"total_count":1,"jobs":[{"id":1}]}),
    );
    // Act.
    let result = provenance::seal(directory.path(), &identity());
    // Assert: a changed observation needs separate preserved evidence.
    assert!(result
        .unwrap_err()
        .contains("original hosted capture changed"));
    assert_eq!(std::fs::read(path).unwrap(), original);
}

fn receipt(stem: &str, identity: &Identity) -> Value {
    json!({"schema_version":"cntryl-stress.v2","suite":SUITE,"started_at":stem,
        "environment":{"git_commit":identity.sha,"command_line":["bench","--workload",CELLS[0].workload]},
        "metadata":{},"benchmark_specs":[],"summaries":[],"samples":[]})
}

#[test]
fn should_ignore_latest_alias_when_binding_exact_native_timestamp_and_pid() {
    // Arrange: minimal native-shape binding fixture, not completed #715 work.
    let directory = tempfile::tempdir().unwrap();
    let stem = "01791126977708914000-0000061690-00000000000000000000";
    let receipt = receipt(stem, &identity());
    write(&directory.path().join(format!("{stem}.json")), &receipt);
    write(&directory.path().join("latest.json"), &receipt);
    // Act.
    let result = native::match_receipt(directory.path(), CELLS[0], &identity(), 61690);
    // Assert: alias is not a second canonical receipt.
    assert!(result.is_ok());
    assert_eq!(native::stem_pid(stem), Some(61690));
}

#[test]
fn should_reject_two_distinct_canonical_receipts_when_both_match_one_process_identity() {
    // Arrange: both timestamps describe the same PID/selector/source.
    let directory = tempfile::tempdir().unwrap();
    for stem in ["1-123-0", "2-123-0"] {
        write(
            &directory.path().join(format!("{stem}.json")),
            &receipt(stem, &identity()),
        );
    }
    // Act.
    let result = native::match_receipt(directory.path(), CELLS[0], &identity(), 123);
    // Assert.
    assert!(result.unwrap_err().contains("ambiguous"));
}

#[test]
fn should_preserve_native_typed_failure_when_diagnostic_receipt_contains_no_progress() {
    // Arrange: canonical failure metadata and actual-counter-shaped partial data.
    let value = json!({"benchmark_specs":[{"metadata":{"failure_kind":"no_progress","benchmark_error":"caller watchdog"}}],
        "summaries":[{"correctness":{"passed":false,"counters":{"completed":7}},"metadata":{"failure_kind":"no_progress"}}],
        "samples":[{"counters":{"completed":7,"timeouts":1}}],"metadata":{}});
    // Act.
    let observed = native::failure_evidence(&value);
    // Assert: cause comes only from native fields; partial counts remain separate.
    assert_eq!(observed["failure_kind"], "no_progress");
    assert_eq!(observed["sample_counters"][0]["completed"], 7);
    assert_eq!(observed["failed"], true);
}

#[test]
fn should_skip_malformed_nested_native_metadata_without_panicking() {
    // Arrange.
    let malformed =
        json!({"benchmark_specs":[{"metadata":[]}],"summaries":[],"samples":[],"metadata":{}});
    // Act.
    let valid = native::valid_receipt(&malformed);
    // Assert.
    assert!(!valid);
    assert!(
        native::selected_workload(&json!(["bench", "--workload", "A", "--filter", "A"])).is_none()
    );
}

#[test]
fn should_report_exact_five_ms_histogram_boundary_when_p95_rank_reaches_the_later_bin() {
    // Arrange: constructed histogram policy, not measured latency.
    let mut counts = vec![0_u64; 80];
    counts[0] = 94;
    counts[20] = 6;
    // Act.
    let result = accounting::p95(&json!(counts)).unwrap();
    // Assert: do not subtract p95 values or invent an exact scalar latency.
    assert_eq!(result, Some((5_000_000, 5_249_999)));
}

fn policy_snapshot() -> Value {
    let counter_names = [
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
    let mut counters = serde_json::Map::new();
    for field in counter_names {
        counters.insert(field.to_owned(), Value::from(0));
    }
    counters.insert("issued_bytes".into(), json!([0, 0, 0]));
    counters.insert("returned_write_bytes".into(), json!([0, 0, 0]));
    let mut buckets = Vec::new();
    for origin in [
        "ordinary_local_flush",
        "cloud_flush",
        "recovery",
        "bootstrap",
        "ddl",
        "compaction_before_gc",
        "administration",
        "shutdown",
        "unclassified",
    ] {
        for medium in ["persistent", "memory_only"] {
            buckets.push(json!({"origin":origin,"medium":medium,
            "counters":counters,"active_operations":0,"checkpoint_latency":{"counts":vec![0_u64;80]},
            "full_publication_latency":{"counts":vec![0_u64;80]},"committed_sst_size_log2":vec![0_u64;64]}));
        }
    }
    let mut snapshot = json!({"owner_id":7,"overflow":false,"late_operation_writes":0,"incomplete_observations":0,"buckets":buckets});
    snapshot["buckets"][0]["counters"]["flush_committed_count"] = json!(1);
    snapshot["buckets"][0]["counters"]["flush_committed_sst_bytes"] = json!(10_000);
    snapshot["buckets"][0]["counters"]["flush_full_publication_elapsed_ns"] = json!(20_000_000);
    snapshot["buckets"][0]["committed_sst_size_log2"][13] = json!(1);
    snapshot["buckets"][0]["full_publication_latency"]["counts"][64] = json!(1);
    snapshot
}

#[test]
fn should_apply_exact_snapshot_threshold_when_fixed_policy_counters_cross_five_percent() {
    // Arrange: constructed recording-contract counters, not real checkpoint bytes.
    let mut snapshot = policy_snapshot();
    snapshot["buckets"][0]["counters"]["issued_bytes"][0] = json!(499);
    // Act: evaluate immediately below and at the preregistered rational boundary.
    let below = accounting::evaluate(&snapshot, 1, 1).unwrap();
    snapshot["buckets"][0]["counters"]["issued_bytes"][0] = json!(500);
    let at = accounting::evaluate(&snapshot, 1, 1).unwrap();
    // Assert.
    assert_eq!(below["miss"], false);
    assert_eq!(at["snapshot_miss"], true);
    assert_eq!(at["miss"], true);
}

#[test]
fn should_reject_forced_checkpoint_failure_when_ordinary_policy_denominators_are_valid() {
    // Arrange: constructed CompactionBeforeGc failure is kept outside ratios.
    let mut snapshot = policy_snapshot();
    assert!(accounting::evaluate(&snapshot, 1, 1).is_ok());
    snapshot["buckets"][10]["counters"]["operation_failures"] = json!(1);
    // Act.
    let result = accounting::evaluate(&snapshot, 1, 1);
    // Assert: safe ordinary data cannot validate failed persistent measurement.
    assert!(result
        .unwrap_err()
        .contains("persistent metadata operation failed"));
}

#[test]
fn should_reject_original_owner_delta_when_histogram_or_counters_decrease() {
    // Arrange: cumulative owner monotonicity is independent of final gate flags.
    let before = policy_snapshot();
    let mut after = before.clone();
    after["buckets"][0]["committed_sst_size_log2"][13] = json!(0);
    // Act.
    let result = accounting::delta(&after, &before);
    // Assert.
    assert!(result.unwrap_err().contains("decreased"));
}

#[test]
fn should_retain_nine_unqualified_rows_when_transport_smoke_has_no_native_evidence() {
    // Arrange: recording-contract input; no Engine or Actions work is fabricated.
    let directory = tempfile::tempdir().unwrap();
    // Act: an empty construction run still returns all planned campaign rows.
    let report = campaign::evaluate_construction_smoke(directory.path(), &identity());
    // Assert: neither a native transport success nor cadence permission exists.
    assert_eq!(report["rows"].as_array().unwrap().len(), 9);
    assert_eq!(report["construction_transport_valid"], false);
    assert_eq!(report["complete"], false);
    assert_eq!(
        report["conditional_policy_measurement_predicate_met"],
        false
    );
    assert_eq!(report["cadence_change_accepted"], false);
}

#[test]
fn should_reject_final_owner_when_forced_checkpoint_fails_after_valid_measured_window() {
    // Arrange: constructed policy data only, not a real Engine/provider failure.
    let measured = policy_snapshot();
    let original_gate = accounting::evaluate(&measured, 1, 1).unwrap();
    let mut final_owner = measured.clone();
    let forced = &mut final_owner["buckets"][10];
    forced["counters"]["operation_attempts"] = json!(1);
    forced["counters"]["operation_failures"] = json!(1);
    forced["counters"]["checkpoint_attempts"] = json!(1);
    forced["counters"]["snapshot_durable_count"] = json!(1);
    forced["checkpoint_latency"]["counts"][0] = json!(1);
    // Act: both final-owner paths consume this shared integrity contract.
    let result = accounting::persistent_integrity(&final_owner);
    // Assert: a valid measured ratio cannot admit later failed forced work.
    assert!(result
        .unwrap_err()
        .contains("persistent metadata operation failed"));
    assert_eq!(
        accounting::evaluate(&measured, 1, 1).unwrap(),
        original_gate
    );
}

#[test]
fn should_reject_final_owner_when_persistent_publication_histogram_omits_completed_flush() {
    // Arrange: constructed counters declare one actual-shape committed flush.
    let mut final_owner = policy_snapshot();
    final_owner["buckets"][0]["full_publication_latency"]["counts"][64] = json!(0);
    // Act: original and reopened final owners use the same semantic checks.
    let result = accounting::persistent_integrity(&final_owner);
    // Assert: final inspection cannot hide incomplete timing coverage.
    assert!(result
        .unwrap_err()
        .contains("full-publication histogram incomplete"));
}

fn constructed_final_owner_contract(root: &Path) -> (super::Cell, Value) {
    // Deliberately constructed accounting-reader input; no native receipt,
    // provider success, Engine acknowledgement or hosted attempt is generated.
    let after = policy_snapshot();
    let mut before = after.clone();
    for bucket in before["buckets"].as_array_mut().unwrap() {
        for value in bucket["counters"].as_object_mut().unwrap().values_mut() {
            if let Some(values) = value.as_array_mut() {
                values.fill(json!(0));
            } else {
                *value = json!(0);
            }
        }
        bucket["checkpoint_latency"]["counts"] = json!(vec![0_u64; 80]);
        bucket["full_publication_latency"]["counts"] = json!(vec![0_u64; 80]);
        bucket["committed_sst_size_log2"] = json!(vec![0_u64; 64]);
    }
    let mut gate = accounting::evaluate(&after, 1, 1).unwrap();
    gate["valid"] = json!(true);
    gate["invalid_reasons"] = json!([]);
    write(&root.join("accounting-before.json"), &before);
    write(&root.join("accounting-after.json"), &after);
    write(&root.join("accounting-original-final.json"), &after);
    let mut reopened = after.clone();
    reopened["owner_id"] = json!(8);
    write(
        &root.join("accounting-reopened-after-shutdown.json"),
        &reopened,
    );
    write(
        &root.join("accounting-window.json"),
        &json!({"delta":after,"gate":gate,"completed_compactions":1,
            "measured_elapsed_ns":1,"measured_acknowledged_rows":1,
            "finalized_after_shutdown":true}),
    );
    write(
        &root.join("runtime-before.json"),
        &json!({"compactions_run":0}),
    );
    write(
        &root.join("runtime-after.json"),
        &json!({"compactions_run":1}),
    );
    (
        super::Cell {
            id: "contract",
            workload: "contract",
            cycles: 2,
            rows: 1,
            families: 1,
        },
        after,
    )
}

#[test]
fn should_check_both_final_owner_files_when_constructed_later_forced_work_fails() {
    // Arrange: this is an accounting-reader contract, not campaign qualification.
    let directory = tempfile::tempdir().unwrap();
    let (cell, after) = constructed_final_owner_contract(directory.path());
    assert!(accounting::qualify(directory.path(), cell).is_ok());
    for (file, owner) in [
        ("accounting-original-final.json", 7),
        ("accounting-reopened-after-shutdown.json", 8),
    ] {
        let mut failed = after.clone();
        failed["owner_id"] = json!(owner);
        failed["buckets"][10]["counters"]["operation_failures"] = json!(1);
        let path = directory.path().join(file);
        let original = std::fs::read(&path).unwrap();
        write(&path, &failed);
        // Act: each actual readback filename must feed final persistence checks.
        let result = accounting::qualify(directory.path(), cell);
        // Assert: original and reopened final failures both refuse the window.
        assert!(result
            .unwrap_err()
            .contains("persistent metadata operation failed"));
        std::fs::write(&path, original).unwrap();
    }
}
