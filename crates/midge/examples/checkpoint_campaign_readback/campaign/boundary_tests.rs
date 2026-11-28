//! Constructed observation-parser contracts only; no executed metadata wait or ACK.

use super::observations;
use crate::checkpoint_campaign_readback::CELLS;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

fn write(path: &Path, value: &Value) {
    std::fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn commit(successes: u64) -> Value {
    json!({"attempts":successes,"successful_commits":successes,"write_stalls":0,
        "wait_calls":0,"wait_timeouts":0,"wait_write_stalls":0,"wait_elapsed_ns":0,"last_stall":null})
}

fn boundary(elapsed: u64) -> Value {
    json!({"samples":3,"active_samples":2,"pause_calls":2,"pause_requested_ns":2_000_000,
        "max_pause_requested_ns":1_000_000,"paused_ns":3_000_000,"elapsed_ns":elapsed,
        "final_persistent_active":0,"complete":true})
}

fn snapshot() -> Value {
    let buckets = (0..9)
        .flat_map(|index| {
            ["persistent", "memory_only"].map(|medium| {
                json!({"origin":format!("constructed-origin-{index}"),"medium":medium,
                "active_operations":if medium=="memory_only" {7} else {0}})
            })
        })
        .collect::<Vec<_>>();
    json!({"owner_id":7,"buckets":buckets})
}

struct Fixture {
    _root: tempfile::TempDir,
    directory: PathBuf,
    status: Value,
    ingestion: Value,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("midge/constructed-workload");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            root.path().join("native-config.txt"),
            "No-progress timeout: 60s (env STRESS_NO_PROGRESS_TIMEOUT_SECS)\n",
        )
        .unwrap();
        let before = boundary(7_000_000_000);
        let after = boundary(5_000_000);
        let status = json!({"commit_backpressure":commit(256),
            "commit_backpressure_policy":{"schema_version":"midge-checkpoint-commit-policy.v1",
                "retry_error":"write_stall_only","retry_budget_ns":30_000_000_000_u64,
                "wait_slice_ns":1_000_000_000_u64,"cell_budget_ns":900_000_000_000_u64,
                "required_no_progress_timeout_ns":60_000_000_000_u64},
            "metadata_boundary_policy":{"schema_version":"midge-checkpoint-boundary-policy.v1",
                "metadata_scope":"persistent_only","boundary_budget_ns":30_000_000_000_u64,
                "pause_slice_ns":1_000_000,"cell_budget_ns":900_000_000_000_u64,
                "sample_order":"runtime_metrics_then_metadata_snapshot",
                "zero_persistent_active_required":true,"progress_advanced":false,
                "before_clock_scope":"warmup_wait_outside_measured_final_query_inside",
                "after_clock_scope":"end_inside_measured"},
            "metadata_boundary_before":before,"metadata_boundary_after":after});
        let ingestion = json!({"flush_latencies_ns":vec![1_u64;230],"pressure_samples":[{}],
            "commit_backpressure_before":commit(26),"commit_backpressure_after":commit(256),
            "metadata_boundary_before":before,"metadata_boundary_after":after});
        write(
            &directory.join("resolved-options.json"),
            &json!({
            "memory_budget_bytes":536_870_912,"memtable_size_limit":8_388_608,
            "memtable_flush_threshold":8_388_608,"transaction_memory_pool_bytes":33_554_432,
            "background_compaction":true,"target_sst_size":134_217_728,"l0_compaction_trigger":3}),
        );
        write(
            &directory.join("accounting-window.json"),
            &json!({"measured_elapsed_ns":6_000_000_000_u64}),
        );
        write(&directory.join("accounting-before.json"), &snapshot());
        write(&directory.join("accounting-after.json"), &snapshot());
        Self {
            _root: root,
            directory,
            status,
            ingestion,
        }
    }

    fn check(&self) -> super::Result<Value> {
        write(
            &self.directory.join("ingestion-observations.json"),
            &self.ingestion,
        );
        observations(&self.directory, CELLS[0], &self.status)
    }

    fn endpoint(&mut self, field: &str, key: &str, value: Value) {
        self.ingestion[field][key] = value.clone();
        self.status[field][key] = value;
    }
}

#[test]
fn should_retain_busy_warmup_diagnostics_when_constructed_metadata_endpoints_are_idle() {
    // Arrange: excluded warmup capture exceeds measured time; recorded paused time
    // exceeds requested sleep, and MemoryOnly metadata remains active diagnostically.
    let fixture = Fixture::new();
    // Act.
    let result = fixture.check();
    // Assert: these are schema contracts, not Engine or metadata-work execution proof.
    assert!(
        result.is_ok(),
        "constructed valid observation rejected: {result:?}"
    );
}

#[test]
fn should_reject_missing_boundary_evidence_when_constructed_observations_are_checked() {
    // Arrange.
    let mut fixture = Fixture::new();
    fixture
        .ingestion
        .as_object_mut()
        .unwrap()
        .remove("metadata_boundary_after");
    // Act.
    let result = fixture.check();
    // Assert.
    assert!(result.is_err());
}

#[test]
fn should_reject_changed_boundary_policy_when_constructed_clock_scope_or_budget_differs() {
    // Arrange: poll budget was silently raised beyond the recorded maximum.
    let mut fixture = Fixture::new();
    fixture.status["metadata_boundary_policy"]["boundary_budget_ns"] = json!(30_000_000_001_u64);
    // Act/Assert.
    assert!(fixture.check().is_err());
    // Arrange: budget is restored but end wait has been excluded from the measured clock.
    fixture.status["metadata_boundary_policy"]["boundary_budget_ns"] = json!(30_000_000_000_u64);
    fixture.status["metadata_boundary_policy"]["after_clock_scope"] = json!("outside_measured");
    // Act/Assert.
    assert!(fixture.check().is_err());
}

#[test]
fn should_reject_boundary_sample_arithmetic_when_constructed_busy_polls_are_omitted() {
    // Arrange.
    let mut fixture = Fixture::new();
    fixture.endpoint("metadata_boundary_after", "samples", json!(2));
    // Act.
    let result = fixture.check();
    // Assert: two busy samples and a final idle sample require three candidates.
    assert!(result.is_err());
}

#[test]
fn should_reject_excluded_end_capture_when_constructed_elapsed_exceeds_measured_interval() {
    // Arrange: boundary budget is respected, but the end wait was not charged to ingestion.
    let mut fixture = Fixture::new();
    fixture.endpoint(
        "metadata_boundary_after",
        "elapsed_ns",
        json!(6_000_000_001_u64),
    );
    // Act.
    let result = fixture.check();
    // Assert: unlike warmup, every end capture belongs inside the actual measured clock.
    assert!(result.is_err());
}

#[test]
fn should_reject_fake_idle_boundary_when_constructed_selected_snapshot_has_persistent_work() {
    // Arrange: the observation says zero but the selected constructed evidence file contradicts it.
    let fixture = Fixture::new();
    let mut selected = snapshot();
    selected["buckets"][0]["active_operations"] = json!(1);
    write(&fixture.directory.join("accounting-after.json"), &selected);
    // Act.
    let result = fixture.check();
    // Assert: neither a later shutdown nor a declared idle result replaces this snapshot.
    assert!(result.is_err());
}

#[test]
fn should_reject_boundary_copy_drift_when_constructed_final_status_differs_from_ingestion() {
    // Arrange.
    let mut fixture = Fixture::new();
    fixture.status["metadata_boundary_before"]["paused_ns"] = json!(4_000_000);
    // Act.
    let result = fixture.check();
    // Assert: status and ingestion retain the same capture, not different later samples.
    assert!(result.is_err());
}

#[test]
fn should_reject_expired_warmup_capture_when_constructed_elapsed_equals_original_allowance() {
    // Arrange: source rejects idle at its deadline, including the excluded warmup boundary.
    let mut fixture = Fixture::new();
    fixture.endpoint(
        "metadata_boundary_before",
        "elapsed_ns",
        json!(30_000_000_000_u64),
    );
    // Act.
    let result = fixture.check();
    // Assert: complete=true cannot make an expired candidate valid.
    assert!(result.is_err());
}
