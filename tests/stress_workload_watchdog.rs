//! Check the real external watchdog without adding a production stall control.
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

#[allow(dead_code)]
#[path = "../benches/bench_support/stress_scenarios.rs"]
mod stress_scenarios;

const CHILD_FLAG: &str = "MIDGE_WATCHDOG_TEST_CHILD";

mod child_harness {
    #[cntryl_stress::stress(tier = 5)]
    fn rejected_clients_with_disk_churn(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_watchdog_fixture(ctx, false);
    }

    #[cntryl_stress::stress(tier = 5)]
    fn resumed_successes_with_disk_churn(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_watchdog_fixture(ctx, true);
    }

    #[cntryl_stress::stress(tier = 5)]
    fn delayed_recovery_with_flat_cache(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_recovery_watchdog_fixture(
            ctx,
            cntryl_midge::__internal::recovery::RecoveryProgressFixtureMode::DelayedRanges,
        );
    }

    #[cntryl_stress::stress(tier = 5)]
    fn held_recovery_with_flat_cache(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_recovery_watchdog_fixture(
            ctx,
            cntryl_midge::__internal::recovery::RecoveryProgressFixtureMode::HeldFirstRange,
        );
    }

    #[cntryl_stress::stress(tier = 5)]
    fn cached_recovery_with_flat_cache(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_recovery_watchdog_fixture(
            ctx,
            cntryl_midge::__internal::recovery::RecoveryProgressFixtureMode::CachedCoverage,
        );
    }

    #[cntryl_stress::stress(tier = 5)]
    fn metadata_inventory_recovery_with_flat_cache(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_recovery_watchdog_fixture(
            ctx,
            cntryl_midge::__internal::recovery::RecoveryProgressFixtureMode::MetadataInventory,
        );
    }

    #[cntryl_stress::stress(tier = 5)]
    fn held_inventory_recovery_with_flat_cache(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_recovery_watchdog_fixture(
            ctx,
            cntryl_midge::__internal::recovery::RecoveryProgressFixtureMode::HeldInventory,
        );
    }

    #[cntryl_stress::stress(tier = 5)]
    fn journal_recovery_with_flat_cache(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_journal_recovery_watchdog_fixture(ctx);
    }

    #[cntryl_stress::stress(tier = 5)]
    fn scoped_recovery_listener(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_recovery_listener_fixture(ctx);
    }

    cntryl_stress::stress_main!();

    pub(super) fn run() {
        main();
    }
}

fn invoke_child(artifacts: &Path, workload: &str) -> Output {
    let mut command = Command::new(std::env::current_exe().expect("watchdog test executable"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("STRESS_") {
            command.env_remove(name);
        }
    }
    if workload.ends_with("recovery_with_flat_cache") {
        command.env("RUST_LOG", "off");
    }
    command
        .arg("--workload")
        .arg(workload)
        .env(CHILD_FLAG, "1")
        .env("STRESS_PROFILE", "smoke")
        .env("STRESS_SAMPLES", "1")
        .env("STRESS_WARMUP_SAMPLES", "0")
        .env("STRESS_COOLDOWN_SAMPLES", "0")
        .env("STRESS_NO_PROGRESS_TIMEOUT_SECS", "1")
        .env("STRESS_JSON", "true")
        .env("STRESS_OUTPUT_DIR", artifacts.join("stress"))
        .env("MIDGE_STRESS_ARTIFACT_DIR", artifacts.join("midge"))
        .env_remove("GITHUB_ACTIONS")
        .output()
        .expect("execute real stress watchdog child")
}

fn receipt(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "parse real watchdog receipt: {error}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn workload_directory(artifacts: &Path) -> PathBuf {
    fs::read_dir(artifacts.join("midge"))
        .expect("retained workload artifacts")
        .next()
        .expect("child workload artifact directory")
        .expect("read child workload entry")
        .path()
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("retained atomic JSON"))
        .expect("complete JSON after child process exit")
}

fn client_snapshot(workload: &Path) -> Value {
    read_json(&workload.join("client-snapshots/stage-00/client-00.json"))
}

fn should_report_no_progress_when_rejected_clients_keep_database_bytes_changing() {
    // Arrange
    let artifacts = tempfile::tempdir().expect("create child artifact directory");

    // Act: only the actual cntryl-stress harness owns the failure decision.
    let output = invoke_child(artifacts.path(), "rejected_clients_with_disk_churn");
    let receipt = receipt(&output);

    // Assert: require typed no-progress, not an empty benchmark or other error.
    let specs = receipt["benchmark_specs"]
        .as_array()
        .expect("canonical specs");
    let failure = specs
        .iter()
        .find(|spec| spec["metadata"]["failure_kind"] == "no_progress_timeout")
        .unwrap_or_else(|| {
            panic!(
                "real watchdog did not emit typed no_progress_timeout; child_status={}; recorded_specs={specs:?}",
                output.status
            )
        });
    assert!(!output.status.success());
    assert_eq!(failure["metadata"]["no_progress_timeout_secs"], "1");
    assert_eq!(failure["metadata"]["progress_completed_units"], "0");
    let workload = workload_directory(artifacts.path());
    let status = read_json(&workload.join("workload-status.json"));
    assert_eq!(status["phase"], "workload");
    assert_eq!(status["stage"], "1-clients");
    let snapshot = client_snapshot(&workload);
    assert!(snapshot["attempted_transactions"].as_u64().unwrap() > 1);
    assert_eq!(snapshot["acknowledged_transactions"], 0);
    assert_eq!(snapshot["last_success_elapsed_ms"], Value::Null);
    assert_eq!(snapshot["terminal_error"], Value::Null);
    assert_eq!(snapshot["stage_index"], 0);
    assert_eq!(snapshot["client_index"], 0);
    assert!(snapshot["process_id"].as_u64().unwrap() > 0);
    let samples = fs::read_to_string(workload.join("resource-samples.csv"))
        .expect("actual sampler artifacts survive abandonment");
    assert!(samples.lines().skip(2).any(|line| {
        line.rsplit(',')
            .next()
            .and_then(|bytes| bytes.parse::<u64>().ok())
            .is_some_and(|bytes| bytes > 0)
    }));
}

fn should_remain_healthy_when_successful_clients_resume_before_watchdog_expiry() {
    // Arrange: initial stall and one later resource rejection are followed by
    // successful operations on a 100ms script, within the one-second watchdog.
    let artifacts = tempfile::tempdir().expect("create resumed child artifact directory");

    // Act
    let output = invoke_child(artifacts.path(), "resumed_successes_with_disk_churn");
    let receipt = receipt(&output);

    // Assert: the same loop, reporter, sampler and watchdog remain healthy.
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let specs = receipt["benchmark_specs"]
        .as_array()
        .expect("canonical specs");
    assert!(specs
        .iter()
        .all(|spec| spec["metadata"]["failure_kind"] != "no_progress_timeout"));
    let workload = workload_directory(artifacts.path());
    let snapshot = client_snapshot(&workload);
    let acknowledged = snapshot["acknowledged_transactions"].as_u64().unwrap();
    assert!(acknowledged > 0);
    assert_eq!(snapshot["write_stall_responses"], 1);
    assert_eq!(snapshot["resource_limit_responses"], 1);
    assert_eq!(snapshot["operation_in_flight"], false);
    assert_eq!(snapshot["terminal_error"], Value::Null);
    assert!(snapshot["last_success_elapsed_ms"].as_u64().unwrap() > 0);
    assert!(specs.iter().any(|spec| {
        spec["metadata"]["fixture_acknowledged_transactions"]
            .as_str()
            .and_then(|value| value.parse::<u64>().ok())
            == Some(acknowledged)
            && spec["metadata"]["fixture_progress_units"]
                .as_str()
                .and_then(|value| value.parse::<u64>().ok())
                == Some(acknowledged)
    }));
}

fn assert_database_stays_empty(workload: &Path) {
    let samples = fs::read_to_string(workload.join("resource-samples.csv"))
        .expect("retained recovery sampler observations");
    let disk_bytes: Vec<_> = samples
        .lines()
        .skip(1)
        .map(|line| {
            line.rsplit(',')
                .next()
                .expect("sample disk column")
                .parse::<u64>()
                .expect("sample disk bytes")
        })
        .collect();
    assert!(
        disk_bytes.len() > 2,
        "sampler must observe the recovery wait"
    );
    assert!(
        disk_bytes.iter().all(|&bytes| bytes == 0),
        "recovery fixture must not acquire progress from disk growth: {disk_bytes:?}"
    );
}

fn recovery_observations(workload: &Path) -> Value {
    read_json(&workload.join("recovery-fixture/fixture-observations.json"))
}

fn assert_successful_recovery_work(output: &Output, workload: &Path) -> Value {
    let receipt = receipt(output);
    let observations = recovery_observations(workload);
    assert!(
        output.status.success(),
        "actual progressing recovery must survive the one-second watchdog; status={}, observations={observations}; receipt={receipt}; stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    let result = read_json(&workload.join("recovery-result.json"));
    assert_eq!(result["mismatches"], 0);
    assert_eq!(result["local_wal_bytes"], 0);
    assert_eq!(result["staged_wal_count"], 0);
    assert!(result["elapsed_ms"].as_u64().unwrap() > 1_000);
    let specs = receipt["benchmark_specs"]
        .as_array()
        .expect("canonical recovery specs");
    assert!(specs
        .iter()
        .all(|spec| spec["metadata"]["failure_kind"] != "no_progress_timeout"));
    assert!(specs.iter().any(|spec| {
        spec["metadata"]["fixture_recovery_progress_units"]
            .as_str()
            .and_then(|units| units.parse::<u64>().ok())
            .is_some_and(|units| units > 0)
    }));
    assert_database_stays_empty(workload);
    result
}

fn assert_successful_recovery(output: &Output, workload: &Path) -> Value {
    let result = assert_successful_recovery_work(output, workload);
    assert!(result["expected_records"].as_u64().unwrap() > 0);
    assert_eq!(result["verified_records"], result["expected_records"]);
    assert_eq!(result["max_sequence"], result["expected_records"]);
    assert_eq!(result["max_epoch"], 7);
    result
}

fn should_remain_healthy_when_actual_cloud_recovery_completes_read_only_work() {
    // Arrange: the real CRC/inspection/replay scans receive successful delayed
    // identity-bound ranges, with RUST_LOG=off and a flat local cache.
    let artifacts = tempfile::tempdir().expect("create delayed recovery artifacts");

    // Act
    let output = invoke_child(artifacts.path(), "delayed_recovery_with_flat_cache");
    let workload = workload_directory(artifacts.path());

    // Assert: completed remote work, rather than phase entry or disk churn,
    // keeps this recovery healthy beyond the configured idle budget.
    let result = assert_successful_recovery(&output, &workload);
    assert!(result["completed_range_reads"].as_u64().unwrap() >= 16);
    assert!(result["completed_range_bytes"].as_u64().unwrap() >= 1_024 * 1_024);
    assert!(result["maximum_range_bytes"].as_u64().unwrap() <= 64 * 1_024);
}

fn should_report_no_progress_when_actual_cloud_recovery_holds_first_range() {
    // Arrange
    let artifacts = tempfile::tempdir().expect("create held recovery artifacts");

    // Act: only the actual external watchdog classifies the held callback.
    let output = invoke_child(artifacts.path(), "held_recovery_with_flat_cache");
    let receipt = receipt(&output);
    let workload = workload_directory(artifacts.path());

    // Assert: a submitted request is not completed recovery work.
    let specs = receipt["benchmark_specs"]
        .as_array()
        .expect("canonical held-recovery specs");
    let failure = specs
        .iter()
        .find(|spec| spec["metadata"]["failure_kind"] == "no_progress_timeout")
        .unwrap_or_else(|| panic!("held range must emit typed no-progress: {receipt}"));
    assert!(!output.status.success());
    assert_eq!(failure["metadata"]["no_progress_timeout_secs"], "1");
    let status = read_json(&workload.join("workload-status.json"));
    assert_eq!(status["phase"], "recovery");
    let observations = recovery_observations(&workload);
    assert_eq!(observations["held_requests"], 1);
    assert_eq!(observations["completed_range_reads"], 0);
    assert_eq!(observations["completed_range_bytes"], 0);
    assert_eq!(observations["local_wal_bytes"], 0);
    assert_database_stays_empty(&workload);
}

fn should_report_no_progress_when_actual_inventory_holds_first_head() {
    // Arrange
    let artifacts = tempfile::tempdir().expect("create held inventory artifacts");

    // Act: the mandatory inventory check submits a real HEAD, whose callback
    // the fixture retains before any response or size validation completes.
    let output = invoke_child(artifacts.path(), "held_inventory_recovery_with_flat_cache");
    let receipt = receipt(&output);
    let workload = workload_directory(artifacts.path());

    // Assert: HEAD submission does not refresh the actual external watchdog.
    let failure = receipt["benchmark_specs"]
        .as_array()
        .expect("canonical held-inventory specs")
        .iter()
        .find(|spec| spec["metadata"]["failure_kind"] == "no_progress_timeout")
        .unwrap_or_else(|| panic!("held HEAD must emit typed no-progress: {receipt}"));
    assert!(!output.status.success());
    assert_eq!(failure["metadata"]["no_progress_timeout_secs"], "1");
    assert_eq!(failure["metadata"]["progress_completed_units"], "0");
    let status = read_json(&workload.join("workload-status.json"));
    assert_eq!(status["phase"], "recovery");
    let observations = recovery_observations(&workload);
    assert_eq!(observations["held_requests"], 1);
    assert_eq!(observations["completed_inventory_heads"], 0);
    assert_eq!(observations["completed_inventory_size_validations"], 0);
    assert_eq!(observations["completed_range_reads"], 0);
    assert_eq!(observations["local_wal_bytes"], 0);
    assert_database_stays_empty(&workload);
}

fn should_remain_healthy_when_actual_inventory_validates_delayed_heads() {
    // Arrange: sixteen real SST HEADs complete on a 100ms response script,
    // beyond the one-second idle budget, with RUST_LOG=off and an empty cache.
    let artifacts = tempfile::tempdir().expect("create delayed inventory artifacts");

    // Act
    let output = invoke_child(
        artifacts.path(),
        "metadata_inventory_recovery_with_flat_cache",
    );
    let workload = workload_directory(artifacts.path());

    // Assert: only completed and correctly sized authoritative entries count;
    // the fixture independently verifies exact retained manifest equality.
    let result = assert_successful_recovery_work(&output, &workload);
    assert_eq!(result["expected_inventory_entries"], 16);
    assert_eq!(
        result["retained_inventory_entries"],
        result["expected_inventory_entries"]
    );
    assert_eq!(
        result["completed_inventory_heads"],
        result["expected_inventory_entries"]
    );
    assert_eq!(
        result["completed_inventory_size_validations"],
        result["expected_inventory_entries"]
    );
    assert_eq!(result["expected_records"], 0);
    assert_eq!(result["max_sequence"], 0);
    assert_eq!(result["max_epoch"], 0);
    assert_eq!(result["completed_range_reads"], 0);
    assert_eq!(result["completed_range_bytes"], 0);
}

fn should_remain_healthy_when_cached_recovery_finishes_verified_coverage_work() {
    // Arrange: the real replay and exact SST coverage checks finish over
    // buffered/local data for longer than the watchdog, without new ranges.
    let artifacts = tempfile::tempdir().expect("create cached recovery artifacts");

    // Act
    let output = invoke_child(artifacts.path(), "cached_recovery_with_flat_cache");
    let workload = workload_directory(artifacts.path());

    // Assert
    let result = assert_successful_recovery(&output, &workload);
    assert!(
        result["coverage_checks"].as_u64().unwrap() >= result["expected_records"].as_u64().unwrap()
    );
    assert_eq!(result["replay_completed_range_reads"], 0);
}

fn should_remain_healthy_when_actual_journal_restores_its_durable_frontier() {
    // Arrange: the real manifest loader reads sixteen committed edit and
    // marker pairs through delayed local reads, with RUST_LOG=off.
    let artifacts = tempfile::tempdir().expect("create delayed journal artifacts");

    // Act
    let output = invoke_child(artifacts.path(), "journal_recovery_with_flat_cache");
    let workload = workload_directory(artifacts.path());

    // Assert: independently completed reads sustain recovery beyond the idle
    // budget; the fixture verifies every restored edit and exact frontier.
    let result = assert_successful_recovery_work(&output, &workload);
    assert_eq!(result["expected_edits"], 16);
    assert_eq!(result["verified_edits"], result["expected_edits"]);
    assert_eq!(result["max_edit_id"], result["expected_edits"]);
    assert_eq!(
        result["manifest_edit_checkpoint_id"],
        result["expected_edits"]
    );
    assert_eq!(result["restored_cf_count"], result["expected_edits"]);
    assert!(result["completed_local_reads"].as_u64().unwrap() > 0);
    let journal_bytes = result["journal_bytes"].as_u64().unwrap();
    assert!(journal_bytes > 0);
    assert!(result["completed_local_read_bytes"].as_u64().unwrap() >= journal_bytes);
}

fn should_count_recovery_work_only_within_its_active_caller_scope() {
    // Arrange
    let artifacts = tempfile::tempdir().expect("create listener isolation artifacts");

    // Act: the child drives the real event layer, with its fmt filter off.
    let output = invoke_child(artifacts.path(), "scoped_recovery_listener");
    let receipt = receipt(&output);

    // Assert: exactly three work kinds and two valid caller-scope events count.
    assert!(
        output.status.success(),
        "listener isolation failed: receipt={receipt}; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(receipt["benchmark_specs"]
        .as_array()
        .expect("canonical listener specs")
        .iter()
        .any(|spec| spec["metadata"]["fixture_listener_progress_units"] == "5"));
}

fn main() {
    if std::env::var_os(CHILD_FLAG).is_some() {
        child_harness::run();
        return;
    }
    should_report_no_progress_when_rejected_clients_keep_database_bytes_changing();
    should_remain_healthy_when_successful_clients_resume_before_watchdog_expiry();
    should_count_recovery_work_only_within_its_active_caller_scope();
    should_report_no_progress_when_actual_cloud_recovery_holds_first_range();
    should_report_no_progress_when_actual_inventory_holds_first_head();
    should_remain_healthy_when_actual_journal_restores_its_durable_frontier();
    should_remain_healthy_when_actual_inventory_validates_delayed_heads();
    should_remain_healthy_when_actual_cloud_recovery_completes_read_only_work();
    should_remain_healthy_when_cached_recovery_finishes_verified_coverage_work();
    println!("nine real stress watchdog integration checks passed");
}
