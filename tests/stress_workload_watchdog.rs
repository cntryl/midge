//! Check the real external watchdog without adding a production stall control.
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

#[allow(dead_code)]
#[path = "../benches/bench_support/stress_scenarios.rs"]
mod stress_scenarios;

const CHILD_FLAG: &str = "MIDGE_WATCHDOG_TEST_CHILD";
const PREPARATION_READY_ENV: &str = "MIDGE_WATCHDOG_PREPARATION_READY";
const PREPARATION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum ChildTimeoutKind {
    PreparationTimeout,
    ChildExecutionTimeout,
}

#[derive(Debug, serde::Serialize)]
struct ChildTimeout {
    failure_kind: ChildTimeoutKind,
    elapsed_ms: u128,
    limit_ms: u128,
    child_id: u32,
    child_reaped: bool,
}

fn run_bounded_child(
    command: &mut Command,
    artifacts: &Path,
    preparation_timeout: Duration,
    execution_timeout: Duration,
) -> Result<Output, ChildTimeout> {
    let stdout = artifacts.join("child-stdout.json");
    let stderr = artifacts.join("child-stderr.log");
    let prepared = artifacts.join("preparation-complete.json");
    let mut child = command
        .env(PREPARATION_READY_ENV, &prepared)
        .stdout(Stdio::from(
            fs::File::create(&stdout).expect("retain child stdout"),
        ))
        .stderr(Stdio::from(
            fs::File::create(&stderr).expect("retain child stderr"),
        ))
        .spawn()
        .expect("spawn watchdog fixture child");
    let started = Instant::now();
    let mut active_started = None;
    let mut timeout = None;
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll watchdog fixture child") {
            break status;
        }
        if active_started.is_none() && prepared.is_file() {
            active_started = Some(Instant::now());
        }
        let (origin, limit, kind) = active_started.map_or(
            (
                started,
                preparation_timeout,
                ChildTimeoutKind::PreparationTimeout,
            ),
            |origin| {
                (
                    origin,
                    execution_timeout,
                    ChildTimeoutKind::ChildExecutionTimeout,
                )
            },
        );
        if origin.elapsed() >= limit {
            child
                .kill()
                .expect("kill watchdog child after parent deadline");
            let status = child.wait().expect("reap killed watchdog child");
            timeout = Some(ChildTimeout {
                failure_kind: kind,
                elapsed_ms: origin.elapsed().as_millis(),
                limit_ms: limit.as_millis(),
                child_id: child.id(),
                child_reaped: true,
            });
            break status;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    fs::write(artifacts.join("child-status.txt"), status.to_string())
        .expect("retain child exit status");
    if let Some(timeout) = timeout {
        fs::write(
            artifacts.join("child-timeout.json"),
            serde_json::to_vec_pretty(&timeout).unwrap(),
        )
        .expect("retain typed parent timeout");
        return Err(timeout);
    }
    Ok(Output {
        status,
        stdout: fs::read(stdout).expect("read retained child stdout"),
        stderr: fs::read(stderr).expect("read retained child stderr"),
    })
}

fn should_bound_child_wait_when_preparation_never_completes() {
    // Arrange: a committed private child mode hangs before setup can finish.
    let artifacts = control_artifacts("held-preparation-");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.arg("--hold-preparation");
    // Act
    let result = run_bounded_child(
        &mut command,
        artifacts.path(),
        Duration::from_millis(100),
        Duration::from_secs(1),
    );
    // Assert: this is preparation failure, separate from the native work watchdog.
    let timeout = result.expect_err("hung preparation must fail within the parent budget");
    assert_eq!(timeout.failure_kind, ChildTimeoutKind::PreparationTimeout);
    assert!(timeout.child_reaped);
    let report = read_json(&artifacts.path().join("child-timeout.json"));
    assert_eq!(report["failure_kind"], "preparation_timeout");
    assert_eq!(report["child_reaped"], true);
    assert!(!artifacts.path().join("preparation-complete.json").exists());
}

fn control_artifacts(label: &str) -> tempfile::TempDir {
    if let Some(root) = std::env::var_os("MIDGE_WATCHDOG_EVIDENCE_DIR") {
        fs::create_dir_all(&root).expect("create watchdog evidence directory");
        let artifacts = tempfile::Builder::new()
            .prefix(label)
            .disable_cleanup(true)
            .tempdir_in(root)
            .expect("retain watchdog control artifacts");
        eprintln!("watchdog evidence: {}", artifacts.path().display());
        artifacts
    } else {
        tempfile::tempdir().expect("create watchdog control artifacts")
    }
}

fn prepare_child(
    workload: &str,
    samples: usize,
) -> Vec<Box<dyn stress_scenarios::PreparationGuard>> {
    use cntryl_midge::__internal::recovery::RecoveryProgressFixtureMode as Mode;
    use std::time::Duration;
    let mut scopes: Vec<Box<dyn stress_scenarios::PreparationGuard>> = Vec::new();
    let mode = match workload {
        "delayed_recovery_with_flat_cache" => Some(Mode::DelayedRanges),
        "held_recovery_with_flat_cache" => Some(Mode::HeldFirstRange),
        "cached_recovery_with_flat_cache" => Some(Mode::CachedCoverage),
        "metadata_inventory_recovery_with_flat_cache" => Some(Mode::MetadataInventory),
        "held_inventory_recovery_with_flat_cache" => Some(Mode::HeldInventory),
        _ => None,
    };
    if let Some(mode) = mode {
        let delay = if mode == Mode::MetadataInventory {
            Duration::from_millis(1_200)
        } else {
            Duration::ZERO
        };
        scopes.push(Box::new(
            stress_scenarios::prepare_recovery_watchdog_fixture(mode, samples, delay),
        ));
    }
    #[cfg(feature = "failpoints")]
    {
        use stress_scenarios::FlushFixtureKind as Kind;
        let kind = match workload {
            "delayed_final_flush_publication" => Some(Kind::Delayed),
            "held_final_flush_publication" => Some(Kind::Held),
            "terminal_final_flush_retry_policy" => Some(Kind::TerminalPolicy),
            _ => None,
        };
        if let Some(kind) = kind {
            let delay = if kind == Kind::Held {
                Duration::ZERO
            } else {
                Duration::from_millis(1_200)
            };
            scopes.push(Box::new(
                stress_scenarios::prepare_final_flush_watchdog_fixture(kind, samples, delay),
            ));
        }
    }
    scopes
}

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

    #[cfg(feature = "failpoints")]
    #[cntryl_stress::stress(tier = 5)]
    fn delayed_final_flush_publication(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_final_flush_watchdog_fixture(ctx, false);
    }

    #[cfg(feature = "failpoints")]
    #[cntryl_stress::stress(tier = 5)]
    fn held_final_flush_publication(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_final_flush_watchdog_fixture(ctx, true);
    }

    #[cfg(feature = "failpoints")]
    #[cntryl_stress::stress(tier = 5)]
    fn terminal_final_flush_retry_policy(ctx: &mut cntryl_stress::StressContext) {
        super::stress_scenarios::run_final_flush_terminal_policy_fixture(ctx);
    }

    cntryl_stress::stress_main!();

    pub(super) fn run() {
        main();
    }
}

fn invoke_child(artifacts: &Path, workload: &str) -> Output {
    invoke_child_with_samples(artifacts, workload, 1, 0, 0)
}

fn invoke_child_with_samples(
    artifacts: &Path,
    workload: &str,
    samples: usize,
    warmup: usize,
    cooldown: usize,
) -> Output {
    let mut command = Command::new(std::env::current_exe().expect("watchdog test executable"));
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("STRESS_") {
            command.env_remove(name);
        }
    }
    if workload.ends_with("recovery_with_flat_cache")
        || workload.ends_with("final_flush_publication")
        || workload == "terminal_final_flush_retry_policy"
    {
        command.env("RUST_LOG", "off");
    }
    // Healthy end-to-end and policy fixtures include real I/O under host
    // contention, including synchronous evidence writes. A retained macOS run
    // exceeded three seconds despite a 423ms publication pause. Give these
    // test-only success controls ten seconds; production settings are unchanged.
    // Deliberate no-work controls retain a one-second native deadline and must
    // still fail with zero completed units.
    let no_progress_timeout =
        if workload.starts_with("held_") || workload == "rejected_clients_with_disk_churn" {
            "1"
        } else {
            "10"
        };
    command
        .arg("--workload")
        .arg(workload)
        .env(CHILD_FLAG, "1")
        .env("STRESS_PROFILE", "smoke")
        .env("STRESS_SAMPLES", samples.to_string())
        .env("STRESS_WARMUP_SAMPLES", warmup.to_string())
        .env("STRESS_COOLDOWN_SAMPLES", cooldown.to_string())
        .env("STRESS_NO_PROGRESS_TIMEOUT_SECS", no_progress_timeout)
        .env("STRESS_JSON", "true")
        .env("STRESS_OUTPUT_DIR", artifacts.join("stress"))
        .env("MIDGE_STRESS_ARTIFACT_DIR", artifacts.join("midge"))
        .env_remove("GITHUB_ACTIONS");
    let total = samples
        .saturating_add(warmup)
        .saturating_add(cooldown)
        .max(1);
    let execution_timeout =
        Duration::from_secs(15).saturating_mul(u32::try_from(total).unwrap_or(u32::MAX));
    run_bounded_child(
        &mut command,
        artifacts,
        PREPARATION_TIMEOUT,
        execution_timeout,
    )
    .unwrap_or_else(|timeout| {
        panic!(
            "watchdog fixture child exceeded parent deadline: {}",
            serde_json::to_string(&timeout).unwrap()
        )
    })
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

#[cfg(feature = "failpoints")]
fn decode_final_flush_attempts(bytes: &[u8], allow_torn_tail: bool) -> Result<Vec<Value>, String> {
    let mut attempts = Vec::new();
    for record in bytes.split_inclusive(|byte| *byte == b'\n') {
        // A newline commits one JSONL record. Native process termination can
        // interrupt the formatter before its final newline; only that last
        // uncommitted record may be ignored, and only for aborted children.
        if !record.ends_with(b"\n") {
            if allow_torn_tail {
                break;
            }
            return Err("successful child left an uncommitted final attempt".into());
        }
        attempts.push(serde_json::from_slice(record).map_err(|error| error.to_string())?);
    }
    Ok(attempts)
}

#[cfg(feature = "failpoints")]
fn read_final_flush_attempts(workload: &Path, allow_torn_tail: bool) -> Vec<Value> {
    let bytes = fs::read(workload.join("final-flush-attempts.jsonl"))
        .expect("retained actual final-flush results");
    decode_final_flush_attempts(&bytes, allow_torn_tail)
        .expect("valid committed final-flush receipts")
}

#[cfg(feature = "failpoints")]
fn final_flush_attempts(workload: &Path) -> Vec<Value> {
    read_final_flush_attempts(workload, false)
}

#[cfg(feature = "failpoints")]
fn aborted_final_flush_attempts(workload: &Path) -> Vec<Value> {
    read_final_flush_attempts(workload, true)
}

#[cfg(feature = "failpoints")]
fn should_read_committed_attempts_when_watchdog_aborts_mid_record() {
    // Arrange
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("final-flush-attempts.jsonl"),
        b"{\"success\":false}\n{\"success\":",
    )
    .unwrap();
    // Act
    let attempts = aborted_final_flush_attempts(dir.path());
    // Assert
    assert_eq!(attempts, vec![serde_json::json!({"success": false})]);
}

#[cfg(feature = "failpoints")]
fn should_reject_malformed_committed_attempts_when_watchdog_aborts() {
    // Arrange
    let bytes = b"{malformed}\n";
    // Act
    let result = decode_final_flush_attempts(bytes, true);
    // Assert
    assert!(result.is_err());
}

fn client_snapshot(workload: &Path) -> Value {
    read_json(&workload.join("client-snapshots/stage-00/client-00.json"))
}

fn should_report_no_progress_when_rejected_clients_keep_database_bytes_changing() {
    // Arrange
    let artifacts = control_artifacts("rejected-clients-");

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
    let artifacts = control_artifacts("resumed-successes-");

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
        "actual progressing recovery must survive its fixture watchdog; status={}, observations={observations}; receipt={receipt}; stderr={}",
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
    assert!(result["progress_units"]
        .as_u64()
        .is_some_and(|units| units > 0));
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
    let artifacts = control_artifacts("delayed-recovery-");

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
    let artifacts = control_artifacts("held-recovery-");

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
    let artifacts = control_artifacts("held-inventory-");

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
    let artifacts = control_artifacts("delayed-inventory-");

    // Act
    let output = invoke_child(
        artifacts.path(),
        "metadata_inventory_recovery_with_flat_cache",
    );
    let workload = workload_directory(artifacts.path());

    // Assert: only completed and correctly sized authoritative entries count;
    // the fixture independently verifies exact retained manifest equality.
    let result = assert_successful_recovery_work(&output, &workload);
    assert!(
        read_json(&workload.join("fixture-preparation.json"))["setup_elapsed_ms"]
            .as_u64()
            .unwrap()
            >= 1_200
    );
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
    let artifacts = control_artifacts("cached-recovery-");

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
    let artifacts = control_artifacts("delayed-journal-");

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
    let artifacts = control_artifacts("listener-isolation-");

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

#[cfg(feature = "failpoints")]
fn should_retry_final_flush_when_real_publication_outlives_its_caller_slice() {
    // Arrange: one genuine publication is delayed 350ms, with a healthy
    // acquired primary lease and actual callers waiting only 50ms each.
    let artifacts = control_artifacts("delayed-final-flush-");

    // Act: the native child invokes the same final-flush boundary as the soaks.
    let output = invoke_child(artifacts.path(), "delayed_final_flush_publication");
    let receipt = receipt(&output);
    let workload = workload_directory(artifacts.path());
    let publication = read_json(&workload.join("publication-observations.json"));
    let activation = read_json(&workload.join("active-work-start.json"));
    let outcomes = read_json(&workload.join("flush-attempt-observations.json"));

    // Assert: a timeout is not successful flush, and abandoned callers cannot
    // abort the accepted publication or allow verification to run early.
    assert!(
        output.status.success(),
        "real eventual flush must precede verification; status={}, publication={publication}, outcomes={outcomes}, receipt={receipt}; stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(publication["worker_entries"], 1);
    assert_eq!(publication["worker_released"], true);
    assert!(activation["setup_elapsed_ms"].as_u64().unwrap() >= 1_200);
    assert!(publication["publication_elapsed_ms"].as_u64().unwrap() >= 350);
    let attempts = outcomes["attempts"]
        .as_array()
        .expect("actual flush attempts");
    assert!(attempts.len() > 1);
    assert_eq!(attempts[0]["error_kind"], "timeout");
    assert!(attempts[..attempts.len() - 1].iter().all(|attempt| {
        attempt["success"] == false
            && attempt["error_kind"] == "timeout"
            && attempt["observed_lease_healthy"] == true
            && attempt["progress_units"] == 0
    }));
    assert_eq!(attempts.last().unwrap()["success"], true);
    let result = read_json(&workload.join("flush-result.json"));
    assert_eq!(
        result["attempts"].as_u64(),
        Some(u64::try_from(attempts.len()).expect("fixture attempts fit u64"))
    );
    assert_eq!(result["flush_progress_units"], 1);
    assert_eq!(result["worker_entries"], 1);
    assert_eq!(result["flush_publish_count"], 1);
    assert_eq!(result["authoritative_sst_count"], 1);
    assert_eq!(result["verified_rows"], 32);
    assert_eq!(result["mismatches"], 0);
    let verification = read_json(&workload.join("verification-summary.json"));
    assert_eq!(verification["passed"], true);
    assert!(verification["checks"]
        .as_array()
        .expect("actual verification checks")
        .iter()
        .all(|check| check["passed"] == true && check["value_mismatches"] == 0));
    let status = read_json(&workload.join("workload-status.json"));
    assert_eq!(status["status"], "passed");
    assert_eq!(status["terminal_error"], Value::Null);
    assert_eq!(status["final_flush"]["completed"], true);
    assert_eq!(status["final_flush"]["operation_in_flight"], false);
    assert_eq!(status["final_flush"]["attempts"], result["attempts"]);
    assert_eq!(status["final_flush"]["last_error_kind"], "timeout");
    let recorded_attempts = final_flush_attempts(&workload);
    assert_eq!(recorded_attempts.len(), attempts.len());
    assert!(recorded_attempts[..recorded_attempts.len() - 1]
        .iter()
        .all(|attempt| attempt["error_kind"] == "timeout"
            && attempt["retry"] == true
            && attempt["lease_healthy_before"] == true
            && attempt["lease_healthy_after"] == true
            && attempt["progress_completed_units"] == 0));
    assert_eq!(recorded_attempts.last().unwrap()["success"], true);
    assert!(receipt["benchmark_specs"]
        .as_array()
        .expect("canonical final-flush specs")
        .iter()
        .all(|spec| spec["metadata"]["failure_kind"] != "no_progress_timeout"));
}

#[cfg(feature = "failpoints")]
fn should_report_no_progress_when_final_flush_publication_remains_held() {
    // Arrange: accepted real publication is retained while caller slots expire.
    let artifacts = control_artifacts("held-final-flush-");

    // Act: only the native watchdog decides that retries made no progress.
    let output = invoke_child(artifacts.path(), "held_final_flush_publication");
    let receipt = receipt(&output);
    let workload = workload_directory(artifacts.path());

    // Assert: attempts, timeout responses and sleeps never make work healthy.
    assert!(!output.status.success());
    let failure = receipt["benchmark_specs"]
        .as_array()
        .expect("canonical held final-flush specs")
        .iter()
        .find(|spec| spec["metadata"]["failure_kind"] == "no_progress_timeout")
        .unwrap_or_else(|| panic!("held publication must native-timeout: {receipt}"));
    assert_eq!(failure["metadata"]["no_progress_timeout_secs"], "1");
    assert_eq!(failure["metadata"]["progress_completed_units"], "0");
    let status = read_json(&workload.join("workload-status.json"));
    assert_eq!(status["phase"], "flush");
    assert_eq!(status["final_flush"]["completed"], false);
    assert_eq!(status["final_flush"]["last_error_kind"], "timeout");
    assert_eq!(
        status["final_flush"]["observed_primary_lease_healthy"],
        true
    );
    let publication = read_json(&workload.join("publication-observations.json"));
    assert_eq!(publication["worker_entries"], 1);
    assert_eq!(publication["worker_released"], false);
    let outcomes = read_json(&workload.join("flush-attempt-observations.json"));
    let attempts = outcomes["attempts"]
        .as_array()
        .expect("actual abandoned callers");
    assert!(attempts.len() > 1);
    assert!(attempts.iter().all(|attempt| {
        attempt["success"] == false
            && attempt["error_kind"] == "timeout"
            && attempt["observed_lease_healthy"] == true
            && attempt["progress_units"] == 0
    }));
    let recorded_attempts = aborted_final_flush_attempts(&workload);
    assert!(recorded_attempts.len() > 1);
    assert!(recorded_attempts.iter().all(|attempt| {
        attempt["success"] == false
            && attempt["error_kind"] == "timeout"
            && attempt["retry"] == true
            && attempt["lease_healthy_before"] == true
            && attempt["lease_healthy_after"] == true
            && attempt["progress_completed_units"] == 0
    }));
    assert!(!workload.join("flush-result.json").exists());
    assert!(!workload.join("verification-summary.json").exists());
}

#[cfg(feature = "failpoints")]
fn should_stop_retry_policy_when_callback_returns_a_terminal_error() {
    // Arrange: errors originate at the benchmark callback boundary. A real
    // healthy Engine backs the control and any unexpected second invocation.
    let artifacts = control_artifacts("terminal-retry-policy-");

    // Act: this proves benchmark classification, not provider error behavior.
    let output = invoke_child(artifacts.path(), "terminal_final_flush_retry_policy");
    let receipt = receipt(&output);
    let workload = workload_directory(artifacts.path());

    // Assert: original error, one call, no success/heartbeat or verification.
    assert!(
        output.status.success(),
        "terminal policy controls failed; receipt={receipt}; stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        read_json(&workload.join("fixture-preparation.json"))["setup_elapsed_ms"]
            .as_u64()
            .unwrap()
            >= 1_200
    );
    let results = read_json(&workload.join("retry-policy-results.json"));
    assert_eq!(results["fixture_scope"], "benchmark_retry_policy");
    assert_eq!(results["error_source"], "callback_injected");
    assert_eq!(results["provider_error_behavior_proved"], false);
    assert_eq!(results["verification_checks"], 0);
    let cases = results["cases"].as_array().expect("typed policy controls");
    let expected_kinds = [
        "resource_limit",
        "fenced",
        "corruption",
        "internal",
        "io",
        "no_space",
        "lease_unavailable",
        "aborted",
    ];
    assert_eq!(cases.len(), expected_kinds.len());
    for (case, expected_kind) in cases.iter().zip(expected_kinds) {
        assert_eq!(case["kind"], expected_kind);
        assert_eq!(case["error_source"], "callback_injected");
        assert_eq!(case["calls"], 1);
        assert_eq!(case["returned_error"], case["original_error"]);
        assert_eq!(case["progress_units"], 0);
        assert_eq!(case["completed"], false);
        assert_eq!(case["operation_in_flight"], false);
        assert_eq!(case["observed_primary_lease_healthy"], true);
        assert_eq!(case["persisted"]["retry"], false);
        assert_eq!(case["persisted"]["success"], false);
        assert_eq!(case["persisted"]["error"], case["original_error"]);
        assert_eq!(case["persisted"]["lease_healthy_before"], true);
        assert_eq!(case["persisted"]["lease_healthy_after"], true);
        assert_eq!(case["persisted"]["progress_completed_units"], 0);
    }
    let recorded_attempts = final_flush_attempts(&workload);
    assert_eq!(recorded_attempts.len(), expected_kinds.len());
    assert!(recorded_attempts
        .iter()
        .all(|attempt| attempt["retry"] == false && attempt["success"] == false));
    assert!(!workload.join("verification-summary.json").exists());
}

fn should_reject_invalid_configuration_before_preparing_watchdog_resources() {
    // Arrange
    let artifacts = control_artifacts("rejected-configuration-");
    // Act
    let output = invoke_child_with_samples(
        artifacts.path(),
        "metadata_inventory_recovery_with_flat_cache",
        0,
        0,
        0,
    );
    // Assert
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid STRESS_SAMPLES"));
    assert!(!artifacts.path().join("midge").exists());
}

fn should_cancel_unused_inputs_when_preparation_scope_ends() {
    // Arrange: the custom parent runs controls serially, with no live worker.
    let artifacts = control_artifacts("canceled-preparation-");
    let original = std::env::var_os("MIDGE_STRESS_ARTIFACT_DIR");
    std::env::set_var("MIDGE_STRESS_ARTIFACT_DIR", artifacts.path());
    let original_log = std::env::var_os("RUST_LOG");
    std::env::set_var("RUST_LOG", "off");
    // Act: unused scopes release every input; a new scope can reuse the slot.
    for _ in 0..2 {
        drop(prepare_child("held_inventory_recovery_with_flat_cache", 2));
    }
    #[cfg(feature = "failpoints")]
    drop(prepare_child("held_final_flush_publication", 2));
    match original {
        Some(value) => std::env::set_var("MIDGE_STRESS_ARTIFACT_DIR", value),
        None => std::env::remove_var("MIDGE_STRESS_ARTIFACT_DIR"),
    }
    match original_log {
        Some(value) => std::env::set_var("RUST_LOG", value),
        None => std::env::remove_var("RUST_LOG"),
    }
    // Assert
    let paths: Vec<_> = fs::read_dir(artifacts.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        paths.len(),
        if cfg!(feature = "failpoints") { 6 } else { 4 }
    );
    for path in paths {
        let status = read_json(&path.join("workload-status.json"));
        assert_eq!(status["status"], "canceled");
        let error = status["terminal_error"].as_str().unwrap();
        assert!(error.contains("prepared fixture was not invoked"));
        if status["backend"] == "cloud-simulated" {
            assert!(error.contains("shutdown=Ok(())"));
        }
    }
}

fn should_prepare_fresh_inputs_when_native_samples_repeat() {
    // Arrange
    let workloads = [
        "metadata_inventory_recovery_with_flat_cache",
        "delayed_recovery_with_flat_cache",
    ];
    for workload in workloads {
        let artifacts = control_artifacts("repeated-recovery-");
        // Act: two measured samples plus warmup/cooldown, each with its own input.
        let output = invoke_child_with_samples(artifacts.path(), workload, 2, 1, 1);
        // Assert
        assert!(
            output.status.success(),
            "repeated {workload}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let receipt = receipt(&output);
        assert_eq!(receipt["samples"].as_array().unwrap().len(), 4);
        let paths: Vec<_> = fs::read_dir(artifacts.path().join("midge"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(paths.len(), 4);
        for path in paths {
            let result = assert_successful_recovery_work(&output, &path);
            assert_eq!(result["mismatches"], 0);
        }
    }
    #[cfg(feature = "failpoints")]
    for workload in [
        "delayed_final_flush_publication",
        "terminal_final_flush_retry_policy",
    ] {
        let artifacts = control_artifacts("repeated-publication-");
        let output = invoke_child_with_samples(artifacts.path(), workload, 2, 1, 1);
        assert!(
            output.status.success(),
            "repeated {workload}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(receipt(&output)["samples"].as_array().unwrap().len(), 4);
        let paths: Vec<_> = fs::read_dir(artifacts.path().join("midge"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(paths.len(), 4);
        for path in paths {
            assert_eq!(
                read_json(&path.join("workload-status.json"))["status"],
                "passed"
            );
            if workload == "delayed_final_flush_publication" {
                assert_eq!(read_json(&path.join("flush-result.json"))["mismatches"], 0);
            } else {
                assert!(read_json(&path.join("retry-policy-results.json"))["cases"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|case| case["calls"] == 1));
            }
        }
    }
}

fn main() {
    if std::env::args().any(|arg| arg == "--hold-preparation") {
        loop {
            std::thread::park();
        }
    }
    if std::env::var_os(CHILD_FLAG).is_some() {
        let args: Vec<_> = std::env::args().collect();
        let workload = args
            .windows(2)
            .find(|pair| pair[0] == "--workload")
            .map(|pair| pair[1].as_str())
            .expect("child workload selector");
        assert_eq!(
            args.len(),
            3,
            "private child accepts only --workload; configure samples through STRESS_* environment"
        );
        let config = cntryl_stress::StressRunnerConfig::from_env();
        assert!(
            config.validation_errors().is_empty(),
            "invalid child configuration: {:?}",
            config.validation_errors()
        );
        assert!(
            config.tier.is_none_or(|tier| tier == 5),
            "watchdog fixtures are tier 5"
        );
        let samples = config
            .samples
            .checked_add(config.warmup_samples)
            .and_then(|n| n.checked_add(config.cooldown_samples))
            .expect("sample count overflow");
        let _preparation = prepare_child(workload, samples);
        if let Some(path) = std::env::var_os(PREPARATION_READY_ENV) {
            fs::write(path, b"{\"prepared\":true}").expect("signal completed child preparation");
        }
        child_harness::run();
        return;
    }
    let args: Vec<_> = std::env::args().collect();
    if let Some(control) = args
        .windows(2)
        .find(|pair| pair[0] == "--control")
        .map(|pair| pair[1].as_str())
    {
        match control {
            "lifecycle" => {
                should_bound_child_wait_when_preparation_never_completes();
                should_reject_invalid_configuration_before_preparing_watchdog_resources();
                should_cancel_unused_inputs_when_preparation_scope_ends();
            }
            "inventory" => should_remain_healthy_when_actual_inventory_validates_delayed_heads(),
            #[cfg(feature = "failpoints")]
            "receipts" => {
                should_read_committed_attempts_when_watchdog_aborts_mid_record();
                should_reject_malformed_committed_attempts_when_watchdog_aborts();
            }
            #[cfg(feature = "failpoints")]
            "policy" => should_stop_retry_policy_when_callback_returns_a_terminal_error(),
            #[cfg(feature = "failpoints")]
            "final-flush" => {
                should_retry_final_flush_when_real_publication_outlives_its_caller_slice();
            }
            _ => panic!("unknown isolated watchdog control: {control}"),
        }
        println!(
            "isolated {control} watchdog control passed (explicit subset; full suite not run)"
        );
        return;
    }
    should_bound_child_wait_when_preparation_never_completes();
    should_reject_invalid_configuration_before_preparing_watchdog_resources();
    should_cancel_unused_inputs_when_preparation_scope_ends();
    should_prepare_fresh_inputs_when_native_samples_repeat();
    should_report_no_progress_when_rejected_clients_keep_database_bytes_changing();
    should_remain_healthy_when_successful_clients_resume_before_watchdog_expiry();
    should_count_recovery_work_only_within_its_active_caller_scope();
    should_report_no_progress_when_actual_cloud_recovery_holds_first_range();
    should_report_no_progress_when_actual_inventory_holds_first_head();
    should_remain_healthy_when_actual_journal_restores_its_durable_frontier();
    should_remain_healthy_when_actual_inventory_validates_delayed_heads();
    should_remain_healthy_when_actual_cloud_recovery_completes_read_only_work();
    should_remain_healthy_when_cached_recovery_finishes_verified_coverage_work();
    #[cfg(feature = "failpoints")]
    {
        should_read_committed_attempts_when_watchdog_aborts_mid_record();
        should_reject_malformed_committed_attempts_when_watchdog_aborts();
        should_retry_final_flush_when_real_publication_outlives_its_caller_slice();
        should_report_no_progress_when_final_flush_publication_remains_held();
        should_stop_retry_policy_when_callback_returns_a_terminal_error();
        println!("eighteen real stress watchdog integration checks passed");
    }
    #[cfg(not(feature = "failpoints"))]
    println!("thirteen real stress watchdog integration checks passed");
}
