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

fn main() {
    if std::env::var_os(CHILD_FLAG).is_some() {
        child_harness::run();
        return;
    }
    should_report_no_progress_when_rejected_clients_keep_database_bytes_changing();
    should_remain_healthy_when_successful_clients_resume_before_watchdog_expiry();
    println!("two real stress watchdog integration checks passed");
}
