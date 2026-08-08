//! Shared subprocess-crash evidence for fault-injection integration tests.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
#[cfg(feature = "failpoints")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "failpoints")]
use std::sync::Arc;

const TRIGGER_SENTINEL_ENV: &str = "MIDGE_CRASH_TRIGGER_SENTINEL";

fn sentinel_path(db_path: &Path, scenario: &str) -> PathBuf {
    db_path.join(format!(".midge-crash-trigger-{scenario}.sentinel"))
}

fn expected_sentinel(scenario: &str, trigger: &str) -> String {
    format!("scenario={scenario}\ntrigger={trigger}\n")
}

/// Run one child crash scenario and require independent proof that the named
/// trigger fired. A fallback panic or unrelated abort cannot create this
/// exact, synced marker and therefore fails loudly with child output.
pub fn run_child_expect_abort(
    command: &mut Command,
    scenario: &str,
    trigger: &str,
    db_path: &Path,
) {
    let marker = sentinel_path(db_path, scenario);
    if marker.exists() {
        fs::remove_file(&marker).expect("remove stale crash-trigger sentinel");
    }
    command.env(TRIGGER_SENTINEL_ENV, &marker);

    let output = command.output().expect("run child test binary");
    if let Err(error) = validate_child_crash(&output, &marker, scenario, trigger) {
        panic!("{error}");
    }
    clear_crashed_process_acquisition_lock(db_path);
}

/// Remove the filesystem lease-mutation lock left by a child whose exact
/// process abort has already been observed. Crash tests use private storage
/// directories, so no live process can own this path after the child exits.
pub fn clear_crashed_process_acquisition_lock(db_path: &Path) {
    let lock_path = db_path.join(".midge_leader.lock");
    if let Err(error) = fs::remove_file(lock_path) {
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::NotFound,
            "remove crashed process acquisition lock: {error}"
        );
    }
}

/// Validate a child result separately from process execution so governance
/// tests can prove a missing or wrong marker is rejected.
pub fn validate_child_crash(
    output: &Output,
    marker: &Path,
    scenario: &str,
    trigger: &str,
) -> Result<(), String> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if output.status.success() {
        return Err(format!(
            "child scenario {scenario} unexpectedly exited successfully; stdout: {stdout}; stderr: {stderr}"
        ));
    }
    if !status_is_abort(output.status) {
        return Err(format!(
            "child scenario {scenario} failed without process abort ({:?}); stdout: {stdout}; stderr: {stderr}",
            output.status.code()
        ));
    }

    validate_trigger_sentinel(marker, scenario, trigger)
        .map_err(|error| format!("{error}; stdout: {stdout}; stderr: {stderr}"))
}

#[cfg(unix)]
fn status_is_abort(status: std::process::ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt as _;
    status.signal() == Some(libc::SIGABRT)
}

#[cfg(windows)]
fn status_is_abort(status: std::process::ExitStatus) -> bool {
    // Windows CRT abort commonly exits with 3. Rust's platform abort may use
    // STATUS_STACK_BUFFER_OVERRUN (fast-fail) or STATUS_FATAL_APP_EXIT.
    status.code().is_some_and(|code| {
        matches!(
            u32::from_ne_bytes(code.to_ne_bytes()),
            3 | 0xC000_0409 | 0x4000_0015
        )
    })
}

#[cfg(not(any(unix, windows)))]
fn status_is_abort(status: std::process::ExitStatus) -> bool {
    !status.success() && status.code().is_none()
}

/// Require exact, independently persisted evidence for one named trigger.
pub fn validate_trigger_sentinel(
    marker: &Path,
    scenario: &str,
    trigger: &str,
) -> Result<(), String> {
    let reached = fs::read_to_string(marker).map_err(|error| {
        format!("child scenario {scenario} did not prove trigger '{trigger}' fired: {error}")
    })?;
    let expected = expected_sentinel(scenario, trigger);
    if reached != expected {
        return Err(format!(
            "child scenario {scenario} wrote the wrong trigger sentinel: expected {expected:?}, got {reached:?}"
        ));
    }
    Ok(())
}

fn record_trigger(scenario: &str, trigger: &str) {
    let marker_path = PathBuf::from(
        std::env::var_os(TRIGGER_SENTINEL_ENV).expect("crash-trigger sentinel path env"),
    );
    let mut marker = fs::File::create(&marker_path).expect("create crash-trigger sentinel");
    marker
        .write_all(expected_sentinel(scenario, trigger).as_bytes())
        .expect("write crash-trigger sentinel");
    marker.sync_all().expect("sync crash-trigger sentinel");
    if let Some(parent) = marker_path.parent() {
        if let Ok(directory) = fs::File::open(parent) {
            directory
                .sync_all()
                .expect("sync crash-trigger sentinel directory");
        }
    }
}

/// Record a logical crash boundary and abort immediately.
pub fn abort_at_trigger(scenario: &str, trigger: &str) -> ! {
    record_trigger(scenario, trigger);
    std::process::abort();
}

/// Arm a named failpoint so the injection itself records durable evidence and
/// aborts. The `FailScenario` is deliberately leaked because the process must
/// terminate at the injected boundary.
#[cfg(feature = "failpoints")]
pub fn configure_abort_failpoint(name: &str, scenario: &str) {
    let fail_scenario = fail::FailScenario::setup();
    let owned_name = name.to_string();
    let owned_scenario = scenario.to_string();
    fail::cfg_callback(name, move || {
        abort_at_trigger(&owned_scenario, &owned_name);
    })
    .expect("configure abort failpoint");
    std::mem::forget(fail_scenario);
}

/// Arm the Nth evaluation of a named failpoint as the intended crash boundary.
#[cfg(feature = "failpoints")]
pub fn configure_nth_abort_failpoint(name: &str, scenario: &str, target: usize) {
    assert!(target > 0, "failpoint target must be positive");
    let fail_scenario = fail::FailScenario::setup();
    let owned_name = name.to_string();
    let owned_scenario = scenario.to_string();
    let hits = Arc::new(AtomicUsize::new(0));
    let callback_hits = Arc::clone(&hits);
    fail::cfg_callback(name, move || {
        if callback_hits.fetch_add(1, Ordering::SeqCst) + 1 == target {
            abort_at_trigger(&owned_scenario, &owned_name);
        }
    })
    .expect("configure nth abort failpoint");
    std::mem::forget(fail_scenario);
}
