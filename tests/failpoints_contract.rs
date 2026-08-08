mod common;

#[cfg(feature = "failpoints")]
use common::crash;
use std::path::PathBuf;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn repository_file(path: &str) -> String {
    std::fs::read_to_string(repository_root().join(path))
        .unwrap_or_else(|error| panic!("read {path}: {error}"))
}

fn rust_files_below(root: &std::path::Path) -> Vec<PathBuf> {
    fn visit(directory: &std::path::Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(directory).expect("read source directory") {
            let path = entry.expect("read source entry").path();
            if path.is_dir() {
                visit(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    let mut files = Vec::new();
    visit(root, &mut files);
    files.sort();
    files
}

fn direct_failpoint_bypasses(root: &std::path::Path, adapter: &std::path::Path) -> Vec<PathBuf> {
    rust_files_below(root)
        .into_iter()
        .filter(|path| path != adapter)
        .filter(|path| {
            let source = std::fs::read_to_string(path).expect("read Rust source");
            source.contains("fail::fail_point!") || source.contains("fail::eval(")
        })
        .collect()
}

fn poison_fragile_test_locks(roots: &[PathBuf]) -> Vec<PathBuf> {
    let expect_pattern = [".lock()", ".expect("].concat();
    let unwrap_pattern = [".lock()", ".unwrap("].concat();
    let mut fragile = Vec::new();
    for root in roots {
        for path in rust_files_below(root) {
            let source = std::fs::read_to_string(&path).expect("read Rust source");
            let compact: String = source
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect();
            if compact.contains(&expect_pattern) || compact.contains(&unwrap_pattern) {
                fragile.push(path);
            }
        }
    }
    fragile.sort();
    fragile
}

#[test]
fn should_exclude_fail_dependency_when_default_features_are_selected() {
    // Arrange
    let manifest = repository_file("Cargo.toml");
    let default_features = manifest
        .lines()
        .find(|line| line.starts_with("default = "))
        .expect("default feature declaration");

    // Act
    let fail_dependency_is_optional = manifest
        .contains("fail = { version = \"0.5\", features = [\"failpoints\"], optional = true }");
    let explicit_feature_exists = manifest.contains("failpoints = [\"dep:fail\"]");

    // Assert
    assert!(fail_dependency_is_optional);
    assert!(explicit_feature_exists);
    assert!(!default_features.contains("failpoints"));
}

#[test]
fn should_require_failpoints_feature_when_injection_only_targets_are_selected() {
    // Arrange
    let manifest = repository_file("Cargo.toml");
    let injection_targets = [
        "failure_injection",
        "transaction_crash_boundaries",
        "chaos_real",
        "chaos_intent_log",
        "chaos_compaction",
        "background_flush_pipeline",
        "shutdown_orchestration",
    ];

    // Act
    let missing_gate = injection_targets.iter().find(|target| {
        let declaration = format!(
            "name = \"{target}\"\npath = \"tests/{target}.rs\"\nrequired-features = [\"failpoints\"]"
        );
        !manifest.contains(&declaration)
    });

    // Assert
    assert_eq!(missing_gate, None);
}

#[test]
fn should_route_production_injection_through_internal_adapter() {
    // Arrange
    let source_root = repository_root().join("src");
    let adapter = source_root.join("failpoints.rs");

    // Act
    let direct_production_references = direct_failpoint_bypasses(&source_root, &adapter);

    // Assert
    assert!(
        direct_production_references.is_empty(),
        "production code bypassed src/failpoints.rs: {direct_production_references:?}"
    );
}

#[test]
fn should_flag_new_production_file_when_it_bypasses_internal_failpoint_adapter() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let source_root = temp_dir.path().join("src");
    let nested = source_root.join("runtime/event_loop");
    std::fs::create_dir_all(&nested).expect("create nested source directory");
    let adapter = source_root.join("failpoints.rs");
    std::fs::write(&adapter, "macro_rules! fail_point { () => {} }\n")
        .expect("write adapter fixture");
    let bypass = nested.join("bypass.rs");
    std::fs::write(&bypass, "fn inject() { fail::fail_point!(\"raw\"); }\n")
        .expect("write bypass fixture");

    // Act
    let detected = direct_failpoint_bypasses(&source_root, &adapter);

    // Assert
    assert_eq!(detected, vec![bypass]);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_report_specific_failpoint_marker_when_child_process_aborts_at_intended_boundary() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let marker = temp_dir.path().join("trigger.sentinel");

    // Act
    let missing = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");
    std::fs::write(&marker, "scenario=scenario\ntrigger=wrong-trigger\n")
        .expect("write wrong trigger sentinel");
    let wrong = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");
    std::fs::write(&marker, "scenario=scenario\ntrigger=expected-trigger\n")
        .expect("write expected trigger sentinel");
    let exact = crash::validate_trigger_sentinel(&marker, "scenario", "expected-trigger");

    // Assert
    assert!(missing.is_err());
    assert!(wrong.is_err());
    assert_eq!(exact, Ok(()));
}

#[cfg(feature = "failpoints")]
#[test]
fn should_reject_non_abort_child_failure_even_when_trigger_marker_matches() {
    // Arrange
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let marker = temp_dir.path().join("trigger.sentinel");
    std::fs::write(&marker, "scenario=scenario\ntrigger=expected-trigger\n")
        .expect("write exact trigger sentinel");
    let output = std::process::Command::new(
        std::env::current_exe().expect("locate failpoint contract test executable"),
    )
    .arg("--definitely-not-a-valid-test-harness-option")
    .output()
    .expect("run ordinary failing child");

    // Act
    let validation = crash::validate_child_crash(&output, &marker, "scenario", "expected-trigger");

    // Assert
    assert!(validation
        .expect_err("ordinary failure must not count as an abort")
        .contains("failed without process abort"));
}

#[test]
fn should_use_poison_tolerant_shared_test_locks_across_repository() {
    // Arrange
    let root = repository_root();
    let source_roots = [root.join("src"), root.join("tests")];

    // Act
    let fragile = poison_fragile_test_locks(&source_roots);

    // Assert
    assert!(
        fragile.is_empty(),
        "shared test locks must recover poisoned guards: {fragile:?}"
    );
}

#[test]
fn should_cascade_no_further_test_failures_when_prior_failpoint_guard_panics() {
    // Arrange
    let lock = std::sync::Arc::new(std::sync::Mutex::new(()));
    let poisoner_lock = std::sync::Arc::clone(&lock);
    let poisoner = std::thread::spawn(move || {
        let _guard = poisoner_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        panic!("synthetic assertion failure while holding test guard");
    });
    assert!(poisoner.join().is_err());

    // Act
    let recovered = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    // Assert
    drop(recovered);
    assert!(lock.is_poisoned());
}

#[test]
fn should_verify_default_release_graph_excludes_failpoints_in_workflows() {
    // Arrange
    let ci = repository_file(".github/workflows/ci.yml");
    let publish = repository_file(".github/workflows/publish.yml");
    let graph_check = "cargo tree --edges normal | grep -E '(^|[[:space:]])fail v'";

    // Act
    let ci_has_gate = ci.contains(graph_check) && ci.contains("cargo check --release");
    let publish_has_gate =
        publish.contains(graph_check) && publish.contains("cargo check --release");

    // Assert
    assert!(ci_has_gate);
    assert!(publish_has_gate);
}

#[test]
fn should_enable_failpoints_when_release_runs_injection_suites() {
    // Arrange
    let publish = repository_file(".github/workflows/publish.yml");
    let injection_commands = [
        "cargo test --test external_adopter_smoke --features failpoints",
        "cargo test --test failure_injection --features failpoints",
        "cargo test --test chaos_compaction --features failpoints",
    ];

    // Act
    let missing_feature = injection_commands
        .iter()
        .find(|command| !publish.contains(*command));

    // Assert
    assert_eq!(missing_feature, None);
}
