use std::path::PathBuf;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn repository_file(path: &str) -> String {
    std::fs::read_to_string(repository_root().join(path))
        .unwrap_or_else(|error| panic!("read {path}: {error}"))
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
    let production_files = [
        "src/metadata/journal.rs",
        "src/metadata/persistence.rs",
        "src/runtime/actors/flush.rs",
        "src/runtime/actors/manifest.rs",
        "src/runtime/actors/wal.rs",
        "src/runtime/event_loop/cloud_integration.rs",
        "src/runtime/event_loop/compaction.rs",
        "src/runtime/intent_persistence.rs",
        "src/runtime/state.rs",
        "src/sst/fs/mod.rs",
        "src/storage/hybrid/backend.rs",
    ];

    // Act
    let direct_production_reference = production_files.iter().find(|path| {
        let source = repository_file(path);
        let production = source
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or(&source);
        production.contains("fail::fail_point!") || production.contains("fail::eval(")
    });

    // Assert
    assert_eq!(direct_production_reference, None);
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
