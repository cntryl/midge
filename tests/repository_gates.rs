use std::fs;
use std::path::{Path, PathBuf};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_workflow(path: impl AsRef<Path>) -> String {
    let path = repository_root().join(path);
    fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read workflow {}: {error}", path.display()))
}

#[test]
fn should_require_rust_formatting_when_ci_runs() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");

    // Act
    let ci_formats = ci.contains("cargo fmt --check");

    // Assert
    assert!(ci_formats, "CI must reject unformatted Rust sources");
}

#[test]
fn should_require_rust_formatting_when_publish_runs() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    let publish_formats = publish.contains("cargo fmt --check");

    // Assert
    assert!(
        publish_formats,
        "publish must reject unformatted Rust sources"
    );
}

#[test]
fn should_trigger_ci_when_benchmark_changes() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");

    // Act
    let benchmark_trigger_count = ci.matches("\"benches/**\"").count();

    // Assert
    assert_eq!(
        benchmark_trigger_count, 2,
        "push and PR must include benches"
    );
}

#[test]
fn should_trigger_ci_when_documentation_changes() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");

    // Act
    let documentation_trigger_count = ci.matches("\"docs/**\"").count();

    // Assert
    assert_eq!(
        documentation_trigger_count, 2,
        "push and PR must include docs"
    );
}

#[test]
fn should_use_sqrzl_emulator_for_cloud_gates() {
    // Arrange
    let compose = read_workflow("compose.yml");
    let ci = read_workflow(".github/workflows/ci.yml");
    let publish = read_workflow(".github/workflows/publish.yml");
    let manifest = read_workflow("Cargo.toml");

    // Act
    let uses_sqrzl_image = compose.contains("ghcr.io/sqrzl/sqrzl-emulator@sha256:");

    // Assert
    assert!(uses_sqrzl_image);
    assert!(compose.contains("  sqrzl:"));
    assert!(ci.contains("docker compose up -d sqrzl"));
    assert!(ci.contains("--features sqrzl-tests"));
    assert!(ci.contains("MIDGE_REQUIRE_SQRZL: 1"));
    assert!(ci.contains("http://127.0.0.1:9001/healthz"));
    assert!(publish.contains("docker compose up -d sqrzl"));
    assert!(publish.contains("MIDGE_REQUIRE_SQRZL: 1"));
    assert!(manifest.contains("sqrzl-tests = []"));
}

#[test]
fn should_verify_checked_in_v2_fixture_in_publish_workflow() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");
    let fixture = repository_root().join("tests/fixtures/compatibility/v2_empty_db");

    // Act
    let uses_v2_fixture = publish.contains("tests/fixtures/compatibility/v2_empty_db");

    // Assert
    assert!(fixture.is_dir(), "release compatibility fixture must exist");
    assert!(
        uses_v2_fixture,
        "publish must verify the checked-in v2 fixture"
    );
    assert!(!publish.contains("tests/fixtures/compatibility/v1_empty_db"));
}

#[test]
fn should_run_full_pedantic_clippy_gate_before_publish() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    let uses_release_clippy_gate = publish.contains(
        "cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic",
    );

    // Assert
    assert!(uses_release_clippy_gate);
}

#[test]
fn should_publish_only_version_matching_tags_with_least_privilege() {
    // Arrange
    let publish = read_workflow(".github/workflows/publish.yml");

    // Act
    // Assert
    assert!(publish.contains("tags:\n      - \"v*\""));
    assert!(!publish.contains("workflow_dispatch"));
    assert!(publish.contains("test \"${GITHUB_REF_NAME}\" = \"v${crate_version}\""));
    assert!(publish.contains("environment: crates-io"));
    assert!(publish.contains("id-token: write"));
    assert!(!publish.contains("contents: write"));
}

#[test]
fn should_pin_external_actions_and_release_tools_to_immutable_revisions() {
    // Arrange
    let workflows = [
        read_workflow(".github/workflows/bench.yml"),
        read_workflow(".github/workflows/ci.yml"),
        read_workflow(".github/workflows/cleanup.yml"),
        read_workflow(".github/workflows/publish.yml"),
    ];

    // Act
    let action_uses = workflows
        .iter()
        .flat_map(|workflow| workflow.lines())
        .filter_map(|line| line.trim().strip_prefix("uses: "));

    // Assert
    for action in action_uses {
        let revision = action
            .rsplit_once('@')
            .map(|(_, revision)| revision.split_whitespace().next().unwrap_or_default())
            .expect("external action must include a revision");
        assert_eq!(
            revision.len(),
            40,
            "external action must use a full immutable commit: {action}"
        );
        assert!(revision.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    let publish = &workflows[3];
    assert!(publish.contains("cargo install --git https://github.com/cntryl/tools --rev "));
}

#[test]
fn should_test_msrv_no_default_and_all_supported_platforms() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");

    // Act
    // Assert
    assert!(ci.contains("os: [ubuntu-latest, windows-latest, macos-latest]"));
    assert!(ci.contains("cargo test --workspace --all-features"));
    assert!(ci.contains("rustup toolchain install 1.93"));
    assert!(ci.contains("cargo +1.93 check --workspace --all-targets"));
    assert!(ci.contains("--all-targets --no-default-features -- -D warnings"));
}

#[test]
fn should_run_ci_for_docker_scripts_and_fuzz_changes() {
    // Arrange
    let ci = read_workflow(".github/workflows/ci.yml");

    // Act
    // Assert
    for path in ["Dockerfile*", "scripts/**", "fuzz/**"] {
        assert_eq!(
            ci.matches(&format!("\"{path}\"")).count(),
            2,
            "push and pull requests must include {path}"
        );
    }
}
