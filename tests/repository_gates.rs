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
    let uses_sqrzl_image = compose.contains("ghcr.io/sqrzl/sqrzl-emulator:latest");

    // Assert
    assert!(uses_sqrzl_image);
    assert!(compose.contains("  sqrzl:"));
    assert!(ci.contains("docker compose up -d sqrzl"));
    assert!(ci.contains("--features sqrzl-tests"));
    assert!(publish.contains("docker compose up -d sqrzl"));
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
