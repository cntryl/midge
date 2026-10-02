use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn fixture_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn run_verify(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_midge"))
        .args(args)
        .output()
        .expect("run midge verify")
}

fn parse_stdout(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout).expect("parse verify JSON")
}

fn write_empty_database(path: &Path) {
    std::fs::create_dir_all(path).expect("create database directory");
    std::fs::write(path.join("FORMAT"), "midge-format-version=3\n").expect("write format marker");
    std::fs::write(
        path.join("manifest.json"),
        serde_json::to_vec_pretty(&json!({
            "last_persisted_sequence": 0,
            "files": [],
            "column_families": [],
            "next_wal_seq": 1,
            "next_sst_seqs": {},
            "edit_checkpoint_id": 0
        }))
        .expect("serialize empty manifest"),
    )
    .expect("write empty manifest");
}

#[test]
fn should_emit_v1_json_given_healthy_database_when_midge_verify_runs() {
    // Arrange
    let path = fixture_path("tests/fixtures/compatibility/v3_populated_v4_sst_db");
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/verification/healthy_v1.json"))
            .expect("parse healthy report golden");

    // Act
    let output = run_verify(&["verify", "--json", path.to_str().expect("fixture path")]);

    // Assert
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(parse_stdout(&output), expected);
    assert_eq!(output.stderr, [] as [u8; 0]);
}

#[test]
fn should_emit_v1_json_given_degraded_local_database_when_midge_verify_runs() {
    // Arrange
    let temp = tempfile::tempdir().expect("create temporary directory");
    write_empty_database(temp.path());
    let sst_dir = temp.path().join("sst");
    std::fs::create_dir_all(&sst_dir).expect("create SST directory");
    std::fs::write(sst_dir.join("orphan.sst"), b"orphan bytes").expect("write orphan SST");
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/verification/degraded_v1.json"))
            .expect("parse degraded report golden");

    // Act
    let output = run_verify(&[
        "verify",
        "--json",
        temp.path().to_str().expect("database path"),
    ]);

    // Assert
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(parse_stdout(&output), expected);
    assert_eq!(output.stderr, [] as [u8; 0]);
}

#[test]
fn should_emit_v1_usage_error_json_given_missing_path_when_midge_verify_runs() {
    // Arrange
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/verification/usage_error_v1.json"))
            .expect("parse usage error golden");

    // Act
    let output = run_verify(&["verify", "--json"]);

    // Assert
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(parse_stdout(&output), expected);
    assert_eq!(output.stderr, [] as [u8; 0]);
}

#[test]
fn should_emit_v1_storage_error_json_given_missing_database_when_midge_verify_runs() {
    // Arrange
    let temp = tempfile::tempdir().expect("create temporary directory");
    let path = temp.path().join("missing-db");
    let expected_message = format!("storage path '{}' does not exist", path.display());
    let expected_template = include_str!("fixtures/verification/storage_error_v1.json").trim();
    let expected: Value = serde_json::from_str(&expected_template.replace(
        "{{message}}",
        &serde_json::to_string(&expected_message).unwrap(),
    ))
    .expect("parse storage error golden");

    // Act
    let output = run_verify(&["verify", "--json", path.to_str().expect("database path")]);

    // Assert
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(parse_stdout(&output), expected);
    assert_eq!(output.stderr, [] as [u8; 0]);
}

#[test]
fn should_emit_v1_corruption_error_json_given_future_format_when_midge_verify_runs() {
    // Arrange
    let path = fixture_path("tests/fixtures/compatibility/future_v5");
    let output_path = path.to_str().expect("fixture path");
    let expected_message = format!(
        "Compatibility error: unsupported on-disk format version 5 at '{output_path}'; this build supports versions 3..=4"
    );
    let expected_template = include_str!("fixtures/verification/corruption_error_v1.json").trim();
    let expected: Value = serde_json::from_str(&expected_template.replace(
        "{{message}}",
        &serde_json::to_string(&expected_message).unwrap(),
    ))
    .expect("parse corruption error golden");

    // Act
    let output = run_verify(&["verify", "--json", output_path]);

    // Assert
    assert_eq!(output.status.code(), Some(4));
    assert_eq!(parse_stdout(&output), expected);
    assert_eq!(output.stderr, [] as [u8; 0]);
}
