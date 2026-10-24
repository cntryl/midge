use serde_json::Value;
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
    std::fs::write(path.join("FORMAT"), "midge-format-version=9\n").expect("write format marker");
    std::fs::write(
        path.join("manifest.json"),
        include_bytes!("fixtures/compatibility/v9_empty_db/manifest.snapshot.json"),
    )
    .expect("write empty manifest");
}

#[test]
fn should_verify_candidate_written_database_when_cli_runs() {
    // Arrange
    let directory = tempfile::tempdir().expect("create candidate database directory");
    let mut engine = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::local(directory.path())
            .build()
            .expect("build candidate options"),
    )
    .expect("open candidate database");
    let family = engine.get_column_family("default").expect("default family");
    let mut write = engine
        .begin_tx(family.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin candidate write");
    write
        .put(b"release/key".to_vec(), b"release/value".to_vec(), None)
        .expect("write candidate record");
    write
        .commit(cntryl_midge::WriteOptions::sync())
        .expect("commit candidate record");
    engine.flush_cf(&family).expect("flush candidate SST");
    engine
        .shutdown(std::time::Duration::from_secs(30))
        .expect("shut down candidate writer");
    drop(engine);

    // Act
    let output = run_verify(&[
        "verify",
        "--json",
        directory.path().to_str().expect("candidate database path"),
    ]);

    // Assert
    assert_eq!(output.status.code(), Some(0));
    let report = parse_stdout(&output);
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["health"], "Healthy");
    assert_eq!(report["sst_files_verified"], 1);
    assert_eq!(report["authoritative"], true);
    assert_eq!(output.stderr, [] as [u8; 0]);
}

#[test]
fn should_emit_v1_json_given_populated_release_fixture_when_midge_verify_runs() {
    // Arrange
    let path = fixture_path("tests/fixtures/compatibility/v9_populated_v4_sst_db");
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
fn should_reject_populated_v3_fixture_when_midge_verify_runs() {
    // Arrange
    let path = fixture_path("tests/fixtures/compatibility/v3_populated_v4_sst_db");
    let before = std::fs::read(path.join("FORMAT")).expect("read fixture marker");

    // Act
    let output = run_verify(&["verify", "--json", path.to_str().expect("fixture path")]);

    // Assert
    assert_eq!(output.status.code(), Some(4));
    let report = parse_stdout(&output);
    assert_eq!(report["error_kind"], "corruption");
    assert!(report["message"]
        .as_str()
        .expect("compatibility message")
        .contains("Compatibility error: unsupported on-disk format version 3"));
    assert_eq!(
        std::fs::read(path.join("FORMAT")).expect("reread fixture marker"),
        before
    );
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
    let path = fixture_path("tests/fixtures/compatibility/future_v10");
    let output_path = path.to_str().expect("fixture path");
    let expected_message = format!(
        "Compatibility error: unsupported on-disk format version 10 at '{output_path}'; this build supports versions 9..=9"
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
