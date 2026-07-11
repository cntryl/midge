use cntryl_midge::{Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy};
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/compatibility")
}

fn copy_fixture_dir(name: &str) -> tempfile::TempDir {
    let source = fixtures_root().join(name);
    assert!(
        source.exists(),
        "missing compatibility fixture: {}",
        source.display()
    );

    let temp = tempfile::tempdir().expect("create temp dir");
    copy_dir_recursive(&source, temp.path()).expect("copy compatibility fixture");
    temp
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;

    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let dest_path = destination.join(entry.file_name());

        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &dest_path)?;
        } else {
            fs::copy(entry.path(), &dest_path)?;
        }
    }

    Ok(())
}

fn assert_compatibility_error(error: MidgeError) {
    match error {
        MidgeError::CompatibilityError(message) => {
            assert!(
                message.contains("unsupported on-disk format version"),
                "expected unsupported-format compatibility error, got: {message}"
            );
        }
        other => panic!("expected CompatibilityError, got: {other:?}"),
    }
}

#[test]
fn should_verify_release_v2_fixture_given_supported_format_when_reopening() {
    // Arrange
    let temp = copy_fixture_dir("v2_empty_db");

    // Act
    let report = Engine::verify_path(temp.path()).expect("verify release fixture");
    assert_eq!(report.health, EngineHealth::Healthy);
    assert_eq!(report.manifest_files_verified, 0);
    assert_eq!(report.sst_files_verified, 0);
    assert_eq!(report.wal_recovery_records_replayed, 0);
    assert_eq!(report.wal_recovery_bytes_replayed, 0);
    assert_eq!(report.intent_entries_loaded, 0);

    let engine = Engine::open(
        OpenOptions::local(temp.path())
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
    )
    .expect("open release fixture");
    let runtime = engine.get_runtime_metrics().expect("runtime metrics");
    // Assert
    assert_eq!(runtime.health, EngineHealth::Healthy);
}

#[test]
fn should_reject_future_format_fixture_given_unsupported_version_when_reopening() {
    // Arrange
    let temp = copy_fixture_dir("future_v3");

    // Act
    let verify_error =
        Engine::verify_path(temp.path()).expect_err("future fixture should fail verify");
    assert_compatibility_error(verify_error);

    let Err(open_error) = Engine::open(
        OpenOptions::local(temp.path())
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
    ) else {
        panic!("future fixture should fail open");
    };
    // Assert
    assert_compatibility_error(open_error);
}
