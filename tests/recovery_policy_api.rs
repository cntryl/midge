use cntryl_midge::{Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy};
use std::fs;
use tempfile::TempDir;

#[test]
fn should_fail_strict_open_when_manifest_journal_is_corrupt() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    fs::write(
        db_path.join("manifest.journal"),
        b"not-a-valid-manifest-journal",
    )
    .expect("write corrupt manifest journal");

    // Act
    let result = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    );

    // Assert
    match result {
        Err(MidgeError::RecoveryFailed(message)) => {
            assert!(
                message.contains("manifest"),
                "expected manifest recovery context, got: {message}"
            );
        }
        Ok(_) => panic!("expected strict recovery failure, got successful open"),
        Err(other) => panic!("expected RecoveryFailed, got: {other}"),
    }
}

#[test]
fn should_open_in_salvage_mode_when_manifest_journal_is_corrupt() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    fs::write(
        db_path.join("manifest.journal"),
        b"not-a-valid-manifest-journal",
    )
    .expect("write corrupt manifest journal");

    // Act
    let engine = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build(),
    )
    .expect("salvage open");

    // Assert
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(metrics.health, EngineHealth::SalvageMode);
    assert_eq!(metrics.salvage_mode_opens, 1);
}

#[test]
fn should_fail_strict_open_when_intent_log_is_corrupt() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    fs::write(db_path.join("intent_log.yaml"), ":\n- broken: [").expect("write corrupt intent log");

    // Act
    let result = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    );

    // Assert
    match result {
        Err(MidgeError::RecoveryFailed(message)) => {
            assert!(
                message.contains("intent"),
                "expected intent recovery context, got: {message}"
            );
        }
        Ok(_) => panic!("expected strict recovery failure, got successful open"),
        Err(other) => panic!("expected RecoveryFailed, got: {other}"),
    }
}

#[test]
fn should_fail_open_given_legacy_persisted_state_without_format_marker() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    fs::write(
        db_path.join("manifest.yaml"),
        "last_persisted_sequence: 1\n",
    )
    .expect("write legacy manifest");

    // Act
    let result = Engine::open(OpenOptions::local(db_path).build());

    // Assert
    match result {
        Err(MidgeError::CompatibilityError(message)) => {
            assert!(
                message.contains("FORMAT"),
                "expected format-marker compatibility context, got: {message}"
            );
        }
        Ok(_) => panic!("expected compatibility failure, got successful open"),
        Err(other) => panic!("expected CompatibilityError, got: {other}"),
    }
}
