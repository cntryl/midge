use cntryl_midge::{Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy};
use std::fs;
use std::time::Duration;
use tempfile::TempDir;

fn initialize_format_marker(db_path: &std::path::Path) {
    let mut engine = Engine::open(OpenOptions::local(db_path).build().expect("build options"))
        .expect("initialize engine");
    engine
        .shutdown(Duration::from_secs(2))
        .expect("shutdown initialized engine");
}

fn write_corrupt_wal(db_path: &std::path::Path) {
    let wal_dir = db_path.join("wal");
    fs::create_dir_all(&wal_dir).expect("create wal dir");
    fs::write(wal_dir.join("wal.log"), b"\x01\x02\x03").expect("write corrupt wal");
}

#[test]
fn should_fail_strict_open_when_manifest_journal_is_corrupt() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    initialize_format_marker(db_path);

    fs::write(
        db_path.join("manifest.journal"),
        b"not-a-valid-manifest-journal",
    )
    .expect("write corrupt manifest journal");

    // Act
    let result = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
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
    initialize_format_marker(db_path);

    fs::write(
        db_path.join("manifest.journal"),
        b"not-a-valid-manifest-journal",
    )
    .expect("write corrupt manifest journal");

    // Act
    let engine = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build()
            .expect("build options"),
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
    initialize_format_marker(db_path);

    fs::write(db_path.join("intent_log.json"), "not-json").expect("write corrupt intent log");

    // Act
    let result = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
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
fn should_fail_strict_open_when_wal_is_corrupt() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    initialize_format_marker(db_path);
    write_corrupt_wal(db_path);

    // Act
    let result = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build()
            .expect("build options"),
    );

    // Assert
    match result {
        Err(MidgeError::RecoveryFailed(message)) => {
            assert!(
                message.contains("WAL"),
                "expected WAL recovery context, got: {message}"
            );
        }
        Ok(_) => panic!("expected strict WAL recovery failure, got successful open"),
        Err(other) => panic!("expected RecoveryFailed, got: {other}"),
    }
}

#[test]
fn should_open_in_salvage_mode_when_wal_is_corrupt() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();
    initialize_format_marker(db_path);
    write_corrupt_wal(db_path);

    // Act
    let engine = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build()
            .expect("build options"),
    )
    .expect("salvage open");

    // Assert
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(metrics.health, EngineHealth::SalvageMode);
    assert_eq!(metrics.salvage_mode_opens, 1);
}

#[test]
fn should_fail_open_given_persisted_manifest_without_format_marker() {
    // Arrange
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    fs::write(db_path.join("manifest.json"), "{}\n").expect("write manifest without marker");

    // Act
    let result = Engine::open(OpenOptions::local(db_path).build().expect("build options"));

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
