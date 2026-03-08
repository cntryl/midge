use cntryl_midge::{Engine, EngineHealth, MidgeError, OpenOptions, RecoveryPolicy};
use std::fs;
use tempfile::TempDir;

#[test]
fn should_fail_strict_open_when_manifest_journal_is_corrupt() {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    fs::write(
        db_path.join("manifest.journal"),
        b"not-a-valid-manifest-journal",
    )
    .expect("write corrupt manifest journal");

    let result = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    );

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
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    fs::write(
        db_path.join("manifest.journal"),
        b"not-a-valid-manifest-journal",
    )
    .expect("write corrupt manifest journal");

    let engine = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Salvage)
            .build(),
    )
    .expect("salvage open");

    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    assert_eq!(metrics.health, EngineHealth::SalvageMode);
    assert_eq!(metrics.salvage_mode_opens, 1);
}

#[test]
fn should_fail_strict_open_when_intent_log_is_corrupt() {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path = temp_dir.path();

    fs::write(db_path.join("intent_log.yaml"), ":\n- broken: [").expect("write corrupt intent log");

    let result = Engine::open(
        OpenOptions::local(db_path)
            .recovery_policy(RecoveryPolicy::Strict)
            .build(),
    );

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
