//! Demonstration of test timeout utility
//!
//! This file shows how to use `run_with_timeout` to detect hanging tests.
//! It includes both passing and intentionally failing examples.

mod common;
use common::*;
use std::time::Duration;

#[test]
fn should_complete_within_timeout_given_fast_operation() {
    // Arrange & Act: run a quick operation with 5-second timeout
    let result = run_with_timeout(
        || {
            // Simple operation that completes quickly
            let _ = 1 + 1;
        },
        Duration::from_secs(5),
    );

    // Assert: should complete successfully
    assert!(result.is_ok(), "Fast operation should not timeout");
}

#[test]
#[ignore] // Ignored by default - uncomment to demonstrate timeout detection
fn should_timeout_given_infinite_loop() {
    // Arrange & Act: run an infinite loop with 2-second timeout
    let result = run_with_timeout(
        || {
            // This will hang forever
            loop {
                std::thread::sleep(Duration::from_millis(100));
            }
        },
        Duration::from_secs(2),
    );

    // Assert: should timeout
    assert!(
        result.is_err(),
        "Infinite loop should be detected as timeout"
    );
    if let Err(msg) = result {
        assert!(
            msg.contains("timed out"),
            "Error message should indicate timeout"
        );
    }
}

#[test]
fn should_detect_panic_as_completion_not_hang() {
    // Arrange & Act: run a panicking closure
    let result = run_with_timeout(
        || {
            panic!("Intentional panic for testing");
        },
        Duration::from_secs(5),
    );

    // Assert: panic should be detected as completion (not hang)
    // The result is Err but indicates panic, not timeout
    assert!(result.is_err());
    if let Err(msg) = result {
        assert!(
            msg.contains("panic"),
            "Error should indicate panic, not timeout. Got: {}",
            msg
        );
    }
}

#[test]
#[ignore] // Ignored by default - demonstrates timeout on engine operations
fn should_timeout_given_hanging_engine_operation() {
    use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

    // This test demonstrates how timeout detects hanging engine operations
    let result = run_with_timeout(
        || {
            let dir = test_temp_dir();
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: dir.path().to_path_buf(),
                },
                ..Default::default()
            };

            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();

            // Simulate a scenario that might hang
            // (in real tests this would be a bug causing the hang)
            eng.put(&cf, b"key", b"value").expect("put");

            // If there was a bug causing hang during flush/shutdown,
            // the timeout would catch it here when engine drops
            drop(eng);
        },
        Duration::from_secs(10),
    );

    result.expect("Engine operations should complete without hanging");
}

/// Example of wrapping a full test with timeout
#[test]
fn should_complete_engine_restart_within_timeout() {
    run_with_timeout(
        || {
            let dir = test_temp_dir();
            let opts = durability_opts(dir.path().to_path_buf());

            with_engine_restart(
                opts,
                |eng| {
                    let cf = eng.default_column_family();
                    eng.put(&cf, b"key", b"value").expect("put");
                },
                |eng| {
                    assert_get_equals(eng, b"key", b"value");
                },
            );
        },
        Duration::from_secs(30),
    )
    .expect("Engine restart test should complete within 30 seconds");
}
