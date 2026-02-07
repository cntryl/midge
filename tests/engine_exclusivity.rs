//! Tests for single-instance exclusivity (primary lease mechanism)
//!
//! Validates that:
//! - Only one Midge instance can be primary at a time
//! - Lease acquisition failures are explicit and fast
//! - Lease release allows subsequent acquisition
//! - Crashes release the lease automatically (TTL expiry)

use cntryl_midge::{Engine, MidgeError, OpenOptions};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Helper: create a temp directory for testing
fn temp_db_path() -> PathBuf {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let path = temp_dir.path().to_path_buf();
    // Keep temp_dir alive so it doesn't get deleted
    std::mem::forget(temp_dir);
    path
}

#[test]
fn should_open_single_instance_when_no_contention() {
    // Arrange
    let db_path = temp_db_path();
    let opts = OpenOptions::local(&db_path);

    // Act
    let engine = Engine::open(opts).expect("should open successfully");

    // Assert
    assert!(
        engine.is_primary_lease_healthy(),
        "lease should be healthy after opening"
    );

    // Clean shutdown
    drop(engine);
}

#[test]
fn should_fail_second_instance_when_first_holds_lease() {
    // Arrange
    let db_path = temp_db_path();

    // Open first instance

    // Act
    let engine1 = Engine::open(OpenOptions::local(&db_path)).expect("first instance should open");
    assert!(engine1.is_primary_lease_healthy());

    // Try to open second instance (should fail)
    let result = Engine::open(OpenOptions::local(&db_path));

    // Assert
    assert!(
        result.is_err(),
        "second instance should fail to acquire lease"
    );

    if let Err(MidgeError::Internal(msg)) = result {
        assert!(
            msg.contains("another Midge instance") || msg.contains("already running"),
            "error message should indicate another instance is running, got: {}",
            msg
        );
    } else {
        panic!("expected MidgeError::Internal with descriptive message");
    }

    // First instance should still be healthy
    assert!(engine1.is_primary_lease_healthy());
}

#[test]
fn should_allow_second_instance_when_first_is_dropped() {
    // Arrange
    let db_path = temp_db_path();

    // Open and drop first instance

    // Act
    {
        let engine1 =
            Engine::open(OpenOptions::local(&db_path)).expect("first instance should open");
        assert!(engine1.is_primary_lease_healthy());
        drop(engine1); // Explicit drop for clarity
    }

    // Small delay to ensure lease is released
    thread::sleep(Duration::from_millis(50));

    // Second instance should now succeed
    let engine2 = Engine::open(OpenOptions::local(&db_path))
        .expect("second instance should open after first is dropped");

    // Assert
    assert!(engine2.is_primary_lease_healthy());
}

#[test]
fn should_maintain_lease_health_during_normal_operation() {
    // Arrange
    let db_path = temp_db_path();
    let engine = Engine::open(OpenOptions::local(&db_path)).expect("should open");
    assert!(engine.is_primary_lease_healthy());

    // Act
    // Wait for multiple heartbeat cycles (3-4 renewal intervals)
    thread::sleep(Duration::from_secs(5));

    // Assert
    // Lease should still be healthy
    assert!(
        engine.is_primary_lease_healthy(),
        "lease should remain healthy after multiple renewal cycles"
    );
}

#[test]
fn should_block_concurrent_opens_when_racing() {
    // Arrange
    let db_path = Arc::new(temp_db_path());
    let barrier = Arc::new(std::sync::Barrier::new(3));

    let mut handles = vec![];

    // Spawn 3 threads all trying to open the same database

    // Act
    for i in 0..3 {
        let path = Arc::clone(&db_path);
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            barrier.wait();

            // Try to open
            let result = Engine::open(OpenOptions::local(&*path));
            (i, result)
        });

        handles.push(handle);
    }

    // Collect results
    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("thread panicked"))
        .collect();

    // Assert
    // Exactly one should succeed
    let success_count = results.iter().filter(|(_, r)| r.is_ok()).count();
    assert_eq!(
        success_count, 1,
        "exactly one instance should acquire the lease"
    );

    // Two should fail
    let failure_count = results.iter().filter(|(_, r)| r.is_err()).count();
    assert_eq!(failure_count, 2, "two instances should fail");

    // Clean up the successful instance
    for (_id, result) in results {
        if let Ok(engine) = result {
            drop(engine);
        }
    }
}

#[test]
fn should_reject_writes_if_lease_becomes_unhealthy() {
    // Arrange
    // This is a placeholder test for future lease loss detection
    // Currently, the filesystem lease doesn't have TTL-based expiry like cloud leases would
    //
    // TODO: Implement this test when cloud lease backend is available
    // or when we add lease expiry simulation for testing

    // Act

    // Assert: Placeholder for future validation when lease expiry simulation is available
}

#[test]
fn should_provide_clear_error_message_when_lease_contention() {
    // Arrange
    let db_path = temp_db_path();

    // Act
    let _engine1 = Engine::open(OpenOptions::local(&db_path)).expect("first should open");

    // Try second instance
    let result = Engine::open(OpenOptions::local(&db_path));

    // Assert
    assert!(result.is_err());

    if let Err(MidgeError::Internal(msg)) = result {
        // Error message must be clear and actionable
        assert!(
            msg.to_lowercase().contains("another"),
            "error should mention 'another instance'"
        );
        assert!(
            msg.to_lowercase().contains("running")
                || msg.to_lowercase().contains("already")
                || msg.to_lowercase().contains("holds"),
            "error should indicate an active instance"
        );
    }
}

#[test]
fn should_work_with_in_memory_storage_when_unique_paths() {
    // Arrange
    // InMemory mode uses unique temp paths, so multiple instances are allowed

    // Act
    let engine1 = Engine::open(OpenOptions::in_memory()).expect("first in-memory should open");
    let engine2 = Engine::open(OpenOptions::in_memory())
        .expect("second in-memory should open (different path)");

    // Assert
    assert!(engine1.is_primary_lease_healthy());
    assert!(engine2.is_primary_lease_healthy());
}

#[test]
fn should_release_lease_on_drop_when_clean_shutdown() {
    // Arrange
    let db_path = temp_db_path();

    // Act
    // Open, perform some work, drop
    {
        let engine = Engine::open(OpenOptions::local(&db_path)).expect("should open");
        assert!(engine.is_primary_lease_healthy());

        // Simulate some work
        thread::sleep(Duration::from_millis(100));

        // Drop will trigger clean shutdown and lease release
    }

    // Give OS time to release file lock
    thread::sleep(Duration::from_millis(50));

    // Should be able to open again immediately
    let engine2 =
        Engine::open(OpenOptions::local(&db_path)).expect("should reopen after clean shutdown");

    // Assert
    assert!(engine2.is_primary_lease_healthy());
}
