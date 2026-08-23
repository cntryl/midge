//! Tests for single-instance exclusivity (primary lease mechanism)
//!
//! Validates that:
//! - Only one Midge instance can be primary at a time
//! - Lease acquisition failures are explicit and fast
//! - Lease release allows subsequent acquisition
//! - Crashes release the lease automatically (TTL expiry)

use cntryl_midge::{Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Name of the `#[test]` function that acts as the crashed-process child.
/// Invoked via `cargo test --exact` from `run_crashed_child_holding_lease`.
const CRASHED_CHILD_TEST_NAME: &str = "should_hold_lease_forever_in_child_process";
const CRASHED_CHILD_ENV_DB_PATH: &str = "MIDGE_EXCLUSIVITY_CRASHED_CHILD_DB_PATH";

/// Helper: create a temp directory for testing
fn temp_db_path() -> PathBuf {
    let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
    let path = temp_dir.path().to_path_buf();
    // Keep temp_dir alive so it doesn't get deleted
    std::mem::forget(temp_dir);
    path
}

fn local_options(path: &std::path::Path) -> cntryl_midge::OpenOptions {
    OpenOptions::local(path)
        .build()
        .expect("build local options")
}

fn memory_options() -> cntryl_midge::OpenOptions {
    OpenOptions::in_memory()
        .build()
        .expect("build memory options")
}

#[test]
fn should_open_single_instance_when_no_contention() {
    // Arrange
    let db_path = temp_db_path();
    let opts = local_options(&db_path);

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
fn should_reject_second_engine_open_given_existing_primary_lease_when_starting() {
    // Arrange
    let db_path = temp_db_path();

    // Open first instance

    // Act
    let engine1 = Engine::open(local_options(&db_path)).expect("first instance should open");
    assert!(engine1.is_primary_lease_healthy());

    // Try to open second instance (should fail)
    let result = Engine::open(local_options(&db_path));

    // Assert
    assert!(
        result.is_err(),
        "second instance should fail to acquire lease"
    );

    if let Err(MidgeError::LeaseHeld(msg)) = result {
        assert!(
            msg.contains("another Midge instance") || msg.contains("already running"),
            "error message should indicate another instance is running, got: {msg}"
        );
    } else {
        panic!("expected MidgeError::LeaseHeld with descriptive message");
    }

    // First instance should still be healthy
    assert!(engine1.is_primary_lease_healthy());
}

#[test]
fn should_allow_second_instance_when_first_is_shutdown() {
    // Arrange
    let db_path = temp_db_path();

    // Open and drop first instance

    // Act
    {
        let mut engine1 =
            Engine::open(local_options(&db_path)).expect("first instance should open");
        assert!(engine1.is_primary_lease_healthy());
        engine1
            .shutdown(Duration::from_secs(2))
            .expect("shutdown first instance");
    }

    // Small delay to ensure lease is released
    thread::sleep(Duration::from_millis(50));

    // Second instance should now succeed
    let engine2 = Engine::open(local_options(&db_path))
        .expect("second instance should open after first is dropped");

    // Assert
    assert!(engine2.is_primary_lease_healthy());
}

#[test]
fn should_maintain_lease_health_during_normal_operation() {
    // Arrange
    let db_path = temp_db_path();
    let engine = Engine::open(local_options(&db_path)).expect("should open");
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
            let result = Engine::open(local_options(&path));
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
        if let Ok(mut engine) = result {
            engine
                .shutdown(Duration::from_secs(2))
                .expect("shutdown racing winner");
        }
    }
}

/// Rewrite the on-disk `.midge_leader` record so its epoch is one higher
/// than the epoch our still-running `engine` acquired, exactly as a rival
/// instance winning a CAS race would leave it. This mirrors the on-disk
/// record format `format_leader_record` writes (see
/// `src/lease/traits.rs`), including its trailing CRC32C checksum, so the
/// running engine's next renewal attempt reads a record that parses and
/// checksums cleanly but no longer matches its own epoch.
fn steal_lease_epoch(db_path: &Path) {
    let lease_path = db_path.join(".midge_leader");
    let content = std::fs::read_to_string(&lease_path).expect("read leader record");

    let mut epoch: Option<u64> = None;
    let mut acquired_at: Option<String> = None;
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("epoch: ") {
            epoch = value.parse::<u64>().ok();
        } else if let Some(value) = line.strip_prefix("acquired_at: ") {
            acquired_at = Some(value.to_string());
        }
    }
    let epoch = epoch.expect("leader record missing epoch");
    let acquired_at = acquired_at.expect("leader record missing acquired_at");

    let body = format!(
        "epoch: {}\nholder_id: rival-instance@stolen\nacquired_at: {acquired_at}\n",
        epoch + 1
    );
    let checksum = crc32c::crc32c(body.as_bytes());
    std::fs::write(&lease_path, format!("{body}checksum: {checksum}\n"))
        .expect("simulate a rival instance stealing the lease");
}

#[test]
fn should_reject_writes_if_lease_becomes_unhealthy() {
    // Arrange
    let db_path = temp_db_path();
    let engine = Engine::open(local_options(&db_path)).expect("should open");
    assert!(engine.is_primary_lease_healthy());

    // Act
    // Simulate another instance winning the lease out from under us (e.g. a
    // split-brain caused by a false crash detection). Our own epoch, held
    // only in memory, no longer matches what is on disk, so the next
    // background renewal must fail and mark the lease unhealthy.
    steal_lease_epoch(&db_path);

    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    while engine.is_primary_lease_healthy() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(100));
    }

    // Assert
    assert!(
        !engine.is_primary_lease_healthy(),
        "lease should become unhealthy once another instance steals the epoch"
    );

    // Fail-closed: once the lease is unhealthy, writes must be rejected
    // rather than silently accepted (which would risk split-brain corruption).
    let default_cf = engine
        .get_column_family("default")
        .expect("default column family");
    let mut tx = engine
        .begin_tx(default_cf.id(), TransactionMode::ReadWrite)
        .expect("begin_tx should still succeed on a fenced engine");
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("buffering a write locally should not fail");
    let commit = tx.commit(WriteOptions::sync());
    assert!(
        matches!(commit, Err(MidgeError::Fenced(_))),
        "commit should be rejected as Fenced once the lease is unhealthy, got {commit:?}"
    );
}

#[test]
fn should_work_with_in_memory_storage_when_unique_paths() {
    // Arrange
    // InMemory mode uses unique temp paths, so multiple instances are allowed

    // Act
    let engine1 = Engine::open(memory_options()).expect("first in-memory should open");
    let engine2 =
        Engine::open(memory_options()).expect("second in-memory should open (different path)");

    // Assert
    assert!(engine1.is_primary_lease_healthy());
    assert!(engine2.is_primary_lease_healthy());
}

#[test]
fn should_release_primary_lease_given_clean_shutdown_when_shutdown_completes() {
    // Arrange
    let db_path = temp_db_path();

    // Act
    // Open, perform some work, and shut down.
    {
        let mut engine = Engine::open(local_options(&db_path)).expect("should open");
        assert!(engine.is_primary_lease_healthy());

        // Simulate some work
        thread::sleep(Duration::from_millis(100));

        engine
            .shutdown(Duration::from_secs(2))
            .expect("clean shutdown");
    }

    // Give OS time to release file lock
    thread::sleep(Duration::from_millis(50));

    // Should be able to open again immediately
    let engine2 =
        Engine::open(local_options(&db_path)).expect("should reopen after clean shutdown");

    // Assert
    assert!(engine2.is_primary_lease_healthy());
}

// ============================================================================
// Additional Test Coverage for Lease Mechanisms
// ============================================================================

#[test]
fn should_survive_rapid_open_close_cycling() {
    eprintln!("\n=== Exclusivity: Rapid Open/Close Cycling ===");

    // Arrange
    let db_path = temp_db_path();

    // Act: Rapid cycle opens and closes
    for cycle in 0..50 {
        match Engine::open(local_options(&db_path)) {
            Ok(mut engine) => {
                assert!(
                    engine.is_primary_lease_healthy(),
                    "lease healthy on cycle {cycle}"
                );
                engine
                    .shutdown(Duration::from_secs(2))
                    .expect("shutdown cycle");
            }
            Err(e) => {
                eprintln!("Failed to open on cycle {cycle}: {e:?}");
                panic!("should not fail during rapid cycling");
            }
        }
    }

    // Assert: Final open succeeds (no resource exhaustion)
    let final_engine = Engine::open(local_options(&db_path)).expect("final open should succeed");
    assert!(final_engine.is_primary_lease_healthy());

    eprintln!("✓ Survived 50 rapid open/close cycles without resource issues");
}

/// Child-process entry point for `should_reject_open_when_lease_held_by_crashed_process`.
/// Not a scenario test on its own: only acts when the parent sets
/// `CRASHED_CHILD_ENV_DB_PATH`, otherwise it's a no-op so `cargo test` runs
/// of the whole suite don't try to open a nonexistent path.
///
/// Acquires the primary lease and then calls `std::process::exit`, which
/// skips all destructors (including `Engine::drop`'s lease release) — the
/// same on-disk state a hard process crash (e.g. SIGKILL) would leave
/// behind: a `.midge_leader` record that is still valid and unexpired.
#[test]
fn should_hold_lease_forever_in_child_process() {
    // Arrange
    let Some(db_path) = std::env::var_os(CRASHED_CHILD_ENV_DB_PATH) else {
        return;
    };
    let db_path = PathBuf::from(db_path);

    // Act
    let engine = Engine::open(local_options(&db_path)).expect("child should acquire lease");

    // Assert
    assert!(engine.is_primary_lease_healthy());

    std::process::exit(0);
}

fn run_crashed_child_holding_lease(db_path: &Path) {
    let current_exe = std::env::current_exe().expect("current exe");
    let status = Command::new(current_exe)
        .arg("--exact")
        .arg(CRASHED_CHILD_TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(CRASHED_CHILD_ENV_DB_PATH, db_path)
        .status()
        .expect("run crashed-child process");
    assert!(
        status.success(),
        "child process should acquire the lease and exit(0) without releasing it, got {status:?}"
    );
}

#[test]
fn should_reject_open_when_lease_held_by_crashed_process() {
    // Arrange: a child process acquires the lease and then "crashes" by
    // calling exit() directly, leaving the on-disk lease record held and
    // unexpired (its TTL is far longer than this test takes to run).
    let db_path = temp_db_path();
    run_crashed_child_holding_lease(&db_path);

    // Act
    let result = Engine::open(local_options(&db_path));

    // Assert: the still-unexpired lease must reject the new open, not
    // silently succeed and risk a second writer against the same storage.
    match result {
        Err(MidgeError::LeaseHeld(msg)) => {
            assert!(
                msg.to_lowercase().contains("another")
                    || msg.to_lowercase().contains("running")
                    || msg.to_lowercase().contains("holds"),
                "expected descriptive lease-held error, got: {msg}"
            );
        }
        Ok(_) => panic!(
            "expected MidgeError::LeaseHeld while a crashed process still holds an \
             unexpired lease, got Ok"
        ),
        Err(other) => panic!(
            "expected MidgeError::LeaseHeld while a crashed process still holds an \
             unexpired lease, got {other:?}"
        ),
    }
}
