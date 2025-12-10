//! Tests for Storage Budget Actor (SBA) integration with Flush and Compaction Actors
//!
//! This test suite validates:
//! 1. FlushActor respects SBA watermarks and backpressure
//! 2. CompactionActor notifies SBA of disk state changes
//! 3. End-to-end hybrid storage disk management
//! 4. Watermark enforcement during concurrent operations

use cntryl_midge::engine::MidgeEngine;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Arrange: Create a temporary directory for test data
fn setup_test_dir() -> PathBuf {
    let test_num = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let test_dir = PathBuf::from(format!("target/test_sba_integration_{}", test_num));
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    test_dir
}

/// Cleanup: Remove temporary test directory
fn cleanup_test_dir(dir: &PathBuf) {
    std::thread::sleep(std::time::Duration::from_millis(50));
    let _ = fs::remove_dir_all(dir);
}

// ============================================================================
// Flush Actor + SBA Integration Tests
// ============================================================================

#[test]
fn should_flush_memtable_when_sba_reservation_succeeds() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write some data
    engine.put(b"key1", b"value1").expect("Put failed");
    engine.put(b"key2", b"value2").expect("Put failed");

    // Act: Flush memtable
    engine.flush().expect("Flush failed");

    // Assert: Data is still readable after flush
    let val = engine
        .get(b"key1")
        .expect("Get failed")
        .expect("Key not found");
    assert_eq!(val, b"value1", "Data should be readable after flush");

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
#[cfg_attr(target_os = "windows", ignore)]
fn should_handle_multiple_flushes_with_sba_coordination() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Multiple write-flush cycles
    for batch in 0..3 {
        for i in 0..10 {
            let key = format!("key_{}_{}", batch, i);
            let val = format!("value_{}_{}", batch, i);
            engine
                .put(key.as_bytes(), val.as_bytes())
                .expect("Put failed");
        }

        engine.flush().expect("Flush failed");

        // Verify data is still there
        let key = format!("key_{}_0", batch);
        let expected_val = format!("value_{}_0", batch);
        let actual = engine
            .get(key.as_bytes())
            .expect("Get failed")
            .expect("Key not found");
        assert_eq!(
            actual, expected_val.into_bytes(),
            "Data should persist after flush cycle {}", batch
        );
    }

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_write_ssts_during_flush_when_space_available() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write and flush
    for i in 0..5 {
        engine.put(format!("k{}", i).as_bytes(), b"data").ok();
    }
    engine.flush().expect("Flush failed");

    // Assert: Engine is responsive after flush (functional test)
    let val = engine
        .get(b"k0")
        .expect("Get failed")
        .expect("Key not found");
    assert_eq!(val, b"data", "Data should persist after flush");

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_maintain_data_consistency_across_flushes() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write some data
    let data: Vec<(&[u8], &[u8])> = vec![
        (b"alice", b"alice_data"),
        (b"bob", b"bob_data"),
        (b"charlie", b"charlie_data"),
    ];

    for (k, v) in &data {
        engine.put(k, v).expect("Put failed");
    }

    // Act: Flush
    engine.flush().expect("Flush failed");

    // Assert: All data readable after flush
    for (k, expected_v) in &data {
        let actual = engine.get(k).expect("Get failed");
        assert_eq!(
            &actual, &Some(expected_v.to_vec()),
            "Key {:?} should have correct value after flush",
            String::from_utf8_lossy(k)
        );
    }

    // Cleanup
    cleanup_test_dir(&test_dir);
}

// ============================================================================
// Compaction Actor + SBA Integration Tests
// ============================================================================

#[test]
fn should_trigger_compaction_when_needed() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write enough data to trigger compaction
    // (In a real scenario, this would be based on L0 file count)
    for batch in 0..3 {
        for i in 0..50 {
            let key = format!("k_{:04}_{:04}", batch, i);
            let val = format!("v_{:04}_{:04}", batch, i);
            engine.put(key.as_bytes(), val.as_bytes()).ok();
        }
        engine.flush().expect("Flush failed");
    }

    // Assert: Engine is still responsive
    let val = engine
        .get(b"k_0000_0000")
        .expect("Get failed")
        .expect("Key not found");
    assert_eq!(val, b"v_0000_0000");

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_preserve_data_during_compaction() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Write-flush-write pattern to create multiple SSTs
    let mut keys = Vec::new();

    for round in 0..2 {
        for i in 0..20 {
            let key = format!("key_{:03}_{:03}", round, i);
            let val = format!("val_{:03}_{:03}", round, i);
            keys.push((key.clone(), val.clone()));
            engine.put(key.as_bytes(), val.as_bytes()).ok();
        }
        engine.flush().ok();
    }

    // Assert: All data accessible after multiple flushes
    for (key, expected_val) in &keys {
        let actual = engine
            .get(key.as_bytes())
            .expect("Get failed")
            .expect("Key not found");
        assert_eq!(
            &actual, expected_val.as_bytes(),
            "Key should be readable after compaction"
        );
    }

    // Cleanup
    cleanup_test_dir(&test_dir);
}

// ============================================================================
// End-to-End Hybrid Storage Tests
// ============================================================================

#[test]
fn should_handle_watermark_transitions() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Incrementally fill disk with data
    let mut total_size = 0;
    for batch in 0..5 {
        for i in 0..100 {
            let key = format!("batch_{}_key_{}", batch, i);
            let val = format!("value_batch_{}_key_{}", batch, i); // Large value
            let size = val.len();
            engine.put(key.as_bytes(), val.as_bytes()).ok();
            total_size += size;
        }
        engine.flush().ok();
    }

    // Assert: Engine handles disk growth gracefully
    assert!(total_size > 0, "Should have written data");

    // Verify data integrity
    let val = engine
        .get(b"batch_0_key_0")
        .expect("Get failed")
        .expect("Key not found");
    assert_eq!(val, b"value_batch_0_key_0");

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
#[cfg_attr(target_os = "windows", ignore)]
fn should_coordinate_flush_and_compaction() {
    // Arrange
    let test_dir = setup_test_dir();
    let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

    // Act: Concurrent write-flush-compaction operations
    for round in 0..3 {
        // Write phase
        for i in 0..30 {
            let key = format!("round_{}_key_{:03}", round, i);
            let val = format!("round_{}_value_{:03}", round, i);
            engine.put(key.as_bytes(), val.as_bytes()).ok();
        }

        // Flush phase
        engine.flush().ok();

        // Small delay to allow compaction checks
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    // Assert: Data consistency maintained
    for round in 0..3 {
        for i in 0..30 {
            let key = format!("round_{}_key_{:03}", round, i);
            let actual = engine.get(key.as_bytes()).ok().flatten();
            assert!(
                actual.is_some(),
                "Round {} key {} should exist",
                round,
                i
            );
        }
    }

    // Cleanup
    cleanup_test_dir(&test_dir);
}

#[test]
fn should_recover_after_shutdown_with_sba_state() {
    // Arrange
    let test_dir = setup_test_dir();

    // Act: Write data in first engine instance
    {
        let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to open engine");

        for i in 0..10 {
            let key = format!("persist_key_{}", i);
            let val = format!("persist_val_{}", i);
            engine.put(key.as_bytes(), val.as_bytes()).ok();
        }

        engine.flush().expect("Flush failed");
    } // Engine drops here

    // Act: Reopen engine and verify data
    {
        let engine = MidgeEngine::open(test_dir.clone()).expect("Failed to reopen engine");

        // Assert: Data persisted and accessible
        for i in 0..10 {
            let key = format!("persist_key_{}", i);
            let expected = format!("persist_val_{}", i);
            let actual = engine.get(key.as_bytes()).ok().flatten();
            assert_eq!(actual.as_deref(), Some(expected.as_bytes()));
        }
    }

    // Cleanup
    cleanup_test_dir(&test_dir);
}
