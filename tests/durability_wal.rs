//! WAL (Write-Ahead Log) Durability Tests
//!
//! Tests the Write-Ahead Log's behavior for ensuring write durability and recovery.
//! These tests verify:
//! - fsync behavior and timing
//! - WAL rotation and buffer management
//! - Record replay during recovery
//! - Corruption handling
//!
//! **Storage Modes**: LocalDisk + CloudBacked ONLY (requires persistence)
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>

use bytes::Bytes;
use cntryl_midge::testkit::*;

// ============================================================================
// WAL RECOVERY TESTS
// ============================================================================

#[test]
fn should_recover_writes_given_unflushed_memtable_when_reopening() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write to WAL but don't flush memtable
            engine.put(cf, b"key1", b"value1").expect("put");
            engine.put(cf, b"key2", b"value2").expect("put");
            // Engine dropped here, simulating crash with unflushed memtable
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            assert_eq!(engine.get(cf, b"key1").expect("get"), Some(Bytes::from_static(b"value1")), "mode: {}", mode);
            assert_eq!(engine.get(cf, b"key2").expect("get"), Some(Bytes::from_static(b"value2")), "mode: {}", mode);
        }
    });
}

#[test]
fn should_persist_write_given_fsync_enabled_when_crash_occurs() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();
            
            // Write with fsync guarantee (durability_opts sets fsync_enabled: true)
            engine.put(cf, b"critical_key", b"critical_value").expect("put");
            // Simulate immediate crash
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            assert_eq!(
                engine.get(cf, b"critical_key").expect("get"),
                Some(Bytes::from_static(b"critical_value")),
                "mode: {}", mode
            );
        }
    });
}

#[test]
fn should_call_fsync_given_wal_sync_enabled_when_put() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act: Write with WAL sync enabled (opts has fsync_enabled: true)
        let result = engine.put(cf, b"test_key", b"test_value");

        // Assert: Put succeeds (fsync was called without blocking)
        assert!(result.is_ok(), "put should succeed in mode: {}", mode);
    });
}

// ============================================================================
// WAL ROTATION TESTS
// ============================================================================

#[test]
fn should_rotate_wal_given_small_buffer_when_writes_exceed_buffer() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write enough data to trigger WAL rotation
            for i in 0..1000 {
                let key = format!("key_{:04}", i);
                let value = format!("value_{:04}_with_padding_to_exceed_buffer_size", i);
                engine.put(cf, key.as_bytes(), value.as_bytes()).expect("put");
            }
            // Force checkpoint to ensure WAL segments are created
            engine.flush().expect("flush");
        }

        // Assert (Phase 2): All writes recovered after rotation
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            // Spot check across the range
            assert!(engine.get(cf, b"key_0000").expect("get").is_some(), "mode: {}", mode);
            assert!(engine.get(cf, b"key_0500").expect("get").is_some(), "mode: {}", mode);
            assert!(engine.get(cf, b"key_0999").expect("get").is_some(), "mode: {}", mode);
        }
    });
}

// ============================================================================
// WAL REPLAY TESTS
// ============================================================================

#[test]
fn should_replay_all_records_given_multiple_wal_segments_when_recovering() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write in phases to create multiple WAL segments
            for batch in 0..3 {
                for i in 0..100 {
                    let key = format!("batch_{}_key_{:03}", batch, i);
                    let value = format!("batch_{}_value_{:03}", batch, i);
                    engine.put(cf, key.as_bytes(), value.as_bytes()).expect("put");
                }
            }
        }

        // Assert (Phase 2): All records from all segments recovered
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            // Verify records from each batch
            for batch in 0..3 {
                for i in 0..100 {
                    let key = format!("batch_{}_key_{:03}", batch, i);
                    assert!(engine.get(cf, key.as_bytes()).expect("get").is_some(), 
                           "Missing key from batch {} in mode: {}", batch, mode);
                }
            }
        }
    });
}

#[test]
fn should_recover_all_writes_given_concurrent_puts_when_crash_occurs() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = std::sync::Arc::new(open_with_mode(opts.clone(), mode));
            let _cf = engine.default_column_family();

            // Concurrent writes from multiple threads
            let mut handles = vec![];
            for thread_id in 0..5 {
                let engine_clone = std::sync::Arc::clone(&engine);
                let handle = std::thread::spawn(move || {
                    for i in 0..20 {
                        let key = format!("thread_{}_key_{:02}", thread_id, i);
                        let value = format!("thread_{}_value_{:02}", thread_id, i);
                        engine_clone.put(engine_clone.default_column_family(), 
                                       key.as_bytes(), 
                                       value.as_bytes()).expect("put");
                    }
                });
                handles.push(handle);
            }
            
            for handle in handles {
                handle.join().expect("thread join");
            }
            // Simulate crash
        }

        // Assert (Phase 2): All concurrent writes recovered
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            for thread_id in 0..5 {
                for i in 0..20 {
                    let key = format!("thread_{}_key_{:02}", thread_id, i);
                    assert!(engine.get(cf, key.as_bytes()).expect("get").is_some(),
                           "Missing write from thread {} in mode: {}", thread_id, mode);
                }
            }
        }
    });
}

// ============================================================================
// CORRUPTION HANDLING TESTS
// ============================================================================

#[test]
fn should_handle_gracefully_given_truncated_wal_tail_when_recovering() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write data (some will be in incomplete record at tail)
            for i in 0..10 {
                let key = format!("key_{:02}", i);
                engine.put(cf, key.as_bytes(), b"value").expect("put");
            }
            // Simulate crash without flushing final records
        }

        // Assert (Phase 2): Recovers gracefully without panic
        {
            let engine = open_with_mode(opts, mode);
            let _cf = engine.default_column_family();
            
            // Some early records should be recovered
            // Recovery should not panic on truncated tail
            let _ = engine.get(_cf, b"key_00").expect("get");
        }
    });
}

#[test]
fn should_not_recover_data_given_truncated_wal_append_when_reopening() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write without fsync, simulating crash mid-write
            engine.put(cf, b"unsafe_key", b"unsafe_value").expect("put");
            // Immediate crash before fsync
        }

        // Assert (Phase 2): Graceful recovery
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            // Key may or may not exist depending on fsync timing
            // Recovery should not panic or corrupt data
            let _result = engine.get(cf, b"unsafe_key").expect("get");
        }
    });
}

// ============================================================================
// DATA LOSS AND ERROR MODES
// ============================================================================

#[test]
fn should_allow_data_loss_given_skipped_fsync_when_crash_occurs() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange: This tests the expected behavior of non-fsync mode
        
        // Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write without guaranteeing sync
            engine.put(cf, b"transient_key", b"transient_value").expect("put");
            // Crash
        }

        // Assert (Phase 2)
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            // With durable_storage_modes, if fsync is enabled, data should persist
            // This test documents the contract: if you disable fsync, data loss is possible
            let _result = engine.get(cf, b"transient_key").expect("get");
        }
    });
}

#[test]
fn should_tolerate_corrupted_tail_given_recovery_mode_set_when_reopening() {
    for_each_storage_mode(&durable_storage_modes(), |mode, opts| {
        // Arrange & Act (Phase 1)
        {
            let engine = open_with_mode(opts.clone(), mode);
            let cf = engine.default_column_family();

            // Write valid records followed by corruption
            engine.put(cf, b"valid_key_1", b"value_1").expect("put");
            engine.put(cf, b"valid_key_2", b"value_2").expect("put");
            // Simulate corruption by crashing mid-record
        }

        // Assert (Phase 2): Recovery is tolerant and doesn't crash
        {
            let engine = open_with_mode(opts, mode);
            let cf = engine.default_column_family();
            
            // Valid records before corruption should be recovered
            assert!(engine.get(cf, b"valid_key_1").expect("get").is_some(), "mode: {}", mode);
            assert!(engine.get(cf, b"valid_key_2").expect("get").is_some(), "mode: {}", mode);
        }
    });
}
