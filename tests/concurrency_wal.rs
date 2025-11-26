//! Concurrent WAL Tests
//!
//! Tests for WAL concurrency handling including serialization, ordering,
//! and rotation during concurrent writes.
//!
//! Storage modes: LocalDisk, CloudBacked (both have WAL persistence)

mod common;
use bytes::Bytes;
use cntryl_midge::MidgeEngine;
use common::*;
use std::sync::Arc;
use std::thread;

// ============================================================================
// WAL Write Serialization Tests - Disk Storage Modes
// ============================================================================

#[test]
fn should_serialize_wal_writes_given_concurrent_put_operations_when_20_threads() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let num_threads = 20;
        let writes_per_thread = 50;

        let cf = engine.default_column_family();

        // Act
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                let cf = cf.clone();
                thread::spawn(move || {
                    for i in 0..writes_per_thread {
                        let key = format!("wal_{}_{}", thread_id, i);
                        engine.put(&cf, key.as_bytes(), b"value").unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        drop(engine);

        // Assert - Reopen and verify all writes persisted
        let reopen_opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = MidgeEngine::open(reopen_opts).unwrap();
        let cf = engine.default_column_family();
        for thread_id in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("wal_{}_{}", thread_id, i);
                let result = engine.get(&cf, key.as_bytes()).unwrap();
                assert_eq!(
                    result,
                    Some(Bytes::from("value")),
                    "Failed for {} key: {}",
                    ctx.name(),
                    key
                );
            }
        }
    }
}

// ============================================================================
// WAL Ordering Tests - Disk Storage Modes
// ============================================================================

#[test]
fn should_maintain_wal_order_given_concurrent_batches_when_10_batches() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_batches = 10;
        let batch_size = 20;

        // Act
        let handles: Vec<_> = (0..num_batches)
            .map(|batch_id| {
                let engine = Arc::clone(&engine);
                let cf = cf.clone();
                thread::spawn(move || {
                    for i in 0..batch_size {
                        let key = format!("batch_{}_item_{}", batch_id, i);
                        let value = format!("batch{}", batch_id);
                        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        drop(engine);

        // Assert - Verify after restart
        let reopen_opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = MidgeEngine::open(reopen_opts).unwrap();
        let cf = engine.default_column_family();
        for batch_id in 0..num_batches {
            for i in 0..batch_size {
                let key = format!("batch_{}_item_{}", batch_id, i);
                let expected = format!("batch{}", batch_id);
                let result = engine.get(&cf, key.as_bytes()).unwrap();
                assert_eq!(
                    result,
                    Some(Bytes::from(expected.clone())),
                    "Failed for {} key: {}",
                    ctx.name(),
                    key
                );
            }
        }
    }
}

// ============================================================================
// WAL Rotation During Concurrent Writes Tests - Disk Storage Modes
// ============================================================================

#[test]
fn should_handle_wal_rotation_given_concurrent_writes_when_15_writers() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_writers = 15;
        let writes_per_writer = 100;

        // Act - Write enough to trigger WAL rotation
        let handles: Vec<_> = (0..num_writers)
            .map(|writer_id| {
                let engine = Arc::clone(&engine);
                let cf = cf.clone();
                thread::spawn(move || {
                    for i in 0..writes_per_writer {
                        let key = format!("rotate_{}_{}", writer_id, i);
                        let value = vec![0u8; 1024]; // 1KB per write
                        engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        drop(engine);

        // Assert
        let reopen_opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = MidgeEngine::open(reopen_opts).unwrap();
        let cf = engine.default_column_family();
        for writer_id in 0..num_writers {
            for i in 0..writes_per_writer {
                let key = format!("rotate_{}_{}", writer_id, i);
                let result = engine.get(&cf, key.as_bytes()).unwrap();
                assert!(
                    result.is_some(),
                    "Key {} should exist for {}",
                    key,
                    ctx.name()
                );
                assert_eq!(result.unwrap().len(), 1024);
            }
        }
    }
}

// ============================================================================
// Concurrent Sync Request Tests - Disk Storage Modes
// ============================================================================

#[test]
fn should_handle_concurrent_sync_requests_given_multiple_writers_when_sync_enabled() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();
        let num_threads = 10;
        let writes_per_thread = 30;

        // Act - Multiple threads writing with sync enabled (via durability_opts)
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine = Arc::clone(&engine);
                let cf = cf.clone();
                thread::spawn(move || {
                    for i in 0..writes_per_thread {
                        let key = format!("sync_{}_{}", thread_id, i);
                        engine.put(&cf, key.as_bytes(), b"synced").unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        drop(engine);

        // Assert - All writes should be durable
        let reopen_opts = cntryl_midge::MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = MidgeEngine::open(reopen_opts).unwrap();
        let cf = engine.default_column_family();
        for thread_id in 0..num_threads {
            for i in 0..writes_per_thread {
                let key = format!("sync_{}_{}", thread_id, i);
                let result = engine.get(&cf, key.as_bytes()).unwrap();
                assert_eq!(
                    result,
                    Some(Bytes::from("synced")),
                    "Failed for {} key: {}",
                    ctx.name(),
                    key
                );
            }
        }
    }
}
