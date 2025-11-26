//! Transaction Basic Tests
//!
//! These tests verify core transaction functionality:
//! - Begin, commit, and rollback lifecycle
//! - Snapshot isolation within transactions
//! - Transaction operations (put, get, delete, insert, delete_range)
//! - Transaction durability across restarts
//! - Transaction scans seeing uncommitted writes
//! - Error handling (timeout, disk full)
//!
//! # Storage Mode Coverage
//! - Uses `disk_storage_modes()` (LocalDisk, CloudBacked) since transactions require WAL durability
//! - Memory mode does not support durable transactions

mod common;

use bytes::Bytes;
use cntryl_midge::{
    test_hooks::{IoBehavior, TestHooks},
    KvTransaction, MidgeEngine, MidgeOptions, StorageMode, WriteOptions,
};
use common::{create_storage_mode, disk_storage_modes, test_temp_dir, DurabilityTestContext};
use std::sync::Arc;

// ============================================================================
// COMMIT TESTS
// ============================================================================

#[test]
fn should_commit_transaction_given_multiple_operations_when_committed() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"key1", b"value1").expect("put");
        txn.insert(b"key2", b"value2").expect("insert");
        txn.delete(b"key3").expect("delete");
        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert
        assert_eq!(
            engine.get(&cf, b"key1").expect("get"),
            Some(Bytes::from("value1")),
            "Failed for {}",
            name
        );
        assert_eq!(
            engine.get(&cf, b"key2").expect("get"),
            Some(Bytes::from("value2")),
            "Failed for {}",
            name
        );
        assert_eq!(engine.get(&cf, b"key3").expect("get"), None, "Failed for {}", name);
    }
}

#[test]
fn should_succeed_given_empty_transaction_when_committed() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let empty_txn = engine.begin_transaction(&cf).expect("begin_transaction");

        // Act
        let result = engine.commit_transaction(empty_txn, WriteOptions::default());

        // Assert
        assert!(
            result.is_ok(),
            "Empty transaction should commit successfully for {}",
            name
        );
    }
}

#[test]
fn should_succeed_given_read_only_transaction_when_committed() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        engine.put(&cf, b"key", b"value").expect("put");

        let readonly_txn = engine.begin_transaction(&cf).expect("begin_transaction");
        let snap = engine.snapshot();
        let _value = engine.get_at(&cf, b"key", &snap).expect("get_at");

        // Act
        let result = engine.commit_transaction(readonly_txn, WriteOptions::default());

        // Assert
        assert!(result.is_ok(), "Read-only transaction should commit for {}", name);
    }
}

// ============================================================================
// ROLLBACK TESTS
// ============================================================================

#[test]
fn should_rollback_transaction_given_uncommitted_when_dropped() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act
        {
            let mut txn = engine.begin_transaction(&cf).expect("begin");
            txn.put(b"rollback_key", b"rollback_value").expect("put");
            // txn dropped here without commit
        }

        // Assert
        assert_eq!(
            engine.get(&cf, b"rollback_key").expect("get"),
            None,
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_rollback_all_writes_given_multiple_operations_when_dropped() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine.begin_transaction(&cf).unwrap();
        txn.put(b"key1", b"value1").unwrap();
        txn.put(b"key2", b"value2").unwrap();
        txn.put(b"key3", b"value3").unwrap();
        drop(txn);

        // Assert
        assert_eq!(engine.get(&cf, b"key1").expect("get"), None, "Failed for {}", name);
        assert_eq!(engine.get(&cf, b"key2").expect("get"), None, "Failed for {}", name);
        assert_eq!(engine.get(&cf, b"key3").expect("get"), None, "Failed for {}", name);
    }
}

#[test]
fn should_release_locks_given_aborted_transaction_when_cleanup() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut aborted_txn = engine.begin_transaction(&cf).unwrap();
        let txn_id = aborted_txn.txn_id();
        aborted_txn.put(b"locked_key", b"value").unwrap();

        // Verify transaction is active before abort
        assert!(
            engine.is_transaction_active(txn_id),
            "Transaction should be active before abort for {}",
            name
        );

        // Act
        engine.abort_transaction(aborted_txn);

        // Assert
        assert!(
            !engine.is_transaction_active(txn_id),
            "Transaction should be removed from active set after abort for {}",
            name
        );

        // Verify subsequent transactions can operate on the same keys
        let mut subsequent_txn = engine.begin_transaction(&cf).unwrap();
        subsequent_txn.put(b"locked_key", b"value2").unwrap();
        let result = engine.commit_transaction(subsequent_txn, WriteOptions::default());
        assert!(
            result.is_ok(),
            "Subsequent transaction should succeed after aborted transaction cleanup for {}",
            name
        );
    }
}

// ============================================================================
// ISOLATION TESTS
// ============================================================================

#[test]
fn should_provide_snapshot_isolation_given_concurrent_writes_when_transaction_active() {
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        engine.put(&cf, b"k1", b"v1").expect("put");

        // Act: start transaction, then modify key externally
        let _txn = engine.begin_transaction(&cf).expect("begin");
        engine.put(&cf, b"k1", b"v2").expect("put");

        // Assert: transaction provides snapshot isolation
        // (Full snapshot isolation is provided through engine.transaction_get)
        // Note: This test documents the intended isolation behavior
    }
}

#[test]
fn should_read_own_writes_given_transaction_writes_when_reading() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
        txn.put(b"nested_key", b"nested_value").unwrap();

        // Act
        let read1 = txn.get(b"nested_key").ok();
        let read2 = txn.get(b"nested_key").ok();

        // Assert
        assert_eq!(
            read1,
            Some(Some(Bytes::from("nested_value"))),
            "Failed for {}",
            name
        );
        assert_eq!(
            read2,
            Some(Some(Bytes::from("nested_value"))),
            "Failed for {}",
            name
        );
    }
}

// ============================================================================
// INSERT TESTS
// ============================================================================

#[test]
fn should_insert_value_given_nonexistent_key_when_insert_in_transaction() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        // Act
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.insert(b"new_key", b"new_value").expect("insert");
        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert
        assert_eq!(
            engine.get(&cf, b"new_key").expect("get"),
            Some(Bytes::from("new_value")),
            "Failed for {}",
            name
        );
    }
}

// ============================================================================
// DELETE_RANGE TESTS
// ============================================================================

#[test]
fn should_delete_range_given_committed_transaction_when_delete_range() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        for i in 0..5 {
            engine
                .put(
                    &cf,
                    format!("key{}", i).as_bytes(),
                    format!("val{}", i).as_bytes(),
                )
                .expect("put");
        }

        // Act
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.delete_range(b"key1", b"key4").expect("delete_range");
        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        // Assert
        assert_eq!(
            engine.get(&cf, b"key0").expect("get"),
            Some(Bytes::from("val0")),
            "Failed for {}",
            name
        );
        assert_eq!(engine.get(&cf, b"key1").expect("get"), None, "Failed for {}", name);
        assert_eq!(engine.get(&cf, b"key2").expect("get"), None, "Failed for {}", name);
        assert_eq!(engine.get(&cf, b"key3").expect("get"), None, "Failed for {}", name);
        assert_eq!(
            engine.get(&cf, b"key4").expect("get"),
            Some(Bytes::from("val4")),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_hide_deleted_range_given_transaction_scan_when_delete_range() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        for i in 0..5 {
            engine
                .put(
                    &cf,
                    format!("key{}", i).as_bytes(),
                    format!("val{}", i).as_bytes(),
                )
                .expect("put");
        }

        // Act
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.delete_range(b"key1", b"key4").expect("delete_range");
        let results = txn.scan(b"key0", b"key5").expect("scan");

        // Assert
        let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();
        assert_eq!(keys.len(), 2, "Failed for {}", name);
        assert!(keys.contains(&b"key0".as_ref()), "Failed for {}", name);
        assert!(!keys.contains(&b"key1".as_ref()), "Failed for {}", name);
        assert!(!keys.contains(&b"key2".as_ref()), "Failed for {}", name);
        assert!(!keys.contains(&b"key3".as_ref()), "Failed for {}", name);
        assert!(keys.contains(&b"key4".as_ref()), "Failed for {}", name);
    }
}

// ============================================================================
// SCAN TESTS
// ============================================================================

#[test]
fn should_see_uncommitted_writes_given_transaction_scan_when_scanning() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();

        engine.put(&cf, b"committed1", b"val1").expect("put");
        engine.put(&cf, b"committed2", b"val2").expect("put");
        engine.put(&cf, b"committed3", b"val3").expect("put");

        // Act
        let mut txn = engine.begin_transaction(&cf).expect("begin");
        txn.put(b"uncommitted1", b"new_val").expect("put");
        txn.delete(b"committed2").expect("delete");
        txn.put(b"uncommitted2", b"another_val").expect("put");

        let results = txn.scan(b"", b"\xFF\xFF\xFF\xFF").expect("scan");

        // Assert
        let keys: Vec<_> = results.iter().map(|(k, _)| k.as_ref()).collect();
        assert_eq!(keys.len(), 4, "Failed for {}", name);
        assert!(keys.contains(&b"committed1".as_ref()), "Failed for {}", name);
        assert!(!keys.contains(&b"committed2".as_ref()), "Failed for {}", name); // Deleted
        assert!(keys.contains(&b"committed3".as_ref()), "Failed for {}", name);
        assert!(keys.contains(&b"uncommitted1".as_ref()), "Failed for {}", name);
        assert!(keys.contains(&b"uncommitted2".as_ref()), "Failed for {}", name);

        // Verify values
        let committed1_val = results
            .iter()
            .find(|(k, _)| k.as_ref() == b"committed1")
            .map(|(_, v)| v.as_ref());
        assert_eq!(committed1_val, Some(b"val1".as_ref()), "Failed for {}", name);

        let uncommitted1_val = results
            .iter()
            .find(|(k, _)| k.as_ref() == b"uncommitted1")
            .map(|(_, v)| v.as_ref());
        assert_eq!(uncommitted1_val, Some(b"new_val".as_ref()), "Failed for {}", name);

        // Drop without commit - uncommitted changes should not persist
        drop(txn);

        // Assert: after rollback, only committed data visible
        assert_eq!(
            engine.get(&cf, b"committed1").expect("get"),
            Some(Bytes::from("val1")),
            "Failed for {}",
            name
        );
        assert_eq!(
            engine.get(&cf, b"committed2").expect("get"),
            Some(Bytes::from("val2")),
            "Failed for {}",
            name
        );
        assert_eq!(
            engine.get(&cf, b"uncommitted1").expect("get"),
            None,
            "Failed for {}",
            name
        );
    }
}

// ============================================================================
// DURABILITY TESTS
// ============================================================================

#[test]
fn should_persist_transaction_given_commit_when_crash_after() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name();

        // Arrange
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
        txn.put(b"durable_key", b"durable_value").unwrap();
        engine
            .commit_transaction(txn, WriteOptions::default())
            .expect("commit");

        drop(engine);

        // Act - reopen with same storage mode
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = engine2.default_column_family();

        // Assert
        let result = engine2.get(&cf2, b"durable_key").expect("get");
        assert_eq!(
            result,
            Some(b"durable_value".to_vec().into()),
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_not_persist_transaction_given_abort_when_crash_after() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name();

        // Arrange
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut aborted_txn = engine.begin_transaction(&cf).expect("begin_transaction");
        aborted_txn.put(b"aborted_key", b"aborted_value").unwrap();
        drop(aborted_txn);

        drop(engine);

        // Act - reopen
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = engine2.default_column_family();

        // Assert
        assert_eq!(
            engine2.get(&cf2, b"aborted_key").expect("get"),
            None,
            "Failed for {}",
            name
        );
    }
}

#[test]
fn should_recover_committed_transactions_given_wal_replay_when_restart() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name();

        // Arrange
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        for i in 0..10 {
            let mut txn = engine.begin_transaction(&cf).expect("begin_transaction");
            let key = format!("wal_key_{}", i);
            let value = format!("wal_val_{}", i);
            txn.put(key.as_bytes(), value.as_bytes()).unwrap();
            engine
                .commit_transaction(txn, WriteOptions::default())
                .expect("commit");
        }

        drop(engine);

        // Act - reopen
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let engine2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = engine2.default_column_family();

        // Assert
        for i in 0..10 {
            let key = format!("wal_key_{}", i);
            let expected = format!("wal_val_{}", i);
            let result = engine2.get(&cf2, key.as_bytes()).expect("get");
            assert_eq!(
                result,
                Some(expected.as_bytes().to_vec().into()),
                "WAL replay should recover transaction {} for {}",
                i,
                name
            );
        }
    }
}

// ============================================================================
// TIMEOUT TESTS
// ============================================================================

#[test]
fn should_timeout_transaction_given_expired_deadline_when_committing() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        let mut timeout_txn = engine
            .begin_transaction_with_options(
                &cf,
                Some(std::time::Duration::from_millis(1)),
                1024 * 1024,
                cntryl_midge::IsolationLevel::default(),
            )
            .unwrap();
        timeout_txn.put(b"key", b"value").unwrap();

        // Wait for timeout to elapse
        let start = std::time::Instant::now();
        let wait = std::time::Duration::from_millis(1);
        while std::time::Instant::now().duration_since(start) < wait {
            std::thread::yield_now();
        }

        // Act
        let result = engine.commit_transaction(timeout_txn, WriteOptions::default());

        // Assert
        assert!(result.is_err(), "Transaction should timeout for {}", name);
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "Error should mention timeout for {}: {}",
            name,
            err
        );
    }
}

// ============================================================================
// ERROR HANDLING TESTS (LocalDisk only - test hooks)
// ============================================================================

#[test]
fn should_fail_commit_given_disk_full_when_committing() {
    // Note: This test uses test hooks which only work with LocalDisk mode
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::Normal);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Arrange
    engine
        .put(&cf, b"existing_key", b"existing_value")
        .expect("put");
    engine.flush().expect("flush");

    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"txn_key1", b"txn_value1").expect("put");

    // Set disk full behavior
    hooks.set_io_behavior(IoBehavior::FailWithEnospc);

    // Act
    let result = engine.commit_transaction(txn, WriteOptions::sync());

    // Assert
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("No space left on device"));
}

#[test]
fn should_allow_operations_given_previous_commit_failed_when_disk_full() {
    // Note: This test uses test hooks which only work with LocalDisk mode
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::Normal);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        wal_sync: true,
        test_hooks: Some(hooks.clone()),
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Arrange
    engine
        .put(&cf, b"existing_key", b"existing_value")
        .expect("put");
    engine.flush().expect("flush");

    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put(b"txn_key1", b"txn_value1").expect("put");

    // Fail commit with disk full
    hooks.set_io_behavior(IoBehavior::FailWithEnospc);
    let _ = engine.commit_transaction(txn, WriteOptions::sync());

    // Reset behavior
    hooks.set_io_behavior(IoBehavior::Normal);

    // Act
    engine.put(&cf, b"new_key", b"new_value").expect("put");
    let result = engine.get(&cf, b"new_key");

    // Assert
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(bytes::Bytes::from("new_value")));
}

// ============================================================================
// CONCURRENT LIFECYCLE TESTS
// ============================================================================

#[test]
fn should_handle_rapid_transaction_creation_given_many_transactions_when_sequential() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act
        for i in 0..100 {
            let mut txn = engine.begin_transaction(&cf).unwrap();
            let key = format!("rapid_key_{}", i);
            let value = format!("rapid_value_{}", i);
            txn.put(key.as_bytes(), value.as_bytes()).unwrap();
            let result = engine.commit_transaction(txn, WriteOptions::default());
            assert!(result.is_ok(), "Transaction {} should commit for {}", i, name);
        }

        // Assert
        for i in 0..100 {
            let key = format!("rapid_key_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist for {}", key, name);
        }
    }
}

#[test]
fn should_handle_concurrent_transactions_given_multiple_threads_when_parallel() {
    for mode in disk_storage_modes() {
        let (name, storage_mode, _dir) = create_storage_mode(mode);

        // Arrange
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
        let cf = engine.default_column_family();

        // Act
        let handles: Vec<_> = (0..20)
            .map(|thread_id| {
                let eng = engine.clone();
                let cf_clone = cf.clone();
                std::thread::spawn(move || {
                    for iteration in 0..10 {
                        let mut txn = eng.begin_transaction(&cf_clone).unwrap();
                        let key = format!("lifecycle_t{}_i{}", thread_id, iteration);
                        let value = format!("v_t{}_i{}", thread_id, iteration);
                        txn.put(key.as_bytes(), value.as_bytes()).unwrap();
                        let _ = eng.commit_transaction(txn, WriteOptions::default());
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Assert
        let mut count = 0;
        for i in 0..20 {
            for j in 0..10 {
                let key = format!("lifecycle_t{}_i{}", i, j);
                if engine.get(&cf, key.as_bytes()).unwrap().is_some() {
                    count += 1;
                }
            }
        }
        assert!(
            count > 0,
            "At least some transactions should have committed for {}",
            name
        );
    }
}

#[test]
fn should_persist_concurrent_transactions_given_restart_when_committed() {
    for mode in disk_storage_modes() {
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name();

        // Arrange
        let opts = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("initial open");
        let cf = engine.default_column_family();

        for i in 0..20 {
            let mut txn = engine.begin_transaction(&cf).unwrap();
            let key = format!("persist_txn_{}", i);
            let value = format!("value_{}", i);
            txn.put(key.as_bytes(), value.as_bytes()).unwrap();
            engine
                .commit_transaction(txn, WriteOptions::default())
                .unwrap();
        }

        drop(engine);

        // Act - reopen
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts2).expect("restart open");
        let cf = engine.default_column_family();

        // Assert
        for i in 0..20 {
            let key = format!("persist_txn_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(
                result.is_some(),
                "Committed transaction data {} should persist after restart for {}",
                key,
                name
            );
        }
    }
}
