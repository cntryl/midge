// Copyright (c) 2025 Cntryl, Inc.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

//! Transaction visibility and last-write-wins behavior tests.
//!
//! These tests cover the currently implemented guarantees: hidden uncommitted
//! writes, read-your-own-writes, read-only snapshot behavior, and LWW commit
//! outcomes. They do not claim serializable, phantom-free, or full snapshot
//! isolation for read-write transactions.

use bytes::Bytes;
mod common;
use common::*;
use std::sync::Arc;

// ============================================================================
// DIRTY READ PREVENTION TESTS
// ============================================================================

#[test]
fn should_prevent_dirty_read_given_uncommitted_write_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key".to_vec(), b"uncommitted".to_vec(), None)
            .unwrap();

        // Other transaction should not see uncommitted write
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key").unwrap();

        // Assert
        assert_eq!(value, None); // No dirty read
    });
}

#[test]
fn should_not_see_uncommitted_write_given_concurrent_transaction_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn1.put(b"key".to_vec(), b"uncommitted".to_vec(), None)
            .unwrap();

        let txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let value = txn2.get(b"key").unwrap();

        // Assert - concurrent read-write transaction does not see the
        // uncommitted write either
        assert_eq!(value, None);

        // Assert - once txn1 commits, a fresh reader observes the value
        txn1.commit(buffered_write_options(mode)).unwrap();
        let confirm_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            confirm_tx.get(b"key").unwrap(),
            Some(Bytes::from_static(b"uncommitted"))
        );

        // Cleanup
        drop(txn2);
    });
}

#[test]
fn should_allow_dirty_write_given_uncommitted_update_when_serialized() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn1.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();

        // txn2 can write to same key (LWW semantics)
        let mut txn2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn2.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();

        txn1.commit(buffered_write_options(mode)).unwrap();
        txn2.commit(buffered_write_options(mode)).unwrap();

        // Assert - last write wins
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key").unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value2")));
    });
}

// ============================================================================
// READ-YOUR-OWN-WRITES TESTS
// ============================================================================

#[test]
fn should_read_uncommitted_value_given_put_in_same_transaction_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
        let value = txn.get(b"key").unwrap();

        // Assert - should read own uncommitted write
        assert_eq!(value, Some(Bytes::from_static(b"value")));
    });
}

// ============================================================================
// READ VISIBILITY TESTS
// ============================================================================

#[test]
fn should_read_latest_committed_value_given_new_reader_after_concurrent_write() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key".to_vec(), b"initial".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let mut concurrent_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        concurrent_tx
            .put(b"key".to_vec(), b"updated".to_vec(), None)
            .unwrap();
        concurrent_tx.commit(buffered_write_options(mode)).unwrap();

        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let value = read_tx.get(b"key").unwrap();

        // Assert
        assert_eq!(value, Some(Bytes::from_static(b"updated")));
    });
}

#[test]
fn should_return_old_value_given_snapshot_before_write_when_reading() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx.put(b"key".to_vec(), b"v1".to_vec(), None).unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act - transaction captures snapshot at start
        let snap_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();

        // Write v2 after snapshot transaction started
        let mut update_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        update_tx
            .put(b"key".to_vec(), b"v2".to_vec(), None)
            .unwrap();
        update_tx.commit(buffered_write_options(mode)).unwrap();

        // Assert - new transaction sees updated value
        let current_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let current_value = current_tx.get(b"key").unwrap();
        assert_eq!(current_value, Some(Bytes::from_static(b"v2")));

        // Assert - snapshot transaction still sees the old value
        let snap_value = snap_tx.get(b"key").unwrap();
        assert_eq!(snap_value, Some(Bytes::from_static(b"v1")));
    });
}

// ============================================================================
// CONCURRENT MODIFICATION TESTS
// ============================================================================

#[test]
fn should_allow_commit_given_read_key_modified_when_concurrent_write() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key".to_vec(), b"initial".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        // The transaction actually reads the key before any concurrent write
        let read_value = txn.get(b"key").unwrap();
        assert_eq!(read_value, Some(Bytes::from_static(b"initial")));

        // Concurrent transaction modifies the same key
        let mut concurrent_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        concurrent_tx
            .put(b"key".to_vec(), b"concurrent".to_vec(), None)
            .unwrap();
        concurrent_tx.commit(buffered_write_options(mode)).unwrap();

        // Transaction commit should succeed (LWW semantics) even though it
        // only read a key that a concurrent transaction subsequently
        // modified and committed
        let result = txn.commit(buffered_write_options(mode));
        assert!(result.is_ok());

        // Assert - since the transaction made no writes of its own, the
        // concurrent write stands as the final value
        let final_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            final_tx.get(b"key").unwrap(),
            Some(Bytes::from_static(b"concurrent"))
        );
    });
}

#[test]
fn should_allow_put_commit_given_read_key_modified_when_concurrent_write() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key".to_vec(), b"initial".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let _value = read_tx.get(b"key").unwrap();

        // Concurrent modification
        let mut concurrent_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        concurrent_tx
            .put(b"key".to_vec(), b"concurrent".to_vec(), None)
            .unwrap();
        concurrent_tx.commit(buffered_write_options(mode)).unwrap();

        // Transaction writes new value
        txn.put(b"key".to_vec(), b"txn_value".to_vec(), None)
            .unwrap();
        txn.commit(buffered_write_options(mode)).unwrap();

        // Assert - transaction write wins
        let final_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let final_value = final_tx.get(b"key").unwrap();
        assert_eq!(final_value, Some(Bytes::from_static(b"txn_value")));
    });
}

#[test]
fn should_allow_concurrent_puts_given_different_keys_when_multiple_transactions() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let engine1 = Arc::clone(&engine);
        let engine2 = Arc::clone(&engine);

        // Act
        let write_options = buffered_write_options(mode);
        let handle1 = std::thread::spawn(move || {
            let cf = engine1.create_column_family("test").expect("create cf");
            let mut txn = engine1
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            txn.commit(write_options)
        });

        let write_options = buffered_write_options(mode);
        let handle2 = std::thread::spawn(move || {
            let cf = engine2.create_column_family("test").expect("create cf");
            let mut txn = engine2
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
            txn.commit(write_options)
        });

        // Assert
        assert!(handle1.join().unwrap().is_ok());
        assert!(handle2.join().unwrap().is_ok());

        let cf = engine.create_column_family("test").expect("create cf");
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(
            tx.get(b"key1").unwrap(),
            Some(Bytes::from_static(b"value1"))
        );
        assert_eq!(
            tx.get(b"key2").unwrap(),
            Some(Bytes::from_static(b"value2"))
        );
    });
}

// ============================================================================
// ROLLBACK AND ABORT TESTS
// ============================================================================

#[test]
fn should_rollback_all_operations_given_transaction_when_aborted() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
        txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
        txn.delete(b"key3".to_vec()).unwrap();

        drop(txn); // Rollback

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        assert_eq!(read_tx.get(b"key1").unwrap(), None);
        assert_eq!(read_tx.get(b"key2").unwrap(), None);
    });
}

#[test]
fn should_read_latest_committed_value_after_multiple_updates() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let mut setup_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        setup_tx
            .put(b"key".to_vec(), b"initial".to_vec(), None)
            .unwrap();
        setup_tx.commit(buffered_write_options(mode)).unwrap();

        // Act
        for i in 1..=5 {
            let mut update_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            update_tx
                .put(b"key".to_vec(), format!("v{i}").as_bytes().to_vec(), None)
                .unwrap();
            update_tx.commit(buffered_write_options(mode)).unwrap();
        }

        // Assert
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let final_value = read_tx.get(b"key").unwrap();
        assert_eq!(final_value, Some(Bytes::from_static(b"v5")));
    });
}

// ============================================================================
// STRESS TESTS
// ============================================================================

#[test]
fn should_maintain_isolation_under_concurrent_transaction_pressure_when_stressed() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let mut handles = vec![];

        // Act - spawn 20 transactions writing different keys
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        for i in 0..20 {
            let engine_clone = Arc::clone(&engine);
            let write_options = buffered_write_options(mode);
            let handle = std::thread::spawn(move || {
                let mut txn = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                let key = format!("key{i}");
                let value = format!("value{i}");
                txn.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .unwrap();
                txn.commit(write_options)
            });
            handles.push(handle);
        }

        // Assert
        for handle in handles {
            assert!(handle.join().unwrap().is_ok());
        }

        let cf = engine.create_column_family("test").expect("create cf");
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        for i in 0..20 {
            let key = format!("key{i}");
            let expected = format!("value{i}");
            assert_eq!(
                read_tx.get(key.as_bytes()).unwrap(),
                Some(Bytes::copy_from_slice(expected.as_bytes()))
            );
        }
    });
}

#[test]
fn should_handle_high_concurrency_readers_given_many_transactions_when_active() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();

        for i in 0..10 {
            let key = format!("key{i}");
            let value = format!("value{i}");
            let mut tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                .unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
        }

        let mut handles = vec![];

        // Act - 50 readers, each capturing what it actually observed
        for _ in 0..50 {
            let engine_clone = Arc::clone(&engine);
            let handle = std::thread::spawn(move || {
                // Read all keys and collect the results
                let mut observed = Vec::with_capacity(10);
                for i in 0..10 {
                    let key = format!("key{i}");
                    let read_tx = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .unwrap();
                    let value = read_tx.get(key.as_bytes()).unwrap();
                    observed.push(value);
                }
                observed
            });
            handles.push(handle);
        }

        // Assert - every reader observes exactly the value that was written
        // for each key; nothing is missing (None) or torn/garbled, since all
        // writes committed before any reader began.
        for handle in handles {
            let observed = handle.join().unwrap();
            assert_eq!(observed.len(), 10);
            for (i, value) in observed.into_iter().enumerate() {
                let expected = format!("value{i}");
                assert_eq!(
                    value,
                    Some(Bytes::copy_from_slice(expected.as_bytes())),
                    "reader observed unexpected value for key{i}"
                );
            }
        }
    });
}

#[test]
fn should_maintain_consistency_with_mixed_reader_writer_load_when_concurrent() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let mut writer_handles = vec![];
        let mut reader_handles = vec![];

        // Act - 10 writers
        for i in 0..10 {
            let engine_clone = Arc::clone(&engine);
            let write_options = buffered_write_options(mode);
            let handle = std::thread::spawn(move || {
                let mut txn = engine_clone
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                let key = format!("key{i}");
                let value = format!("value{i}");
                txn.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .unwrap();
                txn.commit(write_options)
            });
            writer_handles.push(handle);
        }

        // 20 readers, each capturing what it actually observed per key
        for _ in 0..20 {
            let engine_clone = Arc::clone(&engine);
            let handle = std::thread::spawn(move || {
                let mut observed = Vec::with_capacity(5);
                for i in 0..5 {
                    let key = format!("key{i}");
                    let read_tx = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .unwrap();
                    let value = read_tx.get(key.as_bytes()).unwrap();
                    observed.push((i, value));
                }
                observed
            });
            reader_handles.push(handle);
        }

        // Assert - all writers succeed
        for handle in writer_handles {
            handle.join().unwrap().unwrap();
        }

        // Assert - readers racing with writers only ever observe a
        // consistent state for each key: either the key hasn't committed yet
        // (None), or it holds exactly the value that was written for it.
        // This engine does not guarantee snapshot isolation for read-write
        // transactions, so either outcome is valid, but no reader may ever
        // observe a phantom or torn value.
        for handle in reader_handles {
            let observed = handle.join().unwrap();
            for (i, value) in observed {
                let expected = format!("value{i}");
                match value {
                    None => {}
                    Some(bytes) => {
                        assert_eq!(
                            bytes,
                            Bytes::copy_from_slice(expected.as_bytes()),
                            "reader observed a phantom/torn value for key{i}"
                        );
                    }
                }
            }
        }

        // Final state after all writers have committed must reflect every
        // write exactly - no lost updates.
        let final_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        for i in 0..10 {
            let key = format!("key{i}");
            let expected = format!("value{i}");
            assert_eq!(
                final_tx.get(key.as_bytes()).unwrap(),
                Some(Bytes::copy_from_slice(expected.as_bytes())),
                "missing or incorrect final value for key{i}"
            );
        }
    });
}
