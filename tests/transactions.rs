//! Transaction Tests
//!
//! Consolidated from: `transaction_basic.rs`, `transaction_advanced.rs`, `transaction_conflicts.rs`, `transaction_isolation.rs`, `transaction_isolation_lww.rs`, `transaction_semantics_hardening.rs`, `transaction_snapshot_tracking.rs`, `transaction_spill.rs`, `transaction_spill_hardening.rs`, `runtime_transaction_coalescing.rs`

mod common;

mod transaction_basic {
    //! Transaction Basic Tests
    //!
    //! Core transaction functionality: begin, commit, rollback, isolation.

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::{MidgeError, Query, WriteOptions};
    use std::sync::Arc;

    fn collect_scan_and_assert_exhausted(
        mut scan: cntryl_midge::ScanIterator<'_>,
    ) -> Vec<(Bytes, Bytes)> {
        let mut rows = Vec::new();
        for row in scan.by_ref() {
            rows.push(row.expect("scan row"));
        }
        assert!(scan.exhausted());
        assert!(!scan.failed());
        assert!(
            scan.next().is_none(),
            "exhausted iterator must stay exhausted"
        );
        rows
    }

    // ============================================================================
    // Commit Tests
    // ============================================================================

    #[test]
    fn should_reject_insert_given_read_only_transaction_when_called() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin read-only transaction");

            // Act
            let result = txn.insert(b"key".to_vec(), b"value".to_vec(), None);

            // Assert
            assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
        });
    }

    #[test]
    fn should_reject_delete_given_read_only_transaction_when_called() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin read-only transaction");

            // Act
            let result = txn.delete(b"key".to_vec());

            // Assert
            assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
        });
    }

    #[test]
    fn should_reject_delete_range_given_read_only_transaction_when_called() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin read-only transaction");

            // Act
            let result = txn.delete_range(b"a".to_vec(), b"z".to_vec());

            // Assert
            assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
        });
    }

    #[test]
    fn should_commit_transaction_given_multiple_operations_when_committed() {
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
            txn.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key1").unwrap(),
                Some(Bytes::from_static(b"value1"))
            );
            assert_eq!(
                read_tx.get(b"key2").unwrap(),
                Some(Bytes::from_static(b"value2"))
            );
            assert_eq!(read_tx.get(b"key3").unwrap(), None);
        });
    }

    #[test]
    fn should_succeed_given_empty_transaction_when_committed() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let result = txn.commit(buffered_write_options(mode));

            // Assert
            assert!(result.is_ok());
        });
    }

    #[test]
    fn should_reject_cloud_strict_given_empty_non_cloud_transaction_when_committed() {
        // Arrange
        let opts = opts_for_mode("memory");
        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");

        // Act
        let result = txn.commit(WriteOptions::cloud_strict());

        // Assert
        assert!(matches!(result, Err(MidgeError::InvalidArgument(_))));
    }

    #[test]
    fn should_succeed_given_empty_cloud_strict_cloud_transaction_when_committed() {
        // Arrange
        let opts = opts_for_mode("cloud");
        let engine = open_with_mode(&opts, "cloud");
        let cf = engine.create_column_family("test").expect("create cf");
        let txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");

        // Act
        let result = txn.commit(WriteOptions::cloud_strict());

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_local_only_write_options_given_cloud_transaction_when_committed() {
        // Arrange
        let opts = opts_for_mode("cloud");
        let engine = open_with_mode(&opts, "cloud");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let sync_result = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx")
            .commit(WriteOptions::sync());
        let buffered_result = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx")
            .commit(WriteOptions::buffered());

        // Assert
        assert!(matches!(sync_result, Err(MidgeError::InvalidArgument(_))));
        assert!(matches!(
            buffered_result,
            Err(MidgeError::InvalidArgument(_))
        ));
    }

    #[test]
    fn should_unregister_snapshot_given_commit_of_read_only_transaction_when_committing() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut write_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            write_tx
                .put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            write_tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                engine
                    .get_runtime_metrics()
                    .expect("metrics before read-only commit")
                    .active_snapshots,
                1
            );
            let _value = txn.get(b"key1").unwrap();
            let result = txn.commit(buffered_write_options(mode));

            // Assert
            assert!(result.is_ok());
            assert_eq!(
                engine
                    .get_runtime_metrics()
                    .expect("metrics after read-only commit")
                    .active_snapshots,
                0,
                "mode: {mode}; committing must unregister the read-only snapshot"
            );
        });
    }

    // ============================================================================
    // Rollback Tests
    // ============================================================================

    #[test]
    fn should_rollback_transaction_given_uncommitted_when_dropped() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            {
                let mut txn = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
                // txn dropped here without commit
            }

            // Assert - writes not visible
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(read_tx.get(b"key1").unwrap(), None);
        });
    }

    #[test]
    fn should_rollback_all_writes_given_multiple_operations_when_dropped() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"original".to_vec(), None)
                .unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            {
                let mut txn = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn.put(b"key1".to_vec(), b"updated".to_vec(), None)
                    .unwrap();
                txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
                txn.delete(b"key3".to_vec()).unwrap();
                // txn dropped without commit
            }

            // Assert - original value preserved, new writes not visible
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key1").unwrap(),
                Some(Bytes::from_static(b"original"))
            );
            assert_eq!(read_tx.get(b"key2").unwrap(), None);
        });
    }

    #[test]
    fn should_release_locks_given_aborted_transaction_when_cleanup() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act - first txn acquires lock and aborts
            {
                let mut txn1 = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
                    .unwrap();
                // Dropped without commit - should release lock
            }

            // Second txn should be able to acquire the lock
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn2.put(b"key1".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key1").unwrap(),
                Some(Bytes::from_static(b"value2"))
            );
        });
    }

    // ============================================================================
    // Snapshot Isolation
    // ============================================================================

    #[test]
    fn should_allow_concurrent_writes_with_lww_semantics_given_transaction_when_active() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act - start transaction (captures snapshot)
            let txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();

            // Concurrent write happens outside transaction
            let mut tx2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx2.put(b"key1".to_vec(), b"v2".to_vec(), None).unwrap();
            tx2.commit(buffered_write_options(mode)).unwrap();

            // Assert - Midge implements Last-Write-Wins (LWW) semantics
            // Transactions see latest committed data (not true snapshot isolation)
            let value = txn.get(b"key1").unwrap();
            assert!(
                value == Some(Bytes::from_static(b"v1"))
                    || value == Some(Bytes::from_static(b"v2"))
            );

            // Drop transaction
            drop(txn);
        });
    }

    #[test]
    fn should_read_own_writes_given_transaction_when_reading() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();

            // Read within same transaction
            let value = txn.get(b"key1").unwrap();

            // Assert - should see own uncommitted write
            assert_eq!(value, Some(Bytes::from_static(b"value1")));

            txn.commit(buffered_write_options(mode)).unwrap();
        });
    }

    #[test]
    fn should_preserve_write_set_semantics_given_put_delete_put_sequence_when_reading_own_writes() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin transaction");

            // Act
            txn.put(b"key".to_vec(), b"one".to_vec(), None).unwrap();
            txn.delete(b"key".to_vec()).unwrap();
            txn.put(b"key".to_vec(), b"three".to_vec(), None).unwrap();

            // Assert
            assert_eq!(txn.get(b"key").unwrap(), Some(Bytes::from_static(b"three")));
        });
    }

    #[test]
    fn should_preserve_write_set_semantics_given_delete_put_delete_sequence_when_reading_own_writes(
    ) {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin transaction");

            // Act
            txn.delete(b"key".to_vec()).unwrap();
            txn.put(b"key".to_vec(), b"two".to_vec(), None).unwrap();
            txn.delete(b"key".to_vec()).unwrap();

            // Assert
            assert_eq!(txn.get(b"key").unwrap(), None);
        });
    }

    #[test]
    fn should_read_own_writes_given_kv_transaction_when_getting() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"test_key".to_vec(), b"test_value".to_vec(), None)
                .unwrap();
            let value = txn.get(b"test_key").unwrap();

            // Assert
            assert_eq!(value, Some(Bytes::from_static(b"test_value")));
        });
    }

    #[test]
    fn should_hide_deleted_value_given_kv_transaction_when_getting() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"k".to_vec(), b"v".to_vec(), None).unwrap();
            txn.delete(b"k".to_vec()).unwrap();
            let value = txn.get(b"k").unwrap();

            // Assert
            assert_eq!(value, None);
        });
    }

    #[test]
    fn should_persist_writes_given_kv_transaction_when_committed_boxed() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key_commit".to_vec(), b"value_commit".to_vec(), None)
                .unwrap();
            txn.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key_commit").unwrap(),
                Some(Bytes::from_static(b"value_commit"))
            );
        });
    }

    // ============================================================================
    // Transaction Operations
    // ============================================================================

    #[test]
    fn should_insert_value_given_nonexistent_key_when_insert_in_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
            txn.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key1").unwrap(),
                Some(Bytes::from_static(b"value1"))
            );
        });
    }

    #[test]
    fn should_delete_range_given_committed_engine_operation_when_delete_range() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
            tx.put(b"key2".to_vec(), b"v2".to_vec(), None).unwrap();
            tx.put(b"key3".to_vec(), b"v3".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act - delete key1, key2 (not key3)
            let mut delete_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"key1".to_vec(), b"key3".to_vec())
                .unwrap();
            delete_tx.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(read_tx.get(b"key1").unwrap(), None);
            assert_eq!(read_tx.get(b"key2").unwrap(), None);
            assert_eq!(
                read_tx.get(b"key3").unwrap(),
                Some(Bytes::from_static(b"v3"))
            );
        });
    }

    #[test]
    fn should_hide_deleted_key_given_delete_range_and_point_put_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
            tx.put(b"key2".to_vec(), b"v2".to_vec(), None).unwrap();
            tx.put(b"key3".to_vec(), b"v3".to_vec(), None).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let mut delete_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"key1".to_vec(), b"key3".to_vec())
                .unwrap();
            delete_tx.commit(buffered_write_options(mode)).unwrap();

            // Scan after the delete_range is committed
            let txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let results = collect_scan_and_assert_exhausted(
                txn.scan(
                    &Query::new()
                        .start_key(Bytes::from(&b"key0"[..]))
                        .end_key(Bytes::from(&b"key9"[..])),
                )
                .unwrap(),
            );

            // Assert - Should only see key3
            assert_eq!(
                results,
                vec![(Bytes::from_static(b"key3"), Bytes::from_static(b"v3"))]
            );
        });
    }

    #[test]
    fn should_commit_atomically_given_mixed_put_and_delete_range_when_committed_in_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut setup = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup.put(b"key1".to_vec(), b"v1".to_vec(), None).unwrap();
            setup.put(b"key2".to_vec(), b"v2".to_vec(), None).unwrap();
            setup.put(b"key3".to_vec(), b"v3".to_vec(), None).unwrap();
            setup.commit(buffered_write_options(mode)).unwrap();

            // Act - put + delete + delete_range all in one transaction
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(b"key4".to_vec(), b"v4".to_vec(), None).unwrap();
            tx.delete(b"key3".to_vec()).unwrap();
            tx.delete_range(b"key1".to_vec(), b"key3".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert - key1 and key2 gone via range, key3 gone via delete, key4 present
            let read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(read.get(b"key1").unwrap(), None);
            assert_eq!(read.get(b"key2").unwrap(), None);
            assert_eq!(read.get(b"key3").unwrap(), None);
            assert_eq!(read.get(b"key4").unwrap(), Some(Bytes::from_static(b"v4")));
        });
    }

    #[test]
    fn should_reject_delete_range_given_reversed_bounds_when_added_to_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let result = tx.delete_range(b"key9".to_vec(), b"key1".to_vec());

            // Assert
            assert!(
                matches!(result, Err(cntryl_midge::MidgeError::InvalidArgument(_))),
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_see_uncommitted_writes_given_transaction_scan_when_scanning() {
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

            // Scan within transaction
            let results = collect_scan_and_assert_exhausted(
                txn.scan(
                    &Query::new()
                        .start_key(Bytes::from(&b"key0"[..]))
                        .end_key(Bytes::from(&b"key9"[..])),
                )
                .unwrap(),
            );

            // Assert - should see uncommitted writes
            assert_eq!(
                results,
                vec![
                    (Bytes::from_static(b"key1"), Bytes::from_static(b"value1")),
                    (Bytes::from_static(b"key2"), Bytes::from_static(b"value2")),
                ]
            );
        });
    }

    #[test]
    fn should_return_no_rows_given_zero_limit_when_scanning() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin transaction");
            txn.put(b"key".to_vec(), b"value".to_vec(), None)
                .expect("stage value");

            // Act
            let rows = collect_scan_and_assert_exhausted(
                txn.scan(&Query::new().limit(0))
                    .expect("zero-limit scan should initialize"),
            );

            // Assert
            assert!(rows.is_empty());
        });
    }

    // ============================================================================
    // Error Handling
    // ============================================================================

    #[test]
    fn should_unregister_snapshot_given_commit_failure_when_commit_returns_error() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            assert_eq!(
                engine
                    .get_runtime_metrics()
                    .expect("metrics before failed commit")
                    .active_snapshots,
                1
            );

            // Act
            let incompatible_options = if mode == "cloud" {
                WriteOptions::sync()
            } else {
                WriteOptions::cloud_strict()
            };
            let failed_commit = txn1.commit(incompatible_options);

            // Assert
            assert!(matches!(failed_commit, Err(MidgeError::InvalidArgument(_))));
            assert_eq!(
                engine
                    .get_runtime_metrics()
                    .expect("metrics after failed commit")
                    .active_snapshots,
                0,
                "mode: {mode}; failed commit must unregister its snapshot"
            );

            // A later transaction proves failure cleanup did not poison admission.
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key2").unwrap(),
                Some(Bytes::from_static(b"value2"))
            );
        });
    }

    // ============================================================================
    // Persistence Tests
    // ============================================================================

    #[test]
    fn should_persist_transaction_given_commit_when_crash_after() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let mut txn = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
                txn.commit(buffered_write_options(mode)).unwrap();
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.get_column_family("test").expect("get cf");
                let tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .unwrap();
                let value = tx.get(b"key1").unwrap();
                assert_eq!(value, Some(Bytes::from_static(b"value1")));
            }
        });
    }

    #[test]
    fn should_not_persist_transaction_given_abort_when_crash_after() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                let mut txn = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
                // Txn dropped without commit
                drop(txn);
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.get_column_family("test").expect("get cf");
                let tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .unwrap();
                let value = tx.get(b"key1").unwrap();
                assert_eq!(value, None);
            }
        });
    }

    #[test]
    fn should_recover_committed_transactions_given_wal_replay_when_restart() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            // Act (Phase 1)
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Multiple transactions
                let mut txn1 = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
                    .unwrap();
                txn1.commit(buffered_write_options(mode)).unwrap();

                let mut txn2 = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
                    .unwrap();
                txn2.commit(buffered_write_options(mode)).unwrap();

                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert (Phase 2)
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.get_column_family("test").expect("get cf");
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
            }
        });
    }

    // ============================================================================
    // Bulk Load Pattern (best_effort usage)
    // ============================================================================

    #[test]
    fn should_support_best_effort_during_bulk_load_phase_when_followed_by_flush() {
        const BULK_COUNT: usize = 1000;
        const BULK_MEMTABLE_BYTES: usize = 256 * 1024;

        for_each_storage_mode(&all_storage_modes_new(), |mode, mut opts| {
            // Arrange: Initialize engine and column family
            // Keep this fixture's explicit flush authoritative now that memtable
            // limits conservatively include skiplist node and version overhead.
            opts.memtable_size = BULK_MEMTABLE_BYTES;
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("bulk_test").expect("create cf");
            let best_effort_opts = WriteOptions::best_effort();
            let buffered_opts = buffered_write_options(mode);

            // Arrange: Fast bulk load with best_effort (setup; data loss on crash acceptable here)
            for i in 0..BULK_COUNT {
                let mut txn = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                let key = format!("bulk_{i:05}").into_bytes();
                let value = format!("data_{}", i * 2).into_bytes();
                txn.put(key, value, None).unwrap();
                txn.commit(best_effort_opts).unwrap();
            }

            // Act: Flush to ensure bulk-loaded data reaches storage, then write a "measured" key.
            engine.flush_cf(&cf).unwrap();
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"measured_key".to_vec(), b"measured_value".to_vec(), None)
                .unwrap();
            txn.commit(buffered_opts).unwrap();

            // Assert: Verify all data is readable
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();

            // Check bulk-loaded entries
            assert_eq!(
                read_tx.get(b"bulk_00000").unwrap(),
                Some(Bytes::from_static(b"data_0"))
            );
            assert_eq!(
                read_tx.get(b"bulk_00500").unwrap(),
                Some(Bytes::from_static(b"data_1000"))
            );
            assert_eq!(
                read_tx.get(b"bulk_00999").unwrap(),
                Some(Bytes::from_static(b"data_1998"))
            );

            // Check measured-workload entry
            assert_eq!(
                read_tx.get(b"measured_key").unwrap(),
                Some(Bytes::from_static(b"measured_value"))
            );
        });
    }
}

mod transaction_advanced {
    //! Tests for transaction crash recovery and durability semantics
    //!
    //! All tests parametrized across durable storage modes only (`LocalDisk`, `CloudBacked`)
    //! Pattern: `for_each_storage_mode(&durable_storage_modes()`, |mode, opts| { ... })

    use crate::common::*;
    use bytes::Bytes;

    // ============================================================================
    // TRANSACTION CRASH RECOVERY
    // ============================================================================

    /// `should_persist_atomic_transactions_after_restart`
    /// Verify committed transaction persists across crash/restart
    /// Phase 1: Commit transaction, then crash (drop engine)
    /// Phase 2: Restart and verify transaction data persisted
    #[test]
    fn should_persist_atomic_transactions_after_restart() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let opts_clone = opts.clone();

            // Act: Phase 1 - Write and commit
            {
                let mut engine = open_with_mode(&opts_clone, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(b"tx_key1".to_vec(), b"tx_value1".to_vec(), None)
                    .expect("put");
                tx.put(b"tx_key2".to_vec(), b"tx_value2".to_vec(), None)
                    .expect("put");
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before restart");
            }

            // Assert: Phase 2 - Recover
            {
                let engine = open_with_mode(&opts_clone, mode);
                let cf = engine.get_column_family("test").expect("get cf");

                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");
                let got1 = tx_read.get(b"tx_key1").expect("get");
                let got2 = tx_read.get(b"tx_key2").expect("get");

                assert_eq!(
                    got1,
                    Some(Bytes::from_static(b"tx_value1")),
                    "tx_key1 not persisted in mode: {mode}"
                );
                assert_eq!(
                    got2,
                    Some(Bytes::from_static(b"tx_value2")),
                    "tx_key2 not persisted in mode: {mode}"
                );
            }
        });
    }

    /// `should_not_persist_uncommitted_transaction_after_restart`
    /// Verify uncommitted transaction rolls back across crash/restart
    /// Phase 1: Create transaction, write but don't commit, then crash
    /// Phase 2: Restart and verify data was rolled back
    #[test]
    fn should_not_persist_uncommitted_transaction_after_restart() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let opts_clone = opts.clone();

            // Act: Phase 1 - Write but don't commit
            {
                let mut engine = open_with_mode(&opts_clone, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                tx.put(
                    b"uncommitted_key".to_vec(),
                    b"uncommitted_value".to_vec(),
                    None,
                )
                .expect("put");
                drop(tx);
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before restart");
            }

            // Assert: Phase 2 - Recover
            {
                let engine = open_with_mode(&opts_clone, mode);
                let cf = engine.get_column_family("test").expect("get cf");

                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");
                let got = tx_read.get(b"uncommitted_key").expect("get");
                assert_eq!(got, None, "uncommitted data persisted in mode: {mode}");
            }
        });
    }

    /// `should_recover_after_abort_given_transaction_with_delete_range_when_restart`
    /// Verify `delete_range` in transaction persists and recovers correctly
    /// Phase 1: Write initial data, commit transaction with `delete_range`, crash
    /// Phase 2: Restart and verify deleted keys are gone
    #[test]
    fn should_recover_after_abort_given_transaction_with_delete_range_when_restart() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let opts_clone = opts.clone();

            // Act: Phase 1 - Initial data
            {
                let mut engine = open_with_mode(&opts_clone, mode);
                let cf = engine.create_column_family("test").expect("create cf");
                for i in 0..10 {
                    let key = format!("key{i}");
                    let mut tx = engine
                        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin_tx");
                    tx.put(key.as_bytes().to_vec(), b"initial_value".to_vec(), None)
                        .expect("put");
                    tx.commit(buffered_write_options(mode)).expect("commit");
                }
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before delete-range restart");
            }

            // Act: Phase 2 - Delete range as a standalone CF-scoped operation
            {
                let mut engine = open_with_mode(&opts_clone, mode);
                let cf = engine.get_column_family("test").expect("get cf");
                let mut delete_tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin delete_range tx");
                delete_tx
                    .delete_range(b"key3".to_vec(), b"key7".to_vec()) // [key3, key7)
                    .expect("delete_range");
                delete_tx
                    .commit(buffered_write_options(mode))
                    .expect("commit delete_range");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before verification restart");
            }

            // Assert: Phase 3 - Verify delete_range persisted
            {
                let engine = open_with_mode(&opts_clone, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");
                // Keys before range should exist
                assert_eq!(
                    tx_read.get(b"key2").unwrap(),
                    Some(Bytes::from_static(b"initial_value")),
                    "key2 should exist in mode: {mode}"
                );

                // Keys in range should be deleted
                assert_eq!(
                    tx_read.get(b"key5").unwrap(),
                    None,
                    "key5 should be deleted in mode: {mode}"
                );

                // Keys after range should exist
                assert_eq!(
                    tx_read.get(b"key8").unwrap(),
                    Some(Bytes::from_static(b"initial_value")),
                    "key8 should exist in mode: {mode}"
                );
            }
        });
    }

    /// `should_recover_committed_spill_given_restart_after_commit`
    /// Verify large transaction with spill commits and recovers data
    #[test]
    fn should_recover_committed_spill_given_restart_after_commit() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let opts_clone = opts.clone();
            let mut opts = opts.clone();
            opts = opts.memory_budget(100 * 1024); // Force spill with 100KB limit

            // Act: Phase 1 - Write large transaction
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("spill_key{i:04}");
                    let value = format!("spill_value_{i:04}");
                    tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                        .expect("put");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before spill restart");
            }

            // Assert: Phase 2 - Recover and verify
            {
                let engine = open_with_mode(&opts_clone, mode);
                let cf = engine.get_column_family("test").expect("get cf");

                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("spill_key{i:04}");
                    let expected = format!("spill_value_{i:04}");
                    let got = tx_read.get(key.as_bytes()).expect("get");
                    let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                    assert_eq!(
                        got_str,
                        Some(expected),
                        "spill key {key} mismatch in mode: {mode}"
                    );
                }
            }
        });
    }

    /// `should_rollback_uncommitted_spill_given_restart_before_commit`
    /// Verify spilled data from uncommitted transaction is not recovered
    #[test]
    fn should_rollback_uncommitted_spill_given_restart_before_commit() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let opts_clone = opts.clone();
            let mut opts = opts.clone();
            opts = opts.memory_budget(100 * 1024);

            // Act: Phase 1 - Write but don't commit
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("uncom_key{i:04}");
                    let value = format!("uncom_value_{i:04}");
                    tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                        .expect("put");
                }
                drop(tx);
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before spill restart");
            }

            // Assert: Phase 2 - Verify no data recovered
            {
                let engine = open_with_mode(&opts_clone, mode);
                let cf = engine.get_column_family("test").expect("get cf");

                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("uncom_key{i:04}");
                    let got = tx_read.get(key.as_bytes()).expect("get");
                    assert_eq!(
                        got, None,
                        "uncommitted spill data {key} recovered in mode: {mode}"
                    );
                }
            }
        });
    }

    /// `should_handle_transaction_abort_idempotency_given_multiple_restart_cycles`
    /// Verify that an aborted (dropped-without-commit) transaction never resurfaces
    /// across repeated abort+commit+restart cycles, while the sibling committed
    /// write from the same cycle - and every prior cycle's committed/aborted
    /// pair - remains correctly resolved after each restart.
    #[test]
    fn should_handle_transaction_abort_idempotency_given_multiple_restart_cycles() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let opts_clone = opts.clone();

            // Act
            for cycle in 0..3 {
                {
                    let mut engine = open_with_mode(&opts_clone, mode);
                    let cf = engine.create_column_family("test").expect("create cf");

                    // Abort: write then drop without commit.
                    let aborted_key = format!("cycle{cycle}_aborted_key");
                    let mut aborted_tx = engine
                        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin_tx (abort)");
                    aborted_tx
                        .put(
                            aborted_key.as_bytes().to_vec(),
                            b"should_never_persist".to_vec(),
                            None,
                        )
                        .expect("put (abort)");
                    drop(aborted_tx);

                    // Commit: sibling write that should persist.
                    let key = format!("cycle{cycle}_key");
                    let value = format!("cycle{cycle}_value");
                    let mut tx = engine
                        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin_tx");
                    tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                        .expect("put");
                    tx.commit(buffered_write_options(mode)).expect("commit");
                    engine
                        .shutdown(std::time::Duration::from_secs(5))
                        .expect("shutdown before cycle restart");
                }

                {
                    let mut engine = open_with_mode(&opts_clone, mode);
                    let cf = engine.create_column_family("test").expect("create cf");
                    let tx_read = engine
                        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                        .expect("begin_tx");

                    // Assert
                    // This cycle's committed key persisted, aborted key did not.
                    let key = format!("cycle{cycle}_key");
                    let expected = format!("cycle{cycle}_value");
                    let got = tx_read.get(key.as_bytes()).expect("get");
                    let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                    assert_eq!(
                        got_str,
                        Some(expected),
                        "cycle {cycle} committed key missing in mode: {mode}"
                    );

                    let aborted_key = format!("cycle{cycle}_aborted_key");
                    let got_aborted = tx_read.get(aborted_key.as_bytes()).expect("get");
                    assert_eq!(
                        got_aborted, None,
                        "cycle {cycle} aborted key persisted in mode: {mode}"
                    );

                    // Idempotency across restarts: every prior cycle's outcome is unchanged.
                    for prior in 0..cycle {
                        let prior_key = format!("cycle{prior}_key");
                        let prior_expected = format!("cycle{prior}_value");
                        let prior_got = tx_read.get(prior_key.as_bytes()).expect("get");
                        let prior_got_str = prior_got
                            .as_ref()
                            .map(|b| String::from_utf8_lossy(b).to_string());
                        assert_eq!(
                            prior_got_str,
                            Some(prior_expected),
                            "cycle {prior} committed key regressed after cycle {cycle} restart in mode: {mode}"
                        );

                        let prior_aborted_key = format!("cycle{prior}_aborted_key");
                        let prior_got_aborted =
                            tx_read.get(prior_aborted_key.as_bytes()).expect("get");
                        assert_eq!(
                            prior_got_aborted, None,
                            "cycle {prior} aborted key reappeared after cycle {cycle} restart in mode: {mode}"
                        );
                    }

                    drop(tx_read);
                    engine
                        .shutdown(std::time::Duration::from_secs(5))
                        .expect("shutdown after cycle verification");
                }
            }
        });
    }
}

mod transaction_conflicts {
    //! Copyright (c) 2025 Cntryl, Inc.
    //! SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

    // Transaction conflict tests - validates LWW semantics, write conflict handling, and concurrent transaction behavior.
    //
    // Tests ensure that concurrent transactions follow Last-Write-Wins semantics and handle conflicts appropriately.
    // These tests validate logical transaction behavior across all storage modes (Memory, FS, Cloud).

    use crate::common::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn should_assert_expected_value_without_creating_a_write_conflict() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assertions")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin assertion tx");
        txn.assert_value(b"key".to_vec(), Some(b"value".to_vec()))
            .expect("register assertion");
        txn.commit(cntryl_midge::WriteOptions::buffered())
            .expect("assertion commit");

        // Assert
        let mut missing = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin missing assertion tx");
        missing
            .assert_value(b"absent".to_vec(), None)
            .expect("register missing assertion");
        missing
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("missing assertion commit");
    }

    #[test]
    fn should_reject_assertion_when_snapshot_value_differs() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assertions")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"actual".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        // Act
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin assertion tx");
        txn.assert_value(b"key".to_vec(), Some(b"expected".to_vec()))
            .expect("register assertion");
        let result = txn.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
    }

    #[test]
    fn should_validate_assertion_against_snapshot_when_key_is_written() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assertion_snapshot")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"old".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin assertion tx");
        txn.put(b"key".to_vec(), b"new".to_vec(), None)
            .expect("pending write");
        txn.assert_value(b"key".to_vec(), Some(b"old".to_vec()))
            .expect("register assertion");

        // Act
        let result = txn.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(result.is_ok());

        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            read_tx.get(b"key").expect("read value"),
            Some(Bytes::from_static(b"new"))
        );
    }

    #[test]
    fn should_bound_assertion_memory_by_transaction_pool() {
        // Arrange
        let opts = cntryl_midge::OpenOptions::in_memory()
            .transaction_memory_pool_size(512)
            .build()
            .expect("build options");
        let engine = cntryl_midge::Engine::open(opts).expect("open engine");
        let cf = engine
            .create_column_family("assertion_limit")
            .expect("create cf");
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin transaction");

        // Act
        let first = txn.assert_value(vec![b'a'; 128], None);
        let second = txn.assert_value(vec![b'b'; 128], None);

        // Assert
        assert!(first.is_ok());
        assert!(matches!(
            second,
            Err(cntryl_midge::MidgeError::ResourceLimit(_))
        ));
    }

    #[test]
    fn should_reject_assertion_when_write_intent_consumes_shared_transaction_pool() {
        // Arrange
        let opts = cntryl_midge::OpenOptions::in_memory()
            .transaction_memory_pool_size(1_024)
            .build()
            .expect("build options");
        let engine = cntryl_midge::Engine::open(opts).expect("open engine");
        let cf = engine
            .create_column_family("write-pressure")
            .expect("create cf");
        let mut writer = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin writer");
        writer
            .put(b"held".to_vec(), b"value".to_vec(), None)
            .expect("reserve write intent memory");
        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting transaction");

        // Act
        let pressured = asserting.assert_value(vec![b'a'; 512], None);
        drop(writer);
        let admitted_after_release = asserting.assert_value(vec![b'b'; 512], None);

        // Assert
        assert!(matches!(
            pressured,
            Err(cntryl_midge::MidgeError::ResourceLimit(_))
        ));
        assert!(
            admitted_after_release.is_ok(),
            "dropping the writer must release its shared pool reservation"
        );
    }

    #[test]
    fn should_reject_write_intent_when_assertion_consumes_shared_transaction_pool() {
        // Arrange
        let opts = cntryl_midge::OpenOptions::in_memory()
            .transaction_memory_pool_size(1_024)
            .build()
            .expect("build options");
        let engine = cntryl_midge::Engine::open(opts).expect("open engine");
        let cf = engine
            .create_column_family("assertion-pressure")
            .expect("create cf");
        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting transaction");
        asserting
            .assert_value(vec![b'a'; 512], None)
            .expect("reserve assertion memory");
        let mut writer = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin writer");

        // Act
        let pressured = writer.put(b"held".to_vec(), b"value".to_vec(), None);
        drop(asserting);
        let admitted_after_release = writer.put(b"released".to_vec(), b"value".to_vec(), None);

        // Assert
        assert!(matches!(
            pressured,
            Err(cntryl_midge::MidgeError::ResourceLimit(_))
        ));
        assert!(
            admitted_after_release.is_ok(),
            "dropping the assertion owner must release its shared pool reservation"
        );
    }

    #[test]
    fn should_spill_write_intent_when_assertion_consumes_shared_pool_in_local_mode() {
        // Arrange
        let temp_dir = tempfile::tempdir().expect("create database directory");
        let opts = cntryl_midge::OpenOptions::local(temp_dir.path())
            .transaction_memory_pool_size(1_024)
            .build()
            .expect("build options");
        let engine = cntryl_midge::Engine::open(opts).expect("open engine");
        let cf = engine
            .create_column_family("assertion-spill-pressure")
            .expect("create cf");
        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting transaction");
        asserting
            .assert_value(vec![b'a'; 512], None)
            .expect("reserve assertion memory");
        let mut writer = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin writer");

        // Act
        let admitted = writer.put(b"spilled".to_vec(), b"value".to_vec(), None);
        let spill_runs = std::fs::read_dir(temp_dir.path().join("txn"))
            .expect("open transaction spill directory")
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "run"))
            .count();
        let committed = writer.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(
            admitted.is_ok(),
            "local transactions must spill when assertion pressure prevents resident admission"
        );
        assert!(
            spill_runs > 0,
            "assertion pressure must create a transaction spill run before commit"
        );
        assert!(
            committed.is_ok(),
            "the directly spilled write must remain committable: {committed:?}"
        );
        let current = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin verification transaction");
        assert_eq!(
            current.get(b"spilled").expect("read spilled value"),
            Some(Bytes::from_static(b"value"))
        );
        drop(asserting);
    }

    #[test]
    fn should_use_transaction_snapshot_time_when_asserted_value_expires_before_commit() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assertion-ttl")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"ttl-key".to_vec(), b"value".to_vec(), Some(1))
            .expect("seed expiring value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit expiring value");
        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin assertion snapshot");
        asserting
            .assert_value(b"ttl-key".to_vec(), Some(b"value".to_vec()))
            .expect("register ttl assertion");

        // Act
        std::thread::sleep(Duration::from_millis(1_100));
        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(
            result.is_ok(),
            "assertion must use the TTL clock frozen with its transaction snapshot: {result:?}"
        );
        let current = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin current snapshot");
        assert_eq!(
            current.get(b"ttl-key").expect("read current value"),
            None,
            "a new snapshot must observe the value as expired"
        );
    }

    #[test]
    fn should_isolate_assertion_conflicts_between_column_families() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let first_cf = engine
            .create_column_family("assertion-cf-first")
            .expect("create first cf");
        let second_cf = engine
            .create_column_family("assertion-cf-second")
            .expect("create second cf");
        for (cf_id, value) in [
            (first_cf.id(), b"first".as_slice()),
            (second_cf.id(), b"second".as_slice()),
        ] {
            let mut seed = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin seed transaction");
            seed.put(b"shared-key".to_vec(), value.to_vec(), None)
                .expect("seed shared key");
            seed.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit shared key");
        }
        let mut asserting = engine
            .begin_tx(first_cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin first-cf assertion");
        asserting
            .assert_value(b"shared-key".to_vec(), Some(b"first".to_vec()))
            .expect("register first-cf assertion");
        let mut concurrent = engine
            .begin_tx(second_cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin second-cf writer");
        concurrent
            .put(b"shared-key".to_vec(), b"updated".to_vec(), None)
            .expect("update second-cf key");
        concurrent
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit second-cf update");

        // Act
        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(
            result.is_ok(),
            "a same-named key in another column family must not conflict: {result:?}"
        );
    }

    #[test]
    fn should_reject_conflicting_duplicate_assertions_for_one_key() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("duplicate-assertion")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit seed value");
        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting transaction");
        asserting
            .assert_value(b"key".to_vec(), Some(b"value".to_vec()))
            .expect("register matching assertion");
        asserting
            .assert_value(b"key".to_vec(), Some(b"different".to_vec()))
            .expect("register conflicting assertion");

        // Act
        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
    }

    // ============================================================================
    // COMMIT-TIME ASSERTION ENFORCEMENT
    //
    // assert_value validates against the transaction's start snapshot client-side
    // (above), but that alone is a TOCTOU gap: a concurrent commit to the
    // asserted key between validation and this transaction's own commit is
    // invisible to a purely client-side check. These tests cover the server-side
    // enforcement that closes it.
    // ============================================================================

    fn sequence_metric(engine: &cntryl_midge::Engine) -> u64 {
        engine
            .get_runtime_metrics()
            .expect("runtime metrics")
            .current_sequence
    }

    #[test]
    fn should_reject_assertion_when_concurrent_put_lands_after_start_sequence() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-put")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting tx");
        asserting
            .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");
        asserting
            .put(b"other".to_vec(), b"unrelated".to_vec(), None)
            .expect("unrelated write so this isn't an assert-only commit");

        // Act: a concurrent transaction commits a new value to the asserted key
        // before the asserting transaction commits.
        let mut concurrent = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin concurrent tx");
        concurrent
            .put(b"key".to_vec(), b"v2".to_vec(), None)
            .expect("concurrent write");
        concurrent
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("concurrent commit");

        let sequence_before = sequence_metric(&engine);
        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
        assert_eq!(
            sequence_metric(&engine),
            sequence_before,
            "a rejected commit must not advance the sequence"
        );
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            read_tx.get(b"other").expect("read unrelated key"),
            None,
            "a rejected commit must not apply any of its writes"
        );
    }

    #[test]
    fn should_reject_assertion_when_concurrent_delete_lands_after_start_sequence() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-delete")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting tx");
        asserting
            .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");

        // Act
        let mut concurrent = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin concurrent tx");
        concurrent
            .delete(b"key".to_vec())
            .expect("concurrent delete");
        concurrent
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("concurrent commit");

        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
    }

    #[test]
    fn should_reject_assertion_when_concurrent_range_delete_covers_the_key() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-range-delete")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key-b".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting tx");
        asserting
            .assert_value(b"key-b".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");

        // Act: the concurrent range delete never touches "key-b" directly, only
        // covers it — the assertion check must consult covering range deletes,
        // not just point mutations.
        let mut concurrent = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin concurrent tx");
        concurrent
            .delete_range(b"key-a".to_vec(), b"key-z".to_vec())
            .expect("concurrent range delete");
        concurrent
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("concurrent commit");

        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
    }

    #[test]
    fn should_reject_absent_assertion_when_key_is_inserted_concurrently() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-absent-insert")
            .expect("create cf");

        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting tx");
        asserting
            .assert_value(b"key".to_vec(), None)
            .expect("register absent assertion");

        // Act: the key is still absent as of `asserting`'s frozen start
        // snapshot, so client-side validation (checked at commit()) would still
        // pass — only the server-side sequence check catches this.
        let mut concurrent = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin concurrent tx");
        concurrent
            .put(b"key".to_vec(), b"inserted".to_vec(), None)
            .expect("concurrent insert");
        concurrent
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("concurrent commit");

        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
    }

    #[test]
    fn should_reject_assertion_given_aba_value_restored_after_intervening_write() {
        // Arrange: value goes v1 -> v2 -> v1. A value re-read at commit time
        // would see v1 and wrongly pass; the sequence-based check must still
        // reject because the key's sequence advanced twice after start_sequence.
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-aba")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting tx");
        asserting
            .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");

        // Act
        let mut to_v2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx to v2");
        to_v2
            .put(b"key".to_vec(), b"v2".to_vec(), None)
            .expect("write v2");
        to_v2
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit v2");

        let mut back_to_v1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx back to v1");
        back_to_v1
            .put(b"key".to_vec(), b"v1".to_vec(), None)
            .expect("restore v1");
        back_to_v1
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit restored v1");

        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(
            matches!(result, Err(cntryl_midge::MidgeError::WriteConflict(_))),
            "ABA-restoring the value must not defeat the assertion, got: {result:?}"
        );
    }

    #[test]
    fn should_allow_writing_asserted_key_in_same_transaction() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-self")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        // Act: a compare-and-swap pattern — assert the current value, then
        // write a new one, all in the same transaction. The transaction's own
        // pending write must not be mistaken for an external conflict.
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin cas tx");
        txn.assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");
        txn.put(b"key".to_vec(), b"v2".to_vec(), None)
            .expect("cas write");
        let result = txn.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(result.is_ok(), "expected CAS commit to succeed: {result:?}");
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            read_tx.get(b"key").expect("read value"),
            Some(Bytes::from_static(b"v2"))
        );
    }

    #[test]
    fn should_commit_disjoint_assertion_with_write() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-disjoint")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"guard".to_vec(), b"unchanged".to_vec(), None)
            .expect("seed guard value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        // Act: assert an unrelated key while writing a different one; neither
        // should interfere with the other.
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");
        txn.assert_value(b"guard".to_vec(), Some(b"unchanged".to_vec()))
            .expect("register assertion");
        txn.put(b"data".to_vec(), b"value".to_vec(), None)
            .expect("unrelated write");
        let result = txn.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(result.is_ok());
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        assert_eq!(
            read_tx.get(b"data").expect("read data"),
            Some(Bytes::from_static(b"value"))
        );
    }

    #[test]
    fn should_enforce_assertion_conflict_even_under_last_write_wins_policy() {
        // Arrange: LastWriteWins is the default and normally lets a later
        // committer silently overwrite an earlier reader's view. An explicit
        // assertion is a stronger, opt-in guarantee and must still be enforced.
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-lww")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin asserting tx");
        assert_eq!(
            asserting.conflict_policy(),
            cntryl_midge::ConflictPolicy::LastWriteWins,
            "this test exercises the default policy"
        );
        asserting
            .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");
        asserting
            .put(b"other".to_vec(), b"value".to_vec(), None)
            .expect("unrelated write");

        // Act
        let mut concurrent = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin concurrent tx");
        concurrent
            .put(b"key".to_vec(), b"v2".to_vec(), None)
            .expect("concurrent write");
        concurrent
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("concurrent commit");

        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
    }

    #[test]
    fn should_validate_assertion_only_commit_without_allocating_a_sequence() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-only-commit")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        // Act: no put/delete/delete_range calls at all — only an assertion.
        let mut txn = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin assert-only tx");
        txn.assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");

        let sequence_before = sequence_metric(&engine);
        let result = txn.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(
            result.is_ok(),
            "expected assert-only commit to succeed: {result:?}"
        );
        assert_eq!(
            sequence_metric(&engine),
            sequence_before,
            "an assert-only commit must not allocate a sequence"
        );
    }

    #[test]
    fn should_reject_assertion_only_commit_when_key_changed_concurrently() {
        // Arrange
        let engine = Arc::new(open_with_mode(&MidgeOptions::default(), "memory"));
        let cf = engine
            .create_column_family("assert-only-reject")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin seed tx");
        seed.put(b"key".to_vec(), b"v1".to_vec(), None)
            .expect("seed value");
        seed.commit(cntryl_midge::WriteOptions::buffered())
            .expect("seed commit");

        let mut asserting = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin assert-only tx");
        asserting
            .assert_value(b"key".to_vec(), Some(b"v1".to_vec()))
            .expect("register assertion");

        // Act
        let mut concurrent = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin concurrent tx");
        concurrent
            .put(b"key".to_vec(), b"v2".to_vec(), None)
            .expect("concurrent write");
        concurrent
            .commit(cntryl_midge::WriteOptions::buffered())
            .expect("concurrent commit");

        let sequence_before = sequence_metric(&engine);
        let result = asserting.commit(cntryl_midge::WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(cntryl_midge::MidgeError::WriteConflict(_))
        ));
        assert_eq!(
            sequence_metric(&engine),
            sequence_before,
            "a rejected assert-only commit must not allocate a sequence"
        );
    }

    #[test]
    fn should_reject_assertion_conflict_in_a_spilled_transaction() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange: a small memory budget forces the write set to spill to
            // disk, exercising validate_spilled_transaction's assertion check
            // rather than the in-memory path.
            let mut opts = opts;
            opts = opts.memory_budget(256 * 1024);
            let engine = open_with_mode(&opts, mode);
            let cf = engine
                .create_column_family("assert-spill")
                .expect("create cf");

            let mut seed = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin seed tx");
            seed.put(b"guard".to_vec(), b"v1".to_vec(), None)
                .expect("seed value");
            seed.commit(buffered_write_options(mode))
                .expect("seed commit");

            let mut asserting = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin asserting tx");
            asserting
                .assert_value(b"guard".to_vec(), Some(b"v1".to_vec()))
                .expect("register assertion");
            for i in 0..200 {
                let key = format!("spill-key{i:04}");
                let value = format!("spill-value_{i:04}");
                asserting
                    .put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }

            // Act
            let mut concurrent = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin concurrent tx");
            concurrent
                .put(b"guard".to_vec(), b"v2".to_vec(), None)
                .expect("concurrent write");
            concurrent
                .commit(buffered_write_options(mode))
                .expect("concurrent commit");

            let result = asserting.commit(buffered_write_options(mode));

            // Assert
            assert!(
                matches!(result, Err(cntryl_midge::MidgeError::WriteConflict(_))),
                "expected spilled commit to reject the stale assertion in mode {mode}, got: {result:?}"
            );

            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin read tx");
            assert_eq!(
                read_tx.get(b"spill-key0000").expect("read spilled key"),
                None,
                "a rejected spilled commit must not apply any of its writes in mode {mode}"
            );
        });
    }

    // ============================================================================
    // BASIC LWW SEMANTICS TESTS
    // ============================================================================

    #[test]
    fn should_allow_concurrent_puts_to_same_key_given_lww_semantics() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();

            txn1.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
            txn2.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();

            txn1.commit(buffered_write_options(mode)).unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            // Assert - last committed wins
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx.get(b"key").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"value2")));
        });
    }

    #[test]
    fn should_accept_both_committers_given_concurrent_puts_when_lww() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let engine1 = Arc::clone(&engine);
            let engine2 = Arc::clone(&engine);

            // Act
            let write_options = buffered_write_options(mode);
            let handle1 = std::thread::spawn(move || {
                let mut txn = engine1
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
                txn.commit(write_options)
            });

            let write_options = buffered_write_options(mode);
            let handle2 = std::thread::spawn(move || {
                let mut txn = engine2
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();
                txn.commit(write_options)
            });

            // Assert - both commits succeed
            assert!(handle1.join().unwrap().is_ok());
            assert!(handle2.join().unwrap().is_ok());
        });
    }

    #[test]
    fn should_finish_concurrent_local_same_key_lww_buffered_commits_within_timeout() {
        // Arrange
        let mode = "local";
        let opts = opts_for_mode(mode);
        let engine = Arc::new(open_with_mode(&opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");
        let cf_id = cf.id();
        let worker_count = 16usize;
        let barrier = Arc::new(std::sync::Barrier::new(worker_count + 1));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut handles = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let worker_engine = Arc::clone(&engine);
            let worker_barrier = Arc::clone(&barrier);
            let worker_result_tx = result_tx.clone();
            handles.push(std::thread::spawn(move || {
                let result = (|| -> cntryl_midge::MidgeResult<()> {
                    let mut txn =
                        worker_engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)?;
                    txn.put(
                        b"shared-key".to_vec(),
                        format!("value-{worker_id:02}").into_bytes(),
                        None,
                    )?;
                    worker_barrier.wait();
                    txn.commit(buffered_write_options(mode))
                })();

                let _ = worker_result_tx.send((worker_id, result));
            }));
        }
        drop(result_tx);

        // Act
        barrier.wait();

        // Assert
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut seen = vec![false; worker_count];
        for _ in 0..worker_count {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| Duration::from_secs(0));
            let (worker_id, result) = result_rx.recv_timeout(remaining).unwrap_or_else(|error| {
                panic!("timed out waiting for concurrent commit result: {error:?}");
            });
            assert!(worker_id < worker_count, "invalid worker id {worker_id}");
            assert!(
                !std::mem::replace(&mut seen[worker_id], true),
                "duplicate result from worker {worker_id}"
            );
            result.unwrap_or_else(|error| panic!("worker {worker_id} commit failed: {error:?}"));
        }

        for handle in handles {
            handle
                .join()
                .expect("worker should not panic after reporting result");
        }
        assert!(
            seen.into_iter().all(|received| received),
            "every worker should report exactly one result"
        );

        let read_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        let value = read_tx
            .get(b"shared-key")
            .expect("read shared key")
            .expect("shared key should exist");
        let final_value = std::str::from_utf8(value.as_ref()).expect("final value should be utf8");
        assert!(
            final_value.starts_with("value-"),
            "final value should be one of the committed worker values, got {final_value}"
        );
    }

    #[test]
    fn should_preserve_first_commit_given_write_conflict_when_second_aborts() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
            txn1.commit(buffered_write_options(mode)).unwrap();

            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn2.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();
            drop(txn2); // Rollback

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx.get(b"key").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"value1")));
        });
    }

    #[test]
    fn should_allow_concurrent_delete_put_operations_given_lww_semantics() {
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
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();

            txn1.delete(b"key".to_vec()).unwrap();
            txn2.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();

            txn1.commit(buffered_write_options(mode)).unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            // Assert - last operation wins
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx.get(b"key").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"value")));
        });
    }

    #[test]
    fn should_allow_overlapping_put_after_delete_range_given_lww_semantics() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let mut setup_tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            setup_tx
                .put(b"key2".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let mut txn2 = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();

            txn2.put(b"key2".to_vec(), b"newvalue".to_vec(), None)
                .unwrap();

            let mut delete_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"key1".to_vec(), b"key3".to_vec())
                .unwrap();
            delete_tx.commit(buffered_write_options(mode)).unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx.get(b"key2").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"newvalue")));
        });
    }

    #[test]
    fn should_allow_put_then_delete_range_given_lww_semantics() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();

            txn1.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();

            txn1.commit(buffered_write_options(mode)).unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.delete_range(b"key".to_vec(), b"keyz".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx.get(b"key").unwrap();
            assert_eq!(value, None);
        });
    }

    #[test]
    fn should_allow_concurrent_delete_ranges_given_lww_semantics() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let mut setup_tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            setup_tx
                .put(b"key2".to_vec(), b"value2".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.delete_range(b"key1".to_vec(), b"key3".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.delete_range(b"key1".to_vec(), b"key3".to_vec()).unwrap();
            tx.commit(buffered_write_options(mode)).unwrap();

            // Assert - both succeed
            let read_tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert!(read_tx.get(b"key1").unwrap().is_none());
        });
    }

    #[test]
    fn should_allow_delete_range_delete_operations_given_lww_semantics() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"key".to_vec(), b"value".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();

            txn2.delete(b"key".to_vec()).unwrap();

            let mut delete_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            delete_tx
                .delete_range(b"key1".to_vec(), b"key3".to_vec())
                .unwrap();
            delete_tx.commit(buffered_write_options(mode)).unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert!(read_tx.get(b"key").unwrap().is_none());
        });
    }

    // ============================================================================
    // INSERT CONFLICT TESTS
    // ============================================================================

    #[test]
    fn should_overwrite_existing_value_given_put_on_existing_key_when_committed() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"key".to_vec(), b"existing".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Act - transaction attempts put on existing key
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key".to_vec(), b"newvalue".to_vec(), None)
                .unwrap();
            let result = txn.commit(buffered_write_options(mode));

            // Assert - put succeeds (LWW semantics, not insert semantics)
            assert!(result.is_ok());
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx.get(b"key").unwrap();
            assert_eq!(value, Some(Bytes::from_static(b"newvalue")));
        });
    }

    // ============================================================================
    // LOST UPDATE TESTS
    // ============================================================================

    #[test]
    fn should_allow_lost_update_given_put_read_modify_write_when_concurrent() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"counter".to_vec(), b"0".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Act - simulate lost update with LWW semantics
            let read_tx1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let _val1 = read_tx1.get(b"counter").unwrap().unwrap();
            let read_tx2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let _val2 = read_tx2.get(b"counter").unwrap().unwrap();

            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn1.put(b"counter".to_vec(), b"1".to_vec(), None).unwrap();
            txn1.commit(buffered_write_options(mode)).unwrap();

            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn2.put(b"counter".to_vec(), b"1".to_vec(), None).unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            // Assert - lost update allowed with LWW
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let final_value = read_tx.get(b"counter").unwrap();
            assert_eq!(final_value, Some(Bytes::from_static(b"1")));
        });
    }

    #[test]
    fn should_detect_lost_update_given_cas_pattern_when_value_changed() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"counter".to_vec(), b"0".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Act - compare-and-swap pattern: read the counter, then commit a write
            // guarded by an assertion that the value has not changed since the read.
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let original = read_tx.get(b"counter").unwrap().unwrap();
            assert_eq!(original, Bytes::from_static(b"0"));

            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"counter".to_vec(), b"1".to_vec(), None).unwrap();
            txn.assert_value(b"counter".to_vec(), Some(original.to_vec()))
                .expect("register CAS assertion");

            // Concurrent transaction modifies the counter before the CAS transaction commits.
            let mut txn_concurrent = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn_concurrent
                .put(b"counter".to_vec(), b"2".to_vec(), None)
                .unwrap();
            txn_concurrent.commit(buffered_write_options(mode)).unwrap();

            // The stale CAS transaction now tries to commit its read-modify-write.
            let result = txn.commit(buffered_write_options(mode));

            // Assert - the CAS assertion detects that the value changed underneath it
            // and rejects the commit as a write conflict, so the update is not lost.
            assert!(
                matches!(result, Err(cntryl_midge::MidgeError::WriteConflict(_))),
                "expected stale CAS commit to be rejected in mode {mode}, got: {result:?}"
            );

            // The winning value is the concurrent transaction's write, not the stale
            // transaction's "1" - the lost update was successfully prevented.
            let read_tx2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let value = read_tx2.get(b"counter").unwrap();
            assert_eq!(
                value,
                Some(Bytes::from_static(b"2")),
                "concurrent writer's value must win when the CAS commit is rejected in mode {mode}"
            );
        });
    }

    // ============================================================================
    // NON-CONFLICTING TRANSACTION TESTS
    // ============================================================================

    #[test]
    fn should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();

            txn1.put(b"key1".to_vec(), b"value1".to_vec(), None)
                .unwrap();
            txn2.put(b"key2".to_vec(), b"value2".to_vec(), None)
                .unwrap();

            txn1.commit(buffered_write_options(mode)).unwrap();
            txn2.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key1").unwrap(),
                Some(Bytes::from_static(b"value1"))
            );
            assert_eq!(
                read_tx.get(b"key2").unwrap(),
                Some(Bytes::from_static(b"value2"))
            );
        });
    }

    #[test]
    fn should_commit_transaction_given_no_conflicts() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
            let result = txn.commit(buffered_write_options(mode));

            // Assert
            assert!(result.is_ok());
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key").unwrap(),
                Some(Bytes::from_static(b"value"))
            );
        });
    }

    #[test]
    fn should_commit_transaction_given_concurrent_modifications_to_different_keys() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let engine1 = Arc::clone(&engine);
            let engine2 = Arc::clone(&engine);

            // Act
            let write_options = buffered_write_options(mode);
            let handle1 = std::thread::spawn(move || {
                let mut txn = engine1
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn.put(b"key1".to_vec(), b"value1".to_vec(), None).unwrap();
                txn.commit(write_options)
            });

            let write_options = buffered_write_options(mode);
            let handle2 = std::thread::spawn(move || {
                let mut txn = engine2
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn.put(b"key2".to_vec(), b"value2".to_vec(), None).unwrap();
                txn.commit(write_options)
            });

            // Assert
            assert!(handle1.join().unwrap().is_ok());
            assert!(handle2.join().unwrap().is_ok());
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"key1").unwrap(),
                Some(Bytes::from_static(b"value1"))
            );
            assert_eq!(
                read_tx.get(b"key2").unwrap(),
                Some(Bytes::from_static(b"value2"))
            );
        });
    }

    #[test]
    fn should_read_values_within_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"key".to_vec(), b"value".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Act
            let txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let value = txn.get(b"key").unwrap();

            // Assert - should read committed value at transaction start
            assert_eq!(value, Some(Bytes::from_static(b"value")));
        });
    }

    #[test]
    fn should_commit_new_key_given_clean_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            txn.put(b"newkey".to_vec(), b"newvalue".to_vec(), None)
                .unwrap();
            txn.commit(buffered_write_options(mode)).unwrap();

            // Assert
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert_eq!(
                read_tx.get(b"newkey").unwrap(),
                Some(Bytes::from_static(b"newvalue"))
            );
        });
    }

    // ============================================================================
    // STRESS TESTS
    // ============================================================================

    #[test]
    fn should_allow_concurrent_writes_to_different_keys() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let mut handles = vec![];

            // Act - spawn 10 threads writing different keys
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
                handles.push(handle);
            }

            // Assert - all commits succeed
            for handle in handles {
                assert!(handle.join().unwrap().is_ok());
            }

            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            for i in 0..10 {
                let key = format!("key{i}");
                let expected = format!("value{i}");
                assert_eq!(
                    read_tx.get(key.as_bytes()).unwrap(),
                    Some(Bytes::from(expected.as_bytes().to_vec()))
                );
            }
        });
    }

    #[test]
    fn should_handle_high_contention_writes_without_panic() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let mut handles = vec![];

            // Act - multiple threads writing to same key
            for i in 0..8 {
                let engine_clone = Arc::clone(&engine);
                let write_options = buffered_write_options(mode);
                let handle = std::thread::spawn(move || {
                    let mut txn = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .unwrap();
                    let value = format!("value{i}");
                    txn.put(b"hotkey".to_vec(), value.as_bytes().to_vec(), None)
                        .unwrap();
                    txn.commit(write_options)
                });
                handles.push(handle);
            }

            // Assert - all commits succeed (LWW semantics)
            for handle in handles {
                assert!(handle.join().unwrap().is_ok());
            }

            // One of the values should win
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert!(read_tx.get(b"hotkey").unwrap().is_some());
        });
    }

    #[test]
    fn should_handle_concurrent_read_modify_writes_without_panic() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"counter".to_vec(), b"0".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();
            let mut handles = vec![];

            // Act - 10 threads incrementing counter
            for i in 0..10 {
                let engine_clone = Arc::clone(&engine);
                let write_options = buffered_write_options(mode);
                let handle = std::thread::spawn(move || {
                    let read_tx = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .unwrap();
                    let _value = read_tx.get(b"counter").unwrap();
                    let mut txn = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .unwrap();
                    let new_value = format!("{i}");
                    txn.put(b"counter".to_vec(), new_value.as_bytes().to_vec(), None)
                        .unwrap();
                    txn.commit(write_options)
                });
                handles.push(handle);
            }

            // Assert - all commits succeed
            for handle in handles {
                assert!(handle.join().unwrap().is_ok());
            }
        });
    }

    #[test]
    fn should_handle_high_concurrency_optimistic_locking() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let barrier = Arc::new(std::sync::Barrier::new(50));
            let mut handles = vec![];

            // Act - 50 threads performing optimistic lock pattern (read then write)
            for i in 0..50 {
                let engine_clone = Arc::clone(&engine);
                let barrier_clone = Arc::clone(&barrier);
                let write_options = buffered_write_options(mode);
                let handle = std::thread::spawn(move || {
                    // Wait for all threads to be ready before starting
                    barrier_clone.wait();

                    // Optimistic lock pattern: read first
                    let read_tx = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .unwrap();
                    let _current = read_tx.get(b"value").unwrap();

                    // Then write in transaction
                    let mut txn = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .unwrap();
                    let write_val = format!("{i}");
                    txn.put(b"value".to_vec(), write_val.as_bytes().to_vec(), None)
                        .unwrap();
                    txn.commit(write_options)
                });
                handles.push(handle);
            }

            // Assert - all transactions succeed
            for handle in handles {
                assert!(handle.join().unwrap().is_ok());
            }

            // Final value should be one of the writes
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            assert!(read_tx.get(b"value").unwrap().is_some());
        });
    }

    #[test]
    fn should_maintain_transaction_isolation_under_stress() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let cf_id = cf.id();
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"isolated".to_vec(), b"initial".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Take a read-only snapshot before any concurrent stress writers start.
            let snapshot_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();

            // Act - many threads hammering the same key with concurrent read-modify-writes
            // while the snapshot transaction above stays open.
            let mut handles = vec![];
            for i in 0..20 {
                let engine_clone = Arc::clone(&engine);
                let write_options = buffered_write_options(mode);
                let handle = std::thread::spawn(move || {
                    let read_tx = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .unwrap();
                    let _current = read_tx.get(b"isolated").unwrap();
                    let mut txn = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .unwrap();
                    let value = format!("stress{i}");
                    txn.put(b"isolated".to_vec(), value.as_bytes().to_vec(), None)
                        .unwrap();
                    txn.commit(write_options)
                });
                handles.push(handle);
            }

            // Assert - all concurrent commits succeed without panicking
            for handle in handles {
                assert!(handle.join().unwrap().is_ok());
            }

            // The long-lived snapshot must still observe the pre-stress value: isolation
            // means none of the concurrent stress commits are visible to it.
            assert_eq!(
                snapshot_tx.get(b"isolated").unwrap(),
                Some(Bytes::from_static(b"initial")),
                "snapshot transaction leaked a concurrent stress write in mode {mode}"
            );

            // The committed state, on the other hand, must reflect one of the stress
            // writes - proving the stress writers actually raced and mutated the key.
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let final_value = read_tx.get(b"isolated").unwrap();
            assert_ne!(
                final_value,
                Some(Bytes::from_static(b"initial")),
                "expected concurrent stress writers to update the key in mode {mode}"
            );
            assert!(final_value.is_some());
        });
    }

    // ============================================================================
    // RECOVERY TESTS (FS + CLOUD ONLY)
    // ============================================================================

    #[test]
    fn should_recover_conflict_state_after_engine_restart() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            // Act (Phase 1) - create conflicts and commit
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Create conflicting transactions where last-write wins
                let mut txn1 = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn1.put(b"conflict_key".to_vec(), b"value1".to_vec(), None)
                    .unwrap();
                txn1.commit(buffered_write_options(mode)).unwrap();

                let mut txn2 = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn2.put(b"conflict_key".to_vec(), b"value2".to_vec(), None)
                    .unwrap();
                txn2.commit(buffered_write_options(mode)).unwrap();
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before restart");
            }

            // Assert (Phase 2) - restart and verify
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.get_column_family("test").expect("get cf");

                // Assert - last written value persists
                let read_tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .unwrap();
                let value = read_tx.get(b"conflict_key").unwrap();
                assert_eq!(value, Some(Bytes::from_static(b"value2")));
            }
        });
    }

    #[test]
    fn should_persist_lost_update_prevention_after_restart() {
        for_each_storage_mode(&["local", "cloud"], |mode, opts| {
            // Arrange
            // Act (Phase 1) - set up concurrent updates
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                // Initial value
                let mut setup_tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                setup_tx
                    .put(b"counter".to_vec(), b"0".to_vec(), None)
                    .unwrap();
                setup_tx.commit(buffered_write_options(mode)).unwrap();

                // Two transactions attempt concurrent increment
                let mut txn1 = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn1.put(b"counter".to_vec(), b"1".to_vec(), None).unwrap();
                txn1.commit(buffered_write_options(mode)).unwrap();

                let mut txn2 = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                txn2.put(b"counter".to_vec(), b"2".to_vec(), None).unwrap();
                txn2.commit(buffered_write_options(mode)).unwrap();
                engine
                    .shutdown(Duration::from_secs(5))
                    .expect("shutdown before restart");
            }

            // Assert (Phase 2) - restart and verify
            {
                let engine = open_with_mode(&opts, mode);
                let cf = engine.get_column_family("test").expect("get cf");

                // Assert - last written value (2) persists
                let read_tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .unwrap();
                let value = read_tx.get(b"counter").unwrap();
                assert_eq!(value, Some(Bytes::from_static(b"2")));
            }
        });
    }
    // ============================================================================
    // BASELINE CONFLICT PREVENTION (Negative Tests)
    // ============================================================================

    #[test]
    fn should_preserve_both_writes_when_non_overlapping_keys_given_concurrent_commits() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Verify that non-conflicting concurrent writes are both visible
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Pre-populate
            let mut setup_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            setup_tx
                .put(b"key1".to_vec(), b"old1".to_vec(), None)
                .unwrap();
            setup_tx
                .put(b"key2".to_vec(), b"old2".to_vec(), None)
                .unwrap();
            setup_tx.commit(buffered_write_options(mode)).unwrap();

            // Act: Two concurrent updates to different keys
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();

            txn1.put(b"key1".to_vec(), b"new1".to_vec(), None).unwrap();
            txn2.put(b"key2".to_vec(), b"new2".to_vec(), None).unwrap();

            txn1.commit(buffered_write_options(mode))
                .expect("commit first disjoint update");
            txn2.commit(buffered_write_options(mode))
                .expect("commit second disjoint update");

            // Assert: Both updates must be visible
            let read_tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .unwrap();
            let v1 = read_tx.get(b"key1").unwrap();
            let v2 = read_tx.get(b"key2").unwrap();

            assert_eq!(
                v1,
                Some(Bytes::from_static(b"new1")),
                "key1 update lost in {mode}"
            );
            assert_eq!(
                v2,
                Some(Bytes::from_static(b"new2")),
                "key2 update lost in {mode}"
            );
        });
    }
}

mod transaction_isolation {
    //! Copyright (c) 2025 Cntryl, Inc.
    //! SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

    // Transaction visibility and last-write-wins behavior tests.
    //
    // These tests cover the currently implemented guarantees: hidden uncommitted
    // writes, read-your-own-writes, read-only snapshot behavior, and LWW commit
    // outcomes. They do not claim serializable, phantom-free, or full snapshot
    // isolation for read-write transactions.

    use crate::common::*;
    use bytes::Bytes;
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
}

mod transaction_isolation_lww {
    //! Cross-mode verification of the documented last-write-wins transaction model.

    use crate::common::*;
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn should_hide_uncommitted_writes_given_uncommitted_write_when_read_different_mode() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut writer = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin writer");
            writer
                .put(b"key".to_vec(), b"uncommitted".to_vec(), None)
                .expect("put uncommitted value");

            let reader = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin reader");

            // Assert
            assert_eq!(
                reader.get(b"key").expect("read uncommitted key"),
                None,
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_apply_last_committed_write_given_multiple_commits_when_last_write_wins() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin txn1");
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin txn2");

            txn1.put(b"key".to_vec(), b"from_txn1".to_vec(), None)
                .expect("put txn1 value");
            txn2.put(b"key".to_vec(), b"from_txn2".to_vec(), None)
                .expect("put txn2 value");

            txn1.commit(buffered_write_options(mode))
                .expect("commit txn1");
            txn2.commit(buffered_write_options(mode))
                .expect("commit txn2");

            let reader = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin reader");

            // Assert
            assert_eq!(
                reader.get(b"key").expect("read final key"),
                Some(Bytes::from_static(b"from_txn2")),
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_allow_lost_update_given_concurrent_writes_when_lost_update_occurs() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut setup = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin setup");
            setup
                .put(b"counter".to_vec(), b"0".to_vec(), None)
                .expect("put initial counter");
            setup
                .commit(buffered_write_options(mode))
                .expect("commit setup");

            // Act
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin txn1");
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin txn2");

            txn1.get(b"counter")
                .expect("txn1 read counter before increment");
            txn2.get(b"counter")
                .expect("txn2 read counter before increment");

            txn1.put(b"counter".to_vec(), b"1".to_vec(), None)
                .expect("txn1 write increment");
            txn2.put(b"counter".to_vec(), b"1".to_vec(), None)
                .expect("txn2 write increment");

            txn1.commit(buffered_write_options(mode))
                .expect("commit txn1");
            txn2.commit(buffered_write_options(mode))
                .expect("commit txn2");

            let reader = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin reader");

            // Assert
            assert_eq!(
                reader.get(b"counter").expect("read final counter"),
                Some(Bytes::from_static(b"1")),
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_allow_disjoint_writes_after_shared_read_when_transactions_both_commit() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut setup = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin setup");
            setup
                .put(b"shared".to_vec(), b"base_value".to_vec(), None)
                .expect("put shared value");
            setup
                .put(b"flag1".to_vec(), b"false".to_vec(), None)
                .expect("put flag1");
            setup
                .put(b"flag2".to_vec(), b"false".to_vec(), None)
                .expect("put flag2");
            setup
                .commit(buffered_write_options(mode))
                .expect("commit setup");

            // Act
            let mut txn1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin txn1");
            let mut txn2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin txn2");

            assert_eq!(
                txn1.get(b"shared").expect("txn1 read shared"),
                Some(Bytes::from_static(b"base_value")),
                "mode: {mode}"
            );
            assert_eq!(
                txn2.get(b"shared").expect("txn2 read shared"),
                Some(Bytes::from_static(b"base_value")),
                "mode: {mode}"
            );

            txn1.put(b"flag1".to_vec(), b"true".to_vec(), None)
                .expect("txn1 write flag1");
            txn2.put(b"flag2".to_vec(), b"true".to_vec(), None)
                .expect("txn2 write flag2");

            txn1.commit(buffered_write_options(mode))
                .expect("commit txn1");
            txn2.commit(buffered_write_options(mode))
                .expect("commit txn2");

            let reader = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin reader");

            // Assert
            assert_eq!(
                reader.get(b"flag1").expect("read flag1 after commits"),
                Some(Bytes::from_static(b"true")),
                "mode: {mode}"
            );
            assert_eq!(
                reader.get(b"flag2").expect("read flag2 after commits"),
                Some(Bytes::from_static(b"true")),
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_abort_second_commit_given_conflicting_writes_when_abort_on_write_conflict_enabled() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx1");
            let mut tx2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx2");

            tx1.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);
            tx2.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);

            tx1.put(b"key".to_vec(), b"from_tx1".to_vec(), None)
                .expect("put tx1 value");
            tx2.put(b"key".to_vec(), b"from_tx2".to_vec(), None)
                .expect("put tx2 value");

            // Act
            tx1.commit(buffered_write_options(mode))
                .expect("commit tx1");
            let second_commit = tx2.commit(buffered_write_options(mode));

            // Assert
            assert!(
                matches!(
                    second_commit,
                    Err(cntryl_midge::MidgeError::WriteConflict(_))
                ),
                "mode: {mode}"
            );

            let reader = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin reader");
            assert_eq!(
                reader.get(b"key").expect("read final key"),
                Some(Bytes::from_static(b"from_tx1")),
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_abort_delete_range_commit_given_overlapping_recent_writes_when_abort_on_write_conflict_enabled(
    ) {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx1");
            let mut tx2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx2");

            tx1.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);
            tx2.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);

            tx1.put(b"m".to_vec(), b"v1".to_vec(), None)
                .expect("put tx1 value");
            tx2.delete_range(b"a".to_vec(), b"z".to_vec())
                .expect("tx2 delete range");

            // Act
            tx1.commit(buffered_write_options(mode))
                .expect("commit tx1");
            let second_commit = tx2.commit(buffered_write_options(mode));

            // Assert
            assert!(
                matches!(
                    second_commit,
                    Err(cntryl_midge::MidgeError::WriteConflict(_))
                ),
                "mode: {mode}"
            );

            let reader = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin reader");
            assert_eq!(
                reader.get(b"m").expect("read final key"),
                Some(Bytes::from_static(b"v1")),
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_abort_point_write_commit_given_recent_overlapping_delete_range_when_abort_on_write_conflict_enabled(
    ) {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut tx1 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx1");
            let mut tx2 = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx2");

            tx1.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);
            tx2.set_conflict_policy(cntryl_midge::ConflictPolicy::AbortOnWriteConflict);

            tx1.delete_range(b"a".to_vec(), b"z".to_vec())
                .expect("tx1 delete range");
            tx2.put(b"m".to_vec(), b"v2".to_vec(), None)
                .expect("put tx2 value");

            // Act
            tx1.commit(buffered_write_options(mode))
                .expect("commit tx1");
            let second_commit = tx2.commit(buffered_write_options(mode));

            // Assert
            assert!(
                matches!(
                    second_commit,
                    Err(cntryl_midge::MidgeError::WriteConflict(_))
                ),
                "mode: {mode} (empty-range overlap must conflict in strict mode)"
            );
        });
    }
}

mod transaction_semantics_hardening {
    //! Regression coverage for transaction visibility and lifecycle boundaries.

    use bytes::Bytes;
    use cntryl_midge::{MidgeError, Query, TransactionMode, WriteOptions};

    use crate::common::{open_with_mode, opts_for_mode};

    fn collect_scan_and_assert_exhausted(
        mut scan: cntryl_midge::ScanIterator<'_>,
    ) -> Vec<(Bytes, Bytes)> {
        let mut rows = Vec::new();
        for row in scan.by_ref() {
            rows.push(row.expect("scan row"));
        }
        assert!(scan.exhausted());
        assert!(!scan.failed());
        assert!(
            scan.next().is_none(),
            "exhausted iterator must stay exhausted"
        );
        rows
    }

    #[test]
    fn should_reject_insert_when_key_exists_only_in_sst() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("sst").expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"sst-only".to_vec(), b"value".to_vec(), None)
            .expect("put seed value");
        seed.commit(WriteOptions::buffered()).expect("commit seed");
        engine.flush_cf(&cf).expect("flush seed");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin insert transaction");
        tx.insert(b"sst-only".to_vec(), b"replacement".to_vec(), None)
            .expect("queue insert");
        let result = tx.commit(WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message)) if message.contains("already exists")
        ));
    }

    #[test]
    fn should_apply_last_intent_given_duplicate_operations_on_same_key_when_committing() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("intents").expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"existing".to_vec(), b"old".to_vec(), None)
            .expect("put seed value");
        seed.commit(WriteOptions::sync()).expect("commit seed");

        // Act
        let mut duplicate = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin duplicate transaction");
        duplicate
            .put(b"new".to_vec(), b"first".to_vec(), None)
            .expect("queue first value");
        duplicate
            .put(b"new".to_vec(), b"second".to_vec(), None)
            .expect("queue replacing value");
        duplicate
            .commit(WriteOptions::best_effort())
            .expect("last duplicate put must commit");

        let mut replace = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin replace transaction");
        replace.delete(b"existing".to_vec()).expect("queue delete");
        replace
            .insert(b"existing".to_vec(), b"new".to_vec(), None)
            .expect("queue replacement insert");
        replace
            .commit(WriteOptions::best_effort())
            .expect("delete then insert should commit");

        // Assert
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");
        assert_eq!(
            read.get(b"new")
                .expect("read last duplicate intent")
                .as_deref(),
            Some(&b"second"[..])
        );
        assert_eq!(
            read.get(b"existing").expect("read replacement").as_deref(),
            Some(&b"new"[..])
        );
    }

    #[test]
    fn should_honor_prefix_upper_bound_given_prefix_ending_in_ff_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("binary").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin transaction");
        tx.put(vec![0x10, 0xff], b"prefix".to_vec(), None)
            .expect("put prefix");
        tx.put(vec![0x10, 0xff, 0x00], b"child".to_vec(), None)
            .expect("put child");
        tx.put(vec![0x10, 0xfe], b"sibling".to_vec(), None)
            .expect("put sibling");
        tx.commit(WriteOptions::best_effort())
            .expect("commit values");

        // Act
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");
        let query = Query::new().prefix(Bytes::from(vec![0x10, 0xff]));
        let results = collect_scan_and_assert_exhausted(read.scan(&query).expect("scan prefix"));

        // Assert
        assert_eq!(
            results,
            vec![
                (Bytes::from(vec![0x10, 0xff]), Bytes::from_static(b"prefix")),
                (
                    Bytes::from(vec![0x10, 0xff, 0x00]),
                    Bytes::from_static(b"child"),
                ),
            ]
        );
    }

    #[test]
    fn should_return_point_put_given_delete_range_then_put_when_committing() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("memory"), "memory");
        let cf = engine.create_column_family("range-put").expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"middle".to_vec(), b"old".to_vec(), None)
            .expect("seed middle");
        seed.put(b"other".to_vec(), b"removed".to_vec(), None)
            .expect("seed other");
        seed.commit(WriteOptions::sync()).expect("commit seed");

        // Act
        let mut replace = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin replacement transaction");
        replace
            .delete_range(b"a".to_vec(), b"z".to_vec())
            .expect("delete range");
        replace
            .put(b"middle".to_vec(), b"new".to_vec(), None)
            .expect("put after range delete");
        replace
            .commit(WriteOptions::best_effort())
            .expect("commit range and point intents");

        // Assert
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");
        assert_eq!(
            read.get(b"middle").expect("read middle").as_deref(),
            Some(&b"new"[..])
        );
        assert_eq!(read.get(b"other").expect("read other"), None);
    }

    #[test]
    fn should_intersect_explicit_end_with_prefix_bound_given_overlapping_bounds_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine
            .create_column_family("prefix-end")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        for (key, value) in [
            (b"other".as_slice(), b"outside".as_slice()),
            (b"pre:a".as_slice(), b"a".as_slice()),
            (b"pre:b".as_slice(), b"b".as_slice()),
            (b"pre:c".as_slice(), b"c".as_slice()),
        ] {
            seed.put(key.to_vec(), value.to_vec(), None)
                .expect("seed row");
        }
        seed.commit(WriteOptions::best_effort())
            .expect("commit rows");

        // Act
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");
        let rows = collect_scan_and_assert_exhausted(
            read.scan(
                &Query::new()
                    .prefix(Bytes::from_static(b"pre:"))
                    .start_key(Bytes::from_static(b"pre:a"))
                    .end_key(Bytes::from_static(b"pre:c")),
            )
            .expect("scan intersected bounds"),
        );

        // Assert
        assert_eq!(
            rows,
            vec![
                (Bytes::from_static(b"pre:a"), Bytes::from_static(b"a")),
                (Bytes::from_static(b"pre:b"), Bytes::from_static(b"b")),
            ]
        );
    }

    #[test]
    fn should_return_each_key_once_given_same_key_in_snapshot_and_write_set_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine
            .create_column_family("scan-override")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"a".to_vec(), b"old".to_vec(), None)
            .expect("seed a");
        seed.put(b"b".to_vec(), b"stable".to_vec(), None)
            .expect("seed b");
        seed.commit(WriteOptions::sync()).expect("commit seed");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin scanning transaction");
        tx.put(b"a".to_vec(), b"new".to_vec(), None)
            .expect("override a");

        // Act
        let rows =
            collect_scan_and_assert_exhausted(tx.scan(&Query::new()).expect("scan merged rows"));

        // Assert
        assert_eq!(
            rows,
            vec![
                (Bytes::from_static(b"a"), Bytes::from_static(b"new")),
                (Bytes::from_static(b"b"), Bytes::from_static(b"stable")),
            ]
        );
    }

    #[test]
    fn should_return_deleted_key_absent_given_delete_intent_over_snapshot_value_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine
            .create_column_family("scan-delete")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"a".to_vec(), b"removed".to_vec(), None)
            .expect("seed a");
        seed.put(b"b".to_vec(), b"kept".to_vec(), None)
            .expect("seed b");
        seed.commit(WriteOptions::sync()).expect("commit seed");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin scanning transaction");
        tx.delete(b"a".to_vec()).expect("delete a");

        // Act
        let rows =
            collect_scan_and_assert_exhausted(tx.scan(&Query::new()).expect("scan merged rows"));

        // Assert
        assert_eq!(
            rows,
            vec![(Bytes::from_static(b"b"), Bytes::from_static(b"kept"))]
        );
    }

    #[test]
    fn should_preserve_scan_limit_given_deleted_or_filtered_intents_when_scanning() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine
            .create_column_family("scan-limit")
            .expect("create cf");
        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        for key in [b"scan:a", b"scan:b", b"scan:c"] {
            seed.put(key.to_vec(), key.to_vec(), None)
                .expect("seed scan row");
        }
        seed.commit(WriteOptions::sync()).expect("commit seed");
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin scanning transaction");
        tx.delete(b"scan:a".to_vec()).expect("delete first row");
        tx.put(b"outside".to_vec(), b"filtered".to_vec(), None)
            .expect("put filtered intent");

        // Act
        let rows = collect_scan_and_assert_exhausted(
            tx.scan(&Query::new().prefix(Bytes::from_static(b"scan:")).limit(2))
                .expect("scan with filtered intents"),
        );

        // Assert
        assert_eq!(
            rows,
            vec![
                (Bytes::from_static(b"scan:b"), Bytes::from_static(b"scan:b")),
                (Bytes::from_static(b"scan:c"), Bytes::from_static(b"scan:c")),
            ]
        );
    }

    #[test]
    fn should_reject_write_given_dropped_column_family_when_committing() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("stale").expect("create cf");
        let cf_id = cf.id();
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin transaction");
        tx.put(b"must-not-commit".to_vec(), b"value".to_vec(), None)
            .expect("queue write");
        engine.drop_column_family(cf_id).expect("drop cf");

        // Act
        let result = tx.commit(WriteOptions::buffered());

        // Assert
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message))
                if message.contains("column family") && message.contains("does not exist")
        ));
        assert!(matches!(
            engine.begin_tx(cf_id, TransactionMode::ReadOnly),
            Err(MidgeError::InvalidArgument(_))
        ));
    }

    #[test]
    fn should_reject_assertion_only_commit_given_dropped_column_family() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine
            .create_column_family("stale-assertion")
            .expect("create cf");
        let cf_id = cf.id();
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin transaction");
        tx.assert_value(b"must-still-exist".to_vec(), None)
            .expect("register assertion");
        engine.drop_column_family(cf_id).expect("drop cf");

        // Act
        let result = tx.commit(WriteOptions::buffered());

        // Assert: an assertion-only commit is rejected the same way a write commit
        // is, even though it is validated on a distinct path that never touches
        // the write batch (see `should_reject_write_given_dropped_column_family_when_committing`
        // for the write-carrying counterpart).
        assert!(matches!(
            result,
            Err(MidgeError::InvalidArgument(message))
                if message.contains("column family") && message.contains("does not exist")
        ));
        assert!(matches!(
            engine.begin_tx(cf_id, TransactionMode::ReadOnly),
            Err(MidgeError::InvalidArgument(_))
        ));
    }

    #[test]
    fn should_use_transaction_snapshot_time_for_ttl_visibility() {
        // Arrange
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine.create_column_family("ttl").expect("create cf");
        let mut writer = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin writer");
        writer
            .put(b"ttl-key".to_vec(), b"value".to_vec(), Some(1))
            .expect("put ttl value");
        writer
            .commit(WriteOptions::buffered())
            .expect("commit ttl value");
        let snapshot = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin snapshot");

        // Act
        std::thread::sleep(std::time::Duration::from_millis(1_100));

        // Assert: the old snapshot remains stable while a new snapshot observes expiry.
        assert_eq!(
            snapshot.get(b"ttl-key").expect("snapshot read").as_deref(),
            Some(&b"value"[..])
        );
        drop(snapshot);
        let current = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin current read");
        assert_eq!(current.get(b"ttl-key").expect("current read"), None);
    }

    #[test]
    fn should_persist_range_tombstone_when_flush_survives_restart() {
        // Arrange
        let opts = opts_for_mode("local");
        {
            let mut engine = open_with_mode(&opts, "local");
            let cf = engine.create_column_family("ranges").expect("create cf");
            let mut writer = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin writer");
            for key in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
                writer
                    .put(key.to_vec(), key.to_vec(), None)
                    .expect("put value");
            }
            writer
                .commit(WriteOptions::buffered())
                .expect("commit values");
            engine.flush_cf(&cf).expect("flush values");

            let mut deleter = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin delete transaction");
            deleter
                .delete_range(b"b".to_vec(), b"d".to_vec())
                .expect("delete range");
            deleter
                .commit(WriteOptions::buffered())
                .expect("commit range tombstone");
            engine.flush_cf(&cf).expect("flush range tombstone");
            engine
                .shutdown(std::time::Duration::from_secs(2))
                .expect("shutdown before immediate reopen");
        }

        // Act
        let engine = open_with_mode(&opts, "local");
        let cf = engine.get_column_family("ranges").expect("reopen cf");
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");

        // Assert
        assert_eq!(read.get(b"a").expect("read a").as_deref(), Some(&b"a"[..]));
        assert_eq!(read.get(b"b").expect("read b"), None);
        assert_eq!(read.get(b"c").expect("read c"), None);
    }

    #[test]
    fn should_apply_flushed_range_tombstone_outside_point_key_bounds() {
        // Arrange: the second flush has a point key above the range tombstone.
        // Its manifest bounds must still include the tombstone's [a, c) interval.
        let engine = open_with_mode(&opts_for_mode("local"), "local");
        let cf = engine
            .create_column_family("range-bounds")
            .expect("create cf");

        let mut seed = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed transaction");
        seed.put(b"b".to_vec(), b"old".to_vec(), None)
            .expect("put seed value");
        seed.commit(WriteOptions::buffered()).expect("commit seed");
        engine.flush_cf(&cf).expect("flush seed");

        let mut delete = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin range transaction");
        delete
            .put(b"m".to_vec(), b"unrelated".to_vec(), None)
            .expect("put unrelated value");
        delete
            .delete_range(b"a".to_vec(), b"c".to_vec())
            .expect("queue range tombstone");
        delete
            .commit(WriteOptions::buffered())
            .expect("commit range tombstone");
        engine.flush_cf(&cf).expect("flush mixed contents");

        // Act
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin read transaction");

        // Assert: `b` is below the point key `m`, but still covered by [a, c).
        assert_eq!(read.get(b"b").expect("read covered key"), None);
    }
}

mod transaction_snapshot_tracking {
    use crate::common::*;
    use cntryl_midge::{MidgeError, MidgeResult, Query, TransactionMode, WriteOptions};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn wait_for_active_snapshots(
        engine: &cntryl_midge::Engine,
        expected: usize,
        timeout: Duration,
    ) -> MidgeResult<()> {
        let deadline = Instant::now() + timeout;

        loop {
            let metrics = engine.get_runtime_metrics()?;
            if metrics.active_snapshots == expected {
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(cntryl_midge::MidgeError::Internal(format!(
                    "timed out waiting for active_snapshots={}, got {}",
                    expected, metrics.active_snapshots
                )));
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn seed_scan_pin_generations(
        engine: &cntryl_midge::Engine,
        cf: &cntryl_midge::ColumnFamilyHandle,
    ) -> Vec<String> {
        for generation in 0..4 {
            let mut write = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin seed generation");
            if generation == 0 {
                for index in 0..24 {
                    write
                        .put(
                            format!("row-{index:03}").into_bytes(),
                            b"baseline".to_vec(),
                            None,
                        )
                        .expect("put baseline row");
                }
            } else {
                write
                    .put(
                        format!("zz-filler-{generation}").into_bytes(),
                        b"baseline".to_vec(),
                        None,
                    )
                    .expect("put filler row");
            }
            write
                .commit(WriteOptions::sync())
                .expect("commit seed generation");
            engine.flush_cf(cf).expect("flush seed generation");
        }
        let layout = engine.get_storage_layout().expect("layout before scan");
        let input_names: Vec<_> = layout
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .filter(|file| file.cf_id == cf.id())
            .map(|file| file.name.clone())
            .collect();
        assert_eq!(input_names.len(), 4, "fixture must expose four L0 inputs");
        input_names
    }

    fn wait_for_sst_files_removed(db_path: &Path, names: &[String], timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while names
            .iter()
            .any(|name| db_path.join("sst").join(name).exists())
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            names
                .iter()
                .all(|name| !db_path.join("sst").join(name).exists()),
            "retired inputs should become reclaimable after the snapshot closes"
        );
    }

    #[test]
    fn should_register_snapshot_when_begin_tx_starts_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for zero active snapshots");

            // Act
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin read-only tx");

            // Assert
            wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
                .expect("wait for one active snapshot");

            drop(tx);
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for zero active snapshots after drop");
        });
    }

    #[test]
    fn should_report_active_snapshot_immediately_when_begin_tx_returns() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin read-only tx");

            // Assert
            let metrics = engine
                .get_runtime_metrics()
                .expect("get runtime metrics immediately after begin_tx");
            assert_eq!(metrics.active_snapshots, 1, "mode: {mode}");

            drop(tx);
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for zero active snapshots after drop");
        });
    }

    #[test]
    fn should_report_snapshot_retention_pressure_metrics_when_snapshot_pins_ssts() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut seed = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin seed tx");
            for i in 0..32 {
                let key = format!("metric_key_{i:03}");
                seed.put(key.into_bytes(), b"v".to_vec(), None)
                    .expect("seed put");
            }
            seed.commit(buffered_write_options(mode))
                .expect("seed commit");
            engine.flush_cf(&cf).expect("seed flush");

            // Act
            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin snapshot tx");

            // Assert
            let metrics = engine
                .get_runtime_metrics()
                .expect("get runtime metrics with active snapshot");
            assert_eq!(metrics.active_snapshots, 1, "mode: {mode}");
            assert!(metrics.pinned_ssts > 0, "mode: {mode}");
            assert!(metrics.oldest_snapshot_age_seconds <= 1, "mode: {mode}");

            drop(snapshot);
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for zero active snapshots after drop");
        });
    }

    #[test]
    fn should_not_register_snapshot_given_dropped_cf_when_begin_tx_fails() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("dropped").expect("create cf");
            let cf_id = cf.id();

            let mut seed = engine
                .begin_tx(cf_id, TransactionMode::ReadWrite)
                .expect("begin seed tx");
            seed.put(b"cached_key".to_vec(), b"cached_value".to_vec(), None)
                .expect("seed put");
            seed.commit(buffered_write_options(mode))
                .expect("seed commit");
            engine.flush_cf(&cf).expect("flush seed data");

            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for zero active snapshots before drop");
            engine.drop_column_family(cf_id).expect("drop cf");

            // Act and assert
            for tx_mode in [TransactionMode::ReadOnly, TransactionMode::ReadWrite] {
                let result = engine.begin_tx(cf_id, tx_mode);
                match result {
                    Err(MidgeError::InvalidArgument(message)) => {
        // Assert
                        assert_eq!(message, format!("column family {cf_id} does not exist"));
                    }
                    Err(error) => panic!(
                        "expected InvalidArgument for dropped CF in {mode} with {tx_mode:?}, got {error}"
                    ),
                    Ok(_) => panic!("expected dropped CF begin_tx to fail in {mode} with {tx_mode:?}"),
                }

                let metrics = engine
                    .get_runtime_metrics()
                    .expect("get metrics after failed begin_tx");
                assert_eq!(
                    metrics.active_snapshots, 0,
                    "mode: {mode}, tx_mode: {tx_mode:?}"
                );
                assert_eq!(metrics.pinned_ssts, 0, "mode: {mode}, tx_mode: {tx_mode:?}");
            }
        });
    }

    #[test]
    fn should_unregister_snapshot_when_commit_finishes_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin read-write tx");
            tx.put(b"k".to_vec(), b"v".to_vec(), None)
                .expect("put value");
            wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
                .expect("wait for one active snapshot before commit");
            tx.commit(buffered_write_options(mode)).expect("commit tx");

            // Assert
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for zero active snapshots after commit");
        });
    }

    #[test]
    fn should_unregister_snapshot_when_rollback_ends_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let tx1 = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin read-only tx");
            wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
                .expect("wait for one active snapshot before rollback");
            tx1.rollback().expect("rollback tx");

            // Assert
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for zero active snapshots after rollback");
        });
    }

    #[test]
    fn should_unregister_snapshot_when_drop_ends_transaction() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let tx2 = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin read-only tx");
            wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
                .expect("wait for one active snapshot before drop");
            drop(tx2);

            // Assert
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for zero active snapshots after drop");
        });
    }

    #[test]
    fn should_preserve_snapshot_value_when_delete_is_compacted_with_snapshot_active() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut seed = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin seed tx");
            seed.put(b"k".to_vec(), b"v1".to_vec(), None)
                .expect("seed put");
            seed.commit(buffered_write_options(mode))
                .expect("seed commit");
            engine.flush_cf(&cf).expect("seed flush");

            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin snapshot tx");
            wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
                .expect("wait for active snapshot");

            let mut deleter = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin delete tx");
            deleter.delete(b"k".to_vec()).expect("delete key");
            deleter
                .commit(buffered_write_options(mode))
                .expect("delete commit");
            engine.flush_cf(&cf).expect("delete flush");
            for index in 0..2 {
                let mut filler = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin filler tx");
                filler
                    .put(
                        format!("filler-{index}").into_bytes(),
                        b"value".to_vec(),
                        None,
                    )
                    .expect("put filler");
                filler
                    .commit(buffered_write_options(mode))
                    .expect("commit filler");
                engine.flush_cf(&cf).expect("flush filler");
            }
            let layout_before = engine
                .get_storage_layout()
                .expect("layout before compaction");
            let l0_before = layout_before
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count);
            assert_eq!(l0_before, 4, "fixture must create four L0 files");

            // Act
            engine.compact_all().expect("compact all");

            // Assert
            let layout_after = engine
                .get_storage_layout()
                .expect("layout after compaction");
            let l0_after = layout_after
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count);
            let l1_after = layout_after
                .levels
                .iter()
                .find(|level| level.level == 1)
                .map_or(0, |level| level.file_count);
            assert!(
                l0_after < l0_before,
                "fixture must compact L0 in mode {mode}"
            );
            assert!(
                l1_after > 0,
                "fixture must publish L1 output in mode {mode}"
            );
            let snapshot_value = snapshot.get(b"k").expect("snapshot get after compaction");
            assert_eq!(
                snapshot_value,
                Some(bytes::Bytes::from_static(b"v1")),
                "mode: {mode}"
            );

            drop(snapshot);
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for no active snapshots");

            let current = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin current read tx");
            assert_eq!(
                current.get(b"k").expect("current get"),
                None,
                "mode: {mode}"
            );
        });
    }

    #[test]
    fn should_preserve_snapshot_range_scan_when_compaction_gc_runs_with_snapshot_active() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            let mut seed = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin seed tx");
            for i in 0..64 {
                let key = format!("gc_key_{i:03}");
                seed.put(key.into_bytes(), b"old".to_vec(), None)
                    .expect("seed put");
            }
            seed.commit(buffered_write_options(mode))
                .expect("seed commit");
            engine.flush_cf(&cf).expect("seed flush");

            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin snapshot tx");
            wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
                .expect("wait for active snapshot");

            for generation in ["new1", "new2"] {
                let mut overwrite = engine
                    .begin_tx(cf.id(), TransactionMode::ReadWrite)
                    .expect("begin overwrite tx");
                for i in 0..64 {
                    let key = format!("gc_key_{i:03}");
                    overwrite
                        .put(key.into_bytes(), generation.as_bytes().to_vec(), None)
                        .expect("overwrite put");
                }
                overwrite
                    .commit(buffered_write_options(mode))
                    .expect("overwrite commit");
                engine.flush_cf(&cf).expect("overwrite flush");
                engine.compact_all().expect("compact all");
            }

            // Act
            let rows = snapshot
                .scan(&Query::new())
                .expect("snapshot scan")
                .try_collect()
                .expect("collect snapshot scan");

            // Assert
            assert_eq!(rows.len(), 64, "mode: {mode}");
            for (_key, value) in rows {
                assert_eq!(value, bytes::Bytes::from_static(b"old"), "mode: {mode}");
            }

            let current = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin current read tx");
            assert_eq!(
                current.get(b"gc_key_000").expect("current get"),
                Some(bytes::Bytes::from_static(b"new2")),
                "mode: {mode}"
            );
            drop(current);

            drop(snapshot);
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for no active snapshots");
        });
    }

    #[test]
    fn should_keep_snapshot_range_scan_stable_when_compaction_runs_concurrently() {
        for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
            // Arrange
            let engine = Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");

            let mut seed = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin seed tx");
            for i in 0..32 {
                let key = format!("concurrent_key_{i:03}");
                seed.put(key.into_bytes(), b"baseline".to_vec(), None)
                    .expect("seed put");
            }
            seed.commit(buffered_write_options(mode))
                .expect("seed commit");
            engine.flush_cf(&cf).expect("seed flush");

            let snapshot = engine
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("begin snapshot tx");
            wait_for_active_snapshots(&engine, 1, Duration::from_secs(1))
                .expect("wait for active snapshot");

            // Act: drive several overwrite+flush+compact rounds on a background
            // thread while the foreground concurrently rescans the pinned
            // snapshot, so compaction is genuinely racing the scan rather than
            // being sequenced strictly between scan calls.
            let compact_engine = Arc::clone(&engine);
            let compact_cf = cf.clone();
            let compact_write_options = buffered_write_options(mode);
            let worker = std::thread::spawn(move || {
                for round in 0..5 {
                    let mut tx = compact_engine
                        .begin_tx(compact_cf.id(), TransactionMode::ReadWrite)
                        .expect("begin overwrite tx");
                    for i in 0..32 {
                        let key = format!("concurrent_key_{i:03}");
                        let value = format!("round_{round}").into_bytes();
                        tx.put(key.into_bytes(), value, None)
                            .expect("overwrite put");
                    }
                    tx.commit(compact_write_options).expect("overwrite commit");
                    compact_engine
                        .flush_cf(&compact_cf)
                        .expect("overwrite flush");
                    compact_engine.compact_all().expect("compact all");
                }
            });

            while !worker.is_finished() {
                let rows = snapshot
                    .scan(&Query::new())
                    .expect("snapshot scan during concurrent compaction")
                    .try_collect()
                    .expect("collect snapshot scan during concurrent compaction");
                assert_eq!(rows.len(), 32, "mode: {mode}");
                for (_key, value) in rows {
                    assert_eq!(
                        value,
                        bytes::Bytes::from_static(b"baseline"),
                        "mode: {mode}"
                    );
                }
            }
            worker.join().expect("join concurrent compaction worker");

            // Assert
            let rows = snapshot
                .scan(&Query::new())
                .expect("snapshot scan after compaction")
                .try_collect()
                .expect("collect snapshot scan after compaction");
            assert_eq!(rows.len(), 32, "mode: {mode}");
            for (_key, value) in rows {
                assert_eq!(
                    value,
                    bytes::Bytes::from_static(b"baseline"),
                    "mode: {mode}"
                );
            }

            drop(snapshot);
            wait_for_active_snapshots(&engine, 0, Duration::from_secs(1))
                .expect("wait for no active snapshots");
        });
    }

    #[test]
    fn should_preserve_iterator_view_when_flush_and_compaction_run_during_active_scan() {
        // Arrange
        let opts = opts_for_mode("local");
        let db_path = match &opts.storage_mode {
            StorageMode::LocalDisk { db_path } => db_path.clone(),
            _ => unreachable!("local options must expose a local path"),
        };
        let engine = Arc::new(open_with_mode(&opts, "local"));
        let cf = engine.create_column_family("scan-pins").expect("create cf");
        let input_names = seed_scan_pin_generations(&engine, &cf);
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin pinned snapshot");
        let mut scan = read.scan(&Query::new()).expect("open snapshot scan");
        let first = scan
            .next()
            .transpose()
            .expect("read first row")
            .expect("first row");
        assert_eq!(first.0.as_ref(), b"row-000");

        // Act
        let writer_engine = Arc::clone(&engine);
        let writer_cf = cf.clone();
        let worker = std::thread::spawn(move || {
            let mut overwrite = writer_engine
                .begin_tx(writer_cf.id(), TransactionMode::ReadWrite)
                .expect("begin concurrent overwrite");
            for index in 0..24 {
                overwrite
                    .put(
                        format!("row-{index:03}").into_bytes(),
                        b"updated".to_vec(),
                        None,
                    )
                    .expect("overwrite row");
            }
            overwrite
                .commit(WriteOptions::sync())
                .expect("commit overwrite");
            writer_engine
                .flush_cf(&writer_cf)
                .expect("flush concurrent overwrite");
            writer_engine
                .compact_all()
                .expect("compact while scan is open");
        });
        worker.join().expect("join concurrent compaction");
        let remaining = scan.try_collect().expect("finish pinned scan");

        // Assert
        assert_eq!(remaining.len(), 26);
        assert!(
            remaining
                .iter()
                .all(|(_key, value)| value.as_ref() == b"baseline"),
            "the already-open iterator must retain its frozen values"
        );
        let after = engine
            .get_storage_layout()
            .expect("layout after compaction");
        assert!(
            after
                .levels
                .iter()
                .any(|level| level.level == 1 && level.file_count > 0),
            "manual compaction must publish a deeper-level output"
        );
        let live_names: std::collections::HashSet<_> = after
            .levels
            .iter()
            .flat_map(|level| level.files.iter())
            .map(|file| file.name.as_str())
            .collect();
        let retired_input_names: Vec<_> = input_names
            .iter()
            .filter(|name| !live_names.contains(name.as_str()))
            .cloned()
            .collect();
        assert!(
            !retired_input_names.is_empty(),
            "real compaction must retire at least one pinned input"
        );
        assert!(
            retired_input_names
                .iter()
                .all(|name| db_path.join("sst").join(name.as_str()).exists()),
            "snapshot-pinned compaction inputs must remain on disk until the scan closes"
        );
        drop(read);
        wait_for_sst_files_removed(&db_path, &retired_input_names, Duration::from_secs(2));
    }

    #[test]
    fn should_shadow_neither_write_when_delete_range_tombstone_precedes_newer_put_across_sst_generations(
    ) {
        // Arrange
        let opts = opts_for_mode("local");
        let engine = open_with_mode(&opts, "local");
        let cf = engine
            .create_column_family("range-generations")
            .expect("create cf");
        let mut initial = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin initial write");
        initial
            .put(b"middle".to_vec(), b"old".to_vec(), None)
            .expect("put old value");
        initial
            .commit(WriteOptions::sync())
            .expect("commit old value");
        engine.flush_cf(&cf).expect("flush old value");

        let mut delete = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin range delete");
        delete
            .delete_range(b"a".to_vec(), b"z".to_vec())
            .expect("delete range");
        delete
            .commit(WriteOptions::sync())
            .expect("commit range delete");
        engine.flush_cf(&cf).expect("flush range tombstone");

        let mut newer = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin newer write");
        newer
            .put(b"middle".to_vec(), b"new".to_vec(), None)
            .expect("put newer value");
        newer
            .commit(WriteOptions::sync())
            .expect("commit newer value");
        engine.flush_cf(&cf).expect("flush newer value");

        let mut filler = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin filler write");
        filler
            .put(b"zz".to_vec(), b"filler".to_vec(), None)
            .expect("put filler");
        filler.commit(WriteOptions::sync()).expect("commit filler");
        engine.flush_cf(&cf).expect("flush filler");
        let before = engine
            .get_storage_layout()
            .expect("layout before generation compaction");
        assert_eq!(
            before
                .levels
                .iter()
                .find(|level| level.level == 0)
                .map_or(0, |level| level.file_count),
            4,
            "fixture must publish four L0 generations"
        );

        // Act
        engine.compact_all().expect("compact generations");
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin current read");
        let value = read.get(b"middle").expect("read middle");
        let rows = read
            .scan(&Query::new())
            .expect("scan current view")
            .try_collect()
            .expect("collect current view");
        let reverse_rows = read
            .scan(&Query::new().reverse())
            .expect("reverse-scan current view")
            .try_collect()
            .expect("collect reverse current view");

        // Assert
        let after = engine
            .get_storage_layout()
            .expect("layout after generation compaction");
        assert!(
            after
                .levels
                .iter()
                .any(|level| level.level == 1 && level.file_count > 0),
            "the range tombstone and newer point must pass through real compaction"
        );
        assert_eq!(value.as_deref(), Some(&b"new"[..]));
        assert!(rows
            .iter()
            .any(|(key, value)| { key.as_ref() == b"middle" && value.as_ref() == b"new" }));
        assert!(reverse_rows
            .iter()
            .any(|(key, value)| { key.as_ref() == b"middle" && value.as_ref() == b"new" }));
    }

    #[test]
    fn should_return_stable_results_when_scanning_after_column_family_dropped_mid_transaction() {
        // Arrange
        let opts = opts_for_mode("local");
        let engine = Arc::new(open_with_mode(&opts, "local"));
        let cf = engine.create_column_family("drop-scan").expect("create cf");
        let mut write = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin seed write");
        for key in [b"a", b"b", b"c"] {
            write
                .put(key.to_vec(), b"value".to_vec(), None)
                .expect("put row");
        }
        write.commit(WriteOptions::sync()).expect("commit rows");
        engine.flush_cf(&cf).expect("flush rows");
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin snapshot before drop");
        let mut scan = read.scan(&Query::new()).expect("open scan before drop");
        let first = scan
            .next()
            .transpose()
            .expect("read first row")
            .expect("first row");

        // Act
        let drop_engine = Arc::clone(&engine);
        let cf_id = cf.id();
        std::thread::spawn(move || drop_engine.drop_column_family(cf_id))
            .join()
            .expect("join concurrent drop")
            .expect("drop column family with pinned snapshot");
        let remaining = scan.try_collect().expect("finish scan after drop");

        // Assert
        assert_eq!(first.0.as_ref(), b"a");
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].0.as_ref(), b"b");
        assert_eq!(remaining[1].0.as_ref(), b"c");
        assert!(matches!(
            engine.begin_tx(cf.id(), TransactionMode::ReadOnly),
            Err(MidgeError::InvalidArgument(_))
        ));
        drop(read);
    }

    #[test]
    fn should_return_busy_when_scan_iterator_is_still_open_across_shutdown_call() {
        // Arrange
        let opts = opts_for_mode("local");
        let mut engine = open_with_mode(&opts, "local");
        let cf = engine
            .create_column_family("shutdown-scan")
            .expect("create cf");
        let read = engine
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("begin snapshot");
        let scan = read.scan(&Query::new()).expect("open scan");

        // Act
        let result = engine.shutdown(Duration::from_millis(100));

        // Assert
        assert!(matches!(result, Err(MidgeError::Busy(_))));
        drop(scan);
        drop(read);
        engine
            .shutdown(Duration::from_secs(2))
            .expect("shutdown after scan closes");
    }
}

mod transaction_spill {
    //! Tests for transaction spill behavior and memory management
    //!
    //! Tests 1-12: durable storage modes (`LocalDisk`, `CloudBacked`) with spill
    //! Test 13: memory-only mode (no spill files)

    use crate::common::*;
    use bytes::Bytes;
    use cntryl_midge::Query;

    // ============================================================================
    // TRANSACTION SPILL TESTS
    // ============================================================================

    /// `should_commit_large_transaction_given_many_writes_exceeding_memory_limit`
    /// Verify all writes commit despite spill triggered by small memory limit
    /// Act: Write 100 keys with small memory budget, commit
    /// Assert: All keys persisted despite spilling to disk
    #[test]
    fn should_commit_large_transaction_given_many_writes_exceeding_memory_limit() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(256 * 1024); // 256KB limit

            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("key{i:04}");
                let value = format!("value_{i:04}");
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("key{i:04}");
                let expected = format!("value_{i:04}");
                let got = tx_read.get(key.as_bytes()).expect("get");
                let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                assert_eq!(
                    got_str,
                    Some(expected),
                    "key {key} mismatch in mode: {mode}"
                );
            }
        });
    }

    /// `should_handle_very_large_transaction_given_multiple_spills_when_persisted`
    /// Verify multiple spill files created and handled correctly
    /// Act: Write 500 keys to force multiple spill files
    /// Assert: All spill files managed and data recovered
    #[test]
    fn should_handle_very_large_transaction_given_multiple_spills_when_persisted() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(128 * 1024); // 128KB - multiple spills

            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..500 {
                let key = format!("big_key{i:04}");
                let value = format!("big_value_{i:04}");
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in (0..500).step_by(50) {
                let key = format!("big_key{i:04}");
                let got = tx_read.get(key.as_bytes()).expect("get");
                assert!(got.is_some(), "key {key} not found after multiple spills");
            }
        });
    }

    /// `should_preserve_data_integrity_given_large_transaction_with_specific_values`
    /// Verify data integrity maintained through spill/commit cycle
    #[test]
    fn should_preserve_data_integrity_given_large_transaction_with_specific_values() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(256 * 1024);

            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("integrity_test_{i:04}");
                let value = format!("pattern_{}_{}", i % 10, "x".repeat(50));
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("integrity_test_{i:04}");
                let expected = format!("pattern_{}_{}", i % 10, "x".repeat(50));
                let got = tx_read.get(key.as_bytes()).expect("get");
                let got_str = got.as_ref().map(|b| String::from_utf8_lossy(b).to_string());
                assert_eq!(
                    got_str,
                    Some(expected),
                    "integrity check failed for key {key}"
                );
            }
        });
    }

    /// `should_preserve_key_order_given_large_transaction_when_iterating`
    /// Verify key order preserved through spill operations
    #[test]
    fn should_preserve_key_order_given_large_transaction_when_iterating() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(128 * 1024);

            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..200 {
                let key = format!("order_test_{i:04}");
                tx.put(key.as_bytes().to_vec(), b"v".to_vec(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert: a real scan must return every key in ascending sorted
            // order, not merely be individually gettable.
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let scanned = tx_read
                .scan(&Query::new())
                .expect("scan all keys")
                .try_collect()
                .expect("collect scanned keys");

            let expected_keys: Vec<Bytes> = (0..200)
                .map(|i| Bytes::from(format!("order_test_{i:04}").into_bytes()))
                .collect();
            let actual_keys: Vec<Bytes> = scanned.iter().map(|(k, _)| k.clone()).collect();

            assert_eq!(
                actual_keys, expected_keys,
                "scan did not return keys in sorted order for mode: {mode}"
            );
            assert!(
                scanned.iter().all(|(_, v)| v == &Bytes::from_static(b"v")),
                "scanned values corrupted in mode: {mode}"
            );
        });
    }

    /// `should_rollback_spilled_transaction_given_drop_without_commit`
    /// Verify spilled transaction data cleaned up on drop
    #[test]
    fn should_rollback_spilled_transaction_given_drop_without_commit() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(256 * 1024);

            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("rollback_test_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                        .expect("put");
                }
            }

            // Assert
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"rollback_test_0000").expect("get");
            assert_eq!(got, None, "rolled back data persisted in mode: {mode}");
        });
    }

    /// `should_cleanup_spill_files_given_transaction_rollback_when_finalizing`
    /// Verify spill files cleaned up on transaction rollback
    #[test]
    fn should_cleanup_spill_files_given_transaction_rollback_when_finalizing() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(100 * 1024);

            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            {
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..300 {
                    let key = format!("spill_cleanup_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                        .expect("put");
                }
            }

            let mut tx_write = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx_write
                .put(b"test".to_vec(), b"value".to_vec(), None)
                .expect("put");
            tx_write
                .commit(buffered_write_options(mode))
                .expect("commit");

            // Assert
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"test").expect("get");
            assert_eq!(
                got,
                Some(Bytes::from_static(b"value")),
                "engine broken after spill cleanup"
            );
        });
    }

    /// `should_rollback_uncommitted_spill_given_restart_before_commit`
    /// Verify spilled data rolled back after restart if not committed
    #[test]
    fn should_rollback_uncommitted_spill_given_restart_before_commit() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let opts_clone = opts.clone();
            let mut opts = opts.clone();
            opts = opts.memory_budget(100 * 1024);

            // Act
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..300 {
                    let key = format!("uncom_spill_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                        .expect("put");
                }
                drop(tx);
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert
            {
                let engine = open_with_mode(&opts_clone, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");
                let got = tx_read.get(b"uncom_spill_0000").expect("get");
                assert_eq!(got, None, "uncommitted spill recovered in mode: {mode}");
            }
        });
    }

    /// `should_recover_committed_spill_given_restart_after_commit`
    /// Verify spilled data recovered after restart if committed
    #[test]
    fn should_recover_committed_spill_given_restart_after_commit() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let opts_clone = opts.clone();
            let mut opts = opts.clone();
            opts = opts.memory_budget(100 * 1024);

            // Act
            {
                let mut engine = open_with_mode(&opts, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..300 {
                    let key = format!("com_spill_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(buffered_write_options(mode)).expect("commit");
                engine
                    .shutdown(std::time::Duration::from_secs(5))
                    .expect("shutdown before reopen");
            }

            // Assert
            {
                let engine = open_with_mode(&opts_clone, mode);
                let cf = engine.create_column_family("test").expect("create cf");

                let tx_read = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin_tx");
                let got = tx_read.get(b"com_spill_0000").expect("get");
                assert_eq!(
                    got,
                    Some(Bytes::from_static(b"value")),
                    "committed spill not recovered"
                );
            }
        });
    }

    /// `should_not_starve_foreground_writes_given_background_spill_activity`
    /// Verify foreground writes make real, bounded-latency progress while a
    /// concurrent background thread continuously runs large transactions that
    /// spill to disk.
    #[test]
    fn should_not_starve_foreground_writes_given_background_spill_activity() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            // A small budget forces real spill activity.
            opts = opts.memory_budget(64 * 1024);
            // This is a sustained-ingest progress test. With the hard L0 ceiling,
            // background compaction must be enabled so admitted generations can
            // retire and make room for later foreground writes.
            opts.enable_compaction = true;

            let engine = std::sync::Arc::new(open_with_mode(&opts, mode));
            let cf = engine.create_column_family("test").expect("create cf");
            let write_options = buffered_write_options(mode);

            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

            // Background thread: repeatedly commits large transactions that
            // exceed the memory budget and spill to disk, for as long as the
            // foreground thread is still working.
            let bg_engine = std::sync::Arc::clone(&engine);
            let bg_cf_id = cf.id();
            let bg_stop = std::sync::Arc::clone(&stop);
            let background = std::thread::spawn(move || {
                let mut round: u32 = 0;
                while !bg_stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut tx = bg_engine
                        .begin_tx(bg_cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin background tx");
                    for i in 0..300 {
                        let key = format!("bg_{round}_{i:04}");
                        tx.put(key.as_bytes().to_vec(), vec![b'b'; 512], None)
                            .expect("background put");
                    }
                    match tx.commit(write_options) {
                        Ok(()) => round += 1,
                        Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                            // The foreground and background writers share the
                            // same deliberately tiny budget. Backpressure is an
                            // expected retry signal, not a worker failure.
                            std::thread::sleep(std::time::Duration::from_millis(5));
                        }
                        Err(other) => panic!("background commit failed: {other:?}"),
                    }
                }
            });

            // Act: run foreground writes concurrently with the background spill
            // activity and measure how long they take to complete. The shared
            // memory budget means a foreground commit can legitimately observe
            // transient backpressure (WriteStall) while the background thread is
            // saturating the budget; a correctly-behaving engine surfaces that as
            // a retryable signal rather than starving the foreground writer
            // forever, so the foreground loop retries on WriteStall and the test
            // asserts the *overall* wall-clock time stays bounded.
            let fg_count = 50;
            let start = std::time::Instant::now();
            for i in 0..fg_count {
                let key = format!("foreground_{i:04}");
                loop {
                    let mut tx_fg = engine
                        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin foreground tx");
                    tx_fg
                        .put(key.as_bytes().to_vec(), b"works".to_vec(), None)
                        .expect("foreground put");
                    match tx_fg.commit(write_options) {
                        Ok(()) => break,
                        Err(cntryl_midge::MidgeError::WriteStall(_)) => {
                            assert!(
                                start.elapsed() < std::time::Duration::from_secs(30),
                                "foreground writes starved by background spill activity \
                                 after {i} commits, mode: {mode}"
                            );
                            std::thread::sleep(std::time::Duration::from_millis(5));
                        }
                        Err(other) => panic!("foreground commit failed: {other:?}"),
                    }
                }
            }
            let elapsed = start.elapsed();

            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            background.join().expect("background thread panicked");

            // Assert: foreground writes completed in bounded time and are all
            // durably visible, despite concurrent background spill activity.
            assert!(
                elapsed < std::time::Duration::from_secs(30),
                "foreground writes starved by background spill activity: {elapsed:?} \
                 for {fg_count} commits, mode: {mode}"
            );

            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            for i in 0..fg_count {
                let key = format!("foreground_{i:04}");
                let got = tx_read.get(key.as_bytes()).expect("get");
                assert_eq!(
                    got,
                    Some(Bytes::from_static(b"works")),
                    "foreground write {key} lost, mode: {mode}"
                );
            }
        });
    }

    /// `should_handle_concurrent_large_transactions_given_memory_pressure`
    /// Verify system handles concurrent large transactions
    #[test]
    fn should_handle_concurrent_large_transactions_given_memory_pressure() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(256 * 1024);

            let engine = std::sync::Arc::new(open_with_mode(&opts, mode));

            // Act
            let engine_clone = std::sync::Arc::clone(&engine);
            let write_options = buffered_write_options(mode);
            let t1 = std::thread::spawn(move || {
                let cf = engine_clone
                    .create_column_family("test")
                    .expect("create cf");
                let mut tx = engine_clone
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("t1_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"t1_value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(write_options).expect("commit");
            });

            let engine_clone = std::sync::Arc::clone(&engine);
            let write_options = buffered_write_options(mode);
            let t2 = std::thread::spawn(move || {
                let cf = engine_clone
                    .create_column_family("test")
                    .expect("create cf");
                let mut tx = engine_clone
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin_tx");
                for i in 0..200 {
                    let key = format!("t2_key_{i:04}");
                    tx.put(key.as_bytes().to_vec(), b"t2_value".to_vec(), None)
                        .expect("put");
                }
                tx.commit(write_options).expect("commit");
            });

            t1.join().expect("t1 join");
            t2.join().expect("t2 join");

            // Assert
            let cf = engine.create_column_family("test").expect("create cf");
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got1 = tx_read.get(b"t1_key_0000").expect("get");
            let got2 = tx_read.get(b"t2_key_0000").expect("get");
            assert_eq!(
                got1,
                Some(Bytes::from_static(b"t1_value")),
                "t1 data missing"
            );
            assert_eq!(
                got2,
                Some(Bytes::from_static(b"t2_value")),
                "t2 data missing"
            );
        });
    }

    /// `should_handle_transaction_with_tiny_memory_limit_given_forced_spill`
    /// Verify system handles extremely tight memory limits
    #[test]
    fn should_handle_transaction_with_tiny_memory_limit_given_forced_spill() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(32 * 1024); // 32KB - reasonable limit

            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..20 {
                let key = format!("tiny_{i:02}");
                let value = format!("value{i:02}");
                tx.put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), None)
                    .expect("put");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got = tx_read.get(b"tiny_00").expect("get");
            assert!(got.is_some(), "data lost with tiny memory limit");
        });
    }

    /// `should_handle_mixed_value_sizes_in_spilled_transaction_when_committed`
    /// Verify transaction handles mixed sized values through spill
    #[test]
    fn should_handle_mixed_value_sizes_in_spilled_transaction_when_committed() {
        for_each_storage_mode(durable_storage_modes(), |mode, opts| {
            // Arrange
            let mut opts = opts;
            opts = opts.memory_budget(128 * 1024);

            let engine = open_with_mode(&opts, mode);
            let cf = engine.create_column_family("test").expect("create cf");

            // Act
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            for i in 0..100 {
                let key = format!("mixed_{i:04}");
                let value = if i % 3 == 0 {
                    b"tiny".to_vec()
                } else if i % 3 == 1 {
                    vec![b'x'; 256]
                } else {
                    vec![b'y'; 512]
                };
                tx.put(key.as_bytes().to_vec(), value, None).expect("put");
            }
            tx.commit(buffered_write_options(mode)).expect("commit");

            // Assert
            let tx_read = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin_tx");
            let got_tiny = tx_read.get(b"mixed_0000").expect("get");
            assert!(got_tiny.is_some(), "tiny value lost");

            let got_med = tx_read.get(b"mixed_0001").expect("get");
            assert_eq!(
                got_med.as_ref().map(bytes::Bytes::len),
                Some(256),
                "medium size wrong"
            );

            let got_large = tx_read.get(b"mixed_0002").expect("get");
            assert_eq!(
                got_large.as_ref().map(bytes::Bytes::len),
                Some(512),
                "large size wrong"
            );
        });
    }

    /// Count on-disk directories that the engine's in-memory storage path would
    /// use for a spill/data directory (see `StartupStoragePath::resolve` for
    /// `Storage::InMemory`, which names such directories
    /// `target/tmp/midge_test_memory_*`). `StartupStoragePath::prepare` never
    /// calls `create_dir_all` for memory-mode engines, so in-memory mode should
    /// never cause any such directory to come into existence.
    fn count_memory_mode_artifact_dirs() -> usize {
        let tmp_root = std::path::Path::new("target/tmp");
        std::fs::read_dir(tmp_root).map_or(0, |entries| {
            entries
                .filter_map(Result::ok)
                .filter(|entry| {
                    entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with("midge_test_memory_"))
                })
                .count()
        })
    }

    /// `should_not_create_disk_artifacts_given_large_transaction_when_memory_mode`
    /// Verify memory-only mode doesn't create spill files
    #[test]
    fn should_not_create_disk_artifacts_given_large_transaction_when_memory_mode() {
        // Arrange
        let opts = memory_opts();
        let dirs_before = count_memory_mode_artifact_dirs();

        let engine = open_with_mode(&opts, "memory");
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin_tx");
        for i in 0..500 {
            let key = format!("mem_only_{i:04}");
            tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
                .expect("put");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit");

        // Assert: no filesystem artifacts (spill directory or otherwise) were
        // created as a side effect of this large in-memory transaction.
        let dirs_after = count_memory_mode_artifact_dirs();
        assert_eq!(
            dirs_after, dirs_before,
            "memory mode must not create on-disk artifacts"
        );

        let tx_read = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let got = tx_read.get(b"mem_only_0000").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"value")),
            "memory mode data lost"
        );
    }
}

mod runtime_transaction_coalescing {
    use crate::common::opts_for_mode;
    use cntryl_midge::Query;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn should_preserve_runtime_coalescing_when_threads_write_concurrently() {
        // Arrange
        let mut opts = opts_for_mode("memory");
        opts.memtable_size = 64 * 1024 * 1024;
        let engine = Arc::new(cntryl_midge::Engine::open(opts.to_open_options()).unwrap());
        let cf = engine.create_column_family("test_cf").unwrap();
        let cf_id = cf.id();

        let num_threads = 8_usize;
        let ops_per_thread = 500_usize;

        // Act
        let mut handles = vec![];
        for thread_id in 0..num_threads {
            let engine_clone = Arc::clone(&engine);
            let handle = thread::spawn(move || {
                for op_id in 0..ops_per_thread {
                    let key = format!("key-t{thread_id:02}-o{op_id:06}");
                    let value = format!("val-{op_id}");

                    let mut tx = engine_clone
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin");
                    tx.put(key.into_bytes(), value.into_bytes(), None)
                        .expect("put");
                    tx.commit(cntryl_midge::WriteOptions::buffered())
                        .expect("commit");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("thread join");
        }

        // Assert: caller submissions stay distinct, while the runtime coalesces
        // their WAL frames and preserves every logical write.
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");
        let read = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read transaction");
        let rows = read
            .scan(&Query::new())
            .expect("scan writes")
            .try_collect()
            .expect("collect writes");
        let total_ops =
            u64::try_from(num_threads * ops_per_thread).expect("operation count fits u64");
        assert_eq!(rows.len() as u64, total_ops);
        assert!(
            metrics.wal_append_count < total_ops,
            "runtime write draining should coalesce logical transactions"
        );
    }

    #[test]
    fn should_handle_concurrent_writes_correctly_with_runtime_coalescing() {
        // Arrange: Create engine
        let opts = opts_for_mode("memory");
        let engine =
            Arc::new(cntryl_midge::Engine::open(opts.to_open_options()).expect("Engine creation"));
        let cf = engine.create_column_family("test_cf2").expect("create CF");
        let cf_id = cf.id();

        let num_threads = 4_u64;
        let ops_per_thread = 50_u64;

        // Act
        let start = Instant::now();
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let engine_clone = Arc::clone(&engine);
                thread::spawn(move || {
                    for op_num in 0..ops_per_thread {
                        let key = format!("key_{thread_id}_{op_num}");
                        let value = format!("val_{thread_id}_{op_num}");

                        let mut tx = engine_clone
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                            .expect("begin_tx");
                        tx.put(key.into_bytes(), value.into_bytes(), None)
                            .expect("put");
                        tx.commit(cntryl_midge::WriteOptions::buffered())
                            .expect("commit");
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread should complete");
        }

        let elapsed = start.elapsed();

        // Assert: every logical write from every thread is durably visible with
        // the exact value that thread wrote, not merely "readable".
        let read_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        for thread_id in 0..num_threads {
            for op_num in 0..ops_per_thread {
                let key = format!("key_{thread_id}_{op_num}");
                let expected_value = format!("val_{thread_id}_{op_num}");
                let result = read_tx
                    .get(key.as_bytes())
                    .expect("read should not error")
                    .unwrap_or_else(|| {
                        panic!("key {key} should be present after concurrent commits")
                    });
                assert_eq!(
                    result.as_ref(),
                    expected_value.as_bytes(),
                    "value for {key} should match what its writer thread committed"
                );
            }
        }

        let total_ops = ops_per_thread * num_threads;
        let rows = read_tx
            .scan(&cntryl_midge::Query::new())
            .expect("scan writes")
            .try_collect()
            .expect("collect writes");
        assert_eq!(
            rows.len() as u64,
            total_ops,
            "scan should return exactly the rows written by all threads"
        );

        let total_ops_f64 =
            f64::from(u32::try_from(total_ops).expect("test operation count fits in u32"));
        let throughput = total_ops_f64 / elapsed.as_secs_f64();
        println!(
            "OK: Runtime coalescing with backpressure: {total_ops} ops from {num_threads} threads, {throughput:.0} ops/sec"
        );
    }

    #[test]
    fn should_maintain_ordering_with_runtime_coalescing() {
        // Arrange
        let opts = opts_for_mode("memory");
        let engine = cntryl_midge::Engine::open(opts.to_open_options()).expect("Engine creation");
        let cf = engine
            .create_column_family("test_counter")
            .expect("create CF");
        let cf_id = cf.id();

        let num_sequential_ops = 100_u64;

        // Act: Insert sequential values for the same key via concurrent transactions
        for i in 0..num_sequential_ops {
            let mut tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin_tx");
            tx.put(
                "counter".as_bytes().to_vec(),
                i.to_string().into_bytes(),
                None,
            )
            .expect("put");
            tx.commit(cntryl_midge::WriteOptions::buffered())
                .expect("commit");
        }

        // Assert: Verify final value. Use expect (not `if let`) so a missing or
        // errored read fails the test instead of silently skipping the assertion.
        let read_tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin_tx");
        let val = read_tx
            .get("counter".as_bytes())
            .expect("read should not error")
            .expect("counter key should be present after sequential commits");
        let final_val: u64 = std::str::from_utf8(&val)
            .ok()
            .and_then(|s| s.parse().ok())
            .expect("Should parse as u64");

        // The final value should be the last one we wrote
        assert_eq!(
            final_val,
            num_sequential_ops - 1,
            "Final value should match last written value"
        );

        println!("OK: Ordering maintained across {num_sequential_ops} sequential operations");
    }
}
