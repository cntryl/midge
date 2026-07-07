//! Cross-mode verification of the documented last-write-wins transaction model.

use bytes::Bytes;
mod common;
use common::*;
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
