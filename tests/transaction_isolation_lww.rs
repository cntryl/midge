//! Cross-mode verification of the documented last-write-wins transaction model.

use bytes::Bytes;
use cntryl_midge::testkit::*;
use std::sync::Arc;

#[test]
fn should_hide_uncommitted_writes_given_uncommitted_write_when_read_different_mode() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
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
            "mode: {}",
            mode
        );
    });
}

#[test]
fn should_apply_last_committed_write_given_multiple_commits_when_last_write_wins() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
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

        engine
            .commit(txn1, cntryl_midge::WriteOptions::buffered())
            .expect("commit txn1");
        engine
            .commit(txn2, cntryl_midge::WriteOptions::buffered())
            .expect("commit txn2");

        let reader = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin reader");

        // Assert
        assert_eq!(
            reader.get(b"key").expect("read final key"),
            Some(Bytes::from_static(b"from_txn2")),
            "mode: {}",
            mode
        );
    });
}

#[test]
fn should_allow_lost_update_given_concurrent_writes_when_lost_update_occurs() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = Arc::new(open_with_mode(opts, mode));
        let cf = engine.create_column_family("test").expect("create cf");

        let mut setup = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin setup");
        setup
            .put(b"counter".to_vec(), b"0".to_vec(), None)
            .expect("put initial counter");
        engine
            .commit(setup, cntryl_midge::WriteOptions::buffered())
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

        engine
            .commit(txn1, cntryl_midge::WriteOptions::buffered())
            .expect("commit txn1");
        engine
            .commit(txn2, cntryl_midge::WriteOptions::buffered())
            .expect("commit txn2");

        let reader = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin reader");

        // Assert
        assert_eq!(
            reader.get(b"counter").expect("read final counter"),
            Some(Bytes::from_static(b"1")),
            "mode: {}",
            mode
        );
    });
}
