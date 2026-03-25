//! Isolation audit assertions for the current transaction model.
//!
//! These tests classify the engine's observable semantics in a deterministic
//! way instead of printing diagnostics for manual interpretation.

use bytes::Bytes;
mod common;
use common::*;
use std::sync::Arc;

#[test]
fn should_hide_uncommitted_writes_when_reading_from_other_transaction() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut writer = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin writer");
    writer
        .put(b"key".to_vec(), b"uncommitted_value".to_vec(), None)
        .expect("put uncommitted value");

    let reader = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin reader");

    // Assert
    assert_eq!(reader.get(b"key").expect("read uncommitted key"), None);
}

#[test]
fn should_apply_last_committed_value_when_two_transactions_write_same_key() {
    // Arrange
    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut txn1 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin txn1");
    let mut txn2 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin txn2");

    txn1.put(b"key".to_vec(), b"value_from_txn1".to_vec(), None)
        .expect("put txn1 value");
    txn2.put(b"key".to_vec(), b"value_from_txn2".to_vec(), None)
        .expect("put txn2 value");

    assert!(
        txn1.commit(cntryl_midge::WriteOptions::buffered()).is_ok(),
        "first commit should succeed"
    );
    assert!(
        txn2.commit(cntryl_midge::WriteOptions::buffered()).is_ok(),
        "second commit should also succeed"
    );

    let reader = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin reader");

    // Assert
    assert_eq!(
        reader.get(b"key").expect("read final key"),
        Some(Bytes::from_static(b"value_from_txn2"))
    );
}

#[test]
fn should_allow_lost_update_when_two_transactions_increment_same_counter() {
    // Arrange
    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
    let cf = engine.create_column_family("test").expect("create cf");

    let mut setup = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin setup");
    setup
        .put(b"counter".to_vec(), b"0".to_vec(), None)
        .expect("put initial counter");
    setup
        .commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit setup");

    // Act
    let mut txn1 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin txn1");
    let mut txn2 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin txn2");

    let count1: i32 = String::from_utf8_lossy(
        &txn1
            .get(b"counter")
            .expect("txn1 read counter")
            .unwrap_or_default(),
    )
    .parse()
    .expect("parse txn1 counter");
    let count2: i32 = String::from_utf8_lossy(
        &txn2
            .get(b"counter")
            .expect("txn2 read counter")
            .unwrap_or_default(),
    )
    .parse()
    .expect("parse txn2 counter");

    txn1.put(
        b"counter".to_vec(),
        (count1 + 1).to_string().into_bytes(),
        None,
    )
    .expect("txn1 write increment");
    txn2.put(
        b"counter".to_vec(),
        (count2 + 1).to_string().into_bytes(),
        None,
    )
    .expect("txn2 write increment");

    txn1.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit txn1");
    txn2.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit txn2");

    let reader = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin reader");

    // Assert
    assert_eq!(
        reader.get(b"counter").expect("read counter after commits"),
        Some(Bytes::from_static(b"1"))
    );
}

#[test]
fn should_allow_disjoint_writes_after_shared_read_when_transactions_both_commit() {
    // Arrange
    let engine = Arc::new(open_with_mode(opts_for_mode("memory"), "memory"));
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
        .commit(cntryl_midge::WriteOptions::buffered())
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
        Some(Bytes::from_static(b"base_value"))
    );
    assert_eq!(
        txn2.get(b"shared").expect("txn2 read shared"),
        Some(Bytes::from_static(b"base_value"))
    );

    txn1.put(b"flag1".to_vec(), b"true".to_vec(), None)
        .expect("txn1 write flag1");
    txn2.put(b"flag2".to_vec(), b"true".to_vec(), None)
        .expect("txn2 write flag2");

    assert!(
        txn1.commit(cntryl_midge::WriteOptions::buffered()).is_ok(),
        "first disjoint write should commit"
    );
    assert!(
        txn2.commit(cntryl_midge::WriteOptions::buffered()).is_ok(),
        "second disjoint write should also commit"
    );

    let reader = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin reader");

    // Assert
    assert_eq!(
        reader.get(b"flag1").expect("read flag1 after commits"),
        Some(Bytes::from_static(b"true"))
    );
    assert_eq!(
        reader.get(b"flag2").expect("read flag2 after commits"),
        Some(Bytes::from_static(b"true"))
    );
}
