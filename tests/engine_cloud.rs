//! Cloud-mode integration tests with real assertions.

use bytes::Bytes;
mod common;
use cntryl_midge::Query;
use common::*;

#[test]
fn should_create_column_family_given_cloud_mode_when_requested() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");

    // Act
    let cf = engine.create_column_family("test").expect("create cf");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read transaction");
    assert_eq!(tx.get(b"missing_key").expect("read missing key"), None);
}

#[test]
fn should_read_written_value_given_cloud_mode_when_written_and_read() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write transaction");
    tx.put(b"cloud_key".to_vec(), b"cloud_value".to_vec(), None)
        .expect("put cloud value");
    tx.commit(cntryl_midge::WriteOptions::cloud_async())
        .expect("commit cloud write");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read transaction");
    assert_eq!(
        tx.get(b"cloud_key").expect("read cloud key"),
        Some(Bytes::from_static(b"cloud_value"))
    );
}

#[test]
fn should_read_committed_transaction_value_given_cloud_mode_when_committed() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin transaction");
    txn.put(b"tx_key1".to_vec(), b"tx_value1".to_vec(), None)
        .expect("put transaction value");
    txn.commit(cntryl_midge::WriteOptions::cloud_async())
        .expect("commit cloud transaction");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read transaction");
    assert_eq!(
        tx.get(b"tx_key1").expect("read transaction key"),
        Some(Bytes::from_static(b"tx_value1"))
    );
}

#[test]
fn should_scan_inserted_keys_given_cloud_mode_when_range_scanned() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
    let cf = engine.create_column_family("test").expect("create cf");

    for i in 0..20 {
        let key = format!("cloud_scan_{i:02}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin scan seed transaction");
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .expect("put scan seed value");
        tx.commit(cntryl_midge::WriteOptions::cloud_async())
            .expect("commit scan seed transaction");
    }

    // Act
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin scan transaction");
    let results = tx
        .scan(&Query::new())
        .expect("scan cloud keys")
        .try_collect()
        .expect("collect cloud keys");

    // Assert
    assert_eq!(results.len(), 20);
}

#[test]
fn should_preserve_snapshot_value_given_cloud_mode_when_overwritten_after_snapshot() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
    let cf = engine.create_column_family("test").expect("create cf");

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    tx.put(b"snap_key".to_vec(), b"snap_v1".to_vec(), None)
        .expect("put initial snapshot value");
    tx.commit(cntryl_midge::WriteOptions::cloud_async())
        .expect("commit seed transaction");

    let snapshot = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin snapshot transaction");

    // Act
    let mut tx2 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin overwrite transaction");
    tx2.put(b"snap_key".to_vec(), b"snap_v2".to_vec(), None)
        .expect("put overwrite value");
    tx2.commit(cntryl_midge::WriteOptions::cloud_async())
        .expect("commit overwrite transaction");

    // Assert
    assert_eq!(
        snapshot.get(b"snap_key").expect("read snapshot key"),
        Some(Bytes::from_static(b"snap_v1"))
    );
    let current = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin current read transaction");
    assert_eq!(
        current.get(b"snap_key").expect("read current key"),
        Some(Bytes::from_static(b"snap_v2"))
    );
}

#[test]
fn should_hide_deleted_key_given_cloud_mode_when_deleted() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
    let cf = engine.create_column_family("test").expect("create cf");

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin seed transaction");
    tx.put(b"del_key".to_vec(), b"to_delete".to_vec(), None)
        .expect("put delete seed value");
    tx.commit(cntryl_midge::WriteOptions::cloud_async())
        .expect("commit seed transaction");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin delete transaction");
    tx.delete(b"del_key".to_vec()).expect("delete cloud key");
    tx.commit(cntryl_midge::WriteOptions::cloud_async())
        .expect("commit delete transaction");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin verification transaction");
    assert_eq!(tx.get(b"del_key").expect("read deleted key"), None);
}

#[test]
fn should_apply_last_write_wins_given_cloud_mode_when_multiple_writes() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("cloud"), "cloud");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin first write transaction");
    tx.put(b"lww_key".to_vec(), b"v1".to_vec(), None)
        .expect("put first value");
    tx.commit(cntryl_midge::WriteOptions::cloud_async())
        .expect("commit first write");

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin second write transaction");
    tx.put(b"lww_key".to_vec(), b"v2".to_vec(), None)
        .expect("put second value");
    tx.commit(cntryl_midge::WriteOptions::cloud_async())
        .expect("commit second write");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read transaction");
    assert_eq!(
        tx.get(b"lww_key").expect("read lww key"),
        Some(Bytes::from_static(b"v2"))
    );
}
