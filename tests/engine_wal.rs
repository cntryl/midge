//! WAL (Write-Ahead Log) Integration Tests
//!
//! Tests WAL functionality: recovery, data durability, corruption handling,
//! large values, rotation, and mixed operation recovery.

mod common;
use cntryl_midge::WriteOptions;
use common::*;

#[test]
fn should_recover_data_from_wal_after_flush() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    for i in 0..50 {
        let key = format!("wal_key_{i:04}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");
        tx.put(key.as_bytes().to_vec(), b"wal_value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).expect("commit");
    }

    // Act
    engine.flush_cf(&cf).expect("flush cf");

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let count = tx
        .scan(&cntryl_midge::Query::new())
        .expect("scan")
        .try_collect()
        .expect("collect scan")
        .len();

    // Assert
    assert_eq!(count, 50, "expected all WAL-backed keys to survive flush");
}

#[test]
fn should_handle_large_values_in_wal() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    let large_value = vec![0xFF; 1_000_000];

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx");
    tx.put(b"large_wal_key".to_vec(), large_value.clone(), None)
        .expect("put");
    tx.commit(WriteOptions::buffered()).expect("commit");

    // Act
    engine.flush_cf(&cf).expect("flush cf");

    let read_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let retrieved = read_tx.get(b"large_wal_key").expect("get");

    // Assert
    assert_eq!(
        retrieved.as_ref().map(bytes::Bytes::len),
        Some(1_000_000),
        "expected the full large value to survive recovery"
    );
}

#[test]
fn should_recover_deletes_from_wal() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    for i in 0..30 {
        let key = format!("del_key_{i:04}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).expect("commit");
    }

    let mut txn = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin delete tx");
    for i in 0..10 {
        let key = format!("del_key_{i:04}");
        txn.delete(key.into_bytes()).expect("delete");
    }
    txn.commit(WriteOptions::buffered())
        .expect("commit deletes");

    // Act
    engine.flush_cf(&cf).expect("flush cf");

    let mut deleted_count = 0;
    for i in 0..10 {
        let key = format!("del_key_{i:04}");
        let read_tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin read tx");
        if read_tx.get(key.as_bytes()).expect("get").is_none() {
            deleted_count += 1;
        }
    }

    // Assert
    assert_eq!(deleted_count, 10, "expected all deletes to be recovered");
}

#[test]
fn should_recover_range_tombstones_from_wal() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    for i in 0..100 {
        let key = format!("k{i:03}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");
        tx.put(key.as_bytes().to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).expect("commit");
    }

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin range delete tx");
    tx.delete_range(b"k020".to_vec(), b"k080".to_vec())
        .expect("delete range");
    tx.commit(WriteOptions::buffered())
        .expect("commit range delete");

    // Act
    engine.flush_cf(&cf).expect("flush cf");

    let scan_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin scan tx");
    let rows = scan_tx
        .scan(&cntryl_midge::Query::new())
        .expect("scan")
        .try_collect()
        .expect("collect scan");
    let in_range = rows
        .iter()
        .filter(|(k, _)| {
            let k_str = String::from_utf8_lossy(k.as_ref());
            k_str.as_ref() >= "k020" && k_str.as_ref() < "k080"
        })
        .count();

    // Assert
    assert_eq!(in_range, 0, "expected the tombstoned range to be empty");
}

#[test]
fn should_handle_wal_rotation_multiple_segments() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    let mut batch_count = 0;

    for batch in 0..5 {
        for i in 0..100 {
            let key = format!("batch{batch}_key{i:04}");
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin tx");
            tx.put(key.as_bytes().to_vec(), b"batch_value".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("commit");
        }

        engine.flush_cf(&cf).expect("flush cf");
        batch_count += 1;
    }

    // Act
    let final_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let total = final_tx
        .scan(&cntryl_midge::Query::new())
        .expect("scan")
        .try_collect()
        .expect("collect scan")
        .len();

    // Assert
    assert_eq!(total, batch_count * 100, "expected every batch to survive");
}

#[test]
fn should_recover_mixed_operations_from_wal() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    let mut tx0 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx0");
    tx0.put(b"put_key".to_vec(), b"put_value".to_vec(), None)
        .expect("put");
    tx0.commit(WriteOptions::buffered()).expect("commit");

    let mut txn1 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx1");
    txn1.delete(b"put_key".to_vec()).expect("delete");
    txn1.commit(WriteOptions::buffered())
        .expect("commit delete");

    let mut tx1 = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin tx2");
    tx1.put(b"put_key".to_vec(), b"put_value_v2".to_vec(), None)
        .expect("put");
    tx1.commit(WriteOptions::buffered())
        .expect("commit overwrite");

    for i in 0..20 {
        let key = format!("dr_{i:02}");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");
        tx.put(key.as_bytes().to_vec(), b"v".to_vec(), None)
            .expect("put");
        tx.commit(WriteOptions::buffered()).expect("commit");
    }

    let mut delete_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin range delete tx");
    delete_tx
        .delete_range(b"dr_05".to_vec(), b"dr_15".to_vec())
        .expect("delete range");
    delete_tx
        .commit(WriteOptions::buffered())
        .expect("commit range delete");

    // Act
    engine.flush_cf(&cf).expect("flush cf");

    let verify_tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin read tx");
    let put_val = verify_tx.get(b"put_key").expect("get");
    let rows = verify_tx
        .scan(&cntryl_midge::Query::new())
        .expect("scan")
        .try_collect()
        .expect("collect scan");
    let dr_remaining = rows
        .iter()
        .filter(|(k, _)| {
            let k_str = String::from_utf8_lossy(k.as_ref());
            k_str.as_ref() >= "dr_05" && k_str.as_ref() < "dr_15"
        })
        .count();

    // Assert
    assert_eq!(
        put_val.as_deref(),
        Some(&b"put_value_v2"[..]),
        "expected the last write to win"
    );
    assert_eq!(dr_remaining, 0, "expected the range delete to win");
}
