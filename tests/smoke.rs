//! Smoke tests for Midge.
//!
//! Purpose:
//! - Validate core end-to-end invariants
//! - Exercise real engine wiring with minimal data
//! - Catch Ã¢â‚¬Å“green unit tests, broken databaseÃ¢â‚¬Â failures
//!
//! Philosophy:
//! - Tests are intentionally small and deterministic
//! - No sleeps, timing assumptions, or fuzz
//! - Stress, chaos, and performance tests live in the external harness
//! - If all unit tests + this file pass, the database is not fundamentally broken
use bytes::Bytes;
mod common;
use cntryl_midge::Query;
use common::*;

#[test]
fn should_read_written_value_given_memory_mode_when_written() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, Some(Bytes::from_static(b"value")));
}

#[test]
fn should_read_written_value_given_flushed_value_when_read() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    engine.flush_cf(&cf).expect("flush");

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, Some(Bytes::from_static(b"value")));
}

#[test]
fn should_hide_value_given_deleted_key_when_read() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.delete(b"key".to_vec()).expect("delete");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, None);
}

#[test]
fn should_preserve_tombstone_given_flushed_tombstone_when_read() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.delete(b"key".to_vec()).expect("delete");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    engine.flush_cf(&cf).expect("flush");

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, None, "Tombstone should persist through flush");
}

#[test]
fn should_persist_data_given_write_when_restarted() {
    // Arrange
    let opts = opts_for_mode("local");

    // Act - Write and restart
    {
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(
            b"persistent_key".to_vec(),
            b"persistent_value".to_vec(),
            None,
        )
        .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    }

    // Reopen engine
    let engine = open_with_mode(&opts, "local");
    let cf = engine.create_column_family("test").expect("create cf");
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"persistent_key").expect("get");

    // Assert
    assert_eq!(
        result,
        Some(Bytes::from_static(b"persistent_value")),
        "Data should persist after restart"
    );
}

#[test]
fn should_persist_tombstone_given_delete_when_restarted() {
    // Arrange
    let opts = opts_for_mode("local");

    // Act - Delete and restart
    {
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(b"key".to_vec()).expect("delete");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    }

    // Reopen engine
    let engine = open_with_mode(&opts, "local");
    let cf = engine.create_column_family("test").expect("create cf");
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, None, "Tombstone should persist after restart");
}

#[test]
fn should_allow_read_only_snapshot_given_committed_value_when_snapshot_reads() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"v1".to_vec(), None).expect("put");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    let snapshot = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();

    // Assert
    let snap_value = snapshot.get(b"key").expect("get");
    assert_eq!(
        snap_value,
        Some(Bytes::from_static(b"v1")),
        "Snapshot should be usable for reads"
    );

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let current_value = tx.get(b"key").expect("get");
    assert_eq!(
        current_value,
        Some(Bytes::from_static(b"v1")),
        "Engine and snapshot both see data"
    );
}

#[test]
fn should_preserve_latest_version_given_repeated_flushes_when_read() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"v1".to_vec(), None).expect("put");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    engine.flush_cf(&cf).expect("flush");

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"v2".to_vec(), None).expect("put");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    engine.flush_cf(&cf).expect("flush");

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(
        result,
        Some(Bytes::from_static(b"v2")),
        "Repeated flushes should preserve latest version"
    );
}

#[test]
fn should_respect_visibility_rules_given_range_scan_when_scanning() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"a".to_vec(), b"1".to_vec(), None).expect("put");
    tx.put(b"b".to_vec(), b"2".to_vec(), None).expect("put");
    tx.put(b"c".to_vec(), b"3".to_vec(), None).expect("put");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.delete(b"b".to_vec()).expect("delete");
    tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let mut iter = tx
        .scan(
            &Query::new()
                .start_key(Bytes::from(&b"a"[..]))
                .end_key(Bytes::from(&b"d"[..])),
        )
        .expect("scan");
    let results: Vec<_> = std::iter::from_fn(|| iter.next()).collect();

    // Assert - 'b' should be filtered out by delete
    assert_eq!(
        results.len(),
        2,
        "Deleted key should not appear in range scan"
    );
    assert_eq!(results[0].0, Bytes::from_static(b"a"));
    assert_eq!(results[1].0, Bytes::from_static(b"c"));
}

#[test]
fn should_preserve_all_committed_values_given_multiple_writes_when_written() {
    // Arrange
    let engine = open_with_mode(&opts_for_mode("memory"), "memory");
    let cf = engine.create_column_family("test").expect("create cf");

    // Act
    for i in 0..10 {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(format!("key{i}").into_bytes(), b"val".to_vec(), None)
            .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    }

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    for i in 0..10 {
        assert_eq!(
            tx.get(format!("key{i}").as_bytes())
                .expect("get committed key"),
            Some(Bytes::from_static(b"val"))
        );
    }
}

#[test]
fn should_reopen_committed_values_given_engine_dropped_without_close_when_reopened() {
    // Arrange
    let opts = opts_for_mode("local");

    // Act
    {
        let engine = open_with_mode(&opts, "local");
        let cf = engine.create_column_family("test").expect("create cf");
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
            .expect("put");
        tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
            .expect("put");
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        // Intentionally drop without an explicit close call.
    }

    // Reopen and verify state
    let engine = open_with_mode(&opts, "local");
    let cf = engine.create_column_family("test").expect("create cf");

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let v1 = tx.get(b"key1").expect("get");
    let v2 = tx.get(b"key2").expect("get");
    assert_eq!(v1, Some(Bytes::from_static(b"value1")));
    assert_eq!(v2, Some(Bytes::from_static(b"value2")));
}

// Note: Durability frontier enforcement test removed as it requires
// chaos engineering or crash simulation infrastructure that is not yet implemented.
// This should be reintroduced when proper crash testing infrastructure is available.
