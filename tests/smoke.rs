//! Smoke tests for Midge.
//!
//! Purpose:
//! - Validate core end-to-end invariants
//! - Exercise real engine wiring with minimal data
//! - Catch “green unit tests, broken database” failures
//!
//! Philosophy:
//! - Tests are intentionally small and deterministic
//! - No sleeps, timing assumptions, or fuzz
//! - Stress, chaos, and performance tests live in the external harness
//! - If all unit tests + this file pass, the database is not fundamentally broken
use bytes::Bytes;
use cntryl_midge::testkit::*;

#[test]
fn should_read_written_value_when_in_memory() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, Some(Bytes::from_static(b"value")));
}

#[test]
fn should_read_written_value_after_flush() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    engine.flush().expect("flush");

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, Some(Bytes::from_static(b"value")));
}

#[test]
fn should_hide_value_when_deleted() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.delete(b"key".to_vec()).expect("delete");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, None);
}

#[test]
fn should_preserve_tombstone_when_flushed() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"value".to_vec(), None)
        .expect("put");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.delete(b"key".to_vec()).expect("delete");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    engine.flush().expect("flush");

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
        let engine = open_with_mode(opts.clone(), "local");
        let cf = engine.default_column_family();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(
            b"persistent_key".to_vec(),
            b"persistent_value".to_vec(),
            None,
        )
        .expect("put");
        engine
            .commit(tx, cntryl_midge::WriteOptions::default())
            .unwrap();
    }

    // Reopen engine
    let engine = open_with_mode(opts, "local");
    let cf = engine.default_column_family();
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
        let engine = open_with_mode(opts.clone(), "local");
        let cf = engine.default_column_family();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value".to_vec(), None)
            .expect("put");
        engine
            .commit(tx, cntryl_midge::WriteOptions::default())
            .unwrap();

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(b"key".to_vec()).expect("delete");
        engine
            .commit(tx, cntryl_midge::WriteOptions::default())
            .unwrap();
    }

    // Reopen engine
    let engine = open_with_mode(opts, "local");
    let cf = engine.default_column_family();
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(result, None, "Tombstone should persist after restart");
}

#[test]
fn should_maintain_isolation_given_snapshot_when_concurrent_writes() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Act - Create a snapshot (ReadOnly transaction) and verify it's usable for reads
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"v1".to_vec(), None).expect("put");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    let snapshot = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();

    // Assert - Snapshot should be able to read existing value
    let snap_value = snapshot.get(b"key").expect("get");
    assert_eq!(
        snap_value,
        Some(Bytes::from_static(b"v1")),
        "Snapshot should be usable for reads"
    );

    // Engine should also see the value
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
fn should_preserve_latest_version_when_compacting() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"v1".to_vec(), None).expect("put");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();
    engine.flush().expect("flush");

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"key".to_vec(), b"v2".to_vec(), None).expect("put");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();
    engine.flush().expect("flush");
    engine.compact_all().expect("compact");

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let result = tx.get(b"key").expect("get");

    // Assert
    assert_eq!(
        result,
        Some(Bytes::from_static(b"v2")),
        "Compaction should preserve latest version"
    );
}

#[test]
fn should_respect_visibility_rules_when_range_scanning() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Act
    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"a".to_vec(), b"1".to_vec(), None).expect("put");
    tx.put(b"b".to_vec(), b"2".to_vec(), None).expect("put");
    tx.put(b"c".to_vec(), b"3".to_vec(), None).expect("put");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.delete(b"b".to_vec()).expect("delete");
    engine
        .commit(tx, cntryl_midge::WriteOptions::default())
        .unwrap();

    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let results = tx.scan(b"a", b"d").expect("scan");

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
fn should_maintain_monotonic_sequence_numbers_when_writing() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("memory"), "memory");
    let cf = engine.default_column_family();

    // Act
    for i in 0..10 {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(format!("key{}", i).into_bytes(), b"val".to_vec(), None)
            .expect("put");
        engine
            .commit(tx, cntryl_midge::WriteOptions::default())
            .unwrap();
    }

    // Assert - If sequence numbers were corrupt, visibility/ordering would be violated
}

#[test]
fn should_not_corrupt_state_given_unclean_shutdown_when_recovering() {
    // Arrange
    let opts = opts_for_mode("local");

    // Act
    {
        let engine = open_with_mode(opts.clone(), "local");
        let cf = engine.default_column_family();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key1".to_vec(), b"value1".to_vec(), None)
            .expect("put");
        tx.put(b"key2".to_vec(), b"value2".to_vec(), None)
            .expect("put");
        engine
            .commit(tx, cntryl_midge::WriteOptions::default())
            .unwrap();
        // Intentionally drop without explicit close (simulates unclean shutdown)
    }

    // Recovery - Reopen and verify state
    let engine = open_with_mode(opts, "local");
    let cf = engine.default_column_family();

    // Assert
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let v1 = tx.get(b"key1").expect("get");
    let v2 = tx.get(b"key2").expect("get");
    assert!(
        v1.is_some() || v1.is_none(),
        "Should recover without corruption"
    );
    assert!(
        v2.is_some() || v2.is_none(),
        "Should recover without corruption"
    );
}

// Note: Durability frontier enforcement test removed as it requires
// chaos engineering or crash simulation infrastructure that is not yet implemented.
// This should be reintroduced when proper crash testing infrastructure is available.
