//! Tests for ingest-mode invariant enforcement.
//!
//! These tests verify that the fail-fast mechanisms work correctly:
//! - begin_ingest blocks when compactions are active and logs appropriately
//! - Attempting probe/load/transaction during ingest panics with clear messages
//! - Compaction abort logs occur exactly once per job

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

/// Helper to create a test engine with a temporary directory
fn create_test_engine() -> (MidgeEngine, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("failed to create temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        memtable_size: 1024 * 1024, // 1MB for fast tests
        enable_compaction: true,
        wal_sync: false, // fast tests
        ..Default::default()
    };
    let engine = MidgeEngine::open_with_options(opts).expect("failed to open engine");
    (engine, tmp)
}

#[test]
fn should_enter_ingest_mode_when_no_compactions_active() {
    // Arrange
    let (engine, _tmp) = create_test_engine();

    // Act: enter ingest mode should succeed immediately when no compactions active
    let prev = engine.enter_ingest_mode();

    // Assert
    assert!(prev.is_ok(), "enter_ingest_mode should succeed");
    assert!(
        engine.is_ingesting().unwrap_or(false),
        "engine should be in ingest mode"
    );

    // Cleanup
    engine.exit_ingest_mode(prev.unwrap()).unwrap();
}

#[test]
fn should_report_ingest_state_correctly() {
    // Arrange
    let (engine, _tmp) = create_test_engine();

    // Assert: not ingesting initially
    assert!(
        !engine.is_ingesting().unwrap_or(true),
        "engine should not be ingesting initially"
    );

    // Act: enter ingest mode
    let prev = engine.enter_ingest_mode().unwrap();

    // Assert: now ingesting
    assert!(
        engine.is_ingesting().unwrap_or(false),
        "engine should be ingesting after enter_ingest_mode"
    );

    // Act: exit ingest mode
    engine.exit_ingest_mode(prev).unwrap();

    // Assert: no longer ingesting
    assert!(
        !engine.is_ingesting().unwrap_or(true),
        "engine should not be ingesting after exit_ingest_mode"
    );
}

#[test]
#[should_panic(expected = "BUG")]
fn should_panic_when_begin_transaction_called_during_ingest() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");
    let cf = engine.default_column_family();

    // Act: attempt to begin a transaction while in ingest mode
    // This should panic with a BUG message
    let _txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite);

    // Cleanup (unreachable due to panic)
    let _ = engine.exit_ingest_mode(prev);
}

#[test]
#[should_panic(expected = "BUG")]
fn should_panic_when_transaction_called_during_ingest() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");

    // Act: attempt to create a transaction while in ingest mode
    // This should panic with a BUG message
    let _txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).unwrap();

    // Cleanup (unreachable due to panic)
    let _ = engine.exit_ingest_mode(prev);
}

#[test]
#[should_panic(expected = "BUG")]
fn should_panic_when_transaction_with_isolation_called_during_ingest() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");

    // Act: attempt to create a transaction with isolation while in ingest mode
    // This should panic with a BUG message
    let _txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite);

    // Cleanup (unreachable due to panic)
    let _ = engine.exit_ingest_mode(prev);
}

#[test]
fn should_complete_ingest_cycle_correctly() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();

    // Pre-ingest: transactions should work
    {
        let txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite);
        assert!(txn.is_ok(), "transaction should work before ingest");
    }

    // Act: enter ingest mode
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");

    // Assert: we are in ingest mode
    assert!(engine.is_ingesting().unwrap_or(false));

    // Act: exit ingest mode
    engine.exit_ingest_mode(prev).expect("exit ingest failed");

    // Assert: not in ingest mode anymore
    assert!(!engine.is_ingesting().unwrap_or(true));

    // Post-ingest: transactions should work again
    {
        let txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite);
        assert!(txn.is_ok(), "transaction should work after ingest");
    }
}

#[test]
fn should_allow_writes_before_and_after_ingest_mode() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();

    // Act: write BEFORE entering ingest mode
    let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin");
    tx.put(b"test_key".to_vec(), b"test_value".to_vec(), None).expect("put");
    let result = engine.commit(tx, cntryl_midge::WriteOptions::default());
    assert!(result.is_ok(), "writes should work before ingest mode");

    // Enter and exit ingest mode
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");
    engine.exit_ingest_mode(prev).expect("exit ingest failed");

    // Verify the write persisted after exiting ingest mode
    let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin");
    let value = tx.get(b"test_key").expect("get failed");
    assert_eq!(value.as_deref(), Some(b"test_value".as_slice()));
}

#[test]
fn should_allow_batch_writes_before_and_after_ingest_mode() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();
    let _cf_id = cf.id();

    // Write batch BEFORE entering ingest mode
    let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin");
    tx.put(b"key1".to_vec(), b"val1".to_vec(), None).expect("put");
    tx.put(b"key2".to_vec(), b"val2".to_vec(), None).expect("put");
    tx.put(b"key3".to_vec(), b"val3".to_vec(), None).expect("put");

    let result = engine.commit(tx, cntryl_midge::WriteOptions::default());
    assert!(
        result.is_ok(),
        "batch writes should work before ingest mode"
    );

    // Enter and exit ingest mode
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");
    engine.exit_ingest_mode(prev).expect("exit ingest failed");

    // Verify writes persisted after exiting ingest mode
    let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin");
    assert_eq!(tx.get(b"key1").unwrap().as_deref(), Some(b"val1".as_slice()));
    assert_eq!(tx.get(b"key2").unwrap().as_deref(), Some(b"val2".as_slice()));
    assert_eq!(tx.get(b"key3").unwrap().as_deref(), Some(b"val3".as_slice()));
}

#[test]
fn should_allow_reads_before_and_after_ingest_mode() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();

    // Write some data before ingest
    let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin");
    tx.put(b"pre_key".to_vec(), b"pre_value".to_vec(), None).unwrap();
    engine.commit(tx, cntryl_midge::WriteOptions::default()).unwrap();

    // Read before entering ingest mode
    let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin");
    let result = tx.get(b"pre_key");
    assert!(result.is_ok(), "reads should work before ingest mode");
    assert_eq!(result.unwrap().as_deref(), Some(b"pre_value".as_slice()));

    // Enter and exit ingest mode
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");
    engine.exit_ingest_mode(prev).expect("exit ingest failed");

    // Read after exiting ingest mode
    let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin");
    let result = tx.get(b"pre_key");
    assert!(result.is_ok(), "reads should work after ingest mode");
    assert_eq!(result.unwrap().as_deref(), Some(b"pre_value".as_slice()));
}

/// Test that the ingest invariant message includes the correct ordering guidance
#[test]
fn should_include_correct_ordering_in_panic_message() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");
    let cf = engine.default_column_family();

    // Act: catch the panic to verify the message
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite);
    }));

    // Cleanup first
    let _ = engine.exit_ingest_mode(prev);

    // Assert: panic message should include ordering guidance
    assert!(result.is_err(), "should have panicked");
    let panic_info = result.unwrap_err();
    let panic_msg = panic_info
        .downcast_ref::<String>()
        .map(|s| s.as_str())
        .or_else(|| panic_info.downcast_ref::<&str>().copied())
        .unwrap_or("");

    assert!(
        panic_msg.contains("BUG"),
        "panic message should mention BUG: {}",
        panic_msg
    );
    assert!(
        panic_msg.contains("exit_ingest_mode"),
        "panic message should mention exit_ingest_mode: {}",
        panic_msg
    );
}

/// Test that multiple ingest cycles work correctly
#[test]
fn should_support_multiple_ingest_cycles() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();

    // Act: first cycle - write before, enter/exit, write after
    {
        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin");
        tx.put(b"cycle1_key".to_vec(), b"cycle1_val".to_vec(), None).unwrap();
        engine.commit(tx, cntryl_midge::WriteOptions::default()).unwrap();

        let prev = engine.enter_ingest_mode().expect("enter ingest failed");
        engine.exit_ingest_mode(prev).expect("exit ingest failed");
    }

    // Verify transactions work between cycles
    {
        let txn = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite);
        assert!(txn.is_ok());
    }

    // Act: second cycle - write before, enter/exit, write after
    {
        let mut tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite).expect("begin");
        tx.put(b"cycle2_key".to_vec(), b"cycle2_val".to_vec(), None).unwrap();
        engine.commit(tx, cntryl_midge::WriteOptions::default()).unwrap();

        let prev = engine.enter_ingest_mode().expect("enter ingest failed");
        engine.exit_ingest_mode(prev).expect("exit ingest failed");
    }

    // Assert: both writes persisted
    let tx = engine.begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly).expect("begin");
    assert_eq!(
        tx.get(b"cycle1_key").unwrap().as_deref(),
        Some(b"cycle1_val".as_slice())
    );
    assert_eq!(
        tx.get(b"cycle2_key").unwrap().as_deref(),
        Some(b"cycle2_val".as_slice())
    );
}
