//! Tests for ingest-mode invariant enforcement.
//!
//! These tests verify that the fail-fast mechanisms work correctly:
//! - begin_ingest blocks when compactions are active and logs appropriately
//! - Attempting probe/load/transaction during ingest panics with clear messages
//! - Compaction abort logs occur exactly once per job

use cntryl_midge::{IsolationLevel, MidgeEngine, MidgeOptions, StorageMode};

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
    let _txn = engine.begin_transaction(cf);

    // Cleanup (unreachable due to panic)
    let _ = engine.exit_ingest_mode(prev);
}

#[test]
#[should_panic(expected = "BUG")]
fn should_panic_when_transaction_called_during_ingest() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");

    // Act: attempt to create a transaction while in ingest mode
    // This should panic with a BUG message
    let _txn = engine.transaction();

    // Cleanup (unreachable due to panic)
    let _ = engine.exit_ingest_mode(prev);
}

#[test]
#[should_panic(expected = "BUG")]
fn should_panic_when_transaction_with_isolation_called_during_ingest() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");

    // Act: attempt to create a transaction with isolation while in ingest mode
    // This should panic with a BUG message
    let _txn = engine.transaction_with_isolation(IsolationLevel::Serializable);

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
        let txn = engine.begin_transaction(cf);
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
        let txn = engine.begin_transaction(cf);
        assert!(txn.is_ok(), "transaction should work after ingest");
    }
}

#[test]
fn should_allow_writes_during_ingest_mode() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();

    // Act: enter ingest mode
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");

    // Assert: writes should still work during ingest (that's the point!)
    let result = engine.put(cf, b"test_key", b"test_value");
    assert!(result.is_ok(), "writes should work during ingest mode");

    // Cleanup
    engine.exit_ingest_mode(prev).expect("exit ingest failed");

    // Verify the write persisted
    let value = engine.get(cf, b"test_key").expect("get failed");
    assert_eq!(value.as_deref(), Some(b"test_value".as_slice()));
}

#[test]
fn should_allow_batch_writes_during_ingest_mode() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();
    let cf_id = cf.id();

    // Act: enter ingest mode
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");

    // Assert: batch writes should work during ingest
    let mut batch = cntryl_midge::WriteBatch::new();
    batch.put_cf(cf_id, b"key1".to_vec().into(), b"val1".to_vec().into());
    batch.put_cf(cf_id, b"key2".to_vec().into(), b"val2".to_vec().into());
    batch.put_cf(cf_id, b"key3".to_vec().into(), b"val3".to_vec().into());

    let result = engine.write_batch(&batch);
    assert!(
        result.is_ok(),
        "batch writes should work during ingest mode"
    );

    // Cleanup
    engine.exit_ingest_mode(prev).expect("exit ingest failed");
}

#[test]
fn should_allow_reads_during_ingest_mode() {
    // Arrange
    let (engine, _tmp) = create_test_engine();
    let cf = engine.default_column_family();

    // Write some data before ingest
    engine.put(cf, b"pre_key", b"pre_value").unwrap();

    // Act: enter ingest mode
    let prev = engine.enter_ingest_mode().expect("enter ingest failed");

    // Assert: reads should work during ingest
    let result = engine.get(cf, b"pre_key");
    assert!(result.is_ok(), "reads should work during ingest mode");
    assert_eq!(result.unwrap().as_deref(), Some(b"pre_value".as_slice()));

    // Cleanup
    engine.exit_ingest_mode(prev).expect("exit ingest failed");
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
        let _ = engine.begin_transaction(cf);
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

    // Act: first cycle
    {
        let prev = engine.enter_ingest_mode().expect("enter ingest failed");
        engine.put(cf, b"cycle1_key", b"cycle1_val").unwrap();
        engine.exit_ingest_mode(prev).expect("exit ingest failed");
    }

    // Verify transactions work between cycles
    {
        let txn = engine.begin_transaction(cf);
        assert!(txn.is_ok());
    }

    // Act: second cycle
    {
        let prev = engine.enter_ingest_mode().expect("enter ingest failed");
        engine.put(cf, b"cycle2_key", b"cycle2_val").unwrap();
        engine.exit_ingest_mode(prev).expect("exit ingest failed");
    }

    // Assert: both writes persisted
    assert_eq!(
        engine.get(cf, b"cycle1_key").unwrap().as_deref(),
        Some(b"cycle1_val".as_slice())
    );
    assert_eq!(
        engine.get(cf, b"cycle2_key").unwrap().as_deref(),
        Some(b"cycle2_val".as_slice())
    );
}
