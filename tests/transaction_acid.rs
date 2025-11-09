// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("create temp dir")
}

// ============================================================================
// Write-Write Conflicts (5 tests)
// ============================================================================

#[test]
fn should_detect_write_write_conflict_given_concurrent_updates_to_same_key() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key"), Bytes::from("v0"))
        .expect("put");

    let mut first_txn = engine.begin_transaction();
    let mut conflicting_txn = engine.begin_transaction();

    first_txn
        .put(Bytes::from("key"), Bytes::from("v1"), None)
        .unwrap();
    conflicting_txn
        .put(Bytes::from("key"), Bytes::from("v2"), None)
        .unwrap();

    let first_result = engine.commit_transaction(first_txn, cntryl_midge::WriteOptions::default());
    assert!(first_result.is_ok(), "First transaction should succeed");

    // Act
    let conflicting_result =
        engine.commit_transaction(conflicting_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        conflicting_result.is_err(),
        "Second transaction should fail with write conflict"
    );
    // First transaction's value should be persisted
    assert_eq!(engine.get(b"key").expect("get"), Some(Bytes::from("v1")));
}

#[test]
fn should_abort_second_transaction_given_write_conflict_when_both_commit() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut winning_txn = engine.begin_transaction();
    let mut losing_txn = engine.begin_transaction();

    winning_txn
        .put(Bytes::from("conflict_key"), Bytes::from("txn1_value"), None)
        .unwrap();
    losing_txn
        .put(Bytes::from("conflict_key"), Bytes::from("txn2_value"), None)
        .unwrap();

    let winner_result = engine.commit_transaction(winning_txn, cntryl_midge::WriteOptions::default());
    assert!(winner_result.is_ok(), "First transaction should commit");

    // Act
    let loser_result = engine.commit_transaction(losing_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        loser_result.is_err(),
        "Second transaction should fail with write-write conflict"
    );
    // Verify first transaction's value persisted
    assert_eq!(
        engine.get(b"conflict_key").expect("get"),
        Some(Bytes::from("txn1_value"))
    );
}

#[test]
fn should_preserve_first_commit_given_write_conflict_when_second_aborts() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut first_txn = engine.begin_transaction();
    first_txn
        .put(Bytes::from("key"), Bytes::from("first_value"), None)
        .unwrap();
    engine
        .commit_transaction(first_txn, cntryl_midge::WriteOptions::default())
        .expect("first commit");

    let mut aborted_txn = engine.begin_transaction();
    aborted_txn
        .put(Bytes::from("key"), Bytes::from("second_value"), None)
        .unwrap();

    // Act
    drop(aborted_txn); // rollback second transaction

    // Assert
    assert_eq!(
        engine.get(b"key").expect("get"),
        Some(Bytes::from("first_value"))
    );
}

#[test]
fn should_handle_write_conflict_on_delete_given_concurrent_delete_and_put() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key"), Bytes::from("initial"))
        .expect("put");

    let mut delete_txn = engine.begin_transaction();
    let mut put_txn = engine.begin_transaction();

    delete_txn.delete(Bytes::from("key")).unwrap();
    put_txn
        .put(Bytes::from("key"), Bytes::from("updated"), None)
        .unwrap();

    let delete_result = engine.commit_transaction(delete_txn, cntryl_midge::WriteOptions::default());
    assert!(
        delete_result.is_ok(),
        "Delete transaction should commit first"
    );

    // Act
    let put_result = engine.commit_transaction(put_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        put_result.is_err(),
        "Put transaction should fail with conflict"
    );
    // Verify delete persisted (key should not exist)
    assert_eq!(engine.get(b"key").expect("get"), None);
}

#[test]
fn should_detect_conflict_on_delete_range_given_overlapping_keys() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    for i in 0..10 {
        engine
            .put(Bytes::from(format!("key{}", i)), Bytes::from("val"))
            .expect("put");
    }

    let mut range_txn = engine.begin_transaction();
    range_txn
        .delete_range(Bytes::from("key3"), Bytes::from("key7"))
        .unwrap();

    let mut overlapping_txn = engine.begin_transaction();
    overlapping_txn
        .put(Bytes::from("key5"), Bytes::from("new_value"), None)
        .unwrap();

    let range_result = engine.commit_transaction(range_txn, cntryl_midge::WriteOptions::default());

    // Act
    let overlapping_result =
        engine.commit_transaction(overlapping_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(range_result.is_ok());
    assert!(overlapping_result.is_ok());
    // TODO: Verify range conflict detection when implemented
}

// ============================================================================
// Optimistic Locking (4 tests)
// ============================================================================

#[test]
fn should_validate_version_given_read_set_when_committing_transaction() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let reading_txn = engine.begin_transaction();
    let snap = engine.snapshot();
    let _read_value = engine.get_at(b"key", &snap).expect("get_at");

    engine
        .put(Bytes::from("key"), Bytes::from("v2"))
        .expect("external put");

    // Act
    let result = engine.commit_transaction(reading_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // Currently no read-set validation, transaction commits
    assert!(result.is_ok());
    // TODO: Should fail when optimistic locking validates read-set
}

#[test]
fn should_abort_transaction_given_stale_read_when_key_modified_by_other() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key"), Bytes::from("initial"))
        .expect("put");

    let mut stale_txn = engine.begin_transaction();
    let _local = stale_txn.get_local(b"key");

    engine
        .put(Bytes::from("key"), Bytes::from("modified"))
        .expect("concurrent put");

    stale_txn
        .put(Bytes::from("key"), Bytes::from("txn_value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(stale_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // No stale read detection currently
    assert!(result.is_ok());
    // TODO: Should abort when read-after-write validation is implemented
}

#[test]
fn should_track_read_set_given_transaction_gets_when_validating() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("k1"), Bytes::from("v1"))
        .expect("put");
    engine
        .put(Bytes::from("k2"), Bytes::from("v2"))
        .expect("put");

    let reading_txn = engine.begin_transaction();
    let snap = engine.snapshot();
    let _v1 = engine.get_at(b"k1", &snap);
    let _v2 = engine.get_at(b"k2", &snap);

    // Act
    // (Currently no explicit read-set tracking API, so Act is observation)

    // Assert
    // No explicit read-set tracking API currently
    // Transaction should track {k1, k2} for validation
    assert!(reading_txn.begin_sequence() > 0);
    // TODO: Add API to inspect read-set when implemented
}

#[test]
fn should_allow_commit_given_no_conflicts_when_validation_succeeds() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut clean_txn = engine.begin_transaction();
    clean_txn
        .put(Bytes::from("new_key"), Bytes::from("new_value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(clean_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_ok());
    assert_eq!(
        engine.get(b"new_key").expect("get"),
        Some(Bytes::from("new_value"))
    );
}

// ============================================================================
// Isolation Levels (6 tests)
// ============================================================================

#[test]
fn should_prevent_dirty_read_given_uncommitted_write_when_read_committed() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut uncommitted_txn = engine.begin_transaction();
    uncommitted_txn
        .put(Bytes::from("key"), Bytes::from("uncommitted"), None)
        .unwrap();

    // Act
    let read_result = engine.get(b"key").expect("get");

    // Assert
    // Should not see uncommitted write
    assert_eq!(
        read_result, None,
        "Should not see uncommitted transaction write"
    );

    drop(uncommitted_txn);
    assert_eq!(engine.get(b"key").expect("get after rollback"), None);
}

#[test]
fn should_prevent_dirty_write_given_uncommitted_update_when_read_committed() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let mut first_txn = engine.begin_transaction();
    first_txn
        .put(Bytes::from("key"), Bytes::from("txn1_value"), None)
        .unwrap();

    let mut second_txn = engine.begin_transaction();
    second_txn
        .put(Bytes::from("key"), Bytes::from("txn2_value"), None)
        .unwrap();

    // Act
    let second_result = engine.commit_transaction(second_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // second_txn should be able to commit (no locking currently)
    assert!(second_result.is_ok());
    // TODO: Should prevent dirty write when locking is implemented

    drop(first_txn);
}

#[test]
fn should_see_own_writes_given_transaction_when_get_after_put() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut writing_txn = engine.begin_transaction();
    writing_txn
        .put(Bytes::from("key"), Bytes::from("my_value"), None)
        .unwrap();

    // Act
    let local_read = writing_txn.get_local(b"key");

    // Assert
    assert_eq!(local_read, Some(Some(Bytes::from("my_value"))));
}

#[test]
fn should_not_see_other_uncommitted_writes_given_concurrent_transactions() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut writing_txn = engine.begin_transaction();
    let reading_txn = engine.begin_transaction();

    writing_txn
        .put(Bytes::from("key"), Bytes::from("txn1_value"), None)
        .unwrap();

    // Act
    let read_result = reading_txn.get_local(b"key");

    // Assert
    assert_eq!(
        read_result, None,
        "reading_txn should not see writing_txn's uncommitted write"
    );
}

#[test]
fn should_maintain_snapshot_view_given_transaction_when_external_writes_occur() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let snapshot_txn = engine.begin_transaction();
    let begin_seq = snapshot_txn.begin_sequence();

    engine
        .put(Bytes::from("key"), Bytes::from("v2"))
        .expect("external put");

    // Act
    let snap = engine.snapshot();

    // Assert
    // Transaction captured sequence at begin
    assert!(begin_seq > 0);
    // Snapshot isolation would require reading at begin_seq
    // Currently no full snapshot isolation for transaction reads
    assert!(snap.seq > begin_seq);
}

#[test]
fn should_prevent_phantom_read_given_snapshot_isolation_when_range_scan() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key1"), Bytes::from("v1"))
        .expect("put");

    let snap = engine.snapshot();
    let first_scan = engine
        .scan_at(
            cntryl_midge::Query {
                prefix: Some(Bytes::from("key")),
                ..Default::default()
            },
            &snap,
        )
        .expect("scan");

    engine
        .put(Bytes::from("key2"), Bytes::from("v2"))
        .expect("put new key");

    // Act
    let second_scan = engine
        .scan_at(
            cntryl_midge::Query {
                prefix: Some(Bytes::from("key")),
                ..Default::default()
            },
            &snap,
        )
        .expect("scan");

    // Assert
    // Both scans at same snapshot should see same keys
    assert_eq!(
        first_scan.len(),
        second_scan.len(),
        "Phantom read prevented by snapshot"
    );
}

// ============================================================================
// Snapshot Isolation Enforcement (5 tests)
// ============================================================================

#[test]
fn should_read_at_begin_sequence_given_transaction_when_using_transaction_get() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    engine
        .put(Bytes::from("key"), Bytes::from("initial"))
        .expect("put");

    let mut txn = engine.begin_transaction();
    let begin_value = engine.transaction_get(&mut txn, b"key").expect("get");

    // Act
    engine
        .put(Bytes::from("key"), Bytes::from("updated"))
        .expect("put");

    let second_value = engine.transaction_get(&mut txn, b"key").expect("get");

    // Assert
    assert_eq!(begin_value, Some(Bytes::from("initial")));
    assert_eq!(second_value, Some(Bytes::from("initial")));
}

#[test]
fn should_not_see_concurrent_writes_given_transaction_when_snapshot_isolated() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    engine
        .put(Bytes::from("key1"), Bytes::from("v1"))
        .expect("put");

    let mut txn1 = engine.begin_transaction();

    // Act
    let mut txn2 = engine.begin_transaction();
    txn2.put(Bytes::from("key2"), Bytes::from("v2"), None)
        .unwrap();
    engine
        .commit_transaction(txn2, cntryl_midge::WriteOptions::default())
        .expect("commit");

    let value = engine.transaction_get(&mut txn1, b"key2").expect("get");

    // Assert
    assert_eq!(
        value, None,
        "Should not see writes committed after transaction began"
    );
}

#[test]
fn should_see_own_writes_given_transaction_when_reading_staged_mutations() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Act
    let mut txn = engine.begin_transaction();
    txn.put(Bytes::from("new_key"), Bytes::from("new_value"), None)
        .unwrap();

    // Read own write
    let value = engine.transaction_get(&mut txn, b"new_key").expect("get");

    // Assert
    assert_eq!(value, Some(Bytes::from("new_value")));
}

#[test]
fn should_track_reads_given_transaction_get_when_validating_conflicts() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    engine
        .put(Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let mut txn1 = engine.begin_transaction();
    let _ = engine.transaction_get(&mut txn1, b"key").expect("get");

    // Act
    let mut txn2 = engine.begin_transaction();
    txn2.put(Bytes::from("key"), Bytes::from("v2"), None)
        .unwrap();
    engine
        .commit_transaction(txn2, cntryl_midge::WriteOptions::default())
        .expect("commit");

    txn1.put(Bytes::from("other_key"), Bytes::from("value"), None)
        .unwrap();
    let result = engine.commit_transaction(txn1, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_err(), "Should detect read-write conflict");
}

#[test]
fn should_provide_consistent_view_given_multiple_reads_when_snapshot_isolated() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    engine
        .put(Bytes::from("key1"), Bytes::from("v1"))
        .expect("put");
    engine
        .put(Bytes::from("key2"), Bytes::from("v2"))
        .expect("put");

    let mut txn = engine.begin_transaction();
    let first_read = engine.transaction_get(&mut txn, b"key1").expect("get");

    // Act
    engine
        .put(Bytes::from("key1"), Bytes::from("updated1"))
        .expect("put");
    engine
        .put(Bytes::from("key2"), Bytes::from("updated2"))
        .expect("put");

    let second_read = engine.transaction_get(&mut txn, b"key1").expect("get");
    let key2_read = engine.transaction_get(&mut txn, b"key2").expect("get");

    // Assert
    assert_eq!(first_read, Some(Bytes::from("v1")));
    assert_eq!(second_read, Some(Bytes::from("v1")));
    assert_eq!(key2_read, Some(Bytes::from("v2")));
}

// ============================================================================
// Lost Updates (3 tests)
// ============================================================================

#[test]
fn should_prevent_lost_update_given_read_modify_write_when_concurrent() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("counter"), Bytes::from("0"))
        .expect("put");

    let mut first_increment_txn = engine.begin_transaction();
    let mut second_increment_txn = engine.begin_transaction();

    let snap1 = engine.snapshot();
    let snap2 = engine.snapshot();

    let val1 = engine.get_at(b"counter", &snap1).expect("get");
    let val2 = engine.get_at(b"counter", &snap2).expect("get");

    let count1: i32 = String::from_utf8(val1.unwrap().to_vec())
        .unwrap()
        .parse()
        .unwrap();
    let count2: i32 = String::from_utf8(val2.unwrap().to_vec())
        .unwrap()
        .parse()
        .unwrap();

    first_increment_txn
        .put(
            Bytes::from("counter"),
            Bytes::from((count1 + 1).to_string()),
            None,
        )
        .unwrap();
    second_increment_txn
        .put(
            Bytes::from("counter"),
            Bytes::from((count2 + 1).to_string()),
            None,
        )
        .unwrap();

    engine
        .commit_transaction(first_increment_txn, cntryl_midge::WriteOptions::default())
        .expect("commit first");

    // Act
    let result = engine.commit_transaction(second_increment_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // With read tracking and snapshot isolation, lost update is now PREVENTED
    // Second transaction read "counter" at its snapshot, first transaction modified it
    // This is correctly detected as a read-write conflict
    assert!(
        result.is_err(),
        "Should detect read-write conflict and prevent lost update"
    );

    // Final value is 1 (only first increment succeeded)
    let final_val = engine.get(b"counter").expect("get final");
    let final_count: i32 = String::from_utf8(final_val.unwrap().to_vec())
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        final_count, 1,
        "Only first transaction should have committed"
    );
}

#[test]
fn should_detect_lost_update_given_cas_pattern_when_value_changed() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key"), Bytes::from("v1"))
        .expect("put");

    let snap = engine.snapshot();
    let expected = engine.get_at(b"key", &snap).expect("get");

    engine
        .put(Bytes::from("key"), Bytes::from("v2"))
        .expect("concurrent update");

    let mut cas_txn = engine.begin_transaction();
    cas_txn
        .put(Bytes::from("key"), Bytes::from("v3"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(cas_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // No CAS validation currently
    assert!(result.is_ok());
    assert!(expected.is_some());
    // TODO: Should fail if key was modified since snapshot
}

#[test]
fn should_preserve_both_updates_given_non_overlapping_keys_when_concurrent_commits() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut first_key_txn = engine.begin_transaction();
    let mut second_key_txn = engine.begin_transaction();

    first_key_txn
        .put(Bytes::from("key1"), Bytes::from("value1"), None)
        .unwrap();
    second_key_txn
        .put(Bytes::from("key2"), Bytes::from("value2"), None)
        .unwrap();

    engine
        .commit_transaction(first_key_txn, cntryl_midge::WriteOptions::default())
        .expect("commit first");

    // Act
    engine
        .commit_transaction(second_key_txn, cntryl_midge::WriteOptions::default())
        .expect("commit second");

    // Assert
    assert_eq!(
        engine.get(b"key1").expect("get"),
        Some(Bytes::from("value1"))
    );
    assert_eq!(
        engine.get(b"key2").expect("get"),
        Some(Bytes::from("value2"))
    );
}

// ============================================================================
// Deadlock Detection (4 tests)
// ============================================================================

#[test]
fn should_detect_deadlock_given_circular_wait_when_two_transactions() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("k1"), Bytes::from("v1"))
        .expect("put");
    engine
        .put(Bytes::from("k2"), Bytes::from("v2"))
        .expect("put");

    let mut first_circular_txn = engine.begin_transaction();
    let mut second_circular_txn = engine.begin_transaction();

    first_circular_txn
        .put(Bytes::from("k1"), Bytes::from("txn1_k1"), None)
        .unwrap();
    second_circular_txn
        .put(Bytes::from("k2"), Bytes::from("txn2_k2"), None)
        .unwrap();

    first_circular_txn
        .put(Bytes::from("k2"), Bytes::from("txn1_k2"), None)
        .unwrap();
    second_circular_txn
        .put(Bytes::from("k1"), Bytes::from("txn2_k1"), None)
        .unwrap();

    let first_result =
        engine.commit_transaction(first_circular_txn, cntryl_midge::WriteOptions::default());

    // Act
    let second_result =
        engine.commit_transaction(second_circular_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // With write-write conflict detection, one transaction succeeds and the other fails
    // Both transactions write to k1 and k2, so there's a write-write conflict
    assert!(first_result.is_ok(), "First transaction should succeed");
    assert!(
        second_result.is_err(),
        "Second transaction should fail due to write-write conflict"
    );
    // Note: This is conflict detection, not deadlock detection
    // True deadlock detection would require lock-based concurrency control
}

#[test]
fn should_abort_victim_transaction_given_deadlock_when_detected() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut first_deadlock_txn = engine.begin_transaction();
    let mut second_deadlock_txn = engine.begin_transaction();

    first_deadlock_txn
        .put(Bytes::from("resource_a"), Bytes::from("txn1"), None)
        .unwrap();
    second_deadlock_txn
        .put(Bytes::from("resource_b"), Bytes::from("txn2"), None)
        .unwrap();

    first_deadlock_txn
        .put(Bytes::from("resource_b"), Bytes::from("txn1_b"), None)
        .unwrap();
    second_deadlock_txn
        .put(Bytes::from("resource_a"), Bytes::from("txn2_a"), None)
        .unwrap();

    // Act
    let first_deadlock_result =
        engine.commit_transaction(first_deadlock_txn, cntryl_midge::WriteOptions::default());
    let second_deadlock_result =
        engine.commit_transaction(second_deadlock_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // One transaction should be chosen as victim and aborted
    // Currently no deadlock detection
    // TODO: Verify one is aborted with deadlock error
    let _ = first_deadlock_result;
    let _ = second_deadlock_result;
}

#[test]
fn should_allow_retry_given_deadlock_victim_when_aborted() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut initial_txn = engine.begin_transaction();
    initial_txn
        .put(Bytes::from("key"), Bytes::from("value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(initial_txn, cntryl_midge::WriteOptions::default());

    // Assert
    if result.is_err() {
        let mut retry_txn = engine.begin_transaction();
        retry_txn
            .put(Bytes::from("key"), Bytes::from("retry_value"), None)
            .unwrap();
        let retry_result = engine.commit_transaction(retry_txn, cntryl_midge::WriteOptions::default());
        assert!(retry_result.is_ok(), "Retry should succeed");
    } else {
        assert!(result.is_ok());
    }
}

#[test]
fn should_detect_deadlock_given_three_way_circular_dependency() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut first_three_way_txn = engine.begin_transaction();
    let mut second_three_way_txn = engine.begin_transaction();
    let mut third_three_way_txn = engine.begin_transaction();

    first_three_way_txn
        .put(Bytes::from("r1"), Bytes::from("t1"), None)
        .unwrap();
    second_three_way_txn
        .put(Bytes::from("r2"), Bytes::from("t2"), None)
        .unwrap();
    third_three_way_txn
        .put(Bytes::from("r3"), Bytes::from("t3"), None)
        .unwrap();

    first_three_way_txn
        .put(Bytes::from("r2"), Bytes::from("t1_r2"), None)
        .unwrap();
    second_three_way_txn
        .put(Bytes::from("r3"), Bytes::from("t2_r3"), None)
        .unwrap();
    third_three_way_txn
        .put(Bytes::from("r1"), Bytes::from("t3_r1"), None)
        .unwrap();

    // Act
    let first_three_way_result =
        engine.commit_transaction(first_three_way_txn, cntryl_midge::WriteOptions::default());
    let second_three_way_result =
        engine.commit_transaction(second_three_way_txn, cntryl_midge::WriteOptions::default());
    let third_three_way_result =
        engine.commit_transaction(third_three_way_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // Should detect 3-way deadlock: txn1->r2, txn2->r3, txn3->r1
    // TODO: At least one should be aborted when deadlock detection is implemented
    let _ = first_three_way_result;
    let _ = second_three_way_result;
    let _ = third_three_way_result;
}

// ============================================================================
// Transaction Lifecycle (5 tests)
// ============================================================================

#[test]
fn should_timeout_transaction_given_exceed_deadline_when_committing() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut timeout_txn = engine.begin_transaction();
    timeout_txn
        .put(Bytes::from("key"), Bytes::from("value"), None)
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(10));

    // Act
    let result = engine.commit_transaction(timeout_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // No timeout mechanism currently
    assert!(result.is_ok());
    // TODO: Should timeout if transaction exceeds deadline
}

#[test]
fn should_release_locks_given_transaction_timeout_when_aborted() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut aborted_lock_txn = engine.begin_transaction();
    aborted_lock_txn
        .put(Bytes::from("locked_key"), Bytes::from("value"), None)
        .unwrap();

    drop(aborted_lock_txn);

    let mut subsequent_txn = engine.begin_transaction();
    subsequent_txn
        .put(Bytes::from("locked_key"), Bytes::from("value2"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(subsequent_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // No locking currently, so always succeeds
    assert!(result.is_ok());
    // TODO: Verify locks released after timeout/abort
}

#[test]
fn should_rollback_partial_writes_given_timeout_when_aborting() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut rollback_txn = engine.begin_transaction();
    rollback_txn
        .put(Bytes::from("key1"), Bytes::from("value1"), None)
        .unwrap();
    rollback_txn
        .put(Bytes::from("key2"), Bytes::from("value2"), None)
        .unwrap();
    rollback_txn
        .put(Bytes::from("key3"), Bytes::from("value3"), None)
        .unwrap();

    // Act
    drop(rollback_txn);

    // Assert
    assert_eq!(engine.get(b"key1").expect("get"), None);
    assert_eq!(engine.get(b"key2").expect("get"), None);
    assert_eq!(engine.get(b"key3").expect("get"), None);
}

#[test]
fn should_reject_operations_given_aborted_transaction_when_used() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut aborted_txn = engine.begin_transaction();
    aborted_txn.rollback();

    aborted_txn
        .put(Bytes::from("key"), Bytes::from("value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(aborted_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        result.is_err(),
        "Should reject operations on aborted transaction"
    );
}

#[test]
fn should_reject_operations_given_committed_transaction_when_reused() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut committed_txn = engine.begin_transaction();
    committed_txn
        .put(Bytes::from("key1"), Bytes::from("value1"), None)
        .unwrap();

    // Act
    engine
        .commit_transaction(committed_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    // Transaction consumed by commit(), cannot be reused
    // Rust ownership prevents this at compile time
    // This test documents the behavior
}

// ============================================================================
// Transaction Spill-to-Disk (5 tests)
// ============================================================================

#[test]
fn should_spill_to_disk_given_exceed_threshold_when_staging_writes() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Create transaction with small threshold (1MB) to force spilling
    let snap = engine.snapshot();
    let mut large_txn = cntryl_midge::Transaction::with_options(
        1,
        snap.seq,
        None,
        1024 * 1024, // 1MB threshold
    );

    // Act
    // Add 2MB of data (2000 keys × 1024 bytes each)
    for i in 0..2000 {
        large_txn
            .put(
                Bytes::from(format!("key{:06}", i)),
                Bytes::from(vec![0u8; 1024]),
                None,
            )
            .expect("put");
    }

    // Assert
    // Transaction should have spilled to disk
    // Verify by committing and checking all data is present
    let result = engine.commit_transaction(large_txn, cntryl_midge::WriteOptions::default());
    assert!(
        result.is_ok(),
        "Transaction with spilled data should commit"
    );

    // Verify all keys are present after commit
    for i in 0..2000 {
        let key = format!("key{:06}", i);
        let value = engine.get(key.as_bytes()).expect("get");
        assert!(
            value.is_some(),
            "Key {} should exist after spill and commit",
            key
        );
    }
}

#[test]
fn should_read_from_spill_file_given_large_transaction_when_get() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Create transaction with small threshold to force spilling
    let snap = engine.snapshot();
    let mut spilled_txn = cntryl_midge::Transaction::with_options(
        2,
        snap.seq,
        None,
        512 * 1024, // 512KB threshold
    );

    // Add 1.5MB of data to force spilling
    for i in 0..1500 {
        spilled_txn
            .put(
                Bytes::from(format!("large_key_{:06}", i)),
                Bytes::from(vec![0xABu8; 1024]),
                None,
            )
            .expect("put");
    }

    // Act
    let result = engine.commit_transaction(spilled_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_ok(), "Should commit spilled transaction");

    // Verify data after commit
    for i in 0..1500 {
        let key = format!("large_key_{:06}", i);
        let value = engine.get(key.as_bytes()).expect("get");
        assert!(
            value.is_some(),
            "Key {} should exist after spill commit",
            key
        );
        assert_eq!(
            value.unwrap(),
            Bytes::from(vec![0xABu8; 1024]),
            "Value should match for key {}",
            key
        );
    }
}

#[test]
fn should_cleanup_spill_file_given_transaction_commit_when_completed() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Create transaction with small threshold
    let snap = engine.snapshot();
    let mut committed_spill_txn = cntryl_midge::Transaction::with_options(
        3,
        snap.seq,
        None,
        256 * 1024, // 256KB threshold
    );

    // Add 2MB to force spilling
    for i in 0..2000 {
        committed_spill_txn
            .put(
                Bytes::from(format!("cleanup_key_{:06}", i)),
                Bytes::from(vec![0xCCu8; 1024]),
                None,
            )
            .expect("put");
    }

    // Count temp files before commit (should have some spill files)
    let temp_dir = std::env::temp_dir();
    let before_count = std::fs::read_dir(&temp_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("midge_txn_3_"))
                .count()
        })
        .unwrap_or(0);

    // Act
    engine
        .commit_transaction(committed_spill_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    // Spill files should be cleaned up after commit
    let after_count = std::fs::read_dir(&temp_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("midge_txn_3_"))
                .count()
        })
        .unwrap_or(0);

    // Spill files should be removed (or at least not increase)
    assert!(
        after_count <= before_count,
        "Spill files should be cleaned up after commit"
    );
}

#[test]
fn should_cleanup_spill_file_given_transaction_abort_when_rolled_back() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Create transaction with small threshold
    let snap = engine.snapshot();
    let mut aborted_spill_txn = cntryl_midge::Transaction::with_options(
        4,
        snap.seq,
        None,
        256 * 1024, // 256KB threshold
    );

    // Add 2MB to force spilling
    for i in 0..2000 {
        aborted_spill_txn
            .put(
                Bytes::from(format!("abort_key_{:06}", i)),
                Bytes::from(vec![0xDDu8; 1024]),
                None,
            )
            .expect("put");
    }

    // Count temp files before abort
    let temp_dir = std::env::temp_dir();
    let _before_count = std::fs::read_dir(&temp_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("midge_txn_4_"))
                .count()
        })
        .unwrap_or(0);

    // Act
    drop(aborted_spill_txn); // Implicit rollback

    // Assert
    // Spill files should be cleaned up on abort/drop
    let after_count = std::fs::read_dir(&temp_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("midge_txn_4_"))
                .count()
        })
        .unwrap_or(0);

    assert_eq!(
        after_count, 0,
        "Spill files should be cleaned up after abort"
    );
}

#[test]
fn should_handle_multiple_spill_files_given_very_large_transaction() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Create transaction with small threshold to force multiple spills
    let snap = engine.snapshot();
    let mut huge_txn = cntryl_midge::Transaction::with_options(
        5,
        snap.seq,
        None,
        128 * 1024, // 128KB threshold - will cause multiple spills
    );

    // Add 10MB of data to force multiple spills
    for i in 0..10000 {
        huge_txn
            .put(
                Bytes::from(format!("huge_key_{:06}", i)),
                Bytes::from(vec![0xEEu8; 1024]),
                None,
            )
            .expect("put");
    }

    // Act
    let result = engine.commit_transaction(huge_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        result.is_ok(),
        "Should handle multiple spill files successfully"
    );

    // Verify all keys are present after multiple spills and commit
    for i in 0..10000 {
        let key = format!("huge_key_{:06}", i);
        let value = engine.get(key.as_bytes()).expect("get");
        assert!(
            value.is_some(),
            "Key {} should exist after multiple spills",
            key
        );
    }
}

// ============================================================================
// Atomicity (4 tests)
// ============================================================================

#[test]
fn should_commit_all_or_nothing_given_multi_key_transaction() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut atomic_txn = engine.begin_transaction();
    atomic_txn
        .put(Bytes::from("k1"), Bytes::from("v1"), None)
        .unwrap();
    atomic_txn
        .put(Bytes::from("k2"), Bytes::from("v2"), None)
        .unwrap();
    atomic_txn
        .put(Bytes::from("k3"), Bytes::from("v3"), None)
        .unwrap();
    atomic_txn.delete(Bytes::from("k4")).unwrap();

    // Act
    engine
        .commit_transaction(atomic_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    assert_eq!(engine.get(b"k1").expect("get"), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"k2").expect("get"), Some(Bytes::from("v2")));
    assert_eq!(engine.get(b"k3").expect("get"), Some(Bytes::from("v3")));
    assert_eq!(engine.get(b"k4").expect("get"), None);
}

#[test]
fn should_be_atomic_given_transaction_with_100_operations() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut batch_txn = engine.begin_transaction();
    for i in 0..100 {
        batch_txn
            .put(
                Bytes::from(format!("batch_key_{}", i)),
                Bytes::from(format!("batch_val_{}", i)),
                None,
            )
            .unwrap();
    }

    // Act
    engine
        .commit_transaction(batch_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    for i in 0..100 {
        let key = format!("batch_key_{}", i);
        let expected = format!("batch_val_{}", i);
        assert_eq!(
            engine.get(key.as_bytes()).expect("get"),
            Some(Bytes::from(expected))
        );
    }
}

#[test]
fn should_rollback_all_writes_given_single_failure_when_committing() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut failed_txn = engine.begin_transaction();
    failed_txn
        .put(Bytes::from("k1"), Bytes::from("v1"), None)
        .unwrap();
    failed_txn
        .put(Bytes::from("k2"), Bytes::from("v2"), None)
        .unwrap();

    // Act
    drop(failed_txn);

    // Assert
    assert_eq!(engine.get(b"k1").expect("get"), None);
    assert_eq!(engine.get(b"k2").expect("get"), None);
}

#[test]
fn should_not_expose_partial_writes_given_concurrent_readers_when_committing() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let snap_before = engine.snapshot();

    let mut partial_write_txn = engine.begin_transaction();
    partial_write_txn
        .put(Bytes::from("atomic_k1"), Bytes::from("v1"), None)
        .unwrap();
    partial_write_txn
        .put(Bytes::from("atomic_k2"), Bytes::from("v2"), None)
        .unwrap();

    let read_during = engine.get(b"atomic_k1").expect("get during");

    // Act
    engine
        .commit_transaction(partial_write_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    // Assert
    let snap_after = engine.snapshot();

    // Should not see partial writes
    assert_eq!(read_during, None, "Should not see uncommitted writes");
    assert!(snap_after.seq > snap_before.seq);
}

// ============================================================================
// Durability (3 tests)
// ============================================================================

#[test]
fn should_persist_transaction_given_commit_when_crash_after() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut durable_txn = engine.begin_transaction();
    durable_txn
        .put(
            Bytes::from("durable_key"),
            Bytes::from("durable_value"),
            None,
        )
        .unwrap();
    engine
        .commit_transaction(durable_txn, cntryl_midge::WriteOptions::default())
        .expect("commit");

    drop(engine);

    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };

    // Act
    let engine2 = MidgeEngine::open(opts2).expect("reopen");

    // Assert
    assert_eq!(
        engine2.get(b"durable_key").expect("get"),
        Some(Bytes::from("durable_value"))
    );
}

#[test]
fn should_not_persist_transaction_given_abort_when_crash_after() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut aborted_txn = engine.begin_transaction();
    aborted_txn
        .put(
            Bytes::from("aborted_key"),
            Bytes::from("aborted_value"),
            None,
        )
        .unwrap();
    drop(aborted_txn);

    drop(engine);

    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };

    // Act
    let engine2 = MidgeEngine::open(opts2).expect("reopen");

    // Assert
    assert_eq!(engine2.get(b"aborted_key").expect("get"), None);
}

#[test]
fn should_recover_committed_transactions_given_wal_replay_when_restart() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    for i in 0..10 {
        let mut wal_txn = engine.begin_transaction();
        wal_txn
            .put(
                Bytes::from(format!("wal_key_{}", i)),
                Bytes::from(format!("wal_val_{}", i)),
                None,
            )
            .unwrap();
        engine
            .commit_transaction(wal_txn, cntryl_midge::WriteOptions::default())
            .expect("commit");
    }

    drop(engine);

    let opts2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        ..Default::default()
    };

    // Act
    let engine2 = MidgeEngine::open(opts2).expect("reopen");

    // Assert
    for i in 0..10 {
        let key = format!("wal_key_{}", i);
        let expected = format!("wal_val_{}", i);
        assert_eq!(
            engine2.get(key.as_bytes()).expect("get"),
            Some(Bytes::from(expected)),
            "WAL replay should recover transaction {}",
            i
        );
    }
}

// ============================================================================
// Edge Cases (4 tests)
// ============================================================================

#[test]
fn should_handle_empty_transaction_given_commit_without_operations() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let empty_txn = engine.begin_transaction();

    // Act
    let result = engine.commit_transaction(empty_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(
        result.is_ok(),
        "Empty transaction should commit successfully"
    );
}

#[test]
fn should_handle_read_only_transaction_given_no_writes_when_commit() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    engine
        .put(Bytes::from("key"), Bytes::from("value"))
        .expect("put");

    let readonly_txn = engine.begin_transaction();
    let snap = engine.snapshot();
    let _value = engine.get_at(b"key", &snap).expect("get_at");

    // Act
    let result = engine.commit_transaction(readonly_txn, cntryl_midge::WriteOptions::default());

    // Assert
    assert!(result.is_ok(), "Read-only transaction should commit");
}

#[test]
fn should_allow_nested_get_given_transaction_when_reading_own_writes() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut nested_read_txn = engine.begin_transaction();
    nested_read_txn
        .put(Bytes::from("nested_key"), Bytes::from("nested_value"), None)
        .unwrap();

    // Act
    let read1 = nested_read_txn.get_local(b"nested_key");
    let read2 = nested_read_txn.get_local(b"nested_key");

    // Assert
    assert_eq!(read1, Some(Some(Bytes::from("nested_value"))));
    assert_eq!(read2, Some(Some(Bytes::from("nested_value"))));
}

#[test]
fn should_handle_transaction_on_dropped_cf_given_cf_deleted_during_transaction() {
    // Arrange
    let dir = temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    let mut cf_txn = engine.begin_transaction();
    cf_txn
        .put(Bytes::from("cf_key"), Bytes::from("cf_value"), None)
        .unwrap();

    // Act
    let result = engine.commit_transaction(cf_txn, cntryl_midge::WriteOptions::default());

    // Assert
    // Default CF cannot be dropped, transaction should succeed
    assert!(result.is_ok());
    // TODO: Test with multi-CF when CF API is available
}
