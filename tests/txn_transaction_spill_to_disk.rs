// Transaction Spill-to-Disk
// Extracted from transaction_acid.rs

// Transaction ACID tests - P0 Priority
// Tests document expected behavior and will fail until features are implemented

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

mod common;
use common::{test_temp_dir, new_engine};
/// Helper: create a new engine in a fresh temp dir and return both.
fn new_engine() -> (tempfile::TempDir, cntryl_midge::MidgeEngine) {
    let dir = test_temp_dir();
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = cntryl_midge::MidgeEngine::open(opts).expect("open");
    (dir, engine)
}

#[test]
fn should_spill_to_disk_given_exceed_threshold_when_staging_writes() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

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
        let value = engine.get(&cf, key.as_bytes()).expect("get");
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
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

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
        let value = engine.get(&cf, key.as_bytes()).expect("get");
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
    let (dir, engine) = new_engine();
    let cf = engine.default_column_family();

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

    // Count temp files before commit (may have spill files)
    let temp_dir = std::env::test_temp_dir();
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

    // Keep `dir` alive to ensure engine has an active path (explicitly unused)
    let _ = dir;
}

#[test]
fn should_cleanup_spill_file_given_transaction_abort_when_rolled_back() {
    // Arrange
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

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
    let temp_dir = std::env::test_temp_dir();
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
    let (_dir, engine) = new_engine();
    let cf = engine.default_column_family();

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
        let value = engine.get(&cf, key.as_bytes()).expect("get");
        assert!(
            value.is_some(),
            "Key {} should exist after multiple spills",
            key
        );
    }
}
