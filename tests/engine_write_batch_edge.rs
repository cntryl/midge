mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, WriteBatch, WalRecoveryMode, test_hooks::{TestHooks, WalBehavior}};
use common::test_temp_dir;

// Phase 1 WriteBatch Remaining Atomicity Edge Tests

#[test]
fn should_recover_batch_atomically_given_crash_during_wal_write() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks),
        ..Default::default()
    };

    // Act - write batch with simulated WAL crash
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        let mut batch = WriteBatch::new();
        batch.put(cf.id(), Bytes::from_static(b"batch_key1"), Bytes::from_static(b"batch_val1"));
        batch.put(cf.id(), Bytes::from_static(b"batch_key2"), Bytes::from_static(b"batch_val2"));
        batch.put(cf.id(), Bytes::from_static(b"batch_key3"), Bytes::from_static(b"batch_val3"));
        let _ = eng.write_batch(&batch);
    }

    // Assert - reopen and verify atomicity (all or none)
    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts_reopen).expect("reopen");
    let cf = eng.default_column_family();
    
    let key1 = eng.get(&cf, b"batch_key1").expect("get key1");
    let key2 = eng.get(&cf, b"batch_key2").expect("get key2");
    let key3 = eng.get(&cf, b"batch_key3").expect("get key3");
    
    // Atomicity: if one key exists, all should exist (or all should be None)
    if key1.is_some() {
        assert!(key2.is_some() && key3.is_some(), "Batch should be atomic");
    } else {
        assert!(key2.is_none() && key3.is_none(), "Batch should be atomic");
    }
}

#[test]
fn should_commit_all_or_nothing_given_large_batch_when_crash_simulated() {
    // Arrange
    let dir = test_temp_dir();
    let hooks = TestHooks::new().with_wal_behavior(WalBehavior::TruncateAfterWrite);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: Some(hooks),
        ..Default::default()
    };

    // Act - large batch
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        let mut batch = WriteBatch::new();
        for i in 0..100 {
            let key = format!("large_batch_{:03}", i);
            batch.put(cf.id(), Bytes::from(key.into_bytes()), Bytes::from_static(b"value"));
        }
        let _ = eng.write_batch(&batch);
    }

    // Assert - verify atomicity after recovery
    let opts_reopen = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
        test_hooks: None,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts_reopen).expect("reopen");
    let cf = eng.default_column_family();
    
    let first = eng.get(&cf, b"large_batch_000").expect("get first");
    let last = eng.get(&cf, b"large_batch_099").expect("get last");
    
    // Atomicity check
    assert_eq!(first.is_some(), last.is_some(), "Large batch should be atomic");
}

#[test]
fn should_maintain_consistency_given_batch_and_regular_write_concurrent() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: dir.path().to_path_buf() },
        wal_sync: true,
        ..Default::default()
    };

    // Act - interleave batch and regular writes
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    
    // Regular write
    eng.put(&cf, b"regular1", b"val1").expect("put regular1");
    
    // Batch write
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"batch1"), Bytes::from_static(b"bval1"));
    batch.put(cf.id(), Bytes::from_static(b"batch2"), Bytes::from_static(b"bval2"));
    eng.write_batch(&batch).expect("write batch");
    
    // Another regular write
    eng.put(&cf, b"regular2", b"val2").expect("put regular2");

    // Assert - all should be present
    assert!(eng.get(&cf, b"regular1").expect("get").is_some());
    assert!(eng.get(&cf, b"batch1").expect("get").is_some());
    assert!(eng.get(&cf, b"batch2").expect("get").is_some());
    assert!(eng.get(&cf, b"regular2").expect("get").is_some());
}

#[test]
#[ignore] // Requires disk full simulation infrastructure
fn should_propagate_error_given_disk_full_when_writing_batch() {
    // TODO: Implement when disk full simulation layer available
}

#[test]
#[ignore] // Requires explicit error injection in write path
fn should_rollback_batch_given_write_error() {
    // TODO: Implement when write error injection available
}
