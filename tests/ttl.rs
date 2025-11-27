//! TTL (Time-To-Live) Integration Tests
//!
//! Tests for TTL functionality:
//! - Key expiration based on TTL
//! - TTL persistence across restarts
//! - TTL with compaction cleanup
//! - TTL with snapshots
//! - TTL in transactions and write batches
//!
//! ## Coverage
//! - put_with_ttl / insert_with_ttl
//! - TTL metadata in WAL and SST
//! - Compaction-based expiration cleanup
//! - Snapshot visibility of expired keys

mod common;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::{test_temp_dir, with_engine_restart};
use std::thread;
use std::time::Duration;

// ============================================================================
// Basic TTL Expiration Tests
// ============================================================================

#[test]
fn should_return_value_given_ttl_not_elapsed_when_reading() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act - write with long TTL
    engine
        .put_with_ttl(&cf, b"session:123", b"session_data", 3600) // 1 hour TTL
        .expect("put_with_ttl");

    // Assert - value should be readable immediately
    let result = engine.get(&cf, b"session:123").expect("get");
    assert_eq!(
        result,
        Some(Bytes::from_static(b"session_data")),
        "Value should be readable before TTL expires"
    );
}

#[test]
fn should_return_none_given_ttl_elapsed_when_reading() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act - write with very short TTL
    engine
        .put_with_ttl(&cf, b"ephemeral:key", b"temp_data", 1) // 1 second TTL
        .expect("put_with_ttl");

    // Wait for TTL to expire
    thread::sleep(Duration::from_secs(2));

    // Assert - value should be expired
    let result = engine.get(&cf, b"ephemeral:key").expect("get");
    assert!(
        result.is_none(),
        "Value should not be readable after TTL expires"
    );
}

#[test]
fn should_expire_key_given_zero_ttl_means_no_expiration_when_reading() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act - write with TTL=0 (no expiration)
    engine
        .put_with_ttl(&cf, b"permanent:key", b"permanent_data", 0)
        .expect("put_with_ttl");

    // Assert - value should persist (0 means no TTL)
    let result = engine.get(&cf, b"permanent:key").expect("get");
    assert_eq!(
        result,
        Some(Bytes::from_static(b"permanent_data")),
        "TTL=0 should mean no expiration"
    );
}

// ============================================================================
// TTL Persistence Tests
// ============================================================================

#[test]
fn should_persist_ttl_metadata_given_restart_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        ..Default::default()
    };

    // Act - write with TTL, close, reopen
    with_engine_restart(
        opts.clone(),
        |eng| {
            let cf = eng.default_column_family();
            eng.put_with_ttl(&cf, b"persist:key", b"persist_value", 3600)
                .expect("put_with_ttl");
        },
        |eng| {
            // Assert - TTL should be preserved after restart
            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"persist:key").expect("get");
            assert!(
                result.is_some(),
                "Key with non-expired TTL should survive restart"
            );
        },
    );
}

#[test]
fn should_expire_after_restart_given_ttl_elapsed_during_shutdown_when_reopening() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        wal_sync: true,
        ..Default::default()
    };

    // Write with very short TTL
    {
        let engine = MidgeEngine::open(opts.clone()).expect("open");
        let cf = engine.default_column_family();
        engine
            .put_with_ttl(&cf, b"expire:key", b"expire_value", 1)
            .expect("put_with_ttl");
        drop(engine);
    }

    // Wait for TTL to expire
    thread::sleep(Duration::from_secs(2));

    // Reopen and check
    let engine = MidgeEngine::open(opts).expect("reopen");
    let cf = engine.default_column_family();

    // Assert - key should be expired even after restart
    let result = engine.get(&cf, b"expire:key").expect("get");
    assert!(
        result.is_none(),
        "Key should be expired after restart if TTL elapsed"
    );
}

// ============================================================================
// TTL with Compaction Tests
// ============================================================================

#[test]
fn should_remove_expired_entries_given_compaction_when_ttl_exceeded() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: true,
        memtable_size: 4096,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write keys with short TTL
    for i in 0..50 {
        engine
            .put_with_ttl(&cf, format!("ttl_key_{:04}", i).as_bytes(), b"value", 1)
            .expect("put_with_ttl");
    }
    engine.flush_cf(&cf).expect("flush");

    // Wait for TTL to expire
    thread::sleep(Duration::from_secs(2));

    // Trigger compaction to clean up expired entries
    engine.compact_all().expect("compact");

    // Assert - expired keys should be removed
    for i in 0..50 {
        let result = engine
            .get(&cf, format!("ttl_key_{:04}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_none(),
            "Expired key {} should be removed after compaction",
            i
        );
    }
}

#[test]
fn should_preserve_non_expired_entries_given_compaction_when_ttl_not_exceeded() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: true,
        memtable_size: 4096,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write keys with long TTL
    for i in 0..50 {
        engine
            .put_with_ttl(&cf, format!("long_ttl_{:04}", i).as_bytes(), b"value", 3600)
            .expect("put_with_ttl");
    }
    engine.flush_cf(&cf).expect("flush");

    // Trigger compaction
    engine.compact_all().expect("compact");

    // Assert - non-expired keys should survive compaction
    for i in 0..50 {
        let result = engine
            .get(&cf, format!("long_ttl_{:04}", i).as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Non-expired key {} should survive compaction",
            i
        );
    }
}

// ============================================================================
// TTL with Snapshots Tests
// ============================================================================

#[test]
fn should_hide_expired_key_given_snapshot_after_expiry_when_reading_at_snapshot() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write with short TTL
    engine
        .put_with_ttl(&cf, b"snap_ttl:key", b"snap_value", 1)
        .expect("put_with_ttl");

    // Wait for expiry
    thread::sleep(Duration::from_secs(2));

    // Take snapshot after expiry
    let snapshot = engine.snapshot();

    // Assert - snapshot should not see expired key
    let result = engine.get_at(&cf, b"snap_ttl:key", &snapshot).expect("get_at");
    assert!(
        result.is_none(),
        "Snapshot taken after expiry should not see expired key"
    );
}

#[test]
fn should_show_key_given_snapshot_before_expiry_when_reading_at_snapshot() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write with long TTL and take snapshot before expiry
    engine
        .put_with_ttl(&cf, b"snap_valid:key", b"snap_value", 3600)
        .expect("put_with_ttl");
    let snapshot = engine.snapshot();

    // Assert - snapshot should see key
    let result = engine
        .get_at(&cf, b"snap_valid:key", &snapshot)
        .expect("get_at");
    assert_eq!(
        result,
        Some(Bytes::from_static(b"snap_value")),
        "Snapshot should see non-expired key"
    );
}

// ============================================================================
// TTL in Transactions Tests
// ============================================================================

#[test]
fn should_respect_ttl_given_transaction_commit_when_key_expires() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act - write with TTL in transaction
    let mut txn = engine.begin_transaction(&cf).expect("begin");
    txn.put_with_ttl(b"txn_ttl:key", b"txn_value", 1)
        .expect("put_with_ttl in txn");
    engine
        .commit_transaction(txn, Default::default())
        .expect("commit");

    // Wait for TTL to expire
    thread::sleep(Duration::from_secs(2));

    // Assert - key should be expired
    let result = engine.get(&cf, b"txn_ttl:key").expect("get");
    assert!(
        result.is_none(),
        "TTL should be respected for transaction writes"
    );
}

// ============================================================================
// TTL in Write Batch Tests
// ============================================================================

#[test]
fn should_apply_ttl_given_write_batch_with_ttl_when_committed() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Act - write batch with TTL
    let mut batch = cntryl_midge::WriteBatch::new();
    batch.put_with_ttl(
        cf.id(),
        Bytes::from_static(b"batch_ttl:key"),
        Bytes::from_static(b"batch_value"),
        1, // 1 second TTL
    );
    engine.write_batch(&batch).expect("write_batch");

    // Verify immediately readable
    let result = engine.get(&cf, b"batch_ttl:key").expect("get");
    assert!(result.is_some(), "Key should be readable immediately");

    // Wait for TTL
    thread::sleep(Duration::from_secs(2));

    // Assert - should be expired
    let result = engine.get(&cf, b"batch_ttl:key").expect("get after ttl");
    assert!(
        result.is_none(),
        "TTL should be respected for write batch entries"
    );
}

// ============================================================================
// Mixed TTL Tests
// ============================================================================

#[test]
fn should_handle_mixed_ttl_keys_given_some_expire_when_reading() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write mix of TTL and non-TTL keys
    engine
        .put_with_ttl(&cf, b"short_ttl", b"expires_soon", 1)
        .expect("put short ttl");
    engine
        .put_with_ttl(&cf, b"long_ttl", b"expires_later", 3600)
        .expect("put long ttl");
    engine.put(&cf, b"no_ttl", b"permanent").expect("put no ttl");

    // Wait for short TTL to expire
    thread::sleep(Duration::from_secs(2));

    // Assert - only short TTL key should be expired
    assert!(
        engine.get(&cf, b"short_ttl").expect("get").is_none(),
        "Short TTL key should be expired"
    );
    assert!(
        engine.get(&cf, b"long_ttl").expect("get").is_some(),
        "Long TTL key should still exist"
    );
    assert!(
        engine.get(&cf, b"no_ttl").expect("get").is_some(),
        "Non-TTL key should still exist"
    );
}

#[test]
fn should_update_ttl_given_overwrite_with_new_ttl_when_writing() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write with short TTL
    engine
        .put_with_ttl(&cf, b"update_ttl:key", b"v1", 1)
        .expect("put short ttl");

    // Overwrite with longer TTL
    engine
        .put_with_ttl(&cf, b"update_ttl:key", b"v2", 3600)
        .expect("put long ttl");

    // Wait past original TTL
    thread::sleep(Duration::from_secs(2));

    // Assert - key should still exist with new TTL
    let result = engine.get(&cf, b"update_ttl:key").expect("get");
    assert_eq!(
        result,
        Some(Bytes::from_static(b"v2")),
        "Overwritten key should use new TTL"
    );
}
