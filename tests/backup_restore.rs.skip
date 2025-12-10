//! Backup and Restore Integration Tests
//!
//! Tests for the BackupEngine and RestoreEngine functionality:
//! - Full backup creation and verification
//! - Incremental backup creation
//! - Backup restoration to new location
//! - Backup integrity verification
//! - Concurrent backup during database operations
//!
//! ## Coverage
//! - BackupEngine::create_backup (full and incremental)
//! - BackupEngine::verify_backup
//! - BackupEngine::list_backups
//! - RestoreEngine::restore_backup
//! - RestoreEngine::restore_latest

mod common;

use cntryl_midge::backup::{
    BackupEngine, BackupOptions, BackupType, RestoreEngine, RestoreOptions,
};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::testkit::test_temp_dir;
use std::sync::Arc;
use std::thread;

// ============================================================================
// Full Backup Tests
// ============================================================================

#[test]
fn should_create_full_backup_given_active_database_when_requested() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write some data and flush to create SST files
    for i in 0..100 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"value")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");

    // Act
    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .expect("create backup");

    // Assert
    assert_eq!(backup_info.backup_id, 1);
    assert_eq!(backup_info.backup_type, BackupType::Full);
    assert!(
        backup_info.size_bytes > 0,
        "Backup should have non-zero size"
    );
    assert!(
        !backup_info.sst_files.is_empty(),
        "Backup should contain SST files"
    );
}

#[test]
fn should_include_all_sst_files_given_multiple_flushes_when_creating_full_backup() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        enable_compaction: false, // Disable compaction to preserve multiple SST files
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Create multiple SST files via multiple flushes
    for batch in 0..3 {
        for i in 0..50 {
            engine
                .put(
                    &cf,
                    format!("batch{}_key{:04}", batch, i).as_bytes(),
                    b"value",
                )
                .expect("put");
        }
        engine.flush_cf(&cf).expect("flush");
    }

    // Act
    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .expect("create backup");

    // Assert
    assert!(
        backup_info.sst_files.len() >= 3,
        "Should have at least 3 SST files from 3 flushes, got {}",
        backup_info.sst_files.len()
    );
}

// ============================================================================
// Incremental Backup Tests
// ============================================================================

#[test]
fn should_create_incremental_backup_given_previous_full_backup_when_new_data_written() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write initial data and create full backup
    for i in 0..50 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"value1")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");

    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");
    let full_backup = backup_engine
        .create_backup(BackupOptions::default())
        .expect("create full backup");

    // Write more data
    for i in 50..100 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"value2")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");

    // Act - create incremental backup
    let incr_opts = BackupOptions {
        backup_type: BackupType::Incremental {
            since_backup_id: full_backup.backup_id,
        },
        ..Default::default()
    };
    let incr_backup = backup_engine
        .create_backup(incr_opts)
        .expect("create incremental backup");

    // Assert
    assert_eq!(incr_backup.backup_id, 2);
    assert!(matches!(
        incr_backup.backup_type,
        BackupType::Incremental { .. }
    ));
    // Incremental should have fewer SST files than full (only new files)
    // Note: size_bytes might not be smaller because it includes the manifest
    assert!(
        incr_backup.sst_files.len() < full_backup.sst_files.len()
            || incr_backup.sst_files.len() == full_backup.sst_files.len(),
        "Incremental backup should have same or fewer SST files than full backup: incr={}, full={}",
        incr_backup.sst_files.len(),
        full_backup.sst_files.len()
    );
}

// ============================================================================
// Backup Verification Tests
// ============================================================================

#[test]
fn should_verify_backup_integrity_given_valid_backup_when_verifying() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    for i in 0..50 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"value")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");

    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .expect("create backup");

    // Act
    let verify_result = backup_engine
        .verify_backup(backup_info.backup_id)
        .expect("verify");

    // Assert
    assert!(
        verify_result.is_valid(),
        "Backup verification should pass for valid backup"
    );
}

#[test]
fn should_detect_corruption_given_modified_backup_file_when_verifying() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    for i in 0..50 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"value")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");

    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .expect("create backup");

    // Corrupt one of the backup files
    if let Some(sst_file) = backup_info.sst_files.first() {
        let backup_path = backup_dir
            .path()
            .join(format!("backup_{:06}", backup_info.backup_id))
            .join(&sst_file.name);
        if backup_path.exists() {
            std::fs::write(&backup_path, b"corrupted data").expect("corrupt file");
        }
    }

    // Act
    let verify_result = backup_engine
        .verify_backup(backup_info.backup_id)
        .expect("verify");

    // Assert
    assert!(
        !verify_result.is_valid(),
        "Backup verification should fail for corrupted backup"
    );
}

// ============================================================================
// Restore Tests
// ============================================================================

#[test]
fn should_restore_database_given_valid_full_backup_when_target_empty() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let restore_parent = test_temp_dir();
    // Use a subdirectory that doesn't exist yet (TempDir creates the parent)
    let restore_dir = restore_parent.path().join("restored_db");

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write data
    for i in 0..100 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"original_value")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");

    // Create backup
    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .expect("create backup");

    drop(engine); // Close original database

    // Act - restore to new location
    let restore_engine = RestoreEngine::new(backup_dir.path());
    restore_engine
        .restore_backup(
            backup_info.backup_id,
            &restore_dir,
            RestoreOptions {
                verify_before_restore: false, // Skip verification for speed
                overwrite_existing: false,
            },
        )
        .expect("restore");

    // Assert - open restored database and verify data
    let restored_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: restore_dir.clone(),
        },
        ..Default::default()
    };
    let restored_engine = MidgeEngine::open(restored_opts).expect("open restored");
    let restored_cf = restored_engine.default_column_family();

    for i in 0..100 {
        let key = format!("key{:04}", i);
        let result = restored_engine
            .get(&restored_cf, key.as_bytes())
            .expect("get");
        assert!(
            result.is_some(),
            "Key {} should exist in restored database",
            key
        );
    }
}

#[test]
fn should_restore_latest_given_multiple_backups_when_requested() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let restore_parent = test_temp_dir();
    // Use a subdirectory that doesn't exist yet (TempDir creates the parent)
    let restore_dir = restore_parent.path().join("restored_db");

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");

    // Create first backup with initial data
    for i in 0..50 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"v1")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");
    let _backup1 = backup_engine
        .create_backup(BackupOptions::default())
        .expect("backup1");

    // Create second backup with more data
    for i in 50..100 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"v2")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");
    let _backup2 = backup_engine
        .create_backup(BackupOptions::default())
        .expect("backup2");

    drop(engine);

    // Act - restore latest
    let restore_engine = RestoreEngine::new(backup_dir.path());
    restore_engine
        .restore_latest(
            &restore_dir,
            RestoreOptions {
                verify_before_restore: false,
                overwrite_existing: false,
            },
        )
        .expect("restore latest");

    // Assert - should have data from both backups (latest state)
    let restored_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: restore_dir.clone(),
        },
        ..Default::default()
    };
    let restored_engine = MidgeEngine::open(restored_opts).expect("open restored");
    let restored_cf = restored_engine.default_column_family();

    // Check keys from both batches
    assert!(
        restored_engine
            .get(&restored_cf, b"key0000")
            .expect("get")
            .is_some(),
        "Key from first batch should exist"
    );
    assert!(
        restored_engine
            .get(&restored_cf, b"key0099")
            .expect("get")
            .is_some(),
        "Key from second batch should exist"
    );
}

// ============================================================================
// Concurrent Backup Tests
// ============================================================================

#[test]
fn should_create_consistent_backup_given_concurrent_writes_when_backing_up() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    // Write initial data
    for i in 0..50 {
        engine
            .put(&cf, format!("key{:04}", i).as_bytes(), b"initial")
            .expect("put");
    }
    engine.flush_cf(&cf).expect("flush");

    // Act - start concurrent writes while backing up
    let writer_engine = Arc::clone(&engine);
    let writer_cf = cf.clone();
    let writer_handle = thread::spawn(move || {
        for i in 50..150 {
            writer_engine
                .put(&writer_cf, format!("key{:04}", i).as_bytes(), b"concurrent")
                .expect("concurrent put");
        }
    });

    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");
    let backup_result = backup_engine.create_backup(BackupOptions::default());

    writer_handle.join().expect("writer thread");

    // Assert - backup should succeed (may or may not include concurrent writes)
    assert!(
        backup_result.is_ok(),
        "Backup should succeed during concurrent writes"
    );
    let backup_info = backup_result.unwrap();
    assert!(backup_info.size_bytes > 0, "Backup should have content");
}

#[test]
fn should_create_backup_given_compaction_running_when_database_active() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        enable_compaction: true,
        compaction_check_interval_ms: 100,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write enough data to trigger compaction
    for batch in 0..5 {
        for i in 0..100 {
            engine
                .put(
                    &cf,
                    format!("key{:04}", i).as_bytes(),
                    format!("value_batch{}", batch).as_bytes(),
                )
                .expect("put");
        }
        engine.flush_cf(&cf).expect("flush");
    }

    // Act - create backup (compaction may be running)
    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");
    let backup_result = backup_engine.create_backup(BackupOptions::default());

    // Assert
    assert!(
        backup_result.is_ok(),
        "Backup should succeed during compaction"
    );
}

// ============================================================================
// Backup Listing Tests
// ============================================================================

#[test]
fn should_list_all_backups_given_multiple_backups_when_listing() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    let mut backup_engine =
        BackupEngine::open(db_dir.path(), backup_dir.path()).expect("open backup engine");

    // Create multiple backups
    for batch in 0..3 {
        for i in 0..20 {
            engine
                .put(&cf, format!("batch{}_key{}", batch, i).as_bytes(), b"value")
                .expect("put");
        }
        engine.flush_cf(&cf).expect("flush");
        backup_engine
            .create_backup(BackupOptions::default())
            .expect("create backup");
    }

    // Act
    let backups = backup_engine.list_backups().expect("list backups");

    // Assert
    assert_eq!(backups.len(), 3, "Should have 3 backups");
    assert_eq!(backups[0].backup_id, 1);
    assert_eq!(backups[1].backup_id, 2);
    assert_eq!(backups[2].backup_id, 3);
}
