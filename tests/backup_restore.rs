// Backup & Restore End-to-End tests - P1 Priority
// These tests validate the complete backup and restore workflow

mod common;

use bytes::Bytes;
use common::*;
use cntryl_midge::backup::{BackupEngine, BackupOptions, BackupType, RestoreEngine, RestoreOptions};
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

// ============================================================================
// End-to-End Backup (4 tests)
// ============================================================================

#[test]
fn should_create_full_backup_given_live_database() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    engine
        .put(Bytes::from("key2"), Bytes::from("value2"))
        .unwrap();
    engine.flush().unwrap();

    // Act
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();

    // Assert
    assert_eq!(backup_info.backup_id, 1);
    assert_eq!(backup_info.backup_type, BackupType::Full);
    assert!(backup_info.file_count > 0);
    assert!(backup_info.size_bytes > 0);
}

#[test]
fn should_include_all_ssts_given_full_backup_when_created() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    for i in 0..3 {
        engine
            .put(Bytes::from(format!("key_{}", i)), Bytes::from("value"))
            .unwrap();
        engine.flush().unwrap();
    }

    // Act
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();

    // Assert
    assert!(!backup_info.sst_files.is_empty());
    for sst_file in &backup_info.sst_files {
        assert!(sst_file.name.ends_with(".sst"));
        assert!(sst_file.size_bytes > 0);
    }
}

#[test]
fn should_include_manifest_given_backup_when_created() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key"), Bytes::from("value"))
        .unwrap();
    engine.flush().unwrap();

    // Act
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();

    // Assert
    assert!(!backup_info.manifest_path.is_empty());
    let manifest_path = backup_dir
        .path()
        .join(format!("backup_{:06}", backup_info.backup_id))
        .join(&backup_info.manifest_path);
    assert!(
        manifest_path.exists(),
        "Manifest path should exist: {:?}",
        manifest_path
    );
}

#[test]
fn should_verify_backup_integrity_given_checksum_validation() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key"), Bytes::from("value"))
        .unwrap();
    engine.flush().unwrap();
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();

    // Act
    let verify_result = backup_engine.verify_backup(backup_info.backup_id).unwrap();

    // Assert
    assert!(verify_result.is_valid());
}

// ============================================================================
// End-to-End Restore (3 tests)
// ============================================================================

#[test]
fn should_restore_data_given_valid_backup() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let restore_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    engine.flush().unwrap();
    drop(engine);
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();

    // Act
    let restore_engine = RestoreEngine::new(backup_dir.path());
    restore_engine
        .restore_backup(
            backup_info.backup_id,
            restore_dir.path(),
            RestoreOptions {
                verify_before_restore: false,
                overwrite_existing: true,
            },
        )
        .unwrap();

    // Assert
    let restore_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: restore_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let restored_engine = MidgeEngine::open(restore_opts).unwrap();
    assert_get_equals(&restored_engine, b"key1", b"value1");
}

#[test]
fn should_read_all_keys_given_restored_database() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let restore_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let keys = vec!["alpha", "beta", "gamma"];
    for key in &keys {
        engine.put(Bytes::from(*key), Bytes::from("value")).unwrap();
    }
    engine.flush().unwrap();
    drop(engine);
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();
    let restore_engine = RestoreEngine::new(backup_dir.path());
    restore_engine
        .restore_backup(
            backup_info.backup_id,
            restore_dir.path(),
            RestoreOptions {
                verify_before_restore: false,
                overwrite_existing: true,
            },
        )
        .unwrap();

    // Act
    let restore_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: restore_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let restored_engine = MidgeEngine::open(restore_opts).unwrap();

    // Assert
    for key in &keys {
        assert_get_equals(&restored_engine, key.as_bytes(), b"value");
    }
}

#[test]
fn should_restore_to_different_path_given_backup_location() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let restore_dir_1 = test_temp_dir();
    let restore_dir_2 = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key"), Bytes::from("value"))
        .unwrap();
    engine.flush().unwrap();
    drop(engine);
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();
    let restore_engine = RestoreEngine::new(backup_dir.path());

    // Act
    restore_engine
        .restore_backup(
            backup_info.backup_id,
            restore_dir_1.path(),
            RestoreOptions {
                verify_before_restore: false,
                overwrite_existing: true,
            },
        )
        .unwrap();
    restore_engine
        .restore_backup(
            backup_info.backup_id,
            restore_dir_2.path(),
            RestoreOptions {
                verify_before_restore: false,
                overwrite_existing: true,
            },
        )
        .unwrap();

    // Assert
    let opts_1 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: restore_dir_1.path().to_path_buf(),
        },
        ..Default::default()
    };
    let opts_2 = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: restore_dir_2.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine_1 = MidgeEngine::open(opts_1).unwrap();
    let engine_2 = MidgeEngine::open(opts_2).unwrap();
    assert_get_equals(&engine_1, b"key", b"value");
    assert_get_equals(&engine_2, b"key", b"value");
}

// ============================================================================
// Incremental Backup (3 tests)
// ============================================================================

#[test]
fn should_create_incremental_backup_given_previous_full_backup() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    engine.flush().unwrap();
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let full_backup = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();
    engine
        .put(Bytes::from("key2"), Bytes::from("value2"))
        .unwrap();
    engine.flush().unwrap();

    // Act
    let incremental_backup = backup_engine
        .create_backup(BackupOptions {
            backup_type: BackupType::Incremental {
                since_backup_id: full_backup.backup_id,
            },
            ..Default::default()
        })
        .unwrap();

    // Assert
    assert_eq!(incremental_backup.backup_id, 2);
    assert_eq!(
        incremental_backup.backup_type,
        BackupType::Incremental { since_backup_id: 1 }
    );
}

#[test]
fn should_only_backup_new_ssts_given_incremental_when_created() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    engine.flush().unwrap();
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let full_backup = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();
    let full_file_count = full_backup.file_count;
    for i in 0..100 {
        engine
            .put(
                Bytes::from(format!("new_key_{}", i)),
                Bytes::from("new_value"),
            )
            .unwrap();
    }
    engine.flush().unwrap();

    // Act
    let incremental_backup = backup_engine
        .create_backup(BackupOptions {
            backup_type: BackupType::Incremental {
                since_backup_id: full_backup.backup_id,
            },
            ..Default::default()
        })
        .unwrap();

    // Assert
    assert!(incremental_backup.file_count < full_file_count + 10);
}

#[test]
fn should_restore_from_full_plus_incremental_given_backup_chain() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let restore_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key1"), Bytes::from("value1"))
        .unwrap();
    engine.flush().unwrap();
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let full_backup = backup_engine
        .create_backup(BackupOptions::default())
        .unwrap();
    engine
        .put(Bytes::from("key2"), Bytes::from("value2"))
        .unwrap();
    engine.flush().unwrap();
    let incremental_backup = backup_engine
        .create_backup(BackupOptions {
            backup_type: BackupType::Incremental {
                since_backup_id: full_backup.backup_id,
            },
            ..Default::default()
        })
        .unwrap();

    // Act
    let restore_engine = RestoreEngine::new(backup_dir.path());
    restore_engine
        .restore_backup(
            incremental_backup.backup_id,
            restore_dir.path(),
            RestoreOptions {
                verify_before_restore: false,
                overwrite_existing: true,
            },
        )
        .unwrap();

    // Assert
    let restore_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: restore_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let restored_engine = MidgeEngine::open(restore_opts).unwrap();
    assert_get_equals(&restored_engine, b"key1", b"value1");
    assert_get_equals(&restored_engine, b"key2", b"value2");
}

// ============================================================================
// Backup Corruption (2 tests)
// ============================================================================

#[test]
fn should_detect_corrupted_backup_given_invalid_checksum() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key"), Bytes::from("value"))
        .unwrap();
    engine.flush().unwrap();
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions {
            verify_after_create: false,
            ..Default::default()
        })
        .unwrap();
    if let Some(sst) = backup_info.sst_files.first() {
        let corrupt_path = backup_dir
            .path()
            .join(format!("backup_{:06}", backup_info.backup_id))
            .join(&sst.name);
        std::fs::write(&corrupt_path, b"CORRUPTED").unwrap();
    }

    // Act
    let verify_result = backup_engine.verify_backup(backup_info.backup_id).unwrap();

    // Assert
    assert!(!verify_result.is_valid());
}

#[test]
fn should_fail_restore_given_missing_sst_in_backup() {
    // Arrange
    let db_dir = test_temp_dir();
    let backup_dir = test_temp_dir();
    let restore_dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    engine
        .put(Bytes::from("key"), Bytes::from("value"))
        .unwrap();
    engine.flush().unwrap();
    let mut backup_engine = BackupEngine::open(db_dir.path(), backup_dir.path()).unwrap();
    let backup_info = backup_engine
        .create_backup(BackupOptions {
            verify_after_create: false,
            ..Default::default()
        })
        .unwrap();
    if let Some(sst) = backup_info.sst_files.first() {
        let sst_path = backup_dir
            .path()
            .join(format!("backup_{:06}", backup_info.backup_id))
            .join(&sst.name);
        std::fs::remove_file(&sst_path).unwrap();
    }

    // Act
    let restore_engine = RestoreEngine::new(backup_dir.path());
    let result = restore_engine.restore_backup(
        backup_info.backup_id,
        restore_dir.path(),
        RestoreOptions {
            verify_before_restore: true,
            ..Default::default()
        },
    );

    // Assert
    assert!(result.is_err());
}
