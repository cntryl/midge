// Column family create rollback test
// Verifies that if manifest.save_atomic fails during CF creation,
// the in-memory CF registration is rolled back properly.

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use cntryl_midge::api::column_family::ColumnFamilyConfig;
use std::fs;

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

#[test]
fn should_rollback_cf_from_memory_when_manifest_save_fails() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Make manifest.json read-only to force save failure during rename
    let manifest_path = db_path.join("manifest.json");
    
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_mode(0o444); // read-only
        fs::set_permissions(&manifest_path, perms).unwrap();
    }
    
    #[cfg(windows)]
    {
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }

    // Act
    let result = engine.create_column_family("test_cf", ColumnFamilyConfig::default());

    // Restore permissions for cleanup
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }
    
    #[cfg(windows)]
    {
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_readonly(false);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }

    // Assert
    assert!(result.is_err(), "Expected create_column_family to fail");

    // Verify CF was not registered in memory
    let cf_list = engine.list_column_families();
    assert_eq!(
        cf_list.len(),
        1,
        "Expected only default CF after rollback, got: {:?}",
        cf_list.iter().map(|h| h.name()).collect::<Vec<_>>()
    );
    assert_eq!(cf_list[0].name(), "default");

    // Verify we cannot get a handle for the failed CF
    let get_result = engine.get_column_family("test_cf");
    assert!(
        get_result.is_err(),
        "Expected error for non-existent CF after rollback"
    );
}

#[test]
fn should_rollback_cf_name_mapping_when_manifest_save_fails() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Successfully create a CF first
    let _handle1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");

    // Make manifest.json read-only
    let manifest_path = db_path.join("manifest.json");
    
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_mode(0o444);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }
    
    #[cfg(windows)]
    {
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }

    // Act
    let result = engine.create_column_family("cf2", ColumnFamilyConfig::default());

    // Restore permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }
    
    #[cfg(windows)]
    {
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_readonly(false);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }

    // Assert
    assert!(result.is_err(), "Expected second CF creation to fail");

    // Should still have default + cf1 only
    let cf_list = engine.list_column_families();
    assert_eq!(cf_list.len(), 2, "Expected 2 CFs after rollback");
    
    let names: Vec<_> = cf_list.iter().map(|h| h.name()).collect();
    assert!(names.contains(&"default"));
    assert!(names.contains(&"cf1"));
    assert!(!names.contains(&"cf2"), "cf2 should not exist after rollback");

    // Verify cf2 cannot be retrieved
    assert!(engine.get_column_family("cf2").is_err());
}

#[test]
fn should_allow_retry_after_failed_create() {
    // Arrange
    let dir = temp_dir();
    let db_path = dir.path().to_path_buf();
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: db_path.clone(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Make manifest.json read-only to cause first failure
    let manifest_path = db_path.join("manifest.json");
    
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_mode(0o444);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }
    
    #[cfg(windows)]
    {
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }

    // First attempt should fail
    let result1 = engine.create_column_family("retry_cf", ColumnFamilyConfig::default());
    assert!(result1.is_err());

    // Restore permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }
    
    #[cfg(windows)]
    {
        let mut perms = fs::metadata(&manifest_path).unwrap().permissions();
        perms.set_readonly(false);
        fs::set_permissions(&manifest_path, perms).unwrap();
    }

    // Act - Retry with same name should succeed
    let result2 = engine.create_column_family("retry_cf", ColumnFamilyConfig::default());

    // Assert
    assert!(result2.is_ok(), "Retry should succeed after fixing permissions");
    
    let handle = result2.unwrap();
    assert_eq!(handle.name(), "retry_cf");
    
    let cf_list = engine.list_column_families();
    assert_eq!(cf_list.len(), 2); // default + retry_cf
}
