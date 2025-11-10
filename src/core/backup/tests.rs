use super::*;

#[test]
fn should_create_default_backup_options_with_full_type() {
    // Arrange
    // Act
    let opts = BackupOptions::default();

    // Assert
    assert_eq!(opts.backup_type, BackupType::Full);
    assert!(opts.description.is_none());
    assert!(opts.verify_after_create);
}

#[test]
fn should_create_default_restore_options_with_verification() {
    // Arrange
    // Act
    let opts = RestoreOptions::default();

    // Assert
    assert!(opts.verify_before_restore);
    assert!(!opts.overwrite_existing);
}

#[test]
fn should_compare_full_backup_types_for_equality() {
    // Arrange
    let full1 = BackupType::Full;
    let full2 = BackupType::Full;

    // Act
    let is_equal = full1 == full2;

    // Assert
    assert!(is_equal);
}

#[test]
fn should_detect_full_and_incremental_are_not_equal() {
    // Arrange
    let full = BackupType::Full;
    let incremental = BackupType::Incremental {
        since_backup_id: 10,
    };

    // Act
    let is_equal = full == incremental;

    // Assert
    assert!(!is_equal);
}

#[test]
fn should_compare_incremental_backup_types_with_same_id_for_equality() {
    // Arrange
    let incremental1 = BackupType::Incremental {
        since_backup_id: 10,
    };
    let incremental2 = BackupType::Incremental {
        since_backup_id: 10,
    };

    // Act
    let is_equal = incremental1 == incremental2;

    // Assert
    assert!(is_equal);
}

#[test]
fn should_detect_incremental_backup_types_with_different_ids_are_not_equal() {
    // Arrange
    let incremental1 = BackupType::Incremental {
        since_backup_id: 10,
    };
    let incremental2 = BackupType::Incremental {
        since_backup_id: 20,
    };

    // Act
    let is_equal = incremental1 == incremental2;

    // Assert
    assert!(!is_equal);
}

#[test]
fn should_serialize_full_backup_type() {
    // Arrange
    let backup_type = BackupType::Full;

    // Act
    let json = serde_json::to_string(&backup_type).expect("serialize failed");

    // Assert
    assert!(json.contains("Full"));
}

#[test]
fn should_deserialize_full_backup_type() {
    // Arrange
    let backup_type = BackupType::Full;
    let json = serde_json::to_string(&backup_type).expect("serialize failed");

    // Act
    let deserialized: BackupType = serde_json::from_str(&json).expect("deserialize failed");

    // Assert
    assert_eq!(deserialized, backup_type);
}

#[test]
fn should_serialize_incremental_backup_type() {
    // Arrange
    let backup_type = BackupType::Incremental {
        since_backup_id: 42,
    };

    // Act
    let json = serde_json::to_string(&backup_type).expect("serialize failed");

    // Assert
    assert!(json.contains("42"));
}

#[test]
fn should_deserialize_incremental_backup_type() {
    // Arrange
    let backup_type = BackupType::Incremental {
        since_backup_id: 42,
    };
    let json = serde_json::to_string(&backup_type).expect("serialize failed");

    // Act
    let deserialized: BackupType = serde_json::from_str(&json).expect("deserialize failed");

    // Assert
    assert_eq!(deserialized, backup_type);
}

#[test]
fn should_serialize_sst_file_info() {
    // Arrange
    let sst_info = SstFileInfo {
        name: "000042.sst".to_string(),
        size_bytes: 1024,
        checksum: 0x12345678,
        key_range: Some((b"key1".to_vec(), b"key9".to_vec())),
    };

    // Act
    let result = serde_json::to_string(&sst_info);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_deserialize_sst_file_info() {
    // Arrange
    let sst_info = SstFileInfo {
        name: "000042.sst".to_string(),
        size_bytes: 1024,
        checksum: 0x12345678,
        key_range: Some((b"key1".to_vec(), b"key9".to_vec())),
    };
    let json = serde_json::to_string(&sst_info).expect("serialize failed");

    // Act
    let deserialized: SstFileInfo = serde_json::from_str(&json).expect("deserialize failed");

    // Assert
    assert_eq!(deserialized.name, sst_info.name);
    assert_eq!(deserialized.size_bytes, sst_info.size_bytes);
    assert_eq!(deserialized.checksum, sst_info.checksum);
    assert_eq!(deserialized.key_range, sst_info.key_range);
}

#[test]
fn should_skip_serializing_none_key_range_in_sst_file_info() {
    // Arrange
    let sst_info = SstFileInfo {
        name: "test.sst".to_string(),
        size_bytes: 512,
        checksum: 0xABCD,
        key_range: None,
    };

    // Act
    let json = serde_json::to_string(&sst_info).expect("serialize failed");

    // Assert
    assert!(!json.contains("key_range"));
}

#[test]
fn should_serialize_backup_info() {
    // Arrange
    let backup_info = BackupInfo {
        backup_id: 1,
        timestamp: "2025-10-25T10:00:00Z".to_string(),
        backup_type: BackupType::Full,
        sequence_number: 12345,
        size_bytes: 1048576,
        file_count: 10,
        sst_files: vec![SstFileInfo {
            name: "000001.sst".to_string(),
            size_bytes: 1024,
            checksum: 0x11111111,
            key_range: None,
        }],
        manifest_path: "manifest.json".to_string(),
        description: Some("Test backup".to_string()),
    };

    // Act
    let result = serde_json::to_string(&backup_info);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_deserialize_backup_info() {
    // Arrange
    let backup_info = BackupInfo {
        backup_id: 1,
        timestamp: "2025-10-25T10:00:00Z".to_string(),
        backup_type: BackupType::Full,
        sequence_number: 12345,
        size_bytes: 1048576,
        file_count: 10,
        sst_files: vec![SstFileInfo {
            name: "000001.sst".to_string(),
            size_bytes: 1024,
            checksum: 0x11111111,
            key_range: None,
        }],
        manifest_path: "manifest.json".to_string(),
        description: Some("Test backup".to_string()),
    };
    let json = serde_json::to_string(&backup_info).expect("serialize failed");

    // Act
    let deserialized: BackupInfo = serde_json::from_str(&json).expect("deserialize failed");

    // Assert
    assert_eq!(deserialized.backup_id, backup_info.backup_id);
    assert_eq!(deserialized.sequence_number, backup_info.sequence_number);
    assert_eq!(deserialized.size_bytes, backup_info.size_bytes);
    assert_eq!(deserialized.file_count, backup_info.file_count);
    assert_eq!(deserialized.sst_files.len(), 1);
}

#[test]
fn should_return_true_given_valid_result_when_checking_is_valid() {
    // Arrange
    let result = VerifyResult::Valid;

    // Act
    let is_valid = result.is_valid();

    // Assert
    assert!(is_valid);
}

#[test]
fn should_return_false_given_invalid_result_when_checking_is_valid() {
    // Arrange
    let result = VerifyResult::Invalid {
        errors: vec!["checksum mismatch".to_string()],
    };

    // Act
    let is_valid = result.is_valid();

    // Assert
    assert!(!is_valid);
}

#[test]
fn should_return_none_given_valid_result_when_getting_errors() {
    // Arrange
    let result = VerifyResult::Valid;

    // Act
    let errors = result.errors();

    // Assert
    assert!(errors.is_none());
}

#[test]
fn should_return_errors_given_invalid_result_when_getting_errors() {
    // Arrange
    let error_msgs = vec!["file missing".to_string(), "checksum failed".to_string()];
    let result = VerifyResult::Invalid {
        errors: error_msgs.clone(),
    };

    // Act
    let errors = result.errors();

    // Assert
    assert!(errors.is_some());
    assert_eq!(errors.unwrap(), &error_msgs);
}

#[test]
fn should_clone_backup_options() {
    // Arrange
    let opts = BackupOptions {
        backup_type: BackupType::Incremental { since_backup_id: 5 },
        description: Some("test".to_string()),
        verify_after_create: false,
    };

    // Act
    let cloned = opts.clone();

    // Assert
    assert_eq!(cloned.backup_type, opts.backup_type);
    assert_eq!(cloned.description, opts.description);
    assert_eq!(cloned.verify_after_create, opts.verify_after_create);
}

#[test]
fn should_clone_restore_options() {
    // Arrange
    let opts = RestoreOptions {
        verify_before_restore: false,
        overwrite_existing: true,
    };

    // Act
    let cloned = opts.clone();

    // Assert
    assert_eq!(cloned.verify_before_restore, opts.verify_before_restore);
    assert_eq!(cloned.overwrite_existing, opts.overwrite_existing);
}
