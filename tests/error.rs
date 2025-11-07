use midge::error::MidgeError;

#[test]
fn should_display_key_not_found_with_key() {
    // Arrange
    let err = MidgeError::KeyNotFound { key: "abc".into() };

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("Key not found"));
    assert!(msg.contains("abc"));
}

#[test]
fn should_create_corruption_via_helper_and_display() {
    // Arrange
    // Act
    let err = MidgeError::corruption("bad checksum");
    let s = format!("{}", err);

    // Assert
    assert!(s.contains("Corruption detected"));
    assert!(s.contains("bad checksum"));
}

#[test]
fn should_create_internal_via_helper_and_display() {
    // Arrange
    // Act
    let err = MidgeError::internal("oops");
    let s = format!("{}", err);

    // Assert
    assert!(s.contains("Internal error"));
    assert!(s.contains("oops"));
}

#[test]
fn should_create_invalid_config_via_helper_and_display() {
    // Arrange
    // Act
    let err = MidgeError::invalid_config("bad setting");
    let s = format!("{}", err);

    // Assert
    assert!(s.contains("Invalid configuration"));
    assert!(s.contains("bad setting"));
}

#[test]
fn should_display_key_exists_with_key() {
    // Arrange
    let err = MidgeError::KeyExists { key: "xyz".into() };

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("Key already exists"));
    assert!(msg.contains("xyz"));
}

#[test]
fn should_display_transaction_conflict() {
    // Arrange
    let err = MidgeError::TransactionConflict {
        message: "write-write conflict".to_string(),
    };

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("Transaction conflict"));
    assert!(msg.contains("write-write"));
}

#[test]
fn should_display_database_closed() {
    // Arrange
    let err = MidgeError::DatabaseClosed;

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("Database is closed"));
}

#[test]
fn should_display_compaction_error() {
    // Arrange
    let err = MidgeError::CompactionError {
        message: "merge failed".into(),
    };

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("Compaction error"));
    assert!(msg.contains("merge failed"));
}

#[test]
fn should_display_compression_error() {
    // Arrange
    let err = MidgeError::CompressionError {
        message: "codec failure".into(),
    };

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("Compression error"));
    assert!(msg.contains("codec failure"));
}

#[test]
fn should_display_wal_error() {
    // Arrange
    let err = MidgeError::WalError {
        message: "write failed".into(),
    };

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("WAL error"));
    assert!(msg.contains("write failed"));
}

#[test]
fn should_display_read_only() {
    // Arrange
    let err = MidgeError::ReadOnly;

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("Database opened in read-only mode"));
}

#[test]
fn should_display_invalid_data() {
    // Arrange
    let err = MidgeError::InvalidData("malformed entry".into());

    // Act
    let msg = format!("{}", err);

    // Assert
    assert!(msg.contains("Invalid data"));
    assert!(msg.contains("malformed entry"));
}
