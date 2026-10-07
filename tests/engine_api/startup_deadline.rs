use cntryl_midge::{Bytes, Engine, MidgeError, OpenOptions, TransactionMode, WriteOptions};
use std::time::Duration;

#[test]
fn should_preserve_local_data_when_timed_startup_completes() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let options = || {
        OpenOptions::local(directory.path())
            .background_compaction(false)
            .open_timeout(Duration::from_secs(5))
            .build()
            .unwrap()
    };
    let mut engine = Engine::open(options()).unwrap();
    let cf = engine.get_column_family("default").unwrap();
    let mut write = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    write.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
    write.commit(WriteOptions::sync()).unwrap();

    // Act
    engine.flush_cf(&cf).unwrap();
    engine.shutdown(Duration::from_secs(5)).unwrap();
    let mut reopened = Engine::open(options()).unwrap();
    let cf = reopened.get_column_family("default").unwrap();
    let read = reopened
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    let value = read.get(b"key").unwrap();
    drop(read);
    reopened.shutdown(Duration::from_secs(5)).unwrap();

    // Assert
    assert_eq!(value, Some(Bytes::from_static(b"value")));
    assert_eq!(
        std::fs::read_to_string(directory.path().join("FORMAT")).unwrap(),
        "midge-format-version=4\n"
    );
}

#[test]
fn should_preserve_compatibility_error_when_timed_open_finds_unsupported_format() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let marker = b"midge-format-version=999\n";
    std::fs::write(directory.path().join("FORMAT"), marker).unwrap();
    let options = OpenOptions::local(directory.path())
        .open_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    // Act
    let result = Engine::open(options);

    // Assert
    assert!(matches!(result, Err(MidgeError::CompatibilityError(_))));
    assert_eq!(
        std::fs::read(directory.path().join("FORMAT")).unwrap(),
        marker
    );
}

#[test]
fn should_allow_normal_memory_operations_when_timed_startup_has_transferred_ownership() {
    // Arrange
    let mut engine = Engine::open(
        OpenOptions::in_memory()
            .open_timeout(Duration::from_secs(1))
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = engine.get_column_family("default").unwrap();

    // Act
    let mut write = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    write.put(b"key".to_vec(), b"value".to_vec(), None).unwrap();
    write.commit(WriteOptions::buffered()).unwrap();
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    let value = read.get(b"key").unwrap();
    drop(read);
    engine.shutdown(Duration::from_secs(5)).unwrap();

    // Assert
    assert_eq!(value, Some(Bytes::from_static(b"value")));
}
