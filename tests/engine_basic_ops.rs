// Basic Operations (Put, Get, Delete, Batch)
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

mod common;
use common::test_temp_dir;
#[test]
fn should_get_value_given_existing_key_when_put() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        enable_compaction: false,
        wal_buffer_size: 1024 * 1024,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Act
    eng.put(&cf, b"a", b"1").expect("put");

    // Assert
    let got = eng.get(&cf, b"a").expect("get");
    assert_eq!(got, Some(Bytes::from_static(b"1")));

    // range scan sanity
    let rows = eng
        .scan(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
        )
        .expect("scan");
    assert_eq!(
        rows,
        vec![(Bytes::from_static(b"a"), Bytes::from_static(b"1"))]
    );
}

#[test]
fn should_return_none_given_deleted_key_when_delete() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k", b"v").expect("put");

    // Act
    eng.delete(&cf, b"k").expect("del");

    // Assert
    let got = eng.get(&cf, b"k").expect("get");
    assert_eq!(got, None);
}

#[test]
fn should_apply_all_mutations_given_mixed_ops_when_batch() {
    use cntryl_midge::WriteBatch;

    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"a"), Bytes::from_static(b"1"));
    batch.put(cf.id(), Bytes::from_static(b"b"), Bytes::from_static(b"2"));
    batch.delete(cf.id(), Bytes::from_static(b"a"));

    // Act
    eng.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(eng.get(&cf, b"a").unwrap(), None);
    assert_eq!(eng.get(&cf, b"b").unwrap(), Some(Bytes::from_static(b"2")));
}

#[test]
fn should_return_mismatch_given_unexpected_value() {
    // Arrange
    use cntryl_midge::CasResult;
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = b"counter";
    let initial = b"5";
    engine.put(&cf, key, initial).unwrap();

    // Act
    let result = engine
        .compare_and_swap(&cf, key, Some(Bytes::from_static(b"0")), b"1")
        .unwrap();
    let value = engine.get(&cf, key).unwrap();

    // Assert
    assert_eq!(
        result,
        CasResult::Mismatch(Some(Bytes::from_static(initial)))
    );
    assert_eq!(value, Some(Bytes::from_static(initial)));
}

#[test]
fn should_handle_empty_range_given_start_equals_end() {
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

    engine.put(&cf, b"key", b"val").expect("put");

    // Act: delete empty range
    engine
        .delete_range(&cf, b"key", b"key")
        .expect("delete_range");

    // Assert: key still exists (empty range is no-op)
    assert_eq!(
        engine.get(&cf, b"key").expect("get"),
        Some(Bytes::from_static(b"val"))
    );
}

#[test]
fn should_handle_inverted_range_given_start_greater_than_end() {
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

    engine.put(&cf, b"b", b"2").expect("put");

    // Act: delete inverted range (should be no-op)
    engine.delete_range(&cf, b"z", b"a").expect("delete_range");

    // Assert: key still exists
    assert_eq!(
        engine.get(&cf, b"b").expect("get"),
        Some(Bytes::from_static(b"2"))
    );
}

#[test]
fn should_not_create_filesystem_artifacts_when_using_memory_mode() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path();

    // Verify directory is empty before test
    assert!(
        !db_path.exists() || std::fs::read_dir(db_path).unwrap().count() == 0,
        "Directory should be empty before test"
    );

    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        enable_compaction: false,
        ..Default::default()
    };

    // Act
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Write some data
    engine.put(&cf, b"key1", b"value1").expect("put");
    engine.put(&cf, b"key2", b"value2").expect("put");
    engine.delete(&cf, b"key1").expect("delete");

    // Verify data exists in memory
    assert_eq!(engine.get(&cf, b"key1").expect("get"), None);
    assert_eq!(
        engine.get(&cf, b"key2").expect("get"),
        Some(Bytes::from_static(b"value2"))
    );

    // Assert: No filesystem artifacts created
    assert!(
        !db_path.exists() || std::fs::read_dir(db_path).unwrap().count() == 0,
        "No directories or files should be created in memory mode"
    );

    // Explicitly check common paths
    assert!(
        !db_path.join("sst").exists(),
        "sst directory should not exist"
    );
    assert!(
        !db_path.join("wal").exists(),
        "wal directory should not exist"
    );
    assert!(
        !db_path.join("manifest.json").exists(),
        "manifest.json should not exist"
    );
    assert!(!db_path.join("LOCK").exists(), "LOCK file should not exist");
}
