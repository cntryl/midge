// Read-Only Mode
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

mod common;
use common::test_temp_dir;
#[test]
fn should_fail_insert_given_read_only_mode() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        read_only: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).unwrap();
    let cf = engine.default_column_family();
    let key = b"key1";
    let value = b"value1";
    engine.put(&cf, key, value).unwrap();
    drop(engine);

    let opts_ro = MidgeOptions {
        storage_mode: StorageMode::Memory,
        read_only: true,
        ..Default::default()
    };
    let engine_ro = MidgeEngine::open(opts_ro).unwrap();
    let cf_ro = engine_ro.default_column_family();

    // Act
    let result = engine_ro.insert(&cf_ro, key, b"value2");

    // Assert
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        cntryl_midge::error::MidgeError::ReadOnly
    ));
}


#[test]
fn should_allow_reads_when_opened_read_only() {
    // Arrange: create a temp dir DB and write a key, then close
    let tmp = tempfile::tempdir().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let db = MidgeEngine::open(opts.clone()).unwrap();
    let cf = db.default_column_family();
    db.put(&cf, b"k", b"v").unwrap();
    db.flush().unwrap();
    drop(db);

    // Act: Re-open read-only
    let ro = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        enable_compaction: false,
        read_only: true,
        ..Default::default()
    };
    let db_ro = MidgeEngine::open(ro).unwrap();
    let cf_ro = db_ro.default_column_family();

    // Assert: reads work
    let got = db_ro.get(&cf_ro, b"k").unwrap();
    assert_eq!(got, Some(Bytes::from_static(b"v")));
}


#[test]
fn should_reject_writes_when_opened_read_only() {
    // Arrange: prepare an existing DB on disk
    let tmp = tempfile::tempdir().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        enable_compaction: false,
        ..Default::default()
    };
    let db = MidgeEngine::open(opts.clone()).unwrap();
    let cf = db.default_column_family();
    db.put(&cf, b"k", b"v").unwrap();
    db.flush().unwrap();
    drop(db);

    // Act: open read-only and attempt a write
    let ro = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        enable_compaction: false,
        read_only: true,
        ..Default::default()
    };
    let db_ro = MidgeEngine::open(ro).unwrap();
    let cf_ro = db_ro.default_column_family();
    let err = db_ro.put(&cf_ro, b"k2", b"v2").unwrap_err();

    // Assert: error indicates read-only
    let msg = format!("{}", err);
    assert!(msg.contains("read-only"));
}


