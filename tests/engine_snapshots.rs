// Snapshot Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

mod common;
use common::{test_temp_dir, new_engine};
#[test]
#[ignore = "Snapshot isolation not fully implemented - documents expected behavior"]
fn should_hide_newer_writes_given_snapshot_when_get_at() {
    // Arrange
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"k", b"v1").unwrap();
    let snap = eng.snapshot();
    eng.put(&cf, b"k", b"v2").unwrap();

    // Act
    let at = eng.get_at(b"k", &snap).unwrap();
    let full = eng.get(&cf, b"k").unwrap();

    // Assert
    // With multi-version memtable, a snapshot created after v1 should still
    // observe v1 even after a newer v2 is written. Latest read should see v2.
    assert_eq!(at, Some(Bytes::from_static(b"v1")));
    assert_eq!(full, Some(Bytes::from_static(b"v2")));
}


#[test]
#[ignore = "Snapshot isolation not fully implemented - documents expected behavior"]
fn should_scan_at_hides_newer_writes_given_snapshot() {
    // Arrange: put v1, snapshot, then write v2 in memtable (v1 persisted or not)
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    eng.put(&cf, b"k", b"v1").unwrap();
    let snap = eng.snapshot();
    eng.put(&cf, b"k", b"v2").unwrap();

    // Act: scan_at should see the older version (v1) and hide v2
    let rows_at = eng
        .scan_at(
            Query::new()
                .start_key(Bytes::from_static(b"a"))
                .end_key(Bytes::from_static(b"z")),
            &snap,
        )
        .unwrap();

    // Assert: The snapshot should observe v1 only
    assert_eq!(
        rows_at,
        vec![(Bytes::from_static(b"k"), Bytes::from_static(b"v1"))]
    );
}


