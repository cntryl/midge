// Checkpoint Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

mod common;
use common::test_temp_dir;
#[test]
fn should_create_checkpoint_and_read_from_it() {
    // Arrange
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.put(&cf, b"k2", b"v2").unwrap();
    eng.flush().unwrap();
    // Create checkpoint
    let cp_dir = dir.path().join("checkpoint");
    eng.create_checkpoint(&cp_dir).unwrap();

    // Act: open a new engine on the checkpoint directory (read-only in spirit)
    let mut cp_opts = MidgeOptions::default();
    cp_opts.storage_mode = StorageMode::LocalDisk {
        db_path: cp_dir.clone(),
    };
    cp_opts.enable_compaction = false;
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");

    // Assert: data is readable from checkpoint
    assert_eq!(cp.get(&cf, b"k1").unwrap(), Some(Bytes::from_static(b"v1")));
    assert_eq!(cp.get(&cf, b"k2").unwrap(), Some(Bytes::from_static(b"v2")));
}
