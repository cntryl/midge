// Checkpoint Operations
// Extracted from engine.rs

#![allow(clippy::field_reassign_with_default)]
// Engine integration tests consolidated per repo preference
// Structure: Arrange // Act // Assert, one behavior per test, behavior-first names
use bytes::Bytes;
use cntryl_midge::{
    test_hooks::{IoBehavior, TestHooks},
    MidgeEngine, MidgeOptions, StorageMode,
};

mod common;
use common::{
    assert_get_equals, durability_opts, flush_test_opts, new_engine, new_engine_with_test_hooks,
    test_temp_dir,
};
#[test]
fn should_create_checkpoint_when_data_exists() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.put(&cf, b"k2", b"v2").unwrap();
    eng.flush().unwrap();
    eng.wait_for_flush(std::time::Duration::from_secs(5)).unwrap();

    // Act: create checkpoint
    let cp_dir = std::env::temp_dir().join("checkpoint_test");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should succeed
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_when_no_data_exists() {
    // Arrange
    let (_dir, eng) = new_engine();

    // Act: create checkpoint on empty engine
    let cp_dir = std::env::temp_dir().join("empty_checkpoint_test");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should succeed even with no data
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_in_memory_mode() {
    // Arrange
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::Memory;
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();

    // Act: create checkpoint in memory mode
    let cp_dir = std::env::temp_dir().join("memory_checkpoint");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should work in memory mode
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_when_compaction_disabled() {
    // Arrange
    let dir = test_temp_dir();
    let mut opts = durability_opts(dir.path().to_path_buf());
    opts.enable_compaction = false;
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.flush().unwrap();

    // Act: create checkpoint with compaction disabled
    let cp_dir = dir.path().join("checkpoint");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should work with compaction disabled
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_when_target_directory_exists() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.flush().unwrap();

    // Pre-create the checkpoint directory
    let cp_dir = dir.path().join("checkpoint");
    std::fs::create_dir_all(&cp_dir).unwrap();

    // Act: create checkpoint in existing directory
    let result = eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should succeed even if directory exists
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_with_nested_directories() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.flush().unwrap();

    // Act: create checkpoint in nested directory that doesn't exist
    let cp_dir = dir.path().join("deep").join("nested").join("checkpoint");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should create all necessary directories
    assert!(result.is_ok());
    assert!(cp_dir.exists());
}

#[test]
fn should_create_checkpoint_with_multiple_sst_files() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024); // Small memtable to force multiple flushes
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Create multiple SST files by filling memtable multiple times
    for i in 0..10 {
        let key = format!("key{:03}", i);
        let value = format!("value{:03}", i);
        eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    eng.flush().unwrap();

    for i in 10..20 {
        let key = format!("key{:03}", i);
        let value = format!("value{:03}", i);
        eng.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    eng.flush().unwrap();

    // Act: create checkpoint with multiple SST files
    // Wait for at least two SST files to appear in the DB before checkpointing.
    // Flaky systems may schedule flush/manifest writes slightly delayed; ensure
    // the DB directory contains the expected SST files before copying.
    let sst_dir = dir.path().join("sst");
    let db_sst_files = list_all_files(&sst_dir);
    assert!(db_sst_files.len() >= 2, "Expected at least 2 SST files to be present before checkpointing; found {:?}", db_sst_files);

    let cp_dir = dir.path().join("checkpoint");
    // Debug: list SST files present in DB before checkpoint (recursive)
    fn list_all_files(p: &std::path::Path) -> Vec<String> {
        let mut out = Vec::new();
        if let Ok(entries) = std::fs::read_dir(p) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_file() {
                    if let Some(s) = p.file_name().and_then(|n| n.to_str()) {
                        out.push(s.to_string());
                    }
                } else if p.is_dir() {
                    out.extend(list_all_files(&p));
                }
            }
        }
        out
    }

    let db_sst_files = list_all_files(&sst_dir);
    println!("DB SST files before checkpoint: {:?}", db_sst_files);

    let result = eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should handle multiple SST files
    assert!(result.is_ok());

    // Verify SST files were copied
    let cp_sst_dir = cp_dir.join("sst");
    assert!(cp_sst_dir.exists());
    let cp_sst_files = list_all_files(&cp_sst_dir);
    assert!(cp_sst_files.len() >= 2, "Should have at least 2 SST files in checkpoint; db_ssts={:?} cp_ssts={:?}", db_sst_files, cp_sst_files);
}

#[test]
fn should_create_checkpoint_from_readonly_engine() {
    // Arrange: Create a regular engine, add data, then open as read-only
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        eng.put(&cf, b"k1", b"v1").unwrap();
        eng.flush().unwrap();
    }

    // Open as read-only
    let mut readonly_opts = durability_opts(dir.path().to_path_buf());
    readonly_opts.read_only = true;
    let readonly_eng = MidgeEngine::open(readonly_opts).expect("open readonly");

    // Act: create checkpoint from read-only engine
    let cp_dir = dir.path().join("readonly_checkpoint");
    let result = readonly_eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should work from read-only engine
    assert!(result.is_ok());
}

#[test]
fn should_read_data_from_checkpoint_when_created() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    let eng = MidgeEngine::open(opts.clone()).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.put(&cf, b"k2", b"v2").unwrap();
    eng.flush().unwrap();

    let cp_dir = dir.path().join("checkpoint");
    eng.create_checkpoint(&cp_dir).unwrap();

    // Act: open a new engine on the checkpoint directory
    let mut cp_opts = durability_opts(cp_dir.clone());
    cp_opts.enable_compaction = false;
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");

    // Assert: data is readable from checkpoint
    assert_get_equals(&cp, b"k1", b"v1");
    assert_get_equals(&cp, b"k2", b"v2");
}

#[test]
fn should_fail_checkpoint_creation_when_disk_full() {
    // Arrange
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::Normal);
    let (_dir, eng) = new_engine_with_test_hooks(64 * 1024 * 1024, true, hooks.clone());
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.put(&cf, b"k2", b"v2").unwrap();
    eng.flush().unwrap();

    // Set disk full behavior
    hooks.set_io_behavior(IoBehavior::FailWithEnospc);

    // Act: attempt to create checkpoint
    let cp_dir = std::env::temp_dir().join("checkpoint_disk_full");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert: checkpoint creation should fail with disk full error
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("No space left on device"));
}

#[test]
fn should_allow_operations_after_checkpoint_disk_full_failure() {
    // Arrange
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::Normal);
    let (_dir, eng) = new_engine_with_test_hooks(64 * 1024 * 1024, true, hooks.clone());
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.put(&cf, b"k2", b"v2").unwrap();
    eng.flush().unwrap();

    // Set disk full behavior and attempt checkpoint (should fail)
    hooks.set_io_behavior(IoBehavior::FailWithEnospc);
    let cp_dir = std::env::temp_dir().join("checkpoint_after_failure");
    let _ = eng.create_checkpoint(&cp_dir); // Ignore result, expect failure

    // Reset behavior
    hooks.set_io_behavior(IoBehavior::Normal);

    // Act: perform operation after disk full error
    eng.put(&cf, b"k3", b"v3").unwrap();
    let result = eng.get(&cf, b"k3");

    // Assert: engine still works after disk full error
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(Bytes::from_static(b"v3")));
}
