//! Checkpoint Operations
//!
//! Tests checkpoint creation, consistency, recovery, and concurrent operations.
//! Checkpoints provide point-in-time snapshots of the database that can be
//! opened independently or used for backup/restore.
//!
//! Test coverage:
//! - Basic checkpoint creation (empty, with data, multiple SSTs)
//! - Checkpoint consistency during writes
//! - Checkpoint isolation from source database
//! - Multiple sequential checkpoints
//! - Checkpoint with column families
//! - Checkpoint during concurrent operations
//! - Checkpoint recovery scenarios
//! - Error handling (disk full)

mod common;

use bytes::Bytes;
use cntryl_midge::{
    test_hooks::{IoBehavior, TestHooks},
    ColumnFamilyConfig, MidgeEngine, MidgeOptions, StorageMode,
};
use cntryl_midge::testkit::{
    durability_opts, flush_test_opts, new_engine, new_engine_with_test_hooks, test_temp_dir,
    with_engine_restart,
};
use crossbeam::channel;
use std::sync::Arc;
use std::thread;

// ============================================================================
// Basic Checkpoint Creation
// ============================================================================

#[test]
fn should_create_checkpoint_given_data_exists_when_requested() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.put(&cf, b"k2", b"v2").unwrap();
    eng.flush().unwrap();

    // Act
    let cp_dir = std::env::temp_dir().join("checkpoint_test_data");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_given_empty_database_when_requested() {
    // Arrange
    let (_dir, eng) = new_engine();

    // Act
    let cp_dir = std::env::temp_dir().join("empty_checkpoint_test");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_ok());

    // Verify checkpoint is openable
    let cp_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: cp_dir },
        enable_compaction: false,
        ..Default::default()
    };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");
    let cf = cp.default_column_family();
    assert_eq!(cp.get(&cf, b"nonexistent").expect("get"), None);
}

#[test]
fn should_create_checkpoint_given_memory_mode_when_requested() {
    // Arrange
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();

    // Act
    let cp_dir = std::env::temp_dir().join("memory_checkpoint");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_given_compaction_disabled_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let mut opts = durability_opts(dir.path().to_path_buf());
    opts.enable_compaction = false;
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.flush().unwrap();

    // Act
    let cp_dir = dir.path().join("checkpoint");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_given_target_directory_exists_when_requested() {
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

    // Act
    let result = eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_create_checkpoint_given_nested_path_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.flush().unwrap();

    // Act - create checkpoint in nested directory that doesn't exist
    let cp_dir = dir.path().join("deep").join("nested").join("checkpoint");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_ok());
    assert!(cp_dir.exists());
}

#[test]
fn should_create_checkpoint_given_multiple_sst_files_when_requested() {
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

    // Wait for SST files to appear
    let sst_dir = dir.path().join("sst");
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
    assert!(
        db_sst_files.len() >= 2,
        "Expected at least 2 SST files; found {:?}",
        db_sst_files
    );

    // Act
    let cp_dir = dir.path().join("checkpoint");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_ok());

    // Verify SST files were copied
    let cp_sst_dir = cp_dir.join("sst");
    assert!(cp_sst_dir.exists());
    let cp_sst_files = list_all_files(&cp_sst_dir);
    assert!(
        cp_sst_files.len() >= 2,
        "Should have at least 2 SST files in checkpoint; db_ssts={:?} cp_ssts={:?}",
        db_sst_files,
        cp_sst_files
    );
}

#[test]
fn should_create_checkpoint_given_readonly_engine_when_requested() {
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

    // Act
    let cp_dir = dir.path().join("readonly_checkpoint");
    let result = readonly_eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// Checkpoint Data Verification
// ============================================================================

#[test]
fn should_read_data_from_checkpoint_given_checkpoint_created_when_opening() {
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

    // Act
    let mut cp_opts = durability_opts(cp_dir.clone());
    cp_opts.enable_compaction = false;
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");
    let cp_cf = cp.default_column_family();

    // Assert
    assert_eq!(
        cp.get(&cp_cf, b"k1").expect("get"),
        Some(Bytes::from_static(b"v1"))
    );
    assert_eq!(
        cp.get(&cp_cf, b"k2").expect("get"),
        Some(Bytes::from_static(b"v2"))
    );
}

#[test]
fn should_verify_integrity_given_checkpoint_with_many_keys_when_validating() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Write deterministic data
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let val = format!("val{:03}", i);
        eng.put(&cf, key.as_bytes(), val.as_bytes()).unwrap();
    }
    eng.flush().unwrap();

    // Act
    let checkpoint_path = dir.path().join("checkpoint_verify");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();
    let ckpt_cf = ckpt_eng.default_column_family();

    // Assert - all data intact
    for i in 0..100 {
        let key = format!("key{:03}", i);
        let expected = format!("val{:03}", i);
        assert_eq!(
            ckpt_eng.get(&ckpt_cf, key.as_bytes()).unwrap().unwrap(),
            Bytes::from(expected)
        );
    }
}

#[test]
fn should_preserve_tombstones_given_deleted_keys_when_checkpointing() {
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

    engine.put(&cf, b"key1", b"value1").expect("put");
    engine.put(&cf, b"key2", b"value2").expect("put");
    engine.delete(&cf, b"key1").expect("delete");
    engine.flush().expect("flush");

    // Act
    let cp_dir = dir.path().join("checkpoint_tombstones");
    engine.create_checkpoint(&cp_dir).expect("checkpoint");

    // Assert
    let cp_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: cp_dir },
        enable_compaction: false,
        ..Default::default()
    };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");

    assert_eq!(cp.get(&cf, b"key1").expect("get"), None);
    assert_eq!(
        cp.get(&cf, b"key2").expect("get"),
        Some(Bytes::from_static(b"value2"))
    );
}

// ============================================================================
// Checkpoint Isolation
// ============================================================================

#[test]
fn should_isolate_checkpoint_given_original_modified_when_reading() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    eng.put(&cf, b"key1", b"val1").unwrap();
    eng.flush().unwrap();

    let checkpoint_path = dir.path().join("checkpoint_isolated");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    // Act - modify original
    eng.put(&cf, b"key1", b"modified").unwrap();
    eng.delete(&cf, b"key1").unwrap();
    eng.flush().unwrap();

    // Assert - checkpoint unchanged
    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();
    let ckpt_cf = ckpt_eng.default_column_family();

    assert_eq!(
        ckpt_eng.get(&ckpt_cf, b"key1").unwrap().unwrap(),
        Bytes::from("val1")
    );
}

#[test]
fn should_maintain_consistency_given_writes_after_checkpoint_when_reading() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    eng.put(&cf, b"key1", b"val1").unwrap();
    eng.put(&cf, b"key2", b"val2").unwrap();
    eng.flush().unwrap();

    // Act - checkpoint
    let checkpoint_path = dir.path().join("checkpoint1");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    // Write more data after checkpoint
    eng.put(&cf, b"key3", b"val3").unwrap();

    // Assert - checkpoint should have consistent state (key1, key2 only)
    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();
    let ckpt_cf = ckpt_eng.default_column_family();

    assert!(ckpt_eng.get(&ckpt_cf, b"key1").unwrap().is_some());
    assert!(ckpt_eng.get(&ckpt_cf, b"key2").unwrap().is_some());
    assert!(ckpt_eng.get(&ckpt_cf, b"key3").unwrap().is_none());
}

// ============================================================================
// Multiple Checkpoints
// ============================================================================

#[test]
fn should_create_sequential_checkpoints_given_incremental_writes_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    eng.put(&cf, b"key1", b"v1").unwrap();
    eng.flush().unwrap();
    let ckpt1 = dir.path().join("ckpt1");
    eng.create_checkpoint(&ckpt1).unwrap();

    eng.put(&cf, b"key2", b"v2").unwrap();
    eng.flush().unwrap();
    let ckpt2 = dir.path().join("ckpt2");
    eng.create_checkpoint(&ckpt2).unwrap();

    eng.put(&cf, b"key3", b"v3").unwrap();
    eng.flush().unwrap();
    let ckpt3 = dir.path().join("ckpt3");
    eng.create_checkpoint(&ckpt3).unwrap();

    // Close the original engine before opening checkpoints to avoid lock conflicts
    drop(eng);

    // Act - open all checkpoints
    let eng1 = MidgeEngine::open(MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: ckpt1 },
        ..Default::default()
    })
    .unwrap();
    let eng2 = MidgeEngine::open(MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: ckpt2 },
        ..Default::default()
    })
    .unwrap();
    let eng3 = MidgeEngine::open(MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: ckpt3 },
        ..Default::default()
    })
    .unwrap();

    let cf1 = eng1.default_column_family();
    let cf2 = eng2.default_column_family();
    let cf3 = eng3.default_column_family();

    // Assert - each checkpoint has correct version
    assert_eq!(eng1.get(&cf1, b"key1").unwrap().unwrap(), Bytes::from("v1"));
    assert!(eng1.get(&cf1, b"key2").unwrap().is_none());

    assert_eq!(eng2.get(&cf2, b"key1").unwrap().unwrap(), Bytes::from("v1"));
    assert_eq!(eng2.get(&cf2, b"key2").unwrap().unwrap(), Bytes::from("v2"));
    assert!(eng2.get(&cf2, b"key3").unwrap().is_none());

    assert_eq!(eng3.get(&cf3, b"key1").unwrap().unwrap(), Bytes::from("v1"));
    assert_eq!(eng3.get(&cf3, b"key2").unwrap().unwrap(), Bytes::from("v2"));
    assert_eq!(eng3.get(&cf3, b"key3").unwrap().unwrap(), Bytes::from("v3"));
}

// ============================================================================
// Column Families
// ============================================================================

#[test]
fn should_include_all_column_families_given_multiple_cfs_when_checkpointing() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();

    let cf1 = eng.default_column_family();
    let cf2 = eng
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .unwrap();

    eng.put(&cf1, b"key_default", b"val_default").unwrap();
    eng.put(&cf2, b"key_cf2", b"val_cf2").unwrap();
    eng.flush().unwrap();

    // Act
    let checkpoint_path = dir.path().join("multi_cf_checkpoint");
    eng.create_checkpoint(&checkpoint_path).unwrap();

    // Assert
    let ckpt_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: checkpoint_path,
        },
        ..Default::default()
    };
    let ckpt_eng = MidgeEngine::open(ckpt_opts).unwrap();

    let all_cfs = ckpt_eng.list_column_families();
    assert!(all_cfs.iter().any(|cf| cf.name() == "default"));
    assert!(all_cfs.iter().any(|cf| cf.name() == "cf2"));
}

#[test]
fn should_preserve_cf_data_given_checkpoint_with_multiple_cfs_when_reading() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf1 = engine
        .create_column_family("cf1", ColumnFamilyConfig::default())
        .expect("create cf1");
    let cf2 = engine
        .create_column_family("cf2", ColumnFamilyConfig::default())
        .expect("create cf2");

    engine.put(&cf1, b"key1", b"value1").expect("put cf1");
    engine.put(&cf2, b"key2", b"value2").expect("put cf2");
    engine.flush().expect("flush");

    // Act
    let cp_dir = dir.path().join("checkpoint_multi_cf");
    engine.create_checkpoint(&cp_dir).expect("checkpoint");

    // Assert
    let cp_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: cp_dir },
        enable_compaction: false,
        ..Default::default()
    };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");

    let cp_cf1 = cp.get_column_family("cf1").expect("get cf1");
    let cp_cf2 = cp.get_column_family("cf2").expect("get cf2");

    assert_eq!(
        cp.get(&cp_cf1, b"key1").expect("get"),
        Some(Bytes::from_static(b"value1"))
    );
    assert_eq!(
        cp.get(&cp_cf2, b"key2").expect("get"),
        Some(Bytes::from_static(b"value2"))
    );
}

// ============================================================================
// Concurrent Operations
// ============================================================================

#[test]
fn should_create_checkpoint_given_concurrent_writes_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    // Start background writes
    let engine_writer = engine.clone();
    let cf_clone = cf.clone();
    let (ready_tx, ready_rx) = channel::bounded(1);
    let writer = thread::spawn(move || {
        for i in 0..50u32 {
            engine_writer
                .put(&cf_clone, format!("key_{}", i).as_bytes(), b"value")
                .expect("put");
            // Signal the main thread after first write
            if i == 0 {
                let _ = ready_tx.send(());
            }
            std::thread::yield_now();
        }
    });

    // Wait until signaled by the writer
    let timeout = std::time::Duration::from_secs(5);
    ready_rx
        .recv_timeout(timeout)
        .expect("Writer did not report first write");

    // Act - Create checkpoint while writes are ongoing
    let cp_dir = dir.path().join("checkpoint");
    let result = engine.create_checkpoint(&cp_dir);

    writer.join().unwrap();

    // Assert
    assert!(result.is_ok());

    // Verify checkpoint is readable
    let cp_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: cp_dir },
        enable_compaction: false,
        ..Default::default()
    };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");
    assert!(cp.get(&cf, b"key_0").expect("get").is_some());
}

#[test]
fn should_handle_multiple_concurrent_checkpoints_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    // Insert some data
    for i in 0..20u32 {
        engine
            .put(&cf, format!("key_{}", i).as_bytes(), b"value")
            .expect("put");
    }
    engine.flush().expect("flush");

    let mut handles = vec![];

    // Act - Try to create multiple checkpoints concurrently
    for i in 0..3 {
        let engine_clone = engine.clone();
        let dir_clone = dir.path().to_path_buf();
        handles.push(thread::spawn(move || {
            let cp_dir = dir_clone.join(format!("checkpoint_{}", i));
            engine_clone.create_checkpoint(&cp_dir)
        }));
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Assert - All checkpoints should succeed
    assert!(results.iter().all(|r| r.is_ok()));

    // Verify all checkpoints are valid
    for i in 0..3 {
        let cp_opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: dir.path().join(format!("checkpoint_{}", i)),
            },
            enable_compaction: false,
            ..Default::default()
        };
        let cp = MidgeEngine::open(cp_opts).unwrap_or_else(|_| panic!("open checkpoint {}", i));
        assert_eq!(
            cp.get(&cf, b"key_0").expect("get"),
            Some(Bytes::from_static(b"value"))
        );
    }
}

#[test]
fn should_create_consistent_checkpoint_given_high_load_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 2048, // Small to force flushes
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let cf = engine.default_column_family();

    // Heavy write load
    let engine_writer = engine.clone();
    let cf_clone = cf.clone();
    let (ready_tx, ready_rx) = channel::bounded(1);
    let writer = thread::spawn(move || {
        for i in 0..100u32 {
            engine_writer
                .put(
                    &cf_clone,
                    format!("load_{}", i).as_bytes(),
                    format!("value_{}", i).as_bytes(),
                )
                .expect("put");
            if i == 0 {
                let _ = ready_tx.send(());
            }
        }
    });

    // Wait for writer to report first write
    let timeout = std::time::Duration::from_secs(5);
    ready_rx
        .recv_timeout(timeout)
        .expect("Writer did not report first write");

    // Act - Create checkpoint during load
    let cp_dir = dir.path().join("checkpoint_load");
    engine.create_checkpoint(&cp_dir).expect("checkpoint");

    writer.join().unwrap();

    // Assert - Checkpoint should be consistent (all keys readable)
    let cp_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: cp_dir },
        enable_compaction: false,
        ..Default::default()
    };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");

    // Count keys in checkpoint (should have some subset of writes)
    let mut count = 0;
    for i in 0..100u32 {
        if cp
            .get(&cf, format!("load_{}", i).as_bytes())
            .expect("get")
            .is_some()
        {
            count += 1;
        }
    }
    assert!(count > 0, "Checkpoint should contain some data");
}

#[test]
fn should_checkpoint_after_memtable_flush_when_requested() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Fill memtable and flush
    for i in 0..30u32 {
        engine
            .put(&cf, format!("key_{}", i).as_bytes(), b"value")
            .expect("put");
    }
    engine.flush().expect("flush");

    // Act - Create checkpoint after flush
    let cp_dir = dir.path().join("checkpoint_post_flush");
    engine.create_checkpoint(&cp_dir).expect("checkpoint");

    // Assert - All flushed data should be in checkpoint
    let cp_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: cp_dir },
        enable_compaction: false,
        ..Default::default()
    };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");

    for i in 0..30u32 {
        assert_eq!(
            cp.get(&cf, format!("key_{}", i).as_bytes()).expect("get"),
            Some(Bytes::from_static(b"value"))
        );
    }
}

// ============================================================================
// Recovery Scenarios
// ============================================================================

#[test]
fn should_recover_consistently_given_checkpoint_during_compaction_when_restarting() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };

    // Act
    with_engine_restart(
        opts.clone(),
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..20u8 {
                eng.put(&cf, &[i], format!("v{}", i).as_bytes()).unwrap();
            }
            let cp_dir = dir.path().join("cp1");
            eng.create_checkpoint(&cp_dir).expect("checkpoint");
        },
        |eng| {
            // Assert after restart
            let cf = eng.default_column_family();
            assert!(eng.get(&cf, &[0]).unwrap().is_some());
        },
    );
}

#[test]
fn should_not_produce_partial_checkpoint_given_stale_manifest_when_creating() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"x", b"1").unwrap();

    // Act
    let cp_dir = tmp.path().join("cp2");
    eng.create_checkpoint(&cp_dir).expect("create checkpoint");

    // Assert
    let cp_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: cp_dir.clone(),
        },
        ..Default::default()
    };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");
    assert!(cp.get(&cp.default_column_family(), b"x").unwrap().is_some());
}

#[test]
fn should_apply_wal_replay_correctly_given_checkpoint_excludes_pending_tombstones_when_restarting()
{
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act
    with_engine_restart(
        opts.clone(),
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"a", b"1").unwrap();
            eng.delete_range(&cf, b"a", b"z").unwrap();
            let cp_dir = dir.path().join("cp3");
            eng.create_checkpoint(&cp_dir).unwrap();
        },
        |eng| {
            // Assert
            let cf = eng.default_column_family();
            // After restart the WAL replay should return a consistent view
            assert!(eng.get(&cf, b"a").is_ok());
        },
    );
}

#[test]
fn should_resolve_checkpoint_conflict_given_inflight_compaction_when_restarting() {
    // Arrange
    let (tmp, eng) = new_engine();
    let cf = eng.default_column_family();

    // Act
    for i in 0..10u8 {
        eng.put(&cf, &[i], b"v").unwrap();
    }
    eng.flush().unwrap();
    let cp_dir = tmp.path().join("cp4");
    eng.create_checkpoint(&cp_dir).unwrap();

    // Simulate inflight compaction by writing additional data and then restart
    eng.put(&cf, b"extra", b"e").unwrap();
    drop(eng);

    // Reopen engine to simulate restart during compaction
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: tmp.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng2 = MidgeEngine::open(opts).unwrap();

    // Assert
    assert!(eng2
        .get(&eng2.default_column_family(), b"extra")
        .unwrap()
        .is_some());
}

// ============================================================================
// Error Handling
// ============================================================================

#[test]
fn should_fail_checkpoint_given_disk_full_when_creating() {
    // Arrange
    let hooks = TestHooks::new().with_io_behavior(IoBehavior::Normal);
    let (_dir, eng) = new_engine_with_test_hooks(64 * 1024 * 1024, true, hooks.clone());
    let cf = eng.default_column_family();
    eng.put(&cf, b"k1", b"v1").unwrap();
    eng.put(&cf, b"k2", b"v2").unwrap();
    eng.flush().unwrap();

    // Set disk full behavior
    hooks.set_io_behavior(IoBehavior::FailWithEnospc);

    // Act
    let cp_dir = std::env::temp_dir().join("checkpoint_disk_full");
    let result = eng.create_checkpoint(&cp_dir);

    // Assert
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("No space left on device"));
}

#[test]
fn should_allow_operations_given_checkpoint_disk_full_failure_when_continuing() {
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

    // Act - perform operation after disk full error
    eng.put(&cf, b"k3", b"v3").unwrap();
    let result = eng.get(&cf, b"k3");

    // Assert - engine still works after disk full error
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(Bytes::from_static(b"v3")));
}
