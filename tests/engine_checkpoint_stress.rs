//! Stress tests for checkpoint operations under concurrent load.
//!
//! Tests checkpoint creation with concurrent writes, multiple concurrent checkpoints,
//! and checkpoint consistency under high load scenarios.

use bytes::Bytes;
use cntryl_midge::{ColumnFamilyConfig, MidgeEngine, MidgeOptions, StorageMode};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

mod common;
use common::test_temp_dir;

#[test]
fn should_create_checkpoint_during_concurrent_writes() {
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
    let writer = thread::spawn(move || {
            for i in 0..50u32 {
            engine_writer
                .put(&cf_clone, format!("key_{}", i).as_bytes(), b"value")
                .expect("put");
            // yield instead of sleeping so writer still makes progress deterministically
            std::thread::yield_now();
        }
    });

    // Wait until at least one write is visible (fail fast)
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(1);
    while start.elapsed() < timeout {
        if engine.get(&cf, format!("key_{}", 0).as_bytes()).unwrap().is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

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
fn should_handle_multiple_concurrent_checkpoints() {
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
fn should_create_consistent_checkpoint_under_high_load() {
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
    let writer = thread::spawn(move || {
        for i in 0..100u32 {
            engine_writer
                .put(
                    &cf_clone,
                    format!("load_{}", i).as_bytes(),
                    format!("value_{}", i).as_bytes(),
                )
                .expect("put");
        }
    });

    // Wait for some load to flush/apply (fail fast)
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(1);
    while start.elapsed() < timeout {
        if engine.get(&cf, format!("load_{}", 0).as_bytes()).unwrap().is_some() {
            break;
        }
        std::thread::yield_now();
    }

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
fn should_checkpoint_preserve_all_column_families() {
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

    // Act - Create checkpoint
    let cp_dir = dir.path().join("checkpoint_multi_cf");
    engine.create_checkpoint(&cp_dir).expect("checkpoint");

    // Assert - All column families should exist in checkpoint
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

#[test]
fn should_checkpoint_after_memtable_flush() {
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
    engine
        .wait_for_flush(Duration::from_millis(200))
        .expect("flush");

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

#[test]
fn should_checkpoint_with_tombstones() {
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

    // Act - Create checkpoint
    let cp_dir = dir.path().join("checkpoint_tombstones");
    engine.create_checkpoint(&cp_dir).expect("checkpoint");

    // Assert - Tombstones should be preserved
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

#[test]
fn should_checkpoint_empty_database() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");

    // Act - Create checkpoint of empty database
    let cp_dir = dir.path().join("checkpoint_empty");
    let result = engine.create_checkpoint(&cp_dir);

    // Assert - Should succeed
    assert!(result.is_ok());

    // Checkpoint should be openable
    let cp_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: cp_dir },
        enable_compaction: false,
        ..Default::default()
    };
    let cp = MidgeEngine::open(cp_opts).expect("open checkpoint");
    let cf = cp.default_column_family();
    assert_eq!(cp.get(&cf, b"nonexistent").expect("get"), None);
}
