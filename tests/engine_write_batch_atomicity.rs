//! WriteBatch atomicity and correctness tests
//!
//! Critical properties tested:
//! - Atomicity: All operations in batch commit together
//! - Ordering: Operations apply in batch order
//! - Durability: Batches persist across restarts
//! - Concurrency: Batches don't interleave incorrectly

use bytes::Bytes;
use cntryl_midge::{ColumnFamilyConfig, MidgeEngine, MidgeOptions, StorageMode, WriteBatch};
use std::sync::Arc;

mod common;
use common::test_temp_dir;

#[test]
fn should_commit_all_operations_atomically_when_batch_succeeds() {
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
    
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"key1"), Bytes::from_static(b"value1"));
    batch.put(cf.id(), Bytes::from_static(b"key2"), Bytes::from_static(b"value2"));
    batch.put(cf.id(), Bytes::from_static(b"key3"), Bytes::from_static(b"value3"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"key1").unwrap(), Some(Bytes::from_static(b"value1")));
    assert_eq!(engine.get(&cf, b"key2").unwrap(), Some(Bytes::from_static(b"value2")));
    assert_eq!(engine.get(&cf, b"key3").unwrap(), Some(Bytes::from_static(b"value3")));
}

#[test]
fn should_apply_operations_in_order_when_batch_commits() {
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
    
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"key"), Bytes::from_static(b"value1"));
    batch.put(cf.id(), Bytes::from_static(b"key"), Bytes::from_static(b"value2"));
    batch.put(cf.id(), Bytes::from_static(b"key"), Bytes::from_static(b"value3"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert - last write wins
    assert_eq!(engine.get(&cf, b"key").unwrap(), Some(Bytes::from_static(b"value3")));
}

#[test]
fn should_handle_empty_batch_without_error() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let batch = WriteBatch::new();

    // Act
    let result = engine.write_batch(&batch);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_apply_delete_after_put_in_same_batch() {
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
    
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"key"), Bytes::from_static(b"value"));
    batch.delete(cf.id(), Bytes::from_static(b"key"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"key").unwrap(), None);
}

#[test]
fn should_delete_existing_key_when_batch_contains_delete() {
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
    
    engine.put(&cf, b"key", b"old_value").expect("put");
    
    let mut batch = WriteBatch::new();
    batch.delete(cf.id(), Bytes::from_static(b"key"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"key").unwrap(), None);
}

#[test]
fn should_overwrite_existing_value_when_batch_puts() {
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
    
    engine.put(&cf, b"key", b"old_value").expect("put");
    
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"key"), Bytes::from_static(b"new_value"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"key").unwrap(), Some(Bytes::from_static(b"new_value")));
}

#[test]
fn should_apply_mixed_operations_in_order() {
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
    
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"key1"), Bytes::from_static(b"value1"));
    batch.delete(cf.id(), Bytes::from_static(b"key2"));
    batch.put(cf.id(), Bytes::from_static(b"key3"), Bytes::from_static(b"value3"));
    batch.put(cf.id(), Bytes::from_static(b"key1"), Bytes::from_static(b"updated1"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"key1").unwrap(), Some(Bytes::from_static(b"updated1")));
    assert_eq!(engine.get(&cf, b"key2").unwrap(), None);
    assert_eq!(engine.get(&cf, b"key3").unwrap(), Some(Bytes::from_static(b"value3")));
}

#[test]
fn should_handle_large_batch_with_many_operations() {
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
    
    let mut batch = WriteBatch::with_capacity(1000);
    for i in 0..1000 {
        let key = format!("key{:04}", i);
        let value = format!("value{}", i);
        batch.put(cf.id(), Bytes::from(key), Bytes::from(value));
    }

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(batch.len(), 1000);
    assert_eq!(
        engine.get(&cf, b"key0000").unwrap(),
        Some(Bytes::from("value0"))
    );
    assert_eq!(
        engine.get(&cf, b"key0999").unwrap(),
        Some(Bytes::from("value999"))
    );
}

#[test]
fn should_persist_batch_across_reopen_when_synced() {
    // Arrange
    let dir = test_temp_dir();
    let path = dir.path().to_path_buf();
    
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: path.clone(),
            },
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        
        let mut batch = WriteBatch::new();
        batch.put(cf.id(), Bytes::from_static(b"key1"), Bytes::from_static(b"value1"));
        batch.put(cf.id(), Bytes::from_static(b"key2"), Bytes::from_static(b"value2"));
        engine.write_batch(&batch).expect("write_batch");
        engine.flush().expect("flush");
    }

    // Act - reopen
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Assert
    assert_eq!(engine.get(&cf, b"key1").unwrap(), Some(Bytes::from_static(b"value1")));
    assert_eq!(engine.get(&cf, b"key2").unwrap(), Some(Bytes::from_static(b"value2")));
}

#[test]
fn should_apply_batch_with_ttl_correctly() {
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
    
    let mut batch = WriteBatch::new();
    batch.put_with_ttl(cf.id(), Bytes::from_static(b"key1"), Bytes::from_static(b"value1"), 3600);
    batch.put(cf.id(), Bytes::from_static(b"key2"), Bytes::from_static(b"value2"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"key1").unwrap(), Some(Bytes::from_static(b"value1")));
    assert_eq!(engine.get(&cf, b"key2").unwrap(), Some(Bytes::from_static(b"value2")));
}

#[test]
fn should_handle_batch_across_multiple_column_families() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf_default = engine.default_column_family();
    let cf1 = engine.create_column_family("cf1", ColumnFamilyConfig::default()).expect("create_cf");
    let cf2 = engine.create_column_family("cf2", ColumnFamilyConfig::default()).expect("create_cf");
    
    let mut batch = WriteBatch::new();
    batch.put(cf_default.id(), Bytes::from_static(b"key_default"), Bytes::from_static(b"value_default"));
    batch.put(cf1.id(), Bytes::from_static(b"key_cf1"), Bytes::from_static(b"value_cf1"));
    batch.put(cf2.id(), Bytes::from_static(b"key_cf2"), Bytes::from_static(b"value_cf2"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf_default, b"key_default").unwrap(),
        Some(Bytes::from_static(b"value_default"))
    );
    assert_eq!(
        engine.get(&cf1, b"key_cf1").unwrap(),
        Some(Bytes::from_static(b"value_cf1"))
    );
    assert_eq!(
        engine.get(&cf2, b"key_cf2").unwrap(),
        Some(Bytes::from_static(b"value_cf2"))
    );
}

#[test]
fn should_isolate_batches_across_column_families() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf_default = engine.default_column_family();
    let cf1 = engine.create_column_family("cf1", ColumnFamilyConfig::default()).expect("create_cf");
    
    let mut batch = WriteBatch::new();
    batch.put(cf1.id(), Bytes::from_static(b"key"), Bytes::from_static(b"value_cf1"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf1, b"key").unwrap(),
        Some(Bytes::from_static(b"value_cf1"))
    );
    assert_eq!(engine.get(&cf_default, b"key").unwrap(), None);
}

#[test]
fn should_handle_concurrent_batches_without_interleaving() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(MidgeEngine::open(opts).expect("open"));
    let mut handles = vec![];

    // Act
    for thread_id in 0..10 {
        let engine_clone = Arc::clone(&engine);
        let handle = std::thread::spawn(move || {
            let cf = engine_clone.default_column_family();
            let mut batch = WriteBatch::new();
            for i in 0..100 {
                let key = format!("thread{:02}_key{:03}", thread_id, i);
                let value = format!("thread{:02}_value{:03}", thread_id, i);
                batch.put(cf.id(), Bytes::from(key), Bytes::from(value));
            }
            engine_clone.write_batch(&batch).expect("write_batch");
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("thread join");
    }

    // Assert - each thread's data should be intact
    let cf = engine.default_column_family();
    for thread_id in 0..10 {
        for i in 0..100 {
            let key = format!("thread{:02}_key{:03}", thread_id, i);
            let expected = format!("thread{:02}_value{:03}", thread_id, i);
            assert_eq!(
                engine.get(&cf, key.as_bytes()).unwrap(),
                Some(Bytes::from(expected))
            );
        }
    }
}

#[test]
fn should_maintain_batch_atomicity_during_concurrent_reads() {
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
    
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"key1"), Bytes::from_static(b"value1"));
    batch.put(cf.id(), Bytes::from_static(b"key2"), Bytes::from_static(b"value2"));
    batch.put(cf.id(), Bytes::from_static(b"key3"), Bytes::from_static(b"value3"));

    // Act - write batch while readers are active
    let reader_engine = Arc::clone(&engine);
    let reader_cf = cf.clone();
    let reader = std::thread::spawn(move || {
        for _ in 0..100 {
            let k1 = reader_engine.get(&reader_cf, b"key1").unwrap();
            let k2 = reader_engine.get(&reader_cf, b"key2").unwrap();
            let k3 = reader_engine.get(&reader_cf, b"key3").unwrap();
            
            // Keys should be all present or all absent (atomicity)
            if k1.is_some() {
                assert!(k2.is_some() && k3.is_some(), "Partial batch visible!");
            }
        }
    });

    engine.write_batch(&batch).expect("write_batch");
    reader.join().expect("reader join");

    // Assert
    assert_eq!(engine.get(&cf, b"key1").unwrap(), Some(Bytes::from_static(b"value1")));
    assert_eq!(engine.get(&cf, b"key2").unwrap(), Some(Bytes::from_static(b"value2")));
    assert_eq!(engine.get(&cf, b"key3").unwrap(), Some(Bytes::from_static(b"value3")));
}

#[test]
fn should_handle_batch_with_duplicate_keys() {
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
    
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"key"), Bytes::from_static(b"value1"));
    batch.put(cf.id(), Bytes::from_static(b"key"), Bytes::from_static(b"value2"));
    batch.delete(cf.id(), Bytes::from_static(b"key"));
    batch.put(cf.id(), Bytes::from_static(b"key"), Bytes::from_static(b"value3"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert - last operation wins
    assert_eq!(engine.get(&cf, b"key").unwrap(), Some(Bytes::from_static(b"value3")));
}

#[test]
fn should_increment_sequence_numbers_for_batch_operations() {
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
    
    let seq_before = engine.current_sequence();
    
    let mut batch = WriteBatch::new();
    batch.put(cf.id(), Bytes::from_static(b"key1"), Bytes::from_static(b"value1"));
    batch.put(cf.id(), Bytes::from_static(b"key2"), Bytes::from_static(b"value2"));
    batch.put(cf.id(), Bytes::from_static(b"key3"), Bytes::from_static(b"value3"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert - sequence should increase
    let seq_after = engine.current_sequence();
    assert!(seq_after > seq_before);
}

#[test]
fn should_handle_batch_with_only_deletes() {
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
    engine.put(&cf, b"key3", b"value3").expect("put");
    
    let mut batch = WriteBatch::new();
    batch.delete(cf.id(), Bytes::from_static(b"key1"));
    batch.delete(cf.id(), Bytes::from_static(b"key2"));
    batch.delete(cf.id(), Bytes::from_static(b"key3"));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(engine.get(&cf, b"key1").unwrap(), None);
    assert_eq!(engine.get(&cf, b"key2").unwrap(), None);
    assert_eq!(engine.get(&cf, b"key3").unwrap(), None);
}

#[test]
fn should_handle_batch_with_binary_data() {
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
    
    let mut batch = WriteBatch::new();
    let binary_key = vec![0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF];
    let binary_value = vec![0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0xFF];
    batch.put(cf.id(), Bytes::from(binary_key.clone()), Bytes::from(binary_value.clone()));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf, &binary_key).unwrap(),
        Some(Bytes::from(binary_value))
    );
}

#[test]
fn should_handle_batch_with_large_keys_and_values() {
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
    
    let mut batch = WriteBatch::new();
    let large_key = vec![b'k'; 1024];
    let large_value = vec![b'v'; 1024 * 1024];
    batch.put(cf.id(), Bytes::from(large_key.clone()), Bytes::from(large_value.clone()));

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(
        engine.get(&cf, &large_key).unwrap(),
        Some(Bytes::from(large_value))
    );
}

#[test]
fn should_preserve_batch_order_across_memtable_flush() {
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
    
    let mut batch = WriteBatch::new();
    for i in 0..1000 {
        let key = format!("key{:04}", i);
        let value = format!("value{}", i);
        batch.put(cf.id(), Bytes::from(key), Bytes::from(value));
    }

    // Act
    engine.write_batch(&batch).expect("write_batch");
    engine.flush().expect("flush");

    // Assert - all keys should be present after flush
    for i in 0..1000 {
        let key = format!("key{:04}", i);
        let expected = format!("value{}", i);
        assert_eq!(
            engine.get(&cf, key.as_bytes()).unwrap(),
            Some(Bytes::from(expected))
        );
    }
}

#[test]
fn should_recover_batches_from_wal_after_restart() {
    // Arrange
    let dir = test_temp_dir();
    let path = dir.path().to_path_buf();
    
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: path.clone(),
            },
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("open");
        let cf = engine.default_column_family();
        
        let mut batch1 = WriteBatch::new();
        batch1.put(cf.id(), Bytes::from_static(b"key1"), Bytes::from_static(b"value1"));
        batch1.put(cf.id(), Bytes::from_static(b"key2"), Bytes::from_static(b"value2"));
        engine.write_batch(&batch1).expect("write_batch");
        
        let mut batch2 = WriteBatch::new();
        batch2.put(cf.id(), Bytes::from_static(b"key3"), Bytes::from_static(b"value3"));
        batch2.delete(cf.id(), Bytes::from_static(b"key1"));
        engine.write_batch(&batch2).expect("write_batch");
        
        engine.flush().expect("flush");
    }

    // Act - reopen and recover
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("open");
    let cf = engine.default_column_family();

    // Assert
    assert_eq!(engine.get(&cf, b"key1").unwrap(), None); // deleted in batch2
    assert_eq!(engine.get(&cf, b"key2").unwrap(), Some(Bytes::from_static(b"value2")));
    assert_eq!(engine.get(&cf, b"key3").unwrap(), Some(Bytes::from_static(b"value3")));
}

#[test]
fn should_handle_batch_with_preallocated_capacity() {
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
    
    let mut batch = WriteBatch::with_capacity(500);
    for i in 0..500 {
        let key = format!("key{:03}", i);
        let value = format!("value{}", i);
        batch.put(cf.id(), Bytes::from(key), Bytes::from(value));
    }

    // Act
    engine.write_batch(&batch).expect("write_batch");

    // Assert
    assert_eq!(batch.len(), 500);
    assert_eq!(
        engine.get(&cf, b"key000").unwrap(),
        Some(Bytes::from("value0"))
    );
    assert_eq!(
        engine.get(&cf, b"key499").unwrap(),
        Some(Bytes::from("value499"))
    );
}
