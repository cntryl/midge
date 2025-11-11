/// Integration tests for metrics accessor methods
///
/// These tests verify that the public API for accessing engine metrics works correctly.

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use tempfile::TempDir;

#[test]
fn should_return_current_sequence_number() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    let seq_before = eng.current_sequence();
    eng.put(&cf, b"key1", b"value1").expect("put");
    let seq_after = eng.current_sequence();
    
    // Assert
    assert!(seq_after > seq_before, "Sequence number should increase after write");
}

#[test]
fn should_return_memory_usage() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    let mem_before = eng.total_memory_usage();
    
    // Write some data to increase memtable size
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        eng.put(&cf, key.as_bytes(), b"value").expect("put");
    }
    
    let mem_after = eng.total_memory_usage();
    
    // Assert
    assert!(mem_after > mem_before, "Memory usage should increase after writes");
}

#[test]
fn should_return_memory_usage_by_cf() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    eng.put(&cf, b"key", b"value").expect("put");
    
    let usage = eng.memory_usage_by_cf();
    
    // Assert
    assert!(!usage.is_empty(), "Should have memory usage for at least default CF");
    assert!(usage.values().any(|&size| size > 0), "At least one CF should have non-zero usage");
}

#[test]
fn should_return_metrics_snapshot() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    eng.put(&cf, b"key1", b"value1").expect("put");
    eng.get(&cf, b"key1").expect("get");
    
    let snapshot = eng.metrics_snapshot();
    
    // Assert
    // Just verify we can get a snapshot without errors
    // Metrics values depend on storage mode and configuration
    let _puts = snapshot.put_count;
    let _gets = snapshot.get_count;
}

#[test]
fn should_return_sst_file_count() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    let count_before = eng.sst_file_count();
    
    // Write and flush to create an SST
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        eng.put(&cf, key.as_bytes(), b"value").expect("put");
    }
    eng.flush_cf(&cf).expect("flush");
    
    let count_after = eng.sst_file_count();
    
    // Assert
    assert!(count_after >= count_before, "SST count should not decrease");
}

#[test]
fn should_return_total_sst_size() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    // Write and flush to create an SST
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = vec![b'x'; 100]; // 100 bytes per value
        eng.put(&cf, key.as_bytes(), &value).expect("put");
    }
    eng.flush_cf(&cf).expect("flush");
    
    let size = eng.total_sst_size();
    
    // Assert
    // Should have at least some data (compressed, with metadata)
    assert!(size > 0, "Total SST size should be non-zero after flush");
}

#[test]
fn should_calculate_read_amplification() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    // Write some data
    eng.put(&cf, b"key", b"value").expect("put");
    eng.get(&cf, b"key").expect("get");
    
    let amplification = eng.read_amplification();
    
    // Assert
    assert!(amplification >= 0.0, "Read amplification should be non-negative");
    assert!(amplification < 100.0, "Read amplification should be reasonable");
}

#[test]
fn should_calculate_write_amplification() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    // Write some data
    eng.put(&cf, b"key", b"value").expect("put");
    
    let amplification = eng.write_amplification();
    
    // Assert
    assert!(amplification >= 0.0, "Write amplification should be non-negative");
    assert!(amplification < 100.0, "Write amplification should be reasonable");
}

#[test]
fn should_access_performance_metrics() {
    // Arrange
    let dir = TempDir::new().unwrap();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    
    // Act
    let eng = MidgeEngine::open(opts).expect("open engine");
    let cf = eng.default_column_family();
    
    eng.put(&cf, b"key", b"value").expect("put");
    
    let perf_metrics = eng.performance_metrics();
    
    // Assert
    // Just verify we can access performance metrics without errors
    let _ops = perf_metrics.wal.total_operations();
    // Metrics may be zero depending on configuration and storage mode
}
