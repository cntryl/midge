//! Read Latency Smoke Test
//!
//! Quick validation that read latency profiling works across all three paths

use bytes::Bytes;
use midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::time::Instant;
use tempfile::TempDir;

fn generate_key(id: usize) -> Bytes {
    Bytes::from(format!("key{:013}", id))
}

fn generate_value(id: usize) -> Bytes {
    Bytes::from(vec![id as u8; 1000])
}

#[test]
fn should_measure_hot_read_latency() {
    // Arrange
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        memtable_size: 64 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).expect("Failed to open engine");

    // Load 1000 keys into memtable
    for i in 0..1000 {
        engine
            .put(generate_key(i), generate_value(i))
            .expect("Failed to put");
    }

    // Act
    let start = Instant::now();
    for i in 0..100 {
        let _ = engine.get(&generate_key(i)).expect("Read failed");
    }
    let elapsed = start.elapsed();
    let avg_latency_us = elapsed.as_micros() / 100;

    // Assert
    println!("Hot read avg latency: {} µs", avg_latency_us);
    assert!(
        avg_latency_us < 1000,
        "Hot reads should be < 1ms, got {}µs",
        avg_latency_us
    );

    println!("✅ Hot path latency validated");
}

#[test]
fn should_measure_warm_read_latency() {
    // Arrange
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        memtable_size: 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts).expect("Failed to open engine");

    // Load and flush
    for i in 0..1000 {
        engine
            .put(generate_key(i), generate_value(i))
            .expect("Failed to put");
    }
    let _ = engine.flush();

    // Warm up cache
    for i in 0..100 {
        let _ = engine.get(&generate_key(i));
    }

    // Act
    let start = Instant::now();
    for i in 0..100 {
        let _ = engine.get(&generate_key(i)).expect("Read failed");
    }
    let elapsed = start.elapsed();
    let avg_latency_us = elapsed.as_micros() / 100;

    // Assert
    println!("Warm read avg latency: {} µs", avg_latency_us);
    assert!(
        avg_latency_us < 500,
        "Warm reads should be < 500µs, got {}µs",
        avg_latency_us
    );

    println!("✅ Warm path latency validated");
}

#[test]
fn should_measure_cold_read_latency() {
    // Arrange
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: temp_dir.path().to_path_buf(),
        },
        memtable_size: 1024 * 1024,
        enable_compaction: true,
        wal_sync: false,
        ..Default::default()
    };

    let engine = MidgeEngine::open(opts.clone()).expect("Failed to open engine");

    // Load and flush
    for i in 0..1000 {
        engine
            .put(generate_key(i), generate_value(i))
            .expect("Failed to put");
    }
    let _ = engine.flush();

    // Close and reopen to clear cache
    drop(engine);
    let engine = MidgeEngine::open(opts).expect("Failed to reopen engine");

    // Act
    let start = Instant::now();
    for i in 0..10 {
        // Only 10 reads since cold reads are slower
        let _ = engine.get(&generate_key(i)).expect("Read failed");
    }
    let elapsed = start.elapsed();
    let avg_latency_us = elapsed.as_micros() / 10;

    // Assert
    println!("Cold read avg latency: {} µs", avg_latency_us);
    assert!(
        avg_latency_us < 10_000,
        "Cold reads should be < 10ms, got {}µs",
        avg_latency_us
    );

    println!("✅ Cold path latency validated");
}
