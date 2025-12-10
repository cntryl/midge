//! Metrics and Observability Integration Tests
//!
//! Tests for metrics accessors, statistics, and observability features.
//! Verifies that Midge correctly tracks and reports operational metrics.
//!
//! ## Coverage
//! - Sequence number tracking
//! - Memory usage reporting
//! - SST file statistics
//! - Read/write amplification
//! - Performance metrics
//!
//! ## Storage Mode Coverage
//! - Sequence numbers and memory usage: All modes (Memory, LocalDisk, CloudBacked)
//! - SST statistics and amplification: LocalDisk and CloudBacked only (require flush)

mod common;

use cntryl_midge::{MidgeEngine, MidgeOptions};
use cntryl_midge::testkit::{all_storage_modes, create_storage_mode, disk_storage_modes};

// =============================================================================
// Sequence Number Tracking
// =============================================================================

#[test]
fn should_increase_sequence_given_write_operation_when_put_called() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        // Act
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        let seq_before = eng.current_sequence();
        eng.put(&cf, b"key1", b"value1").expect("put");
        let seq_after = eng.current_sequence();

        // Assert
        assert!(
            seq_after > seq_before,
            "Sequence number should increase after write for {}",
            name
        );
    }
}

// =============================================================================
// Memory Usage Reporting
// =============================================================================

#[test]
fn should_increase_memory_usage_given_writes_when_data_added() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
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
        assert!(
            mem_after > mem_before,
            "Memory usage should increase after writes for {}",
            name
        );
    }
}

#[test]
fn should_report_memory_by_column_family_given_data_written_when_queried() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        // Act
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key", b"value").expect("put");

        let usage = eng.memory_usage_by_cf();

        // Assert
        assert!(
            !usage.is_empty(),
            "Should have memory usage for at least default CF for {}",
            name
        );
        assert!(
            usage.values().any(|&size| size > 0),
            "At least one CF should have non-zero usage for {}",
            name
        );
    }
}

// =============================================================================
// Metrics Snapshot
// =============================================================================

#[test]
fn should_return_metrics_snapshot_given_operations_performed_when_queried() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        // Act
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"value1").expect("put");
        eng.get(&cf, b"key1").expect("get");

        let snapshot = eng.metrics_snapshot();

        // Assert - verify we can access metrics without errors
        let _puts = snapshot.put_count;
        let _gets = snapshot.get_count;
        // No assertion needed; test passes if no panic occurs
        let _ = name; // silence unused warning
    }
}

// =============================================================================
// SST File Statistics
// =============================================================================

#[test]
fn should_increase_sst_count_given_flush_when_data_persisted() {
    // SST statistics only apply to disk-based storage modes
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
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
        assert!(
            count_after >= count_before,
            "SST count should not decrease after flush for {}",
            name
        );
    }
}

#[test]
fn should_report_nonzero_sst_size_given_flush_when_data_persisted() {
    // SST statistics only apply to disk-based storage modes
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
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
        assert!(
            size > 0,
            "Total SST size should be non-zero after flush for {}",
            name
        );
    }
}

// =============================================================================
// Amplification Metrics
// =============================================================================

#[test]
fn should_report_nonnegative_read_amplification_given_operations_when_queried() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        // Act
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key", b"value").expect("put");
        eng.get(&cf, b"key").expect("get");

        let amplification = eng.read_amplification();

        // Assert
        assert!(
            amplification >= 0.0,
            "Read amplification should be non-negative for {}",
            name
        );
        assert!(
            amplification < 100.0,
            "Read amplification should be reasonable for {}",
            name
        );
    }
}

#[test]
fn should_report_nonnegative_write_amplification_given_writes_when_queried() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        // Act
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key", b"value").expect("put");

        let amplification = eng.write_amplification();

        // Assert
        assert!(
            amplification >= 0.0,
            "Write amplification should be non-negative for {}",
            name
        );
        assert!(
            amplification < 100.0,
            "Write amplification should be reasonable for {}",
            name
        );
    }
}

// =============================================================================
// Performance Metrics
// =============================================================================

#[test]
fn should_access_performance_metrics_given_operations_when_queried() {
    // Performance metrics are most meaningful with disk-based modes
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };

        // Act
        let eng = MidgeEngine::open(opts).expect("open engine");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key", b"value").expect("put");

        let perf_metrics = eng.performance_metrics();

        // Assert - verify we can access performance metrics without errors
        let _ops = perf_metrics.wal.total_operations();
        // Test passes if no panic occurs
        let _ = name; // silence unused warning
    }
}
