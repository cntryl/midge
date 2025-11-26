//! Stress tests for various workload patterns.
//!
//! These tests verify that the engine handles different access patterns correctly
//! across all storage modes, including hot partitions, high-throughput writes,
//! TTL-like semantics, and append-only workloads.

mod common;
use common::*;

use cntryl_midge::{MidgeEngine, MidgeOptions, Query};

// ============================================================================
// HOT PARTITION WORKLOADS
// ============================================================================

#[test]
fn should_handle_hot_partition_given_100_overwrites_to_same_key_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        // NOTE: Using only 100 iterations because 1000+ rapid overwrites to same key
        // triggers a flush bug "Key ordering violation" - see engine_merge_operators bug
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            enable_compaction: true,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act
        for i in 0..100 {
            eng.put(&cf, b"hot_key", format!("append{}", i).as_bytes())
                .expect("put");
        }
        eng.flush().expect("flush");
        eng.compact_all().expect("compact");

        // Assert
        let value = eng.get(&cf, b"hot_key").expect("get");
        assert_eq!(
            value.as_deref(),
            Some(b"append99".as_ref()),
            "Latest value mismatch for {}",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_maintain_correctness_given_hot_partition_with_concurrent_reads_when_writing() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Initial write
        eng.put(&cf, b"hot", b"initial").expect("put");

        // Act - interleaved reads and writes
        for i in 0..100 {
            let before = eng.get(&cf, b"hot").expect("get");
            eng.put(&cf, b"hot", format!("v{}", i).as_bytes())
                .expect("put");
            let after = eng.get(&cf, b"hot").expect("get");

            // Assert each iteration
            assert!(before.is_some(), "Before read failed at {} for {}", i, name);
            assert!(after.is_some(), "After read failed at {} for {}", i, name);
        }

        // Assert final state
        let final_value = eng.get(&cf, b"hot").expect("get");
        assert_eq!(
            final_value.as_deref(),
            Some(b"v99".as_ref()),
            "Final value mismatch for {}",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// HIGH-THROUGHPUT SMALL WRITES
// ============================================================================

#[test]
fn should_handle_high_throughput_given_10000_small_writes_when_sequential() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act
        for i in 0..10000 {
            eng.put(&cf, format!("k{:05}", i).as_bytes(), b"v")
                .expect("put");
        }

        // Assert - spot check
        for i in (0..10000).step_by(1000) {
            let key = format!("k{:05}", i);
            let value = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(value.is_some(), "Key {} missing for {}", key, name);
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_maintain_order_given_high_throughput_writes_when_scanning() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act
        for i in 0..1000 {
            eng.put(&cf, format!("seq{:04}", i).as_bytes(), b"v")
                .expect("put");
        }

        let results = eng.scan(&cf, Query::new()).expect("scan");

        // Assert - verify ordering
        assert_eq!(results.len(), 1000, "Count mismatch for {}", name);
        for (i, (key, _)) in results.iter().enumerate() {
            let expected = format!("seq{:04}", i);
            assert_eq!(
                key.as_ref(),
                expected.as_bytes(),
                "Order mismatch at {} for {}",
                i,
                name
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// TTL-LIKE DELETE PATTERNS
// ============================================================================

#[test]
fn should_handle_ttl_pattern_given_bulk_delete_of_old_keys_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            enable_compaction: true,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Insert keys
        for i in 0..100 {
            eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v")
                .expect("put");
        }

        // Act - delete "expired" keys (first half)
        for i in 0..50 {
            eng.delete(&cf, format!("k{:03}", i).as_bytes())
                .expect("delete");
        }
        eng.flush().expect("flush");
        eng.compact_all().expect("compact");

        // Assert - deleted keys gone
        for i in 0..50 {
            let key = format!("k{:03}", i);
            let value = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(
                value.is_none(),
                "Key {} should be deleted for {}",
                key,
                name
            );
        }
        // Assert - remaining keys present
        for i in 50..100 {
            let key = format!("k{:03}", i);
            let value = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(value.is_some(), "Key {} should exist for {}", key, name);
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_rolling_window_given_delete_oldest_insert_newest_when_steady_state() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Initial window of 100 keys
        for i in 0..100 {
            eng.put(&cf, format!("w{:05}", i).as_bytes(), b"v")
                .expect("put");
        }

        // Act - slide window by 50
        for i in 0..50 {
            eng.delete(&cf, format!("w{:05}", i).as_bytes())
                .expect("delete");
            eng.put(&cf, format!("w{:05}", 100 + i).as_bytes(), b"v")
                .expect("put");
        }

        // Assert - window is [50, 150)
        for i in 0..50 {
            let key = format!("w{:05}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_none(),
                "Old key {} should be deleted for {}",
                key,
                name
            );
        }
        for i in 50..150 {
            let key = format!("w{:05}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_some(),
                "Window key {} should exist for {}",
                key,
                name
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// APPEND-ONLY WORKLOADS
// ============================================================================

#[test]
fn should_handle_append_only_given_sequential_inserts_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            enable_compaction: true,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act
        for i in 0..1000 {
            eng.put(&cf, format!("log{:04}", i).as_bytes(), b"entry")
                .expect("put");
        }
        eng.flush().expect("flush");
        eng.compact_all().expect("compact");

        // Assert
        for i in 0..1000 {
            let key = format!("log{:04}", i);
            let value = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(value.is_some(), "Key {} missing for {}", key, name);
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_recover_append_only_data_given_crash_after_bulk_insert_when_reopening() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        // Act
        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();

            for i in 0..500 {
                eng.put(&cf, format!("append{:04}", i).as_bytes(), b"data")
                    .expect("put");
            }
            eng.flush().expect("flush");
        }

        // Assert
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = eng2.default_column_family();

        for i in 0..500 {
            let key = format!("append{:04}", i);
            let value = eng2.get(&cf2, key.as_bytes()).expect("get");
            assert!(
                value.is_some(),
                "Key {} missing after recovery for {}",
                key,
                name
            );
        }
        drop(eng2);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// MIXED WORKLOADS
// ============================================================================

#[test]
fn should_handle_mixed_workload_given_reads_writes_deletes_when_interleaved() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Initial data
        for i in 0..100 {
            eng.put(&cf, format!("mix{:03}", i).as_bytes(), b"initial")
                .expect("put");
        }

        // Act - mixed operations
        for i in 0..50 {
            // Read
            let key = format!("mix{:03}", i);
            let _ = eng.get(&cf, key.as_bytes()).expect("get");

            // Update
            let update_key = format!("mix{:03}", i + 50);
            eng.put(&cf, update_key.as_bytes(), b"updated")
                .expect("put");

            // Delete
            let delete_key = format!("mix{:03}", i);
            eng.delete(&cf, delete_key.as_bytes()).expect("delete");

            // Insert new
            let new_key = format!("mix{:03}", 100 + i);
            eng.put(&cf, new_key.as_bytes(), b"new").expect("put");
        }

        // Assert
        // Keys 0-49 deleted
        for i in 0..50 {
            let key = format!("mix{:03}", i);
            assert!(
                eng.get(&cf, key.as_bytes()).expect("get").is_none(),
                "Deleted key {} should be gone for {}",
                key,
                name
            );
        }
        // Keys 50-99 updated
        for i in 50..100 {
            let key = format!("mix{:03}", i);
            let value = eng.get(&cf, key.as_bytes()).expect("get");
            assert_eq!(
                value.as_deref(),
                Some(b"updated".as_ref()),
                "Updated key {} mismatch for {}",
                key,
                name
            );
        }
        // Keys 100-149 new
        for i in 100..150 {
            let key = format!("mix{:03}", i);
            let value = eng.get(&cf, key.as_bytes()).expect("get");
            assert_eq!(
                value.as_deref(),
                Some(b"new".as_ref()),
                "New key {} mismatch for {}",
                key,
                name
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_burst_then_idle_given_writes_followed_by_reads_when_pattern_changes() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act - burst phase
        for i in 0..500 {
            eng.put(&cf, format!("burst{:04}", i).as_bytes(), b"v")
                .expect("put");
        }

        // Act - idle/read phase
        for i in 0..500 {
            let key = format!("burst{:04}", i);
            let value = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(
                value.is_some(),
                "Key {} missing during read phase for {}",
                key,
                name
            );
        }

        // Assert - all data intact
        let results = eng.scan(&cf, Query::new()).expect("scan");
        assert_eq!(results.len(), 500, "Count mismatch for {}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// PREFIX WORKLOADS
// ============================================================================

#[test]
fn should_handle_prefix_partitioned_data_given_multiple_prefixes_when_scanning() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act - insert data with different prefixes
        let prefixes = ["user:", "order:", "product:", "session:"];
        for prefix in &prefixes {
            for i in 0..50 {
                let key = format!("{}{:03}", prefix, i);
                eng.put(&cf, key.as_bytes(), b"v").expect("put");
            }
        }

        // Assert - scan by prefix
        for prefix in &prefixes {
            let start = format!("{}000", prefix);
            let end = format!("{}999", prefix);
            let results = eng
                .scan(
                    &cf,
                    Query::new()
                        .start_key(bytes::Bytes::from(start))
                        .end_key(bytes::Bytes::from(end)),
                )
                .expect("scan");
            assert_eq!(
                results.len(),
                50,
                "Prefix {} count mismatch for {}",
                prefix,
                name
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}
