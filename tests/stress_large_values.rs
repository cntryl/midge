//! Stress tests for large value handling.
//!
//! These tests verify that the engine correctly handles large values across
//! all storage modes, including mixed value sizes, backpressure, crash recovery,
//! and snapshot visibility.

mod common;
use common::*;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query};

// ============================================================================
// LARGE VALUE PUT/GET TESTS
// ============================================================================

#[test]
fn should_store_large_value_given_16kb_payload_when_put() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        let large = Bytes::from(vec![b'x'; 1024 * 16]);

        // Act
        eng.put(&cf, b"large_key", large.as_ref()).expect("put");
        let result = eng.get(&cf, b"large_key").expect("get");

        // Assert
        assert_eq!(result, Some(large.clone()), "Failed for {}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_flush_memtable_given_mixed_value_sizes_when_small_with_large_present() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act
        eng.put(&cf, b"small", b"s").expect("put small");
        let large = Bytes::from(vec![b'x'; 1024 * 16]);
        eng.put(&cf, b"large", large.as_ref()).expect("put large");
        eng.flush().expect("flush");

        // Assert
        assert_eq!(
            eng.get(&cf, b"small").expect("get small"),
            Some(Bytes::from("s")),
            "Small value mismatch for {}",
            name
        );
        assert_eq!(
            eng.get(&cf, b"large").expect("get large"),
            Some(large),
            "Large value mismatch for {}",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// BACKPRESSURE TESTS
// ============================================================================

#[test]
fn should_apply_backpressure_given_flood_of_large_writes_when_memtable_fills() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 64 * 1024, // Small memtable to trigger pressure
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Act
        let large = vec![b'y'; 1024 * 8];
        for i in 0..50u8 {
            let r = eng.put(&cf, &[i], large.as_slice());
            assert!(r.is_ok(), "Write {} failed for {}", i, name);
        }

        // Assert
        assert!(
            eng.get(&cf, &[0]).expect("get").is_some(),
            "Failed for {}",
            name
        );
        assert!(
            eng.get(&cf, &[49]).expect("get").is_some(),
            "Failed for {}",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_burst_of_large_values_given_100_writes_when_8kb_each() {
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
        for i in 0..100u32 {
            let large = vec![(i % 256) as u8; 1024 * 8];
            eng.put(&cf, format!("burst_{:03}", i).as_bytes(), &large)
                .expect("put");
        }

        // Assert
        for i in 0..100u32 {
            let key = format!("burst_{:03}", i);
            let result = eng.get(&cf, key.as_bytes()).expect("get");
            assert!(result.is_some(), "Key {} missing for {}", key, name);
            assert_eq!(
                result.unwrap().len(),
                1024 * 8,
                "Size mismatch for {} in {}",
                key,
                name
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// CRASH RECOVERY TESTS
// ============================================================================

#[test]
fn should_recover_large_values_given_crash_after_put_when_reopening() {
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

            for i in 0..10u8 {
                let large = vec![i; 1024 * 4];
                eng.put(&cf, &[i], large.as_slice()).expect("put");
            }
            // No explicit flush - rely on WAL for recovery
        }

        // Assert
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = eng2.default_column_family();

        for i in 0..10u8 {
            let result = eng2.get(&cf2, &[i]).expect("get");
            assert!(
                result.is_some(),
                "Key {} missing after recovery for {}",
                i,
                name
            );
            let value = result.unwrap();
            assert_eq!(
                value.len(),
                1024 * 4,
                "Size mismatch for key {} in {}",
                i,
                name
            );
            assert!(
                value.iter().all(|&b| b == i),
                "Content mismatch for key {} in {}",
                i,
                name
            );
        }
        drop(eng2);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_recover_mixed_sizes_given_crash_after_flush_when_reopening() {
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

            // Mix of small and large values
            eng.put(&cf, b"tiny", b"t").expect("put tiny");
            eng.put(&cf, b"small", b"small_value").expect("put small");
            let medium = vec![b'm'; 1024];
            eng.put(&cf, b"medium", &medium).expect("put medium");
            let large = vec![b'L'; 1024 * 32];
            eng.put(&cf, b"large", &large).expect("put large");

            eng.flush().expect("flush");
        }

        // Assert
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = eng2.default_column_family();

        assert_eq!(
            eng2.get(&cf2, b"tiny").expect("get").map(|v| v.len()),
            Some(1),
            "Tiny mismatch for {}",
            name
        );
        assert_eq!(
            eng2.get(&cf2, b"small").expect("get").map(|v| v.len()),
            Some(11),
            "Small mismatch for {}",
            name
        );
        assert_eq!(
            eng2.get(&cf2, b"medium").expect("get").map(|v| v.len()),
            Some(1024),
            "Medium mismatch for {}",
            name
        );
        assert_eq!(
            eng2.get(&cf2, b"large").expect("get").map(|v| v.len()),
            Some(1024 * 32),
            "Large mismatch for {}",
            name
        );
        drop(eng2);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// SNAPSHOT TESTS
// ============================================================================

#[test]
fn should_preserve_snapshot_view_given_large_value_overwrite_when_reading_snapshot() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"k", b"v1").expect("put initial");
        let snap = eng.snapshot();

        // Act
        let large = Bytes::from(vec![b'z'; 1024 * 12]);
        eng.put(&cf, b"k", large.as_ref()).expect("put large");

        // Assert
        assert_eq!(
            snap.get(&eng, &cf, b"k").expect("snapshot get"),
            Some(Bytes::from("v1")),
            "Snapshot should see old value for {}",
            name
        );
        assert_eq!(
            eng.get(&cf, b"k").expect("current get"),
            Some(large),
            "Current should see new value for {}",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_isolate_snapshot_given_multiple_large_overwrites_when_reading() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        let v1 = Bytes::from(vec![b'1'; 1024 * 4]);
        eng.put(&cf, b"key", v1.as_ref()).expect("put v1");
        let snap1 = eng.snapshot();

        let v2 = Bytes::from(vec![b'2'; 1024 * 8]);
        eng.put(&cf, b"key", v2.as_ref()).expect("put v2");
        let snap2 = eng.snapshot();

        // Act
        let v3 = Bytes::from(vec![b'3'; 1024 * 16]);
        eng.put(&cf, b"key", v3.as_ref()).expect("put v3");

        // Assert
        assert_eq!(
            snap1.get(&eng, &cf, b"key").expect("snap1 get"),
            Some(v1),
            "snap1 mismatch for {}",
            name
        );
        assert_eq!(
            snap2.get(&eng, &cf, b"key").expect("snap2 get"),
            Some(v2),
            "snap2 mismatch for {}",
            name
        );
        assert_eq!(
            eng.get(&cf, b"key").expect("current get"),
            Some(v3),
            "current mismatch for {}",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// SCAN WITH LARGE VALUES
// ============================================================================

#[test]
fn should_scan_correctly_given_mixed_size_values_when_iterating() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Insert values of varying sizes
        eng.put(&cf, b"a_tiny", b"t").expect("put");
        eng.put(&cf, b"b_small", &[b's'; 100]).expect("put");
        eng.put(&cf, b"c_medium", &vec![b'm'; 1024]).expect("put");
        eng.put(&cf, b"d_large", &vec![b'L'; 1024 * 16])
            .expect("put");

        // Act
        let results = eng.scan(&cf, Query::new()).expect("scan");

        // Assert
        assert_eq!(results.len(), 4, "Expected 4 results for {}", name);
        assert_eq!(results[0].0.as_ref(), b"a_tiny", "Key order for {}", name);
        assert_eq!(results[0].1.len(), 1, "Size mismatch for {}", name);
        assert_eq!(results[3].0.as_ref(), b"d_large", "Key order for {}", name);
        assert_eq!(results[3].1.len(), 1024 * 16, "Size mismatch for {}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// DELETE WITH LARGE VALUES
// ============================================================================

#[test]
fn should_delete_large_value_given_existing_key_when_delete() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        let large = vec![b'D'; 1024 * 32];
        eng.put(&cf, b"to_delete", &large).expect("put");
        assert!(eng.get(&cf, b"to_delete").expect("get").is_some());

        // Act
        eng.delete(&cf, b"to_delete").expect("delete");

        // Assert
        assert!(
            eng.get(&cf, b"to_delete").expect("get").is_none(),
            "Key should be deleted for {}",
            name
        );
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_reclaim_space_given_large_value_deletion_when_compacting() {
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

        // Write and flush large values
        for i in 0..20u8 {
            let large = vec![i; 1024 * 8];
            eng.put(&cf, &[b'k', i], &large).expect("put");
        }
        eng.flush().expect("flush");

        // Act - delete half and compact
        for i in 0..10u8 {
            eng.delete(&cf, &[b'k', i]).expect("delete");
        }
        eng.flush().expect("flush");
        eng.compact_all().expect("compact");

        // Assert - deleted keys gone, remaining keys present
        for i in 0..10u8 {
            assert!(
                eng.get(&cf, &[b'k', i]).expect("get").is_none(),
                "Deleted key {} should be gone for {}",
                i,
                name
            );
        }
        for i in 10..20u8 {
            let result = eng.get(&cf, &[b'k', i]).expect("get");
            assert!(
                result.is_some(),
                "Remaining key {} should exist for {}",
                i,
                name
            );
        }
        drop(eng);
        eprintln!("✓ {}", name);
    }
}
