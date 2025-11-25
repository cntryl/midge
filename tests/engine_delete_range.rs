//! Delete Range Operation Tests
//!
//! Tests for range deletion (range tombstone) functionality.
//!
//! # Test Categories
//!
//! - Basic range deletion: simple delete_range operations
//! - Scan/get behavior: visibility of deleted keys
//! - Recovery: persistence across restarts
//! - Compaction: tombstone handling during compaction
//! - Snapshot isolation: range deletes with MVCC
//! - Edge cases: empty ranges, overlapping ranges, interleaved ops
//!
//! # Storage Mode Coverage
//!
//! All tests run on both LocalDisk and CloudBacked modes via `disk_storage_modes()`.

mod common;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query};
use common::{create_storage_mode, disk_storage_modes, DurabilityTestContext};

// ============================================================================
// BASIC RANGE DELETION TESTS
// ============================================================================

#[test]
fn should_delete_keys_in_range_given_delete_range_when_querying() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"a", b"1").expect("put");
        eng.put(&cf, b"b", b"2").expect("put");
        eng.put(&cf, b"c", b"3").expect("put");
        eng.put(&cf, b"d", b"4").expect("put");
        eng.put(&cf, b"e", b"5").expect("put");

        // Act: delete range [b, d)
        eng.delete_range(&cf, b"b", b"d").expect("delete_range");

        // Assert: keys b and c are deleted, others remain
        assert_eq!(eng.get(&cf, b"a").expect("get"), Some(Bytes::from("1")));
        assert_eq!(eng.get(&cf, b"b").expect("get"), None, "{}: b deleted", name);
        assert_eq!(eng.get(&cf, b"c").expect("get"), None, "{}: c deleted", name);
        assert_eq!(eng.get(&cf, b"d").expect("get"), Some(Bytes::from("4")));
        assert_eq!(eng.get(&cf, b"e").expect("get"), Some(Bytes::from("5")));
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_delete_keys_across_levels_given_flushed_data_when_delete_range() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Data in first SST
        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.put(&cf, b"key2", b"val2").expect("put");
        eng.put(&cf, b"key3", b"val3").expect("put");
        eng.flush().expect("flush");

        // Data in second SST
        eng.put(&cf, b"key4", b"val4").expect("put");
        eng.put(&cf, b"key5", b"val5").expect("put");
        eng.flush().expect("flush");

        // Act - delete range spanning both SSTs
        eng.delete_range(&cf, b"key2", b"key5").expect("delete_range");

        // Assert
        assert!(eng.get(&cf, b"key1").expect("get").is_some());
        assert!(eng.get(&cf, b"key2").expect("get").is_none(), "{}: key2", name);
        assert!(eng.get(&cf, b"key3").expect("get").is_none(), "{}: key3", name);
        assert!(eng.get(&cf, b"key4").expect("get").is_none(), "{}: key4", name);
        assert!(eng.get(&cf, b"key5").expect("get").is_some());
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_empty_range_given_start_equals_end_when_delete_range() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.put(&cf, b"key2", b"val2").expect("put");

        // Act - empty range
        eng.delete_range(&cf, b"key1", b"key1").expect("delete_range");

        // Assert - no keys deleted
        assert!(eng.get(&cf, b"key1").expect("get").is_some(), "{}", name);
        assert!(eng.get(&cf, b"key2").expect("get").is_some(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// SCAN/GET BEHAVIOR TESTS
// ============================================================================

#[test]
fn should_hide_deleted_range_in_scan_given_delete_range_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..10 {
            let key = format!("key{:02}", i);
            let val = format!("val{}", i);
            eng.put(&cf, key.as_bytes(), val.as_bytes()).expect("put");
        }

        // Act: delete range [key03, key07)
        eng.delete_range(&cf, b"key03", b"key07").expect("delete_range");

        let results = eng
            .scan(
                &cf,
                Query::new()
                    .start_key(Bytes::from("key00"))
                    .end_key(Bytes::from("key10")),
            )
            .expect("scan");

        // Assert: scan shows 6 keys (keys 03-06 deleted)
        assert_eq!(results.len(), 6, "{}: expected 6 results", name);
        let expected = vec![
            (Bytes::from("key00"), Bytes::from("val0")),
            (Bytes::from("key01"), Bytes::from("val1")),
            (Bytes::from("key02"), Bytes::from("val2")),
            (Bytes::from("key07"), Bytes::from("val7")),
            (Bytes::from("key08"), Bytes::from("val8")),
            (Bytes::from("key09"), Bytes::from("val9")),
        ];
        assert_eq!(results, expected, "{}: results match", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_handle_large_range_deletion_given_many_keys_when_deleting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..1000 {
            let key = format!("key{:06}", i);
            eng.put(&cf, key.as_bytes(), b"value").expect("put");
        }
        eng.flush().expect("flush");

        // Act - delete large range
        eng.delete_range(&cf, b"key000100", b"key000900").expect("delete_range");

        // Assert - spot check
        assert!(eng.get(&cf, b"key000050").expect("get").is_some(), "{}", name);
        assert!(eng.get(&cf, b"key000500").expect("get").is_none(), "{}", name);
        assert!(eng.get(&cf, b"key000950").expect("get").is_some(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// RECOVERY TESTS
// ============================================================================

#[test]
fn should_persist_delete_range_given_wal_when_recovering() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                wal_sync: true,
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();

            eng.put(&cf, b"key1", b"value1").expect("put");
            eng.put(&cf, b"key2", b"value2").expect("put");
            eng.put(&cf, b"key3", b"value3").expect("put");
            eng.put(&cf, b"key4", b"value4").expect("put");
            eng.put(&cf, b"key5", b"value5").expect("put");

            // Act - delete range
            eng.delete_range(&cf, b"key2", b"key4").expect("delete_range");
        }

        // Assert - reopen and verify
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = eng2.default_column_family();

        assert_eq!(eng2.get(&cf2, b"key1").expect("get"), Some(Bytes::from("value1")), "{}", name);
        assert_eq!(eng2.get(&cf2, b"key2").expect("get"), None, "{}", name);
        assert_eq!(eng2.get(&cf2, b"key3").expect("get"), None, "{}", name);
        assert_eq!(eng2.get(&cf2, b"key4").expect("get"), Some(Bytes::from("value4")), "{}", name);
        assert_eq!(eng2.get(&cf2, b"key5").expect("get"), Some(Bytes::from("value5")), "{}", name);
        drop(eng2);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_recover_range_tombstone_given_no_flush_when_restarting() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                wal_sync: true,
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();

            eng.put(&cf, b"key1", b"val1").expect("put");
            eng.put(&cf, b"key2", b"val2").expect("put");
            eng.put(&cf, b"key3", b"val3").expect("put");

            // Act - delete range without flush
            eng.delete_range(&cf, b"key1", b"key3").expect("delete_range");
        }

        // Assert - reopen and check
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = eng2.default_column_family();

        assert!(eng2.get(&cf2, b"key1").expect("get").is_none(), "{}", name);
        assert!(eng2.get(&cf2, b"key2").expect("get").is_none(), "{}", name);
        assert!(eng2.get(&cf2, b"key3").expect("get").is_some(), "{}", name);
        drop(eng2);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_apply_delete_range_after_crash_given_flushed_tombstone_when_recovering() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            let cf = eng.default_column_family();

            for i in 0..100 {
                eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v").expect("put");
            }
            eng.flush().expect("flush");

            // Apply range delete and flush to SST
            eng.delete_range(&cf, b"k020", b"k080").expect("delete_range");
            eng.flush().expect("flush");
            // Simulate crash - no compaction
        }

        // Assert - reopen and scan
        let opts2 = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            ..Default::default()
        };
        let eng2 = MidgeEngine::open(opts2).expect("reopen");
        let cf2 = eng2.default_column_family();

        let results = eng2
            .scan(
                &cf2,
                Query::new()
                    .start_key(Bytes::from("k000"))
                    .end_key(Bytes::from("k100")),
            )
            .expect("scan");

        // 100 - 60 deleted = 40 remaining
        assert_eq!(results.len(), 40, "{}", name);
        drop(eng2);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// COMPACTION TESTS
// ============================================================================

#[test]
fn should_apply_range_tombstone_during_compaction_given_flushed_data_when_compacting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..20 {
            let key = format!("key{:03}", i);
            eng.put(&cf, key.as_bytes(), b"val").expect("put");
        }
        eng.flush().expect("flush");

        eng.delete_range(&cf, b"key005", b"key015").expect("delete_range");
        eng.flush().expect("flush");

        // Act - compact
        eng.compact_range(&cf, Some(b""), Some(b"~")).expect("compact");

        // Assert
        assert!(eng.get(&cf, b"key004").expect("get").is_some(), "{}", name);
        assert!(eng.get(&cf, b"key010").expect("get").is_none(), "{}", name);
        assert!(eng.get(&cf, b"key015").expect("get").is_some(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_not_resurrect_deleted_keys_given_compaction_when_range_delete_applied() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key5", b"old_val").expect("put");
        eng.flush().expect("flush");
        eng.compact_range(&cf, Some(b""), Some(b"~")).expect("compact 1");

        eng.delete_range(&cf, b"key0", b"key9").expect("delete_range");
        eng.flush().expect("flush");

        // Act - compact again
        eng.compact_range(&cf, Some(b""), Some(b"~")).expect("compact 2");

        // Assert - key should not resurrect
        assert!(eng.get(&cf, b"key5").expect("get").is_none(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// SNAPSHOT ISOLATION TESTS
// ============================================================================

#[test]
fn should_preserve_snapshot_view_given_delete_range_after_snapshot_when_reading() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.put(&cf, b"key2", b"val2").expect("put");
        eng.flush().expect("flush");

        let snapshot = eng.snapshot();

        // Act - delete range after snapshot
        eng.delete_range(&cf, b"key1", b"key3").expect("delete_range");
        eng.flush().expect("flush");

        // Assert - snapshot sees original values
        assert_eq!(
            eng.get_at(&cf, b"key1", &snapshot).expect("get_at"),
            Some(Bytes::from("val1")),
            "{}: snapshot sees key1",
            name
        );

        // Current view doesn't see deleted keys
        assert!(eng.get(&cf, b"key1").expect("get").is_none(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_include_deleted_range_in_snapshot_scan_given_delete_after_snapshot_when_scanning() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..100 {
            eng.put(&cf, format!("r{:03}", i).as_bytes(), b"v").expect("put");
        }
        eng.flush().expect("flush");

        let snapshot = eng.snapshot();

        // Act - delete range after snapshot
        eng.delete_range(&cf, b"r020", b"r080").expect("delete_range");
        eng.flush().expect("flush");

        let results = eng
            .scan_at(
                &cf,
                Query::new()
                    .start_key(Bytes::from("r000"))
                    .end_key(Bytes::from("r100")),
                &snapshot,
            )
            .expect("scan_at");

        // Assert - snapshot should see all 100 original keys
        assert_eq!(results.len(), 100, "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// OVERLAPPING AND INTERLEAVED OPERATIONS TESTS
// ============================================================================

#[test]
fn should_merge_overlapping_ranges_given_multiple_delete_ranges_when_deleting() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        for i in 0..10 {
            let key = format!("key{:02}", i);
            eng.put(&cf, key.as_bytes(), b"val").expect("put");
        }
        eng.flush().expect("flush");

        // Act - overlapping ranges
        eng.delete_range(&cf, b"key02", b"key06").expect("delete_range 1");
        eng.delete_range(&cf, b"key04", b"key08").expect("delete_range 2");

        // Assert - union of ranges deleted (keys 02-07)
        assert!(eng.get(&cf, b"key01").expect("get").is_some(), "{}", name);
        assert!(eng.get(&cf, b"key02").expect("get").is_none(), "{}", name);
        assert!(eng.get(&cf, b"key05").expect("get").is_none(), "{}", name);
        assert!(eng.get(&cf, b"key07").expect("get").is_none(), "{}", name);
        assert!(eng.get(&cf, b"key08").expect("get").is_some(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_allow_put_after_delete_range_given_interleaved_ops_when_writing() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        eng.put(&cf, b"key1", b"val1").expect("put");
        eng.put(&cf, b"key2", b"val2").expect("put");
        eng.put(&cf, b"key3", b"val3").expect("put");

        // Act - delete range, then put back
        eng.delete_range(&cf, b"key1", b"key3").expect("delete_range");
        eng.put(&cf, b"key2", b"new_val2").expect("put");

        // Assert - key2 has new value
        assert!(eng.get(&cf, b"key1").expect("get").is_none(), "{}", name);
        assert_eq!(
            eng.get(&cf, b"key2").expect("get"),
            Some(Bytes::from("new_val2")),
            "{}: key2 has new value",
            name
        );
        assert!(eng.get(&cf, b"key3").expect("get").is_some(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

#[test]
fn should_apply_memtable_and_sst_tombstones_given_mixed_sources_when_reading() {
    for mode in disk_storage_modes() {
        // Arrange
        let (name, storage_mode, _dir) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            ..Default::default()
        };
        let eng = MidgeEngine::open(opts).expect("open");
        let cf = eng.default_column_family();

        // Data in SST
        eng.put(&cf, b"key1", b"sst_val1").expect("put");
        eng.put(&cf, b"key2", b"sst_val2").expect("put");
        eng.flush().expect("flush");

        // Delete range (in memtable)
        eng.delete_range(&cf, b"key0", b"key2").expect("delete_range");

        // Act - new key after delete range
        eng.put(&cf, b"key1", b"mem_val1").expect("put");

        // Assert
        assert_eq!(
            eng.get(&cf, b"key1").expect("get"),
            Some(Bytes::from("mem_val1")),
            "{}: key1 has memtable value",
            name
        );
        assert!(eng.get(&cf, b"key2").expect("get").is_some(), "{}", name);
        drop(eng);
        eprintln!("✓ {}", name);
    }
}

// ============================================================================
// READ-ONLY MODE TESTS
// ============================================================================

#[test]
fn should_reject_delete_range_given_read_only_mode_when_attempting() {
    for mode in disk_storage_modes() {
        // Arrange
        let ctx = DurabilityTestContext::new(mode);
        let name = ctx.name().to_string();

        {
            let opts = MidgeOptions {
                storage_mode: ctx.create_storage_mode(),
                ..Default::default()
            };
            let eng = MidgeEngine::open(opts).expect("open");
            drop(eng);
        }

        let opts_ro = MidgeOptions {
            storage_mode: ctx.create_storage_mode(),
            read_only: true,
            ..Default::default()
        };
        let eng_ro = MidgeEngine::open(opts_ro).expect("open read-only");
        let cf = eng_ro.default_column_family();

        // Act
        let result = eng_ro.delete_range(&cf, b"a", b"z");

        // Assert
        assert!(result.is_err(), "{}", name);
        assert!(matches!(
            result.unwrap_err(),
            cntryl_midge::error::MidgeError::ReadOnly
        ), "{}", name);
        drop(eng_ro);
        eprintln!("✓ {}", name);
    }
}
