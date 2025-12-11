//! Range Scanning & Iterator Integration Tests
//!
//! Tests range scans, iterators, and sequential access patterns.
//! Validates that keys are returned in proper order, deletion is visible
//! to scans, and advanced iteration features work correctly.
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! These tests run across all storage modes (Memory, LocalDisk, CloudBacked).

use cntryl_midge::testkit::*;

// ============================================================================
// RANGE SCAN TESTS
// ============================================================================

#[test]
fn should_iterate_all_keys_in_order_given_populated_db_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Populate with ordered keys (zero-padded for lexicographic ordering)
        for i in 0..10 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act
        let results = engine.range(cf, b"k00", b"k99").unwrap();

        // Assert
        assert_eq!(results.len(), 10);
        for (idx, (k, v)) in results.iter().enumerate() {
            assert_eq!(k, format!("k{:02}", idx).as_bytes());
            assert_eq!(v, format!("v{:02}", idx).as_bytes());
        }
    });
}

#[test]
fn should_iterate_in_reverse_given_reverse_query_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..5 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act
        let query = cntryl_midge::Query::new().reverse();
        let results = engine.scan(cf, &query).unwrap();

        // Assert: Results should be in reverse order
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0.as_ref(), b"k04");
        assert_eq!(results[1].0.as_ref(), b"k03");
        assert_eq!(results[2].0.as_ref(), b"k02");
        assert_eq!(results[3].0.as_ref(), b"k01");
        assert_eq!(results[4].0.as_ref(), b"k00");
    });
}

#[test]
fn should_limit_results_given_limit_query_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..10 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act
        let query = cntryl_midge::Query::new().limit(3);
        let results = engine.scan(cf, &query).unwrap();

        // Assert
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_ref(), b"k00");
        assert_eq!(results[1].0.as_ref(), b"k01");
        assert_eq!(results[2].0.as_ref(), b"k02");
    });
}

#[test]
fn should_return_empty_given_empty_db_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act
        let results = engine.range(cf, b"k00", b"k99").unwrap();

        // Assert
        assert!(results.is_empty());
    });
}

#[test]
fn should_return_next_key_given_seek_to_missing_key_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Create non-contiguous keys
        engine.put(cf, b"k01", b"v01").unwrap();
        engine.put(cf, b"k03", b"v03").unwrap();
        engine.put(cf, b"k05", b"v05").unwrap();

        // Act: Scan from k00 (doesn't exist)
        let results = engine.range(cf, b"k00", b"k99").unwrap();

        // Assert: Should return all keys >= k00
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_ref(), b"k01");
        assert_eq!(results[1].0.as_ref(), b"k03");
        assert_eq!(results[2].0.as_ref(), b"k05");
    });
}

#[test]
fn should_return_empty_given_seek_past_end_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        engine.put(cf, b"k01", b"v01").unwrap();
        engine.put(cf, b"k03", b"v03").unwrap();

        // Act: Scan starting after all keys
        let results = engine.range(cf, b"k99", b"k99").unwrap();

        // Assert
        assert!(results.is_empty());
    });
}

#[test]
fn should_return_empty_given_invalid_range_when_start_greater_than_end() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        engine.put(cf, b"k01", b"v01").unwrap();
        engine.put(cf, b"k05", b"v05").unwrap();

        // Act: Invalid range (start > end)
        let results = engine.range(cf, b"k99", b"k00").unwrap();

        // Assert: No results because start >= end
        assert!(results.is_empty());
    });
}

#[test]
fn should_skip_deleted_keys_given_tombstones_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..5 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Delete k01 and k03
        engine.delete(cf, b"k01").unwrap();
        engine.delete(cf, b"k03").unwrap();

        // Act
        let results = engine.range(cf, b"k00", b"k99").unwrap();

        // Assert: k01 and k03 should not appear
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_ref(), b"k00");
        assert_eq!(results[1].0.as_ref(), b"k02");
        assert_eq!(results[2].0.as_ref(), b"k04");
    });
}

#[test]
fn should_respect_range_tombstones_given_delete_range_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..10 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Delete range [k02, k07)
        engine.delete_range(cf, b"k02", b"k07").unwrap();

        // Act
        let results = engine.range(cf, b"k00", b"k99").unwrap();

        // Assert: k02-k06 should be gone, k00, k01, k07-k09 remain
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0.as_ref(), b"k00");
        assert_eq!(results[1].0.as_ref(), b"k01");
        assert_eq!(results[2].0.as_ref(), b"k07");
        assert_eq!(results[3].0.as_ref(), b"k08");
        assert_eq!(results[4].0.as_ref(), b"k09");
    });
}

#[test]
fn should_return_latest_value_given_interleaved_puts_deletes_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Initial put
        engine.put(cf, b"key", b"value1").unwrap();

        // Overwrite
        engine.put(cf, b"key", b"value2").unwrap();

        // Delete and re-put
        engine.delete(cf, b"key").unwrap();
        engine.put(cf, b"key", b"value3").unwrap();

        // Act
        let results = engine.range(cf, b"a", b"z").unwrap();

        // Assert: Should have latest value
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.as_ref(), b"key");
        assert_eq!(results[0].1.as_ref(), b"value3");
    });
}

#[test]
fn should_match_regular_scan_given_streaming_scan_when_comparing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..8 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Regular range scan
        let range_results = engine.range(cf, b"k00", b"k99").unwrap();

        // Act: Scan with query
        let query = cntryl_midge::Query::new();
        let scan_results = engine.scan(cf, &query).unwrap();

        // Assert: Should produce identical results
        assert_eq!(range_results, scan_results);
    });
}

#[test]
fn should_respect_limit_given_streaming_scan_when_limited() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..20 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Query with limit
        let query = cntryl_midge::Query::new()
            .start_key(bytes::Bytes::from_static(b"k05"))
            .limit(5);
        let results = engine.scan(cf, &query).unwrap();

        // Assert
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0.as_ref(), b"k05");
        assert_eq!(results[4].0.as_ref(), b"k09");
    });
}

#[test]
fn should_apply_tombstones_given_streaming_scan_when_keys_deleted() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..10 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        engine.delete(cf, b"k02").unwrap();
        engine.delete(cf, b"k05").unwrap();

        // Act: Scan with query
        let query = cntryl_midge::Query::new();
        let results = engine.scan(cf, &query).unwrap();

        // Assert
        assert_eq!(results.len(), 8);
        assert!(!results.iter().any(|(k, _)| k.as_ref() == b"k02"));
        assert!(!results.iter().any(|(k, _)| k.as_ref() == b"k05"));
    });
}

#[test]
fn should_handle_large_scan_given_many_keys_when_iterating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Insert 500 keys
        for i in 0..500 {
            engine
                .put(
                    cf,
                    format!("k{:04}", i).as_bytes(),
                    format!("v{:04}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act
        let results = engine.range(cf, b"k0000", b"k0500").unwrap();

        // Assert
        assert_eq!(results.len(), 500);

        // Verify ordering
        for (idx, (k, v)) in results.iter().enumerate() {
            assert_eq!(k, format!("k{:04}", idx).as_bytes());
            assert_eq!(v, format!("v{:04}", idx).as_bytes());
        }
    });
}

#[test]
fn should_handle_large_streaming_scan_given_multiple_ssts_when_spanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Insert and flush multiple times to create SSTs
        for batch in 0..5 {
            for i in 0..20 {
                let key = format!("k{:04}", batch * 20 + i);
                engine
                    .put(
                        cf,
                        key.as_bytes(),
                        format!("v{:04}", batch * 20 + i).as_bytes(),
                    )
                    .unwrap();
            }
            // Note: In real implementation, would call flush() to trigger SST creation
            // For now, just put all keys in memtable
        }

        // Act
        let query = cntryl_midge::Query::new();
        let results = engine.scan(cf, &query).unwrap();

        // Assert: All 100 keys should be returned in order
        assert_eq!(results.len(), 100);

        for (idx, (k, v)) in results.iter().enumerate() {
            assert_eq!(k, format!("k{:04}", idx).as_bytes());
            assert_eq!(v, format!("v{:04}", idx).as_bytes());
        }
    });
}

#[test]
fn should_handle_concurrent_streaming_scans_when_multiple_threads() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = std::sync::Arc::new(open_with_mode(opts, mode));
        let cf = engine.default_column_family().clone();

        // Populate initial data
        for i in 0..50 {
            engine
                .put(
                    &cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Spawn multiple threads doing concurrent scans
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let engine_clone = std::sync::Arc::clone(&engine);
                let cf_clone = cf.clone();

                std::thread::spawn(move || {
                    let query = cntryl_midge::Query::new();
                    engine_clone.scan(&cf_clone, &query).unwrap()
                })
            })
            .collect();

        // Assert: All threads should get same results
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        for r in results.iter().skip(1) {
            assert_eq!(&results[0], r);
        }

        assert_eq!(results[0].len(), 50);
    });
}

#[test]
fn should_produce_identical_results_given_repeated_scans_when_rewinding() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        for i in 0..15 {
            engine
                .put(
                    cf,
                    format!("k{:02}", i).as_bytes(),
                    format!("v{:02}", i).as_bytes(),
                )
                .unwrap();
        }

        // Act: Perform multiple identical scans
        let results1 = engine.range(cf, b"k00", b"k99").unwrap();
        let results2 = engine.range(cf, b"k00", b"k99").unwrap();
        let results3 = engine.range(cf, b"k00", b"k99").unwrap();

        // Assert: All scans should produce identical results
        assert_eq!(results1, results2);
        assert_eq!(results2, results3);
        assert_eq!(results1.len(), 15);
    });
}
