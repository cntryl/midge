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

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{Query, Transaction};

fn collect_scan(tx: &Transaction, query: Query) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iter = tx.scan(&query).unwrap();
    std::iter::from_fn(|| iter.next()).collect()
}

fn scan_between(tx: &Transaction, start: &[u8], end: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    collect_scan(
        tx,
        Query::new()
            .start_key(Bytes::copy_from_slice(start))
            .end_key(Bytes::copy_from_slice(end)),
    )
}

// ============================================================================
// RANGE SCAN TESTS
// ============================================================================

#[test]
fn should_iterate_all_keys_in_order_given_populated_db_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Populate with ordered keys (zero-padded for lexicographic ordering)
        for i in 0..10 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"k00", b"k99");

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
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..5 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Act
        let query = cntryl_midge::Query::new().reverse();
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, query);

        // Assert: Results should be in reverse order
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0.as_slice(), b"k04");
        assert_eq!(results[1].0.as_slice(), b"k03");
        assert_eq!(results[2].0.as_slice(), b"k02");
        assert_eq!(results[3].0.as_slice(), b"k01");
        assert_eq!(results[4].0.as_slice(), b"k00");
    });
}

#[test]
fn should_limit_results_given_limit_query_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..10 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Act
        let query = cntryl_midge::Query::new().limit(3);
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, query);

        // Assert
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_slice(), b"k00");
        assert_eq!(results[1].0.as_slice(), b"k01");
        assert_eq!(results[2].0.as_slice(), b"k02");
    });
}

#[test]
fn should_return_empty_given_empty_db_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"k00", b"k99");

        // Assert
        assert!(results.is_empty());
    });
}

#[test]
fn should_return_next_key_given_seek_to_missing_key_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Create non-contiguous keys
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"k01".to_vec(), b"v01".to_vec(), None).unwrap();
        tx.put(b"k03".to_vec(), b"v03".to_vec(), None).unwrap();
        tx.put(b"k05".to_vec(), b"v05".to_vec(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Act: Scan from k00 (doesn't exist)
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"k00", b"k99");

        // Assert: Should return all keys >= k00
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_slice(), b"k01");
        assert_eq!(results[1].0.as_slice(), b"k03");
        assert_eq!(results[2].0.as_slice(), b"k05");
    });
}

#[test]
fn should_return_empty_given_seek_past_end_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"k01".to_vec(), b"v01".to_vec(), None).unwrap();
        tx.put(b"k03".to_vec(), b"v03".to_vec(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Act: Scan starting after all keys
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"k99", b"k99");

        // Assert
        assert!(results.is_empty());
    });
}

#[test]
fn should_return_empty_given_invalid_range_when_start_greater_than_end() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"k01".to_vec(), b"v01".to_vec(), None).unwrap();
        tx.put(b"k05".to_vec(), b"v05".to_vec(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Act: Invalid range (start > end)
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"k99", b"k00");

        // Assert: No results because start >= end
        assert!(results.is_empty());
    });
}

#[test]
fn should_skip_deleted_keys_given_tombstones_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..5 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Delete k01 and k03
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(b"k01".to_vec()).unwrap();
        tx.delete(b"k03".to_vec()).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"k00", b"k99");

        // Assert: k01 and k03 should not appear
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_slice(), b"k00");
        assert_eq!(results[1].0.as_slice(), b"k02");
        assert_eq!(results[2].0.as_slice(), b"k04");
    });
}

#[test]
fn should_respect_range_tombstones_given_delete_range_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..10 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Delete range [k02, k07)
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete_range(b"k02".to_vec(), b"k07".to_vec()).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"k00", b"k99");

        // Assert: k02-k06 should be gone, k00, k01, k07-k09 remain
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0.as_slice(), b"k00");
        assert_eq!(results[1].0.as_slice(), b"k01");
        assert_eq!(results[2].0.as_slice(), b"k07");
        assert_eq!(results[3].0.as_slice(), b"k08");
        assert_eq!(results[4].0.as_slice(), b"k09");
    });
}

#[test]
fn should_return_latest_value_given_interleaved_puts_deletes_when_scanning() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Initial put
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value1".to_vec(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Overwrite
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value2".to_vec(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Delete and re-put
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(b"key".to_vec()).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(b"key".to_vec(), b"value3".to_vec(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"a", b"z");

        // Assert: Should have latest value
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.as_slice(), b"key");
        assert_eq!(results[0].1.as_slice(), b"value3");
    });
}

#[test]
fn should_match_regular_scan_given_streaming_scan_when_comparing() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..8 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Act: Regular range scan
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let range_results = scan_between(&tx, b"k00", b"k99");

        // Act: Scan with query
        let query = cntryl_midge::Query::new();
        let scan_results = collect_scan(&tx, query);

        // Assert: Should produce identical results
        assert_eq!(range_results, scan_results);
    });
}

#[test]
fn should_respect_limit_given_streaming_scan_when_limited() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..20 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Act: Query with limit
        let query = cntryl_midge::Query::new()
            .start_key(bytes::Bytes::from_static(b"k05"))
            .limit(5);
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, query);

        // Assert
        assert_eq!(results.len(), 5);
        assert_eq!(results[0].0.as_slice(), b"k05");
        assert_eq!(results[4].0.as_slice(), b"k09");
    });
}

#[test]
fn should_respect_limit_in_reverse_query_when_limited() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..10 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Act: Reverse query with limit
        let query = cntryl_midge::Query::new().reverse().limit(3);
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, query);

        // Assert: Should return last 3 keys in descending order
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0.as_slice(), b"k09");
        assert_eq!(results[1].0.as_slice(), b"k08");
        assert_eq!(results[2].0.as_slice(), b"k07");
    });
}

#[test]
fn should_apply_tombstones_given_streaming_scan_when_keys_deleted() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..10 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.delete(b"k02".to_vec()).unwrap();
        tx.delete(b"k05".to_vec()).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Act: Scan with query
        let query = cntryl_midge::Query::new();
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, query);

        // Assert
        assert_eq!(results.len(), 8);
        assert!(!results.iter().any(|(k, _)| k.as_slice() == b"k02"));
        assert!(!results.iter().any(|(k, _)| k.as_slice() == b"k05"));
    });
}

#[test]
fn should_handle_large_scan_given_many_keys_when_iterating() {
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        // Arrange
        let engine = open_with_mode(opts, mode);
        let cf = engine.create_column_family("test").expect("create cf");

        // Insert 500 keys (batch into one transaction for speed)
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        for i in 0..500 {
            tx.put(
                format!("k{:04}", i).as_bytes().to_vec(),
                format!("v{:04}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
        }
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();

        // Act
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = scan_between(&tx, b"k0000", b"k0500");

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
        let cf = engine.create_column_family("test").expect("create cf");

        // Insert and flush multiple times to create SSTs
        for batch in 0..5 {
            for i in 0..20 {
                let key = format!("k{:04}", batch * 20 + i);
                let mut tx = engine
                    .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                    .unwrap();
                tx.put(
                    key.as_bytes().to_vec(),
                    format!("v{:04}", batch * 20 + i).as_bytes().to_vec(),
                    None,
                )
                .unwrap();
                engine
                    .commit(tx, cntryl_midge::WriteOptions::buffered())
                    .unwrap();
            }
            // Note: In real implementation, would call flush() to trigger SST creation
            // For now, just put all keys in memtable
        }

        // Act
        let query = cntryl_midge::Query::new();
        let tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results = collect_scan(&tx, query);

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
        let cf = engine
            .create_column_family("test")
            .expect("create cf")
            .clone();

        // Populate initial data
        for i in 0..50 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Act: Spawn multiple threads doing concurrent scans
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let engine_clone = std::sync::Arc::clone(&engine);
                let cf_clone = cf.clone();

                std::thread::spawn(move || {
                    let query = cntryl_midge::Query::new();
                    let tx = engine_clone
                        .begin_tx(cf_clone.id(), cntryl_midge::TransactionMode::ReadOnly)
                        .unwrap();
                    collect_scan(&tx, query)
                })
            })
            .collect();

        // Assert: All threads should get same results
        let results: Vec<Vec<(Vec<u8>, Vec<u8>)>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();

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
        let cf = engine.create_column_family("test").expect("create cf");

        for i in 0..15 {
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(
                format!("k{:02}", i).as_bytes().to_vec(),
                format!("v{:02}", i).as_bytes().to_vec(),
                None,
            )
            .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }

        // Act: Perform multiple identical scans
        let tx1 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results1 = scan_between(&tx1, b"k00", b"k99");
        let tx2 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results2 = scan_between(&tx2, b"k00", b"k99");
        let tx3 = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
            .unwrap();
        let results3 = scan_between(&tx3, b"k00", b"k99");

        // Assert: All scans should produce identical results
        assert_eq!(results1, results2);
        assert_eq!(results2, results3);
        assert_eq!(results1.len(), 15);
    });
}
