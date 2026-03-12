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
mod common;
use cntryl_midge::{Query, Transaction};
use common::*;

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
        engine
            .delete_range(
                &cf,
                b"k02".to_vec(),
                b"k07".to_vec(),
                cntryl_midge::WriteOptions::buffered(),
            )
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
fn should_iterate_memtable_plus_multiple_ssts_given_flushed_batches_when_scanning() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    for batch in 0..3 {
        for i in 0..20 {
            let key = format!("sst{:02}_k{:02}", batch, i);
            let value = format!("sst{:02}_v{:02}", batch, i);
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.into_bytes(), value.into_bytes(), None).unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }
        engine.flush_cf(&cf).expect("flush batch into SST");
    }

    for i in 0..10 {
        let key = format!("mem_k{:02}", i);
        let value = format!("mem_v{:02}", i);
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .unwrap();
        tx.put(key.into_bytes(), value.into_bytes(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();
    }

    // Act
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let results = collect_scan(&tx, cntryl_midge::Query::new());

    // Assert
    assert_eq!(results.len(), 70);
    assert!(results.iter().any(|(k, _)| k.as_slice() == b"sst00_k00"));
    assert!(results.iter().any(|(k, _)| k.as_slice() == b"sst02_k19"));
    assert!(results.iter().any(|(k, _)| k.as_slice() == b"mem_k00"));
    assert!(results.iter().any(|(k, _)| k.as_slice() == b"mem_k09"));
}

#[test]
fn should_return_latest_value_across_levels_given_overwrite_in_newer_sst_when_scanning() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"shared".to_vec(), b"v1".to_vec(), None).unwrap();
    tx.put(b"stable".to_vec(), b"keep".to_vec(), None).unwrap();
    engine
        .commit(tx, cntryl_midge::WriteOptions::buffered())
        .unwrap();
    engine.flush_cf(&cf).expect("flush initial sst");

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    tx.put(b"shared".to_vec(), b"v2".to_vec(), None).unwrap();
    engine
        .commit(tx, cntryl_midge::WriteOptions::buffered())
        .unwrap();
    engine.flush_cf(&cf).expect("flush overwrite sst");

    // Act
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let results = collect_scan(&tx, cntryl_midge::Query::new());

    // Assert
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .any(|(k, v)| k.as_slice() == b"shared" && v.as_slice() == b"v2"));
    assert!(results
        .iter()
        .any(|(k, v)| k.as_slice() == b"stable" && v.as_slice() == b"keep"));
}

#[test]
fn should_hide_deleted_keys_across_compacted_ssts_when_scanning() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    for batch in 0..4 {
        for i in 0..25 {
            let key = format!("k{:03}", batch * 25 + i);
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.into_bytes(), b"value".to_vec(), None).unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }
        engine.flush_cf(&cf).expect("flush seed batch");
    }

    let mut tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    for deleted in [b"k020", b"k021", b"k050", b"k079"] {
        tx.delete(deleted.to_vec()).expect("delete compacted key");
    }
    engine
        .commit(tx, cntryl_midge::WriteOptions::buffered())
        .unwrap();
    engine.flush_cf(&cf).expect("flush delete tombstones");
    engine.compact_all().expect("compact levels");

    // Act
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let results = collect_scan(&tx, cntryl_midge::Query::new());

    // Assert
    for deleted in [b"k020", b"k021", b"k050", b"k079"] {
        assert!(
            !results.iter().any(|(k, _)| k.as_slice() == deleted),
            "deleted key {:?} should stay hidden after compaction",
            String::from_utf8_lossy(deleted)
        );
    }
    assert!(results.iter().any(|(k, _)| k.as_slice() == b"k000"));
    assert!(results.iter().any(|(k, _)| k.as_slice() == b"k099"));
}

#[test]
fn should_scan_compacted_ssts_given_new_iterator_after_compaction_when_levels_change() {
    // Arrange
    let engine = open_with_mode(opts_for_mode("local"), "local");
    let cf = engine.create_column_family("test").expect("create cf");

    for batch in 0..5 {
        for i in 0..20 {
            let key = format!("k{:03}", batch * 20 + i);
            let value = format!("v{:03}", batch * 20 + i);
            let mut tx = engine
                .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
                .unwrap();
            tx.put(key.into_bytes(), value.into_bytes(), None).unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }
        engine.flush_cf(&cf).expect("flush batch");
    }

    // Act
    engine.compact_all().expect("compact levels");
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let results = collect_scan(&tx, cntryl_midge::Query::new());

    // Assert
    assert_eq!(results.len(), 100);
    assert_eq!(results.first().expect("first result").0.as_slice(), b"k000");
    assert_eq!(results.last().expect("last result").0.as_slice(), b"k099");
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
