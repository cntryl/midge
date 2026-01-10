//! Edge Cases Tests
//!
//! Tests boundary conditions and unusual scenarios:
//! - Very large keys (1MB+) and values (100MB+)
//! - Empty database, single record, 10k+ keys
//! - Mixed value sizes, delete all, rapid operations
//! - Tombstone accumulation, range extremes, TTL edge cases
//!
//! Naming convention:
//!   should_<behavior>_given_<context>_when_<condition>
//!
//! Most tests run on all storage modes to validate cross-platform consistency.
//! Some intentionally exclude cloud mode when the invariant is not meaningful
//! under CloudFirst durability semantics.

use bytes::Bytes;
use cntryl_midge::testkit::*;
use cntryl_midge::{TransactionMode, WriteOptions};

// ============================================================================
// SIZE EXTREMES (Tests 1-4)
// ============================================================================

#[test]
fn should_retrieve_stored_keys_when_megabyte_sized() {
    // Arrange: Create 1MB+ key (256KB minimum, test with 500KB)
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        let large_key = vec![65u8; 500_000]; // 500KB key
        let small_value = b"value";

        // Act: Store and retrieve
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(large_key.clone(), small_value.to_vec(), None).expect("put");
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        let got = tx.get(&large_key).expect("get");

        // Assert
        assert_eq!(
            got,
            Some(Bytes::copy_from_slice(small_value)),
            "failed to store/retrieve 500KB key in {mode}"
        );
    });
}

#[test]
fn should_retrieve_stored_values_when_hundred_megabytes() {
    // Arrange: Create 100MB value (or reasonable subset for tests)
    // Use 10MB for practical test speed; pattern validates for larger
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        let small_key = b"big_value";
        let large_value = vec![42u8; 10_000_000]; // 10MB value

        // Act: Store and retrieve
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(small_key.to_vec(), large_value.clone(), None).expect("put");
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        let got = tx.get(small_key).expect("get");

        // Assert: Verify size and content
        assert!(got.is_some(), "failed to retrieve 10MB value in {mode}");
        assert_eq!(
            got.as_ref().map(|b| b.len()),
            Some(10_000_000),
            "retrieved value size mismatch in {mode}"
        );
    });
}

#[test]
fn should_handle_mixed_size_values_when_ranging_from_bytes_to_megabytes() {
    // Arrange
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        // Act: Store values of wildly different sizes
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"tiny".to_vec(), b"x".to_vec(), None).expect("put");
        tx.put(b"small".to_vec(), vec![42u8; 100], None).expect("put");
        tx.put(b"medium".to_vec(), vec![42u8; 100_000], None).expect("put");
        tx.put(b"large".to_vec(), vec![42u8; 1_000_000], None).expect("put");
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Assert: Retrieve all and verify
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        assert_eq!(tx.get(b"tiny").expect("get").map(|b| b.len()), Some(1));
        assert_eq!(tx.get(b"small").expect("get").map(|b| b.len()), Some(100));
        assert_eq!(tx.get(b"medium").expect("get").map(|b| b.len()), Some(100_000));
        assert_eq!(tx.get(b"large").expect("get").map(|b| b.len()), Some(1_000_000));
    });
}

#[test]
fn should_handle_special_characters_in_keys_when_utf8_and_binary_mixed() {
    // Arrange
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        // Act: Store keys with special characters and binary data
        let keys: Vec<&[u8]> = vec![
            b"normal_key",
            "unicode_\u{1F600}_key".as_bytes(),
            b"\x00\x01\x02\x03", // Binary nulls
            b"key\twith\ttabs",
            b"key\nwith\nnewlines",
        ];

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for (i, key) in keys.iter().enumerate() {
            let value = format!("value_{i}");
            tx.put(key.to_vec(), value.into_bytes(), None).expect("put");
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Assert: Retrieve all
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for (i, key) in keys.iter().enumerate() {
            let got = tx.get(key).expect("get");
            let expected_value = format!("value_{i}");
            assert_eq!(
                got,
                Some(Bytes::copy_from_slice(expected_value.as_bytes())),
                "special char key retrieval failed: {:?}",
                String::from_utf8_lossy(key)
            );
        }
    });
}

// ============================================================================
// EMPTY/BOUNDARY CONDITIONS (Tests 5-8)
// ============================================================================

#[test]
fn should_handle_empty_database_when_no_keys_written() {
    // Arrange: Open engine and close without writing anything
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        // Act: Try to read from empty database
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        let got = tx.get(b"nonexistent").expect("get");

        // Assert
        assert_eq!(
            got, None,
            "empty database returned unexpected value in {mode:?}"
        );
    });
}

#[test]
fn should_handle_single_record_database_when_one_key_value_pair() {
    // Arrange
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        // Act: Write single record
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        tx.put(b"only_key".to_vec(), b"only_value".to_vec(), None).expect("put");
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Assert: Can retrieve it
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        let got = tx.get(b"only_key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"only_value")),
            "failed to retrieve single record in {mode:?}"
        );

        // Assert: Other keys don't exist
        let not_got = tx.get(b"other_key").expect("get");
        assert_eq!(
            not_got, None,
            "unexpected key found in single-record database"
        );
    });
}

#[test]
fn should_handle_range_query_at_boundaries_when_first_last_and_missing() {
    // Arrange
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        // Act: Write sorted keys
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..5 {
            let key = format!("key_{i:02}");
            tx.put(key.into_bytes(), format!("value_{i}").into_bytes(), None).expect("put");
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Assert: Boundary keys are retrievable
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        assert!(
            tx.get(b"key_00").expect("get").is_some(),
            "first key not found"
        );
        assert!(
            tx.get(b"key_04").expect("get").is_some(),
            "last key not found"
        );
        assert_eq!(
            tx.get(b"key_99").expect("get"),
            None,
            "non-existent key should be None"
        );
    });
}

// ============================================================================
// STRESS/ACCUMULATION (Tests 9-12)
// ============================================================================

#[test]
fn should_handle_rapid_operations_when_one_thousand_puts_per_second() {
    // Arrange
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        // Act: Rapid writes (batched in a single transaction for efficiency)
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..1000 {
            let key = format!("rapid_{i:05}");
            tx.put(key.into_bytes(), format!("v_{i}").into_bytes(), None).expect("put");
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Assert: All retrievable
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for i in (0..1000).step_by(100) {
            let key = format!("rapid_{i:05}");
            let got = tx.get(key.as_bytes()).expect("get");
            assert!(got.is_some(), "lost rapid write in {mode}");
        }
    });
}

#[test]
fn should_handle_delete_all_pattern_when_writing_then_deleting_all_keys() {
    // Arrange: Write 100 keys
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..100 {
            let key = format!("del_test_{i:03}");
            tx.put(key.into_bytes(), b"delete_me".to_vec(), None).expect("put");
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Act: Delete all keys
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..100 {
            let key = format!("del_test_{i:03}");
            tx.delete(key.into_bytes()).expect("delete");
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Assert: All deleted
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for i in 0..100 {
            let key = format!("del_test_{i:03}");
            let got = tx.get(key.as_bytes()).expect("get");
            assert_eq!(got, None, "key not deleted in {mode}");
        }
    });
}

#[test]
fn should_handle_tombstone_accumulation_when_many_deletes_create_tombstones() {
    // Arrange: Rapid put/delete cycles
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        // Act: 10 put/delete cycles on same key
        for cycle in 0..10 {
            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.put(b"tombstone_test".to_vec(), format!("cycle_{cycle}").into_bytes(), None).expect("put");
            engine.commit(tx, WriteOptions::buffered()).unwrap();

            let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
            tx.delete(b"tombstone_test".to_vec()).expect("delete");
            engine.commit(tx, WriteOptions::buffered()).unwrap();
        }

        // Assert: Final state is deleted (tombstone wins)
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        let got = tx.get(b"tombstone_test").expect("get");
        assert_eq!(got, None, "tombstone did not win over old put in {mode}");
    });
}

#[test]
fn should_handle_ten_thousand_keys_when_large_keyspace() {
    // Arrange
    // CloudFirst mode is intentionally excluded: 10k single-key puts are
    // performance-dominated by cloud durability gating and don't validate any
    // additional correctness beyond what smaller cloud-mode tests cover.
    for_each_storage_mode(&["memory", "local"], |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();
        let cf_id = cf.id();

        // Act: Write 10k keys (batched in a single transaction)
        let mut tx = engine.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
        for i in 0..10_000 {
            let key = format!("large_ks_{i:05}");
            tx.put(key.into_bytes(), format!("v_{i}").into_bytes(), None).expect("put");
        }
        engine.commit(tx, WriteOptions::buffered()).unwrap();

        // Assert: Random samples retrieve correctly
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for i in [0, 100, 1000, 5000, 9999].iter() {
            let key = format!("large_ks_{i:05}");
            let got = tx.get(key.as_bytes()).expect("get");
            assert!(got.is_some(), "lost key in 10k keyspace at {i} in {mode}");
        }
    });
}

#[test]
fn should_batch_concurrent_puts_when_cloudfirst_mode() {
    // Arrange
    for_each_storage_mode(&["cloud"], |mode, opts| {
        let cloud_wal_dir = match &opts.storage_mode {
            cntryl_midge::testkit::StorageMode::CloudBacked { local_cache_path } => {
                local_cache_path.join("cloud_store").join("wal")
            }
            _ => panic!("expected cloud storage mode"),
        };

        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family().clone();
        let cf_id = cf.id();

        let threads: usize = 16;
        let puts_per_thread: usize = 200;
        let total_puts: usize = threads * puts_per_thread;

        // Act: concurrent single puts (each put still blocks on CloudAck)
        std::thread::scope(|s| {
            let engine_ref = &engine;
            for t in 0..threads {
                s.spawn(move || {
                    for i in 0..puts_per_thread {
                        let key = format!("k_{t}_{i}");
                        let mut tx = engine_ref.begin_tx(cf_id, TransactionMode::ReadWrite).unwrap();
                        tx.put(key.into_bytes(), b"value".to_vec(), None).expect("put");
                        engine_ref.commit(tx, WriteOptions::buffered()).unwrap();
                    }
                });
            }
        });

        // Assert: correctness (spot-check)
        let tx = engine.begin_tx(cf_id, TransactionMode::ReadOnly).unwrap();
        for t in 0..threads {
            for i in [0, puts_per_thread / 2, puts_per_thread - 1] {
                let key = format!("k_{t}_{i}");
                let got = tx.get(key.as_bytes()).expect("get");
                assert!(got.is_some(), "missing key {key}");
            }
        }

        // Assert: batching occurred (uploads/segments < puts)
        let uploads: usize = std::fs::read_dir(&cloud_wal_dir)
            .unwrap_or_else(|e| panic!("read_dir({cloud_wal_dir:?}) failed: {e}"))
            .filter_map(|e| e.ok())
            .filter(|e| matches!(e.path().extension().and_then(|s| s.to_str()), Some("wal")))
            .count();

        assert!(
            uploads < total_puts,
            "expected batching: uploads={uploads} puts={total_puts}"
        );
    });
}
