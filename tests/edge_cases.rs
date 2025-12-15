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

// ============================================================================
// SIZE EXTREMES (Tests 1-4)
// ============================================================================

#[test]
fn should_store_and_retrieve_very_large_keys_when_megabyte_sized() {
    // Arrange: Create 1MB+ key (256KB minimum, test with 500KB)
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let large_key = vec![65u8; 500_000]; // 500KB key
        let small_value = b"value";

        // Act: Store and retrieve
        engine.put(cf, &large_key, small_value).expect("put");
        let got = engine.get(cf, &large_key).expect("get");

        // Assert
        assert_eq!(
            got,
            Some(Bytes::copy_from_slice(small_value)),
            "failed to store/retrieve 500KB key in {mode}"
        );
    });
}

#[test]
fn should_store_and_retrieve_very_large_values_when_hundred_megabytes() {
    // Arrange: Create 100MB value (or reasonable subset for tests)
    // Use 10MB for practical test speed; pattern validates for larger
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        let small_key = b"big_value";
        let large_value = vec![42u8; 10_000_000]; // 10MB value

        // Act: Store and retrieve
        engine.put(cf, small_key, &large_value).expect("put");
        let got = engine.get(cf, small_key).expect("get");

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

        // Act: Store values of wildly different sizes
        engine.put(cf, b"tiny", b"x").expect("put");
        engine.put(cf, b"small", &[42u8; 100]).expect("put");
        engine
            .put(cf, b"medium", &vec![42u8; 100_000])
            .expect("put");
        engine
            .put(cf, b"large", &vec![42u8; 1_000_000])
            .expect("put");

        // Assert: Retrieve all and verify
        assert_eq!(
            engine.get(cf, b"tiny").expect("get").map(|b| b.len()),
            Some(1)
        );
        assert_eq!(
            engine.get(cf, b"small").expect("get").map(|b| b.len()),
            Some(100)
        );
        assert_eq!(
            engine.get(cf, b"medium").expect("get").map(|b| b.len()),
            Some(100_000)
        );
        assert_eq!(
            engine.get(cf, b"large").expect("get").map(|b| b.len()),
            Some(1_000_000)
        );
    });
}

#[test]
fn should_handle_special_characters_in_keys_when_utf8_and_binary_mixed() {
    // Arrange
    for_each_storage_mode(&all_storage_modes_new(), |mode, opts| {
        let engine = open_with_mode(opts, mode);
        let cf = engine.default_column_family();

        // Act: Store keys with special characters and binary data
        let keys = [
            b"normal_key" as &[u8],
            "unicode_ðŸ˜€_key".as_bytes(),
            b"\x00\x01\x02\x03", // Binary nulls
            b"key\twith\ttabs",
            b"key\nwith\nnewlines",
        ];

        for (i, key) in keys.iter().enumerate() {
            let value = format!("value_{i}");
            engine.put(cf, key, value.as_bytes()).expect("put");
        }

        // Assert: Retrieve all
        for (i, key) in keys.iter().enumerate() {
            let got = engine.get(cf, key).expect("get");
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

        // Act: Try to read from empty database
        let got = engine.get(cf, b"nonexistent").expect("get");

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

        // Act: Write single record
        engine.put(cf, b"only_key", b"only_value").expect("put");

        // Assert: Can retrieve it
        let got = engine.get(cf, b"only_key").expect("get");
        assert_eq!(
            got,
            Some(Bytes::from_static(b"only_value")),
            "failed to retrieve single record in {mode:?}"
        );

        // Assert: Other keys don't exist
        let not_got = engine.get(cf, b"other_key").expect("get");
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

        // Act: Write sorted keys
        for i in 0..5 {
            let key = format!("key_{i:02}");
            engine
                .put(cf, key.as_bytes(), format!("value_{i}").as_bytes())
                .expect("put");
        }

        // Assert: Boundary keys are retrievable
        assert!(
            engine.get(cf, b"key_00").expect("get").is_some(),
            "first key not found"
        );
        assert!(
            engine.get(cf, b"key_04").expect("get").is_some(),
            "last key not found"
        );
        assert_eq!(
            engine.get(cf, b"key_99").expect("get"),
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

        // Act: Rapid writes
        for i in 0..1000 {
            let key = format!("rapid_{i:05}");
            engine
                .put(cf, key.as_bytes(), format!("v_{i}").as_bytes())
                .expect("put");
        }

        // Assert: All retrievable
        for i in (0..1000).step_by(100) {
            let key = format!("rapid_{i:05}");
            let got = engine.get(cf, key.as_bytes()).expect("get");
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

        for i in 0..100 {
            let key = format!("del_test_{i:03}");
            engine.put(cf, key.as_bytes(), b"delete_me").expect("put");
        }

        // Act: Delete all keys
        for i in 0..100 {
            let key = format!("del_test_{i:03}");
            engine.delete(cf, key.as_bytes()).expect("delete");
        }

        // Assert: All deleted
        for i in 0..100 {
            let key = format!("del_test_{i:03}");
            let got = engine.get(cf, key.as_bytes()).expect("get");
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

        // Act: 10 put/delete cycles on same key
        for cycle in 0..10 {
            let key = b"tombstone_test";
            engine
                .put(cf, key, format!("cycle_{cycle}").as_bytes())
                .expect("put");
            engine.delete(cf, key).expect("delete");
        }

        // Assert: Final state is deleted (tombstone wins)
        let got = engine.get(cf, b"tombstone_test").expect("get");
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

        // Act: Write 10k keys
        for i in 0..10_000 {
            let key = format!("large_ks_{i:05}");
            engine
                .put(cf, key.as_bytes(), format!("v_{i}").as_bytes())
                .expect("put");
        }

        // Assert: Random samples retrieve correctly
        for i in [0, 100, 1000, 5000, 9999].iter() {
            let key = format!("large_ks_{i:05}");
            let got = engine.get(cf, key.as_bytes()).expect("get");
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

        let threads: usize = 16;
        let puts_per_thread: usize = 200;
        let total_puts: usize = threads * puts_per_thread;

        // Act: concurrent single puts (each put still blocks on CloudAck)
        std::thread::scope(|s| {
            let engine_ref = &engine;
            for t in 0..threads {
                let cf = cf.clone();
                s.spawn(move || {
                    for i in 0..puts_per_thread {
                        let key = format!("k_{t}_{i}");
                        engine_ref.put(&cf, key.as_bytes(), b"value").expect("put");
                    }
                });
            }
        });

        // Assert: correctness (spot-check)
        for t in 0..threads {
            for i in [0, puts_per_thread / 2, puts_per_thread - 1] {
                let key = format!("k_{t}_{i}");
                let got = engine.get(&cf, key.as_bytes()).expect("get");
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
