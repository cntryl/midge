// Unit test to prove SST key encoding bug
// SST scan is returning internal keys (with sequence number suffixes) instead of user keys

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};

mod common;
use common::test_temp_dir;

#[test]
fn should_return_user_keys_not_internal_keys_when_scanning_sst() {
    // Arrange: Write keys to SST using explicit flush
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 1024 * 1024; // Large to avoid rotation
    opts.memtable_size = 1024 * 1024;

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write simple keys
    eng.put(&cf, b"a", b"1").unwrap();
    eng.put(&cf, b"ab", b"2").unwrap();
    eng.put(&cf, b"ac", b"3").unwrap();

    // Flush to SST
    eng.flush().unwrap();

    // Act: Scan to get keys from SST
    let rows = eng
        .scan(&cf, Query::new().prefix(Bytes::from_static(b"a")))
        .expect("scan");

    // Assert: Keys should be user keys, not internal keys with sequence suffixes

    // Debug: Print what we actually got
    for (i, (key, val)) in rows.iter().enumerate() {
        eprintln!("Row {}: key={:?}, value={:?}", i, key, val);
    }

    assert_eq!(rows.len(), 3, "Should have 3 keys");

    // Check first key
    assert_eq!(
        rows[0].0.as_ref(),
        b"a",
        "First key should be 'a', not 'a' with sequence suffix. Got: {:?}",
        rows[0].0
    );

    // Check second key
    assert_eq!(
        rows[1].0.as_ref(),
        b"ab",
        "Second key should be 'ab', not 'ab' with sequence suffix. Got: {:?}",
        rows[1].0
    );

    // Check third key
    assert_eq!(
        rows[2].0.as_ref(),
        b"ac",
        "Third key should be 'ac', not 'ac' with sequence suffix. Got: {:?}",
        rows[2].0
    );

    // Additional check: keys should not contain \xff bytes (internal key marker)
    for (key, _value) in &rows {
        assert!(
            !key.contains(&0xff),
            "Key should not contain internal key markers (0xff). Got: {:?}",
            key
        );
    }
}

#[test]
fn should_return_user_keys_for_tombstones_in_sst() {
    // Arrange
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 1024 * 1024;
    opts.memtable_size = 1024 * 1024;

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write and delete a key, then flush to SST
    eng.put(&cf, b"key1", b"value1").unwrap();
    eng.delete(&cf, b"key1").unwrap();
    eng.put(&cf, b"key2", b"value2").unwrap();

    eng.flush().unwrap();

    // Act: Scan should filter out tombstones but if we did see them, they should be user keys
    let rows = eng.scan(&cf, Query::new()).expect("scan");

    // Assert: Only key2 should be returned (key1 is deleted)
    assert_eq!(rows.len(), 1, "Should only return non-deleted keys");
    assert_eq!(rows[0].0.as_ref(), b"key2");

    // The tombstone for key1 should not leak through as an internal key
    assert!(
        !rows.iter().any(|(k, _)| k.contains(&0xff)),
        "No internal keys should be returned"
    );
}

#[test]
fn should_not_expose_internal_key_format_in_multi_version_scan() {
    // Arrange: Create multiple versions of the same key in SST
    let dir = test_temp_dir();
    let mut opts = MidgeOptions::default();
    opts.storage_mode = StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    };
    opts.enable_compaction = false;
    opts.wal_buffer_size = 1024 * 1024;
    opts.memtable_size = 1024 * 1024;

    let eng = MidgeEngine::open(opts).expect("open");
    let cf = eng.default_column_family();

    // Write same key multiple times (creates multiple versions with different sequences)
    eng.put(&cf, b"key", b"v1").unwrap();
    eng.flush().unwrap();

    eng.put(&cf, b"key", b"v2").unwrap();
    eng.flush().unwrap();

    eng.put(&cf, b"key", b"v3").unwrap();
    eng.flush().unwrap();

    // Act: Scan should return latest version
    let rows = eng.scan(&cf, Query::new()).expect("scan");

    // Assert: Should only see one user key "key", not multiple versions with sequence suffixes
    assert_eq!(rows.len(), 1, "Should only return latest version");
    assert_eq!(rows[0].0.as_ref(), b"key", "Key should be plain user key");
    assert_eq!(rows[0].1.as_ref(), b"v3", "Value should be latest");

    // Verify no internal key format leaked
    assert!(
        !rows[0].0.contains(&0xff),
        "Key should not contain internal markers"
    );
}
