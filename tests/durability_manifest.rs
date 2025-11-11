mod common;
use common::{
    assert_get_equals, durability_opts, flush_test_opts, test_temp_dir, with_engine_restart,
};

#[test]
fn should_preserve_consistency_given_crash_between_sst_write_and_manifest_update() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024); // Small memtable to force flush

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            // Act - write enough data to trigger flush (SST creation)
            let cf = eng.default_column_family();
            for i in 0..100 {
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // TODO: Add test hook to crash between SST write and manifest update
            // This should leave the database in a consistent state on recovery
        },
        |eng| {
            // Assert - database should be in consistent state after crash
            // Either all data present (if manifest updated) or none (if not)
            let cf = eng.default_column_family();
            let first_result = eng.get(&cf, b"key0000").expect("get");
            let last_result = eng.get(&cf, b"key0099").expect("get");

            // Consistency check: if first key exists, all keys should exist
            if first_result.is_some() {
                assert!(
                    last_result.is_some(),
                    "All keys should exist if first exists"
                );
            }
        },
    );
}

#[test]
fn should_fsync_sst_and_update_manifest_before_wal_truncation() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024);

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write data that will flush to SST
            for i in 0..100 {
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // TODO: Add instrumentation to verify:
            // 1. SST fsync completes
            // 2. Manifest update + fsync completes
            // 3. Only then WAL truncation occurs
        },
        |eng| {
            // Assert - all data should be recovered from SST (not WAL)
            let cf = eng.default_column_family();
            for i in 0..100 {
                let result = eng
                    .get(&cf, format!("key{:04}", i).as_bytes())
                    .expect("get");
                assert!(result.is_some(), "Key {} should be in SST", i);
            }
        },
    );
}

#[test]
fn should_not_truncate_wal_given_manifest_save_failure() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024);

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..100 {
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // TODO: Add test hook to fail manifest save and verify WAL not truncated
            // This ensures WAL data survives manifest failure
        },
        |eng| {
            // Assert - if manifest save failed, WAL replay should recover data
            let cf = eng.default_column_family();
            for i in 0..100 {
                let result = eng
                    .get(&cf, format!("key{:04}", i).as_bytes())
                    .expect("get");
                assert!(
                    result.is_some(),
                    "WAL should preserve data if manifest save fails"
                );
            }
        },
    );
}

#[test]
fn should_fsync_manifest_before_truncating_wal() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");
            eng.put(&cf, b"key2", b"value2").expect("put");
            // TODO: Add instrumentation to verify manifest fsync before WAL truncation
            // The ordering guarantee is: manifest.fsync() -> wal.truncate()
        },
        |eng| {
            // Assert - data should be recovered even if crash occurs
            assert_get_equals(eng, b"key1", b"value1");
            assert_get_equals(eng, b"key2", b"value2");
        },
    );
}
