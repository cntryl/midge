mod common;
use cntryl_midge::MidgeOptions;
use common::{assert_get_equals, durability_opts, test_temp_dir, with_engine_restart};

#[test]
fn should_commit_new_ssts_and_manifest_together_given_compaction_successful() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        memtable_size: 1024,
        enable_compaction: true,
        ..durability_opts(dir.path().to_path_buf())
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write overlapping keys to trigger compaction
            for i in 0..200 {
                eng.put(
                    &cf,
                    format!("key{:04}", i % 50).as_bytes(),
                    format!("value{}", i).as_bytes(),
                )
                .expect("put");
            }
            // TODO: Wait for compaction to complete and verify atomic commit
        },
        |eng| {
            // Assert - all latest values should be present after restart
            let cf = eng.default_column_family();
            for i in 0..50 {
                let result = eng
                    .get(&cf, format!("key{:04}", i).as_bytes())
                    .expect("get");
                assert!(result.is_some(), "Compacted key {} should exist", i);
            }
        },
    );
}

#[test]
fn should_cleanup_partial_output_given_compaction_failure() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        memtable_size: 1024,
        enable_compaction: true,
        ..durability_opts(dir.path().to_path_buf())
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..200 {
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // TODO: Inject compaction failure and verify partial outputs cleaned up
        },
        |eng| {
            // Assert - database should be consistent (no orphaned partial SSTs)
            let cf = eng.default_column_family();
            for i in 0..200 {
                let result = eng
                    .get(&cf, format!("key{:04}", i).as_bytes())
                    .expect("get");
                assert!(
                    result.is_some(),
                    "Data should be preserved despite compaction failure"
                );
            }
        },
    );
}

#[test]
fn should_delete_old_sst_files_only_after_manifest_persisted() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        memtable_size: 1024,
        enable_compaction: true,
        ..durability_opts(dir.path().to_path_buf())
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write data to create multiple SSTs
            for round in 0..3 {
                for i in 0..100 {
                    eng.put(
                        &cf,
                        format!("key{:04}", i).as_bytes(),
                        format!("v{}", round).as_bytes(),
                    )
                    .expect("put");
                }
            }
            // TODO: Verify old SSTs deleted only after manifest persisted
        },
        |eng| {
            // Assert - latest values should be present
            let _cf = eng.default_column_family();
            for i in 0..100 {
                assert_get_equals(eng, format!("key{:04}", i).as_bytes(), b"v2");
            }
        },
    );
}

#[test]
fn should_fsync_new_ssts_before_updating_manifest() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        memtable_size: 1024,
        enable_compaction: true,
        ..durability_opts(dir.path().to_path_buf())
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..200 {
                eng.put(&cf, format!("key{:04}", i % 50).as_bytes(), b"value")
                    .expect("put");
            }
            // TODO: Add instrumentation to verify new SST fsync before manifest update
        },
        |eng| {
            // Assert - compacted data should be durable
            let cf = eng.default_column_family();
            for i in 0..50 {
                let result = eng
                    .get(&cf, format!("key{:04}", i).as_bytes())
                    .expect("get");
                assert!(result.is_some(), "Compacted key should be durable");
            }
        },
    );
}

#[test]
fn should_recover_consistent_state_given_crash_mid_compaction_when_restart() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        memtable_size: 1024,
        enable_compaction: true,
        ..durability_opts(dir.path().to_path_buf())
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..200 {
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // TODO: Simulate crash during compaction
        },
        |eng| {
            // Assert - all data should be present (either from old SSTs or new SSTs)
            let cf = eng.default_column_family();
            for i in 0..200 {
                let result = eng
                    .get(&cf, format!("key{:04}", i).as_bytes())
                    .expect("get");
                assert!(result.is_some(), "Data should survive crash mid-compaction");
            }
        },
    );
}

#[test]
fn should_preserve_source_ssts_when_compaction_output_not_fsynced() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        memtable_size: 1024,
        enable_compaction: true,
        ..durability_opts(dir.path().to_path_buf())
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..200 {
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // TODO: Simulate crash before compaction output fsync completes
            // Source SSTs should be preserved
        },
        |eng| {
            // Assert - data should be recoverable from source SSTs
            let cf = eng.default_column_family();
            for i in 0..200 {
                let result = eng
                    .get(&cf, format!("key{:04}", i).as_bytes())
                    .expect("get");
                assert!(result.is_some(), "Source SSTs should preserve data");
            }
        },
    );
}
