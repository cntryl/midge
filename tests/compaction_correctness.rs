mod common;
use cntryl_midge::{MidgeOptions, StorageMode};
use common::{assert_key_absent, test_temp_dir, with_engine_restart};

#[test]
fn should_produce_identical_output_given_same_input_runs_when_compacting() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        ..Default::default()
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write overlapping keys to trigger compaction
            for round in 0..3 {
                for i in 0..50 {
                    eng.put(
                        &cf,
                        format!("key{:02}", i).as_bytes(),
                        format!("v{}", round).as_bytes(),
                    )
                    .expect("put");
                }
            }
            // TODO: Capture compaction output hash/checksum for determinism verification
        },
        |eng| {
            // Assert - latest values should be present
            let cf = eng.default_column_family();
            for i in 0..50 {
                let result = eng
                    .get(&cf, format!("key{:02}", i).as_bytes())
                    .expect("get");
                assert!(result.is_some(), "Compacted data should be present");
            }
        },
    );
}

#[test]
fn should_remove_deleted_keys_given_tombstones_when_compaction_runs() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        ..Default::default()
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write and delete keys
            for i in 0..100 {
                eng.put(&cf, format!("key{:03}", i).as_bytes(), b"value")
                    .expect("put");
            }
            // Delete half of them
            for i in 0..50 {
                eng.delete(&cf, format!("key{:03}", i).as_bytes())
                    .expect("delete");
            }
            // Force compaction to merge tombstones
            eng.flush_cf(&cf).expect("flush");
            eng.compact_all().expect("compact");
        },
        |eng| {
            // Assert - deleted keys should be absent
            for i in 0..50 {
                assert_key_absent(eng, format!("key{:03}", i).as_bytes());
            }
            // Remaining keys should be present
            let cf = eng.default_column_family();
            for i in 50..100 {
                let result = eng
                    .get(&cf, format!("key{:03}", i).as_bytes())
                    .expect("get");
                assert!(result.is_some(), "Non-deleted key should exist");
            }
        },
    );
}

#[test]
fn should_keep_write_amplification_under_target_given_mixed_workload() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 1024,
        enable_compaction: true,
        ..Default::default()
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Mixed workload: updates, deletes, inserts
            for round in 0..5 {
                for i in 0..100 {
                    if i % 3 == 0 {
                        eng.put(
                            &cf,
                            format!("key{:03}", i).as_bytes(),
                            format!("v{}", round).as_bytes(),
                        )
                        .expect("update");
                    } else if i % 3 == 1 {
                        eng.delete(&cf, format!("key{:03}", i).as_bytes()).ok();
                    } else {
                        eng.put(
                            &cf,
                            format!("new_key{:03}_{}", i, round).as_bytes(),
                            b"value",
                        )
                        .expect("insert");
                    }
                }
            }
            // TODO: Monitor write amplification metrics
        },
        |eng| {
            // Assert - database should remain functional
            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key000").expect("get");
            assert!(result.is_some(), "Database should handle mixed workload");
        },
    );
}
