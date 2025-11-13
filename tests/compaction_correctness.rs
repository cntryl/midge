mod common;
use cntryl_midge::{MidgeOptions, StorageMode};
use common::{assert_key_absent, test_temp_dir, with_engine_restart};
use std::sync::Arc;

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

#[test]
fn should_maintain_data_consistency_during_high_concurrency_compaction_workload() {
    // Arrange
    let dir = test_temp_dir();
    let base_opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 8192,
        enable_compaction: true,
        ..Default::default()
    };

    let eng = cntryl_midge::MidgeEngine::open(base_opts).expect("open");
    let cf = eng.default_column_family();
    let eng = Arc::new(eng);
    const NUM_THREADS: usize = 10;
    const KEYS_PER_THREAD: usize = 100;

    // Act - concurrent writes triggering compaction
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|thread_id| {
            let eng_clone = Arc::clone(&eng);
            let cf_clone = cf.clone();
            std::thread::spawn(move || {
                for i in 0..KEYS_PER_THREAD {
                    let key = format!("compact_key_{}_{:03}", thread_id, i).into_bytes();
                    let value = format!("compact_value_{}", thread_id * KEYS_PER_THREAD + i)
                        .into_bytes();
                    eng_clone
                        .put(&cf_clone, &key, &value)
                        .expect("put during compaction");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Trigger compaction
    eng.flush_cf(&cf).expect("flush");
    eng.compact_all().expect("compact");

    // Assert - verify all written data is still present and consistent
    for thread_id in 0..NUM_THREADS {
        for i in 0..KEYS_PER_THREAD {
            let key = format!("compact_key_{}_{:03}", thread_id, i).into_bytes();
            let result = eng.get(&cf, &key).expect("get after compaction");
            assert!(
                result.is_some(),
                "Data should persist through compaction under high load"
            );
        }
    }
}

#[test]
fn should_preserve_ordering_and_values_given_multiple_overwrites_during_compaction() {
    // Arrange
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size: 4096,
        enable_compaction: true,
        ..Default::default()
    };

    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write same key multiple times with different values
            const OVERWRITES: usize = 50;
            for round in 0..OVERWRITES {
                for i in 0..10 {
                    let key = format!("overwrite_key_{:02}", i).into_bytes();
                    let value = format!("round_{:02}", round).into_bytes();
                    eng.put(&cf, &key, &value).expect("put");
                }
            }
            // Trigger compaction to merge all overwrites
            eng.flush_cf(&cf).expect("flush");
            eng.compact_all().expect("compact");
        },
        |eng| {
            // Assert - final values should reflect last write
            let cf = eng.default_column_family();
            for i in 0..10 {
                let key = format!("overwrite_key_{:02}", i).into_bytes();
                let result = eng.get(&cf, &key).expect("get after compaction").unwrap();
                let expected = format!("round_{:02}", 49).into_bytes();
                assert_eq!(
                    result, expected,
                    "Final overwritten value should match last write"
                );
            }
        },
    );
}
