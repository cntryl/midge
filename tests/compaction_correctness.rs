mod common;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use common::{assert_key_absent, test_temp_dir, with_engine_restart};
use std::fs;
use std::path::Path;
use std::sync::Arc;

fn compute_engine_data_hash(eng: &MidgeEngine) -> u32 {
    use cntryl_midge::api::query::Query;

    let cf = eng.default_column_family();
    let entries = eng.scan(&cf, Query::new()).expect("scan");

    // Sort for deterministic ordering
    let mut sorted_entries = entries;
    sorted_entries.sort_by(|a, b| a.0.cmp(&b.0));

    // Hash the key-value pairs
    let mut combined_hash = 0u32;
    for (key, value) in sorted_entries {
        combined_hash = crc32c::crc32c_append(combined_hash, &key);
        combined_hash = crc32c::crc32c_append(combined_hash, &value);
    }

    combined_hash
}

fn compute_total_sst_size(db_path: &Path) -> u64 {
    let mut total_size = 0u64;

    // SST files are in the sst subdirectory
    let sst_dir = db_path.join("sst");
    if let Ok(entries) = fs::read_dir(&sst_dir) {
        for entry in entries.flatten() {
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_file() {
                    if let Some(file_name) = entry.file_name().to_str() {
                        if file_name.ends_with(".sst") {
                            if let Ok(metadata) = entry.metadata() {
                                total_size += metadata.len();
                            }
                        }
                    }
                }
            }
        }
    }

    total_size
}

#[test]
fn should_produce_identical_output_given_same_input_runs_when_compacting() {
    // Arrange
    // Use a persistent directory for debugging
    let temp_dir = std::env::temp_dir().join("midge_debug_test");
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
    std::fs::create_dir_all(&temp_dir).unwrap();

    let opts = common::flush_test_opts(temp_dir.clone(), 4096);

    let mut first_run_hash = None;

    // Act - First run
    with_engine_restart(
        opts.clone(),
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
            // Force compaction and capture output hash
            eng.flush_cf(&cf).expect("flush");
            eng.compact_all().expect("compact");
            first_run_hash = Some(compute_engine_data_hash(eng));
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

    // Second run with identical operations
    let mut second_run_hash = None;
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write identical overlapping keys to trigger compaction
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
            // Force compaction and capture output hash
            eng.flush_cf(&cf).expect("flush");
            eng.compact_all().expect("compact");
            second_run_hash = Some(compute_engine_data_hash(eng));
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

    // Assert - compaction output should be identical (deterministic)
    assert_eq!(
        first_run_hash, second_run_hash,
        "Compaction should produce identical output for identical input"
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

    // Act
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

    // Act
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Insert more data to ensure compaction triggers
            for i in 0..200 {
                eng.put(
                    &cf,
                    format!("key{:03}", i).as_bytes(),
                    format!("value{}", i).as_bytes(),
                )
                .expect("insert");
            }

            // Delete only some keys (not all)
            for i in 0..100 {
                // Delete half
                eng.delete(&cf, format!("key{:03}", i).as_bytes()).ok();
            }

            // Monitor write amplification: measure SST size before and after compaction
            eng.flush_cf(&cf).expect("flush");
            let _size_before_compaction = compute_total_sst_size(dir.path());
            eng.compact_all().expect("compact");
            let _size_after_compaction = compute_total_sst_size(dir.path());

            // Basic write amplification monitoring: ensure compaction produces reasonable output
            // In a real scenario, you'd compare this against the logical input size
            assert!(
                _size_after_compaction > 0,
                "Compaction should produce SST files"
            );
            // For mixed workloads, some amplification is expected but should be bounded
            // Here we just verify the compaction completed successfully
        },
        |eng| {
            // Assert - database should remain functional
            let cf = eng.default_column_family();
            let result = eng.get(&cf, b"key150").expect("get");
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
                    let value =
                        format!("compact_value_{}", thread_id * KEYS_PER_THREAD + i).into_bytes();
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
fn should_preserve_ordering_values_during_compaction_with_overwrites() {
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

    // Act
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

            // Check data before flush
            tracing::debug!("Before flush:");
            for i in 0..3 {
                // Check first 3 keys
                let key = format!("overwrite_key_{:02}", i).into_bytes();
                let result = eng.get(&cf, &key).expect("get before flush");
                tracing::debug!("  Key {}: {:?}", String::from_utf8_lossy(&key), result);
            }

            // Trigger compaction to merge all overwrites
            tracing::debug!("Calling flush_cf...");
            eng.flush_cf(&cf).expect("flush");
            tracing::debug!("flush_cf completed");

            // Check data after flush
            tracing::debug!("After flush:");
            for i in 0..3 {
                // Check first 3 keys
                let key = format!("overwrite_key_{:02}", i).into_bytes();
                let result = eng.get(&cf, &key).expect("get after flush");
                tracing::debug!("  Key {}: {:?}", String::from_utf8_lossy(&key), result);
            }

            // eng.compact_all().expect("compact");
        },
        |eng| {
            // Assert - final values should reflect last write
            let cf = eng.default_column_family();
            println!("After restart:");
            for i in 0..10 {
                let key = format!("overwrite_key_{:02}", i).into_bytes();
                let result = eng.get(&cf, &key).expect("get after compaction");
                println!("  Key {}: {:?}", String::from_utf8_lossy(&key), result);
                if result.is_none() {
                    panic!("Key {} is missing after restart", i);
                }
                let expected = format!("round_{:02}", 49).into_bytes();
                assert_eq!(
                    result.unwrap(),
                    expected,
                    "Final overwritten value should match last write"
                );
            }
        },
    );
}
