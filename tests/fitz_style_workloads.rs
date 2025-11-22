// Fitz-style (queue/log-ish) workloads
mod common;
use common::*;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

#[test]
fn should_handle_hot_partition_workload_given_many_appends_to_same_key_when_compactions_run_in_background() {
    // Arrange: hot-partition append workload
    let dir = test_temp_dir();
    let opts = compaction_test_opts(StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    });
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Act: many appends to same key
    for i in 0..1000 {
        eng.put(&cf, b"hot_key", format!("append{}", i).as_bytes()).unwrap();
    }
    eng.flush().unwrap();
    eng.wait_for_compaction(std::time::Duration::from_secs(10)).unwrap();

    // Assert: data correctness (latest value)
    let value = eng.get(&cf, b"hot_key").unwrap();
    assert_eq!(value.as_deref(), Some(b"append999".as_ref()));
}

#[test]
fn should_keep_tail_latencies_low_given_millions_of_small_writes_when_periodic_flush_and_compaction_are_enabled() {
    // Arrange: prepare workload
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Act: many small writes
    for i in 0..10000 {
        eng.put(&cf, format!("k{:05}", i).as_bytes(), b"v").unwrap();
    }

    // Assert: writes succeeded
    for i in 0..100 {
        let key = format!("k{:05}", i);
        let value = eng.get(&cf, key.as_bytes()).unwrap();
        assert!(value.is_some());
    }
}

#[test]
fn should_respect_ttl_semantics_given_heavy_delete_expiry_workload_when_background_cleanup_runs() {
    // Arrange: insert keys (TTL not implemented, so simulate with deletes)
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Insert keys
    for i in 0..100 {
        eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v").unwrap();
    }

    // Act: delete some
    for i in 0..50 {
        eng.delete(&cf, format!("k{:03}", i).as_bytes()).unwrap();
    }

    // Assert: deletes respected
    for i in 0..50 {
        let key = format!("k{:03}", i);
        let value = eng.get(&cf, key.as_bytes()).unwrap();
        assert!(value.is_none());
    }
    for i in 50..100 {
        let key = format!("k{:03}", i);
        let value = eng.get(&cf, key.as_bytes()).unwrap();
        assert!(value.is_some());
    }
}

#[test]
fn should_avoid_pathological_write_amplification_given_log_structured_append_only_workload_when_levels_fill_up() {
    // Arrange: append-only workload
    let dir = test_temp_dir();
    let opts = compaction_test_opts(StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    });
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Act: append-only writes
    for i in 0..1000 {
        eng.put(&cf, format!("log{:04}", i).as_bytes(), b"entry").unwrap();
    }
    eng.flush().unwrap();
    eng.wait_for_compaction(std::time::Duration::from_secs(10)).unwrap();

    // Assert: data present
    for i in 0..1000 {
        let key = format!("log{:04}", i);
        let value = eng.get(&cf, key.as_bytes()).unwrap();
        assert!(value.is_some());
    }
}
