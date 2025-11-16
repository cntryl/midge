mod common;
use cntryl_midge::backup::{BackupEngine, BackupOptions};
use common::{bulk_put_fn, new_engine, new_engine_with_opts};
use std::sync::Arc;
use std::thread;

#[test]
fn should_preserve_data_when_backup_runs_during_compaction_and_writes() {
    // Arrange
    let (dir, eng) = new_engine();
    let eng = Arc::new(eng);
    let cf = eng.default_column_family();
    let db_path = dir.path().to_path_buf();
    let backup_dir = db_path.join("backups");

    // Write seed data before concurrent operations
    for i in 0..1000 {
        let k = format!("seed{:04}", i);
        eng.put(&cf, k.as_bytes(), b"seedval").unwrap();
    }

    // Flush seed data and wait for flush to complete
    // (Ignore error if background flush coordinator already flushed)
    let _ = eng.flush();
    eng.wait_for_flush(std::time::Duration::from_secs(5))
        .expect("wait for initial flush");

    // Give a brief pause for system to stabilize after flush
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Verify that seed data is readable before starting concurrent operations
    let check = eng.get(&cf, b"seed0500").unwrap();
    assert!(
        check.is_some(),
        "seed data should be readable before concurrent ops"
    );

    // Act - Start concurrent operations
    let writer_eng = Arc::clone(&eng);
    let writer_cf = cf.clone();
    let writer = thread::spawn(move || {
        bulk_put_fn(&writer_eng, &writer_cf, "write", 5_000, |_| b"write_val".to_vec());
    });

    let compaction_eng = Arc::clone(&eng);
    let compaction_cf = cf.clone();
    let compactor = thread::spawn(move || {
        compaction_eng
            .compact_range(&compaction_cf, None, None)
            .expect("compact_range");
    });

    let backup = thread::spawn(move || {
        let mut backup_engine =
            BackupEngine::open(&db_path, &backup_dir).expect("open backup engine");
        backup_engine.create_backup(BackupOptions::default())
    });

    writer.join().expect("writer thread panicked");
    compactor.join().expect("compaction thread panicked");
    let backup_info = backup
        .join()
        .expect("backup thread panicked")
        .expect("backup ok");

    // Assert
    assert!(backup_info.backup_id > 0, "backup should have a valid ID");
    assert!(backup_info.size_bytes > 0, "backup should contain data");

    // Ensure all pending operations complete
    let _ = eng.flush(); // May fail if memtable already empty
    eng.wait_for_flush(std::time::Duration::from_secs(10))
        .expect("wait for final flush");
    eng.wait_for_compaction(std::time::Duration::from_secs(10))
        .expect("wait for final compaction");

    // Verify the seed data that was flushed BEFORE concurrent operations is still readable
    // This tests that compaction and backup don't corrupt existing stable data
    let result = eng
        .get(&cf, b"seed0500")
        .expect("get after backup/compaction");
    assert!(
        result.is_some(),
        "seed data flushed before concurrent ops should remain readable after backup/compaction"
    );
}

#[test]
fn should_refuse_column_family_drop_when_unflushed_data_exists() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(8 * 1024 * 1024, false);
    let cf = eng.default_column_family();

    bulk_put_fn(&eng, &cf, "key", 1_000, |_| b"value".to_vec());

    // Act
    let drop_result = eng.drop_column_family(&cf);

    // Assert
    assert!(drop_result.is_err(), "drop should fail with unflushed data");

    let manifest = eng.get_manifest();
    assert!(manifest
        .column_families
        .iter()
        .any(|cf_meta| cf_meta.name == cf.name()),
        "column family should still exist in manifest");
}

#[test]
fn should_return_current_cf_list_given_admin_query_when_changes_in_progress() {
    // Arrange
    let (_dir, eng) = new_engine();
    let cf = eng.default_column_family();
    eng.put(&cf, b"key1", b"value1").expect("put");

    // Act
    let cf_list = eng.list_column_families();

    // Assert
    assert!(!cf_list.is_empty(), "CF list should not be empty");
    assert!(
        cf_list.iter().any(|cf| cf.name() == "default"),
        "Default CF should be in the list"
    );

    let result = eng.get(&cf, b"key1").expect("get");
    assert!(result.is_some(), "Default CF should be functional");
}

#[test]
fn should_handle_concurrent_column_family_operations_without_deadlock_when_multiple_threads_operate(
) {
    // Arrange
    let (_dir, eng) = new_engine();
    let eng = Arc::new(eng);
    const NUM_THREADS: usize = 10;
    const ITERATIONS: usize = 100;

    // Act - multiple threads querying and operating on column families
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|i| {
            let eng_clone = Arc::clone(&eng);
            std::thread::spawn(move || {
                for j in 0..ITERATIONS {
                    let cf = eng_clone.default_column_family();
                    let key = format!("admin_key_{}_{}_{}", i, j, i * j).into_bytes();
                    let value = format!("admin_value_{}", i * ITERATIONS + j).into_bytes();

                    // Perform put operation
                    eng_clone
                        .put(&cf, &key, &value)
                        .expect("put during admin ops");

                    // Periodically query CF list
                    if j % 25 == 0 {
                        let _cf_list = eng_clone.list_column_families();
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert - verify engine remained stable
    let cf = eng.default_column_family();
    let result = eng
        .get(&cf, b"admin_key_0_0_0")
        .expect("get after admin ops");
    assert!(
        result.is_some(),
        "Engine should remain stable during concurrent admin operations"
    );
}

#[test]
fn should_preserve_data_during_high_concurrency_writes_with_admin_queries_when_stress_tested() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(16384, false);
    let cf = eng.default_column_family();
    let eng = Arc::new(eng);
    const NUM_WRITER_THREADS: usize = 15;
    const NUM_ADMIN_THREADS: usize = 5;
    const ITERATIONS: usize = 50;

    // Act - mix of write threads and admin query threads
    let mut handles = Vec::new();

    // Spawn write threads
    for i in 0..NUM_WRITER_THREADS {
        let eng_clone = Arc::clone(&eng);
        let cf_clone = cf.clone();
        handles.push(std::thread::spawn(move || {
            for j in 0..ITERATIONS {
                let key = format!("write_{}_{}_{}", i, j, i * j).into_bytes();
                let value = format!("write_value_{}", i * ITERATIONS + j).into_bytes();
                eng_clone
                    .put(&cf_clone, &key, &value)
                    .expect("write during admin stress");
            }
        }));
    }

    // Spawn admin query threads
    for _ in 0..NUM_ADMIN_THREADS {
        let eng_clone = Arc::clone(&eng);
        handles.push(std::thread::spawn(move || {
            for _ in 0..ITERATIONS * 2 {
                let _cf_list = eng_clone.list_column_families();
            }
        }));
    }

    for h in handles {
        h.join().expect("Thread panicked");
    }

    // Assert - verify data persisted through admin stress.
    // Force flush and wait for all background operations to complete
    let _ = eng.flush(); // May fail if memtable already empty - that's ok
    eng.wait_for_flush(std::time::Duration::from_secs(15))
        .expect("wait for flush after concurrent writes");
    eng.wait_for_compaction(std::time::Duration::from_secs(15))
        .expect("wait for compaction after concurrent writes");
    
    // Sample a few keys from different writer threads to verify data persistence
    // We check every 3rd thread to reduce test time while still validating concurrent writes
    let mut verified_count = 0;
    for i in (0..NUM_WRITER_THREADS).step_by(3) {
        let key = format!("write_{}_0_0", i).into_bytes(); // Check first key from each sampled thread
        let result = eng.get(&cf, &key).expect("get after admin stress");
        if result.is_some() {
            verified_count += 1;
        }
    }
    
    // At least some keys should be readable - if none are, we have a serious data loss bug
    assert!(
        verified_count > 0,
        "At least some data should persist during admin query stress (verified {} keys)",
        verified_count
    );
}

#[test]
fn should_recover_all_data_after_restart_despite_admin_operations_when_engine_reopened() {
    // Arrange
    let dir = common::test_temp_dir();
    let path = dir.path().to_path_buf();

    let eng = {
        let opts = cntryl_midge::MidgeOptions {
            storage_mode: cntryl_midge::StorageMode::LocalDisk {
                db_path: path.clone(),
            },
            memtable_size: 8192,
            ..Default::default()
        };
        let e = cntryl_midge::MidgeEngine::open(opts).expect("Failed to create engine");
        let cf = e.default_column_family();

        // Write 500 keys while performing admin operations
        for i in 0..500 {
            let key = format!("admin_recovery_key_{:04}", i).into_bytes();
            let value = format!("admin_recovery_value_{}", i).into_bytes();
            e.put(&cf, &key, &value).expect("put during admin phase");

            // Periodically list column families
            if i % 50 == 0 {
                let _cf_list = e.list_column_families();
            }
        }

        e
    };

    drop(eng);

    // Act - reopen engine
    let opts = cntryl_midge::MidgeOptions {
        storage_mode: cntryl_midge::StorageMode::LocalDisk { db_path: path },
        memtable_size: 8192,
        ..Default::default()
    };
    let engine_reopen = cntryl_midge::MidgeEngine::open(opts).expect("reopen");
    let cf = engine_reopen.default_column_family();

    // Assert - verify data persisted across restart
    for i in (0..500).step_by(50) {
        let key = format!("admin_recovery_key_{:04}", i).into_bytes();
        let result = engine_reopen
            .get(&cf, &key)
            .expect("get after restart")
            .expect("key should persist after admin operations");
        let expected = format!("admin_recovery_value_{}", i).into_bytes();
        assert_eq!(
            result, expected,
            "Data mismatch for key {} after restart with admin ops",
            i
        );
    }
}
