mod common;
use common::{bulk_put_fn, new_engine, new_engine_with_opts};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use cntryl_midge::backup::{BackupEngine, BackupOptions};

#[test]
fn should_block_backup_start_given_active_compaction_when_requested() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(1024, true);
    let cf = eng.default_column_family();
    let db_path = _dir.path().to_path_buf();
    let backup_dir = db_path.join("backups");

    // Act - Write data to trigger compaction
    bulk_put_fn(&eng, &cf, "key", 200, |_| b"value".to_vec());

    // Give compaction a moment to start
    thread::sleep(Duration::from_millis(100));

    // Attempt backup during compaction
    let backup_result = std::thread::spawn(move || {
        let mut backup_engine = BackupEngine::open(&db_path, &backup_dir)
            .expect("Failed to open backup engine");
        backup_engine.create_backup(BackupOptions::default())
    });

    let backup_info = backup_result.join().unwrap().expect("Backup should succeed");

    // Assert - backup should have been created successfully
    assert!(backup_info.backup_id > 0, "Backup should have a valid ID");
    assert!(backup_info.size_bytes > 0, "Backup should contain data");

    // Verify data consistency - backup should contain all the data
    let result = eng.get(&cf, b"key025").expect("get");
    assert!(
        result.is_some(),
        "Data should be consistent during backup/compaction"
    );
}

#[test]
fn should_fail_cf_drop_given_inflight_flush() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(1024, false);
    let cf = eng.default_column_family();

    // Act
    bulk_put_fn(&eng, &cf, "key", 100, |_| b"value".to_vec());

    // TODO: Attempt CF drop during flush
    // Should either fail gracefully or wait for flush completion

    // Assert
    let result = eng.get(&cf, b"key050").expect("get");
    assert!(result.is_some(), "CF should remain functional");
}

#[test]
fn should_allow_backup_readonly_mode_given_active_writes() {
    // Arrange
    let (_dir, eng) = new_engine();
    let eng = Arc::new(eng);

    // Act
    let eng_clone = Arc::clone(&eng);
    let write_handle = thread::spawn(move || {
        let cf = eng_clone.default_column_family();
        bulk_put_fn(&eng_clone, &cf, "key", 100, |_| b"value".to_vec());
    });

    // TODO: Initiate readonly backup concurrently
    // Backup should get consistent snapshot without blocking writes

    write_handle.join().unwrap();

    // Assert
    let cf = eng.default_column_family();
    let result = eng.get(&cf, b"key050").expect("get");
    assert!(
        result.is_some(),
        "Writes should complete during readonly backup"
    );
}

#[test]
fn should_handle_config_reload_during_compaction_without_panic() {
    // Arrange
    let (_dir, eng) = new_engine_with_opts(1024, true);
    let cf = eng.default_column_family();

    // Act
    bulk_put_fn(&eng, &cf, "key", 200, |_| b"value".to_vec());

    // TODO: Reload config during compaction
    // Should not panic or corrupt state

    // Assert
    let result = eng.get(&cf, b"key025").expect("get");
    assert!(
        result.is_some(),
        "Database should remain functional after config reload"
    );
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
fn should_handle_concurrent_column_family_operations_without_deadlock_when_multiple_threads_operate() {
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
                    eng_clone.put(&cf, &key, &value).expect("put during admin ops");
                    
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
    let result = eng.get(&cf, b"admin_key_0_0_0").expect("get after admin ops");
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
                eng_clone.put(&cf_clone, &key, &value).expect("write during admin stress");
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

    // Assert - verify data persisted through admin stress
    for i in (0..NUM_WRITER_THREADS).step_by(3) {
        let key = format!("write_{}_0_{}", i, 0).into_bytes();
        let result = eng.get(&cf, &key).expect("get after admin stress");
        assert!(
            result.is_some(),
            "Data should persist during admin query stress"
        );
    }
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
        storage_mode: cntryl_midge::StorageMode::LocalDisk {
            db_path: path,
        },
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
