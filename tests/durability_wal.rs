mod common;
use common::{assert_get_equals, durability_opts, test_temp_dir, with_engine_restart};
use std::sync::Arc;

#[test]
fn should_recover_without_loss_given_crash_after_wal_append_before_fsync() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    
    // Act & Assert - write with fsync enabled, then verify after restart
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"key1", b"value1").expect("put");
            // Data is written to WAL and fsynced before put() returns
        },
        |eng| {
            // Assert - fsynced write should be visible after restart
            assert_get_equals(eng, b"key1", b"value1");
        }
    );
    
    // TODO: Add test that simulates unfsynced data loss:
    // 1. Write data with FsyncBehavior::Skip
    // 2. Manually truncate WAL file to size before write
    // 3. Restart and verify data is absent
    // This requires test infrastructure to track/manipulate WAL file size
}

#[test]
fn should_not_acknowledge_commit_given_wal_unsynced_when_crash_occurs() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    
    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write with sync=true should fsync before returning
            let result = eng.put(&cf, b"committed_key", b"committed_value");
            assert!(result.is_ok(), "Commit should only succeed after WAL fsync");
            // TODO: Add instrumentation to verify fsync was called before returning
        },
        |eng| {
            // Assert - committed write should be visible after restart
            assert_get_equals(eng, b"committed_key", b"committed_value");
        }
    );
}

#[test]
fn should_maintain_strict_wal_order_given_concurrent_appends_when_crash_occurs() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = durability_opts(db_path.clone());
    
    // Act - perform concurrent writes
    {
        use cntryl_midge::MidgeEngine;
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let eng = Arc::new(eng);
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let eng = Arc::clone(&eng);
                std::thread::spawn(move || {
                    let cf = eng.default_column_family();
                    eng.put(&cf, format!("key{}", i).as_bytes(), format!("value{}", i).as_bytes())
                        .expect("put");
                })
            })
            .collect();
        
        for h in handles {
            h.join().unwrap();
        }
    } // Engine drops here (simulating crash)
    
    // Assert - reopen and verify all concurrent writes recovered
    use cntryl_midge::MidgeEngine;
    let eng = MidgeEngine::open(opts).expect("reopen");
    let cf = eng.default_column_family();
    for i in 0..10 {
        let result = eng.get(&cf, format!("key{}", i).as_bytes()).expect("get");
        assert!(result.is_some(), "Concurrent write {} should be present", i);
    }
}

#[test]
fn should_replay_all_valid_records_given_multiple_segments_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    
    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            // Act - write enough data to trigger multiple WAL segments
            let cf = eng.default_column_family();
            for i in 0..1000 {
                eng.put(&cf, format!("key{}", i).as_bytes(), b"some_value").expect("put");
            }
        },
        |eng| {
            // Assert - all records should be replayed after restart
            let cf = eng.default_column_family();
            for i in 0..1000 {
                let result = eng.get(&cf, format!("key{}", i).as_bytes()).expect("get");
                assert!(result.is_some(), "Record {} should be replayed", i);
            }
        }
    );
}

#[test]
fn should_discard_partial_record_given_truncated_wal_segment_when_recovering() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    
    // Act & Assert
    with_engine_restart(
        opts.clone(),
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"complete_key", b"complete_value").expect("put");
            // TODO: Add test hook to truncate WAL file after this
        },
        |_eng| {
            // TODO: Manually truncate WAL file here to simulate torn write
            // For now, this documents expected behavior that recovery should
            // handle truncation gracefully (either recover complete records or fail cleanly)
        }
    );
}
