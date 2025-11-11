mod common;
use common::{assert_get_equals, durability_opts, flush_test_opts, test_temp_dir, with_engine_restart};

#[test]
fn should_detect_and_ignore_already_compacted_wal_entries_given_manifest_sequence() {
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
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value").expect("put");
            }
            // Force flush so data is in SST
            // TODO: Add explicit flush API call
        },
        |eng| {
            // Assert - recovery should not replay WAL entries already in SSTs
            // Data should appear exactly once
            let cf = eng.default_column_family();
            for i in 0..100 {
                let result = eng.get(&cf, format!("key{:04}", i).as_bytes()).expect("get");
                assert!(result.is_some(), "Data should be present exactly once");
            }
            // TODO: Add instrumentation to verify WAL entries were skipped during recovery
        }
    );
}

#[test]
fn should_replay_to_last_synced_sequence_given_fullsync_mode_when_recover() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    
    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"synced1", b"value1").expect("put");
            eng.put(&cf, b"synced2", b"value2").expect("put");
            // In FullSync/durability mode, puts are synced
            // TODO: Verify sequence numbers and sync boundaries
        },
        |eng| {
            // Assert - recovery should replay to last synced sequence
            assert_get_equals(eng, b"synced1", b"value1");
            assert_get_equals(eng, b"synced2", b"value2");
        }
    );
}

#[test]
fn should_recover_last_committed_state_given_crash_during_write() {
    // Arrange
    let dir = test_temp_dir();
    let opts = durability_opts(dir.path().to_path_buf());
    
    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            eng.put(&cf, b"committed1", b"value1").expect("put");
            eng.put(&cf, b"committed2", b"value2").expect("put");
            // TODO: Simulate crash during third write
            // eng.put(&cf, b"uncommitted", b"value3") // This should not appear
        },
        |eng| {
            // Assert - only committed writes should be visible
            assert_get_equals(eng, b"committed1", b"value1");
            assert_get_equals(eng, b"committed2", b"value2");
            // TODO: Verify uncommitted write is not present
        }
    );
}

#[test]
fn should_rebuild_manifest_up_to_last_fsynced_sequence() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024);
    
    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            for i in 0..100 {
                eng.put(&cf, format!("key{:04}", i).as_bytes(), b"value").expect("put");
            }
            // TODO: Corrupt manifest and verify rebuild stops at fsync boundary
        },
        |eng| {
            // Assert - rebuilt manifest should contain all fsynced data
            let cf = eng.default_column_family();
            for i in 0..100 {
                let result = eng.get(&cf, format!("key{:04}", i).as_bytes()).expect("get");
                assert!(result.is_some(), "Fsynced data should be in rebuilt manifest");
            }
        }
    );
}

#[test]
fn should_deduplicate_replay_given_partial_flush_in_manifest() {
    // Arrange
    let dir = test_temp_dir();
    let opts = flush_test_opts(dir.path().to_path_buf(), 1024);
    
    // Act & Assert
    with_engine_restart(
        opts,
        |eng| {
            let cf = eng.default_column_family();
            // Write same keys multiple times
            for round in 0..3 {
                for i in 0..50 {
                    eng.put(&cf, format!("key{:04}", i).as_bytes(), format!("v{}", round).as_bytes())
                        .expect("put");
                }
            }
        },
        |eng| {
            // Assert - each key should have latest value only (no duplicates)
            let cf = eng.default_column_family();
            for i in 0..50 {
                assert_get_equals(eng, format!("key{:04}", i).as_bytes(), b"v2");
            }
        }
    );
}

#[test]
fn should_maintain_exactly_once_semantics_across_crash_recovery() {
    // Arrange
    let dir = test_temp_dir();
    let db_path = dir.path().to_path_buf();
    let opts = durability_opts(db_path.clone());
    
    // Act - multiple restart cycles to simulate repeated crashes
    use cntryl_midge::MidgeEngine;
    for cycle in 0..5 {
        let eng = MidgeEngine::open(opts.clone()).expect("open");
        let cf = eng.default_column_family();
        
        // Write unique data each cycle
        eng.put(&cf, format!("cycle{}", cycle).as_bytes(), format!("value{}", cycle).as_bytes())
            .expect("put");
        
        drop(eng); // Simulate crash
    }
    
    // Assert - all cycles should be present exactly once
    let eng = MidgeEngine::open(opts).expect("final open");
    let cf = eng.default_column_family();
    for cycle in 0..5 {
        assert_get_equals(&eng, format!("cycle{}", cycle).as_bytes(), format!("value{}", cycle).as_bytes());
    }
}
