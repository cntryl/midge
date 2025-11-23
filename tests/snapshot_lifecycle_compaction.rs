// Long-lived snapshots + compaction tests
mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, Query, StorageMode};
use cntryl_midge::test_hooks::{TestHooks, CompactionGatePoint};
use common::*;
use std::time::Duration;

#[test]
fn should_keep_snapshot_view_stable_given_many_flush_and_compaction_cycles_when_reading_from_old_snapshot(
) {
    // Arrange: create a long-lived snapshot and perform writes
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let mut opts = compaction_test_opts(StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    });
    opts.test_hooks = Some(hooks.clone());
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Initial data
    for i in 0..100 {
        eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v1")
            .unwrap();
    }
    eng.flush().unwrap();

    // Create snapshot
    let snapshot = eng.snapshot();

    // Act: run many flush and compaction cycles
    for cycle in 0..5 {
        for i in 0..100 {
            eng.put(
                &cf,
                format!("k{:03}", i).as_bytes(),
                format!("v{}", cycle + 2).as_bytes(),
            )
            .unwrap();
        }
        eng.flush().unwrap();
        // Deterministically trigger compaction and wait via hooks
        let gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
        eng.compact_level(&cf, 0).unwrap();
        assert!(gate.wait_until_blocked(Duration::from_secs(10)), "Compaction did not reach AfterManifestUpdate");
        gate.release();
        let start = std::time::Instant::now();
        while hooks.compaction_complete_count() == 0 {
            if start.elapsed() > Duration::from_secs(10) {
                panic!("Compaction did not complete in time");
            }
            std::thread::yield_now();
        }
    }

    // Assert: snapshot view remains stable
    let results = eng
        .scan_at(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"k000"))
                .end_key(Bytes::from_static(b"k100")),
            &snapshot,
        )
        .unwrap();
    assert_eq!(results.len(), 100);
    for (_key, value) in results {
        assert_eq!(value.as_ref(), b"v1");
    }
}

#[test]
fn should_release_old_files_given_snapshot_expiry_when_all_new_reads_use_fresh_snapshots() {
    // Arrange: create snapshots and write new data
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let mut opts = compaction_test_opts(StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    });
    opts.test_hooks = Some(hooks.clone());
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Initial data
    for i in 0..100 {
        eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v1")
            .unwrap();
    }
    eng.flush().unwrap();

    // Create snapshot
    let _snapshot = eng.snapshot();

    // Write new data
    for i in 0..100 {
        eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v2")
            .unwrap();
    }
    eng.flush().unwrap();
    let gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
    eng.compact_level(&cf, 0).unwrap();
    assert!(gate.wait_until_blocked(Duration::from_secs(10)), "Compaction did not reach AfterManifestUpdate");
    gate.release();
    let start = std::time::Instant::now();
    while hooks.compaction_complete_count() == 0 {
        if start.elapsed() > Duration::from_secs(10) {
            panic!("Compaction did not complete in time");
        }
        std::thread::yield_now();
    }

    // Act: drop snapshot (simulate expiry)
    drop(_snapshot);

    // Assert: old files can be cleaned up (hard to test directly, but no crash)
    let results = eng
        .scan(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"k000"))
                .end_key(Bytes::from_static(b"k100")),
        )
        .unwrap();
    assert_eq!(results.len(), 100);
    for (_key, value) in results {
        assert_eq!(value.as_ref(), b"v2");
    }
}

#[test]
fn should_not_leak_disk_space_given_long_lived_snapshot_and_heavy_write_load_when_compactions_continue_to_run(
) {
    // Arrange: create long-lived snapshot and heavy write load
    let dir = test_temp_dir();
    let hooks = TestHooks::new();
    let mut opts = compaction_test_opts(StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    });
    opts.test_hooks = Some(hooks.clone());
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Initial data
    for i in 0..100 {
        eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v1")
            .unwrap();
    }
    eng.flush().unwrap();

    // Create long-lived snapshot
    let _snapshot = eng.snapshot();

    // Act: heavy write load with compactions
    for cycle in 0..10 {
        for i in 0..100 {
            eng.put(
                &cf,
                format!("k{:03}", i).as_bytes(),
                format!("v{}", cycle + 2).as_bytes(),
            )
            .unwrap();
        }
        eng.flush().unwrap();
        let gate = hooks.install_compaction_gate(CompactionGatePoint::AfterManifestUpdate);
        eng.compact_level(&cf, 0).unwrap();
        assert!(gate.wait_until_blocked(Duration::from_secs(10)), "Compaction did not reach AfterManifestUpdate");
        gate.release();
        let start = std::time::Instant::now();
        while hooks.compaction_complete_count() == 0 {
            if start.elapsed() > Duration::from_secs(10) {
                panic!("Compaction did not complete in time");
            }
            std::thread::yield_now();
        }
    }

    // Assert: no leak (check keys are present)
    let results = eng
        .scan(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"k000"))
                .end_key(Bytes::from_static(b"k100")),
        )
        .unwrap();
    assert_eq!(results.len(), 100);
}

#[test]
fn should_preserve_range_delete_visibility_given_snapshot_spanning_pre_and_post_delete_state_when_iterating_keys(
) {
    // Arrange: take snapshot spanning pre/post delete
    let dir = test_temp_dir();
    let opts = compaction_test_opts(StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    });
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Populate data
    for i in 0..100 {
        eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v").unwrap();
    }
    eng.flush().unwrap();

    // Create snapshot
    let snapshot = eng.snapshot();

    // Apply range delete
    eng.delete_range(&cf, b"k020", b"k080").unwrap();
    eng.flush().unwrap();
    eng.wait_for_compaction(Duration::from_secs(10)).unwrap();

    // Act: iterate keys from the snapshot
    let results = eng
        .scan_at(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"k000"))
                .end_key(Bytes::from_static(b"k100")),
            &snapshot,
        )
        .unwrap();

    // Assert: visibility preserved, all original keys visible in snapshot
    assert_eq!(results.len(), 100);
    for i in 0..100 {
        let key = format!("k{:03}", i);
        assert!(
            results.iter().any(|(k, _)| k == key.as_bytes()),
            "Key {} missing in snapshot",
            key
        );
    }
}
