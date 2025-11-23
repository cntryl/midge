// Range delete deep edge cases
mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, Query, StorageMode};
use common::*;

#[test]
fn should_honor_large_range_deletes_given_many_levels_when_compactions_run_repeatedly() {
    // Arrange: create many levels with keys overlapping target range
    // Act: apply large range deletes and trigger compactions repeatedly
    // Assert: deleted keys remain deleted and no resurrection occurs
    for mode in disk_storage_modes() {
        let (_n, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = compaction_test_opts(storage_mode);

        with_engine(opts.clone(), |eng| {
            let cf = eng.default_column_family();
            // Write keys in range 000-999
            for i in 0..1000 {
                eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v").unwrap();
                if i % 100 == 0 {
                    eng.flush().unwrap();
                }
            }
            // Act: delete range 100-199
            eng.delete_range(&cf, b"k100", b"k200").unwrap();
            eng.flush().unwrap();
            // Run compaction
            eng.wait_for_compaction(std::time::Duration::from_secs(2))
                .ok();

            // Assert: keys in range deleted
            for i in 100..200 {
                let got = eng.get(&cf, format!("k{:03}", i).as_bytes()).unwrap();
                assert!(got.is_none(), "key {} should be deleted", i);
            }
            // Keys outside range present
            let got = eng.get(&cf, b"k000").unwrap();
            assert!(got.is_some(), "key outside range should exist");
        });
    }
}

#[test]
fn should_not_resurrect_deleted_keys_given_interleaved_puts_and_range_deletes_when_reading_after_multiple_compactions(
) {
    // Arrange: interleave puts and range deletes across memtable/ssts
    // Act: run compactions and reopen engine to simulate recovery
    // Assert: deleted keys are not resurrected after compactions/recovery
    for mode in disk_storage_modes() {
        let (_n, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = compaction_test_opts(storage_mode);

        with_engine_restart(
            opts.clone(),
            |eng| {
                let cf = eng.default_column_family();
                // Puts
                for i in 0..100 {
                    eng.put(&cf, format!("r{:03}", i).as_bytes(), b"v").unwrap();
                }
                eng.flush().unwrap();
                // Range delete
                eng.delete_range(&cf, b"r020", b"r080").unwrap();
                eng.flush().unwrap();
                // More puts
                for i in 50..150 {
                    eng.put(&cf, format!("r{:03}", i).as_bytes(), b"v2")
                        .unwrap();
                }
                eng.flush().unwrap();
            },
            |eng| {
                let cf = eng.default_column_family();
                // Assert: deleted range r020-r049 is gone (not rewritten in batch 3)
                for i in 20..50 {
                    let got = eng.get(&cf, format!("r{:03}", i).as_bytes()).unwrap();
                    assert!(got.is_none(), "deleted key {} resurrected", i);
                }
                // Keys r050-r079 were rewritten in batch 3 so should be visible
                for i in 50..80 {
                    let got = eng.get(&cf, format!("r{:03}", i).as_bytes()).unwrap();
                    assert!(
                        got.is_some(),
                        "key {} should be visible (rewritten after delete_range)",
                        i
                    );
                }
                // Other keys present
                let got = eng.get(&cf, b"r000").unwrap();
                assert!(got.is_some());
            },
        );
    }
}

#[test]
fn should_respect_snapshot_visibility_given_range_delete_applied_after_snapshot_when_iterating_from_snapshot(
) {
    // Arrange: populate data, create snapshot, then apply range delete
    let dir = test_temp_dir();
    let opts = compaction_test_opts(StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    });
    let eng = MidgeEngine::open(opts).unwrap();
    let cf = eng.default_column_family();

    // Populate keys r000 to r099
    for i in 0..100 {
        eng.put(&cf, format!("r{:03}", i).as_bytes(), b"v").unwrap();
    }
    eng.flush().unwrap();

    // Create snapshot
    let snapshot = eng.snapshot();

    // Apply range delete after snapshot
    eng.delete_range(&cf, b"r020", b"r080").unwrap();
    eng.flush().unwrap();

    // Act: scan_at from the snapshot
    let results = eng
        .scan_at(
            &cf,
            Query::new()
                .start_key(Bytes::from_static(b"r000"))
                .end_key(Bytes::from_static(b"r100")),
            &snapshot,
        )
        .unwrap();

    // Assert: snapshot should see all original keys, including the deleted range
    assert_eq!(results.len(), 100);
    for i in 0..100 {
        let key = format!("r{:03}", i);
        let expected = (Bytes::from(key.clone()), Bytes::from_static(b"v"));
        assert!(
            results.contains(&expected),
            "Missing key {} in snapshot",
            key
        );
    }
}

#[test]
fn should_apply_range_deletes_consistently_across_memtable_and_sst_boundaries_given_crash_between_flush_and_compaction(
) {
    // Arrange: apply range delete, flush to SST, then crash (restart)
    let dir = test_temp_dir();
    let opts = compaction_test_opts(StorageMode::LocalDisk {
        db_path: dir.path().to_path_buf(),
    });

    with_engine_restart(
        opts.clone(),
        |eng| {
            let cf = eng.default_column_family();
            // Populate keys
            for i in 0..100 {
                eng.put(&cf, format!("k{:03}", i).as_bytes(), b"v").unwrap();
            }
            eng.flush().unwrap();
            // Apply range delete
            eng.delete_range(&cf, b"k020", b"k080").unwrap();
            // Flush to create SST with tombstones
            eng.flush().unwrap();
            // Don't compact yet - simulate crash
        },
        |eng| {
            let cf = eng.default_column_family();
            // Act: scan after restart (recovery may trigger compaction)
            let results = eng
                .scan(
                    &cf,
                    Query::new()
                        .start_key(Bytes::from_static(b"k000"))
                        .end_key(Bytes::from_static(b"k100")),
                )
                .unwrap();

            // Assert: range delete applied consistently - deleted keys absent
            assert_eq!(results.len(), 40); // 100 - 60 deleted
            for i in 0..20 {
                let key = format!("k{:03}", i);
                assert!(
                    results.iter().any(|(k, _)| k == key.as_bytes()),
                    "Key {} should be present",
                    key
                );
            }
            for i in 20..80 {
                let key = format!("k{:03}", i);
                assert!(
                    !results.iter().any(|(k, _)| k == key.as_bytes()),
                    "Key {} should be deleted",
                    key
                );
            }
            for i in 80..100 {
                let key = format!("k{:03}", i);
                assert!(
                    results.iter().any(|(k, _)| k == key.as_bytes()),
                    "Key {} should be present",
                    key
                );
            }
        },
    );
}
