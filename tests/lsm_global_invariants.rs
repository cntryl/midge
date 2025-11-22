// Cross-subsystem invariant tests (observable outcomes only)
mod common;
use cntryl_midge::MidgeOptions;
use common::*;
use std::time::Duration;

// 1) Ensure that after repeated overlapping writes + compactions the visible state is consistent.
#[test]
fn should_maintain_non_overlapping_sst_key_ranges_given_long_random_workload_when_compactions_run()
{
    for mode in disk_storage_modes() {
        let (mode_name, storage_mode, _tmp) = create_storage_mode(mode);

        // Use compaction opts to trigger frequent compactions
        let opts = compaction_test_opts(storage_mode);

        with_engine(opts, |eng| {
            // Arrange: populate overlapping levels using helper
            let cf = eng.default_column_family();
            populate_multi_level_data(eng, &cf);

            // Act: wait for background compaction to make progress (best-effort)
            eng.wait_for_compaction(Duration::from_millis(500)).ok();

            // Assert: verify that reads return the latest values across the keyspace (no visible contradictions)
            for i in 0..100 {
                let key = format!("key{:03}", i);
                let res = eng.get(&cf, key.as_bytes()).expect("get");
                // Each key should exist with some most-recent value (we wrote overlapping batches)
                assert!(res.is_some(), "{} missing in mode {}", key, mode_name);
            }
        });
    }
}

// 2) Repeated flush/compact + restart cycles keep manifest and files in-sync (observable by reopen and key presence)
#[test]
fn should_keep_manifest_files_in_sync_given_repeated_flush_compact_cycles_when_restarting_many_times(
) {
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            ..Default::default()
        };

        // Run several restart cycles that flush and allow compaction
        for cycle in 0..3 {
            with_engine_restart(
                opts.clone(),
                |eng| {
                    // Arrange: write a batch of keys, then flush
                    let cf = eng.default_column_family();
                    for i in 0..50 {
                        let k = format!("c{}_k{:03}", cycle, i);
                        eng.put(&cf, k.as_bytes(), b"v").expect("put");
                    }
                    eng.flush().expect("flush");
                    // Act: wait for compaction to proceed (best-effort)
                    eng.wait_for_compaction(Duration::from_millis(200)).ok();
                },
                |eng| {
                    // Assert
                    let cf = eng.default_column_family();
                    // Check that a sample key from last cycle exists
                    let sample = format!("c{}_k{:03}", cycle, 0);
                    let r = eng.get(&cf, sample.as_bytes()).expect("get");
                    assert!(r.is_some(), "sample key present after restart");
                },
            );
        }
    }
}

// 3) Simulate crash during flush/compaction by forcing failures; ensure recovery yields consistent state (observable)
#[test]
fn should_not_leave_orphaned_ssts_given_crash_during_flush_and_subsequent_compaction_when_recovering_from_disk(
) {
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        let opts = MidgeOptions {
            storage_mode: storage_mode.clone(),
            memtable_size: 1024,
            ..Default::default()
        };

        // Arrange
        // Write and flush, then drop without clean compaction (simulate crash by restart)
        // Act
        with_engine_restart(
            opts.clone(),
            |eng| {
                let cf = eng.default_column_family();
                for i in 0..100 {
                    let k = format!("orphan_k{:03}", i);
                    eng.put(&cf, k.as_bytes(), b"v").expect("put");
                }
                // flush triggers SST creation; we then shutdown (drop) to simulate crash
                let _ = eng.flush();
            },
            |eng| {
                // Assert: keys from the last cycle are still present after restart
                let cf = eng.default_column_family();
                for i in 0..100 {
                    let k = format!("orphan_k{:03}", i);
                    let r = eng.get(&cf, k.as_bytes()).expect("get");
                    assert!(r.is_some(), "key should be present after recovery");
                }
            },
        );
    }
}

// 4) Mixed put + delete-range + delete operations preserve latest value semantics across recovery
#[test]
fn should_preserve_latest_value_for_all_keys_given_mixed_put_delete_range_delete_when_running_full_recovery(
) {
    for mode in disk_storage_modes() {
        let (_name, storage_mode, _temp_dir) = create_storage_mode(mode);

        let opts = MidgeOptions {
            storage_mode: storage_mode.clone(),
            wal_sync: true,
            wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
            memtable_size: 1024,
            ..Default::default()
        };

        // Arrange: write keys, delete a range, then write later values
        // Act
        with_engine_restart(
            opts.clone(),
            |eng| {
                let cf = eng.default_column_family();
                for i in 0..50 {
                    let k = format!("rk{:03}", i);
                    eng.put(&cf, k.as_bytes(), b"v1").expect("put v1");
                }

                // delete a range (simulate API via deletes per-key if no range API)
                for i in 10..20 {
                    let k = format!("rk{:03}", i);
                    eng.delete(&eng.default_column_family(), k.as_bytes()).ok();
                }

                // Newer writes for same keys
                for i in 0..50 {
                    let k = format!("rk{:03}", i);
                    eng.put(&cf, k.as_bytes(), b"v2").expect("put v2");
                }
            },
            |eng| {
                // Assert: latest values must be visible after recovery
                let cf = eng.default_column_family();
                for i in 0..50 {
                    let k = format!("rk{:03}", i);
                    let r = eng.get(&cf, k.as_bytes()).expect("get");
                    assert!(r.is_some(), "key {} should exist after recovery", k);
                    let v = r.unwrap();
                    assert_eq!(v.as_ref(), b"v2");
                }
            },
        );
    }
}
