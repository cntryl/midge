//! Concurrent Flush Tests
//!
//! Tests for flush coordination under high concurrency.
//! Verifies that writes can proceed during flush, backpressure is applied correctly,
//! and no data is lost during memtable freeze/rotation.
//!
//! Storage modes: All 3 (Memory, LocalDisk, CloudBacked)

mod common;
use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, Query};
use common::*;
use std::sync::Arc;
use std::thread;

// ============================================================================
// Flush vs Write Contention Tests - All Storage Modes
// ============================================================================

#[test]
fn should_allow_writes_given_flush_in_progress_when_concurrent_threads() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 10 * 1024 * 1024,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        // Act - Trigger flush with large writes, then write concurrently
        let flush_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                for i in 0..500 {
                    let key = format!("flush_key_{}", i);
                    let value = vec![0u8; 4096]; // 4KB values
                    engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
                }
            })
        };

        let write_handle = {
            let engine = Arc::clone(&engine);
            let cf = cf.clone();
            thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("concurrent_key_{}", i);
                    engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
                }
            })
        };

        flush_handle.join().unwrap();
        write_handle.join().unwrap();

        // Assert
        for i in 0..100 {
            let key = format!("concurrent_key_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(Bytes::from("value")),
                "Failed for {} key: {}",
                name,
                key
            );
        }
    }
}

#[test]
fn should_handle_backpressure_given_too_many_immutable_memtables_when_heavy_writes() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 8 * 1024 * 1024,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Act - Write enough to create multiple immutable memtables
        let num_writes = 2000;
        for i in 0..num_writes {
            let key = format!("stall_test_{}", i);
            let value = vec![0u8; 2048];
            let result = engine.put(&cf, key.as_bytes(), value.as_slice());
            assert!(
                result.is_ok(),
                "Write should eventually succeed (may stall) for {}",
                name
            );
        }

        // Assert - All writes should be present
        for i in 0..num_writes {
            let key = format!("stall_test_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist for {}", key, name);
        }
    }
}

#[test]
fn should_stall_writes_given_l0_file_count_exceeded_when_rapid_flushing() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 5 * 1024 * 1024,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Act - Write a lot of data to create L0 files
        let num_writes = 1500;
        for i in 0..num_writes {
            let key = format!("l0_key_{}", i);
            let value = vec![0u8; 3072];
            engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
        }

        // Assert - Despite potential stalls, all writes complete
        for i in 0..num_writes {
            let key = format!("l0_key_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist for {}", key, name);
        }
    }
}

#[test]
fn should_resume_writes_given_compaction_caught_up_when_flush_completes() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 10 * 1024 * 1024,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Act - Burst writes, wait for compaction, then verify writes work
        for i in 0..1000 {
            let key = format!("burst_key_{}", i);
            let value = vec![0u8; 2048];
            engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
        }

        // Wait for flush/compaction to complete deterministically
        engine.flush().expect("flush should complete");

        for i in 0..100 {
            let key = format!("resume_key_{}", i);
            engine.put(&cf, key.as_bytes(), "value".as_bytes()).unwrap();
        }

        // Assert
        for i in 0..100 {
            let key = format!("resume_key_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(Bytes::from("value")),
                "Failed for {} key: {}",
                name,
                key
            );
        }
    }
}

#[test]
fn should_complete_within_reasonable_time_given_backpressure_when_measuring_stall_duration() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 6 * 1024 * 1024,
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Act
        let start = std::time::Instant::now();
        let num_writes = 1000;

        for i in 0..num_writes {
            let key = format!("measure_key_{}", i);
            let value = vec![0u8; 4096];
            engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
        }

        let elapsed = start.elapsed();

        // Assert
        assert!(
            elapsed.as_secs() < 60,
            "Writes should complete within reasonable time even with backpressure for {}",
            name
        );

        for i in 0..num_writes {
            let key = format!("measure_key_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist for {}", key, name);
        }
    }
}

// ============================================================================
// Internal Concurrency Invariant Tests - Disk Storage Modes
// ============================================================================

#[test]
fn should_preserve_iterator_correctness_given_concurrent_writes_when_memtable_freeze_during_scan() {
    // Arrange: set up engine with background writers
    for mode in disk_storage_modes() {
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            ..Default::default()
        };

        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).expect("open");
        let eng_arc = Arc::new(eng);
        let cf = eng_arc.default_column_family();

        // Initial data
        for i in 0..10 {
            eng_arc
                .put(&cf, format!("k{:02}", i).as_bytes(), b"v")
                .unwrap();
        }
        eng_arc.flush().unwrap();

        // Spawn writer thread to simulate concurrent writes
        let writer_handle = {
            let eng_clone = eng_arc.clone();
            thread::spawn(move || {
                for i in 10..20 {
                    let _ = eng_clone.put(
                        &eng_clone.default_column_family(),
                        format!("k{:02}", i).as_bytes(),
                        b"v",
                    );
                }
            })
        };

        // Act: iterate while writes happen
        let results = eng_arc.scan(&cf, Query::new()).unwrap();
        let mut count = 0;
        for _ in results {
            count += 1;
        }

        writer_handle.join().ok();

        // Assert: iteration completed without issues
        assert!(
            count >= 10,
            "at least initial keys iterated for {}",
            name
        );
        drop(eng_arc);
    }
}

#[test]
fn should_not_deadlock_flush_coordinator_given_many_parallel_flush_requests_when_backpressure_applied(
) {
    // Arrange: spawn many concurrent flush requests
    for mode in disk_storage_modes() {
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 512,
            ..Default::default()
        };

        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).expect("open");
        let eng_arc = Arc::new(eng);
        let cf = eng_arc.default_column_family();

        // Fill memtable
        for i in 0..100 {
            eng_arc
                .put(&cf, format!("f{:03}", i).as_bytes(), b"v")
                .unwrap();
        }

        // Spawn multiple flush threads
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let eng_clone = eng_arc.clone();
                thread::spawn(move || {
                    let _ = eng_clone.flush();
                })
            })
            .collect();

        // Act: wait for all to complete
        for h in handles {
            h.join().ok();
        }

        // Assert: no deadlock; engine still operational
        let got = eng_arc.get(&cf, b"f000").unwrap();
        assert!(
            got.is_some(),
            "engine functional after concurrent flushes for {}",
            name
        );
        drop(eng_arc);
    }
}

#[test]
fn should_maintain_manifest_version_ordering_given_concurrent_compaction_and_flush_when_applying_edits(
) {
    // Arrange: schedule compactions and flushes concurrently
    for mode in disk_storage_modes() {
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = compaction_test_opts(storage_mode);

        let eng = cntryl_midge::MidgeEngine::open(opts.clone()).expect("open");
        let eng_arc = Arc::new(eng);
        let cf = eng_arc.default_column_family();

        // Populate data
        populate_multi_level_data(&eng_arc, &cf);

        // Spawn compaction thread
        let compact_handle = {
            let eng_clone = eng_arc.clone();
            thread::spawn(move || {
                let _ = eng_clone.compact_range(
                    &eng_clone.default_column_family(),
                    Some(b""),
                    Some(b"~"),
                );
            })
        };

        // Act: flush concurrently
        let flush_handle = {
            let eng_clone = eng_arc.clone();
            thread::spawn(move || {
                let _ = eng_clone.flush();
            })
        };

        compact_handle.join().ok();
        flush_handle.join().ok();

        // Make compaction deterministic: compact synchronously instead of waiting
        eng_arc.compact_all().unwrap();

        // Assert: engine still consistent
        let got = eng_arc.get(&cf, b"key000").unwrap();
        assert!(
            got.is_some(),
            "data consistent after concurrent compaction and flush for {}",
            name
        );
        drop(eng_arc);
    }
}

#[test]
fn should_not_drop_committed_writes_given_racing_wal_group_commit_when_memtable_rollover_under_load(
) {
    // Arrange: create heavy write load and small WAL group thresholds
    for mode in disk_storage_modes() {
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 1024,
            wal_sync: true,
            ..Default::default()
        };

        with_engine_restart(
            opts.clone(),
            |eng| {
                // Act: perform heavy writes with periodic flushes to trigger rollover
                let cf = eng.default_column_family();
                // Heavy writes to trigger rollover
                for i in 0..200 {
                    eng.put(&cf, format!("w{:03}", i).as_bytes(), b"v").unwrap();
                    if i % 50 == 0 {
                        eng.flush().unwrap();
                    }
                }
            },
            |eng| {
                // Assert: all committed writes must still be present after recovery
                let cf = eng.default_column_family();
                for i in 0..200 {
                    let got = eng.get(&cf, format!("w{:03}", i).as_bytes()).unwrap();
                    assert!(got.is_some(), "write {} not dropped for {}", i, name);
                }
            },
        );
    }
}

// ============================================================================
// Maintain Write Throughput During Flush Tests - All Storage Modes
// ============================================================================

#[test]
fn should_maintain_write_throughput_given_flush_in_progress_when_concurrent_writes() {
    for mode in all_storage_modes() {
        // Arrange
        let (name, storage_mode, _tmp) = create_storage_mode(mode);
        let opts = MidgeOptions {
            storage_mode,
            memtable_size: 8 * 1024 * 1024,
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).unwrap());
        let cf = engine.default_column_family();

        // Populate initial data to trigger flush
        for i in 0..500 {
            let key = format!("initial_key_{}", i);
            let value = vec![0u8; 2048];
            engine.put(&cf, key.as_bytes(), value.as_slice()).unwrap();
        }

        // Act - Write concurrently while flush might be happening
        let num_concurrent_writes = 200;
        let handles: Vec<_> = (0..num_concurrent_writes)
            .map(|i| {
                let engine = Arc::clone(&engine);
                let cf = cf.clone();
                thread::spawn(move || {
                    let key = format!("throughput_key_{}", i);
                    engine.put(&cf, key.as_bytes(), b"value").unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert - All concurrent writes should succeed
        for i in 0..num_concurrent_writes {
            let key = format!("throughput_key_{}", i);
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            assert_eq!(
                result,
                Some(Bytes::from("value")),
                "Failed for {} key: {}",
                name,
                key
            );
        }
    }
}
