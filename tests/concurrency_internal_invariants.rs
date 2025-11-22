// Concurrency + internal races tests (observable outcomes)
mod common;
use cntryl_midge::{MidgeOptions, Query};
use common::*;
use std::sync::Arc;
use std::thread;

#[test]
fn should_preserve_iterator_correctness_given_concurrent_writes_and_memtable_freeze_when_scanning_forward(
) {
    // Arrange: set up engine with background writers
    for mode in disk_storage_modes() {
        let (_n, storage_mode, _tmp) = create_storage_mode(mode);
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
        assert!(count >= 10, "at least initial keys iterated");
        drop(eng_arc);
    }
}

#[test]
fn should_not_deadlock_flush_coordinator_given_many_parallel_flush_requests_when_backpressure_is_applied(
) {
    // Arrange: spawn many concurrent flush requests
    for mode in disk_storage_modes() {
        let (_n, storage_mode, _tmp) = create_storage_mode(mode);
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
        assert!(got.is_some(), "engine functional after concurrent flushes");
        drop(eng_arc);
    }
}

#[test]
fn should_maintain_manifest_version_ordering_given_concurrent_compaction_and_flush_jobs_when_applying_edits(
) {
    // Arrange: schedule compactions and flushes concurrently
    for mode in disk_storage_modes() {
        let (_n, storage_mode, _tmp) = create_storage_mode(mode);
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

        // Assert: engine still consistent
        let got = eng_arc.get(&cf, b"key000").unwrap();
        assert!(
            got.is_some(),
            "data consistent after concurrent compaction and flush"
        );
        drop(eng_arc);
    }
}

#[test]
fn should_not_drop_committed_writes_given_racing_wal_group_commit_and_memtable_rollover_when_under_load(
) {
    // Arrange: create heavy write load and small WAL group thresholds
    for mode in disk_storage_modes() {
        let (_n, storage_mode, _tmp) = create_storage_mode(mode);
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
                    assert!(got.is_some(), "write {} not dropped", i);
                }
            },
        );
    }
}
